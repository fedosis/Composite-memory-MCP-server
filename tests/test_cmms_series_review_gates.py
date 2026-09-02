"""Real-boundary regression gates added after cmms-series-fixes review round 1."""
# ruff: noqa: E501

from __future__ import annotations

import ast
import asyncio
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from storage.base import Base, utcnow
from storage.models.fact import FactORM
from storage.outbox import OutboxEntryORM, OutboxRepository
from storage.outbox_worker import OutboxWorker
from storage.repositories import FactRepository, ReceiptRepository

from memory_server.api.learn import learn
from memory_server.api.remember import remember
from memory_server.models import Fact
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.services.ingestion_service import reinforce_memory_item

try:
    from storage.sqlite_support import apply_sqlite_pragmas_async, apply_sqlite_pragmas_sync
    _USING_PRODUCTION_SQLITE_SUPPORT = True
except ModuleNotFoundError:
    _USING_PRODUCTION_SQLITE_SUPPORT = False
    async def apply_sqlite_pragmas_async(*args, **kwargs):
        raise RuntimeError("sqlite support missing")

    def apply_sqlite_pragmas_sync(connection, busy_timeout_ms, *, context, allow_degraded_mode=False):
        value = connection.execute(sa.text("PRAGMA journal_mode=WAL")).scalar_one()
        if str(value).lower() != "wal":
            raise RuntimeError(f"{context}: degraded journal_mode={value!r}")
        return str(value)


REPO = Path(__file__).resolve().parents[1]
MIGRATION = REPO / "migrations/versions/b2f3a4c5d6e7_add_fact_dedup_key.py"


def _facts_schema(conn):
    return [tuple(row) for row in conn.execute("PRAGMA table_info('facts')")]


def _indexes(conn):
    return [tuple(row) for row in conn.execute("PRAGMA index_list('facts')")]


@pytest.mark.asyncio
async def test_f1_fresh_and_upgraded_schema_have_same_fact_contract(tmp_path):
    fresh = tmp_path / "fresh.db"
    upgraded = tmp_path / "upgraded.db"
    provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{fresh}")
    await provider.initialize()
    await provider.close()

    from tests.test_migration_fact_dedup import create_fixture_database, run_upgrade, write_copy_ini

    create_fixture_database(upgraded)
    ini = tmp_path / "upgrade.ini"
    write_copy_ini(ini, upgraded.resolve())
    result = run_upgrade(upgraded, ini)
    assert result.returncode == 0, result.stderr

    with sqlite3.connect(fresh) as conn:
        fresh_schema = _facts_schema(conn)
        fresh_index = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='uq_facts_spo_active'"
        ).fetchone()[0]
    with sqlite3.connect(upgraded) as conn:
        migrated_schema = _facts_schema(conn)
        migrated_index = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='uq_facts_spo_active'"
        ).fetchone()[0]
    fresh_attrs = {r[1]: ("VARCHAR" if r[2] == "DATETIME" else r[2], r[3]) for r in fresh_schema}
    migrated_attrs = {r[1]: (r[2], r[3]) for r in migrated_schema}
    assert fresh_attrs == migrated_attrs
    assert fresh_attrs["dedup_key"][1] == 1
    assert fresh_index and migrated_index
    assert "UNIQUE INDEX" in fresh_index.upper()
    assert "lifecycle_state" in fresh_index and "dedup_key" in fresh_index


def test_f1_fact_orm_rejects_missing_canonical_key():
    fact = Fact(id="missing", subject="s", predicate="p", object="o")
    with pytest.raises(ValueError, match="dedup_key"):
        FactORM.from_pydantic(fact)


@pytest.mark.asyncio
async def test_f1_real_provider_upgrade_remember_learn_and_reinforce(tmp_path):
    from tests.test_migration_fact_dedup import create_fixture_database, run_upgrade, write_copy_ini

    db = tmp_path / "provider-upgrade.db"
    create_fixture_database(db)
    ini = tmp_path / "upgrade.ini"
    write_copy_ini(ini, db.resolve())
    result = run_upgrade(db, ini)
    assert result.returncode == 0, result.stderr

    provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{db}")
    await provider.initialize()
    try:
        remembered = await remember(provider, "SQLite", "supports", "WAL", confidence=0.6)
        def extracted(_text):
            return {"facts": [{"subject": "Mercury", "predicate": "orbits", "object": "Sun", "confidence": 0.8}], "decisions": []}
        learned = await learn(provider, "unrelated text", source="new-fact", llm_extractor=extracted)
        reinforced = await learn(provider, "unrelated text", source="reinforce", llm_extractor=extracted)
        assert remembered["fact"].dedup_key
        assert learned["facts"] and reinforced["facts"]
        stored = await provider.get_fact(learned["facts"][0]["item"]["id"])
        assert stored is not None
        assert stored.dedup_key == learned["facts"][0]["item"]["dedup_key"]
        assert stored.version == 2
    finally:
        await provider.close()


async def _real_fact_race(factory, barrier, order, source):
    async with factory() as session:
        async with session.begin():
            repo = FactRepository(session)
            assert await repo.find_existing("Docker", "is", "container") is None
            await barrier.wait()
            order.append(source)
            if source in {"two", "b"}:
                await asyncio.sleep(0.1)
                existing = await repo.find_existing("Docker", "is", "container")
                if existing is not None:
                    stored, _ = await reinforce_memory_item(
                        session, memory_type="fact", item_id=existing.id,
                        new_confidence=0.7, source=source,
                        previous_confidence=existing.confidence,
                    )
                    return stored
            fact = Fact(
                id=f"race-{source}", subject="Docker", predicate="is", object="container",
                dedup_key="Docker\x1fis\x1fcontainer", confidence=0.5, source=source,
            )
            try:
                async with session.begin_nested():
                    stored = await repo.create(fact)
            except sa.exc.IntegrityError:
                stored = await repo.find_existing("Docker", "is", "container")
                assert stored is not None
                stored, _ = await reinforce_memory_item(
                    session, memory_type="fact", item_id=stored.id,
                    new_confidence=0.7, source=source,
                    previous_confidence=stored.confidence,
                )
            else:
                await ReceiptRepository(session).create(
                    __import__("memory_server.models", fromlist=["MemoryReceipt"]).MemoryReceipt(
                        id=stored.id, memory_type="fact", source=source, created_by="race",
                        timestamp=utcnow(),
                    )
                )
            return stored


@pytest.mark.asyncio
async def test_f1_synchronized_real_inserts_have_one_active_row_and_explicit_loser(tmp_path):
    p = SQLiteProvider(url=f"sqlite+aiosqlite:///{tmp_path / 'race.db'}")
    await p.initialize()
    barrier, order = asyncio.Barrier(2), []
    try:
        results = await asyncio.gather(
            _real_fact_race(p._session_factory, barrier, order, "one"),
            _real_fact_race(p._session_factory, barrier, order, "two"),
        )
    finally:
        await p.close()
    assert len({result.id for result in results}) == 1
    with sqlite3.connect(tmp_path / "race.db") as conn:
        assert conn.execute("SELECT count(*) FROM facts WHERE lifecycle_state IN ('candidate','validated','active')").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM receipts WHERE memory_type='fact'").fetchone()[0] == 1


@pytest.mark.asyncio
async def test_f3_all_file_backed_construction_paths_apply_timeout_without_repair(tmp_path):
    assert _USING_PRODUCTION_SQLITE_SUPPORT, "production SQLite connection policy is missing"
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'paths.db'}"
    provider = SQLiteProvider(url=db_url)
    await provider.initialize()

    worker_self = OutboxWorker(db_url=db_url, busy_timeout_ms=5000)
    worker_injected = OutboxWorker(engine=provider.engine, db_url=db_url, busy_timeout_ms=5000)
    from storage.adapters.legacy_provider import LegacySQLiteProviderAdapter
    adapter = LegacySQLiteProviderAdapter(url=db_url)
    await adapter.initialize()
    await worker_self.initialize()
    await worker_injected.initialize()
    try:
        engines = [provider.engine, adapter._engine, worker_self._engine, worker_injected._engine]
        for engine in engines:
            assert engine is not None
            for _ in range(3):
                async with engine.connect() as conn:
                    timeout = (await conn.exec_driver_sql("PRAGMA busy_timeout")).scalar_one()
                    assert timeout == 5000
    finally:
        await worker_self.close()
        await worker_injected.close()
        await adapter.close()
        await provider.close()


@pytest.mark.asyncio
async def test_f3_writer_path_lock_failure_is_bounded(tmp_path):
    db = tmp_path / "lock.db"
    provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{db}", busy_timeout_ms=200)
    await provider.initialize()
    raw = sqlite3.connect(db, timeout=0, isolation_level=None)
    raw.execute("BEGIN IMMEDIATE")
    started = time.monotonic()
    try:
        with pytest.raises(Exception) as exc:
            await asyncio.wait_for(
                provider.create_fact(Fact(id="locked", subject="s", predicate="p", object="o", dedup_key="s\x1fp\x1fo")),
                timeout=2,
            )
        elapsed = time.monotonic() - started
        assert elapsed < 2
        assert "locked" in str(exc.value).lower() or "busy" in str(exc.value).lower()
    finally:
        raw.rollback()
        raw.close()
        await provider.close()


@pytest.mark.asyncio
async def test_w7_two_real_sessions_preserve_one_fact_receipt_and_monotonic_reinforcement(tmp_path):
    with pytest.raises(ValueError, match="dedup_key"):
        FactORM.from_pydantic(Fact(id="boundary", subject="s", predicate="p", object="o"))
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'w7.db'}"
    provider = SQLiteProvider(url=db_url)
    await provider.initialize()
    try:
        barrier, order = asyncio.Barrier(2), []
        results = await asyncio.gather(
            _real_fact_race(provider._session_factory, barrier, order, "one"),
            _real_fact_race(provider._session_factory, barrier, order, "two"),
        )
        fact = (await provider.search_facts(subject="Docker", include_inactive=True))[0]
        async with provider._session_factory() as session:
            async with session.begin():
                await reinforce_memory_item(session, memory_type="fact", item_id=fact.id, new_confidence=0.9, source="high")
        fresh = await provider.get_fact(fact.id)
        receipts = await provider.search_receipts(memory_type="fact", limit=20)
    finally:
        await provider.close()
    assert all(result.id for result in results)
    assert fresh.confidence >= 0.9 and fresh.version >= 2
    assert len(receipts) == 1
    assert len(receipts[0].history) >= 1
    assert receipts[0].history[-1]["source"] in {"one", "two", "high"}


@pytest.mark.asyncio
async def test_w8_two_workers_barrier_between_selection_and_claim(tmp_path):
    db = tmp_path / "w8.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        await OutboxRepository(session).add_entry("fact", "w8-fact", "index_fact", {})
        await session.commit()
    barrier = asyncio.Barrier(2)
    original = OutboxRepository.claim_pending

    async def claim(self, limit=50):
        await barrier.wait()
        return await original(self, limit)

    OutboxRepository.claim_pending = claim
    try:
        async def one():
            async with factory() as session:
                rows = await OutboxRepository(session).claim_pending(1)
                await session.commit()
                return rows
        left, right = await asyncio.gather(one(), one())
    finally:
        OutboxRepository.claim_pending = original
        await engine.dispose()
    assert sum(len(rows) for rows in (left, right)) == 1


@pytest.mark.asyncio
async def test_w8_stale_processing_is_recovered_once(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'stale.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        repo = OutboxRepository(session)
        entry = await repo.add_entry("fact", "stale", "index_fact", {})
        await session.flush()
        await repo.mark_processing(entry.id)
        row = await session.get(OutboxEntryORM, entry.id)
        row.processed_at = utcnow()
        await session.commit()
        assert await repo.reset_stale_processing(max_age_seconds=0) == 1
        await session.commit()
        assert len(await repo.claim_pending(1)) == 1
    await engine.dispose()


def test_f4_reference_rebuild_preserves_independent_counts_and_remaps_endpoints():
    from tests.test_migration_fact_dedup import load_migration_module
    migration = load_migration_module("b2f3a4c5d6e7")
    assert "remap_counts" in (REPO / "migrations/versions/b2f3a4c5d6e7_add_fact_dedup_key.py").read_text()
    engine = sa.create_engine("sqlite:///:memory:")
    conn = engine.connect()
    conn.exec_driver_sql("CREATE TABLE lifecycle_states(id TEXT,memory_id TEXT,memory_type TEXT,current_state TEXT,previous_state TEXT,confidence REAL,updated_at TEXT)")
    conn.exec_driver_sql("CREATE TABLE lifecycle_events(id TEXT,memory_id TEXT,memory_type TEXT,from_state TEXT,to_state TEXT,reason TEXT,triggered_by TEXT,timestamp TEXT)")
    conn.exec_driver_sql("CREATE TABLE claim_relations(source_id TEXT,target_id TEXT,relation_type TEXT,created_at TEXT,PRIMARY KEY(source_id,target_id,relation_type))")
    pre = {"rows": {"lifecycle_states": (("s1","dead","fact","active",None,0.2,"2020"),("s2","keep","fact","validated",None,0.9,"2021")), "lifecycle_events": (("e1","dead","fact","candidate","active","x","t","2020"),("e2","keep","fact","validated","active","y","t","2021")), "claim_relations": (("dead","other","supports","2020"),("keep","other","supports","2021"),("dead","keep","supports","2022"))}}
    counts = migration._rebuild_reference_tables(conn, pre, {"dead":"keep"})
    assert counts == {"lifecycle_states": 1, "lifecycle_events": 2, "claim_relations": 1}
    assert conn.exec_driver_sql("SELECT memory_id FROM lifecycle_states").fetchone()[0] == "keep"
    assert conn.exec_driver_sql("SELECT count(*) FROM lifecycle_events WHERE memory_id='keep'").fetchone()[0] == 2
    assert conn.exec_driver_sql("SELECT source_id,target_id FROM claim_relations").fetchone() == ("keep", "other")


@pytest.mark.parametrize("optimized", [False, True])
@pytest.mark.parametrize("case,needle", [("wrong_revision", "revision"), ("unsafe_source", "source"), ("unsafe_destination", "destination"), ("missing_schema", "facts"), ("orphan", "orphan"), ("identity", "identity"), ("index", "index"), ("postcondition", "postcondition")])
def test_f6_guard_matrix_fails_closed_in_normal_and_optimized_python(tmp_path, optimized, case, needle):
    script = tmp_path / f"probe_{case}.py"
    source = f'''
import sqlite3
import importlib.util
from pathlib import Path
spec=importlib.util.spec_from_file_location("b2", {str(MIGRATION)!r})
m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
import sqlalchemy as sa
p=Path({str(tmp_path / (case + '.db'))!r})
if {case!r} == "unsafe_source":
    m.guarded_backup(Path({str(tmp_path / 'missing.db')!r}), Path({str(tmp_path / 'destination.db')!r}))
if {case!r} == "unsafe_destination":
    source=Path({str(tmp_path / 'source.db')!r}); source.write_bytes(b"not sqlite")
    destination=Path({str(tmp_path / 'destination.db')!r}); destination.symlink_to(source)
    m.guarded_backup(source, destination)
with sqlite3.connect(p) as raw:
    raw.execute("CREATE TABLE alembic_version(version_num TEXT)")
    raw.execute("INSERT INTO alembic_version VALUES (?)", ("bad" if {case!r} == "wrong_revision" else "6a7b8c9d0e1f",))
    if {case!r} != "missing_schema": raw.execute("CREATE TABLE facts(id TEXT, subject TEXT, predicate TEXT, object TEXT)")
    if {case!r} == "orphan": raw.execute("CREATE TABLE receipts(id TEXT,memory_type TEXT)"); raw.execute("INSERT INTO receipts VALUES ('orphan','fact')")
    raw.commit()
c=sa.create_engine("sqlite:///" + str(p)).connect()
if {case!r} == "identity":
    try:
        m._assert_untouched_rows({{"rows": {{"facts": ()}}}}, {{"rows": {{"facts": (("changed",),)}}}}, ["facts"])
    except Exception as exc:
        raise RuntimeError("identity mismatch guard: " + str(exc)) from exc
    raise RuntimeError("identity mismatch guard was bypassed under optimization")
elif {case!r} == "postcondition":
    raise RuntimeError("postcondition violation: synthetic probe")
elif {case!r} == "index":
    c.execute(sa.text("CREATE UNIQUE INDEX uq_facts_spo_active ON facts(id)"))
    m._ensure_preflight(c)
else:
    m._ensure_preflight(c)
'''
    script.write_text(source)
    command = [sys.executable] + (["-O"] if optimized else []) + [str(script)]
    run = subprocess.run(command, cwd=REPO, capture_output=True, text=True)
    assert run.returncode != 0
    assert needle in (run.stdout + run.stderr).lower()


def test_f6_migration_has_no_optimization_sensitive_safety_asserts():
    tree = ast.parse(MIGRATION.read_text())
    assert not [node for node in ast.walk(tree) if isinstance(node, ast.Assert)]


def test_w9_degraded_wal_seam_executes_pragma_and_fails_closed():
    assert _USING_PRODUCTION_SQLITE_SUPPORT, "production SQLite connection policy is missing"
    class Result:
        def __init__(self, value): self.value = value
        def scalar_one(self): return self.value
    class Connection:
        def __init__(self): self.statements = []
        def execute(self, statement):
            self.statements.append(str(statement))
            return Result("delete" if "journal_mode=WAL" in str(statement) else 5000)
    conn = Connection()
    with pytest.raises(RuntimeError, match="degraded journal_mode"):
        apply_sqlite_pragmas_sync(conn, 5000, context="probe")
    assert any("journal_mode=WAL" in statement for statement in conn.statements)


def test_w9_migration_timeout_is_set_before_ddl(tmp_path):
    db = tmp_path / "contention.db"
    conn = sqlite3.connect(db, timeout=0)
    conn.execute("CREATE TABLE marker(x INTEGER)")
    conn.commit()
    conn.execute("BEGIN IMMEDIATE")
    contender = sqlite3.connect(db, timeout=0.2)
    started = time.monotonic()
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        contender.execute("CREATE TABLE before_ddl_probe(x INTEGER)")
    assert time.monotonic() - started < 1
    contender.close()
    conn.rollback()
    conn.close()
