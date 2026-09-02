"""Real-boundary regression gates added after cmms-series-fixes review round 1."""
# ruff: noqa: E501

from __future__ import annotations

import ast
import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError, OperationalError
from storage.base import utcnow
from storage.models.fact import FactORM
from storage.outbox import OutboxEntryORM, OutboxRepository
from storage.outbox_worker import OutboxWorker
from storage.repositories import FactRepository

from memory_server.api.learn import learn
from memory_server.api.remember import remember
from memory_server.models import Fact
from memory_server.plugins.hermes.provider import HermesProvider
from memory_server.plugins.hermes.resolver import ExtractorRuntimeConfig
from memory_server.plugins.hermes.writer import WriterQueue
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


async def _real_ingestion_race(provider, barrier, source):
    """Race two real learn() transactions after their production SELECT."""
    original = FactRepository.find_existing
    arrived = {"count": 0}

    async def synchronized_find(self, subject, predicate, object):
        result = await original(self, subject, predicate, object)
        arrived["count"] += 1
        if arrived["count"] <= 2:
            await barrier.wait()
        return result

    try:
        result = await learn(
            provider,
            "Docker is container",
            source=source,
            llm_extractor=lambda _text: {
                "facts": [{"subject": "Docker", "predicate": "is", "object": "container", "confidence": 0.7}],
                "decisions": [],
            },
        )
        result["_race_outcome"] = "initial-success"
        result["_race_source"] = source
        return result
    except (OperationalError, IntegrityError) as exc:
        # SQLite snapshot upgrade can classify the loser as busy; recover by
        # re-entering the same production ingestion boundary.
        assert "locked" in str(exc).lower() or "busy" in str(exc).lower() or "unique" in str(exc).lower()
        result = await learn(
            provider, "Docker is container", source=f"{source}-recovery",
            llm_extractor=lambda _text: {"facts": [{"subject": "Docker", "predicate": "is", "object": "container", "confidence": 0.7}], "decisions": []},
        )
        result["_race_outcome"] = "loser-recovered"
        result["_race_source"] = source
        return result


@pytest.mark.asyncio
async def test_f1_synchronized_real_inserts_have_one_active_row_and_explicit_loser(tmp_path, monkeypatch):
    db = tmp_path / "race.db"
    url = f"sqlite+aiosqlite:///{db}"
    providers = [SQLiteProvider(url=url), SQLiteProvider(url=url)]
    for provider in providers:
        await provider.initialize()
    barrier = asyncio.Barrier(2)
    original = FactRepository.find_existing
    calls = 0

    async def synchronized_find(self, subject, predicate, object):
        nonlocal calls
        result = await original(self, subject, predicate, object)
        calls += 1
        if calls <= 2:
            await barrier.wait()
        return result

    monkeypatch.setattr(FactRepository, "find_existing", synchronized_find)
    try:
        results = await asyncio.gather(*(
            _real_ingestion_race(provider, barrier, source)
            for provider, source in zip(providers, ("one", "two"), strict=True)
        ))
        for provider, source in zip(providers, ("one", "two"), strict=True):
            async with provider._session_factory() as session:
                async with session.begin():
                    fact = (await provider.search_facts(subject="Docker", include_inactive=True))[0]
                    await reinforce_memory_item(
                        session, memory_type="fact", item_id=fact.id,
                        new_confidence=0.8, source=f"{source}-confirmation",
                    )
    finally:
        for provider in providers:
            await provider.close()
    with sqlite3.connect(db) as conn:
        active = conn.execute("SELECT count(*) FROM facts WHERE lifecycle_state IN ('candidate','validated','active')").fetchone()[0]
        receipt_rows = conn.execute("SELECT count(*) FROM receipts WHERE memory_type='fact'").fetchone()[0]
        receipt = conn.execute(
            "SELECT source, confidence, version, history FROM receipts WHERE memory_type='fact'"
        ).fetchone()
        outbox_rows = conn.execute("SELECT count(*) FROM outbox_entries WHERE record_type='fact'").fetchone()[0]
    assert len(results) == 2
    assert all(result["facts"] or result["receipts"] for result in results)
    assert active == 1
    assert receipt_rows == 1
    assert receipt is not None
    receipt_source, confidence, version, raw_history = receipt
    history = json.loads(raw_history)
    assert len(history) >= 2
    sources = {receipt_source, *(entry["source"] for entry in history)}
    assert {"one", "two"} <= {source.split("-", 1)[0] for source in sources}
    assert sum(result["_race_outcome"] == "loser-recovered" for result in results) == 1
    assert sum(result["_race_outcome"] == "initial-success" for result in results) == 1
    loser = next(result for result in results if result["_race_outcome"] == "loser-recovered")
    assert loser["_race_source"] in {"one", "two"}
    assert any(entry["source"].startswith(loser["_race_source"]) for entry in history)
    assert [entry["confidence"] for entry in history] == sorted(entry["confidence"] for entry in history)
    assert float(confidence) >= 0.7
    assert str(version) == "0.1.0"
    assert outbox_rows in (0, 1)  # loser recovery may roll back its outbox write


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
    hermes = HermesProvider()
    hermes._provider = provider
    hermes._extractor_runtime = ExtractorRuntimeConfig("regex", None, 5.0, 10000, 0.0)
    hermes._llm_extractor = lambda _text: {
        "facts": [{"subject": "locked", "predicate": "is", "object": "busy", "confidence": 0.7}],
        "decisions": [],
    }
    queue = WriterQueue(
        hermes._handle_batch_write,
        flush_interval=60,
        max_batch=1,
    )
    await queue.add_turn({"user_content": "locked is busy", "assistant_content": ""}, "locked-turn")
    started = time.monotonic()
    try:
        flushed = await asyncio.wait_for(queue.shutdown(), timeout=1)
        elapsed = time.monotonic() - started
        assert flushed == 0
        assert elapsed <= 0.2 * 3 + 0.4
        assert queue.failed_items
        assert "OperationalError" in queue.failed_items[0]["error"]
        assert "locked" in queue.failed_items[0]["error"].lower()
        assert queue.total_requeued == 2
        assert queue.total_failed == 1
    finally:
        raw.rollback()
        raw.close()
        await provider.close()


@pytest.mark.asyncio
async def test_w7_two_real_sessions_preserve_one_fact_receipt_and_monotonic_reinforcement(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="dedup_key"):
        FactORM.from_pydantic(Fact(id="boundary", subject="s", predicate="p", object="o"))
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'w7.db'}"
    provider_a = SQLiteProvider(url=db_url)
    provider_b = SQLiteProvider(url=db_url)
    await provider_a.initialize()
    await provider_b.initialize()
    try:
        barrier = asyncio.Barrier(2)
        original = FactRepository.find_existing
        calls = 0

        async def synchronized_find(self, subject, predicate, object):
            nonlocal calls
            result = await original(self, subject, predicate, object)
            calls += 1
            if calls <= 2:
                await barrier.wait()
            return result

        monkeypatch.setattr(FactRepository, "find_existing", synchronized_find)
        results = await asyncio.gather(
            _real_ingestion_race(provider_a, barrier, "one"),
            _real_ingestion_race(provider_b, barrier, "two"),
        )
        fact = (await provider_a.search_facts(subject="Docker", include_inactive=True))[0]
        async with provider_a._session_factory() as session:
            async with session.begin():
                await reinforce_memory_item(session, memory_type="fact", item_id=fact.id, new_confidence=0.8, source="one-confirmation")
                await reinforce_memory_item(session, memory_type="fact", item_id=fact.id, new_confidence=0.85, source="two-confirmation")
                await reinforce_memory_item(session, memory_type="fact", item_id=fact.id, new_confidence=0.9, source="high")
        fresh = await provider_a.get_fact(fact.id)
        receipts = await provider_a.search_receipts(memory_type="fact", limit=20)
    finally:
        await provider_a.close()
        await provider_b.close()
    assert all(result["facts"][0]["item"]["id"] == fact.id for result in results)
    assert fresh.confidence >= 0.9 and fresh.version >= 2
    assert len(receipts) == 1
    assert len(receipts[0].history) >= 2
    sources = {receipts[0].source, *(entry["source"] for entry in receipts[0].history)}
    assert {"one", "two"} <= {source.split("-", 1)[0] for source in sources}
    assert [entry["confidence"] for entry in receipts[0].history] == sorted(entry["confidence"] for entry in receipts[0].history)
    assert sum(result["_race_outcome"] == "loser-recovered" for result in results) == 1
    assert sum(result["_race_outcome"] == "initial-success" for result in results) == 1


@pytest.mark.asyncio
async def test_w8_two_workers_barrier_between_selection_and_claim(tmp_path):
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'w8.db'}"
    calls = []

    class Graph:
        def sync_facts_batch(self, payloads):
            calls.extend(payloads)
            return True

        def sync_fact(self, subject, predicate, obj):
            calls.append({"subject": subject, "predicate": predicate, "object": obj})
            return True

    workers = [OutboxWorker(db_url=db_url, graph_router=Graph(), poll_interval_seconds=0.01, stale_processing_seconds=10**9) for _ in range(2)]
    for worker in workers:
        await worker.initialize()
    async with workers[0]._session_factory() as session:
        await OutboxRepository(session).add_entry("fact", "w8-fact", "index_fact", {"subject": "s", "predicate": "p", "object": "o"})
        await session.commit()
    barrier = asyncio.Barrier(2)
    arrivals = 0

    async def between(candidate_ids):
        nonlocal arrivals
        arrivals += 1
        await barrier.wait()

    original_hook = OutboxRepository.claim_between_select_and_update
    OutboxRepository.claim_between_select_and_update = between
    original_reset = OutboxRepository.reset_stale_processing
    async def no_stale_rows(self, max_age_seconds=600):
        return 0
    OutboxRepository.reset_stale_processing = no_stale_rows
    try:
        results = await asyncio.gather(*(worker._poll_once() for worker in workers))
    finally:
        OutboxRepository.reset_stale_processing = original_reset
        OutboxRepository.claim_between_select_and_update = original_hook
        for worker in workers:
            await worker.close()
    assert arrivals == 2
    assert sum(results) == 1
    assert len(calls) == 1



@pytest.mark.asyncio
async def test_w8_stale_processing_is_recovered_once(tmp_path):
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'stale.db'}"
    calls = []

    class Graph:
        def sync_facts_batch(self, payloads):
            calls.extend(payloads)
            return True

    worker = OutboxWorker(db_url=db_url, graph_router=Graph(), stale_processing_seconds=0)
    await worker.initialize()
    try:
        async with worker._session_factory() as session:
            repo = OutboxRepository(session)
            entry = await repo.add_entry("fact", "stale", "index_fact", {"subject": "s", "predicate": "p", "object": "o"})
            await session.flush()
            await repo.mark_processing(entry.id)
            row = await session.get(OutboxEntryORM, entry.id)
            row.processed_at = utcnow()
            await session.commit()
        # The worker boundary performs stale reset and claim, then processes once.
        assert await worker._poll_once() == 1
        assert len(calls) == 1
    finally:
        await worker.close()


def test_f4_reference_rebuild_preserves_independent_counts_and_remaps_endpoints(tmp_path):
    from tests.test_migration_fact_dedup import create_fixture_database, run_upgrade, write_copy_ini

    db = tmp_path / "full-pre-b2.db"
    create_fixture_database(db)
    expected_pre = {
        "facts": 12, "receipts": 3, "outbox_entries": 3,
        "lifecycle_states": 2, "lifecycle_events": 2, "claim_relations": 2,
    }
    with sqlite3.connect(db) as conn:
        assert {table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in expected_pre} == expected_pre
        fact_ids = {row[0] for row in conn.execute("SELECT id FROM facts")}
        assert all(row[1] in fact_ids for row in conn.execute("SELECT id, memory_id FROM lifecycle_states WHERE memory_type='fact'"))
        assert any(row[1] == "fact" for row in conn.execute("SELECT id, memory_type FROM receipts"))
    ini = tmp_path / "full-upgrade.ini"
    write_copy_ini(ini, db.resolve())
    result = run_upgrade(db, ini)
    assert result.returncode == 0, result.stderr
    with sqlite3.connect(db) as conn:
        expected_post = {
            "facts": 6, "receipts": 1, "outbox_entries": 1,
            "lifecycle_states": 2, "lifecycle_events": 2, "claim_relations": 2,
        }
        assert {table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in expected_post} == expected_post
        survivors = {row[0] for row in conn.execute("SELECT id FROM facts")}
        deleted = {"fact-active-1", "fact-active-3", "fact-active-4", "fact-archived-2", "fact-empty-1", "fact-tie-b"}
        assert deleted.isdisjoint(survivors)
        refs = [row[0] for row in conn.execute("SELECT memory_id FROM lifecycle_states")]
        refs += [row[0] for row in conn.execute("SELECT memory_id FROM lifecycle_events")]
        refs += [x for row in conn.execute("SELECT source_id, target_id FROM claim_relations") for x in row]
        assert set(refs) <= survivors | {"decision-1"}
        assert conn.execute("SELECT count(*) FROM lifecycle_events WHERE memory_id IN ('fact-active-2','fact-empty-2')").fetchone()[0] == 1


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
if {case!r} in ("identity", "postcondition"):
    from tests.test_migration_fact_dedup import create_fixture_database, write_copy_ini
    create_fixture_database(p)
    with sqlite3.connect(p) as probe:
        if {case!r} == "identity":
            probe.execute("CREATE TRIGGER mutate_decision AFTER UPDATE OF dedup_key ON facts BEGIN UPDATE decisions SET choice=choice || ' mutated' WHERE id='decision-1'; END")
    if {case!r} == "postcondition":
        with sqlite3.connect(p) as probe:
            probe.execute("CREATE TRIGGER mutate_survivor AFTER UPDATE OF dedup_key ON facts BEGIN UPDATE facts SET subject=subject || ' mutated' WHERE id='fact-active-2'; END")
            probe.commit()
    ini=Path({str(tmp_path / (case + '.ini'))!r})
    write_copy_ini(ini, p.resolve())
    ini.write_text(ini.read_text().replace("/home/shtorm/memory-server/migrations", str(Path({str(REPO / 'migrations')!r}))))
    from alembic import command, config
    command.upgrade(config.Config(str(ini)), "head")
    raise RuntimeError("real guard accepted a violating fixture")
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
    with sqlite3.connect(p) as probe:
        probe.execute("CREATE TABLE identity_rows(id TEXT PRIMARY KEY, value TEXT)")
        probe.execute("INSERT INTO identity_rows VALUES ('real-row', 'before')")
        probe.commit()
        before = {{"rows": {{"identity_rows": tuple(probe.execute("SELECT * FROM identity_rows"))}}}}
        probe.execute("UPDATE identity_rows SET value='after' WHERE id='real-row'")
        probe.commit()
        after = {{"rows": {{"identity_rows": tuple(probe.execute("SELECT * FROM identity_rows"))}}}}
    m._assert_untouched_rows(before, after, ["identity_rows"])
elif {case!r} == "postcondition":
    with sqlite3.connect(p) as probe:
        probe.execute("CREATE TABLE postcondition_rows(id TEXT PRIMARY KEY, value TEXT)")
        probe.execute("INSERT INTO postcondition_rows VALUES ('real-row', 'before')")
        probe.commit()
        before = {{"rows": {{"postcondition_rows": tuple(probe.execute("SELECT * FROM postcondition_rows"))}}}}
        probe.execute("UPDATE postcondition_rows SET value='after' WHERE id='real-row'")
        probe.commit()
        after = {{"rows": {{"postcondition_rows": tuple(probe.execute("SELECT * FROM postcondition_rows"))}}}}
    try:
        m._assert_untouched_rows(before, after, ["postcondition_rows"])
    except RuntimeError as exc:
        raise RuntimeError("postcondition violation: " + str(exc)) from exc
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


def test_w9_degraded_wal_seam_executes_real_pragma_and_fails_closed(tmp_path):
    assert _USING_PRODUCTION_SQLITE_SUPPORT
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'degraded.db'}")
    real = engine.connect()

    observed = []

    class ResultProxy:
        def __init__(self, result, value):
            self.result, self.value = result, value
            observed.append(value)
        def scalar_one(self):
            return self.value

    class DegradedResultConnection:
        def execute(self, statement, *args, **kwargs):
            result = real.execute(statement, *args, **kwargs)
            if "journal_mode=WAL" in str(statement):
                return ResultProxy(result, "delete")
            return result

    with pytest.raises(RuntimeError, match="degraded journal_mode"):
        apply_sqlite_pragmas_sync(DegradedResultConnection(), 5000, context="real-file-probe")
    assert observed == ["delete"]
    assert real.exec_driver_sql("PRAGMA journal_mode").scalar_one().lower() == "wal"
    real.close()
    engine.dispose()


def test_w9_migration_timeout_is_set_before_ddl(tmp_path):
    from tests.test_migration_fact_dedup import create_fixture_database, write_copy_ini

    db = tmp_path / "contention.db"
    create_fixture_database(db)
    ini = tmp_path / "contention.ini"
    write_copy_ini(ini, db.resolve())
    ini.write_text(ini.read_text().replace("/home/shtorm/memory-server/migrations", str(REPO / "migrations")))
    events = tmp_path / "migration-connection.jsonl"
    conn = sqlite3.connect(db, timeout=0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("BEGIN IMMEDIATE")
    env = os.environ.copy()
    env["MEMORY_SERVER_SQLITE_BUSY_TIMEOUT_MS"] = "200"
    env["B2_MIGRATION_CONNECTION_EVENT_PATH"] = str(events)
    started = time.monotonic()
    try:
        run = subprocess.run(
            [sys.executable, "-m", "alembic", "-c", str(ini), "upgrade", "head"],
            cwd=str(REPO), env=env, capture_output=True, text=True, check=False,
        )
        elapsed = time.monotonic() - started
    finally:
        conn.rollback()
        conn.close()
    assert run.returncode != 0
    assert elapsed < 1.5
    records = [json.loads(line) for line in events.read_text().splitlines()]
    assert records and records[0]["event"] == "before_first_ddl"
    assert records[0]["busy_timeout"] == 200
    assert records[0]["journal_mode"] == "wal"
    with sqlite3.connect(db) as check:
        assert check.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    assert "locked" in (run.stdout + run.stderr).lower() or "busy" in (run.stdout + run.stderr).lower()
