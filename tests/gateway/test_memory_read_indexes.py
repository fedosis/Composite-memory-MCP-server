"""SLICE-01A read-index acceptance tests.

These tests deliberately exercise the real provider SQL, SQLite's planner, the
startup capability gate, and the official Alembic path.  The fixture is local
and synthetic: four ORM tables, 3,000 rows each, with deterministic ties and
microsecond/date boundaries.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import event, text
from storage.models.belief import BeliefORM
from storage.models.decision import DecisionORM
from storage.models.fact import FactORM
from storage.models.skill import SkillORM

from memory_server.application.memory_read_service import ReadKey
from memory_server.providers.sqlite_provider import SQLiteProvider

REPO = Path(__file__).resolve().parents[2]
KINDS = ("fact", "belief", "decision", "skill")
INDEXES = {
    "fact": "ix_facts_created_type_id",
    "belief": "ix_beliefs_created_type_id",
    "decision": "ix_decisions_created_type_id",
    "skill": "ix_skills_created_type_id",
}
BASE = datetime(2024, 1, 1, tzinfo=UTC)
# The reproducible evidence runner substitutes the immutable ad50ec3 class.
# Normal regression runs exercise the retained production fallback instead.
REFERENCE_PROVIDER = SQLiteProvider
EQUIVALENCE_TRACE = None


def _query(limit=200, **overrides):
    values = dict(
        limit=limit,
        cursor=None,
        memory_types=[],
        lifecycle_states=[],
        min_confidence=None,
        max_confidence=None,
        source=None,
        tags=[],
        authority_level=None,
        epistemic_status=None,
        created_from=None,
        created_to=None,
        updated_from=None,
        updated_to=None,
        valid_at=None,
        risk_tags=[],
    )
    values.update(overrides)
    # Exercise the provider boundary with ISO strings without DTO coercion.
    return SimpleNamespace(**values)


def _storage_dt(value: datetime) -> str:
    value = value.astimezone(UTC).replace(tzinfo=None)
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")


def _models(kind, i, created, updated):
    ident = f"memory-{i:04d}"
    common = dict(
        id=ident,
        confidence=(i % 100) / 100,
        source="slice01a",
        creator="slice01a",
        created_at=created,
        updated_at=updated,
        verification_status="candidate",
        lifecycle_state="active" if i % 7 else "archived",
        version=1 if kind == "belief" else "1",
    )
    if kind == "fact":
        return FactORM(**common, subject="s", predicate="p", object=f"o-{i}", dedup_key=f"dedup-{kind}-{i}")
    if kind == "belief":
        return BeliefORM(**common, proposition=f"prop-{i}", source_ids=[], tags=[])
    if kind == "decision":
        return DecisionORM(
            **common,
            context="ctx",
            choice=f"choice-{i}",
            dedup_key=f"dedup-{kind}-{i}",
            rejected_alternatives="[]",
            reason="reason",
        )
    return SkillORM(
        **common,
        name=f"skill-{i}",
        purpose="purpose",
        steps="[]",
        constraints="[]",
        validation="[]",
        success_rate=0.5,
    )


async def _seed_12k(provider):
    rows = []
    for kind in KINDS:
        for i in range(3000):
            # Same timestamps occur in every type; include 0/1/999999 usecs and
            # adjacent days so the type/id tie-breaker is unavoidable.
            created = BASE + timedelta(days=i // 100, microseconds=(0, 1, 999999)[i % 3])
            updated = created + timedelta(hours=1)
            rows.append(_models(kind, i, created, updated))
    async with provider._session_factory() as session:
        async with session.begin():
            session.add_all(rows)


async def _page(provider, query, *, after=None, upper=None):
    async with provider._session_factory() as session:
        return await provider.read_memory_page(session, query, after=after, upper=upper)


def _keys(page):
    return tuple((r.memory["created_at"], r.memory_type, r.memory["id"]) for r in page.records)


def _next_after(page):
    if not page.records:
        return None
    created_at, memory_type, ident = _keys(page)[-1]
    return ReadKey(created_at, memory_type, ident)


def _all_index_sql(db):
    with sqlite3.connect(db) as conn:
        return dict(conn.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"))


def _eqp_lines(statement, params, db):
    with sqlite3.connect(db) as conn:
        conn.create_function("s07_timestamp", 1, lambda value: value)
        return [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + statement, params)]


@pytest.fixture
async def seeded(tmp_path):
    db = tmp_path / "slice01a.db"
    provider = SQLiteProvider(f"sqlite+aiosqlite:///{db}")
    await provider.initialize()
    await _seed_12k(provider)
    try:
        yield provider, db
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_fresh_initialize_has_exact_created_type_indexes_and_reopen_does_not_migrate(tmp_path):
    db = tmp_path / "fresh.db"
    provider = SQLiteProvider(f"sqlite+aiosqlite:///{db}")
    await provider.initialize()
    await provider.close()
    sql = _all_index_sql(db)
    for kind, name in INDEXES.items():
        expected = f"CREATE INDEX {name} ON {kind}s (created_at, CAST('{kind}' AS TEXT), id)"
        assert sql[name] == expected
        with sqlite3.connect(db) as conn:
            info = conn.execute(f"PRAGMA index_xinfo('{name}')").fetchall()
            assert [(r[2], r[3], r[4]) for r in info if r[5]] == [
                ("created_at", 0, "BINARY"),
                (None, 0, "BINARY"),
                ("id", 0, "BINARY"),
            ]
            assert [r for r in conn.execute(f"PRAGMA index_list('{kind}s')") if r[1] == name][0][2:] == (0, "c", 0)
    with sqlite3.connect(db) as conn:
        for name in INDEXES.values():
            conn.execute(f"DROP INDEX {name}")
    before = set(_all_index_sql(db))
    reopened = SQLiteProvider(f"sqlite+aiosqlite:///{db}")
    await reopened.initialize()
    await reopened.close()
    assert set(_all_index_sql(db)) == before
    assert not reopened._read_index_capable
    assert not set(INDEXES.values()) & before


def _assert_index_plan(plan, kinds):
    assert "TEMP B-TREE" not in plan, plan
    for kind in kinds:
        assert f"USING INDEX {INDEXES[kind]}" in plan, plan


@pytest.mark.asyncio
async def test_real_read_page_eqp_uses_each_index_without_temp_sort(seeded):
    provider, db = seeded
    captured = []

    def trace(conn, cursor, statement, params, context, many):
        if statement.lstrip().upper().startswith("SELECT"):
            captured.append((statement, params))

    event.listen(provider.engine.sync_engine, "before_cursor_execute", trace)
    try:
        for limit in (1, 200):
            for memory_types in ([], ["fact"]):
                for filters in (
                    {},
                    dict(created_from="2024-01-03T00:00:00Z", created_to="2024-01-08T00:00:00+00:00"),
                    dict(updated_from="2024-01-03T01:00:00Z", updated_to="2024-01-08T01:00:00+00:00"),
                    dict(lifecycle_states=["active"]),
                ):
                    query = _query(limit, memory_types=memory_types, **filters)
                    captured.clear()
                    first = await _page(provider, query)
                    await _page(provider, query, after=_next_after(first), upper=first.upper)
                    # Also exercise max with an explicit after and no upper.
                    await _page(provider, query, after=_next_after(first))
                    selects = [(sql, params) for sql, params in captured if "ORDER BY" in sql.upper()]
                    assert len(selects) == 5, captured
                    for sql, params in selects:
                        assert "subject,predicate,object" in sql  # real wide projection
                        assert "s07_timestamp(created_at)" not in sql
                        plan = "\n".join(_eqp_lines(sql, params, db))
                        kinds = memory_types or KINDS
                        if filters.get("lifecycle_states") and "belief" in kinds:
                            assert "USING INDEX ix_beliefs_lifecycle_state" in plan, plan
                            assert "TEMP B-TREE FOR ORDER BY" in plan, plan
                            for kind in set(kinds) - {"belief"}:
                                assert f"USING INDEX {INDEXES[kind]}" in plan, plan
                        elif not filters.get("lifecycle_states"):
                            _assert_index_plan(plan, kinds)
                        else:
                            # No no-TEMP promise applies to lifecycle filters.
                            # SQLite may scan the lone facts arm for its max.
                            assert any("facts" in line for line in plan.splitlines())
    finally:
        event.remove(provider.engine.sync_engine, "before_cursor_execute", trace)


@pytest.mark.asyncio
async def test_negative_drop_after_initialized_capability_fails_eqp_instead_of_fallback(seeded):
    provider, db = seeded
    assert provider._read_index_capable is True
    async with provider.engine.begin() as conn:
        for name in INDEXES.values():
            await conn.exec_driver_sql(f"DROP INDEX {name}")
    async with provider.engine.connect() as conn:
        assert await provider._check_read_index_capability(conn) is False

    captured = []

    def trace(conn, cursor, statement, params, context, many):
        captured.append((statement, params))

    event.listen(provider.engine.sync_engine, "before_cursor_execute", trace)
    try:
        await _page(provider, _query(200))
    finally:
        event.remove(provider.engine.sync_engine, "before_cursor_execute", trace)
    ordered = [(sql, params) for sql, params in captured if "ORDER BY" in sql.upper()]
    assert ordered
    plans = "\n".join("; ".join(_eqp_lines(sql, params, db)) for sql, params in ordered)
    assert provider._read_index_capable is True
    with pytest.raises(AssertionError):
        _assert_index_plan(plans, KINDS)


@pytest.mark.asyncio
async def test_one_noncanonical_table_disables_all_table_raw_path(tmp_path):
    db = tmp_path / "mixed.db"
    first = SQLiteProvider(f"sqlite+aiosqlite:///{db}")
    await first.initialize()
    await _seed_12k(first)
    await first.close()
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE beliefs SET created_at = replace(created_at, ' ', 'T') WHERE id='memory-0000'")
        conn.commit()
    provider = SQLiteProvider(f"sqlite+aiosqlite:///{db}")
    await provider.initialize()
    try:
        assert provider._read_index_capable is False
        captured = []

        def trace(conn, cursor, sql, params, context, many):
            if sql.startswith("SELECT") and "ORDER BY" in sql:
                captured.append((sql, params))

        event.listen(provider.engine.sync_engine, "before_cursor_execute", trace)
        page = await _page(provider, _query(1))
        event.remove(provider.engine.sync_engine, "before_cursor_execute", trace)
        assert page.records
        assert len(captured) == 2
        for sql, params in captured:
            assert sql.count("s07_timestamp(created_at) AS created_at") == 4
            assert "TEMP B-TREE" in "\n".join(_eqp_lines(sql, params, db))
        # A request for a canonical table must still use the global fallback.
        captured.clear()
        event.listen(provider.engine.sync_engine, "before_cursor_execute", trace)
        await _page(provider, _query(1, memory_types=["fact"]))
        event.remove(provider.engine.sync_engine, "before_cursor_execute", trace)
        assert all("s07_timestamp(created_at) AS created_at" in sql for sql, _ in captured)
    finally:
        await provider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [1, 200])
async def test_legacy_and_raw_keyset_filter_pages_are_identical_on_same_immutable_db(seeded, limit):
    raw, db = seeded
    legacy = REFERENCE_PROVIDER(f"sqlite+aiosqlite:///{db}")
    await legacy.initialize()
    try:
        assert raw._read_index_capable is True
        legacy._read_index_capable = False
        filters = (
            {},
            {"memory_types": ["fact", "belief"], "created_to": "2024-01-01T00:00:00.999999Z"},
            {"created_from": "2024-01-03T00:00:00Z", "created_to": "2024-01-03T00:00:00.999999+00:00"},
            {"updated_from": "2024-01-03T01:00:00+00:00", "updated_to": "2024-01-03T01:00:00.999999Z"},
            {"lifecycle_states": ["active"], "created_to": "2024-01-01T00:00:00.999999Z"},
        )
        for overrides in filters:
            # Legacy accepts normalized datetime filters; compare to genuine ISO
            # input on the raw path, including Z and offset serialization.
            old_overrides = {
                key: datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
                for key, value in overrides.items()
            }
            query = _query(limit, **old_overrides)
            raw_query = _query(limit, **overrides)
            left, right = [], []
            after_l = after_r = None
            upper_l = upper_r = None
            while True:
                old = await _page(legacy, query, after=after_l, upper=upper_l)
                new = await _page(raw, raw_query, after=after_r, upper=upper_r)
                assert old.upper == new.upper
                assert old.has_more == new.has_more
                assert _keys(old) == _keys(new)
                if EQUIVALENCE_TRACE is not None:
                    EQUIVALENCE_TRACE.append(
                        dict(
                            limit=limit,
                            filters=overrides,
                            after=after_l,
                            upper=old.upper,
                            has_more=old.has_more,
                            old_keys=_keys(old),
                            new_keys=_keys(new),
                        )
                    )
                left.extend(_keys(old))
                right.extend(_keys(new))
                if not old.has_more:
                    break
                after_l = _next_after(old)
                after_r = _next_after(new)
                upper_l = old.upper
                upper_r = new.upper
            assert left == right
            assert len(left) == len(right)
    finally:
        await legacy.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("field", ["created_at", "updated_at"])
async def test_legacy_mixed_offsets_preserve_global_order_and_subset_fallback(tmp_path, kind, field):
    db = tmp_path / "offset.db"
    initial = SQLiteProvider(f"sqlite+aiosqlite:///{db}")
    await initial.initialize()
    async with initial._session_factory() as session:
        for entry_kind in KINDS:
            session.add(_models(entry_kind, 0, BASE, BASE))
        await session.commit()
    await initial.close()
    with sqlite3.connect(db) as conn:
        conn.execute(f"UPDATE {kind}s SET {field}=?", ("2024-01-01T03:00:00+03:00",))
        conn.commit()
    provider = SQLiteProvider(f"sqlite+aiosqlite:///{db}")
    await provider.initialize()
    try:
        assert not provider._read_index_capable
        page = await _page(provider, _query(200))
        assert _keys(page) == tuple((SQLiteProvider._utc(BASE), k, "memory-0000") for k in sorted(KINDS))
        assert not page.has_more
        empty = await _page(provider, _query(200), after=page.upper, upper=page.upper)
        assert not empty.records
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_keyset_bounds_upper_empty_after_and_offset_iso_edges(seeded):
    provider, _ = seeded
    query = _query(200, created_from="2024-01-01T00:00:00Z", created_to="2024-01-01T00:00:00.000001+00:00")
    first = await _page(provider, query)
    # 67 timestamps per type in the inclusive 0..1us interval.
    assert len(first.records) == 200
    assert first.has_more is True
    expected_upper = ReadKey("2024-01-01T00:00:00.000001+00:00", "skill", "memory-0097")
    assert first.upper == expected_upper
    assert first.upper is not None
    empty = await _page(provider, query, after=first.upper, upper=first.upper)
    assert empty.records == () and empty.has_more is False
    before_upper = await _page(provider, _query(1), upper=first.upper)
    assert before_upper.upper == first.upper
    offset = ReadKey("2024-01-01T03:00:00.000001+03:00", "skill", "memory-0097")
    offset_page = await _page(provider, _query(1), upper=offset)
    assert _keys(offset_page) == _keys(before_upper)
    # Same id/timestamp across types must retain the BINARY type tie-breaker.
    after = ReadKey("2024-01-01T03:00:00+03:00", "belief", "memory-0000")
    cross_type = await _page(provider, _query(200), after=after, upper=offset)
    assert all(key > (SQLiteProvider._utc(after.created_at), after.memory_type, after.id) for key in _keys(cross_type))
    assert all(
        key <= (SQLiteProvider._utc(offset.created_at), offset.memory_type, offset.id) for key in _keys(cross_type)
    )
    iso_filter = await _page(
        provider, _query(200, created_from="2024-01-01T03:00:00+03:00", created_to="2024-01-01T03:00:00.000001+03:00")
    )
    assert _keys(iso_filter) == _keys(first)


@pytest.mark.asyncio
async def test_migration_upgrade_repeat_down_preserves_rows_fts_and_unique_index(tmp_path):
    db = tmp_path / "migration.db"
    provider = SQLiteProvider(f"sqlite+aiosqlite:///{db}")
    await provider.initialize()
    await _seed_12k(provider)
    await provider.close()
    with sqlite3.connect(db) as conn:
        for name in INDEXES.values():
            conn.execute(f"DROP INDEX {name}")
        conn.execute("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        conn.execute("INSERT INTO alembic_version VALUES ('0005')")
        conn.commit()
    ini = tmp_path / "alembic.ini"
    ini.write_text(
        (REPO / "alembic.ini")
        .read_text()
        .replace("%(here)s", str(REPO))
        .replace("sqlite:///memory.db", f"sqlite:///{db}")
    )

    command = pytest.importorskip("alembic.command", reason="Alembic is required for migration acceptance")
    from alembic.config import Config

    def run(*args):
        config = Config(str(ini))
        config.set_main_option("sqlalchemy.url", f"sqlite:///{db}")
        try:
            command.upgrade(config, args[-1]) if args[0] == "upgrade" else command.downgrade(config, args[-1])
            return SimpleNamespace(returncode=0, stderr="")
        except Exception as exc:  # Alembic exposes no stable result object.
            return SimpleNamespace(returncode=1, stderr=str(exc))

    with sqlite3.connect(db) as conn:
        before = conn.execute("SELECT count(*) FROM facts").fetchone()[0]
        before_fts = conn.execute("SELECT count(*) FROM facts_fts").fetchone()[0]
        before_unique = conn.execute("SELECT name FROM sqlite_master WHERE name='uq_facts_spo_active'").fetchall()
    upgraded = run("upgrade", "head")
    assert upgraded.returncode == 0, upgraded.stderr
    repeated = run("upgrade", "head")
    assert repeated.returncode == 0, repeated.stderr
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM facts").fetchone()[0] == before
        assert conn.execute("SELECT count(*) FROM facts_fts").fetchone()[0] == before_fts
        unique = conn.execute("SELECT name FROM sqlite_master WHERE name='uq_facts_spo_active'").fetchall()
        assert unique == before_unique
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone()[0] == "0006"
    downgraded = run("downgrade", "0005")
    assert downgraded.returncode == 0, downgraded.stderr
    with sqlite3.connect(db) as conn:
        assert not conn.execute("SELECT 1 FROM sqlite_master WHERE name='ix_facts_created_type_id'").fetchone()
        assert conn.execute("SELECT count(*) FROM facts").fetchone()[0] == before
        assert conn.execute("SELECT count(*) FROM facts_fts").fetchone()[0] == before_fts
        assert conn.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone() == ("0005",)
        assert not set(INDEXES.values()) & set(_all_index_sql(db))
        # Occupy the final name: preflight must refuse before creating any of
        # the first three missing indexes, regardless of transaction rollback.
        conn.execute("CREATE INDEX ix_skills_created_type_id ON skills(id)")
        conn.commit()
    attempted_ddl = []

    def trace_ddl(conn, cursor, sql, params, context, many):
        if sql.lstrip().upper().startswith("CREATE INDEX"):
            attempted_ddl.append(sql)

    from sqlalchemy.engine import Engine

    event.listen(Engine, "before_cursor_execute", trace_ddl)
    try:
        refused = run("upgrade", "head")
    finally:
        event.remove(Engine, "before_cursor_execute", trace_ddl)
    assert refused.returncode == 1 and "occupied" in refused.stderr
    assert attempted_ddl == []
    assert set(INDEXES.values()) & set(_all_index_sql(db)) == {INDEXES["skill"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("receipts", [False, True])
async def test_orm_write_after_startup_and_bounded_statement_counts(seeded, receipts):
    provider, _ = seeded
    async with provider._session_factory() as session:
        session.add(_models("fact", 9000, BASE - timedelta(days=1), BASE))
        await session.commit()
        if receipts:
            await session.execute(
                text(
                    "INSERT INTO receipts (id,memory_type,source,created_by,timestamp,confidence,"
                    "verification_status,lifecycle_state,version,history,updated_at) VALUES "
                    "('memory-9000','fact','slice01a','slice01a','2024-01-01 00:00:00.000000',"
                    "0.5,'candidate','active','1','[]','2024-01-01 00:00:00.000000')"
                )
            )
            await session.commit()
    statements = []

    def trace(conn, cursor, sql, params, context, many):
        if sql.startswith("SELECT"):
            statements.append(sql)

    event.listen(provider.engine.sync_engine, "before_cursor_execute", trace)
    try:
        first = await _page(provider, _query(1))
        assert first.records[0].memory["id"] == "memory-9000"
        assert (first.records[0].receipt is not None) == receipts
        assert len(statements) == 3
        statements.clear()
        await _page(provider, _query(1), after=_next_after(first), upper=first.upper)
        assert len(statements) == 2
        assert provider._read_index_capable
    finally:
        event.remove(provider.engine.sync_engine, "before_cursor_execute", trace)


@pytest.mark.asyncio
async def test_capability_check_is_startup_only_and_not_per_page(seeded):
    provider, _ = seeded
    checks = 0
    original = provider._check_read_index_capability

    async def counted(conn):
        nonlocal checks
        checks += 1
        return await original(conn)

    provider._check_read_index_capability = counted
    await _page(provider, _query(1))
    await _page(provider, _query(200))
    assert checks == 0
