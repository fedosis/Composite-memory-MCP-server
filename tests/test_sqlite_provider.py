"""Tests for SQLite provider (Card 003)."""

import ast
import os
import re
import sqlite3
from collections.abc import Mapping
from pathlib import Path

import pytest
from sqlalchemy import event
from storage.adapters.legacy_provider import LegacySQLiteProviderAdapter
from storage.dedup import fact_dedup_key
from storage.outbox_worker import OutboxWorker

import memory_server.providers.sqlite_provider as sqlite_provider_module
from memory_server import profile_migration
from memory_server.models import Fact as DomainFact
from memory_server.models import MemoryReceipt, VerificationStatus
from memory_server.providers.sqlite_provider import SQLiteProvider


def make_fact(**kwargs):
    return DomainFact(
        **kwargs,
        dedup_key=fact_dedup_key(kwargs["subject"], kwargs["predicate"], kwargs["object"]),
    )


@pytest.fixture
async def provider():
    """Create an in-memory SQLite provider for testing."""
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


@pytest.mark.asyncio
class TestFactCRUD:
    async def test_create_fact_without_key_persists_computed_key(self, tmp_path):
        url = f"sqlite+aiosqlite:///{tmp_path / 'facts.db'}"
        first = SQLiteProvider(url=url)
        await first.initialize()
        fact = DomainFact(id="no-key", subject="Docker", predicate="runs_on", object="OMV8")
        created = await first.create_fact(fact)
        await first.close()

        second = SQLiteProvider(url=url)
        await second.initialize()
        retrieved = await second.get_fact("no-key")
        await second.close()

        expected = fact_dedup_key("Docker", "runs_on", "OMV8")
        assert created.dedup_key == expected
        assert retrieved is not None
        assert retrieved.dedup_key == expected

    async def test_create_fact(self, provider):
        f = make_fact(id="f1", subject="Docker", predicate="runs_on", object="OMV8")
        created = await provider.create_fact(f)
        assert created.id == "f1"
        assert created.subject == "Docker"

    async def test_get_fact(self, provider):
        f = make_fact(id="f2", subject="Test", predicate="is", object="Working")
        await provider.create_fact(f)
        retrieved = await provider.get_fact("f2")
        assert retrieved is not None
        assert retrieved.subject == "Test"
        assert retrieved.predicate == "is"
        assert retrieved.object == "Working"

    async def test_get_fact_not_found(self, provider):
        result = await provider.get_fact("nonexistent")
        assert result is None

    async def test_search_facts_by_subject(self, provider):
        await provider.create_fact(
            make_fact(id="f3", subject="Docker", predicate="uses", object="Port 8080")
        )
        await provider.create_fact(
            make_fact(id="f4", subject="Nginx", predicate="uses", object="Port 80")
        )
        results = await provider.search_facts(subject="Docker")
        assert len(results) == 1
        assert results[0].id == "f3"

    async def test_search_facts_by_predicate(self, provider):
        await provider.create_fact(make_fact(id="f5", subject="A", predicate="runs_on", object="X"))
        await provider.create_fact(make_fact(id="f6", subject="B", predicate="depends_on", object="Y"))
        results = await provider.search_facts(predicate="runs_on")
        assert len(results) == 1
        assert results[0].id == "f5"

    async def test_search_facts_by_object(self, provider):
        await provider.create_fact(make_fact(id="f7", subject="S1", predicate="has", object="Target"))
        results = await provider.search_facts(object="Target")
        assert len(results) == 1

    async def test_search_facts_by_source(self, provider):
        await provider.create_fact(
            make_fact(id="f8", subject="X", predicate="is", object="Y", source="manual")
        )
        await provider.create_fact(
            make_fact(id="f9", subject="X", predicate="is", object="Z", source="auto")
        )
        results = await provider.search_facts(source="manual")
        assert len(results) == 1

    async def test_search_facts_text_search(self, provider):
        await provider.create_fact(
            make_fact(id="f10", subject="Docker", predicate="is", object="Container")
        )
        await provider.create_fact(
            make_fact(id="f11", subject="Caddy", predicate="is", object="Web Server")
        )
        results = await provider.search_facts(text="Docker")
        assert len(results) == 1

    async def test_search_facts_empty_results(self, provider):
        results = await provider.search_facts(subject="DoesNotExist")
        assert results == []

    async def test_search_facts_excludes_inactive_by_default(self, provider):
        active = await provider.create_fact(
            make_fact(id="f-inactive-1", subject="Active", predicate="is", object="Visible")
        )
        inactive = await provider.create_fact(
            make_fact(id="f-inactive-2", subject="Old", predicate="is", object="Hidden")
        )
        await provider.update_fact(inactive.id, lifecycle_state="superseded")

        default_results = await provider.search_facts(limit=10)
        assert [fact.id for fact in default_results] == [active.id]

        all_results = await provider.search_facts(limit=10, include_inactive=True)
        assert {fact.id for fact in all_results} == {active.id, inactive.id}

    async def test_update_fact(self, provider):
        f = make_fact(id="f12", subject="Old", predicate="is", object="Value")
        await provider.create_fact(f)
        updated = await provider.update_fact("f12", object="NewValue")
        assert updated is not None
        assert updated.object == "NewValue"
        # Verify persisted
        retrieved = await provider.get_fact("f12")
        assert retrieved.object == "NewValue"

    async def test_update_fact_not_found(self, provider):
        result = await provider.update_fact("nonexistent", object="value")
        assert result is None

    async def test_delete_fact(self, provider):
        f = make_fact(id="f13", subject="Temp", predicate="is", object="Removed")
        await provider.create_fact(f)
        result = await provider.delete_fact("f13")
        assert result is True
        retrieved = await provider.get_fact("f13")
        assert retrieved is None

    async def test_delete_fact_not_found(self, provider):
        result = await provider.delete_fact("nonexistent")
        assert result is False


@pytest.mark.asyncio
class TestProviderInitialization:
    async def test_initialize_skips_facts_fts_rebuild_when_index_already_populated(self, tmp_path, monkeypatch):
        db_url = f"sqlite+aiosqlite:///{tmp_path / 'provider.db'}"

        provider = SQLiteProvider(url=db_url)
        await provider.initialize()
        await provider.create_fact(
            make_fact(id="fts-existing", subject="Docker", predicate="runs_on", object="OMV")
        )
        await provider.close()

        executed_sql: list[str] = []
        original_create_async_engine = sqlite_provider_module.create_async_engine

        def instrumented_create_async_engine(*args, **kwargs):
            engine = original_create_async_engine(*args, **kwargs)

            def capture_sql(conn, cursor, statement, parameters, context, executemany):
                executed_sql.append(statement)

            event.listen(engine.sync_engine, "before_cursor_execute", capture_sql)
            return engine

        monkeypatch.setattr(
            sqlite_provider_module,
            "create_async_engine",
            instrumented_create_async_engine,
        )

        provider = SQLiteProvider(url=db_url)
        await provider.initialize()
        results = await provider.search_facts(text="Docker")
        await provider.close()

        assert [fact.id for fact in results] == ["fts-existing"]
        assert not any(
            "facts_fts" in statement and "'rebuild'" in statement
            for statement in executed_sql
        )

    async def test_initialize_rebuilds_facts_fts_when_index_is_missing(self, tmp_path):
        db_url = f"sqlite+aiosqlite:///{tmp_path / 'provider.db'}"

        provider = SQLiteProvider(url=db_url)
        await provider.initialize()
        await provider.create_fact(
            make_fact(id="fts-rebuild", subject="Docker", predicate="runs_on", object="OMV")
        )

        engine = provider.engine
        assert engine is not None
        async with engine.begin() as conn:
            await conn.exec_driver_sql("DROP TRIGGER IF EXISTS facts_ai")
            await conn.exec_driver_sql("DROP TRIGGER IF EXISTS facts_ad")
            await conn.exec_driver_sql("DROP TRIGGER IF EXISTS facts_au")
            await conn.exec_driver_sql("DROP TABLE IF EXISTS facts_fts")

        await provider.close()

        provider = SQLiteProvider(url=db_url)
        await provider.initialize()
        results = await provider.search_facts(text="Docker")

        engine = provider.engine
        assert engine is not None
        async with engine.connect() as conn:
            facts_fts_count = await conn.exec_driver_sql("SELECT count(*) FROM facts_fts")
            count = facts_fts_count.scalar_one()

        await provider.close()

        assert [fact.id for fact in results] == ["fts-rebuild"]
        assert count == 1


@pytest.mark.asyncio
class TestFileBackedJournalMode:
    async def test_file_backed_connections_have_wal_and_busy_timeout(self, tmp_path):
        db_url = f"sqlite+aiosqlite:///{tmp_path / 'journal-policy.db'}"

        provider = SQLiteProvider(url=db_url)
        await provider.initialize()
        try:
            engine = provider.engine
            assert engine is not None
            async with engine.connect() as conn:
                journal_mode = (await conn.exec_driver_sql("PRAGMA journal_mode")).scalar_one()
                busy_timeout = (await conn.exec_driver_sql("PRAGMA busy_timeout")).scalar_one()
            assert str(journal_mode).lower() == "wal"
            assert busy_timeout == provider._busy_timeout_ms
        finally:
            await provider.close()

        adapter = LegacySQLiteProviderAdapter(url=db_url)
        await adapter.initialize()
        try:
            engine = adapter._engine
            assert engine is not None
            async with engine.connect() as conn:
                journal_mode = (await conn.exec_driver_sql("PRAGMA journal_mode")).scalar_one()
                busy_timeout = (await conn.exec_driver_sql("PRAGMA busy_timeout")).scalar_one()
            assert str(journal_mode).lower() == "wal"
            assert busy_timeout == 5000
        finally:
            await adapter.close()

        provider_for_worker = SQLiteProvider(url=db_url)
        await provider_for_worker.initialize()
        worker = OutboxWorker(engine=provider_for_worker.engine, db_url=db_url)
        await worker.initialize()
        try:
            engine = worker._engine
            assert engine is not None
            async with engine.connect() as conn:
                journal_mode = (await conn.exec_driver_sql("PRAGMA journal_mode")).scalar_one()
                busy_timeout = (await conn.exec_driver_sql("PRAGMA busy_timeout")).scalar_one()
            assert str(journal_mode).lower() == "wal"
            assert busy_timeout == worker._busy_timeout_ms
        finally:
            await worker.close()
            await provider_for_worker.close()


@pytest.mark.asyncio
class TestReceiptCRUD:
    async def test_create_receipt(self, provider):
        from datetime import datetime, timezone

        r = MemoryReceipt(
            id="r1",
            memory_type="fact",
            source="agent1",
            created_by="test",
            timestamp=datetime.now(timezone.utc),
        )
        created = await provider.create_receipt(r)
        assert created.id == "r1"
        assert created.memory_type == "fact"

    async def test_get_receipt(self, provider):
        from datetime import datetime, timezone

        r = MemoryReceipt(
            id="r2",
            memory_type="decision",
            source="user",
            created_by="alice",
            timestamp=datetime.now(timezone.utc),
            confidence=0.8,
            verification_status=VerificationStatus.CANDIDATE,
        )
        await provider.create_receipt(r)
        retrieved = await provider.get_receipt("r2")
        assert retrieved is not None
        assert retrieved.source == "user"
        assert retrieved.verification_status == VerificationStatus.CANDIDATE

    async def test_get_receipt_not_found(self, provider):
        result = await provider.get_receipt("nonexistent")
        assert result is None

    async def test_search_receipts_by_source(self, provider):
        from datetime import datetime, timezone

        await provider.create_receipt(
            MemoryReceipt(
                id="r3", memory_type="fact", source="test-src",
                created_by="u1", timestamp=datetime.now(timezone.utc),
            )
        )
        await provider.create_receipt(
            MemoryReceipt(
                id="r4", memory_type="fact", source="other-src",
                created_by="u2", timestamp=datetime.now(timezone.utc),
            )
        )
        results = await provider.search_receipts(source="test-src")
        assert len(results) == 1
        assert results[0].id == "r3"

    async def test_search_receipts_by_memory_type(self, provider):
        from datetime import datetime, timezone

        await provider.create_receipt(
            MemoryReceipt(
                id="r5", memory_type="fact", source="s1",
                created_by="u1", timestamp=datetime.now(timezone.utc),
            )
        )
        await provider.create_receipt(
            MemoryReceipt(
                id="r6", memory_type="skill", source="s1",
                created_by="u1", timestamp=datetime.now(timezone.utc),
            )
        )
        results = await provider.search_receipts(memory_type="fact")
        assert len(results) == 1
        assert results[0].id == "r5"


# ---------------------------------------------------------------------------
# S2-03 -- the qualified source inspection never goes through SQLiteProvider
#
# DETAIL 7.3: ``SQLiteProvider``'s normal ``initialize`` is NOT usable for
# source inspection; the source is inspected through the exact bounded
# write-lock probe plus the immutable read-only snapshot path only. These nodes
# run against a provider-WRITTEN, cleanly closed database (a real
# runtime-shaped source: WAL-mode header with the sidecars removed by the last
# clean close) and against a synthetic sidecar-free source.
#
# CLASSIFICATION OF THE FILED PRE-FIX RED (S2-03 fix round, review F2): the
# accessors are shape-tolerant and return ``({}, [])`` on a module that lacks the
# S2-03 API, so the filed RED failures here are SENTINEL missing-capability
# results, never behavioural proof. Labelled in
# S2-03_EVIDENCE/S2-03_FIX1_RED_RELABEL.md.
# ---------------------------------------------------------------------------


def _s203_tree_head_revision() -> str:
    """This tree's real Alembic head, from BOTH configured version locations.

    Review F1: the S2-03 literal mirrored the production constant
    (``7a1b2c3d4e5f``), which is an INTERIOR node of the merged Alembic DAG, so
    no node in the card could detect that the real head is ``0005``. Deriving it
    here from ``alembic.ini``'s two ``version_locations`` keeps this file honest
    without importing alembic (a dev-only extra).
    """
    tree_root = Path(profile_migration.__file__).resolve().parents[2]
    ini_text = (tree_root / "alembic.ini").read_text(encoding="utf-8")
    match = re.search(r"^version_locations\s*=\s*(.+)$", ini_text, re.MULTILINE)
    assert match is not None, "alembic.ini has no version_locations"
    revisions: dict = {}
    for location in match.group(1).strip().split(os.pathsep):
        for revision_file in sorted(Path(location.replace("%(here)s", str(tree_root))).glob("*.py")):
            found: dict = {}
            for node in ast.parse(revision_file.read_text(encoding="utf-8")).body:
                names = []
                if isinstance(node, ast.Assign):
                    names = [(t.id, node.value) for t in node.targets if isinstance(t, ast.Name)]
                elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                    names = [(node.target.id, node.value)]
                for name, value in names:
                    if name in {"revision", "down_revision"} and value is not None:
                        found[name] = ast.literal_eval(value)
            assert isinstance(found.get("revision"), str), revision_file
            revisions[found["revision"]] = found.get("down_revision")
    parents = set()
    for down in revisions.values():
        if isinstance(down, str):
            parents.add(down)
        elif isinstance(down, (tuple, list)):
            parents.update(item for item in down if isinstance(item, str))
    heads = set(revisions) - parents
    assert len(heads) == 1, heads
    return next(iter(heads))


S203_HEAD_REVISION = _s203_tree_head_revision()


def _s203_get(report, *path):
    """Nested lookup yielding ``{}`` for anything the module does not report."""
    current = report
    for key in path:
        if not isinstance(current, Mapping) or key not in current:
            return {}
        current = current[key]
    return current


def _s203_call(name, *args, **kwargs):
    """Call an S2-03 entrypoint, or ``({}, [])`` on a module that lacks it."""
    api = getattr(profile_migration, name, None)
    if api is None:
        return {}, []
    report, diagnostics = api(*args, **kwargs)
    return dict(report), list(diagnostics)


def _s203_codes(diagnostics):
    return [str(getattr(item, "code", "")) for item in diagnostics]


def _s203_seed_head_source(path: Path, *, revision: str = S203_HEAD_REVISION) -> Path:
    """A sidecar-free synthetic source carrying the snapshot's target schema."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute("create table alembic_version(version_num text)")
        connection.execute("insert into alembic_version values(?)", (revision,))
        connection.execute("create table facts(id text, subject text)")
        connection.execute("insert into facts values('fact-1','subject-1')")
        connection.execute("create table outbox_entries(id text, status text)")
        connection.execute("insert into outbox_entries values('outbox-1','pending')")
        connection.commit()
    finally:
        connection.close()
    return path


def _s203_directory_bytes(root: Path) -> dict:
    """Name -> (mode, bytes) for every regular file under *root*."""
    return {
        path.name: (path.stat().st_mode, path.read_bytes())
        for path in sorted(root.iterdir())
        if path.is_file()
    }


@pytest.mark.asyncio
class TestQualifiedSourceInspection:
    async def test_provider_written_wal_source_is_qualified_without_a_provider_reopen(
        self, tmp_path
    ):
        """A real provider-written, cleanly closed source is qualified end to end."""
        db_path = tmp_path / "provider-wal.db"
        provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{db_path}")
        await provider.initialize()
        await provider.create_fact(
            make_fact(id="wal-1", subject="Docker", predicate="runs_on", object="OMV")
        )
        await provider.close()

        # A clean close leaves a WAL-mode image with the sidecars removed.
        assert db_path.read_bytes()[18] == 2
        assert not (tmp_path / "provider-wal.db-wal").exists()
        assert not (tmp_path / "provider-wal.db-shm").exists()
        before = _s203_directory_bytes(tmp_path)

        report, diagnostics = _s203_call(
            "qualify_sqlite_source", db_path, run_dir=tmp_path / "run"
        )

        codes = _s203_codes(diagnostics)
        assert _s203_get(report, "probe", "policy") == "sidecars_absent_writable_probe"
        assert _s203_get(report, "probe", "performed") is True
        assert _s203_get(report, "probe", "rolled_back") is True
        assert _s203_get(report, "probe", "qualified") is True
        assert _s203_get(report, "probe", "journal_mode_header") == "wal"
        assert _s203_get(report, "snapshot", "created") is True
        assert _s203_get(report, "snapshot", "api") == "sqlite3.Connection.backup"
        assert _s203_get(report, "snapshot", "verification", "integrity") == "ok"
        assert _s203_get(report, "snapshot", "verification", "ids", "facts", "count") == 1
        # The provider's own schema carries no alembic_version marker.
        assert "E_SQLITE_SCHEMA" in codes
        assert _s203_get(report, "snapshot", "verification", "alembic_revision") is None
        # Probing a WAL-mode source created no WAL/SHM and did not touch it.
        assert _s203_directory_bytes(tmp_path) == before

    async def test_qualification_never_constructs_the_sqlite_provider(self, tmp_path, monkeypatch):
        """Source inspection is stdlib sqlite3 only; the provider is never built."""
        db_path = _s203_seed_head_source(tmp_path / "synthetic" / "memory.db")

        def _refuse(*args, **kwargs):
            raise AssertionError("SQLiteProvider must not be used for source inspection")

        monkeypatch.setattr(SQLiteProvider, "__init__", _refuse)
        monkeypatch.setattr(SQLiteProvider, "initialize", _refuse)

        report, diagnostics = _s203_call(
            "qualify_sqlite_source", db_path, run_dir=tmp_path / "run"
        )

        assert diagnostics == []
        assert _s203_get(report, "probe", "qualified") is True
        assert _s203_get(report, "snapshot", "created") is True
        assert _s203_get(report, "snapshot", "verification", "integrity") == "ok"
        assert (
            _s203_get(report, "snapshot", "verification", "alembic_revision")
            == S203_HEAD_REVISION
        )
