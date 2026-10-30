"""S10A observational canonical/projection foundation tests (no HTTP or repairs)."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
from datetime import datetime

from sqlalchemy import text

from memory_server.providers.sqlite_provider import SQLiteProvider


async def _provider(tmp_path):
    provider = SQLiteProvider("sqlite+aiosqlite:///" + str(tmp_path / "drift.sqlite"))
    await provider.initialize()
    return provider


def _api():
    module = importlib.import_module("memory_server.providers.sqlite_provider")
    required = {"DiagnosticsReadWork", "AuditSnapshot", "BeliefWindow"}
    assert required <= set(vars(module)), required - set(vars(module))
    for method in (
        "diagnostics_window",
        "diagnostics_count",
        "diagnostics_lifecycle_counts",
        "diagnostics_check_count",
        "diagnostics_audit_snapshot",
        "diagnostics_unresolved_conflict_count",
        "diagnostics_reflect_beliefs",
        "diagnostics_schema_revision",
    ):
        assert hasattr(SQLiteProvider, method), method
    return module


async def _hash_tables(provider):
    async with provider._session_factory() as session:
        values = []
        for table in ("facts", "beliefs", "decisions", "skills", "receipts", "claim_relations", "outbox_entries"):
            result = await session.execute(text(f"SELECT * FROM {table} ORDER BY 1"))
            values.append((table, tuple(tuple(row) for row in result)))
    return hashlib.sha256(repr(values).encode()).hexdigest()


async def test_foundation_counts_lifecycle_snapshot_and_no_write(tmp_path):
    module = _api()
    provider = await _provider(tmp_path)
    try:
        async with provider._session_factory() as session:
            async with session.begin():
                await session.execute(
                    text(
                        "INSERT INTO beliefs(id,proposition,confidence,source,creator,source_ids,tags,"
                        "created_at,updated_at,"
                        "last_reinforced_at,version,verification_status,lifecycle_state) VALUES "
                        "(:a,'secret a',.8,'s','c','[]','[]',:at,:at,:at,1,'candidate','contradicted'),"
                        "(:b,'secret b',.2,'s','c','[]','[]',:at,:at,:at,1,'trusted','contradicted'),"
                        "(:c,'secret c',1.2,'s','c','[]','[]',:at,:at,:at,1,'candidate','trusted')"
                    ),
                    {
                        "a": "00000000-0000-4000-8000-000000000001",
                        "b": "00000000-0000-4000-8000-000000000002",
                        "c": "00000000-0000-4000-8000-000000000003",
                        "at": datetime(2026, 10, 30),
                    },
                )
                await session.execute(
                    text(
                        "INSERT INTO claim_relations(source_id,target_id,relation_type,created_at) VALUES "
                        "(:a,:b,'contradicts',:at),(:b,:a,'contradicts',:at),"
                        "(:a,:a,'contradicts',:at)"
                    ),
                    {
                        "a": "00000000-0000-4000-8000-000000000001",
                        "b": "00000000-0000-4000-8000-000000000002",
                        "at": datetime(2026, 10, 30),
                    },
                )
        before = await _hash_tables(provider)
        async with provider._session_factory() as session:
            now = asyncio.get_running_loop().time()
            work = module.DiagnosticsReadWork(soft_deadline=now + 10, hard_deadline=now + 10)
            async with provider.diagnostics_window(session, work=work):
                await session.execute(text("BEGIN"))
                assert await provider.diagnostics_count(session, "beliefs", work=work) == 3
                lifecycle = await provider.diagnostics_lifecycle_counts(session, "beliefs", work=work)
                assert lifecycle == {"contradicted": 2, "unknown": 1}
                assert (
                    await provider.diagnostics_check_count(session, "invalid_lifecycle", table="beliefs", work=work)
                    == 1
                )
                assert await provider.diagnostics_check_count(session, "confidence", table="beliefs", work=work) == 1
                assert await provider.diagnostics_unresolved_conflict_count(session, work=work) == 1
                snapshot = await provider.diagnostics_audit_snapshot(session, per_type_limit=2, work=work)
                assert snapshot.truncated and len(snapshot.records) <= 8
                beliefs = await provider.diagnostics_reflect_beliefs(
                    session, topic=None, min_confidence=0.0, limit=2, work=work
                )
                assert beliefs.truncated and len(beliefs.rows) == 2
                assert await provider.diagnostics_schema_revision(session, work=work) is None
        assert await _hash_tables(provider) == before
    finally:
        await provider.close()


async def test_projection_unknown_is_not_invented_zero(tmp_path):
    """An absent production graph resource yields an unavailable projection."""
    module = _api()
    graph = importlib.import_module("memory_server.application.graph_read_service")
    provider = await _provider(tmp_path)
    try:
        async with provider._session_factory() as session:
            now = asyncio.get_running_loop().time()
            work = module.DiagnosticsReadWork(soft_deadline=now + 10, hard_deadline=now + 10)
            async with provider.diagnostics_window(session, work=work):
                counts = {
                    table: await provider.diagnostics_count(session, table, work=work)
                    for table in ("facts", "beliefs", "decisions", "skills")
                }
        observation = {
            "canonical": counts,
            "vector": None,
            "graph": graph.map_consistency("unavailable").projection,
        }
        assert observation["vector"] is None and observation["graph"] == "unavailable"
        assert observation["canonical"] == {"facts": 0, "beliefs": 0, "decisions": 0, "skills": 0}
    finally:
        await provider.close()
