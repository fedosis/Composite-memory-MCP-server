"""Private retirement filtering and all six native/MCP read handlers."""

import json
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, text
from storage.models.relation import ClaimRelationORM
from storage.repositories import BeliefRepository, FactRepository
from storage.repositories.relation_repo import _TYPES, RelationReadWork, RelationRepository

from memory_server.api.retrieval_filter import (
    filter_rank_results,
    filter_semantic_results,
    load_retired,
    rebuild_route_payload,
    rebuild_semantic_payload,
    retired_vector_ids,
)
from memory_server.api.search import search
from memory_server.projection_rebuild import deterministic_vector_id
from tests.test_invalidate import database as database
from tests.test_invalidate import seed


@dataclass
class Row:
    source: str
    metadata: dict


def test_graph_unknown_ids_and_rule_payload_preserved():
    rows = [Row("semantic", {"id": "dead"}), Row("graph", {"id": "dead"}),
            Row("semantic", {"id": "unknown"}), Row("fts", {})]
    assert filter_rank_results(rows, {"dead"}) == rows[1:]
    assert filter_rank_results(rows, {"dead"}, True) == rows
    for payload in ({"rule_match": {"route": "rules"}}, {"stage": 3, "graph_result": {}},
                    {"error": "unavailable"}):
        assert rebuild_route_payload(payload, {"dead"}) is payload
        assert rebuild_semantic_payload(payload, {"dead"}) is payload


@pytest.mark.parametrize("sources", [False, True])
def test_filtered_counts_sources_and_envelopes(sources):
    rows = [Row("semantic", {"vector_id": "dead"}), Row("graph", {"id": "dead"}),
            Row("fts", {"fact_id": "unknown"})]
    payload = dict(all_results=rows, ranked_results=["stale"], total=99,
                   stage=1, route="hybrid", rule_match={"name": "keep"}, extra=7)
    if sources:
        payload["sources"] = {"wrong": 99}
    result = rebuild_route_payload(payload, {"dead"})
    assert result["all_results"] == rows[1:]
    assert result["ranked_results"] == [r.__dict__ for r in rows[1:]]
    assert result["total"] == 2
    assert (result["stage"], result["route"], result["rule_match"], result["extra"]) == (
        1, "hybrid", {"name": "keep"}, 7)
    assert ("sources" in result) == sources
    if sources:
        assert result["sources"] == {"fts": 1, "semantic": 0, "graph": 1}
    semantic = dict(semantic_results=[{"id": "dead"}, {"id": "unknown"}, {}], total=99, route="semantic")
    assert rebuild_semantic_payload(semantic, {"dead"}) == dict(
        semantic_results=[{"id": "unknown"}, {}], total=2, route="semantic")
    assert filter_semantic_results(semantic["semantic_results"], {"dead"}, True) == semantic["semantic_results"]


@pytest.mark.asyncio
async def test_load_retired_uses_both_source_repository_methods(database, monkeypatch):
    active, retired = ("candidate", "validated", "active"), ("stale", "contradicted", "discarded", "superseded")
    for kind in ("fact", "belief"):
        for state in (*active, *retired):
            await seed(database, kind, f"{kind}-{state}", state)
    async with await database._get_session() as session:
        assert await FactRepository(session).non_active_ids() == {f"fact-{s}" for s in retired}
        assert await BeliefRepository(session).non_active_ids() == {f"belief-{s}" for s in retired}
    ids = {f"{kind}-{state}" for kind in ("fact", "belief") for state in retired}
    assert await load_retired(database) == retired_vector_ids(ids)
    assert deterministic_vector_id("fact", "fact-stale") in await load_retired(database)
    monkeypatch.setattr(BeliefRepository, "non_active_ids", AsyncMock(side_effect=RuntimeError("SQL failed")))
    with pytest.raises(RuntimeError, match="SQL failed"):
        await load_retired(database)


@pytest.mark.asyncio
async def test_private_successor_search_enrichment_without_public_expansion(database):
    await seed(database)
    await seed(database, ident="a")
    await seed(database, ident="z")
    async with await database._get_session() as session:
        for target in ("z", "a"):
            session.add(ClaimRelationORM(source_id="old", target_id=target, relation_type="superseded_by"))
        await session.commit()
        repo = RelationRepository(session)
        assert await repo._superseded_targets([]) == {}
        sizes = []

        def count(_conn, _cursor, sql, params, *_):
            if "claim_relations" in sql:
                sizes.append(len(params))

        connection = await session.connection()
        event.listen(connection.sync_connection, "before_cursor_execute", count)
        try:
            assert await repo._superseded_targets(["old"] + [f"x{i}" for i in range(260)]) == {"old": "a"}
        finally:
            event.remove(connection.sync_connection, "before_cursor_execute", count)
        assert len(sizes) == 3 and max(sizes) <= 129  # 128 IDs + literal type bind
        assert "superseded_by" not in _TYPES
        assert await repo.get_by_tuple("old", "a", "superseded_by", work=RelationReadWork()) is None
    payload = await search(database, include_inactive=True)
    assert payload["total"] == len(payload["results"])
    assert next(r for r in payload["results"] if r["id"] == "old")["superseded_by"] == "a"
    assert all("superseded_by" not in r for r in payload["results"] if r["id"] != "old")
    async with await database._get_session() as session:
        assert (await session.execute(text("SELECT COUNT(*) FROM relation_revisions"))).scalar_one() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("flag", [None, False, True])
@pytest.mark.parametrize("surface", ["native", "mcp"])
@pytest.mark.parametrize("tool", ["search", "semantic_search", "route"])
async def test_all_read_surfaces_active_only_and_include_inactive(
    database, monkeypatch, synthetic_storage_env, flag, surface, tool,
):
    from memory_server.plugins.hermes import provider as native
    from memory_server.router.embedding_router import EmbeddingRouter
    from memory_server.router.hybrid_router import HybridRouter
    from memory_server.transports import mcp_stdio as mcp

    for state in ("active", "validated", "candidate", "discarded", "superseded", "stale", "contradicted"):
        await seed(database, ident=state, state=state)
    ids = [deterministic_vector_id("fact", state) for state in
           ("active", "discarded", "superseded", "stale", "contradicted")]
    payload = dict(semantic_results=[{"id": i} for i in [*ids, "unknown"]], total=6, stage=2, route="semantic")
    ranked = [Row("semantic", {"id": i}) for i in ids] + [
        Row("graph", {"id": ids[-1]}), Row("semantic", {"id": "unknown"}),
    ]
    route_payload = dict(all_results=ranked, total=7, sources={}, stage=2, route="hybrid", extra=True)
    router_payload = route_payload if tool == "route" else payload
    monkeypatch.setattr(EmbeddingRouter, "route", AsyncMock(return_value=router_payload))
    monkeypatch.setattr(HybridRouter, "route", AsyncMock(return_value=router_payload))
    for symbol in ("_get_vector_provider", "_get_embedder", "_get_graph"):
        monkeypatch.setattr(native, symbol, AsyncMock(return_value=SimpleNamespace()))
    plugin = native.HermesProvider()
    monkeypatch.setattr(plugin, "_require_provider", lambda: database)
    monkeypatch.setattr(mcp, "_get_provider", AsyncMock(return_value=database))
    router = SimpleNamespace(route=AsyncMock(return_value=router_payload))
    monkeypatch.setattr(mcp, "_get_router", AsyncMock(return_value=router))
    monkeypatch.setattr(mcp, "_get_hybrid_router", AsyncMock(return_value=router))
    runtime = SimpleNamespace(admit=lambda: None, release_admitted=lambda token: None)
    monkeypatch.setattr(mcp, "_runtime_or_raise", lambda: SimpleNamespace(runtime=runtime))
    if flag is True and tool != "search":
        # Opt-in must not load retirement SQL at all.
        monkeypatch.setattr(mcp, "load_retired", AsyncMock(side_effect=AssertionError("loaded")))
        monkeypatch.setattr(
            "memory_server.api.retrieval_filter.load_retired", AsyncMock(side_effect=AssertionError("loaded")),
        )
    handler = plugin._get_tool_handler(tool) if surface == "native" else getattr(mcp, f"{tool}_tool")
    kwargs = {} if flag is None else {"include_inactive": flag}
    result = json.loads(await handler(**kwargs))
    if tool == "search":
        assert result["total"] == (7 if flag is True else 3)
        assert {r["id"] for r in result["results"]} == (
            {"active", "validated", "candidate", "discarded", "superseded", "stale", "contradicted"}
            if flag is True else {"active", "validated", "candidate"})
    elif tool == "semantic_search":
        assert result["total"] == (6 if flag is True else 2)
        assert result["semantic_results"][-1] == {"id": "unknown"}
    else:
        assert result["total"] == (7 if flag is True else 3)
        assert result["stage"] == 2 and result["extra"] is True
        assert result["sources"] == {"fts": 0, "semantic": 6 if flag is True else 2, "graph": 1}
        assert len(result["ranked_results"]) == len(result["all_results"]) == result["total"]
    schemas = {s["name"]: s for s in plugin.get_tool_schemas()}
    assert schemas[tool]["parameters"]["properties"]["include_inactive"] == {"type": "boolean", "default": False}
    mcp_schemas = {t.name: t.inputSchema for t in await mcp.mcp.list_tools()}
    assert mcp_schemas[tool]["properties"]["include_inactive"]["default"] is False
