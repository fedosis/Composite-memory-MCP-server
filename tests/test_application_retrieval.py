"""C07 paired baseline/candidate tests; base imports remain available at collection."""
from __future__ import annotations

import ast
import hashlib
import json
import math
import os
import subprocess
import sys
from dataclasses import FrozenInstanceError, fields
from datetime import datetime, timezone
from pathlib import Path

import pytest

from memory_server.api.get_context import get_context
from memory_server.api.search import search
from memory_server.models import Decision, Fact
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.router.graph_router import GraphRouter

SUBJECT = os.environ["C07_SUBJECT"]
SCRATCH = Path(os.environ["C07_SCRATCH"]).resolve()
ORACLE = SCRATCH / "evidence" / "oracle.json"
ORACLE_HASH = SCRATCH / "evidence" / "oracle.sha256"
STAMP = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _policy():
    from memory_server.application.contracts import RetrievalPolicy
    return RetrievalPolicy(50, 10, 4, 10, 0.0)


def _graph_policy(depth=4):
    from memory_server.application.contracts import GraphPolicy
    return GraphPolicy(depth)


def _graph():
    graph = SimpleGraph()
    for ident in ("alpha", "beta", "gamma", "delta", "omega"):
        graph.add_node(id=ident, type="entity", name=ident.title(), attributes={"tag": ident})
    for source, target, relation in (
        ("alpha", "beta", "links"), ("alpha", "gamma", "links"),
        ("beta", "delta", "uses"), ("gamma", "delta", "uses"),
        ("delta", "alpha", "cycles"),
    ):
        graph.add_edge(source_id=source, target_id=target, relation=relation,
                       attributes={"tag": source + target})
    return GraphRouter(graph, max_path_depth=4)


async def _sqlite(tmp_path):
    assert tmp_path.resolve().is_relative_to(SCRATCH)
    db = tmp_path / "memory.db"
    provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{db}")
    await provider.initialize()
    return provider


async def _seed(provider):
    facts = (
        ("f1", "Docker", "runs_on", "OMV8", 0.9, "active"),
        ("f2", "Docker", "uses", "Caddy", 0.8, "active"),
        ("f3", "Caddy", "uses", "Port 443", 0.8, "active"),
        ("f4", "Nginx", "is", "Reverse proxy", 0.7, "active"),
        ("f5", "Docker", "used", "OldHost", 0.95, "archived"),
    )
    for ident, subject, predicate, obj, confidence, state in facts:
        await provider.create_fact(Fact(id=ident, subject=subject, predicate=predicate,
                                        object=obj, confidence=confidence, lifecycle_state=state,
                                        created_at=STAMP, updated_at=STAMP))
    for i in range(4):
        await provider.create_decision(Decision(
            id=f"d{i}", context="Topic", choice=f"Choice{i}", reason="Topic plan",
            lifecycle_state="archived" if i else "active",
            created_at=STAMP.replace(day=i + 1), updated_at=STAMP.replace(day=i + 1)))


async def _context_subject(provider, *, max_results=1, overfetch=4):
    if SUBJECT == "legacy":
        return lambda: get_context(provider, task="Topic", max_results=max_results)
    from memory_server.application.contracts import ContextCommand, RetrievalPolicy
    from memory_server.application.retrieval_service import RetrievalService
    svc = RetrievalService(provider, policy=RetrievalPolicy(50, max_results, overfetch, 10, 0.0))
    return lambda: svc.get_context(ContextCommand("Topic"))


async def _graph_subject(router, *, depth=4):
    if SUBJECT == "legacy":
        import memory_server.server as server
        return lambda **kwargs: server.graph_search_fn(**kwargs)
    from memory_server.application.contracts import GraphSearchCommand
    from memory_server.application.graph_service import GraphService
    async def accessor():
        return router
    svc = GraphService(accessor, policy=_graph_policy(depth))
    return lambda **kwargs: svc.search(GraphSearchCommand(**kwargs))


async def _legacy_graph(router, monkeypatch):
    import memory_server.server as server
    async def accessor():
        return router
    monkeypatch.setattr(server, "_get_graph_router", accessor)
    return server


@pytest.mark.asyncio
async def test_context_policy_is_frozen_after_construction(tmp_path, monkeypatch):
    import memory_server.settings as settings
    monkeypatch.setenv("MEMORY_SERVER_CONTEXT_DEDUP_OVERFETCH_FACTOR", "4")
    settings.get_settings.cache_clear()
    provider = await _sqlite(tmp_path)
    try:
        await _seed(provider)
        bound = await _context_subject(provider)
        monkeypatch.setenv("MEMORY_SERVER_CONTEXT_DEDUP_OVERFETCH_FACTOR", "1")
        settings.get_settings.cache_clear()
        real = settings.get_settings
        reads = []
        monkeypatch.setattr(settings, "get_settings", lambda: (reads.append(1), real())[1])
        result = await bound()
        observed = ([d["id"] for d in result["decisions"]], reads)
        assert observed == (["d0"], []), f"context observation: {observed!r}"
    finally:
        await provider.close()
        real.cache_clear()


@pytest.mark.asyncio
async def test_graph_path_depth_is_frozen_after_construction(monkeypatch):
    import memory_server.settings as settings
    monkeypatch.setenv("MEMORY_SERVER_GRAPH_MAX_PATH_DEPTH", "4")
    settings.get_settings.cache_clear()
    router = _graph()
    if SUBJECT == "legacy":
        server = await _legacy_graph(router, monkeypatch)
    bound = await _graph_subject(router)
    monkeypatch.setenv("MEMORY_SERVER_GRAPH_MAX_PATH_DEPTH", "1")
    settings.get_settings.cache_clear()
    reads = []
    real = settings.get_settings
    if SUBJECT == "legacy":
        monkeypatch.setattr(server, "get_settings", lambda: (reads.append(1), real())[1])
    else:
        monkeypatch.setattr(settings, "get_settings", lambda: (reads.append(1), real())[1])
    try:
        output = await bound(source_id="alpha", target_id="delta")
        result = json.loads(output) if isinstance(output, str) else output
        observed = ([[n["id"] for n in p] for p in result["paths"]], reads)
        assert observed == ([['alpha', 'beta', 'delta'], ['alpha', 'gamma', 'delta']], []), observed
    finally:
        real.cache_clear()


def test_graph_recipe_invocation_is_transport_free(tmp_path):
    script = '''
import builtins, json, sys
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.router.graph_router import GraphRouter
attempts=[]
old=builtins.__import__
def audit(name,*args,**kwargs):
    if (name == 'mcp' or name.startswith('mcp.') or name == 'typer' or
            name.startswith('typer.') or name == 'memory_server.server' or
            name.startswith(('memory_server.transports','memory_server.plugins','memory_server.runtime'))):
        attempts.append(name)
    return old(name,*args,**kwargs)
builtins.__import__=audit
router=GraphRouter(SimpleGraph())
router.graph.add_node(id='alpha',type='entity',name='Alpha')
async def accessor(): return router
import asyncio
if sys.argv[1]=='legacy':
    import memory_server.server as server
    server._get_graph_router=accessor
    async def run(): return json.loads(await server.graph_search_fn(entity_id='alpha'))
else:
    from memory_server.application.graph_service import GraphService
    from memory_server.application.contracts import GraphSearchCommand, GraphPolicy
    svc=GraphService(accessor, policy=GraphPolicy(4))
    async def run(): return await svc.search(GraphSearchCommand(entity_id='alpha'))
result=asyncio.run(run())
assert result['nodes'][0]['name']=='Alpha', result
assert attempts==[], f'forbidden import attempts after real graph query: {attempts!r}'
'''
    target = tmp_path / "probe.py"
    target.write_text(script)
    run = subprocess.run([sys.executable, str(target), SUBJECT], cwd=tmp_path,
                         env=os.environ.copy(), text=True, capture_output=True)
    assert run.returncode == 0, run.stdout + run.stderr


@pytest.mark.asyncio
async def test_legacy_api_defaults_still_follow_ambient_settings(tmp_path, monkeypatch):
    import memory_server.settings as settings
    provider = await _sqlite(tmp_path)
    try:
        await _seed(provider)
        monkeypatch.setenv("MEMORY_SERVER_SEARCH_DEFAULT_LIMIT", "1")
        monkeypatch.setenv("MEMORY_SERVER_CONTEXT_DEDUP_OVERFETCH_FACTOR", "1")
        settings.get_settings.cache_clear()
        assert (await search(provider))["total"] == 1
        assert (await get_context(provider, "Topic", max_results=1))["decisions"] == []
        monkeypatch.setenv("MEMORY_SERVER_SEARCH_DEFAULT_LIMIT", "2")
        monkeypatch.setenv("MEMORY_SERVER_CONTEXT_DEDUP_OVERFETCH_FACTOR", "4")
        settings.get_settings.cache_clear()
        assert (await search(provider))["total"] == 2
        assert [d["id"] for d in (await get_context(provider, "Topic", max_results=1))["decisions"]] == ["d0"]
    finally:
        await provider.close()
        settings.get_settings.cache_clear()


def _oracle(label, value):
    action = os.environ.get("C07_ORACLE_ACTION", "compare")
    if action == "capture":
        assert SUBJECT == "legacy"
        contents = json.loads(ORACLE.read_text()) if ORACLE.exists() else {}
        contents[label] = value
        encoded = json.dumps(contents, sort_keys=True, ensure_ascii=True).encode()
        ORACLE.write_bytes(encoded)
        ORACLE_HASH.write_text(hashlib.sha256(encoded).hexdigest() + "\n")
        return
    encoded = ORACLE.read_bytes()
    assert hashlib.sha256(encoded).hexdigest() == ORACLE_HASH.read_text().strip()
    assert value == json.loads(encoded)[label]


@pytest.mark.asyncio
async def test_keyword_and_context_results_match_frozen_base(tmp_path):
    provider = await _sqlite(tmp_path)
    try:
        await _seed(provider)
        observations = []
        for query, subject, predicate, limit, inactive in (
            ("", None, None, 50, False), ("Docker", "", "", 2, False),
            ("caddy", None, None, 1, True), ("nonexistent", None, None, 50, False),
            ("", "Docker", "uses", 50, False), ("", "Docker", None, 50, True),
        ):
            if SUBJECT == "legacy":
                result = await search(provider, query, subject or None, predicate or None, limit, inactive)
            else:
                from memory_server.application.contracts import SearchCommand
                from memory_server.application.retrieval_service import RetrievalService
                result = await RetrievalService(provider, policy=_policy()).search(
                    SearchCommand(query, subject, predicate, limit, inactive))
            observations.append(result)
        for limit, inactive in ((1, False), (2, True), (10, False)):
            if SUBJECT == "legacy":
                result = await get_context(provider, "Topic", max_results=limit, include_inactive=inactive)
            else:
                from memory_server.application.contracts import ContextCommand
                from memory_server.application.retrieval_service import RetrievalService
                result = await RetrievalService(provider, policy=_policy()).get_context(
                    ContextCommand("Topic", max_results=limit, include_inactive=inactive))
            observations.append(result)
        _oracle("keyword-context", observations)
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_graph_modes_and_precedence_match_frozen_base(monkeypatch):
    router = _graph()
    if SUBJECT == "legacy":
        await _legacy_graph(router, monkeypatch)
    bound = await _graph_subject(router)
    cases = (
        {}, {"relation_type": "links"}, {"relation_type": " LINKS "},
        {"relation_type": "   ", "entity_id": "alpha"},
        {"entity_id": "alpha"}, {"entity_id": "missing"},
        {"source_id": "alpha", "target_id": "delta"},
        {"source_id": "omega", "target_id": "delta"},
        {"query": "Alpha Delta"}, {"query": "missing"},
        {"relation_type": "uses", "entity_id": "alpha", "query": "Alpha"},
        {"entity_id": "alpha", "source_id": "alpha", "target_id": "delta"},
    )
    observations = []
    for case in cases:
        output = await bound(**case)
        observations.append(json.loads(output) if isinstance(output, str) else output)
    _oracle("graph", observations)


class _Embedder:
    def embed(self, text):
        return [1.0, 0.0, 0.0, 0.0]


def _rank_json(value):
    if isinstance(value, dict):
        return {k: _rank_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_rank_json(v) for v in value]
    if hasattr(value, "__dict__"):
        return _rank_json(value.__dict__)
    return value


@pytest.mark.asyncio
async def test_semantic_and_hybrid_score_order_threshold_parity(tmp_path, monkeypatch):
    from memory_server.providers.qdrant_provider import QdrantProvider
    from memory_server.router.embedding_router import EmbeddingRouter
    from memory_server.router.hybrid_router import HybridRouter
    provider = QdrantProvider(location=":memory:", collection="memories", vector_size=4,
                              distance="cosine", prefer_grpc=False)
    try:
        for idx, vector in enumerate(([1, 0, 0, 0], [0.8, 0.6, 0, 0], [0.6, 0.8, 0, 0]), 1):
            assert await provider.upsert("memories", point_id=f"00000000-0000-0000-0000-{idx:012d}",
                                         vector=vector,
                                         payload={"content": f"Vector {idx}", "confidence": 0.8})
        embedder = _Embedder()
        semantic = EmbeddingRouter(provider, embedder=embedder)
        graph = _graph().graph
        hybrid = HybridRouter(provider, embedder=embedder, graph=graph)
        if SUBJECT == "legacy":
            import memory_server.server as server
            async def get_semantic():
                return semantic
            async def get_hybrid():
                return hybrid
            monkeypatch.setattr(server, "_get_router", get_semantic)
            monkeypatch.setattr(server, "_get_hybrid_router", get_hybrid)
        else:
            from memory_server.application.retrieval_service import RetrievalService
            async def get_semantic():
                return semantic
            async def get_hybrid():
                return hybrid
            svc = RetrievalService(None, policy=_policy(), get_embedding_router=get_semantic,
                                   get_hybrid_router=get_hybrid)
        async def observe(query, top_k, threshold, route=False):
            if SUBJECT == "legacy":
                tool = server.route_tool if route else server.semantic_search_tool
                return json.loads(await tool(query=query, top_k=top_k,
                                             score_threshold=threshold))
            from memory_server.application.contracts import RouteCommand, SemanticSearchCommand
            command = RouteCommand(query, top_k, threshold) if route else SemanticSearchCommand(query, top_k, threshold)
            output = await (svc.route(command) if route else svc.semantic_search(command))
            return _rank_json(output)
        reference = await observe("vector obscure", 10, 0.0)
        scores = [r["score"] for r in reference["semantic_results"]]
        assert scores, "real Qdrant produced no scores"
        threshold = scores[1]
        observations = []
        for query in ("", " ", "Docker", "vector obscure", "Alpha Delta"):
            for top_k in (1, 2, 10):
                for cutoff in (0.0, threshold, math.nextafter(threshold, -math.inf),
                               math.nextafter(threshold, math.inf)):
                    observations.append(await observe(query, top_k, cutoff))
                    observations.append(await observe(query, top_k, cutoff, route=True))
        _oracle("ranking", observations)
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_projection_error_identity_and_observable_response_match_base(monkeypatch):
    import memory_server.server as server
    sentinel = RuntimeError("E_PROJECTION_UNAVAILABLE: vector fixture unavailable")
    graph_error = RuntimeError("E_PROJECTION_UNAVAILABLE: graph fixture unavailable")
    async def failure(exc):
        raise exc
    observations = []
    for exc, kind in ((sentinel, "semantic"), (graph_error, "graph")):
        if SUBJECT == "legacy":
            monkeypatch.setattr(server, "_get_router" if kind == "semantic" else "_get_graph_router",
                                lambda exc=exc: failure(exc))
            tool = server.semantic_search_tool if kind == "semantic" else server.graph_search_fn
            value = await tool(query="abc")
        else:
            from memory_server.application.contracts import GraphSearchCommand, SemanticSearchCommand
            from memory_server.application.graph_service import GraphService
            from memory_server.application.retrieval_service import RetrievalService
            async def accessor(exc=exc):
                raise exc
            svc = (
                RetrievalService(None, policy=_policy(), get_embedding_router=accessor)
                if kind == "semantic"
                else GraphService(accessor, policy=_graph_policy())
            )
            async def invoke():
                return await (svc.semantic_search(SemanticSearchCommand("abc")) if kind == "semantic"
                              else svc.search(GraphSearchCommand(query="abc")))
            with pytest.raises(RuntimeError) as caught:
                await invoke()
            assert caught.value is exc
            value = await server._stable_projection_errors(invoke)()
        assert value == server._projection_error(exc)
        parsed = json.loads(value)
        assert parsed == {"error": "E_PROJECTION_UNAVAILABLE", "message": str(exc),
                          "hint": server.PROJECTION_MIGRATION_HINT}
        observations.append(value)
    _oracle("errors", observations)
    if SUBJECT == "application":
        from memory_server.application.contracts import SemanticSearchCommand
        from memory_server.application.retrieval_service import RetrievalService
        for exc in (RuntimeError("other failure"), ValueError("E_PROJECTION_UNAVAILABLE bogus")):
            async def failing(exc=exc):
                raise exc
            svc = RetrievalService(None, policy=_policy(), get_embedding_router=failing)
            with pytest.raises(type(exc)) as caught:
                await svc.semantic_search(SemanticSearchCommand("abc"))
            assert caught.value is exc


def test_application_composition_defaults_and_closed_surfaces(tmp_path):
    assert SUBJECT == "application"
    import memory_server.settings as settings
    from memory_server.application.application import MemoryApplication
    from memory_server.application.contracts import GraphPolicy, MemoryPolicy, RetrievalPolicy, SearchCommand
    from memory_server.application.errors import UnknownDependencyError
    values = settings.get_settings()
    assert RetrievalPolicy.capture(values) == RetrievalPolicy(
        values.search_default_limit, values.context_default_limit,
        values.context_dedup_overfetch_factor, values.semantic_top_k,
        values.semantic_score_threshold)
    assert GraphPolicy.capture(values) == GraphPolicy(values.graph_max_path_depth)
    assert [f.name for f in fields(SearchCommand)] == ["query", "subject", "predicate", "limit", "include_inactive"]
    with pytest.raises(FrozenInstanceError):
        SearchCommand().query = "changed"
    app = MemoryApplication(lambda: None, policy=MemoryPolicy(500))
    assert app.describe() == {"policy": {"max_active_beliefs": 500}}
    with pytest.raises(UnknownDependencyError):
        _ = app.retrieval
    with pytest.raises(UnknownDependencyError):
        _ = app.graph
    source = Path(__file__).resolve().parents[1] / "src/memory_server/server.py"
    tree = ast.parse(source.read_text())
    names = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for dec in node.decorator_list:
            if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute) and dec.func.attr == "tool":
                names.append(next((k.value.value for k in dec.keywords if k.arg == "name"), node.name))
    assert set(names) == {"add_relation", "audit", "get_belief", "get_context", "graph_search",
                          "invalidate", "learn", "metrics", "ping", "reflect", "remember",
                          "resolve_conflict", "route", "search", "semantic_search", "set_belief"}
    assert "bulk_import" not in names
    from memory_server.profile_migration import DETAIL_DIAGNOSTIC_CODES
    assert len(DETAIL_DIAGNOSTIC_CODES) == 52
