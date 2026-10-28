"""S04 production router regressions and legacy composition parity."""
from __future__ import annotations

import logging

import pytest

from memory_server.providers.embedding_provider import MockEmbeddingProvider
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.router.embedding_router import EmbeddingRouter
from memory_server.router.graph_router import GraphRouter
from memory_server.router.hybrid_router import HybridRouter


class EmptyVector:
    async def search(self, **kwargs):
        return []


@pytest.mark.parametrize("router", [
    lambda: GraphRouter(),
    lambda: EmbeddingRouter(vector_provider=EmptyVector()),
    lambda: HybridRouter(vector_provider=EmptyVector(), graph=SimpleGraph()),
])
def test_router_missing_dependency_fails(router):
    with pytest.raises((TypeError, ValueError)):
        router()


@pytest.mark.asyncio
async def test_router_logs_metadata_only(caplog):
    canary = "S04_SYNTHETIC_QUERY_CANARY"
    caplog.set_level(logging.DEBUG)
    vector, embedder, graph = EmptyVector(), MockEmbeddingProvider(), SimpleGraph()
    await EmbeddingRouter(vector, embedder).route(canary)
    await HybridRouter(vector, embedder, graph=graph).route(canary)
    with pytest.raises(ValueError):
        GraphRouter(graph).sync_facts_batch([{"subject": canary}])
    assert canary not in caplog.text


@pytest.mark.parametrize("failure", [RuntimeError, ValueError])
@pytest.mark.asyncio
async def test_router_warning_paths_do_not_log_backend_content(caplog, failure):
    canary = "S04_SYNTHETIC_BACKEND_TOKEN_PATH_CANARY"
    caplog.set_level(logging.DEBUG)

    class FailedVector:
        async def search(self, **kwargs):
            raise failure(canary)

    class FailedFTS:
        async def search_facts(self, **kwargs):
            raise failure(canary)

    graph = SimpleGraph()
    embedder = MockEmbeddingProvider()
    assert await EmbeddingRouter(FailedVector(), embedder).search(canary) == []
    router = HybridRouter(FailedVector(), embedder, graph=graph, fts_provider=FailedFTS())

    def failed_graph(query):
        raise failure(canary)

    router._graph_router.query = failed_graph
    await router.route(canary)
    assert "FTS search failed" in caplog.text
    assert "Semantic search failed" in caplog.text
    assert "Graph search failed" in caplog.text
    assert canary not in caplog.text


@pytest.mark.asyncio
async def test_mcp_native_bundle_exact_payload_schema_and_close_parity(tmp_path, monkeypatch):
    import json
    from pathlib import Path

    from memory_server.api.remember import remember
    from memory_server.api.search import search
    from memory_server.application.contracts import RememberCommand, SearchCommand
    from memory_server.application.profile_bundle import BundlePolicies
    from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
    from memory_server.control.store import ControlStore
    from memory_server.runtime.daemon import Daemon
    from memory_server.runtime.dependencies import RuntimeDependencies
    from memory_server.runtime.runtime_manager import RuntimeManager
    from memory_server.settings import Settings
    from memory_server.transports import mcp_stdio as mcp
    from tests.gateway.test_daemon import ctx, factory, make_binding
    from tests.test_mcp_contract_parity import _normalize

    settings = Settings(_env_file=None, outbox_poll_interval_seconds=3600)
    bindings = [make_binding(tmp_path/name, name, settings) for name in ("mcp", "native", "alpha")]
    mcp_factory = factory(bindings[0].layout, config=bindings[0].config,
                          dependencies=RuntimeDependencies(hooks=mcp._MCPRuntimeHooks()))
    legacy = mcp.MCPComposition(settings=settings, layout=bindings[0].layout,
                                factory=mcp_factory, runtime=mcp_factory.runtime)
    native = factory(bindings[1].layout, config=bindings[1].config).runtime
    store = ControlStore(tmp_path/"control")
    daemon = Daemon(ProfileRegistry(store, RegistryConfiguration((bindings[2],))),
                    policies=BundlePolicies.capture(settings), manager=RuntimeManager(factory_builder=factory))
    monkeypatch.setattr(mcp, "_active_composition", legacy)
    payload = dict(subject="S04 synthetic", predicate="checks", object="parity", source="synthetic")

    def canonical(value):
        # Only generated UUIDs/timestamps differ; retain all other fields/values.
        if not isinstance(value, str):
            value = json.dumps(value, default=lambda item: item.model_dump(mode="json"), sort_keys=True)
        else:
            value = json.dumps(json.loads(value), sort_keys=True)
        return _normalize(value, tmp_path)

    orders = [[], [], []]

    def track(runtime, order):
        for name in ("vector", "sqlite"):
            resource = getattr(runtime.dependencies, name)
            close = resource.close

            async def record(_name=name, _close=close):
                order.append(_name)
                return await _close()

            monkeypatch.setattr(resource, "close", record)

    try:
        await legacy.start()
        await native.start()
        tools_before = [dict(name=t.name, description=t.description or "", input_schema=t.inputSchema)
                        for t in await mcp.mcp.list_tools()]
        golden = json.loads((Path(__file__).parents[1]/"fixtures/runtime_contracts.json").read_text())
        assert tools_before == golden["mcp"]["tools"]
        legacy_write = await mcp.remember_tool(**payload)
        native_write = await remember(native.dependencies.sqlite, **payload)
        async with daemon.acquire(ctx()) as session:
            bundle_write = await session.application.memory.remember(RememberCommand(**payload))
            assert canonical(bundle_write) == canonical(legacy_write) == canonical(native_write)
            legacy_read = await mcp.search_tool(query="S04 synthetic")
            native_read = await search(native.dependencies.sqlite, query="S04 synthetic")
            bundle_read = await session.application.retrieval.search(SearchCommand(query="S04 synthetic"))
            assert bundle_read["total"] == 1
            assert canonical(bundle_read) == canonical(legacy_read) == canonical(native_read)
            assert session.runtime.dependencies.hybrid_router.fts_provider is session.runtime.dependencies.sqlite
            track(session.runtime, orders[2])
        assert [dict(name=t.name, description=t.description or "", input_schema=t.inputSchema)
                for t in await mcp.mcp.list_tools()] == tools_before
        track(legacy.runtime, orders[0])
        track(native, orders[1])
        legacy_report, native_report = await legacy.close(), await native.close()
        daemon_report = await daemon.close_all()
        assert legacy_report.code is native_report.code is None and daemon_report.complete
        assert orders[0] == orders[1] == orders[2] == ["vector", "sqlite"]
        assert all(runtime.state == "CLOSED" and runtime.lock is None
                   for runtime in (legacy.runtime, native, session.runtime))
    finally:
        await legacy.close()
        await native.close()
        await daemon.close_all()
        store.close()
