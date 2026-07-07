"""Graph integration tests — e2e: learn/remember → graph sync → search/route/pathfinding.

Card 021: verifies the full pipeline where extracted facts and decisions
are synced into the knowledge graph and become discoverable via
graph_search and the hybrid route() tool.
"""

import json

import pytest

from memory_server.api.learn import learn as learn_fn
from memory_server.api.remember import remember as remember_fn
from memory_server.providers.embedding_provider import MockEmbeddingProvider
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.qdrant_provider import QdrantProvider
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.router.graph_router import GraphRouter
from memory_server.router.hybrid_router import HybridRouter


# Import server globals so we can inject the test graph
from memory_server import server as server_module


@pytest.mark.asyncio
class TestGraphIntegration:
    """End-to-end tests: learn/remember → graph → search/route/pathfinding."""

    @pytest.fixture
    async def provider(self):
        """Fresh in-memory SQLite provider."""
        p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
        await p.initialize()
        yield p
        await p.close()

    @pytest.fixture
    def graph(self):
        """Fresh in-memory graph — also inject into server globals so
        graph_search_fn and route_tool use the same instance."""
        g = SimpleGraph()
        # Inject into global singleton so server tool functions use it
        server_module._graph = g
        server_module._graph_router = GraphRouter(graph=g)
        server_module._hybrid_router = None
        return g

    @pytest.fixture
    def hybrid_router(self, graph):
        """HybridRouter with MockEmbeddingProvider and the shared graph."""
        qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        embedder = MockEmbeddingProvider(vector_size=384)
        return HybridRouter(
            qdrant_provider=qdrant,
            embedder=embedder,
            graph=graph,
        )

    # ----------------------------------------------------------------
    # Test 1: learn() → fact extracted → synced to graph → graph_search
    # ----------------------------------------------------------------

    async def test_learn_syncs_facts_to_graph_and_graph_search_finds_them(
        self, provider, graph, hybrid_router
    ):
        """learn() extracts facts, syncing to graph makes them visible to graph_search."""
        # Step 1: learn() extracts facts from text
        result = await learn_fn(
            provider=provider,
            text="Docker is a container runtime used for virtualization",
            source="test-graph",
        )

        assert len(result["facts"]) >= 1
        docker_facts = [
            f for f in result["facts"]
            if "Docker" in f["item"]["subject"]
        ]
        assert len(docker_facts) >= 1

        # Step 2: Sync extracted facts to graph
        for f in result["facts"]:
            item = f["item"]
            hybrid_router.sync_fact(
                subject=item["subject"],
                predicate=item["predicate"],
                object=item["object"],
            )

        # Step 3: graph_search finds related entities (uses injected global graph)
        from memory_server.server import graph_search_fn

        search_result = json.loads(await graph_search_fn(query="Docker"))
        assert "nodes" in search_result
        node_names = {n["name"] for n in search_result["nodes"]}
        assert "Docker" in node_names, (
            f"Expected 'Docker' in graph nodes, got {node_names}"
        )
        # Edges include the neighbor "container runtime"
        assert len(search_result["edges"]) >= 1

    # ----------------------------------------------------------------
    # Test 2: learn() with Docker/Caddy → route() finds via graph
    # ----------------------------------------------------------------

    async def test_learn_with_docker_caddy_and_route_finds_via_graph(
        self, provider, graph, hybrid_router
    ):
        """learn() Docker/Caddy text, sync to graph, then route() discovers entities."""
        # Step 1: learn() from text mentioning Docker and Caddy
        result = await learn_fn(
            provider=provider,
            text=(
                "Docker is a container runtime. "
                "we decided to use Caddy for reverse proxy because simpler."
            ),
            source="test-route",
        )

        assert len(result["facts"]) >= 1
        assert len(result["decisions"]) >= 1

        # Step 2: Sync facts to graph
        for f in result["facts"]:
            item = f["item"]
            hybrid_router.sync_fact(
                subject=item["subject"],
                predicate=item["predicate"],
                object=item["object"],
            )

        # Sync decisions to graph — ensure Caddy entity node exists first
        # (sync_decision only links to existing nodes)
        hybrid_router.sync_fact(
            subject="Caddy", predicate="is", object="reverse proxy"
        )

        for d in result["decisions"]:
            item = d["item"]
            entities = []
            ctx = (item.get("context") or "") + " " + (item.get("choice") or "")
            for name in ["Docker", "Caddy", "Nginx"]:
                if name.lower() in ctx.lower():
                    entities.append(name)
            hybrid_router.sync_decision(
                choice=item["choice"],
                reason=item.get("reason", ""),
                entities=entities,
            )

        # Step 3: route() finds Docker via graph (stage 3)
        route_docker = await hybrid_router.route("Tell me about Docker")
        assert route_docker.get("stage") == 3, (
            f"Expected stage 3 (graph) for Docker query, got stage "
            f"{route_docker.get('stage')}: {route_docker.get('route')}"
        )

        # Step 4: route() finds Caddy via graph
        route_caddy = await hybrid_router.route("What is Caddy?")
        assert route_caddy.get("stage") == 3, (
            f"Expected stage 3 (graph) for Caddy query, got stage "
            f"{route_caddy.get('stage')}: {route_caddy.get('route')}"
        )

    # ----------------------------------------------------------------
    # Test 3: remember() → graph sync → pathfinding
    # ----------------------------------------------------------------

    async def test_remember_fact_reflected_in_graph_and_pathfinding(
        self, provider, graph, hybrid_router
    ):
        """remember() stores a fact; syncing to graph enables pathfinding."""
        # Step 1: remember() stores facts
        result_a = await remember_fn(
            provider=provider,
            subject="ServerAlpha",
            predicate="hosts",
            object="WebApp",
            confidence=1.0,
            source="test-path",
        )
        result_b = await remember_fn(
            provider=provider,
            subject="WebApp",
            predicate="uses",
            object="PostgreSQL",
            confidence=1.0,
            source="test-path",
        )

        assert result_a["receipt"].memory_type == "fact"
        assert result_b["receipt"].memory_type == "fact"

        # Step 2: Sync remembered facts to graph
        hybrid_router.sync_fact(
            subject="ServerAlpha", predicate="hosts", object="WebApp"
        )
        hybrid_router.sync_fact(
            subject="WebApp", predicate="uses", object="PostgreSQL"
        )

        # Step 3: Graph reflects the entities (uses injected global graph)
        from memory_server.server import graph_search_fn

        server_result = json.loads(await graph_search_fn(query="ServerAlpha"))
        assert "nodes" in server_result
        server_names = {n["name"] for n in server_result["nodes"]}
        assert "ServerAlpha" in server_names

        # Step 4: Pathfinding finds the path ServerAlpha → WebApp → PostgreSQL
        path_result = json.loads(await graph_search_fn(
            source_id="serveralpha", target_id="postgresql"
        ))
        assert "paths" in path_result
        assert len(path_result["paths"]) >= 1, (
            "Expected at least one path between ServerAlpha and PostgreSQL"
        )
        first_path = path_result["paths"][0]
        path_names = [n["name"] for n in first_path]
        assert "ServerAlpha" in path_names
        assert "WebApp" in path_names
        assert "PostgreSQL" in path_names, (
            f"Expected path ServerAlpha → WebApp → PostgreSQL, got {path_names}"
        )

    # ----------------------------------------------------------------
    # Test 4: Full pipeline — learn, sync, route, graph_search round-trip
    # ----------------------------------------------------------------

    async def test_full_pipeline_learn_sync_route_graph_search(
        self, provider, graph, hybrid_router
    ):
        """Complete round-trip: learn → sync → route → graph_search."""
        # Learn with technical infrastructure text
        result = await learn_fn(
            provider=provider,
            text="Docker is a container platform. Caddy serves port 443 for web traffic.",
            source="pipeline",
        )

        assert len(result["facts"]) >= 1

        # Sync all facts
        for f in result["facts"]:
            item = f["item"]
            hybrid_router.sync_fact(
                subject=item["subject"],
                predicate=item["predicate"],
                object=item["object"],
            )

        # route() discovers via graph
        route_omv8 = await hybrid_router.route("Tell me about Docker")
        assert route_omv8.get("stage") == 3, (
            f"Expected graph stage for Docker, got stage "
            f"{route_omv8.get('stage')} — {route_omv8.get('route')}"
        )

        # graph_search finds entities (uses injected global graph)
        from memory_server.server import graph_search_fn

        search_result = json.loads(await graph_search_fn(query="Docker"))
        node_names = {n["name"] for n in search_result["nodes"]}
        assert "Docker" in node_names
