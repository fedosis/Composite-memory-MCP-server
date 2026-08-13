"""Tests for typed inter-claim relation API."""

import pytest

from memory_server.api.add_relation import add_relation
from memory_server.providers.graph_provider import SimpleGraph


@pytest.mark.asyncio
class TestAddRelation:
    async def test_add_relation_creates_typed_edge(self):
        graph = SimpleGraph()
        graph.add_node(id="claim-a", type="fact", name="Claim A")
        graph.add_node(id="claim-b", type="fact", name="Claim B")

        result = await add_relation(
            graph=graph,
            source_id="claim-a",
            target_id="claim-b",
            relation_type="supports",
        )

        assert result["edge"]["relation"] == "supports"
        edge = graph.get_edge("claim-a", "claim-b")
        assert edge is not None
        assert edge.relation == "supports"

    async def test_add_relation_rejects_invalid_relation_type(self):
        graph = SimpleGraph()
        graph.add_node(id="claim-a", type="fact", name="Claim A")
        graph.add_node(id="claim-b", type="fact", name="Claim B")

        with pytest.raises(ValueError, match="relation_type"):
            await add_relation(
                graph=graph,
                source_id="claim-a",
                target_id="claim-b",
                relation_type="invalid",
            )

    async def test_add_relation_missing_node_raises(self):
        graph = SimpleGraph()
        graph.add_node(id="claim-a", type="fact", name="Claim A")

        with pytest.raises(KeyError, match="Target node"):
            await add_relation(
                graph=graph,
                source_id="claim-a",
                target_id="claim-b",
                relation_type="contradicts",
            )
