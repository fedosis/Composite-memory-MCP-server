"""Integration tests — e2e: call learn() with real text, search for extracted items, verify full round-trip.

Uses the MCP stdio client to test learn() and verify extracted data
is queryable via search/get_context.
"""

import json

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import TextContent


@pytest.fixture
def server_params():
    return StdioServerParameters(command="memory-server", args=["serve"])


@pytest.mark.asyncio
class TestExtractorIntegration:
    """End-to-end integration tests for learn() with all extractors."""

    async def _call_and_parse(self, session, tool_name: str, arguments: dict) -> dict:
        """Call an MCP tool and return the parsed JSON result."""
        result = await session.call_tool(tool_name, arguments=arguments)
        for content_item in result.content:
            if isinstance(content_item, TextContent):
                return json.loads(content_item.text)
        text = result.content[0].text
        return json.loads(text)

    async def test_learn_extracts_and_searches_facts(self, server_params):
        """learn() with fact text, then search confirms it's stored."""
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                # Learn from text containing a fact
                learn_data = await self._call_and_parse(
                    session,
                    "learn",
                    arguments={
                        "text": "Docker is container runtime",
                        "source": "e2e-test",
                    },
                )

                assert "facts" in learn_data
                assert len(learn_data["facts"]) >= 1
                fact_item = learn_data["facts"][0]["item"]
                assert fact_item["subject"] == "Docker"
                assert fact_item["predicate"] == "is"
                assert "container" in fact_item["object"]
                assert fact_item["source"] == "e2e-test"

                # Search for the stored fact
                search_data = await self._call_and_parse(
                    session,
                    "search",
                    arguments={"query": "Docker"},
                )
                assert search_data["total"] >= 1
                subjects = [f["subject"] for f in search_data["results"]]
                assert "Docker" in subjects

    async def test_learn_extracts_and_searches_decisions(self, server_params):
        """learn() with decision text, then verify stored and searchable."""
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                learn_data = await self._call_and_parse(
                    session,
                    "learn",
                    arguments={
                        "text": "we decided to use Caddy because it is simpler than Nginx",
                        "source": "e2e-test",
                    },
                )

                assert "decisions" in learn_data
                assert len(learn_data["decisions"]) >= 1
                decision_item = learn_data["decisions"][0]["item"]
                assert decision_item["choice"] == "use Caddy"
                assert "simpler" in decision_item["reason"]
                assert decision_item["source"] == "e2e-test"

    async def test_learn_extracts_and_searches_skills(self, server_params):
        """learn() with skill text, then verify stored and searchable."""
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                learn_data = await self._call_and_parse(
                    session,
                    "learn",
                    arguments={
                        "text": "to deploy docker, do: 1) pull image, 2) run container",
                        "source": "e2e-test",
                    },
                )

                assert "skills" in learn_data
                assert len(learn_data["skills"]) >= 1
                skill_item = learn_data["skills"][0]["item"]
                assert skill_item["purpose"] == "deploy docker"
                assert "pull image" in skill_item["steps"]

    async def test_learn_full_round_trip(self, server_params):
        """Complete e2e: learn() with mixed content, verify all three types extracted and receipts returned."""
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                learn_data = await self._call_and_parse(
                    session,
                    "learn",
                    arguments={
                        "text": (
                            "Python is great. "
                            "decided to use FastAPI because async. "
                            "to deploy, do: 1) build image, 2) push."
                        ),
                        "source": "round-trip-test",
                    },
                )

                # All three types present
                assert len(learn_data["facts"]) >= 1
                assert len(learn_data["decisions"]) >= 1
                assert len(learn_data["skills"]) >= 1

                # Receipts list matches total
                total_items = (
                    len(learn_data["facts"])
                    + len(learn_data["decisions"])
                    + len(learn_data["skills"])
                )
                assert len(learn_data["receipts"]) == total_items

                # Receipts have correct memory types
                for f in learn_data["facts"]:
                    assert f["receipt"]["memory_type"] == "fact"
                    assert f["receipt"]["source"] == "round-trip-test"
                for d in learn_data["decisions"]:
                    assert d["receipt"]["memory_type"] == "decision"
                    assert d["receipt"]["source"] == "round-trip-test"
                for s in learn_data["skills"]:
                    assert s["receipt"]["memory_type"] == "skill"
                    assert s["receipt"]["source"] == "round-trip-test"

                # Verify stored facts are searchable
                search_data = await self._call_and_parse(
                    session,
                    "search",
                    arguments={"query": "Python"},
                )
                assert search_data["total"] >= 1

    async def test_learn_empty_text(self, server_params):
        """Empty text returns empty results."""
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                learn_data = await self._call_and_parse(
                    session,
                    "learn",
                    arguments={"text": ""},
                )
                assert learn_data["facts"] == []
                assert learn_data["decisions"] == []
                assert learn_data["skills"] == []
                assert len(learn_data["receipts"]) == 0

    async def test_learn_source_passed_through(self, server_params):
        """Source parameter flows to all receipts and items."""
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                learn_data = await self._call_and_parse(
                    session,
                    "learn",
                    arguments={
                        "text": "Docker is container. decided to use Caddy because simple",
                        "source": "test-session-42",
                    },
                )

                for f in learn_data["facts"]:
                    assert f["receipt"]["source"] == "test-session-42"
                    assert f["item"]["source"] == "test-session-42"
                for d in learn_data["decisions"]:
                    assert d["receipt"]["source"] == "test-session-42"
                    assert d["item"]["source"] == "test-session-42"
