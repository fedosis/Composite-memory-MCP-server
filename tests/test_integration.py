"""Integration tests — full e2e: server start → remember → search → get_context → verify.

Uses the MCP stdio client to test the server end-to-end via its MCP interface.
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
class TestIntegration:
    """Full end-to-end integration tests via MCP stdio client."""

    async def _call_and_parse(self, session, tool_name: str, arguments: dict) -> dict:
        """Call an MCP tool and return the parsed JSON result."""
        result = await session.call_tool(tool_name, arguments=arguments)
        # Extract text content from the response
        for content_item in result.content:
            if isinstance(content_item, TextContent):
                return json.loads(content_item.text)
        # If no TextContent found, try direct .text access as fallback
        text = result.content[0].text
        return json.loads(text)

    async def test_ping(self, server_params):
        """Server responds to ping."""
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                data = await self._call_and_parse(session, "ping", {})
                assert data["status"] == "ok"

    async def test_remember_and_search_flow(self, server_params):
        """Remember a fact, then search for it."""
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                # Remember a fact
                remember_data = await self._call_and_parse(
                    session,
                    "remember",
                    arguments={
                        "subject": "Docker",
                        "predicate": "runs_on",
                        "object": "OMV8",
                        "confidence": 1.0,
                        "source": "test",
                    },
                )
                assert "receipt" in remember_data
                assert "fact" in remember_data
                assert remember_data["fact"]["subject"] == "Docker"
                assert remember_data["fact"]["predicate"] == "runs_on"
                assert remember_data["fact"]["object"] == "OMV8"

                # Search for the fact by subject
                search_data = await self._call_and_parse(
                    session,
                    "search",
                    arguments={"query": "Docker"},
                )
                assert search_data["total"] >= 1
                subjects = [f["subject"] for f in search_data["results"]]
                assert "Docker" in subjects

    async def test_remember_and_get_context_flow(self, server_params):
        """Remember a fact, then retrieve it via get_context."""
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                # Remember
                await self._call_and_parse(
                    session,
                    "remember",
                    arguments={
                        "subject": "Caddy",
                        "predicate": "uses",
                        "object": "Port 443",
                        "source": "test",
                    },
                )

                # Get context
                ctx_data = await self._call_and_parse(
                    session,
                    "get_context",
                    arguments={"task": "Caddy"},
                )
                assert ctx_data["total"] >= 1
                subjects = [f["subject"] for f in ctx_data["facts"]]
                assert "Caddy" in subjects

    async def test_remember_and_verify_receipt(self, server_params):
        """Remember a fact, then search the receipt data is valid."""
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                # Remember
                data = await self._call_and_parse(
                    session,
                    "remember",
                    arguments={
                        "subject": "Nginx",
                        "predicate": "is",
                        "object": "Reverse proxy",
                        "confidence": 0.85,
                        "source": "manual",
                    },
                )
                receipt = data["receipt"]

                # Verify receipt structure
                assert receipt["memory_type"] == "fact"
                assert receipt["source"] == "manual"
                assert receipt["confidence"] == 0.85
                assert receipt["verification_status"] == "candidate"
                assert "id" in receipt
                assert "timestamp" in receipt

                # Verify fact structure
                fact = data["fact"]
                assert fact["subject"] == "Nginx"
                assert fact["predicate"] == "is"
                assert fact["object"] == "Reverse proxy"
                assert fact["confidence"] == 0.85
                assert fact["source"] == "manual"

    async def test_full_e2e_flow(self, server_params):
        """Complete e2e: remember multiple facts, search, get_context, verify."""
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                # Remember multiple facts
                facts = [
                    {"subject": "Python", "predicate": "is", "object": "Language", "source": "test"},
                    {"subject": "FastMCP", "predicate": "is", "object": "Framework", "source": "test"},
                    {"subject": "Python", "predicate": "uses", "object": "FastMCP", "source": "test"},
                ]
                for fact in facts:
                    await self._call_and_parse(session, "remember", arguments=fact)

                # Search for Python
                search_data = await self._call_and_parse(
                    session, "search", arguments={"query": "Python"}
                )
                assert search_data["total"] == 2

                # Get context for Python
                ctx_data = await self._call_and_parse(
                    session, "get_context", arguments={"task": "Python"}
                )
                assert ctx_data["total"] >= 2

                # Search with filter
                filtered_data = await self._call_and_parse(
                    session, "search", arguments={"query": "", "subject": "Python"}
                )
                assert filtered_data["total"] == 2
                for f in filtered_data["results"]:
                    assert f["subject"] == "Python"

                # Search with no results
                empty_data = await self._call_and_parse(
                    session, "search", arguments={"query": "XYZZZDoesNotExist"}
                )
                assert empty_data["total"] == 0
                assert empty_data["results"] == []
