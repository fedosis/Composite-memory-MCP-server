"""MCP server lifecycle and tool registration."""

import asyncio
import logging

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

logger = logging.getLogger(__name__)


def handle_ping() -> list[TextContent]:
    """Standalone ping handler — returns status ok."""
    return [TextContent(type="text", text='{"status": "ok"}')]


def get_tools() -> list[Tool]:
    """Return the list of registered MCP tools."""
    return [
        Tool(
            name="ping",
            description="Connectivity check — returns status ok",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
    ]


async def call_tool_handler(name: str, arguments: dict) -> list[TextContent]:
    """Route tool calls to the correct handler."""
    if name == "ping":
        return handle_ping()
    raise ValueError(f"Unknown tool: {name}")


def create_server() -> Server:
    """Create and configure the MCP server with all tools."""
    server = Server("memory-server")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return get_tools()

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        return await call_tool_handler(name, arguments)

    return server


def run_server_stdio(server: Server) -> None:
    """Run the MCP server over stdio transport (blocking)."""

    async def _run():
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, server.create_initialization_options())

    asyncio.run(_run())
