"""MCP server entry point with tool registrations."""

import json

from mcp.server.fastmcp import FastMCP

from memory_server.api.get_context import get_context as get_context_fn
from memory_server.providers.sqlite_provider import SQLiteProvider

mcp = FastMCP("CompositeMemoryServer")

# Lazy provider — initialized on first use
_provider: SQLiteProvider | None = None


async def _get_provider() -> SQLiteProvider:
    """Get or create the SQLiteProvider singleton."""
    global _provider
    if _provider is None:
        _provider = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
        await _provider.initialize()
    return _provider


@mcp.tool()
def ping() -> str:
    """Connectivity check — returns OK if server is alive"""
    return json.dumps({"status": "ok"})


@mcp.tool()
async def get_context_tool(task: str, subject: str = "", max_results: int = 10) -> str:
    """Retrieve structured context about a task.

    Args:
        task: The task description or search query.
        subject: Optional subject filter (pass empty string for no filter).
        max_results: Maximum number of facts to return (default 10).
    """
    provider = await _get_provider()
    result = await get_context_fn(
        provider,
        task=task,
        subject=subject if subject else None,
        max_results=max_results,
    )
    return json.dumps(result)


def run():
    mcp.run(transport="stdio")
