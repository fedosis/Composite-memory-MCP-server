"""Transport integrations for the memory server.

The MCP stdio transport is the single composition owner; this package only
re-exports its public transport entry points.
"""

from .mcp_stdio import mcp, run

__all__ = ["mcp", "run"]
