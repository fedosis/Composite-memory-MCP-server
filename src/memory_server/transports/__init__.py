"""Transport entry points; MCP dependencies are loaded only on request."""

from importlib import import_module

__all__ = ["mcp", "run"]


def __getattr__(name):
    if name in {"mcp", "run"}:
        return getattr(import_module(".mcp_stdio", __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
