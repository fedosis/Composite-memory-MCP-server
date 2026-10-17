"""Compatibility facade for the single MCP stdio composition.

The implementation lives in :mod:`memory_server.transports.mcp_stdio`; this
module aliases that module so existing imports and monkeypatches address the
same mutable composition rather than a second singleton.
"""

import sys as _sys

from memory_server.transports import mcp_stdio as _mcp_stdio

_sys.modules[__name__] = _mcp_stdio
