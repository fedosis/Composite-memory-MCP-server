import os

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


@pytest.mark.asyncio
async def test_ping(tmp_path):
    env = os.environ.copy()
    env["MEMORY_SERVER_DB_URL"] = f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}"
    server_params = StdioServerParameters(command="memory-server", args=["serve"], env=env, cwd=tmp_path)
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("ping", arguments={})
            assert result.content[0].text == '{"status": "ok"}'


def test_server_import_does_not_import_optional_vector_backends(monkeypatch):
    """Clean base installs must import the stdio server without optional vector deps."""
    import builtins
    import sys

    blocked_roots = {"lancedb", "numpy", "pyarrow", "qdrant_client", "sentence_transformers"}
    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name.split(".", maxsplit=1)[0] in blocked_roots:
            raise ImportError(f"blocked optional dependency: {name}")
        return original_import(name, globals, locals, fromlist, level)

    for module_name in list(sys.modules):
        if module_name == "memory_server.server" or module_name.startswith("memory_server.router"):
            sys.modules.pop(module_name)
        if module_name.split(".", maxsplit=1)[0] in blocked_roots:
            sys.modules.pop(module_name)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    import memory_server.server as server

    assert server.mcp is not None
