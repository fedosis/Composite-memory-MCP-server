import json

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("CompositeMemoryServer")


@mcp.tool()
def ping() -> str:
    """Connectivity check — returns OK if server is alive"""
    return json.dumps({"status": "ok"})


def run():
    mcp.run(transport='stdio')
