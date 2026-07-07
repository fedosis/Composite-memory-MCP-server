import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


@pytest.mark.asyncio
async def test_ping():
    server_params = StdioServerParameters(
        command="memory-server", args=["serve"]
    )
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("ping", arguments={})
            assert result.content[0].text == '{"status": "ok"}'
