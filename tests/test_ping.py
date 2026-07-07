"""Tests for the ping tool and server lifecycle."""

import json
import subprocess
import sys
from pathlib import Path

import pytest


def test_cli_ping():
    """--ping subcommand returns status ok."""
    result = subprocess.run(
        [sys.executable, "-m", "memory_server.cli", "ping"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parent.parent,
    )
    assert result.returncode == 0
    assert result.stdout.strip() == '{"status": "ok"}'


def test_cli_version():
    """--version prints version string."""
    result = subprocess.run(
        [sys.executable, "-m", "memory_server.cli", "--version"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parent.parent,
    )
    assert result.returncode == 0
    assert result.stdout.startswith("memory-server v")


def test_server_tool_list():
    """get_tools returns ping tool in the list."""
    from memory_server.server import get_tools

    tools = get_tools()
    assert len(tools) == 1
    assert tools[0].name == "ping"
    assert tools[0].description is not None and "status" in tools[0].description


@pytest.mark.asyncio
async def test_server_ping_handler():
    """call_tool_handler('ping', {}) returns status ok."""
    from memory_server.server import call_tool_handler, handle_ping

    # Test standalone handler
    result = handle_ping()
    assert len(result) == 1
    payload = json.loads(result[0].text)
    assert payload == {"status": "ok"}

    # Test via router
    result2 = await call_tool_handler("ping", {})
    assert len(result2) == 1
    payload2 = json.loads(result2[0].text)
    assert payload2 == {"status": "ok"}


@pytest.mark.asyncio
async def test_server_unknown_tool():
    """Unknown tool name raises ValueError."""
    from memory_server.server import call_tool_handler

    with pytest.raises(ValueError, match="Unknown tool: nonexistent"):
        await call_tool_handler("nonexistent", {})
