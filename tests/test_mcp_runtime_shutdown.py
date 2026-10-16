"""C10 I4-R0 lifecycle REDs and future-side characterization seams.

The first two tests intentionally cross the real MCP stdio lifespan.  They are
behavioral RED selectors against the immutable pre-C10 server; a collection or
import error is not an acceptable result.  The remaining five tests document
candidate-only assertions and stay explicitly disabled until I4-G enables the
future implementation side.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

ROOT = Path(os.environ["MEMORY_SERVER_DATA_ROOT"]).absolute().parent
ORACLE = Path(os.environ["CMMS_C00_ORACLE"])
PYTHON = Path("/home/shtorm/memory-server/.venv/bin/python")
SERVER_BIN = Path("/home/shtorm/memory-server/.venv/bin/memory-server")


def _contained(path: Path) -> bool:
    path, root = path.absolute(), ROOT
    return path == root or root in path.parents


def _env(root: Path) -> dict[str, str]:
    repo = Path(__file__).resolve().parents[1]
    values = {
        "PATH": os.environ["PATH"], "PYTHONHASHSEED": "0", "LC_ALL": "C", "TZ": "UTC",
        "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
        "HOME": str(root / "home"), "HERMES_HOME": str(root / "hermes"), "TMPDIR": str(root / "tmp"),
        "PYTHONPATH": f"{repo / 'src'}:{repo}",
        "MEMORY_SERVER_DATA_ROOT": str(root / "data"),
        "MEMORY_SERVER_DB_URL": f"sqlite+aiosqlite:///{root / 'data' / 'memory.db'}",
        "MEMORY_SERVER_LANCEDB_PATH": str(root / "data" / "lancedb"),
        "MEMORY_SERVER_GRAPH_SNAPSHOT_PATH": str(root / "data" / "graph.json"),
        "MEMORY_SERVER_VECTOR_BACKEND": "lancedb",
        "CMMS_C00_ORACLE": str(ORACLE),
    }
    # Keep the spelling of the storage paths explicit and fail closed on an
    # accidental live-root substitution.
    values["MEMORY_SERVER_LANCEDB_PATH"] = str(root / "data" / "lancedb")
    for key in ("HOME", "HERMES_HOME", "TMPDIR", "MEMORY_SERVER_DATA_ROOT", "MEMORY_SERVER_LANCEDB_PATH"):
        Path(values[key]).mkdir(parents=True, exist_ok=True)
    Path(values["MEMORY_SERVER_GRAPH_SNAPSHOT_PATH"]).parent.mkdir(parents=True, exist_ok=True)
    assert all(_contained(Path(values[key])) for key in ("HOME", "HERMES_HOME", "TMPDIR", "MEMORY_SERVER_DATA_ROOT", "MEMORY_SERVER_LANCEDB_PATH", "MEMORY_SERVER_GRAPH_SNAPSHOT_PATH"))
    return values


def _driver_env(root: Path, extra: dict[str, str] | None = None) -> dict[str, str]:
    env = _env(root)
    if extra:
        env.update(extra)
    return env


async def _call_ping(command: list[str], root: Path, env: dict[str, str]) -> bool:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    started = False
    params = StdioServerParameters(command=command[0], args=command[1:], env=env, cwd=str(root / "stdio"))
    (root / "stdio").mkdir(parents=True, exist_ok=True)
    try:
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await asyncio.wait_for(session.initialize(), timeout=12)
                started = True
                result = await asyncio.wait_for(session.call_tool("ping", arguments={}), timeout=12)
                assert result.content[0].text == '{"status": "ok"}'
    except Exception:
        started = False
    return started


@pytest.mark.asyncio
async def test_mcp_exclusive_startup_blocks_l1_before_provider_construction(tmp_path):
    """Real L1 SH holder must block old MCP startup before SQLite construction."""
    root = ROOT / "l1-red"
    root.mkdir(parents=True, exist_ok=True)
    env = _driver_env(root)
    lock_root = root / "data"
    ready = root / "evidence" / "l1-ready"
    ready.parent.mkdir(parents=True, exist_ok=True)
    sentinel = root / "evidence" / "provider-constructed"
    (root / "stdio").mkdir(parents=True, exist_ok=True)
    l1_code = f"""
from pathlib import Path
import sys
from memory_server.storage_lock import RuntimeStorageLock
lock = RuntimeStorageLock.acquire(Path({str(lock_root)!r}), timeout=5)
Path({str(ready)!r}).write_text({str(lock_root / '.cmms-storage.lock')!r})
sys.stdin.read()
lock.release()
"""
    l1 = await asyncio.create_subprocess_exec(
        str(PYTHON), "-c", l1_code, cwd=str(root / "stdio"), env=env,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    for _ in range(120):
        if ready.exists():
            break
        await asyncio.sleep(0.05)
    if not ready.exists():
        if l1.returncode is None:
            l1.terminate()
            await asyncio.wait_for(l1.wait(), timeout=5)
        stderr = await l1.stderr.read() if l1.stderr is not None else b""
        raise AssertionError(
            f"L1 subprocess did not establish the real shared lock: "
            f"exit={l1.returncode}, stderr={stderr.decode(errors='replace')}"
        )
    lock_path = lock_root / ".cmms-storage.lock"
    inode_before = lock_path.stat().st_ino
    driver = f"""
from pathlib import Path
from memory_server import server
sentinel = Path({str(sentinel)!r})
original = server.SQLiteProvider.__init__
def mark(self, *args, **kwargs):
    sentinel.write_text('constructed')
    return original(self, *args, **kwargs)
server.SQLiteProvider.__init__ = mark
server.run()
"""
    started = False
    try:
        started = await _call_ping([str(PYTHON), "-c", driver], root, env)
    finally:
        assert l1.stdin is not None
        l1.stdin.write(b"release\n")
        await l1.stdin.drain()
        l1.stdin.close()
        await asyncio.wait_for(l1.wait(), timeout=5)
    inode_after = lock_path.stat().st_ino
    assert started is False, "old SH MCP path coexisted with L1; EX startup RED was not captured"
    assert not sentinel.exists(), "provider construction crossed the lock boundary"
    assert inode_before == inode_after


@pytest.mark.asyncio
async def test_mcp_s1_trace_has_one_schedule_and_retains_uncertain_owner():
    """Real stdio shutdown must expose one runtime S1 schedule and owner state."""
    root = ROOT / "s1-red"
    root.mkdir(parents=True, exist_ok=True)
    trace = root / "evidence" / "runtime-cleanup.json"
    trace.parent.mkdir(parents=True, exist_ok=True)
    driver = f"""
import json
from pathlib import Path
from memory_server.runtime.runtime import CMMSRuntime
trace = Path({str(trace)!r})
original = CMMSRuntime._cleanup
async def wrapped(self, t0, *, rollback=False):
    report = await original(self, t0, rollback=rollback)
    trace.write_text(json.dumps({{
        't0': t0, 'rollback': rollback, 'phases': report.phases,
        'code': report.code, 'lock_retained': report.lock_retained,
        'retained_resources': report.retained_resources, 'errors': report.errors,
    }}))
    return report
CMMSRuntime._cleanup = wrapped
from memory_server import server
server.run()
"""
    env = _driver_env(root)
    (root / "stdio").mkdir(parents=True, exist_ok=True)
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    params = StdioServerParameters(command=str(PYTHON), args=["-c", driver], env=env, cwd=str(root / "stdio"))
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await asyncio.wait_for(session.initialize(), timeout=30)
            result = await asyncio.wait_for(session.call_tool("ping", arguments={}), timeout=12)
            assert result.content[0].text == '{"status": "ok"}'
    assert trace.exists(), "MCP shutdown did not cross CMMSRuntime._cleanup"
    observed = json.loads(trace.read_text(encoding="utf-8"))
    deadlines = observed["phases"]["deadlines"]
    assert [round(deadlines[i + 1] - deadlines[0]) for i in range(4)] == [10, 15, 25, 30]
    assert observed["phases"]["closing"] == pytest.approx(observed["t0"])
    assert observed["rollback"] is False
    assert isinstance(observed["retained_resources"], list)
    if observed["code"] in {"E_RUNTIME_SHUTDOWN_TIMEOUT", "E_RUNTIME_CLEANUP_FAILED", "E_RUNTIME_FLUSH_FAILED"}:
        assert observed["lock_retained"] is True
    else:
        assert observed["lock_retained"] is False


_FUTURE = pytest.mark.skipif(
    os.environ.get("C10_ENABLE_FUTURE_TESTS") != "1",
    reason="candidate-side assertion; enable only after I1-I3 and I4-G",
)


@_FUTURE
def test_mcp_borrowed_sqlite_engine_is_closed_once_and_worker_is_not_closed():
    from memory_server.runtime.dependencies import CLOSE_ORDER

    assert CLOSE_ORDER[-1] == "sqlite"
    assert "outbox_worker" not in CLOSE_ORDER


@_FUTURE
@pytest.mark.asyncio
async def test_mcp_admission_errors_use_existing_fastmcp_channel():
    from memory_server.runtime.errors import RuntimeClosingError, RuntimeFailedError, RuntimeNotReadyError
    from memory_server.runtime.runtime import CMMSRuntime

    assert RuntimeNotReadyError.code == "E_RUNTIME_NOT_READY"
    assert RuntimeClosingError.code == "E_RUNTIME_CLOSING"
    assert RuntimeFailedError.code == "E_RUNTIME_FAILED"
    assert callable(CMMSRuntime.admit)


@_FUTURE
def test_mcp_graph_stdio_reaches_c07_graph_service():
    from memory_server import server

    source = Path(server.__file__).read_text(encoding="utf-8")
    assert "GraphSearchCommand" in source
    assert "application.graph.search" in source
    assert "search_by_relation" not in source
    assert "find_path" not in source
    assert "router.query" not in source


@_FUTURE
def test_mcp_stdio_clean_shutdown_exits_zero_and_cli_smoke_is_unchanged():
    from memory_server import cli, server

    assert callable(cli.app)
    assert "transport=\"stdio\"" in Path(server.__file__).read_text(encoding="utf-8")
    assert Path(cli.__file__).read_text(encoding="utf-8").count("from .server import run") == 1


@_FUTURE
def test_every_resolved_storage_lock_queue_and_stdio_path_is_inside_scratch():
    assert _contained(ROOT)
    assert _contained(Path(os.environ["TMPDIR"]))
    assert _contained(Path(os.environ["HOME"]))
    assert _contained(Path(os.environ["HERMES_HOME"]))
    assert _contained(Path(os.environ["MEMORY_SERVER_DATA_ROOT"]))
    assert _contained(Path(os.environ["MEMORY_SERVER_LANCEDB_PATH"]))
    assert _contained(Path(os.environ["MEMORY_SERVER_GRAPH_SNAPSHOT_PATH"]))
    assert Path("/home/shtorm/.hermes/data") not in ROOT.parents
    assert Path("/home/shtorm/memory-server/data") not in ROOT.parents
