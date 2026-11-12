"""S20A1 recovery and frozen-source assertions against concrete owner APIs."""

import ast
import asyncio
import hashlib
import json
import os
import selectors
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from memory_server import projection_rebuild as projections
from memory_server.runtime.dependencies import RuntimeDependencies
from memory_server.runtime.runtime import CMMSRuntime

BASE = "9b07be1262cc7b1185bd97596a934f998ad592d2"
ROOT = Path(__file__).resolve().parents[2]
P = "src/memory_server/projection_rebuild.py"
R = "src/memory_server/runtime/runtime.py"
PINS = (
    (P, 178, 408, "3fb6caf05e08019e0c38189976700a176c2787410cf4fb81cb5f83a946863fb0"),
    (P, 691, 759, "4d031ea6b9ccb2b90199ff7fdcf5e135b71000999d87f2d42eacb2cc809d20ef"),
    (P, 987, 1049, "47a46b4cc309c44e5ebdcfb89bfa1a701623fb58f92b48636e4bd1b970732386"),
    (P, 869, 980, "ad333cfc4c33df34baf6fd1b1f3a9883a032e23ffabbcc2856c2cc6b7004d5dc"),
    (P, 900, 980, "0a28958e6b9863d4640c4f7ee917d4bc5bd25514e47fc5fbe74c008bee8dab5c"),
    (R, 487, 535, "6ba05337500f30676f3338225af517cd67df9edf96eed46e990247e01781eb6d"),
    (R, 608, 693, "ffefa1461f87ca417f451da7d4e1b8176508fe15dcd65f4c8c2ae7406c271968"),
    (R, 746, 753, "74f433f4fb270b4c2672a5b05a73b7282a3d83b9510a8580396bb5e294be8900"),
    (R, 801, 912, "7d3016157937b744364e2fd924a583fa44aeae886e7df186550c792ebd5c4651"),
)


def owned_surface():
    for name in ("snapshot_owned_projection_records", "rebuild_owned_projections"):
        assert callable(getattr(projections, name, None)), f"S20A_SURFACE: {name}"


def nodes(raw):
    tree = ast.parse(raw)
    result = {}

    def walk(node, prefix=""):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                key = prefix + child.name
                assert key not in result, f"S20A_FROZEN: duplicate {key}"
                result[key] = child
                walk(child, key + ".")
            else:
                walk(child, prefix)

    walk(tree)
    return result


@pytest.mark.parametrize("path,start,end,digest", PINS)
def test_s20a_frozen_boundaries(path, start, end, digest):
    before = subprocess.check_output(["git", "show", f"{BASE}:{path}"], cwd=ROOT)
    after = (ROOT / path).read_bytes()
    base_lines, new_lines = before.splitlines(keepends=True), after.splitlines(keepends=True)
    expected = b"".join(base_lines[start - 1:end])
    assert hashlib.sha256(expected).hexdigest() == digest, "S20A_FROZEN: pinned oracle"
    old_nodes, new_nodes = nodes(before), nodes(after)
    if start == 900:
        base_node = old_nodes["rebuild_projections"]
        new_node = new_nodes["rebuild_projections"]
        actual = b"".join(new_lines[new_node.lineno + start - base_node.lineno - 1:new_node.end_lineno])
    else:
        selected = {key: node for key, node in old_nodes.items() if start <= node.lineno <= end}
        assert selected, "S20A_FROZEN: missing span"
        first = min(selected, key=lambda k: selected[k].lineno)
        last = max(selected, key=lambda k: selected[k].end_lineno)
        actual = b"".join(new_lines[new_nodes[first].lineno - 1:new_nodes[last].end_lineno])
    assert actual == expected, "S20A_FROZEN: byte identity"


def owned_references(owned):
    return {
        item.attr if isinstance(item, ast.Attribute) else item.id
        for node in owned for item in ast.walk(node)
        if isinstance(item, (ast.Name, ast.Attribute))
    }


@pytest.mark.parametrize("expression,name", [
    ("f(asyncio.to_thread, work)", "to_thread"),
    ("f(to_thread, work)", "to_thread"),
    ("Thread(target=_observe_vector_artifact)", "_observe_vector_artifact"),
    ("f(_staged_ids)", "_staged_ids"),
])
def test_s20a_ast_function_arguments(expression, name):
    assert name in owned_references([ast.parse(expression)]), "S20A_AST_ARGUMENT"


@pytest.mark.parametrize("name", ["snapshot_owned_projection_records", "rebuild_owned_projections"])
def test_s20a_new_api_bytes(name):
    before = subprocess.check_output(["git", "show", f"{BASE}:{P}"], cwd=ROOT)
    after = (ROOT / P).read_bytes()
    declaration = f"async def {name}(".encode()
    assert declaration not in before, "S20A_NEW_API_BYTES: invalid BASE oracle"
    assert after != before and declaration in after, "S20A_NEW_API_BYTES: no new API bytes"


def test_s20a_owned_surface():
    owned_surface()


def test_s20a_owned_reader_no_legacy_observer():
    owned_surface()
    source = (ROOT / P).read_bytes()
    tree = ast.parse(source)
    base = subprocess.check_output(["git", "show", f"{BASE}:{P}"], cwd=ROOT)
    old_nodes, new_nodes = nodes(base), nodes(source)
    definitions = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
    legacy_names = {node.name for node in ast.parse(base).body if isinstance(node, definitions)}
    owned = [node for node in tree.body if isinstance(node, definitions) and node.name not in legacy_names]
    forbidden = {"to_thread", "_staged_ids", "_observe_vector_artifact", "rebuild_projections", "to_arrow"}
    references = owned_references(owned)
    assert not (references & forbidden), "S20A_OWNED_NO_LEGACY_OBSERVER"
    for name, start, end in (("_observe_vector_artifact", 496, 561), ("_staged_ids", 840, 866)):
        old_node, new_node = old_nodes[name], new_nodes[name]
        assert (old_node.lineno, old_node.end_lineno) == (start, end), "S20A_FROZEN: observer span"
        expected = base.splitlines(keepends=True)[start - 1:end]
        actual = source.splitlines(keepends=True)[new_node.lineno - 1:new_node.end_lineno]
        assert actual == expected, "S20A_FROZEN: observer byte identity"


@pytest.mark.asyncio
@pytest.mark.parametrize("manifest", ["{", '{"version": 999}', None])
async def test_s20a_startup_preprovider_rejects_unsafe_journal(tmp_path, manifest):
    from memory_server.providers.sqlite_provider import SQLiteProvider

    root = tmp_path / "profile"
    run = root / ".projection-rebuild" / ("a" * 32)
    run.mkdir(mode=0o700, parents=True)
    root.chmod(0o700)
    run.parent.chmod(0o700)
    if manifest is not None:
        (run / "journal.json").write_text(manifest)
        (run / "journal.json").chmod(0o600)
    trace = []

    async def build(owner):
        trace.append("provider")
        provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{root / 'memory.db'}")
        await provider.initialize()
        return provider

    runtime = CMMSRuntime(root, profile_id="a", dependencies=RuntimeDependencies(recipes=(("sqlite", build),)))
    try:
        rejected = None
        try:
            await runtime.start()
        except Exception as exc:
            rejected = getattr(exc, "code", None)
        assert rejected == "E_STORAGE", "S20A_STARTUP: unsafe journal admitted"
        assert trace == [], "S20A_STARTUP: provider constructed before reconciliation"
    finally:
        await runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("snapshot,phase,allowed", [
    pytest.param({"unknown": 1}, "captured", False, id="unknown-object"),
    pytest.param([], "captured", False, id="empty-list"),
    pytest.param([1, 2], "captured", False, id="short-list"),
    pytest.param([1, 2, "ZZ", 4], "captured", False, id="bad-digest"),
    pytest.param([True, 2, "a" * 64, 4], "captured", False, id="bool-device"),
    pytest.param("x", "captured", False, id="string"),
    pytest.param(None, "captured", False, id="null-captured"),
    pytest.param([1, True, "a" * 64, 4], "captured", False, id="bool-inode"),
    pytest.param([1, 2, "a" * 64, True], "captured", False, id="bool-size"),
    pytest.param([-1, 2, "a" * 64, 4], "captured", False, id="negative-device"),
    pytest.param([1, -2, "a" * 64, 4], "captured", False, id="negative-inode"),
    pytest.param([1, 2, "a" * 64, -4], "captured", False, id="negative-size"),
    pytest.param([1.0, 2, "a" * 64, 4], "captured", False, id="float-device"),
    pytest.param([1, 2, "A" * 64, 4], "captured", False, id="uppercase-digest"),
    pytest.param([1, 2, "a" * 63, 4], "captured", False, id="short-digest"),
    pytest.param([1, 2, "a" * 64 + "\n", 4], "captured", False, id="digest-newline"),
    pytest.param([1, 2, 3, 4], "captured", False, id="nonstring-digest"),
    pytest.param([1, 2, "a" * 64, 4, 5], "captured", False, id="long-list"),
    pytest.param({"unknown": 1}, "snapshot", False, id="object-before-capture"),
    pytest.param(None, "snapshot", True, id="partial-snapshot-control"),
    pytest.param("captured-identity", "captured", True, id="captured-control"),
])
async def test_s20a_journal_snapshot_schema(tmp_path, snapshot, phase, allowed):
    import json

    from memory_server.providers.sqlite_provider import SQLiteProvider
    from tests.gateway.test_online_rebuild import projection_owner

    root = tmp_path / "profile"
    async with projection_owner(root) as (owner, custody):
        captured = await projections.snapshot_owned_projection_records(owner, custody=custody)
        layout, config = owner.layout, owner.config
    run_path = root / projections._OWNED_DIRECTORY / captured.run_id
    state = json.loads((run_path / "journal.json").read_bytes())
    state.update(snapshot=state["snapshot"] if allowed and phase == "captured" else snapshot, phase=phase)
    with projections._owned_dir(run_path) as run:
        projections._owned_journal(run, state)
    before = (run_path / "journal.json").read_bytes()
    calls = 0

    async def build(owner):
        nonlocal calls
        calls += 1
        provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{root / 'memory.db'}")
        await provider.initialize()
        return provider

    runtime = CMMSRuntime(layout, profile_id="a", config=config,
                          dependencies=RuntimeDependencies(recipes=(("sqlite", build),)))
    try:
        rejected = None
        try:
            await runtime.start()
        except Exception as exc:
            rejected = getattr(exc, "code", None)
        if allowed:
            assert rejected is None and runtime.state == "READY", "S20A_JOURNAL: valid state blocked"
            assert calls == 1, "S20A_JOURNAL: valid state did not construct provider once"
            assert json.loads((run_path / "journal.json").read_bytes())["phase"] == "reconciled", "S20A_JOURNAL"
        else:
            assert rejected == "E_STORAGE", "S20A_JOURNAL: malformed snapshot schema admitted"
            assert calls == 0, "S20A_JOURNAL: provider constructed before rejection"
            assert runtime.state != "READY", "S20A_JOURNAL: malformed journal reached READY"
            assert (run_path / "journal.json").read_bytes() == before, "S20A_JOURNAL: journal mutated"
    finally:
        assert (await runtime.close()).code is None, "S20A_JOURNAL: close failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("closing", [False, True], ids=["retained-block", "closing-priority"])
async def test_s20a_native_cancel_retains_owner(tmp_path, closing):
    from sqlalchemy import text

    from tests.gateway.test_online_rebuild import projection_owner

    async with projection_owner(tmp_path / "profile") as (owner, custody):
        loop = asyncio.get_running_loop()
        entered, release = asyncio.Event(), threading.Event()
        lock = owner.lock
        provider = owner.dependencies.sqlite
        old_graph = (owner.root / "graph.json").read_bytes()
        wrappers = []

        def held():
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(10):
                raise AssertionError("S20A_NATIVE: parent did not release thread")
            return "drained"

        handle = await owner.submit_native(held, kind="s20a-held-native")
        try:
            await asyncio.wait_for(entered.wait(), 2)
            for _ in range(2):
                wrapper = asyncio.create_task(handle.result())
                wrappers.append(wrapper)
                # An IPC-independent loop barrier lets result() enter its shield.
                await asyncio.wait({wrapper}, timeout=0.01)
                wrapper.cancel()
                await asyncio.gather(wrapper, return_exceptions=True)
                assert wrapper.cancelled(), "S20A_NATIVE: wrapper not cancelled"
                assert handle.thread.is_alive() and not handle.terminated, "S20A_NATIVE: false termination"
                assert not await handle.drain(time.monotonic()), "S20A_NATIVE: expired drain succeeded"
                try:
                    async with asyncio.timeout(0.02):
                        await owner.drain_maintenance(custody)
                except TimeoutError:
                    pass
                else:
                    pytest.fail("S20A_NATIVE: maintenance drained live native")
                assert owner._maintenance_state == "blocked", "S20A_NATIVE: cancellation opened gate"

            if closing:
                owner.release_admitted(custody.self_task)
                for _ in range(2):
                    waiter = asyncio.create_task(owner.close())
                    wrappers.append(waiter)
                    await asyncio.wait({waiter}, timeout=0.02)
                    assert not waiter.done(), "S20A_NATIVE: close completed while thread live"
                    waiter.cancel()
                    await asyncio.gather(waiter, return_exceptions=True)
                assert owner.state == "CLOSING", "S20A_NATIVE: close lost lifecycle priority"

            assert owner.lock is lock and lock._fh is not None, "S20A_NATIVE: EX released early"
            assert not owner._closed, "S20A_NATIVE: provider closed before native exit"
            assert owner.dependencies.sqlite is provider, "S20A_NATIVE: provider replaced"
            async with provider.engine.connect() as connection:
                value = (await connection.execute(text("SELECT id FROM facts"))).scalar()
                assert value == "f", "S20A_NATIVE: SQL closed"
            assert (owner.root / "graph.json").read_bytes() == old_graph, "S20A_NATIVE: early publication"
            assert not (owner.root / projections._OWNED_DIRECTORY).exists(), "S20A_NATIVE: early journal/rename"
            contender = subprocess.run(
                [sys.executable, "-c", "import fcntl,sys\n"
                 "with open(sys.argv[1], 'rb') as f:\n"
                 " try: fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
                 " except BlockingIOError: print('REFUSED')\n"
                 " else: print('ACQUIRED')\n", str(owner.root / ".cmms-storage.lock")],
                capture_output=True, text=True, timeout=3, check=False,
            )
            assert contender.returncode == 0, "S20A_NATIVE: OS contender failed"
            assert contender.stdout.strip() == "REFUSED", "S20A_NATIVE: OS contender acquired EX"
            release.set()
            assert await handle.drain(time.monotonic() + 3), "S20A_NATIVE: actual drain timed out"
            assert not handle.thread.is_alive() and handle.terminated, "S20A_NATIVE: reaper not terminal"
            assert owner._maintenance_state == "blocked", "S20A_NATIVE: drain unlocked retained block"
            try:
                owner.finish_maintenance(custody)
            except Exception as exc:
                assert getattr(exc, "code", None) in {
                    "E_RUNTIME_NOT_READY", "E_MAINTENANCE_CONFLICT",
                }, "S20A_NATIVE: wrong finish rejection"
            else:
                pytest.fail("S20A_NATIVE: finish reopened retained owner")
            if closing:
                assert owner.state in {"CLOSING", "CLOSED"}, "S20A_NATIVE: lifecycle reopened"
            else:
                assert owner.lock is lock and lock._fh is not None, "S20A_NATIVE: drain released EX"
                assert owner.state == "READY", "S20A_NATIVE: retained owner lost lifecycle"
        finally:
            release.set()
            await asyncio.gather(*wrappers, return_exceptions=True)
            assert await handle.drain(time.monotonic() + 3), "S20A_NATIVE: cleanup native leak"
            owner.release_admitted(custody.self_task)
            assert (await owner.close()).code is None, "S20A_NATIVE: terminal close failed"
        assert owner.state == "CLOSED" and owner.lock is None, "S20A_NATIVE: terminal ownership not released"


def recovery_layout(root):
    return SimpleNamespace(data_root=root, vector=SimpleNamespace(backend="lancedb", local_path=root / "vector"),
                           graph_snapshot_path=root / "graph.json")


def disk_scope(root):
    """Independent SQL readback, including outbox; never compare mutable WAL bytes."""
    with sqlite3.connect(root / "memory.db") as db:
        tables = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        rows = {name: sorted(db.execute(f'SELECT * FROM "{name}"').fetchall(), key=repr) for name in tables}
    vector = [(str(p.relative_to(root)), p.stat().st_ino, p.read_bytes())
              for p in sorted((root / "vector").rglob("*")) if p.is_file()]
    graph = root / "graph.json"
    return rows, vector, graph.read_bytes() if graph.exists() else None


async def restart_owner(root, *, callback=None):
    from memory_server.providers.sqlite_provider import SQLiteProvider

    trace = []

    async def build(owner):
        assert owner.lock._fh is not None, "S20A_RESTART: recipe without EX"
        for journal in (root / projections._OWNED_DIRECTORY).glob("*/journal.json"):
            assert json.loads(journal.read_bytes())["phase"] == "reconciled", "S20A_RESTART: early recipe"
        trace.append("recipes")
        provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{root / 'memory.db'}")
        await provider.initialize()
        return provider

    owner = CMMSRuntime(recovery_layout(root), profile_id="a",
                        dependencies=RuntimeDependencies(recipes=(("sqlite", build),)))
    if callback is not None:
        owner.configure_startup_recovery(callback, timeout_seconds=5)
    code = None
    try:
        try:
            await owner.start()
        except Exception as exc:
            code = getattr(exc, "code", type(exc).__name__)
        return code, owner.state, trace
    finally:
        assert (await owner.close()).code is None, "S20A_RESTART: cleanup"


async def crash_child(root, phase, operation, edge):
    """Only syscall observers are patched; each observed operation actually executes."""
    from tests.gateway.test_online_rebuild import projection_owner

    def install(owner):
        originals = {name: getattr(os, name) for name in ("fsync", "replace", "rename")}

        def wrapper(name):
            def call(*args, **kwargs):
                state = None
                if name == "fsync":
                    path = Path(os.readlink(f"/proc/self/fd/{args[0]}"))
                    candidate = path / "journal.json" if path.is_dir() else path
                    if candidate.name == "journal.json" or candidate.name.startswith("journal-"):
                        try:
                            state = json.loads(candidate.read_bytes())
                        except (ValueError, OSError):
                            pass
                    kind = "dir-fsync" if path.is_dir() else "file-fsync"
                else:
                    fd = kwargs.get("src_dir_fd")
                    if fd is None:
                        return originals[name](*args, **kwargs)
                    candidate = Path(os.readlink(f"/proc/self/fd/{fd}")) / str(args[0])
                    if name == "replace":
                        if not candidate.name.startswith("journal-"):
                            return originals[name](*args, **kwargs)
                        state = json.loads(candidate.read_bytes())
                    else:
                        for journal in (root / projections._OWNED_DIRECTORY).glob("*/journal.json"):
                            state = json.loads(journal.read_bytes())
                    kind = "record" if name == "replace" else "rename"
                    if name == "rename" and operation == "swap" and str(args[0]) in {"graph", "vector"}:
                        kind = "swap"
                match = state is not None and state["phase"] == phase and kind == operation

                def barrier():
                    assert owner.lock._fh is not None, "S20A_RESTART: child no EX"
                    print(json.dumps({"pid": os.getpid(), "phase": phase, "operation": operation,
                                      "edge": edge}), flush=True)
                    sys.stdin.buffer.read(1)
                    raise AssertionError("S20A_RESTART: parent did not kill")

                if match and edge == "before":
                    barrier()
                result = originals[name](*args, **kwargs)
                if match and edge == "after":
                    barrier()
                return result
            return call

        for name in originals:
            setattr(os, name, wrapper(name))
        return originals

    if phase in {"rollback", "reconciled"}:
        owner = CMMSRuntime(recovery_layout(root), profile_id="a")
        originals = install(owner)
        try:
            await owner.start()
        finally:
            for name, function in originals.items():
                setattr(os, name, function)
            await owner.close()
    else:
        async with projection_owner(root) as (owner, custody):
            from sqlalchemy import text

            async with owner.dependencies.sqlite.engine.begin() as db:
                await db.execute(text("PRAGMA journal_mode=WAL"))
            originals = install(owner)
            try:
                captured = await projections.snapshot_owned_projection_records(owner, custody=custody)
                await projections.rebuild_owned_projections(captured, custody=custody, targets=frozenset({"graph"}))
            finally:
                for name, function in originals.items():
                    setattr(os, name, function)


def kill_at(root, phase, operation, edge):
    command = [sys.executable, str(Path(__file__)), "crash", str(root), phase, operation, edge]
    child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            assert selector.select(10), "S20A_RESTART: child barrier timeout"
            line = child.stdout.readline()
        if not line:
            raise AssertionError(f"S20A_RESTART: child exited: {child.communicate(timeout=3)[1].decode()}")
        event = json.loads(line)
        assert event == dict(pid=child.pid, phase=phase, operation=operation, edge=edge), "S20A_RESTART: IPC"
        before = disk_scope(root)
        contender = subprocess.run(
            [sys.executable, "-c", "import fcntl,sys\nf=open(sys.argv[1],'rb')\n"
             "try: fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)\n"
             "except BlockingIOError: print('EX')\n", str(root / ".cmms-storage.lock")],
            capture_output=True, text=True, timeout=3, check=True,
        )
        assert contender.stdout.strip() == "EX", "S20A_RESTART: no actual child EX"
        child.send_signal(signal.SIGKILL)
        assert child.wait(timeout=3) == -signal.SIGKILL, "S20A_RESTART: kill/reap"
        assert not Path(f"/proc/{child.pid}").exists(), "S20A_RESTART: child survives reap"
        return before
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=3)
        for stream in (child.stdin, child.stdout, child.stderr):
            stream.close()


RESTART_POINTS = [
    (phase, operation, edge)
    for phase in ("snapshot", "captured", "stage", "verified", "publish", "published", "rollback", "reconciled")
    for operation in ("file-fsync", "record", "dir-fsync")
    for edge in ("before", "after")
] + [(phase, "rename", edge) for phase in ("publish", "rollback") for edge in ("before", "after")]


@pytest.mark.parametrize("phase,operation,edge", RESTART_POINTS)
def test_s20a_restart_each_phase(tmp_path, phase, operation, edge):
    root = tmp_path / "profile"
    if phase in {"rollback", "reconciled"}:
        kill_at(root, "published", "dir-fsync", "after")
    before = kill_at(root, phase, operation, edge)
    # The graph can be mid-swap; its original generation is independently known.
    from memory_server.providers.graph_provider import SimpleGraph

    old_path = tmp_path / "old.json"
    SimpleGraph().save_snapshot(old_path)
    old_graph = old_path.read_bytes()
    results = [asyncio.run(restart_owner(root)) for _ in range(2)]
    assert results[0] == results[1], "S20A_RESTART: second reconciliation not idempotent"
    code, state, trace = results[1]
    assert (code, state, trace) in [(None, "READY", ["recipes"]), ("E_STORAGE", "FAILED", [])], "S20A_RESTART"
    if (phase, operation, edge) == ("snapshot", "dir-fsync", "after"):
        assert code is None and state == "READY", "S20A_RESTART: partial snapshot blocks restart"
    after = disk_scope(root)
    assert after[:2] == before[:2], "S20A_RESTART: canonical/outbox/nonselected mutated"
    if code is None:
        assert after[2] == old_graph, "S20A_RESTART: incoherent old graph"


async def rebuild_existing(root):
    from memory_server.providers.sqlite_provider import SQLiteProvider

    async def build(owner):
        provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{root / 'memory.db'}")
        await provider.initialize()
        return provider

    owner = CMMSRuntime(recovery_layout(root), profile_id="a",
                        dependencies=RuntimeDependencies(recipes=(("sqlite", build),)))
    token = None
    try:
        await owner.start()
        token = owner.admit()
        custody = owner.begin_maintenance(profile_id="a", canonical_root=root, deadline=time.monotonic() + 20)
        await owner.drain_maintenance(custody)
        snapshot = await projections.snapshot_owned_projection_records(owner, custody=custody)
        await projections.rebuild_owned_projections(snapshot, custody=custody, targets=frozenset({"graph"}))
        assert owner._maintenance_state == "blocked", "S20A_KILL9: publication reopened gate"
        # Reopen actual disk through the frozen graph observer, not RebuildResult.
        expected = projections.expected_projection(snapshot.records)
        observed, errors = projections._ProjectionObservation(), []
        projections._observe_graph_artifact(expected, root / "graph.json", observed, errors)
        assert not errors, "S20A_KILL9: independent disk verification failed"
    finally:
        if token is not None:
            owner.release_admitted(token)
        assert (await owner.close()).code is None, "S20A_KILL9: close"


def test_s20a_kill9_mid_publish_then_rebuild(tmp_path):
    root = tmp_path / "profile"
    before = kill_at(root, "publish", "swap", "after")
    assert before[2] is not None, "S20A_KILL9: barrier was not after target swap"
    code, state, recipes = asyncio.run(restart_owner(root))
    assert (code, state, recipes) == (None, "READY", ["recipes"]), "S20A_KILL9: recovery blocked"
    from memory_server.providers.graph_provider import SimpleGraph

    old_path = tmp_path / "old.json"
    SimpleGraph().save_snapshot(old_path)
    assert disk_scope(root) == (*before[:2], old_path.read_bytes()), "S20A_KILL9: old generation not restored"
    asyncio.run(rebuild_existing(root))
    assert disk_scope(root)[:2] == before[:2], "S20A_KILL9: canonical/outbox/nonselected mutated"


@pytest.mark.parametrize("case", ["recovery", "no-journal", "truncated", "ambiguity"])
def test_s20a_startup_preprovider_single_callback(tmp_path, monkeypatch, case):
    root = tmp_path / "profile"
    if case != "no-journal":
        kill_at(root, "published", "dir-fsync", "after")
        journal = next((root / projections._OWNED_DIRECTORY).glob("*/journal.json"))
        if case == "truncated":
            journal.write_bytes(b'{"version":')
        elif case == "ambiguity":
            (root / "graph.json").write_bytes(b"unknown generation")

    async def exercise():
        from memory_server.providers.sqlite_provider import SQLiteProvider

        trace, effects = [], []

        async def build(owner):
            trace.append("recipes")
            provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{root / 'memory.db'}")
            await provider.initialize()
            return provider

        owner = CMMSRuntime(recovery_layout(root), profile_id="a",
                            dependencies=RuntimeDependencies(recipes=(("sqlite", build),)))
        fsync = os.fsync

        def observed(fd):
            fsync(fd)
            path = Path(os.readlink(f"/proc/self/fd/{fd}"))
            if path.is_dir() and (path / "journal.json").is_file():
                if json.loads((path / "journal.json").read_bytes())["phase"] == "reconciled":
                    assert owner.lock._fh is not None, "S20A_STARTUP: recovery without EX"
                    if "H03 recovery" not in trace:
                        trace.extend(["EX", "H03 recovery"])

        async def forbidden_task():
            effects.append("spawn")

        async def callback(runtime, *, deadline):
            assert runtime is owner and runtime.state == "STARTING", "S20A_STARTUP: wrong callback window"
            runtime.check_startup_custody(profile_id="a", canonical_root=root)
            if case == "no-journal":
                assert owner.lock._fh is not None, "S20A_STARTUP: no EX"
                assert not (root / projections._OWNED_DIRECTORY).exists(), "S20A_STARTUP: unexpected journal"
                trace.extend(["EX", "H03 no-journal"])
            else:
                assert trace == ["EX", "H03 recovery"], "S20A_STARTUP: callback before recovery"
            trace.append("callback")
            with pytest.raises(Exception) as rejected:
                await runtime.submit_native(lambda: effects.append("native"), kind="forbidden-startup")
            assert rejected.value.code == "E_RUNTIME_NOT_READY", "S20A_STARTUP: native admitted"
            with pytest.raises(Exception) as rejected:
                runtime.spawn_owned_task(forbidden_task, kind="forbidden-startup")
            assert rejected.value.code == "E_RUNTIME_NOT_READY", "S20A_STARTUP: spawn admitted"

        owner.configure_startup_recovery(callback, timeout_seconds=5)
        with pytest.raises(ValueError):
            owner.configure_startup_recovery(callback, timeout_seconds=5)
        try:
            with monkeypatch.context() as patch:
                patch.setattr(os, "fsync", observed)
                if case in {"truncated", "ambiguity"}:
                    with pytest.raises(Exception) as rejected:
                        await owner.start()
                    assert rejected.value.code == "E_STORAGE", "S20A_STARTUP: unsafe admitted"
                    assert trace == [], "S20A_STARTUP: provider/callback constructed on unsafe evidence"
                else:
                    await owner.start()
                    trace.append(owner.state)
                    recovery = "H03 no-journal" if case == "no-journal" else "H03 recovery"
                    assert trace == ["EX", recovery, "callback", "recipes", "READY"], "S20A_STARTUP: order/count"
                assert effects == [], "S20A_STARTUP: native/spawn side effect"
        finally:
            assert (await owner.close()).code is None, "S20A_STARTUP: close"

    asyncio.run(exercise())


def metadata_rows(store, profile):
    with store.borrow_transaction() as tx:
        return tuple((table, tuple(tuple(sorted(dict(row).items())) for row in tx.fetchmany(
            f"SELECT * FROM {table} WHERE profile_id=? ORDER BY "
            + ("id" if table == "jobs" else "subject,route_operation,key") + " LIMIT ?", (profile, 100), limit=100)))
                     for table in ("jobs", "job_requests"))


async def scope_child(root, control, job_id):
    from memory_server.control.jobs import JobMetadata
    from memory_server.control.store import ControlStore
    from memory_server.providers.sqlite_provider import SQLiteProvider
    from tests.gateway.test_jobs import NOW

    async def build(owner):
        provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{root / 'memory.db'}")
        await provider.initialize()
        return provider

    owner = CMMSRuntime(root, profile_id="b", dependencies=RuntimeDependencies(recipes=(("sqlite", build),)))
    await owner.start()
    token = owner.admit()
    try:
        with ControlStore(control) as store:
            metadata = JobMetadata(store)
            claim = metadata.claim(job_id, "b", "subject", 0, now=NOW)
            assert claim is not None and claim.job.status == "running", "S20A_SCOPE: B not running"
            (root / "canary").write_bytes(b"B-owned")
            print("B-RUNNING", flush=True)
            assert sys.stdin.buffer.read(1) == b"G", "S20A_SCOPE: release protocol"
            job = metadata.fail(claim, "E_INTERNAL", now=NOW)
            assert job.status == "failed", "S20A_SCOPE: B did not progress"
            async with owner.dependencies.sqlite.engine.connect() as db:
                from sqlalchemy import text

                assert (await db.execute(text("SELECT 1"))).scalar() == 1, "S20A_SCOPE: B engine closed"
            print("B-CONTINUED", flush=True)
    finally:
        owner.release_admitted(token)
        assert (await owner.close()).code is None, "S20A_SCOPE: B close"


def test_s20a_recovery_scope_and_rollback(tmp_path):
    from dataclasses import replace

    from memory_server.application.job_runner import JobCoordinator
    from memory_server.control.jobs import JobMetadata
    from memory_server.control.store import ControlStore
    from memory_server.gateway_contracts.ports import ProfileRecord
    from tests.gateway.test_jobs import NOW, context, enqueue, final

    root, other, control = (tmp_path / name for name in ("a", "b", "control"))
    kill_at(root, "published", "dir-fsync", "after")
    with ControlStore(control) as store:
        store._configure(tuple(ProfileRecord(name, path, True, name, None, {}, 1)
                               for name, path in (("a", root), ("b", other))))
        metadata = JobMetadata(store)
        a = final(store, enqueue(store, replace(context(), profile_id="a")))
        b = final(store, enqueue(store, replace(context(), profile_id="b")))
        assert metadata.claim(a.job_id, "a", "subject", 0, now=NOW) is not None, "S20A_SCOPE: A claim"
        child = subprocess.Popen([sys.executable, str(Path(__file__)), "scope", str(other), str(control), b.job_id],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ)
                assert selector.select(8), "S20A_SCOPE: B barrier timeout"
                assert child.stdout.readline() == b"B-RUNNING\n", "S20A_SCOPE: B failed before barrier"
            before = metadata_rows(store, "b")
            artifacts = (other / "canary").stat().st_ino, (other / "canary").read_bytes()
            journal = next((root / projections._OWNED_DIRECTORY).glob("*/journal.json"))
            old_journal = journal.read_bytes()

            async def recover():
                active = None

                def custody(runtime, *, profile_id):
                    assert runtime is active, "S20A_SCOPE: wrong runtime admitted"
                    runtime.check_startup_custody(profile_id=profile_id, canonical_root=root)

                coordinator = JobCoordinator(metadata, None, authorize=None, startup_custody=custody)

                async def callback(runtime, *, deadline):
                    nonlocal active
                    active = runtime
                    for wrong_runtime, profile in ((CMMSRuntime(root, profile_id="a"), "a"), (runtime, "b")):
                        with pytest.raises(Exception):
                            custody(wrong_runtime, profile_id=profile)

                    async def wrong_task():
                        with pytest.raises(Exception):
                            custody(runtime, profile_id="a")

                    await asyncio.create_task(wrong_task())
                    assert await coordinator.reconcile_startup(
                        profile_id="a", custody=runtime, deadline=deadline) == 1, "S20A_SCOPE: scoped count"

                no_ex = CMMSRuntime(root, profile_id="a")
                with pytest.raises(Exception):
                    no_ex.check_startup_custody(profile_id="a", canonical_root=root)
                assert await restart_owner(root, callback=callback) == (None, "READY", ["recipes"]), "S20A_SCOPE"
                assert not coordinator._attachments, "S20A_SCOPE: redispatch"
                await coordinator.close(deadline=time.monotonic() + 2)

            asyncio.run(recover())
            assert journal.read_bytes() != old_journal, "S20A_SCOPE: A journal not reconciled"
            assert metadata.get(a.job_id, "a", "subject").status == "interrupted", "S20A_SCOPE: A not interrupted"
            assert metadata_rows(store, "b") == before, "S20A_SCOPE: B ordered metadata changed"
            assert ((other / "canary").stat().st_ino, (other / "canary").read_bytes()) == artifacts, "S20A_SCOPE"
            assert child.poll() is None, "S20A_SCOPE: B did not retain process/EX"
            child.stdin.write(b"G")
            child.stdin.flush()
            stdout, stderr = child.communicate(timeout=8)
            assert child.returncode == 0 and stdout == b"B-CONTINUED\n", f"S20A_SCOPE: {stderr.decode()}"
            assert metadata.get(b.job_id, "b", "subject").status == "failed", "S20A_SCOPE: B did not continue"
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=3)
            for stream in (child.stdin, child.stdout, child.stderr):
                stream.close()


@pytest.mark.parametrize("fault", ["publish-fsync", "readback", "rollback", "closing"])
def test_s20a_publication_readback_rollback_failure(tmp_path, monkeypatch, fault):
    root = tmp_path / "profile"
    if fault == "rollback":
        before = kill_at(root, "published", "dir-fsync", "after")
        rename = os.rename
        attempts = []

        def fail_rollback(src, dst, **kwargs):
            if src == "old-graph":
                attempts.append((src, dst))
                raise OSError("rollback syscall fault")
            return rename(src, dst, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(os, "rename", fail_rollback)
            assert asyncio.run(restart_owner(root)) == ("E_STORAGE", "FAILED", []), "S20A_PUBLISH: rollback admitted"
        assert attempts == [("old-graph", "graph.json")], "S20A_PUBLISH: blind retry"
        journal = next((root / projections._OWNED_DIRECTORY).glob("*/journal.json"))
        assert json.loads(journal.read_bytes())["phase"] == "rollback", "S20A_PUBLISH: missing rollback intent"
        # Crash the real next recovery while the retained rollback is executing.
        kill_at(root, "rollback", "rename", "after")
        assert asyncio.run(restart_owner(root)) == (None, "READY", ["recipes"]), "S20A_PUBLISH: repeated rollback"
        assert disk_scope(root)[:2] == before[:2], "S20A_PUBLISH: nonselected/canonical mutation"
        return

    async def exercise():
        from tests.gateway.test_online_rebuild import projection_owner

        async with projection_owner(root) as (owner, custody):
            before = disk_scope(root)
            snapshot = await projections.snapshot_owned_projection_records(owner, custody=custody)
            rename, fsync, stat = os.rename, os.fsync, os.stat
            swapped, injected = False, []

            def observed_rename(src, dst, **kwargs):
                nonlocal swapped
                result = rename(src, dst, **kwargs)
                if src == "graph" and dst == "graph.json":
                    swapped = True
                    if fault == "closing":
                        owner._begin_close()
                return result

            def failing_fsync(fd):
                if swapped and fault == "publish-fsync" and not injected:
                    injected.append("fsync")
                    raise OSError("publish fsync fault")
                return fsync(fd)

            def failing_stat(path, *args, **kwargs):
                if swapped and fault == "readback" and path == "graph.json" and not injected:
                    injected.append("readback")
                    raise OSError("readback stat fault")
                return stat(path, *args, **kwargs)

            with monkeypatch.context() as patch:
                patch.setattr(os, "rename", observed_rename)
                patch.setattr(os, "fsync", failing_fsync)
                patch.setattr(os, "stat", failing_stat)
                if fault == "closing":
                    await projections.rebuild_owned_projections(snapshot, custody=custody, targets=frozenset({"graph"}))
                    assert owner.state == "CLOSING", "S20A_PUBLISH: close reopened"
                    with pytest.raises(Exception):
                        owner.finish_maintenance(custody)
                    assert owner.state == "CLOSING", "S20A_PUBLISH: finish reopened"
                else:
                    with pytest.raises(Exception) as rejected:
                        await projections.rebuild_owned_projections(snapshot, custody=custody,
                                                                    targets=frozenset({"graph"}))
                    assert rejected.value.code == "E_STORAGE", "S20A_PUBLISH: syscall failure escaped"
                    assert injected, "S20A_PUBLISH: fault did not cross production boundary"
                assert swapped and owner._maintenance_state == "blocked", "S20A_PUBLISH: fence reopened"
                assert owner.lock._fh is not None, "S20A_PUBLISH: EX dropped"
            assert disk_scope(root)[:2] == before[:2], "S20A_PUBLISH: canonical/outbox/nonselected mutation"
            return before

    before = asyncio.run(exercise())
    assert asyncio.run(restart_owner(root)) == (None, "READY", ["recipes"]), "S20A_PUBLISH: recovery"
    assert disk_scope(root) == before, "S20A_PUBLISH: whole selected rollback not coherent"
    assert asyncio.run(restart_owner(root)) == (None, "READY", ["recipes"]), "S20A_PUBLISH: repeat recovery"


if __name__ == "__main__":
    if sys.argv[1] == "scope":
        asyncio.run(scope_child(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4]))
    else:
        assert sys.argv[1] == "crash", "S20A_RESTART: invalid child mode"
        asyncio.run(crash_child(Path(sys.argv[2]), *sys.argv[3:]))
