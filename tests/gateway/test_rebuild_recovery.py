"""S20A1 recovery and frozen-source assertions against concrete owner APIs."""

import ast
import asyncio
import hashlib
import subprocess
import sys
import threading
import time
from pathlib import Path

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
