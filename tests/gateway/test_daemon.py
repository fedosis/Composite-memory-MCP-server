"""Real SQLite/EX/manager/worker S04 lifecycle boundaries; synthetic vector only."""
from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from memory_server.application.contracts import SearchCommand
from memory_server.application.profile_bundle import BundlePolicies, ProfileBundle
from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
from memory_server.control.store import ControlStore
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import Principal, ProfileBinding, RequestContext
from memory_server.paths import StorageResolutionInputs, resolve_storage_layout
from memory_server.providers.embedding_provider import MockEmbeddingProvider
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.runtime.daemon import Daemon, LeaseError
from memory_server.runtime.runtime import CMMSRuntime
from memory_server.runtime.runtime_factory import RuntimeFactory
from memory_server.runtime.runtime_manager import RuntimeDescriptor, RuntimeManager
from memory_server.settings import Settings


class SyntheticVector:
    async def search(self, **kwargs):
        return []

    async def close(self):
        pass


def make_binding(root, name, settings):
    root.mkdir(parents=True, exist_ok=True)
    layout = resolve_storage_layout(StorageResolutionInputs(
        mode="standalone", data_root=root, sqlite_url="sqlite+aiosqlite:///memory.db",
        lancedb_path="lancedb", graph_snapshot_path="graph.json", legacy_split_compat=False,
    ))
    from memory_server.runtime.config import RuntimeConfig

    config = RuntimeConfig.capture(layout, settings)
    return ProfileBinding(d.Profile(
        id=name, display_name=name, description=None, enabled=True, generation=1,
        runtime_state="not_started", capabilities=(), storage_root=None,
    ), layout, config, RuntimeDescriptor.from_layout(name, layout=layout, config=config))


@pytest.mark.asyncio
async def test_exception_releases_once_and_retains_owner(setup, monkeypatch):
    daemon, *_ = setup
    release, tokens = CMMSRuntime.release_admitted, []

    def recorded(self, token):
        tokens.append(token)
        release(self, token)

    monkeypatch.setattr(CMMSRuntime, "release_admitted", recorded)
    with pytest.raises(ValueError, match="synthetic"):
        async with daemon.acquire(ctx()) as session:
            raise ValueError("synthetic")
    assert len(tokens) == 1 and not session.runtime._admitted
    async with daemon.acquire(ctx()) as again:
        assert again.runtime is session.runtime
    assert len(tokens) == 2


@pytest.mark.asyncio
async def test_registry_root_swap_before_admission_rejected(setup, tmp_path, monkeypatch):
    daemon, registry, _, bindings, _ = setup
    original = registry.resolve
    root = bindings[0].layout.data_root
    outside = tmp_path/"outside"
    outside.mkdir()
    sentinel = outside/"sentinel"
    sentinel.write_bytes(b"synthetic-untouched")

    async def swapped(who, name):
        binding = await original(who, name)
        root.rename(tmp_path/"original-alpha")
        root.symlink_to(outside, target_is_directory=True)
        return binding

    monkeypatch.setattr(registry, "resolve", swapped)
    with pytest.raises(LeaseError) as exc:
        async with daemon.acquire(ctx()):
            pytest.fail("swapped binding must not be admitted")
    assert exc.value.code == "E_STORAGE"
    assert str(root) not in str(exc.value)
    assert sentinel.read_bytes() == b"synthetic-untouched"
    assert sorted(p.name for p in outside.iterdir()) == ["sentinel"]
    assert all(entry.factory is None for entry in daemon.manager._entries)


@pytest.mark.parametrize("deadline", [0.0, -1.0])
@pytest.mark.asyncio
async def test_expired_deadline_never_starts_runtime(setup, deadline):
    daemon, *_ = setup
    with pytest.raises(LeaseError) as exc:
        async with daemon.acquire(replace(ctx(), deadline=deadline)):
            pass
    assert exc.value.code == "E_TIMEOUT"
    assert not daemon.manager._entries


def principal(*profiles):
    return Principal("S04_SYNTHETIC_TOKEN_CANARY", "synthetic", None, frozenset(profiles),
                     frozenset({"profile.read", "profile.manage", "memory.read"}), 1,
                     datetime.now(timezone.utc) + timedelta(hours=1))


def ctx(profile="alpha", who=None):
    return RequestContext("req_synthetic", who or principal(profile), profile, None, time.monotonic() + 30)


def factory(layout, **kwargs):
    # Real manager/factory/SQLite/graph/worker/custody. Only network/model backends
    # are synthetic; no monkeypatch of production admission or resource closing.
    return RuntimeFactory.create(layout, overrides={
        "embedder": lambda runtime: MockEmbeddingProvider(),
        "vector": lambda runtime: SyntheticVector(),
    }, **kwargs)


@pytest.fixture
async def setup(tmp_path):
    settings = Settings(_env_file=None, outbox_poll_interval_seconds=0.01)
    bindings = tuple(make_binding(tmp_path/name, name, settings) for name in ("alpha", "beta"))
    store = ControlStore(tmp_path/"control")
    registry = ProfileRegistry(store, RegistryConfiguration(bindings))
    daemon = Daemon(registry, policies=BundlePolicies.capture(settings),
                    manager=RuntimeManager(factory_builder=factory))
    yield daemon, registry, store, bindings, settings
    await daemon.close_all()
    store.close()


@pytest.mark.asyncio
async def test_concurrent_identity_admissions_and_once_production_spawn(setup, monkeypatch):
    daemon, _, _, _, _ = setup
    counts = {"snapshot": 0, "spawn": 0, "get": 0}
    load, spawn, get = SimpleGraph.load_snapshot, CMMSRuntime.spawn_owned_task, daemon.manager.get

    def counted_load(self, *args, **kwargs):
        counts["snapshot"] += 1
        return load(self, *args, **kwargs)

    def counted_spawn(self, coroutine_factory, *, kind):
        if kind == "daemon-worker-parent":
            counts["spawn"] += 1
        return spawn(self, coroutine_factory, kind=kind)

    async def counted_get(*args, **kwargs):
        counts["get"] += 1
        return await get(*args, **kwargs)

    monkeypatch.setattr(SimpleGraph, "load_snapshot", counted_load)
    monkeypatch.setattr(CMMSRuntime, "spawn_owned_task", counted_spawn)
    monkeypatch.setattr(daemon.manager, "get", counted_get)
    ready = [asyncio.Event(), asyncio.Event()]
    release = asyncio.Event()
    sessions = []

    async def hold(index):
        async with daemon.acquire(ctx()) as session:
            sessions.append(session)
            ready[index].set()
            await release.wait()

    tasks = [asyncio.create_task(hold(i)) for i in range(2)]
    await asyncio.gather(*(event.wait() for event in ready))
    assert sessions[0].runtime is sessions[1].runtime
    runtime = sessions[0].runtime
    assert len(runtime._admitted) == 2
    assert runtime.lock is not None
    assert len(daemon.manager._entries) == 1
    assert counts == {"snapshot": 1, "spawn": 1, "get": 3}
    release.set()
    await asyncio.gather(*tasks)
    assert not runtime._admitted


@pytest.mark.asyncio
async def test_nested_lease_has_one_token_and_no_early_release(setup, monkeypatch):
    daemon, *_ = setup
    released = []
    release = CMMSRuntime.release_admitted

    def spy(self, token):
        released.append(token)
        release(self, token)

    monkeypatch.setattr(CMMSRuntime, "release_admitted", spy)
    async with daemon.acquire(ctx()) as outer:
        async with daemon.acquire(ctx()) as inner:
            assert inner is outer
            assert len(inner.runtime._admitted) == 1
        assert len(outer.runtime._admitted) == 1 and not released
    assert len(released) == 1 and not outer.runtime._admitted


@pytest.mark.asyncio
async def test_cancel_one_retains_survivor_and_stop_waits_last(setup, monkeypatch):
    daemon, *_ = setup
    ready = [asyncio.Event(), asyncio.Event()]
    release = asyncio.Event()
    sessions, releases = [], []
    original = CMMSRuntime.release_admitted

    def spy(self, token):
        releases.append(token)
        original(self, token)

    monkeypatch.setattr(CMMSRuntime, "release_admitted", spy)

    async def hold(i):
        async with daemon.acquire(ctx()) as session:
            sessions.append(session)
            ready[i].set()
            await release.wait()
            assert await session.application.retrieval.search(SearchCommand()) == {"results": [], "total": 0}

    tasks = [asyncio.create_task(hold(i)) for i in range(2)]
    await asyncio.gather(*(event.wait() for event in ready))
    runtime = sessions[0].runtime
    tasks[0].cancel()
    with pytest.raises(asyncio.CancelledError):
        await tasks[0]
    assert len(runtime._admitted) == 1 and len(releases) == 1
    assert await daemon.stop_profile("alpha") is None
    assert runtime.state == "READY" and runtime.lock is not None
    release.set()
    await tasks[1]
    assert len(releases) == 2 and len(set(releases)) == 2
    assert runtime.state == "CLOSED" and runtime.lock is None


@pytest.mark.asyncio
async def test_cancel_during_composition_retains_shared_work(setup, monkeypatch):
    daemon, *_ = setup
    entered, release = asyncio.Event(), asyncio.Event()
    compose = ProfileBundle.compose.__func__

    async def blocked(cls, runtime, policies):
        entered.set()
        await release.wait()
        return await compose(cls, runtime, policies)

    monkeypatch.setattr(ProfileBundle, "compose", classmethod(blocked))

    async def caller():
        async with daemon.acquire(ctx()):
            pytest.fail("cancelled caller must not be admitted")

    task = asyncio.create_task(caller())
    await entered.wait()
    runtime = daemon._entries["alpha"].runtime
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not runtime._admitted
    release.set()
    async with daemon.acquire(ctx()) as session:
        assert session.runtime is runtime and runtime.lock is not None


@pytest.mark.asyncio
async def test_stop_alpha_keeps_beta_sqlite_live_and_bytes_unchanged(setup, record_property):
    daemon, _, _, bindings, _ = setup

    def open_count(root):
        count = 0
        for fd in os.listdir("/proc/self/fd"):
            try:
                target = os.readlink("/proc/self/fd/" + fd)
            except FileNotFoundError:
                continue
            count += target.startswith(str(root) + "/")
        return count

    async with daemon.acquire(ctx("alpha")) as alpha:
        pass
    async with daemon.acquire(ctx("beta")) as beta:
        pass
    a_before, b_before = open_count(alpha.runtime.root), open_count(beta.runtime.root)
    record_property("alpha_fds_before", a_before)
    record_property("beta_fds_before", b_before)
    assert a_before >= 2 and b_before >= 2  # Real SQLite + EX owner descriptors.
    path = bindings[1].layout.sqlite.local_path
    before = path.read_bytes()
    assert (await daemon.stop_profile("alpha")).code is None
    assert alpha.runtime.state == "CLOSED" and alpha.runtime.lock is None
    assert open_count(alpha.runtime.root) == 0
    assert open_count(beta.runtime.root) >= 2
    assert await beta.application.retrieval.search(SearchCommand()) == {"results": [], "total": 0}
    assert beta.runtime.state == "READY" and beta.runtime.lock is not None
    assert path.read_bytes() == before
    assert (await daemon.stop_profile("beta")).code is None
    assert open_count(beta.runtime.root) == 0
    record_property("alpha_fds_after", 0)
    record_property("beta_fds_after", 0)


@pytest.mark.asyncio
async def test_failed_close_retains_owner_and_denies_replacement(setup, monkeypatch):
    daemon, *_ = setup
    async with daemon.acquire(ctx()) as session:
        pass
    runtime = session.runtime
    lock, owner = runtime.lock, runtime.runtime_id
    original = runtime.dependencies.sqlite.close

    async def fail():
        raise RuntimeError("S04_SYNTHETIC_CLOSE_CANARY")

    monkeypatch.setattr(runtime.dependencies.sqlite, "close", fail)
    report = await daemon.stop_profile("alpha")
    assert report.lock_retained and runtime.lock is lock
    with pytest.raises(LeaseError) as exc:
        async with daemon.acquire(ctx()):
            pass
    assert exc.value.code == "E_RUNTIME_FAILED" and runtime.runtime_id == owner
    assert len(daemon.manager._entries) == 1
    monkeypatch.setattr(runtime.dependencies.sqlite, "close", original)
    # Explicit operator cleanup of the same retained owner, never a replacement.
    await daemon.manager.stop_profile("alpha")


@pytest.mark.asyncio
async def test_snapshot_exception_no_publication_no_worker_then_retry(setup, monkeypatch):
    daemon, *_ = setup
    load, spawn = SimpleGraph.load_snapshot, CMMSRuntime.spawn_owned_task
    calls, workers = [], []

    def first_fails(self, *args, **kwargs):
        calls.append(self)
        if len(calls) == 1:
            raise ValueError("S04_SYNTHETIC_SNAPSHOT_CANARY")
        return load(self, *args, **kwargs)

    def spawn_spy(self, coroutine_factory, *, kind):
        if kind == "daemon-worker-parent":
            workers.append(self)
        return spawn(self, coroutine_factory, kind=kind)

    monkeypatch.setattr(SimpleGraph, "load_snapshot", first_fails)
    monkeypatch.setattr(CMMSRuntime, "spawn_owned_task", spawn_spy)
    with pytest.raises(LeaseError):
        async with daemon.acquire(ctx()):
            pass
    assert daemon._entries["alpha"].bundle is None and not workers
    async with daemon.acquire(ctx()) as session:
        assert session.runtime is workers[0]
    assert len(workers) == 1 and len(calls) == 2


@pytest.mark.asyncio
async def test_disable_cas_active_holder_then_enable_fresh_runtime(setup):
    daemon, registry, store, _, _ = setup
    ready, release = asyncio.Event(), asyncio.Event()
    sessions = []

    async def holder():
        async with daemon.acquire(ctx()) as session:
            sessions.append(session)
            ready.set()
            await release.wait()
            assert session.runtime.state == "READY"

    task = asyncio.create_task(holder())
    await ready.wait()
    record = await registry.set_enabled(principal("alpha"), "alpha", enabled=False, expected_generation=1)
    assert record.generation == 2
    assert await daemon.stop_profile("alpha") is None
    with pytest.raises(LeaseError) as exc:
        async with daemon.acquire(ctx()):
            pass
    assert exc.value.code == "E_PROFILE_DISABLED"
    release.set()
    await task
    assert sessions[0].runtime.state == "CLOSED"
    record = await registry.set_enabled(principal("alpha"), "alpha", enabled=True, expected_generation=2)
    assert record.generation == 3
    async with daemon.acquire(ctx()) as fresh:
        assert fresh.runtime is not sessions[0].runtime
    assert [row["desired_state"] for row in store.journal(profile_ids=["alpha"])] == ["disabled", "enabled"]


@pytest.mark.asyncio
async def test_shutdown_reverse_idempotent_partial_report(setup, monkeypatch):
    daemon, *_ = setup
    sessions = []
    # Reverse creation order differs from the manager's alphabetical order.
    for name in ("alpha", "beta"):
        async with daemon.acquire(ctx(name)) as session:
            sessions.append(session)
    order = []
    stop = daemon.manager.stop_profile

    async def spy(name):
        order.append(name)
        return await stop(name)

    monkeypatch.setattr(daemon.manager, "stop_profile", spy)
    original = sessions[1].runtime.dependencies.sqlite.close

    async def fail():
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr(sessions[1].runtime.dependencies.sqlite, "close", fail)
    report = await daemon.close_all()
    assert order == ["beta", "alpha"]
    assert not report.complete and report.error_code is not None
    assert report.outcomes[0].retained and not report.outcomes[1].retained
    assert sessions[0].runtime.state == "CLOSED"
    assert await daemon.close_all() is report
    assert order == ["beta", "alpha"]
    monkeypatch.setattr(sessions[1].runtime.dependencies.sqlite, "close", original)
    await stop("beta")


@pytest.mark.asyncio
async def test_acquire_shutdown_race_never_resurrects(setup, monkeypatch):
    daemon, *_ = setup
    entered, release = asyncio.Event(), asyncio.Event()
    compose = ProfileBundle.compose.__func__

    async def blocked(cls, runtime, policies):
        entered.set()
        await release.wait()
        return await compose(cls, runtime, policies)

    monkeypatch.setattr(ProfileBundle, "compose", classmethod(blocked))

    async def caller():
        with pytest.raises(LeaseError) as exc:
            async with daemon.acquire(ctx()):
                pytest.fail("must not publish after shutdown begins")
        assert exc.value.code == "E_RUNTIME_CLOSING"

    acquire = asyncio.create_task(caller())
    await entered.wait()
    close = asyncio.create_task(daemon.close_all())
    # Event handshake, not a scheduling sleep.
    await asyncio.wait_for(asyncio.shield(asyncio.create_task(_closing(daemon))), 5)
    with pytest.raises(LeaseError) as exc:
        async with daemon.acquire(ctx()):
            pass
    assert exc.value.code == "E_RUNTIME_CLOSING"
    release.set()
    await acquire
    assert (await close).complete
    assert all(entry.factory.runtime.state == "CLOSED" for entry in daemon.manager._entries)


async def _closing(daemon):
    if not daemon._closing:
        # A scheduled callback explicitly observes close's admission transition.
        done = asyncio.get_running_loop().create_future()
        asyncio.get_running_loop().call_soon(done.set_result, None)
        await done
    assert daemon._closing


@pytest.mark.parametrize("target,who,code", [
    ("beta", principal("alpha"), "E_PERMISSION_DENIED"),
    ("unknown", principal("unknown"), "E_PROFILE_NOT_FOUND"),
    (None, principal("alpha"), "E_VALIDATION"),
])
@pytest.mark.asyncio
async def test_scoped_redacted_errors(setup, caplog, target, who, code):
    daemon, *_ = setup
    with pytest.raises(LeaseError) as exc:
        async with daemon.acquire(ctx(target, who)):
            pass
    assert exc.value.code == code
    assert str(exc.value) == code
    assert who.token_id not in str(exc.value) + caplog.text
    assert not daemon.manager._entries


@pytest.mark.asyncio
async def test_frozen_registry_binding_not_ambient_reresolution(setup, monkeypatch):
    daemon, registry, _, bindings, _ = setup
    resolve = registry.resolve
    seen = []
    get = daemon.manager.get

    async def resolved(who, name):
        result = await resolve(who, name)
        # An ambient resolver must never run after this point.
        monkeypatch.setenv("MEMORY_SERVER_DATA_ROOT", str(bindings[1].layout.data_root))
        return result

    async def admit(descriptor, *, layout, config, **kwargs):
        seen.append((layout, config))
        return await get(descriptor, layout=layout, config=config, **kwargs)

    monkeypatch.setattr(registry, "resolve", resolved)
    monkeypatch.setattr(daemon.manager, "get", admit)
    async with daemon.acquire(ctx()) as session:
        assert session.runtime.root == bindings[0].layout.data_root
    assert all(layout.data_root == bindings[0].layout.data_root and config is bindings[0].config
               for layout, config in seen)


@pytest.mark.parametrize("extra", [[], ["--enable-mcp-http"]])
def test_cli_subprocess_no_bind_no_listen(tmp_path, extra):
    events = tmp_path/"socket-events"
    code = '''
import sys
from pathlib import Path
path=Path(sys.argv[1])
def audit(event,args):
    if event in {"socket.bind", "socket.listen"}:
        with path.open("a") as out: out.write(event+"\\n")
sys.addaudithook(audit)
from memory_server.cli import app
sys.argv=["memory-server", "daemon", "--http", "127.0.0.1:18084",
          "--registry", "synthetic.yaml", "--control-path", "synthetic.sqlite", *sys.argv[2:]]
app()
'''
    result = subprocess.run([sys.executable, "-c", code, str(events), *extra],
                            capture_output=True, text=True, env=dict(os.environ), timeout=20)
    assert result.returncode == 2, result.stderr
    assert "E_CAPABILITY_UNAVAILABLE" in result.stderr
    assert not events.exists() or not events.read_text()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 18084))


@pytest.mark.asyncio
async def test_second_process_real_ex_lock_refused(setup, tmp_path):
    daemon, *_ = setup
    async with daemon.acquire(ctx()) as session:
        lock_files = {path: (path.stat().st_ino, path.read_bytes()) for path in session.runtime.root.glob("*lock*")}
        assert lock_files
        code = '''
import asyncio, sys
from pathlib import Path
from memory_server.application.profile_bundle import BundlePolicies
from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
from memory_server.control.store import ControlStore
from memory_server.runtime.daemon import Daemon, LeaseError
from memory_server.runtime.runtime_manager import RuntimeManager
from memory_server.settings import Settings
from tests.gateway.test_daemon import ctx, factory, make_binding
async def main():
    root = Path(sys.argv[1])
    settings = Settings(_env_file=None)
    binding = make_binding(root, "alpha", settings)
    store = ControlStore(root.parent/"second-control")
    daemon = Daemon(ProfileRegistry(store, RegistryConfiguration((binding,))),
                    policies=BundlePolicies.capture(settings), manager=RuntimeManager(factory_builder=factory))
    try:
        async with daemon.acquire(ctx()):
            return 0
    except LeaseError as exc:
        print(exc.code)
        return 2
    finally:
        await daemon.close_all()
        store.close()
raise SystemExit(asyncio.run(main()))
'''
        process = await asyncio.create_subprocess_exec(sys.executable, "-c", code, str(session.runtime.root),
                                                      stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()
        assert process.returncode == 2, stderr.decode()
        assert b"E_WRITER_ACTIVE" in stdout
        assert all(path.stat().st_ino == inode and path.read_bytes() == data
                   for path, (inode, data) in lock_files.items())
