"""Slice A manager contracts over the real factory/runtime/lock boundary."""

import asyncio
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from memory_server.paths import (
    SQLiteLocation,
    StorageLayout,
    ValueOrigin,
    VectorLocation,
)
from memory_server.runtime.config import RuntimeConfig
from memory_server.runtime.runtime import ShutdownReport
from memory_server.runtime.runtime_factory import RECIPE_ORDER, RuntimeFactory
from memory_server.runtime.runtime_manager import RuntimeDescriptor, RuntimeManager, RuntimeManagerError


def layout(root: Path, sqlite_path: Path | None = None) -> StorageLayout:
    origin = ValueOrigin("default")
    sqlite = SQLiteLocation("", "", sqlite_path, "file" if sqlite_path else "memory", "", origin)
    return StorageLayout(
        "standalone", None, root, sqlite,
        VectorLocation("lancedb", None, None, "memories", "memory", origin),
        root / "graph.json", root / "graph.json.lock", root / ".cmms-storage.lock", {}, (), frozenset()
    )


def config_for(value: StorageLayout, busy: int = 5000) -> RuntimeConfig:
    return RuntimeConfig(
        "", busy, "lancedb", None, "memories", "cosine", 3, ":memory:", 6333, False,
        value.graph_snapshot_path,
        {
            "provider": "none", "model": "m", "device": "cpu", "batch_size": 1,
            "openai_model": "m", "openai_vector_size": 3, "base_url": "",
        },
        {"max_retries": 1}, {"ttl_days": {"fact": 1}}, {"graph_max_path_depth": 2}, value,
    )


def request(root: Path, profile: str = "p", sqlite_path: Path | None = None, busy: int = 5000):
    value = layout(root, sqlite_path)
    config = config_for(value, busy)
    return RuntimeDescriptor.from_layout(profile, layout=value, config=config), value, config


def manager(calls, gate=None):
    async def slow(_runtime):
        await gate.wait()
        return None

    def build(value, **kwargs):
        calls.append(value)
        overrides = {name: (slow if name == "sqlite" and gate else (lambda _runtime: None)) for name in RECIPE_ORDER}
        return RuntimeFactory.create(value, overrides=overrides, **kwargs)

    return RuntimeManager(loop=asyncio.get_running_loop(), factory_builder=build)


class ControlledRuntime:
    def __init__(self, reports, gate=None):
        self.reports, self.gate = reports, gate
        self.state, self.started, self.close_calls = "NEW", False, 0
        self.close_started = asyncio.Event()

    async def start(self):
        self.state, self.started = "READY", True
        return self

    async def close(self):
        self.close_calls += 1
        self.state = "CLOSING"
        self.close_started.set()
        if self.gate is not None:
            await self.gate.wait()
        report = self.reports.pop(0)
        self.state = "CLOSED" if report.code in {None, "E_RUNTIME_FLUSH_FAILED"} else "FAILED"
        return report


def controlled_manager(runtimes, reports=None, gates=None):
    reports, gates = {key: list(value) for key, value in (reports or {}).items()}, gates or {}

    def build(value, **_):
        name = value.data_root.name; runtime = ControlledRuntime(reports.get(name, [ShutdownReport()]), gates.get(name))  # noqa: E702
        runtimes.setdefault(name, []).append(runtime); return SimpleNamespace(runtime=runtime)  # noqa: E702

    return RuntimeManager(loop=asyncio.get_running_loop(), factory_builder=build)


async def test_descriptor_equality_compares_full_config_without_mappingproxy_hash(tmp_path):
    first, value, config = request(tmp_path / "a")
    changed = replace(config, outbox={"max_retries": 2})
    second = RuntimeDescriptor.from_layout("p", layout=value, config=changed)
    assert first != second
    assert first == RuntimeDescriptor.from_layout("p", layout=value, config=config)
    assert first.__hash__ is None
    with pytest.raises(TypeError):
        hash(config)


async def test_same_descriptor_concurrent_get_returns_one_ready_instance(tmp_path):
    gate = asyncio.Event()
    calls = []
    manager_a = manager(calls, gate)
    descriptor, value, config = request(tmp_path / "a")
    first = asyncio.create_task(manager_a.get(descriptor, layout=value, config=config))
    await asyncio.sleep(0)
    second = asyncio.create_task(manager_a.get(descriptor, layout=value, config=config))
    await asyncio.sleep(0)
    assert len(calls) == 1
    gate.set()
    left, right = await asyncio.gather(first, second)
    assert left is right
    assert left.state == "READY"
    await left.close()


async def test_get_never_returns_starting_or_failed_runtime(tmp_path):
    calls = []
    manager_a = manager(calls)
    descriptor, value, config = request(tmp_path / "a")
    runtime = await manager_a.get(descriptor, layout=value, config=config)
    assert runtime.state == "READY"
    await runtime.close()

    def failing(value, **kwargs):
        async def fail(_runtime):
            raise RuntimeError("startup")
        overrides = {name: fail if name == "sqlite" else (lambda _runtime: None) for name in RECIPE_ORDER}
        return RuntimeFactory.create(value, overrides=overrides, **kwargs)

    failed_manager = RuntimeManager(loop=asyncio.get_running_loop(), factory_builder=failing)
    descriptor, value, config = request(tmp_path / "failed", profile="failed")
    with pytest.raises(RuntimeError):
        await failed_manager.get(descriptor, layout=value, config=config)
    with pytest.raises(RuntimeManagerError) as caught:
        await failed_manager.get(descriptor, layout=value, config=config)
    assert caught.value.code == "E_RUNTIME_START_FAILED"


async def test_conflicting_profile_config_is_rejected_before_factory_build(tmp_path):
    calls = []
    manager_a = manager(calls)
    descriptor, value, config = request(tmp_path / "a")
    await manager_a.get(descriptor, layout=value, config=config)
    changed = replace(config, sqlite_busy_timeout_ms=7000)
    with pytest.raises(RuntimeManagerError) as caught:
        await manager_a.get(descriptor, layout=value, config=changed)
    assert caught.value.code == "E_RUNTIME_DESCRIPTOR_CONFLICT"
    assert len(calls) == 1


async def test_parent_child_overlap_is_rejected_in_both_admission_orders(tmp_path):
    orders = (
        (tmp_path / "parent", tmp_path / "parent" / "child"),
        (tmp_path / "parent" / "child", tmp_path / "parent"),
    )
    for roots in orders:
        calls = []
        manager_a = manager(calls)
        first, value, config = request(roots[0], profile="first")
        runtime = await manager_a.get(first, layout=value, config=config)
        second, value, config = request(roots[1], profile="second")
        with pytest.raises(RuntimeManagerError) as caught:
            await manager_a.get(second, layout=value, config=config)
        assert caught.value.code == "E_RUNTIME_RESOURCE_ALIAS"
        assert len(calls) == 1
        await runtime.close()


async def test_physical_alias_is_rejected_without_following_links(tmp_path):
    source = tmp_path / "one" / "db.sqlite"
    source.parent.mkdir()
    source.write_text("db")
    link = tmp_path / "two" / "db.sqlite"
    link.parent.mkdir()
    link.symlink_to(source)
    calls = []
    manager_a = manager(calls)
    first, value, config = request(tmp_path / "one", "one", source)
    runtime = await manager_a.get(first, layout=value, config=config)
    second, value, config = request(tmp_path / "two", "two", link)
    with pytest.raises(RuntimeManagerError) as caught:
        await manager_a.get(second, layout=value, config=config)
    assert caught.value.code == "E_RUNTIME_RESOURCE_ALIAS"
    assert len(calls) == 1
    await runtime.close()


async def test_same_path_identity_change_is_rejected(tmp_path):
    database = tmp_path / "db.sqlite"
    database.write_text("one")
    calls = []
    manager_a = manager(calls)
    descriptor, value, config = request(tmp_path / "root", sqlite_path=database)
    runtime = await manager_a.get(descriptor, layout=value, config=config)
    database.unlink()
    database.mkdir()
    with pytest.raises(RuntimeManagerError) as caught:
        await manager_a.get(descriptor, layout=value, config=config)
    assert caught.value.code == "E_RUNTIME_RESOURCE_IDENTITY_CHANGED"
    await runtime.close()


async def test_cancelled_waiter_does_not_cancel_shared_start(tmp_path):
    gate = asyncio.Event()
    calls = []
    manager_a = manager(calls, gate)
    descriptor, value, config = request(tmp_path / "a")
    owner = asyncio.create_task(manager_a.get(descriptor, layout=value, config=config))
    await asyncio.sleep(0)
    waiter = asyncio.create_task(manager_a.get(descriptor, layout=value, config=config))
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    gate.set()
    runtime = await owner
    assert runtime.state == "READY"
    assert len(calls) == 1
    await runtime.close()


async def test_wrong_loop_access_is_rejected_deterministically(tmp_path):
    foreign = asyncio.new_event_loop()
    try:
        manager_a = RuntimeManager(loop=foreign)
        descriptor, value, config = request(tmp_path / "a")
        with pytest.raises(RuntimeManagerError) as caught:
            await manager_a.get(descriptor, layout=value, config=config)
        assert caught.value.code == "E_RUNTIME_MANAGER_WRONG_LOOP"
    finally:
        foreign.close()


async def test_manager_never_mutates_ambient_environment_or_settings_cache(tmp_path, monkeypatch):
    calls = []
    manager_a = manager(calls)
    descriptor, value, config = request(tmp_path / "a")
    monkeypatch.setenv("MEMORY_SERVER_DATA_ROOT", "/outside/ambient")
    before = os.environ["MEMORY_SERVER_DATA_ROOT"]
    runtime = await manager_a.get(descriptor, layout=value, config=config)
    assert os.environ["MEMORY_SERVER_DATA_ROOT"] == before
    assert runtime.layout.data_root == value.data_root
    await runtime.close()


async def test_stop_absent_profile_is_idempotent_without_resource_work(tmp_path):
    runtimes = {}
    manager_a = controlled_manager(runtimes)
    assert await manager_a.stop_profile("missing") is None
    assert runtimes == {}


async def test_stop_during_start_settles_without_late_ready_publication(tmp_path):
    gate, calls = asyncio.Event(), []
    manager_a = manager(calls, gate)
    descriptor, value, config = request(tmp_path / "a")
    owner = asyncio.create_task(manager_a.get(descriptor, layout=value, config=config))
    await asyncio.sleep(0)
    stopper = asyncio.create_task(manager_a.stop_profile("p"))
    report = await stopper
    result = await asyncio.gather(owner, return_exceptions=True)
    assert report.code is None
    assert not any(getattr(item, "state", None) == "READY" for item in result)


async def test_close_all_closes_admission_before_awaiting_profiles(tmp_path):
    gate, runtimes = asyncio.Event(), {}
    manager_a = controlled_manager(runtimes, gates={"a": gate})
    descriptor, value, config = request(tmp_path / "a", profile="a")
    await manager_a.get(descriptor, layout=value, config=config)
    closing = asyncio.create_task(manager_a.close_all())
    await runtimes["a"][0].close_started.wait()
    with pytest.raises(RuntimeManagerError) as caught:
        await manager_a.get(descriptor, layout=value, config=config)
    assert caught.value.code == "E_RUNTIME_MANAGER_CLOSED"
    gate.set()
    assert (await closing).complete


async def test_close_all_attempts_later_profiles_after_one_failure(tmp_path):
    unsafe = ShutdownReport(code="E_RUNTIME_CLEANUP_FAILED", retained_resources=("sqlite",))
    runtimes = {}
    manager_a = controlled_manager(runtimes, reports={"a": [ShutdownReport()], "b": [unsafe], "c": [ShutdownReport()]})
    for name in ("c", "a", "b"):
        descriptor, value, config = request(tmp_path / name, profile=name)
        await manager_a.get(descriptor, layout=value, config=config)
    report = await manager_a.close_all()
    assert [outcome.profile_id for outcome in report.outcomes] == ["a", "b", "c"]
    assert all(runtimes[name][0].close_calls == 1 for name in "abc")
    assert not report.complete


async def test_close_all_aggregate_order_is_deterministic_and_redacted(tmp_path):
    runtimes = {}; manager_a = controlled_manager(runtimes)  # noqa: E702
    for name in ("z", "m", "a"):
        descriptor, value, config = request(tmp_path / name, profile=name)
        await manager_a.get(descriptor, layout=value, config=config)
    report = await manager_a.close_all()
    assert tuple(item.profile_id for item in report.outcomes) == ("a", "m", "z")
    assert "password" not in repr(report)


async def test_close_all_waiter_cancellation_does_not_cancel_owned_cleanup(tmp_path):
    gate, runtimes = asyncio.Event(), {}
    manager_a = controlled_manager(runtimes, gates={"a": gate})
    descriptor, value, config = request(tmp_path / "a", profile="a")
    await manager_a.get(descriptor, layout=value, config=config)
    owner = asyncio.create_task(manager_a.close_all())
    await runtimes["a"][0].close_started.wait()
    waiter = asyncio.create_task(manager_a.close_all())
    await asyncio.sleep(0)
    assert not waiter.done()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    gate.set()
    assert (await owner).complete


async def test_unsafe_cleanup_retains_owner_and_denies_replacement(tmp_path):
    unsafe = ShutdownReport(code="E_RUNTIME_CLEANUP_FAILED", retained_resources=("sqlite",))
    runtimes = {}
    manager_a = controlled_manager(runtimes, reports={"a": [unsafe, ShutdownReport()]})
    descriptor, value, config = request(tmp_path / "a", profile="a")
    original = await manager_a.get(descriptor, layout=value, config=config)
    assert (await manager_a.stop_profile("a")).code == "E_RUNTIME_CLEANUP_FAILED"
    with pytest.raises(RuntimeManagerError) as caught:
        await manager_a.get(descriptor, layout=value, config=config)
    assert caught.value.code == "E_RUNTIME_CLEANUP_FAILED"
    assert (await manager_a.stop_profile("a")).code is None
    replacement = await manager_a.get(descriptor, layout=value, config=config)
    assert replacement is not original
    assert runtimes["a"][0].close_calls == 2


async def test_lock_release_uncertain_retains_owner_and_reservation(tmp_path):
    uncertain = ShutdownReport(code="E_LOCK_RELEASE_UNCERTAIN", lock_retained=True)
    runtimes = {}
    manager_a = controlled_manager(runtimes, reports={"a": [uncertain, ShutdownReport()]})
    descriptor, value, config = request(tmp_path / "a", profile="a")
    owner = await manager_a.get(descriptor, layout=value, config=config)
    report = await manager_a.stop_profile("a")
    assert report.lock_retained and runtimes["a"][0].state == "FAILED"
    with pytest.raises(RuntimeManagerError) as caught:
        await manager_a.get(descriptor, layout=value, config=config)
    assert caught.value.code == "E_LOCK_RELEASE_UNCERTAIN"
    await manager_a.stop_profile("a")
    assert await manager_a.get(descriptor, layout=value, config=config) is not owner


async def test_safe_flush_failure_is_reported_before_replacement(tmp_path):
    flushed = ShutdownReport(code="E_RUNTIME_FLUSH_FAILED", failed=1)
    runtimes = {}
    manager_a = controlled_manager(runtimes, reports={"a": [flushed, ShutdownReport()]})
    descriptor, value, config = request(tmp_path / "a", profile="a")
    original = await manager_a.get(descriptor, layout=value, config=config)
    report = await manager_a.stop_profile("a")
    replacement = await manager_a.get(descriptor, layout=value, config=config)
    assert report.code == "E_RUNTIME_FLUSH_FAILED"
    assert replacement is not original


async def test_repeated_stop_and_close_all_do_not_double_close_or_flush(tmp_path):
    runtimes = {}; manager_a = controlled_manager(runtimes)  # noqa: E702
    descriptor, value, config = request(tmp_path / "a", profile="a")
    await manager_a.get(descriptor, layout=value, config=config)
    first = await manager_a.stop_profile("a")
    second = await manager_a.stop_profile("a")
    aggregate = await manager_a.close_all()
    again = await manager_a.close_all()
    assert first is second
    assert aggregate is again and aggregate.complete
    assert runtimes["a"][0].close_calls == 1


async def test_independent_profile_stop_does_not_close_other_ready_profile(tmp_path):
    runtimes = {}; manager_a = controlled_manager(runtimes)  # noqa: E702
    for name in ("a", "b"):
        descriptor, value, config = request(tmp_path / name, profile=name)
        await manager_a.get(descriptor, layout=value, config=config)
    await manager_a.stop_profile("a")
    assert runtimes["a"][0].state == "CLOSED"
    assert runtimes["b"][0].state == "READY"
    assert runtimes["b"][0].close_calls == 0


async def test_real_subprocess_contender_is_blocked_until_verified_release(tmp_path):
    manager_a = manager([])
    descriptor, value, config = request(tmp_path / "a", profile="a")
    await manager_a.get(descriptor, layout=value, config=config)
    lock = value.root_lock_path
    before = lock.stat().st_ino
    code = "from pathlib import Path; import sys; from memory_server.storage_lock import RuntimeStorageLock; print('ready', flush=True); owner=RuntimeStorageLock.acquire_exclusive(Path(sys.argv[1]), timeout=3); owner.release_exclusive(); print('acquired', flush=True)"  # noqa: E501
    contender = subprocess.Popen([sys.executable, "-c", code, str(value.data_root)], stdout=subprocess.PIPE, text=True)
    assert contender.stdout.readline().strip() == "ready"
    assert contender.poll() is None
    await manager_a.stop_profile("a")
    stdout, _ = contender.communicate(timeout=5)
    assert contender.returncode == 0 and "acquired" in stdout
    assert lock.stat().st_ino == before


@pytest.mark.parametrize("shape", ["symlink", "hardlink"])
@pytest.mark.parametrize(
    "resource", ["sqlite", "-wal", "-shm", "-journal", "vector", "graph", "graph_lock", "root_lock"]
)
async def test_writable_resource_alias_still_rejected_before_factory(tmp_path, resource, shape):
    root = tmp_path / "root"
    root.mkdir()
    value = layout(root, root / "db.sqlite")
    value = replace(value, vector=replace(value.vector, kind="local", local_path=root / "vector"))
    paths = {
        "sqlite": value.sqlite.local_path,
        "vector": value.vector.local_path,
        "graph": value.graph_snapshot_path,
        "graph_lock": value.graph_lock_path,
        "root_lock": value.root_lock_path,
        **{suffix: Path(str(value.sqlite.local_path) + suffix) for suffix in ("-wal", "-shm", "-journal")},
    }
    target = tmp_path / "external"
    target.write_bytes(b"untouched")
    path = paths[resource]
    if shape == "symlink":
        path.symlink_to(target)
    else:
        os.link(target, path)
        assert path.lstat().st_nlink == 2
    config = config_for(value)
    descriptor = RuntimeDescriptor.from_layout("alias", layout=value, config=config)
    calls = []
    manager_a = manager(calls)
    with pytest.raises(RuntimeManagerError) as caught:
        await manager_a.get(descriptor, layout=value, config=config)
    assert caught.value.code == "E_RUNTIME_RESOURCE_ALIAS"
    assert calls == []
    assert target.read_bytes() == b"untouched"
    if resource != "root_lock":
        assert not value.root_lock_path.exists()


@pytest.mark.parametrize("projection", ["vector", "graph"])
async def test_resolver_unavailable_projection_never_resolved_or_opened(tmp_path, monkeypatch, projection):
    import builtins
    import io

    from memory_server.paths import StorageResolutionInputs, resolve_storage_layout
    from memory_server.providers import graph_provider, lancedb_provider

    home = tmp_path / "home"
    root = home / "data"
    root.mkdir(parents=True)
    external = tmp_path / "external"
    external.mkdir()
    sentinel = external / "sentinel"
    sentinel.write_bytes(b"untouched")
    link = root / ("lancedb" if projection == "vector" else "graph.json")
    link.symlink_to(external, target_is_directory=True)
    value = resolve_storage_layout(StorageResolutionInputs(mode="profile", profile_home=home))
    assert value.unavailable_projections == frozenset({projection})
    assert f"legacy-split-layout:{projection}" in value.compatibility
    config = replace(config_for(value), sqlite_url=value.sqlite.effective_url)
    descriptor = RuntimeDescriptor.from_layout("degraded", layout=value, config=config)
    blocked = [link, external]
    if projection == "graph":
        blocked.append(value.graph_lock_path)
    accesses, constructors = [], []

    def guard(original):
        def checked(path, *args, **kwargs):
            if isinstance(path, (str, bytes, os.PathLike)):
                candidate = Path(os.path.abspath(os.fsdecode(path)))
                if any(candidate == item or item in candidate.parents for item in blocked):
                    accesses.append(candidate)
                    raise AssertionError(f"unavailable projection resolved/opened: {candidate}")
            return original(path, *args, **kwargs)
        return checked

    def forbidden_constructor(*args, **kwargs):
        constructors.append((args, kwargs))
        raise AssertionError("unavailable projection constructor called")

    module, name = (lancedb_provider, "LanceDBProvider") if projection == "vector" else (graph_provider, "SimpleGraph")
    monkeypatch.setattr(module, name, forbidden_constructor)
    real_io_open = io.open
    monkeypatch.setattr(builtins, "open", guard(builtins.open))
    monkeypatch.setattr(io, "open", guard(io.open))
    monkeypatch.setattr(os, "open", guard(os.open))
    monkeypatch.setattr(Path, "resolve", guard(Path.resolve))

    def build(value, **kwargs):
        # Keep the unavailable projection's REAL recipe and real SQLite/lock.
        other = "graph" if projection == "vector" else "vector"
        return RuntimeFactory.create(value, overrides={"embedder": lambda _: None, other: lambda _: None}, **kwargs)

    manager_a = RuntimeManager(factory_builder=build)
    try:
        runtime = await manager_a.get(descriptor, layout=value, config=config)
        assert runtime.started
        assert getattr(runtime.dependencies, projection) is None
        assert runtime.dependencies.outbox_worker is None
        assert await manager_a.get(descriptor, layout=value, config=config) is runtime
    finally:
        await manager_a.close_all()
    assert accesses == []
    assert constructors == []
    # Use the saved reader only for post-shutdown sentinel inspection.
    with real_io_open(sentinel, "rb") as stream:
        assert stream.read() == b"untouched"
    assert link.is_symlink()
