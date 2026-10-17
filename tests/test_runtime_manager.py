"""Slice A manager contracts over the real factory/runtime/lock boundary."""

import asyncio
import os
from dataclasses import replace
from pathlib import Path

import pytest

from memory_server.paths import (
    SQLiteLocation,
    StorageLayout,
    ValueOrigin,
    VectorLocation,
)
from memory_server.runtime.config import RuntimeConfig
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
