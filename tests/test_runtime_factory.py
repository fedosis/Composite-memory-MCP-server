"""C03 factory: ordered lazy recipes, borrowed engine, partial rollback.

Boundaries crossed: ``runtime_factory.py`` recipes (C03), ``runtime/runtime.py``
(construction ledger + reverse rollback), ``sqlite_provider.py:97-190`` (real
engine + real canonical schema), ``storage/outbox_worker.py:67-143`` (borrowed
engine; ``close()`` disposes it), ``storage_lock.py`` (real EX before the first
constructor). This module also owns the shared fixture/helpers of the three C03
test modules; every store is created under TMPDIR-derived scratch.
"""
from __future__ import annotations

import builtins
import os
import sqlite3
import sys
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from memory_server.paths import StorageResolutionInputs, resolve_storage_layout
from memory_server.runtime.dependencies import RuntimeDependencies
from memory_server.storage_lock import inspect_existing_lock_readonly

BLOCKED_OPTIONAL = {"lancedb", "numpy", "pyarrow", "qdrant_client", "sentence_transformers"}
RECIPE_NAMES = ["sqlite", "graph", "embedder", "vector", "outbox_worker"]
SCRUBBED = (
    "MEMORY_SERVER_DB_URL MEMORY_SERVER_DATA_ROOT MEMORY_SERVER_STORAGE_MODE MEMORY_SERVER_VECTOR_BACKEND "
    "MEMORY_SERVER_LANCEDB_PATH MEMORY_SERVER_GRAPH_SNAPSHOT_PATH MEMORY_SERVER_QDRANT_LOCATION MEMORY_QDRANT_URL "
    "MEMORY_SERVER_EMBEDDING_MODEL"
).split()


def factory_api():
    from memory_server.runtime.runtime_factory import RuntimeFactory

    return RuntimeFactory


def coherent_layout(tmp_path, name="root", **overrides):
    """Coherent standalone layout: every store resolves inside ``root``."""
    root = tmp_path / name
    (root / "data").mkdir(parents=True, exist_ok=True)  # deployment creates store parents
    values = dict(
        mode="standalone", data_root=str(root), sqlite_url="sqlite+aiosqlite:///data/memory.db",
        vector_backend="lancedb", lancedb_path="data/lancedb", graph_snapshot_path="data/graph.json",
    )
    values.update(overrides)
    return resolve_storage_layout(StorageResolutionInputs(**values))


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    """Scratch-only environment: no live root, cache, config, credential or endpoint."""
    scratch = Path(os.environ["TMPDIR"]).resolve().parent
    assert tmp_path.resolve().is_relative_to(scratch)
    for name in ("HOME", "HERMES_HOME", "TMPDIR"):
        assert Path(os.environ[name]).resolve().is_relative_to(scratch)
    for key in SCRUBBED:
        monkeypatch.delenv(key, raising=False)
    from memory_server import settings

    settings.get_settings.cache_clear()
    monkeypatch.chdir(tmp_path)
    yield
    settings.get_settings.cache_clear()


class Resource:
    """Close-order double; allowed only at the failure/close edge."""

    def __init__(self, name, events, fail=False):
        self.name, self.events, self.fail, self.closed = name, events, fail, False

    async def close(self):
        self.events.append(self.name)
        if self.fail:
            raise RuntimeError("close failure")
        self.closed = True


def _tables(path):
    with sqlite3.connect(str(path)) as connection:
        return {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def block_optional(monkeypatch):
    """Block the optional roots at the import boundary (tests/test_ping.py:20-53)."""
    original_import = builtins.__import__

    def guarded(name, globals=None, locals=None, fromlist=(), level=0):
        if name.split(".", 1)[0] in BLOCKED_OPTIONAL:
            raise ImportError(f"blocked optional dependency: {name}")
        return original_import(name, globals, locals, fromlist, level)

    removed = {key: sys.modules.pop(key) for key in list(sys.modules) if key.split(".")[0] in BLOCKED_OPTIONAL}
    monkeypatch.setattr(builtins, "__import__", guarded)
    return removed


def imported_optional():
    return {name.split(".")[0] for name in sys.modules if name.split(".")[0] in BLOCKED_OPTIONAL}


async def test_factory_recipes_are_lazy_and_optional_imports_are_deferred(tmp_path, monkeypatch):
    removed = block_optional(monkeypatch)
    try:
        assert imported_optional() == set()
        layout = coherent_layout(tmp_path)
        factory = factory_api().create(layout)
        assert factory.config.layout is layout and imported_optional() == set()  # create(): no import
        runtime = factory.runtime
        await runtime.start()
        assert runtime.started and imported_optional() == set()  # construction imports none either
        with pytest.raises(ImportError, match="blocked optional dependency: lancedb"):
            await runtime.dependencies.vector._get_db()  # the recipe deferred the optional root
        await runtime.close()
        qdrant = coherent_layout(tmp_path, name="qdrant-root", vector_backend="qdrant", qdrant_location=":memory:")
        qfactory = factory_api().create(qdrant)
        with pytest.raises(ImportError, match="blocked optional dependency: qdrant_client"):
            await qfactory.runtime.start()
        assert qfactory.built_names == ("sqlite", "graph", "embedder")
        assert qfactory.runtime.state == "FAILED" and qfactory.runtime.dependencies.vector is None
    finally:
        sys.modules.update(removed)


async def test_factory_builds_real_sqlite_engine_and_borrows_it_for_outbox(tmp_path, monkeypatch):
    import storage.outbox_worker as worker_module

    import memory_server.providers.sqlite_provider as sqlite_module

    def forbidden(self):
        raise AssertionError("OutboxWorker.close() disposes the engine it borrows")

    monkeypatch.setattr(worker_module.OutboxWorker, "close", forbidden)
    layout = coherent_layout(tmp_path)
    runtime = factory_api().create(layout).runtime
    await runtime.start()
    sqlite, worker = runtime.dependencies.sqlite, runtime.dependencies.outbox_worker
    assert worker._engine is sqlite.engine and sqlite.engine is not None
    assert "outbox_worker" not in runtime.dependencies.owns_close
    assert runtime.dependencies.borrowed_engine is True
    db_path = Path(layout.sqlite.local_path)
    assert db_path.exists() and {"facts", "receipts", "outbox_entries"} <= _tables(db_path)
    closes, original_close = [], sqlite_module.SQLiteProvider.close

    async def counting_close(self):
        closes.append(self)
        await original_close(self)

    monkeypatch.setattr(sqlite_module.SQLiteProvider, "close", counting_close)
    report = await runtime.close()
    assert report.code is None and closes == [sqlite]  # disposed exactly once, by its owner
    assert "sqlite" in report.phases and report.retained_resources == ()
    assert inspect_existing_lock_readonly(layout.data_root).availability == "available"


async def test_factory_partial_failure_closes_only_successful_steps_in_reverse(tmp_path):
    events = []

    def failing(rt):
        raise ValueError("vector construction failed")

    factory = factory_api().create(
        coherent_layout(tmp_path),
        overrides={
            "graph": lambda rt: Resource("graph", events),
            "embedder": lambda rt: Resource("embedder", events),
            "vector": failing,
        },
    )
    runtime = factory.runtime
    with pytest.raises(ValueError, match="vector construction failed") as caught:
        await runtime.start()
    assert events == ["embedder", "graph"]  # reverse order, successful steps only
    assert factory.built_names == ("sqlite", "graph", "embedder")
    report = caught.value.cleanup_report
    assert report.code is None and report.retained_resources == () and report.lock_retained is False
    assert runtime.state == "FAILED" and runtime.lock is None
    assert inspect_existing_lock_readonly(runtime.layout.data_root).availability == "available"


async def test_factory_partial_cleanup_failure_retains_handles_and_reports(tmp_path):
    events = []
    double = Resource("vector", events, fail=True)

    def failing(rt):
        raise ValueError("outbox construction failed")

    factory = factory_api().create(
        coherent_layout(tmp_path), overrides={"vector": lambda rt: double, "outbox_worker": failing}
    )
    runtime = factory.runtime
    with pytest.raises(ValueError, match="outbox construction failed") as caught:
        await runtime.start()
    report = caught.value.cleanup_report
    assert report.code == "E_RUNTIME_CLEANUP_FAILED"
    assert report.retained_resources == ("sqlite", "graph", "embedder", "vector")
    assert runtime.state == "FAILED" and runtime.dependencies.vector is double and double.closed is False
    assert events == ["vector"] and factory.built_names == ("sqlite", "graph", "embedder", "vector")


def test_factory_container_is_immutable_after_create(tmp_path):
    dependencies = RuntimeDependencies()
    with pytest.raises(FrozenInstanceError):
        dependencies.sqlite = object()
    replaced = dependencies.with_resource("sqlite", object())
    assert replaced is not dependencies and dependencies.sqlite is None and replaced.sqlite is not None
    with pytest.raises(ValueError, match="unknown dependency"):
        dependencies.with_resource("not_a_resource", object())
    layout = coherent_layout(tmp_path)
    factory = factory_api().create(layout, dependencies=dependencies)
    snapshot = factory.config
    with pytest.raises(TypeError):
        snapshot.outbox["max_retries"] = 11
    with pytest.raises(FrozenInstanceError):
        snapshot.vector_size = 8
    runtime = factory.runtime
    assert runtime.config is snapshot and runtime.state == "NEW"
    assert [name for name, _ in runtime.dependencies.recipes] == RECIPE_NAMES
    assert factory.built == () and factory.built_names == ()
    # create() is zero-I/O: no lock entry and no store, only the pre-made parent dir.
    assert {path.name for path in layout.data_root.iterdir()} == {"data"}
    assert not layout.root_lock_path.exists() and not (layout.data_root / "data" / "memory.db").exists()


async def test_factory_injects_spawner_and_submitter_into_runtime_worker_and_lancedb(tmp_path):
    runtime = factory_api().create(coherent_layout(tmp_path)).runtime
    await runtime.start()
    worker, vector = runtime.dependencies.outbox_worker, runtime.dependencies.vector
    assert worker._task_spawner is runtime.spawn_owned_task
    assert worker._native_submitter is runtime.submit_native
    assert vector._native_submitter is runtime.submit_native
    await runtime.close()
async def test_factory_runtime_worker_missing_submitter_fails_startup(tmp_path):
    from storage.outbox_worker import OutboxWorker
    from memory_server.router.graph_router import GraphRouter
    async def bare(rt):
        worker = OutboxWorker(engine=rt.dependencies.sqlite.engine, qdrant=rt.dependencies.vector, embedder=rt.dependencies.embedder, graph_router=GraphRouter(graph=rt.dependencies.graph, max_path_depth=4))
        await worker.initialize()
        return worker
    factory = factory_api().create(coherent_layout(tmp_path), overrides={"outbox_worker": bare})
    with pytest.raises(RuntimeError, match="missing task_spawner/native_submitter"):
        await factory.runtime.start()
    assert factory.runtime.state == "FAILED"
async def test_factory_runtime_lancedb_missing_submitter_fails_startup(tmp_path):
    from memory_server.providers.lancedb_provider import LanceDBProvider
    factory = factory_api().create(coherent_layout(tmp_path), overrides={"vector": lambda rt: LanceDBProvider(db_path=str(rt.config.vector_local_path), table=rt.config.vector_collection, metric=rt.config.vector_metric, vector_size=rt.config.vector_size)})
    with pytest.raises(RuntimeError, match="missing native_submitter"):
        await factory.runtime.start()
    assert factory.runtime.state == "FAILED"
async def test_factory_qdrant_recipe_does_not_require_lancedb_submitter(tmp_path, monkeypatch):
    removed = block_optional(monkeypatch)
    try:
        layout = coherent_layout(tmp_path, name="qdrant-root", vector_backend="qdrant", qdrant_location=":memory:")
        factory = factory_api().create(layout)
        with pytest.raises(ImportError, match="blocked optional dependency: qdrant_client"):
            await factory.runtime.start()
        assert factory.runtime.state == "FAILED"
        assert factory.runtime.dependencies.vector is None
    finally:
        sys.modules.update(removed)
