"""C03 O2 layout eligibility: refusal before the lock and before any constructor.

Boundaries crossed: ``paths.py:314-350`` (legacy split compatibility),
``paths.py:361-369`` (Qdrant classification), ``storage_lock.py:262-300`` (real EX
on the real inode), ``sqlite_provider.py:97-120`` (real engine + real out-of-root
store), ``runtime/runtime.py:ensure_layout_eligible`` and
``runtime/runtime_factory.py``. The shared fixture/helpers live in
``tests/test_runtime_factory.py``.

C03 entry points are resolved through ``_eligibility``/``factory_api`` because
they do not exist before C03; the committed RED of
``test_writable_split_layout_is_rejected_before_lock_and_construction`` reaches a
behavioural failure on base ("DID NOT RAISE") after proving the legacy path really
opened the out-of-root store. Every path stays inside TMPDIR-derived scratch.
"""
from __future__ import annotations

import sys

import pytest

from memory_server.paths import (
    StorageLayoutError,
    StorageResolutionInputs,
    resolve_storage_layout,
    serialize_layout_redacted,
)
from memory_server.runtime.runtime import CMMSRuntime
from memory_server.storage_lock import RuntimeStorageLock, inspect_existing_lock_readonly
from tests.test_runtime_factory import coherent_layout, factory_api, isolated  # noqa: F401

ABS_SQLITE = "sqlite+aiosqlite:///"


def _eligibility():
    from memory_server.runtime.runtime import ensure_layout_eligible

    return ensure_layout_eligible


def _spy(calls, name):
    def build(*args, **kwargs):
        calls.append(name)
        raise AssertionError(f"provider constructor invoked on a refused layout: {name}")

    return build


def _split(tmp_path):
    """Legacy standalone split: root is the CWD; the SQLite store lives outside it."""
    outside = tmp_path.parent / "out-of-root"
    outside.mkdir(exist_ok=True)
    root = tmp_path / "split-root"
    root.mkdir(exist_ok=True)
    db = outside / f"{tmp_path.name}.db"
    layout = resolve_storage_layout(
        StorageResolutionInputs(
            mode="standalone", data_root=None, sqlite_url=f"{ABS_SQLITE}{db}",
            lancedb_path=str(root / "data/lancedb"), graph_snapshot_path=str(root / "data/graph.json"),
        )
    )
    return layout, db


async def _legacy_opens_out_of_root_store(layout, db_path):
    """Boundary: the legacy standalone recipe really opens the split store."""
    from memory_server.providers.sqlite_provider import SQLiteProvider

    provider = SQLiteProvider(url=layout.sqlite.effective_url, busy_timeout_ms=5000)
    await provider.initialize()
    await provider.close()
    return db_path.exists()


async def test_writable_split_layout_is_rejected_before_lock_and_construction(tmp_path):
    layout, outside_db = _split(tmp_path)
    assert "legacy-split-layout:sqlite" in layout.compatibility
    assert layout.sqlite.local_path == outside_db
    # Baseline contrast: the legacy standalone path accepts the split layout and
    # really opens the out-of-root store (paths.py:314-350 + real engine).
    assert await _legacy_opens_out_of_root_store(layout, outside_db)
    never_opened = tmp_path.parent / "out-of-root" / "runtime-never-opened.db"
    runtime = CMMSRuntime.create(layout)
    with pytest.raises(StorageLayoutError) as caught:
        await runtime.start()
    assert caught.value.code == "E_RUNTIME_WRITABLE_SPLIT_LAYOUT"
    assert runtime.lock is None and runtime.state != "READY"
    assert not layout.root_lock_path.exists() and not never_opened.exists()
    factory = factory_api().create(layout)  # the factory drives the same refusal
    with pytest.raises(StorageLayoutError) as via_factory:
        await factory.runtime.start()
    assert via_factory.value.code == "E_RUNTIME_WRITABLE_SPLIT_LAYOUT"
    assert factory.built == () and factory.built_names == ()


async def test_ephemeral_projection_vector_is_not_treated_as_alias(tmp_path):
    layout = coherent_layout(tmp_path, vector_backend="qdrant", qdrant_location=":memory:")
    assert layout.vector.kind == "memory" and "ephemeral-projection:vector" in layout.compatibility
    _eligibility()(layout)  # no refusal: an in-memory projection aliases nothing
    factory = factory_api().create(layout)
    assert factory.config.vector_backend == "qdrant" and factory.degraded == ()


async def test_unavailable_symlink_projection_stays_unopened_and_degraded(tmp_path):
    home = tmp_path / "home"
    (home / "data").mkdir(parents=True)
    target = tmp_path / "elsewhere" / "lancedb"
    target.mkdir(parents=True)
    (home / "data" / "lancedb").symlink_to(target, target_is_directory=True)
    layout = resolve_storage_layout(StorageResolutionInputs(mode="profile", profile_home=home))
    assert "vector" in layout.unavailable_projections
    assert "legacy-split-layout:vector" in layout.compatibility
    _eligibility()(layout)  # a degraded, unopened projection is not over-rejected
    factory = factory_api().create(layout)
    assert factory.degraded == ("vector",)
    before = {name.split(".")[0] for name in sys.modules}
    runtime = factory.runtime
    await runtime.start()
    assert runtime.started and runtime.dependencies.vector is None
    assert {name.split(".")[0] for name in sys.modules} - before == set()
    assert list(target.iterdir()) == []  # the projection stayed unopened
    await runtime.close()


@pytest.mark.parametrize("mode", ["standalone", "shared"])
async def test_remote_qdrant_standalone_and_shared_fail_closed_before_construction(tmp_path, monkeypatch, mode):
    calls = []
    import memory_server.providers.sqlite_provider as sqlite_module

    monkeypatch.setattr(sqlite_module, "SQLiteProvider", _spy(calls, "SQLiteProvider"))
    values = dict(mode=mode, vector_backend="qdrant", qdrant_location="qdrant.example.invalid:6333")
    layout = coherent_layout(tmp_path, **values)
    assert layout.mode == mode and layout.vector.kind == "remote"
    with pytest.raises(StorageLayoutError) as direct:
        _eligibility()(layout)
    assert direct.value.code == "E_QDRANT_REMOTE_NAMESPACE_UNCONFIRMED"
    before = {name.split(".")[0] for name in sys.modules}
    with pytest.raises(StorageLayoutError) as caught:
        await factory_api().create(layout).runtime.start()
    assert caught.value.code == "E_QDRANT_REMOTE_NAMESPACE_UNCONFIRMED"
    assert calls == [] and not layout.root_lock_path.exists()
    assert {name.split(".")[0] for name in sys.modules} - before == set()


def test_remote_qdrant_profile_rejection_is_unchanged(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    with pytest.raises(StorageLayoutError) as caught:
        resolve_storage_layout(
            StorageResolutionInputs(
                mode="profile", profile_home=home, vector_backend="qdrant", qdrant_location="qdrant.invalid:6333"
            )
        )
    assert caught.value.code == "E_QDRANT_PROFILE_NAMESPACE_UNDEFINED"


@pytest.mark.parametrize(
    "sqlite_url", ["file:memdb1?mode=memory&cache=shared", "sqlite+aiosqlite:///data/memory.db?uri=true"]
)
async def test_ambiguous_file_uri_sqlite_is_rejected(tmp_path, sqlite_url):
    layout = coherent_layout(tmp_path, sqlite_url=sqlite_url)
    assert layout.sqlite.kind == "file_uri" and layout.sqlite.local_path is None
    with pytest.raises(StorageLayoutError) as caught:
        _eligibility()(layout)
    assert caught.value.code == "E_RUNTIME_AMBIGUOUS_SQL_URI"
    with pytest.raises(StorageLayoutError) as via_factory:
        await factory_api().create(layout).runtime.start()
    assert via_factory.value.code == "E_RUNTIME_AMBIGUOUS_SQL_URI"
    assert not layout.root_lock_path.exists()


async def test_coherent_profile_standalone_shared_layouts_are_accepted(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    layouts = [
        resolve_storage_layout(StorageResolutionInputs(mode="profile", profile_home=home)),
        coherent_layout(tmp_path),
        coherent_layout(tmp_path, mode="shared", data_root=str(tmp_path / "shared-root")),
    ]
    for layout in layouts:
        assert _eligibility()(layout) is None
    runtime = factory_api().create(layouts[1]).runtime
    await runtime.start()
    assert runtime.started and runtime.state == "READY"
    assert (tmp_path / "root" / "data" / "memory.db").exists()
    await runtime.close()
    assert runtime.state == "CLOSED"
    assert inspect_existing_lock_readonly(layouts[1].data_root).availability == "available"


async def test_eligibility_runs_before_lock_acquisition(tmp_path):
    layout, _ = _split(tmp_path)
    layout.data_root.mkdir(exist_ok=True)
    entry = layout.root_lock_path
    entry.write_bytes(b"sentinel-owner")
    before = entry.stat()
    runtime = factory_api().create(layout).runtime
    with pytest.raises(StorageLayoutError) as caught:
        await runtime.start()
    assert caught.value.code == "E_RUNTIME_WRITABLE_SPLIT_LAYOUT"
    after = entry.stat()
    assert entry.read_bytes() == b"sentinel-owner"
    assert (after.st_ino, after.st_size, after.st_mtime_ns) == (before.st_ino, before.st_size, before.st_mtime_ns)
    assert runtime.lock is None
    assert inspect_existing_lock_readonly(layout.data_root).availability == "available"


async def test_refusal_does_not_invoke_provider_constructors(tmp_path, monkeypatch):
    calls = []
    import memory_server.providers.graph_provider as graph_module
    import memory_server.providers.lancedb_provider as lancedb_module
    import memory_server.providers.sqlite_provider as sqlite_module

    for module, name in (
        (sqlite_module, "SQLiteProvider"),
        (lancedb_module, "LanceDBProvider"),
        (graph_module, "SimpleGraph"),
    ):
        monkeypatch.setattr(module, name, _spy(calls, name))
    layout, _ = _split(tmp_path)
    runtime = factory_api().create(layout).runtime
    with pytest.raises(StorageLayoutError):
        await runtime.start()
    assert calls == []
    assert runtime.dependencies.sqlite is None and runtime.state == "NEW"


def test_resolver_and_maintenance_inspection_remain_usable_on_refused_layouts(tmp_path):
    layout, outside_db = _split(tmp_path)
    with pytest.raises(StorageLayoutError) as caught:
        _eligibility()(layout)
    assert caught.value.code == "E_RUNTIME_WRITABLE_SPLIT_LAYOUT"
    assert layout.sqlite.local_path == outside_db
    redacted = serialize_layout_redacted(layout)
    assert redacted["mode"] == "standalone" and "legacy-split-layout:sqlite" in redacted["compatibility"]
    assert inspect_existing_lock_readonly(layout.data_root).availability in {"unknown", "available", "held"}
    lock = RuntimeStorageLock.acquire_exclusive(layout.data_root, timeout=0)
    try:
        assert inspect_existing_lock_readonly(layout.data_root).availability == "held"
    finally:
        lock.release_exclusive()
    assert inspect_existing_lock_readonly(layout.data_root).availability == "available"
    assert inspect_existing_lock_readonly(layout.data_root).error is None
