"""S02 real control SQLite and filesystem boundaries; no memory providers."""

from __future__ import annotations

import asyncio
import inspect
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from threading import Barrier

import pytest
from pydantic import ValidationError

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import Principal, ProfileBinding, ProfileRecord
from memory_server.paths import StorageResolutionInputs, resolve_storage_layout
from memory_server.runtime.config import RuntimeConfig
from memory_server.runtime.runtime_manager import RuntimeDescriptor
from memory_server.settings import Settings


def api():
    from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
    from memory_server.control.store import ControlError, ControlStore

    return ProfileRegistry, RegistryConfiguration, ControlError, ControlStore


def binding(root, name="alpha", enabled=True):
    layout = resolve_storage_layout(
        StorageResolutionInputs(
            mode="standalone",
            data_root=root,
            sqlite_url="sqlite+aiosqlite:///memory.db",
            lancedb_path="lancedb",
            graph_snapshot_path="graph.json",
            legacy_split_compat=False,
        )
    )
    config = RuntimeConfig.capture(layout, Settings())
    return ProfileBinding(
        d.Profile(
            id=name,
            display_name=name,
            description=None,
            enabled=enabled,
            generation=1,
            runtime_state="not_started",
            capabilities=(),
            storage_root=None,
        ),
        layout,
        config,
        RuntimeDescriptor.from_layout(name, layout=layout, config=config),
    )


def principal(*ids, manage=True):
    return Principal(
        "synthetic",
        "synthetic",
        None,
        frozenset(ids),
        frozenset({"profile.read", "profile.manage"} if manage else {"profile.read"}),
        1,
        datetime.now(timezone.utc) + timedelta(hours=1),
    )


@pytest.fixture
def setup(tmp_path):
    registry_cls, configuration, error, store_cls = api()
    roots = (tmp_path / "a", tmp_path / "b")
    for root in roots:
        root.mkdir()
    bindings = (binding(roots[0]), binding(roots[1], "beta"))
    store = store_cls(tmp_path / "control")
    registry = registry_cls(store, configuration(bindings))
    yield registry, store, error, bindings
    store.close()


@pytest.mark.asyncio
async def test_resolve_and_deep_immutability(setup):
    registry, _, _, bindings = setup
    result = await registry.resolve(principal("alpha"), "alpha")
    assert isinstance(result, ProfileBinding)
    assert result.layout.data_root == bindings[0].layout.data_root.resolve()
    with pytest.raises(FrozenInstanceError):
        result.layout = bindings[1].layout
    with pytest.raises(ValidationError):
        result.profile.enabled = False
    with pytest.raises(AttributeError):
        result.profile.capabilities.append("other")
    with pytest.raises(TypeError):
        result.layout.origins["bad"] = None
    with pytest.raises(TypeError):
        result.config.embedding["provider"] = "other"
    with pytest.raises(FrozenInstanceError):
        result.descriptor.profile_id = "beta"
    records = await registry.list(principal("alpha"))
    assert isinstance(records, tuple) and isinstance(records[0], ProfileRecord)
    with pytest.raises(TypeError):
        records[0].permissions_metadata["x"] = ("profile.manage",)


@pytest.mark.asyncio
async def test_scope_and_listing_no_disclosure(setup, caplog):
    registry, _, error, bindings = setup
    assert [r.id for r in await registry.list(principal("alpha"))] == ["alpha"]
    assert await registry.list(principal()) == ()
    for target in ("beta", "missing"):
        with pytest.raises(error) as exc:
            await registry.resolve(principal("alpha"), target)
        assert exc.value.code == "E_PERMISSION_DENIED"
        assert target not in str(exc.value)
        assert str(bindings[1].layout.data_root) not in str(exc.value)
    assert "beta" not in caplog.text and str(bindings[1].layout.data_root) not in caplog.text
    with pytest.raises(error) as exc:
        await registry.resolve(principal("missing"), "missing")
    assert exc.value.code == "E_PROFILE_NOT_FOUND"


@pytest.mark.parametrize(
    "bad",
    ["../beta", "%2f", "%2e", "%61lpha", "/alpha", "a/b", "a\\b", "..", "a\0", "a" * 65, "", "é"],
)
@pytest.mark.asyncio
async def test_ids_not_normalised(setup, bad):
    registry, _, error, _ = setup
    with pytest.raises(error) as exc:
        await registry.resolve(principal(bad), bad)
    assert exc.value.code == "E_VALIDATION"
    assert str(exc.value) == "Invalid control input"


@pytest.mark.asyncio
async def test_disabled_cas_metadata_journal_reopen(setup):
    registry, store, error, bindings = setup
    admin = principal("alpha")
    result = await registry.set_enabled(admin, "alpha", enabled=False, expected_generation=1)
    assert not result.enabled and result.generation == 2
    with pytest.raises(error) as exc:
        await registry.resolve(admin, "alpha")
    assert exc.value.code == "E_PROFILE_DISABLED"
    for _ in range(10):
        with pytest.raises(error) as exc:
            await registry.set_enabled(admin, "alpha", enabled=True, expected_generation=1)
        assert exc.value.code == "E_VERSION_CONFLICT"
    record = await registry.set_metadata(
        admin, "alpha", display_name="Display", description="Label", expected_generation=2
    )
    assert record.display_name == "Display" and record.generation == 3
    assert [(r["generation"], r["desired_state"]) for r in store.journal(profile_ids=["alpha"])] == [
        (2, "disabled"),
        (3, "disabled"),
    ]
    before = await registry.list(admin)
    path = store.root
    store.close()
    registry_cls, configuration, _, store_cls = api()
    for _ in range(2):
        with store_cls(path) as reopened:
            again = registry_cls(reopened, configuration(bindings))
            assert await again.list(admin) == before
            assert len(reopened.journal(profile_ids=["alpha"])) == 2


class _Token(int):
    """int subclass so identity, not equality, can be asserted on the bound parameter."""


def test_cas_predicate_binds_client_expectation(setup, monkeypatch):
    # If the UPDATE bound the row's own generation instead of the client's expectation,
    # the last parameter would be a plain int and this identity check would fail.
    registry, store, error, _ = setup

    class Recording:
        def __init__(self, inner):
            self.inner = inner
            self.calls = []

        def execute(self, sql, params=()):
            self.calls.append((sql, params))
            return self.inner.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self.inner, name)

    recorder = Recording(store._connection)
    monkeypatch.setattr(store, "_connection", recorder)
    token = _Token(1)
    store._mutate("alpha", expected_generation=token, operation="enabled", enabled=False)
    updates = [params for sql, params in recorder.calls if sql.lstrip().startswith("UPDATE profiles")]
    assert updates, "no UPDATE profiles statement was issued"
    assert updates[-1][-1] is token


def test_concurrent_cas_real_connections(setup):
    registry, store, error, bindings = setup
    registry_cls, configuration, _, store_cls = api()
    barrier = Barrier(2)

    def writer():
        with store_cls(store.root) as other:
            peer = registry_cls(other, configuration(bindings))
            barrier.wait(timeout=10)
            try:
                return asyncio.run(peer.set_enabled(principal("alpha"), "alpha", enabled=False, expected_generation=1))
            except error as exc:
                return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(writer) for _ in range(2)]
        results = [f.result(timeout=20) for f in futures]
    assert sum(isinstance(r, ProfileRecord) for r in results) == 1
    assert results.count("E_VERSION_CONFLICT") == 1
    assert len(store.journal(profile_ids=["alpha"])) == 1
    assert asyncio.run(registry.list(principal("alpha")))[0].generation == 2


@pytest.mark.parametrize("phase", ["after_row", "after_journal", "after_commit"])
def test_transaction_failure_reopen(setup, phase):
    registry, store, _, bindings = setup
    registry_cls, configuration, _, store_cls = api()

    def fail(current):
        if current == phase:
            raise RuntimeError("injected")

    store._checkpoint = fail
    with pytest.raises(RuntimeError, match="injected"):
        asyncio.run(registry.set_enabled(principal("alpha"), "alpha", enabled=False, expected_generation=1))
    path = store.root
    store.close()
    with store_cls(path) as reopened:
        peer = registry_cls(reopened, configuration(bindings))
        record = asyncio.run(peer.list(principal("alpha")))[0]
        committed = phase == "after_commit"
        assert record.generation == (2 if committed else 1)
        assert record.enabled is not committed
        assert len(reopened.journal(profile_ids=["alpha"])) == int(committed)


@pytest.mark.asyncio
async def test_permission_and_validation_leave_no_effect(setup):
    registry, store, error, _ = setup
    for actor, generation, enabled, code in [
        (principal("beta"), 1, True, "E_PERMISSION_DENIED"),
        (principal("alpha", manage=False), 1, True, "E_PERMISSION_DENIED"),
        (principal("alpha"), None, True, "E_PRECONDITION_REQUIRED"),
        (principal("alpha"), True, True, "E_VALIDATION"),
        (principal("alpha"), 0, True, "E_VALIDATION"),
        (principal("alpha"), 1, 1, "E_VALIDATION"),
    ]:
        with pytest.raises(error) as exc:
            await registry.set_enabled(actor, "alpha", enabled=enabled, expected_generation=generation)
        assert exc.value.code == code
    assert store.journal(profile_ids=["alpha"]) == ()


def test_configuration_only_no_root_mutator(setup):
    registry, store, _, _ = setup
    assert set(inspect.signature(type(registry)).parameters) == {"store", "configuration"}
    for method in (registry.resolve, registry.list, registry.set_enabled, registry.set_metadata):
        assert not {"root", "path", "url"} & set(inspect.signature(method).parameters)
    assert not any(n in type(store).__dict__ for n in ("register", "set_root", "add_profile"))


@pytest.mark.parametrize(
    "kind", ["duplicate_id", "duplicate_root", "nested", "symlink", "hardlink", "special", "parent_link"]
)
def test_unsafe_configuration_real_filesystem(tmp_path, kind):
    registry_cls, configuration, error, store_cls = api()
    a, b = tmp_path / "a", tmp_path / "b"
    a.mkdir()
    b.mkdir()
    first, second = binding(a), binding(b, "beta")
    if kind == "duplicate_id":
        second = binding(b)
    elif kind == "duplicate_root":
        second = binding(a, "beta")
    elif kind == "nested":
        second = binding(a / "nested", "beta")
    elif kind == "symlink":
        (a / "memory.db").symlink_to(b / "missing")
    elif kind == "hardlink":
        (b / "payload").write_text("CANARY_MEMORY")
        os.link(b / "payload", a / "memory.db")
    elif kind == "special":
        os.mkfifo(a / "graph.json")
    else:
        a.rename(tmp_path / "moved")
        a.symlink_to(tmp_path / "moved", target_is_directory=True)
    with store_cls(tmp_path / "control") as store:
        with pytest.raises(error) as exc:
            registry_cls(store, configuration((first, second)))
        assert exc.value.code == "E_VALIDATION"
        assert str(tmp_path) not in str(exc.value)
        assert store._connection.execute("SELECT count(*) FROM profiles").fetchone()[0] == 0


def test_schema_permissions_no_memory_open(setup, monkeypatch):
    registry, store, _, _ = setup
    assert store.root.stat().st_mode & 0o777 == 0o700
    assert (store.root / "control.sqlite").stat().st_mode & 0o777 == 0o600
    tables = {r[0] for r in store._connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert tables == {"profiles", "registry_state", "control_journal"}
    assert not any(word in table for word in ("memory", "fact", "belief", "decision", "evidence") for table in tables)
    original = sqlite3.connect
    opened = []

    def connect(path, *args, **kwargs):
        opened.append(str(path))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", connect)
    registry_cls, configuration, _, store_cls = api()
    with store_cls(store.root) as other:
        registry_cls(other, registry._configuration)
    assert opened and all("memory.db" not in path for path in opened)
    assert not any(root.exists() for root in [b.layout.sqlite.local_path for b in registry._configuration.bindings])


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "sidecar", "root_link"])
def test_control_unsafe_path_refused(tmp_path, kind):
    _, _, error, store_cls = api()
    root = tmp_path / "control"
    root.mkdir(mode=0o700)
    target = tmp_path / "target"
    target.write_text("CANARY_UNTOUCHED")
    if kind == "symlink":
        (root / "control.sqlite").symlink_to(target)
    elif kind == "hardlink":
        os.link(target, root / "control.sqlite")
    elif kind == "sidecar":
        (root / "control.sqlite-journal").symlink_to(target)
    else:
        root.rmdir()
        root.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(error) as exc:
        store_cls(root)
    assert exc.value.code == "E_STORAGE"
    assert str(tmp_path) not in str(exc.value)
    assert target.read_text() == "CANARY_UNTOUCHED"


def test_backend_errors_redacted(setup, caplog):
    registry, store, error, _ = setup
    store._connection.execute("DROP TABLE control_journal")
    with pytest.raises(error) as exc:
        asyncio.run(registry.set_enabled(principal("alpha"), "alpha", enabled=False, expected_generation=1))
    assert exc.value.code == "E_STORAGE"
    assert str(exc.value) == "Control storage unavailable"
    assert exc.value.__suppress_context__
    assert not any(word in str(exc.value) + caplog.text for word in (str(store.root), "UPDATE", "INSERT", "CANARY"))
    assert asyncio.run(registry.list(principal("alpha")))[0].generation == 1


def test_missing_control_parent_is_not_created(tmp_path):
    _, _, error, store_cls = api()
    parent = tmp_path / "control"
    with pytest.raises(error) as exc:
        store_cls(parent / "control")
    assert exc.value.code == "E_STORAGE"
    assert not parent.exists()


def test_reopen_retarget_rejected(setup, tmp_path):
    registry, store, error, bindings = setup
    cls, configuration, _, _ = api()
    with pytest.raises(error) as exc:
        cls(store, configuration((binding(tmp_path / "elsewhere"), bindings[1])))
    assert exc.value.code == "E_VALIDATION"
    assert asyncio.run(registry.resolve(principal("alpha"), "alpha")).layout == bindings[0].layout


@pytest.mark.parametrize("mode", [0o755, 0o770])
def test_nonprivate_control_directory_refused(tmp_path, mode):
    _, _, error, store_cls = api()
    root = tmp_path / "control"
    root.mkdir()
    root.chmod(mode)
    with pytest.raises(error) as exc:
        store_cls(root)
    assert exc.value.code == "E_STORAGE"
    assert not (root / "control.sqlite").exists()


def test_no_memory_connection_even_on_initial_configuration(tmp_path, monkeypatch):
    registry_cls, configuration, _, store_cls = api()
    configured = (binding(tmp_path / "alpha"), binding(tmp_path / "beta", "beta"))
    original = sqlite3.connect
    connections = []

    def guard(path, *args, **kwargs):
        assert str(path).endswith("/control.sqlite")
        connections.append(str(path))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", guard)
    with store_cls(tmp_path / "control") as store:
        registry = registry_cls(store, configuration(configured))
        asyncio.run(registry.resolve(principal("alpha"), "alpha"))
        asyncio.run(registry.set_enabled(principal("beta"), "beta", enabled=False, expected_generation=1))
        assert len(connections) == 1
        dump = "\n".join(store._connection.iterdump())
        assert "CANARY_MEMORY" not in dump
    assert not any(item.layout.data_root.exists() for item in configured)


def test_forward_schema_not_downgraded(setup):
    _, store, error, _ = setup
    _, _, _, store_cls = api()
    store._connection.execute("PRAGMA user_version=2")
    with pytest.raises(error) as exc:
        store_cls(store.root)
    assert exc.value.code == "E_STORAGE"
    assert store._connection.execute("PRAGMA user_version").fetchone()[0] == 2


def test_configuration_limit_and_list_permission(setup):
    registry, _, error, bindings = setup
    _, configuration, _, _ = api()
    with pytest.raises(error):
        configuration(bindings * 51)
    with pytest.raises(error):
        configuration(list(bindings))
    with pytest.raises(FrozenInstanceError):
        registry._configuration.bindings = ()
    actor = Principal(
        "synthetic",
        "synthetic",
        None,
        frozenset({"alpha"}),
        frozenset(),
        1,
        datetime.now(timezone.utc) + timedelta(hours=1),
    )
    with pytest.raises(error) as exc:
        asyncio.run(registry.list(actor))
    assert exc.value.code == "E_PERMISSION_DENIED"


def test_stage_import_graph_has_no_runtime_or_soft_neighbours():
    import ast

    import memory_server.control as control

    package = __import__("pathlib").Path(control.__file__).parent
    allowed = {"memory_server.gateway_contracts", "memory_server.paths"}
    for source in package.glob("*.py"):
        tree = ast.parse(source.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("memory_server"):
                assert any(node.module == name or node.module.startswith(name + ".") for name in allowed)
            elif isinstance(node, ast.Import):
                assert all(not name.name.startswith("memory_server.runtime") for name in node.names)
