"""S06 amendments: real control storage boundaries, no HTTP shell."""
from __future__ import annotations

import sqlite3
from dataclasses import replace

import pytest

from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
from memory_server.control.store import ControlError, ControlStore
from tests.gateway.test_registry import binding, principal


@pytest.fixture
def configured(tmp_path):
    bindings = tuple(binding(tmp_path / name, name, enabled=name != "beta") for name in ("alpha", "beta", "gamma"))
    with ControlStore(tmp_path / "control") as store:
        registry = ProfileRegistry(store, RegistryConfiguration(bindings))
        yield registry, store, bindings


async def test_q3_page_scoped_keyset_and_disabled_record(configured):
    registry, store, bindings = configured
    actor = principal("alpha", "beta")
    statements = []
    store._connection.set_trace_callback(statements.append)
    page, upper = await registry.page(actor, limit=1, after=None, upper=None)
    assert [item.id for item in page] == ["alpha"]
    assert upper == "beta"
    assert page[0].storage_root is None
    other, same_upper = await registry.page(actor, limit=1, after="alpha", upper=upper)
    assert [item.id for item in other] == ["beta"] and same_upper == upper
    assert not other[0].enabled and other[0].storage_root is None
    record = await registry.record(actor, "beta", permission="profile.manage")
    assert not record.enabled
    with pytest.raises(ControlError, match="Profile disabled"):
        await registry.resolve(actor, "beta")
    select = [s for s in statements if s.startswith("SELECT")]
    assert any("LIMIT 2" in s and "ORDER BY id" in s and " IN (" in s for s in select)
    assert statements[0] == "BEGIN"
    assert statements.index("COMMIT") > next(i for i, s in enumerate(statements) if "LIMIT 2" in s)
    assert not any(b.layout.data_root.exists() for b in bindings)


async def test_q3_scope_before_lookup_and_position_validation(configured, monkeypatch):
    registry, store, _ = configured
    actor = principal("alpha")
    def forbidden(*args, **kwargs):
        pytest.fail("lookup before scope/validation")
    monkeypatch.setattr(store, "_get", forbidden)
    for target in ("beta", "absent"):
        with pytest.raises(ControlError) as exc:
            await registry.record(actor, target, permission="profile.read")
        assert exc.value.code == "E_PERMISSION_DENIED"
    with pytest.raises(ControlError) as exc:
        await registry.record(actor, "alpha", permission="token.manage")
    assert exc.value.code == "E_VALIDATION"
    for options in (
        dict(limit=0, after=None, upper=None), dict(limit=True, after=None, upper=None),
        dict(limit=201, after=None, upper=None), dict(limit=1, after="../alpha", upper="beta"),
        dict(limit=1, after="alpha", upper=None), dict(limit=1, after="beta", upper="alpha"),
    ):
        with pytest.raises(ControlError) as exc:
            await registry.page(actor, **options)
        assert exc.value.code == "E_VALIDATION"
    with pytest.raises(ControlError) as exc:
        await registry.page(replace(actor, permissions=frozenset()), limit=1, after=None, upper=None)
    assert exc.value.code == "E_PERMISSION_DENIED"
    empty, upper = await registry.page(principal(), limit=1, after=None, upper=None)
    assert empty == () and upper is None


async def test_q3_read_only_and_no_filesystem_after_configuration(configured, monkeypatch):
    from pathlib import Path
    registry, store, _ = configured
    def forbidden(*args, **kwargs):
        pytest.fail("read path opened filesystem")
    before = store._connection.total_changes
    with monkeypatch.context() as patch:
        patch.setattr(Path, "stat", forbidden)
        patch.setattr(sqlite3, "connect", forbidden)
        await registry.page(principal("alpha"), limit=200, after=None, upper=None)
        await registry.record(principal("beta"), "beta", permission="profile.read")
    assert store._connection.total_changes == before
