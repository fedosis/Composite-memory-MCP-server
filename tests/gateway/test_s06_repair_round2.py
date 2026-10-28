"""Round-two regressions across real registry/store and config boundaries."""

from dataclasses import FrozenInstanceError

import pytest

from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
from memory_server.control.store import ControlStore
from tests.gateway.test_registry import binding, principal


@pytest.fixture
def configured(tmp_path):
    configured_binding = binding(tmp_path / "alpha", "alpha")
    with ControlStore(tmp_path / "control") as store:
        registry = ProfileRegistry(store, RegistryConfiguration((configured_binding,)))
        yield registry, store, configured_binding


@pytest.mark.parametrize("method", ["list", "record", "page", "set_enabled", "set_metadata"])
async def test_f3_every_public_record_root_is_null(configured, method):
    registry, store, configured_binding = configured
    actor = principal("alpha")
    if method == "list":
        records = await registry.list(actor)
    elif method == "record":
        records = (await registry.record(actor, "alpha", permission="profile.read"),)
    elif method == "page":
        records, upper = await registry.page(actor, limit=1, after=None, upper=None)
        assert upper == "alpha"
    elif method == "set_enabled":
        records = (await registry.set_enabled(actor, "alpha", enabled=True, expected_generation=1),)
    else:
        records = (await registry.set_metadata(actor, "alpha", display_name="renamed",
                                              description="updated", expected_generation=1),)
    assert len(records) == 1
    record = records[0]
    root_field = "storage_root" if method == "page" else "canonical_root"
    assert getattr(record, root_field) is None
    if method != "page":  # Profile DTO immutability uses Pydantic, not dataclasses.
        with pytest.raises(FrozenInstanceError):
            setattr(record, root_field, configured_binding.layout.data_root)
    row = store._connection.execute("SELECT * FROM profiles WHERE id='alpha'").fetchone()
    assert row["canonical_root"] == str(configured_binding.layout.data_root)
    resolved = await registry.resolve(actor, "alpha")
    assert resolved.layout == configured_binding.layout
    assert resolved.config == configured_binding.config
    assert resolved.descriptor == configured_binding.descriptor
    assert resolved.profile.storage_root is None


def test_f3_record_producer_redacts_raw_database_row(configured):
    # A future facade returning _get()/_record() directly must still be safe.
    _, store, configured_binding = configured
    row = store._connection.execute("SELECT * FROM profiles WHERE id='alpha'").fetchone()
    assert row["canonical_root"] == str(configured_binding.layout.data_root)
    record = store._record(row)
    assert record.canonical_root is None
    assert store._get("alpha").canonical_root is None
    assert (record.id, record.enabled, record.generation) == ("alpha", True, 1)
