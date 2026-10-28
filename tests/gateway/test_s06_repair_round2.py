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


@pytest.mark.parametrize("overrides", [{}, {"max_header_bytes": 10**9}])
def test_f4_exact_type_rejects_even_valid_bypass_subclasses(tmp_path, overrides):
    from memory_server.gateway_contracts.ports import HttpConfig, validate_http_config

    class Bypass(HttpConfig):
        def __post_init__(self):
            pass

    config = Bypass(control_path=tmp_path, **overrides)
    with pytest.raises(ValueError, match="^E_VALIDATION:"):
        validate_http_config(config)


def test_f4_inherited_validation_rejects_subclass_construction(tmp_path):
    from memory_server.gateway_contracts.ports import HttpConfig

    class Extension(HttpConfig):
        pass

    with pytest.raises(ValueError, match="^E_VALIDATION:"):
        Extension(control_path=tmp_path)


@pytest.mark.parametrize("value", [None, object()])
def test_f4_non_config_is_validation_error(value):
    from memory_server.gateway_contracts.ports import validate_http_config

    with pytest.raises(ValueError, match="^E_VALIDATION:"):
        validate_http_config(value)


@pytest.mark.parametrize("field,value", [
    ("max_header_bytes", 10**9),
    ("max_connections", 10**9),
    ("header_timeout_seconds", 10**6),
    ("request_timeout_seconds", 0),
])
def test_f4_config_fields_cannot_be_mutated_or_deleted(tmp_path, field, value):
    from memory_server.gateway_contracts.ports import HttpConfig, validate_http_config

    config = HttpConfig(control_path=tmp_path)
    before = getattr(config, field)
    with pytest.raises(FrozenInstanceError):
        setattr(config, field, value)
    with pytest.raises(FrozenInstanceError):
        delattr(config, field)
    assert getattr(config, field) == before
    validate_http_config(config)


@pytest.mark.parametrize("header,total,valid", [(5, 5, True), (4, 5, True), (6, 5, False)])
def test_f4_deadline_ordering_at_construction_and_validation(tmp_path, header, total, valid):
    from memory_server.gateway_contracts.ports import HttpConfig, validate_http_config

    if valid:
        config = HttpConfig(control_path=tmp_path, header_timeout_seconds=header, request_timeout_seconds=total)
        validate_http_config(config)
    else:
        with pytest.raises(ValueError, match="^E_VALIDATION:"):
            HttpConfig(control_path=tmp_path, header_timeout_seconds=header, request_timeout_seconds=total)
