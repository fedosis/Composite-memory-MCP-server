"""HERM-1/2: unified env resolver for HermesPluginConfig.

The config block resolves env vars through ONE snapshot resolver used by
both ``from_dict`` (``use_env=True``) and ``from_env``:

* unset/set/invalid matrix for every env-backed field;
* both MEMORY_SERVER_WRITER_FLUSH_INTERVAL and MEMORY_SERVER_WRITER_MAX_BATCH
  are honoured by from_dict when use_env=True (previously from_dict ignored
  them entirely);
* ``use_env=False`` ignores EVERY env field (db_url/max_facts/writer/base_url/
  path) so doctor can validate the raw config value.

Env manipulation uses monkeypatch AFTER clearing the vars (teardown restores
the original environment automatically).
"""

import json
from types import MappingProxyType

import pytest

from memory_server.paths import StorageLayoutError
from memory_server.plugins.hermes.config import HermesPluginConfig

CONFIG_ENV = [
    "MEMORY_SERVER_PATH",
    "MEMORY_SERVER_DB_URL",
    "MEMORY_SERVER_MAX_FACTS",
    "MEMORY_SERVER_WRITER_FLUSH_INTERVAL",
    "MEMORY_SERVER_WRITER_MAX_BATCH",
    "MEMORY_SERVER_LLM_BASE_URL",
]


@pytest.fixture
def clean_env(monkeypatch):
    """Clear every config-block env var first; monkeypatch restores on teardown."""
    for name in CONFIG_ENV:
        monkeypatch.delenv(name, raising=False)
    yield


def _default_db_url():
    from memory_server.settings import get_settings

    return str(get_settings().db_url)


class TestUnifiedEnvResolver:
    def test_unset_env_uses_defaults(self, clean_env):
        from_dict = HermesPluginConfig.from_dict({})
        from_env = HermesPluginConfig.from_env()

        for cfg in (from_dict, from_env):
            assert cfg.db_url == _default_db_url()
            assert cfg.max_facts == 5
            assert cfg.writer.flush_interval == 5.0
            assert cfg.writer.max_batch == 50
            assert cfg.llm_base_url is None

    def test_from_dict_env_overrides_all_env_backed_fields(self, clean_env, monkeypatch):
        monkeypatch.setenv("MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///env.db")
        monkeypatch.setenv("MEMORY_SERVER_MAX_FACTS", "9")
        monkeypatch.setenv("MEMORY_SERVER_WRITER_FLUSH_INTERVAL", "2.5")
        monkeypatch.setenv("MEMORY_SERVER_WRITER_MAX_BATCH", "7")
        monkeypatch.setenv("MEMORY_SERVER_LLM_BASE_URL", "https://env.example/v1")

        cfg = HermesPluginConfig.from_dict(
            {
                "db_url": "sqlite+aiosqlite:///cfg.db",
                "max_facts": 3,
                "writer": {"flush_interval": 1.0, "max_batch": 2},
                "llm_base_url": "https://cfg.example/v1",
            }
        )

        assert cfg.db_url == "sqlite+aiosqlite:///env.db"
        assert cfg.max_facts == 9
        assert cfg.writer.flush_interval == 2.5
        assert cfg.writer.max_batch == 7
        assert cfg.llm_base_url == "https://env.example/v1"

    def test_writer_env_both_honoured_in_from_dict(self, clean_env, monkeypatch):
        """HERM-1 regression: from_dict ignored both writer env vars."""
        monkeypatch.setenv("MEMORY_SERVER_WRITER_FLUSH_INTERVAL", "0.75")
        monkeypatch.setenv("MEMORY_SERVER_WRITER_MAX_BATCH", "12")
        cfg = HermesPluginConfig.from_dict({"writer": {"flush_interval": 9.0, "max_batch": 90}})
        assert cfg.writer.flush_interval == 0.75
        assert cfg.writer.max_batch == 12

    def test_config_values_used_when_env_unset(self, clean_env):
        cfg = HermesPluginConfig.from_dict(
            {
                "db_url": "sqlite+aiosqlite:///cfg.db",
                "max_facts": 3,
                "writer": {"flush_interval": 1.0, "max_batch": 2},
                "llm_base_url": "https://cfg.example/v1",
            }
        )
        assert cfg.db_url == "sqlite+aiosqlite:///cfg.db"
        assert cfg.max_facts == 3
        assert cfg.writer.flush_interval == 1.0
        assert cfg.writer.max_batch == 2
        assert cfg.llm_base_url == "https://cfg.example/v1"

    def test_invalid_env_falls_back_to_config_or_default(self, clean_env, monkeypatch):
        monkeypatch.setenv("MEMORY_SERVER_DB_URL", "   ")
        monkeypatch.setenv("MEMORY_SERVER_MAX_FACTS", "abc")
        monkeypatch.setenv("MEMORY_SERVER_WRITER_FLUSH_INTERVAL", "nan")
        monkeypatch.setenv("MEMORY_SERVER_WRITER_MAX_BATCH", "12.5")

        cfg = HermesPluginConfig.from_dict(
            {
                "db_url": "sqlite+aiosqlite:///cfg.db",
                "max_facts": 4,
                "writer": {"flush_interval": 3.0, "max_batch": 30},
            }
        )
        # Invalid/blank env is treated as unset: config values win.
        assert cfg.db_url == "sqlite+aiosqlite:///cfg.db"
        assert cfg.max_facts == 4
        assert cfg.writer.flush_interval == 3.0
        assert cfg.writer.max_batch == 30

        env_only = HermesPluginConfig.from_env()
        # Blank db_url -> settings default; malformed numbers -> defaults.
        assert env_only.db_url == _default_db_url()
        assert env_only.max_facts == 5
        assert env_only.writer.flush_interval == 5.0
        assert env_only.writer.max_batch == 50

    def test_use_env_false_ignores_every_env_field(self, clean_env, monkeypatch):
        """doctor path: use_env=False must never read env for ANY field."""
        monkeypatch.setenv("MEMORY_SERVER_PATH", "/env/cmms")
        monkeypatch.setenv("MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///env.db")
        monkeypatch.setenv("MEMORY_SERVER_MAX_FACTS", "99")
        monkeypatch.setenv("MEMORY_SERVER_WRITER_FLUSH_INTERVAL", "0.1")
        monkeypatch.setenv("MEMORY_SERVER_WRITER_MAX_BATCH", "1")
        monkeypatch.setenv("MEMORY_SERVER_LLM_BASE_URL", "https://env.example/v1")

        cfg = HermesPluginConfig.from_dict(
            {
                "path": "/cfg/cmms",
                "db_url": "sqlite+aiosqlite:///cfg.db",
                "max_facts": 6,
                "writer": {"flush_interval": 4.0, "max_batch": 40},
                "llm_base_url": "https://cfg.example/v1",
            },
            use_env=False,
        )

        assert cfg.cmms_path == "/cfg/cmms"
        assert cfg.cmms_path_source == "config"
        assert cfg.db_url == "sqlite+aiosqlite:///cfg.db"
        assert cfg.max_facts == 6
        assert cfg.writer.flush_interval == 4.0
        assert cfg.writer.max_batch == 40
        assert cfg.llm_base_url == "https://cfg.example/v1"

    def test_from_env_reads_all_env_backed_fields(self, clean_env, monkeypatch):
        monkeypatch.setenv("MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///envonly.db")
        monkeypatch.setenv("MEMORY_SERVER_MAX_FACTS", "11")
        monkeypatch.setenv("MEMORY_SERVER_WRITER_FLUSH_INTERVAL", "1.25")
        monkeypatch.setenv("MEMORY_SERVER_WRITER_MAX_BATCH", "8")
        monkeypatch.setenv("MEMORY_SERVER_LLM_BASE_URL", "https://envonly.example/v1")

        cfg = HermesPluginConfig.from_env()

        assert cfg.db_url == "sqlite+aiosqlite:///envonly.db"
        assert cfg.max_facts == 11
        assert cfg.writer.flush_interval == 1.25
        assert cfg.writer.max_batch == 8
        assert cfg.llm_base_url == "https://envonly.example/v1"
        # from_env keeps extraction/LLM tuning fields None (resolver's job).
        assert cfg.extraction_mode is None
        assert cfg.llm_model is None

    def test_blank_env_path_still_defaults_to_repo_root(self, clean_env, monkeypatch):
        monkeypatch.setenv("MEMORY_SERVER_PATH", "")
        cfg = HermesPluginConfig.from_dict({})
        assert cfg.cmms_path_source == "default"
        assert cfg.cmms_path == str(HermesPluginConfig.from_env().cmms_path)


def test_snapshot_is_immutable_and_records_precedence(clean_env, monkeypatch):
    monkeypatch.setenv("MEMORY_SERVER_VECTOR_BACKEND", "qdrant")
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    cfg = HermesPluginConfig.from_dict({"vector_backend": "lancedb"})
    assert cfg.storage_snapshot.vector_backend == "qdrant"
    assert cfg.storage_origins["vector"].kind == "env"
    with pytest.raises((AttributeError, TypeError)):
        cfg.storage_snapshot.vector_backend = "lancedb"


def test_yaml_storage_paths_are_used_and_have_origins(clean_env):
    cfg = HermesPluginConfig.from_dict(
        {
            "storage_mode": "profile",
            "data_root": "profile-data",
            "vector_backend": "lancedb",
            "lancedb_path": "vectors",
            "graph_snapshot_path": "graph.json",
            "qdrant_location": "http://qdrant",
            "vector_collection": "custom",
        }
    )
    assert cfg.data_root == "profile-data"
    assert cfg.storage_snapshot.lancedb_path == "vectors"
    assert cfg.storage_snapshot.graph_snapshot_path == "graph.json"
    assert cfg.storage_origins["root"].kind == "yaml"
    assert cfg.storage_origins["vector"].kind == "yaml"
    assert cfg.storage_origins["graph"].kind == "yaml"


def test_use_env_false_does_not_construct_settings_or_read_environment(monkeypatch):
    monkeypatch.setattr(
        "memory_server.plugins.hermes.config.get_settings",
        lambda: (_ for _ in ()).throw(AssertionError("settings read")),
    )
    monkeypatch.setattr("memory_server.plugins.hermes.config.os.environ", {"MEMORY_SERVER_DB_URL": "bad"})
    cfg = HermesPluginConfig.from_dict({}, use_env=False)
    assert cfg.db_url == "sqlite+aiosqlite:///data/memory.db"
    assert cfg.storage_origins["sqlite"].kind == "default"


def test_shared_env_requires_paired_absolute_root(clean_env, monkeypatch):
    monkeypatch.setenv("MEMORY_SERVER_STORAGE_MODE", "shared")
    with pytest.raises(StorageLayoutError, match="(?i)data_root"):
        HermesPluginConfig.from_env()
    monkeypatch.delenv("MEMORY_SERVER_STORAGE_MODE")
    monkeypatch.setenv("MEMORY_SERVER_DATA_ROOT", "/srv/cmms")
    with pytest.raises(StorageLayoutError, match="(?i)storage_mode"):
        HermesPluginConfig.from_env()


def test_legacy_storage_env_contains_only_deployed_aliases(clean_env, monkeypatch):
    monkeypatch.setenv("MEMORY_STORAGE_MODE", "shared")
    monkeypatch.setenv("MEMORY_DATA_ROOT", "/invented")
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "qdrant")
    cfg = HermesPluginConfig.from_env()
    assert cfg.storage_mode == "profile"
    assert cfg.data_root == "."
    assert cfg.vector_backend == "qdrant"


def test_snapshot_exposes_immutable_values_and_origins(clean_env):
    cfg = HermesPluginConfig.from_dict({"vector_backend": "qdrant"})
    snapshot = cfg.storage_snapshot
    assert isinstance(snapshot.values, MappingProxyType)
    assert snapshot.values["vector_backend"] == "qdrant"
    assert snapshot.origins["vector_backend"].kind == "yaml"
    with pytest.raises(TypeError):
        snapshot.values["vector_backend"] = "lancedb"
    with pytest.raises(TypeError):
        snapshot.origins["vector_backend"] = snapshot.origins["vector_backend"]


@pytest.mark.parametrize("use_env", [True, False])
def test_s1_parent_frozen_storage_values_not_replaced_by_supplied_settings(tmp_path, monkeypatch, use_env):
    from memory_server.plugins.hermes import config as config_module
    from memory_server.settings import Settings

    synthetic = Settings(_env_file=None, lancedb_path="settings-vectors", graph_snapshot_path="settings-graph")
    monkeypatch.setattr(config_module, "get_settings", lambda: synthetic)
    monkeypatch.setenv("MEMORY_SERVER_LANCEDB_PATH", "env-vectors")
    cfg = config_module.HermesPluginConfig.from_dict({}, use_env=use_env)
    layout = cfg.resolve_storage_layout(hermes_home=str(tmp_path), settings=synthetic)
    assert layout.vector.local_path == tmp_path / ("env-vectors" if use_env else "data/lancedb")
    assert layout.origins["lancedb_path"].kind == ("env" if use_env else "default")


# ---------------------------------------------------------------------------
# S2-02 -- raw / effective storage configuration report
#
# The report is the config half of the S2-02 contract: the RAW YAML layer
# (``use_env=False``) selects the canonical SQL with ZERO environment reads,
# the environment is reported SEPARATELY, and the effective value is derived
# without ever consulting ``Settings``/``.env``.
# ---------------------------------------------------------------------------

_S202_RAW_BLOCK = {
    "storage_mode": "profile",
    "data_root": ".",
    "db_url": "sqlite+aiosqlite:///data/memory.db",
    "vector_backend": "lancedb",
    "lancedb_path": "data/lancedb",
    "graph_snapshot_path": "data/graph.json",
}

_S202_REPORT_KEYS = (
    "db_url",
    "storage_mode",
    "data_root",
    "vector_backend",
    "lancedb_path",
    "graph_snapshot_path",
    "qdrant_location",
    "vector_collection",
)


def _s202_report_builder():
    """The config report builder, or a hard failure naming the missing capability."""
    from memory_server.plugins.hermes import config as config_module

    builder = getattr(config_module, "build_storage_config_report", None)
    if builder is None:
        pytest.fail(
            "S2-02: no raw/effective storage config report exists in "
            "memory_server.plugins.hermes.config (missing capability on this revision)"
        )
    return builder


def test_s202_raw_layer_selects_canonical_sql_with_zero_env_reads(clean_env, monkeypatch):
    """The raw layer is the single canonical-SQL selector and reads no env."""
    builder = _s202_report_builder()
    monkeypatch.setenv("MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///env/other.db")
    monkeypatch.setenv("MEMORY_SERVER_VECTOR_BACKEND", "qdrant")

    raw = builder(_S202_RAW_BLOCK, include_env=False)

    assert raw.canonical_sqlite_url == "sqlite+aiosqlite:///data/memory.db"
    assert raw.raw["db_url"] == "sqlite+aiosqlite:///data/memory.db"
    assert raw.raw["vector_backend"] == "lancedb"
    assert raw.divergence == ()
    assert raw.settings_consulted is False
    # With include_env=False no environment key is read at all.
    assert all(raw.env[key] is None for key in _S202_REPORT_KEYS)
    assert raw.effective["db_url"] == raw.raw["db_url"]

    effective = builder(_S202_RAW_BLOCK, include_env=True)
    assert effective.raw == raw.raw
    assert effective.env["db_url"] == "sqlite+aiosqlite:///env/other.db"
    assert effective.effective["db_url"] == "sqlite+aiosqlite:///env/other.db"
    assert effective.effective_origins["db_url"] == "env"
    assert effective.raw_origins["db_url"] == "yaml"
    assert effective.divergence == ("db_url", "vector_backend")


def test_s202_raw_layer_does_not_touch_environ_or_settings(monkeypatch):
    """``include_env=False`` must not read os.environ nor construct Settings."""
    import os as _os

    from memory_server.plugins.hermes import config as config_module

    builder = _s202_report_builder()
    monkeypatch.setattr(
        config_module,
        "get_settings",
        lambda: (_ for _ in ()).throw(AssertionError("settings read")),
    )
    monkeypatch.setattr(config_module.os, "environ", {"MEMORY_SERVER_DB_URL": "sqlite+aiosqlite:///leak.db"})

    raw = builder(_S202_RAW_BLOCK, include_env=False)

    assert raw.raw["db_url"] == "sqlite+aiosqlite:///data/memory.db"
    assert raw.canonical_sqlite_url == "sqlite+aiosqlite:///data/memory.db"
    monkeypatch.undo()
    assert _os.environ is not None


def test_s202_raw_report_redacts_sensitive_uri_values(clean_env, monkeypatch):
    builder = _s202_report_builder()
    monkeypatch.setenv("MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///env.db?token=SECRETVALUE")

    payload = builder(_S202_RAW_BLOCK, include_env=True).as_dict()

    encoded = json.dumps(payload, sort_keys=True)
    assert "SECRETVALUE" not in encoded
    assert payload["canonical_sqlite_url"] == "sqlite+aiosqlite:///data/memory.db"
    assert "<redacted>" in encoded
