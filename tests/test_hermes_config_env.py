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

import pytest

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

        cfg = HermesPluginConfig.from_dict({
            "db_url": "sqlite+aiosqlite:///cfg.db",
            "max_facts": 3,
            "writer": {"flush_interval": 1.0, "max_batch": 2},
            "llm_base_url": "https://cfg.example/v1",
        })

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
        cfg = HermesPluginConfig.from_dict({
            "db_url": "sqlite+aiosqlite:///cfg.db",
            "max_facts": 3,
            "writer": {"flush_interval": 1.0, "max_batch": 2},
            "llm_base_url": "https://cfg.example/v1",
        })
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

        cfg = HermesPluginConfig.from_dict({
            "db_url": "sqlite+aiosqlite:///cfg.db",
            "max_facts": 4,
            "writer": {"flush_interval": 3.0, "max_batch": 30},
        })
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

        cfg = HermesPluginConfig.from_dict({
            "path": "/cfg/cmms",
            "db_url": "sqlite+aiosqlite:///cfg.db",
            "max_facts": 6,
            "writer": {"flush_interval": 4.0, "max_batch": 40},
            "llm_base_url": "https://cfg.example/v1",
        }, use_env=False)

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
