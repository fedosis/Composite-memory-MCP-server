"""Tests for the central Settings model and its production wiring (Card 1).

Covers the amended SPEC inventory (including ``embedding_provider`` and
``openai_embedding_vector_size``), byte-for-byte default equality, the
two-context precedence policy, secrets exclusion, validation, and wiring of
Settings into the server, Hermes plugin, direct API layer, ingestion service,
and outbox worker.
"""

from __future__ import annotations

import inspect
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from memory_server.settings import (
    Settings,
    get_openai_api_key,
    get_qdrant_api_key,
    get_settings,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------------
# Frozen inventory — amended per plan_review (+embedding_provider,
# +openai_embedding_vector_size). Asserted set-equal to Settings.model_fields
# so a silently added/missing field fails the suite.
# ---------------------------------------------------------------------------

EXPECTED_FIELDS = frozenset({
    "db_url",
    "graph_snapshot_path",
    "vector_backend",
    "lancedb_path",
    "vector_collection",
    "vector_size",
    "vector_metric",
    "qdrant_location",
    "qdrant_port",
    "qdrant_prefer_grpc",
    "embedding_provider",
    "embedding_model",
    "embedding_device",
    "embedding_batch_size",
    "openai_embedding_model",
    "openai_embedding_vector_size",
    "embedding_base_url",
    "outbox_max_retries",
    "outbox_poll_interval_seconds",
    "outbox_poll_batch_size",
    "outbox_fact_batch_chunk_size",
    "outbox_compact_interval_seconds",
    "outbox_compact_cleanup_hours",
    "outbox_process_pending_limit",
    "outbox_stale_processing_seconds",
    "sqlite_busy_timeout_ms",
    "max_active_beliefs",
    "min_belief_confidence",
    "search_default_limit",
    "context_default_limit",
    "context_dedup_overfetch_factor",
    "semantic_top_k",
    "semantic_score_threshold",
    "belief_search_limit",
    "reflect_min_confidence",
    "reflect_limit",
    "max_contradiction_pairs",
    "reflect_belief_cap",
    "graph_max_path_depth",
    "ttl_days",
    "decay_archive_threshold",
    "decay_stale_ratio",
    "decay_archive_ratio",
    "decay_forgotten_ratio",
    "decay_default_ttl_days",
    "decay_confidence_floor",
    "decay_base",
    "decay_forecast_window_days",
    "confidence_source_reliability",
    "confidence_ttl_days",
    "confidence_lifecycle_multipliers",
    "confidence_corroboration_boost",
    "confidence_boost_threshold",
    "confidence_conflict_penalty",
    "confidence_penalty_threshold",
    "admission_default_ttl_days",
    "admission_default_score",
    "admission_ephemeral_ttl_days",
    "admission_ephemeral_score",
    "admission_important_score",
    "extraction_mode",
    "llm_model",
    "llm_timeout_seconds",
    "llm_max_input_chars",
    "llm_confidence_gate",
})

# Byte-for-byte defaults == today's hardcoded values (verified against source).
DEFAULTS: dict[str, object] = {
    "db_url": "sqlite+aiosqlite:///data/memory.db",
    "graph_snapshot_path": Path("data/graph.json"),
    "vector_backend": "lancedb",
    "lancedb_path": Path("data/lancedb"),
    "vector_collection": "memories",
    "vector_size": 384,
    "vector_metric": "cosine",
    "qdrant_location": ":memory:",
    "qdrant_port": 6333,
    "qdrant_prefer_grpc": False,
    "embedding_provider": "sentence-transformers",
    "embedding_model": "all-MiniLM-L6-v2",
    "embedding_device": None,
    "embedding_batch_size": 32,
    "openai_embedding_model": "text-embedding-3-small",
    "openai_embedding_vector_size": 1536,
    "embedding_base_url": None,
    "outbox_max_retries": 3,
    "outbox_poll_interval_seconds": 1.0,
    "outbox_poll_batch_size": 500,
    "outbox_fact_batch_chunk_size": 32,
    "outbox_compact_interval_seconds": 1800,
    "outbox_compact_cleanup_hours": 1,
    "outbox_process_pending_limit": 500,
    "outbox_stale_processing_seconds": 600,
    "sqlite_busy_timeout_ms": 5000,
    "max_active_beliefs": 500,
    "min_belief_confidence": 0.6,
    "search_default_limit": 50,
    "context_default_limit": 10,
    "context_dedup_overfetch_factor": 4,
    "semantic_top_k": 10,
    "semantic_score_threshold": 0.0,
    "belief_search_limit": 10,
    "reflect_min_confidence": 0.0,
    "reflect_limit": 50,
    "max_contradiction_pairs": 100_000,
    "reflect_belief_cap": 10000,
    "graph_max_path_depth": 4,
    "ttl_days": {
        "fact": 90.0,
        "decision": 180.0,
        "skill": 365.0,
        "entity": 365.0,
        "belief": 180.0,
    },
    "decay_archive_threshold": 0.2,
    "decay_stale_ratio": 0.7,
    "decay_archive_ratio": 1.0,
    "decay_forgotten_ratio": 2.0,
    "decay_default_ttl_days": 90.0,
    "decay_confidence_floor": 0.1,
    "decay_base": 2.0,
    "decay_forecast_window_days": 7.0,
    "confidence_source_reliability": {
        "verified": 0.9,
        "admin": 0.85,
        "inferred": 0.7,
        "extracted": 0.6,
        "unknown": 0.3,
    },
    "confidence_ttl_days": 90.0,
    "confidence_lifecycle_multipliers": {
        "active": 1.0,
        "validated": 0.95,
        "candidate": 0.85,
        "stale": 0.6,
        "archived": 0.3,
        "forgotten": 0.0,
        "superseded": 0.3,
        "contradicted": 0.3,
        "discarded": 0.0,
        "trusted": 1.0,
        "deprecated": 0.6,
    },
    "confidence_corroboration_boost": 0.10,
    "confidence_boost_threshold": 2,
    "confidence_conflict_penalty": 0.20,
    "confidence_penalty_threshold": 1,
    "admission_default_ttl_days": 365,
    "admission_default_score": 0.65,
    "admission_ephemeral_ttl_days": 1,
    "admission_ephemeral_score": 0.05,
    "admission_important_score": 0.95,
    "extraction_mode": "regex",
    "llm_model": None,
    "llm_timeout_seconds": 15.0,
    "llm_max_input_chars": 8000,
    "llm_confidence_gate": 0.7,
}


# ---------------------------------------------------------------------------
# Inventory / defaults
# ---------------------------------------------------------------------------


class TestInventoryAndDefaults:
    def test_field_set_matches_frozen_inventory(self):
        """No silently extra/missing Settings fields vs the agreed inventory."""
        assert set(Settings.model_fields) == EXPECTED_FIELDS

    @pytest.mark.parametrize("field", sorted(EXPECTED_FIELDS))
    def test_default_byte_for_byte(self, field):
        """Every default equals today's hardcoded value."""
        assert getattr(get_settings(), field) == DEFAULTS[field]

    def test_cache_isolation(self):
        """Same instance across calls; cache_clear() forces a re-read."""
        assert get_settings() is get_settings()
        get_settings.cache_clear()
        assert get_settings() is get_settings()

    def test_cache_clear_reflects_env_change(self, monkeypatch):
        monkeypatch.setenv("MEMORY_SERVER_MAX_ACTIVE_BELIEFS", "42")
        get_settings.cache_clear()
        assert get_settings().max_active_beliefs == 42
        monkeypatch.delenv("MEMORY_SERVER_MAX_ACTIVE_BELIEFS")
        get_settings.cache_clear()
        assert get_settings().max_active_beliefs == 500


# ---------------------------------------------------------------------------
# Precedence — Context A (Settings-resolved keys)
# ---------------------------------------------------------------------------


class TestPrecedenceContextA:
    @pytest.mark.parametrize(
        ("env_name", "field", "expected"),
        [
            ("MEMORY_VECTOR_BACKEND", "vector_backend", "qdrant"),
            (
                "MEMORY_GRAPH_SNAPSHOT_PATH",
                "graph_snapshot_path",
                "legacy/graph.json",
            ),
            ("MEMORY_QDRANT_URL", "qdrant_location", "http://legacy:6333"),
        ],
    )
    def test_legacy_alias_alone(self, monkeypatch, env_name, field, expected):
        """Each legacy alias works alone (no canonical env)."""
        monkeypatch.setenv(env_name, expected)
        get_settings.cache_clear()
        # Path fields coerce the string; compare string forms.
        assert str(getattr(get_settings(), field)) == expected

    @pytest.mark.parametrize(
        ("legacy_name", "canonical_name", "field", "expected"),
        [
            (
                "MEMORY_VECTOR_BACKEND",
                "MEMORY_SERVER_VECTOR_BACKEND",
                "vector_backend",
                "lancedb",
            ),
            (
                "MEMORY_GRAPH_SNAPSHOT_PATH",
                "MEMORY_SERVER_GRAPH_SNAPSHOT_PATH",
                "graph_snapshot_path",
                "canonical/graph.json",
            ),
            (
                "MEMORY_QDRANT_URL",
                "MEMORY_SERVER_QDRANT_LOCATION",
                "qdrant_location",
                "http://canonical:6333",
            ),
        ],
    )
    def test_canonical_wins_when_both_set(
        self, monkeypatch, legacy_name, canonical_name, field, expected
    ):
        monkeypatch.setenv(legacy_name, "legacy-value")
        monkeypatch.setenv(canonical_name, expected)
        get_settings.cache_clear()
        assert str(getattr(get_settings(), field)) == expected

    def test_env_beats_dotenv_file(self, tmp_path, monkeypatch):
        """Process env beats `.env`; `.env` beats the field default."""
        (tmp_path / ".env").write_text(
            "MEMORY_SERVER_DB_URL=sqlite+aiosqlite:///dotenv.db\n"
        )
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("MEMORY_SERVER_DB_URL", raising=False)
        get_settings.cache_clear()
        assert get_settings().db_url == "sqlite+aiosqlite:///dotenv.db"

        monkeypatch.setenv("MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///env.db")
        get_settings.cache_clear()
        assert get_settings().db_url == "sqlite+aiosqlite:///env.db"

    def test_canonical_db_url_env(self, monkeypatch):
        monkeypatch.setenv("MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///custom.db")
        get_settings.cache_clear()
        assert get_settings().db_url == "sqlite+aiosqlite:///custom.db"


# ---------------------------------------------------------------------------
# Precedence — Context B (plugin-resolved db_url)
# ---------------------------------------------------------------------------


class TestPrecedenceContextB:
    def test_yaml_wins_over_settings_default(self):
        from memory_server.plugins.hermes.config import HermesPluginConfig

        config = HermesPluginConfig.from_dict({"db_url": "sqlite+aiosqlite:///yaml.db"})
        assert config.db_url == "sqlite+aiosqlite:///yaml.db"

    def test_canonical_env_wins_over_yaml(self, monkeypatch):
        from memory_server.plugins.hermes.config import HermesPluginConfig

        monkeypatch.setenv(
            "MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///env.db"
        )
        config = HermesPluginConfig.from_dict({"db_url": "sqlite+aiosqlite:///yaml.db"})
        assert config.db_url == "sqlite+aiosqlite:///env.db"

    def test_settings_fallback_default(self):
        from memory_server.plugins.hermes.config import HermesPluginConfig

        config = HermesPluginConfig.from_dict({})
        assert config.db_url == "sqlite+aiosqlite:///data/memory.db"

    def test_from_env_canonical(self, monkeypatch):
        from memory_server.plugins.hermes.config import HermesPluginConfig

        monkeypatch.setenv(
            "MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///env.db"
        )
        assert HermesPluginConfig.from_env().db_url == "sqlite+aiosqlite:///env.db"

    def test_from_env_settings_fallback(self):
        from memory_server.plugins.hermes.config import HermesPluginConfig

        assert HermesPluginConfig.from_env().db_url == "sqlite+aiosqlite:///data/memory.db"

    def test_use_env_false_ignores_env(self, monkeypatch):
        """use_env=False skips the MEMORY_SERVER_PATH env override; db_url
        env behavior is unchanged (existing semantics preserved)."""
        from memory_server.plugins.hermes.config import HermesPluginConfig

        monkeypatch.setenv("MEMORY_SERVER_PATH", "/env/cmms")
        config = HermesPluginConfig.from_dict({"path": "/cfg/cmms"}, use_env=False)
        assert config.cmms_path == "/cfg/cmms"
        config_env = HermesPluginConfig.from_dict({"path": "/cfg/cmms"}, use_env=True)
        assert config_env.cmms_path == "/env/cmms"

    def test_dotenv_inside_settings_fallback(self, tmp_path, monkeypatch):
        """`.env` participates in the Settings fallback: beats the default,
        loses to canonical env and to YAML."""
        from memory_server.plugins.hermes.config import HermesPluginConfig

        (tmp_path / ".env").write_text(
            "MEMORY_SERVER_DB_URL=sqlite+aiosqlite:///dotenv.db\n"
        )
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("MEMORY_SERVER_DB_URL", raising=False)
        get_settings.cache_clear()

        # .env only → dotenv value beats the field default.
        assert HermesPluginConfig.from_dict({}).db_url == "sqlite+aiosqlite:///dotenv.db"
        # YAML beats .env.
        assert (
            HermesPluginConfig.from_dict(
                {"db_url": "sqlite+aiosqlite:///yaml.db"}
            ).db_url
            == "sqlite+aiosqlite:///yaml.db"
        )
        # Canonical env beats .env.
        monkeypatch.setenv(
            "MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///env.db"
        )
        get_settings.cache_clear()
        assert (
            HermesPluginConfig.from_dict({"db_url": "sqlite+aiosqlite:///yaml.db"}).db_url
            == "sqlite+aiosqlite:///env.db"
        )

    def test_public_shape_unchanged(self):
        """HermesPluginConfig keeps old fields plus five extraction fields."""
        from memory_server.plugins.hermes.config import HermesPluginConfig

        config = HermesPluginConfig.from_dict({})
        expected = {
            "db_url",
            "cmms_path",
            "cmms_path_source",
            "writer",
            "max_facts",
            "extraction_mode",
            "llm_model",
            "llm_timeout_seconds",
            "llm_max_input_chars",
            "llm_confidence_gate",
        }
        assert set(config.__dataclass_fields__) == expected


@pytest.mark.parametrize(
    "name,value,expected",
    [
        ("MEMORY_SERVER_EXTRACTION_MODE", "llm", "llm"),
        ("MEMORY_SERVER_LLM_MODEL", "  model-x  ", "model-x"),
        ("MEMORY_SERVER_LLM_TIMEOUT_SECONDS", "2.5", 2.5),
        ("MEMORY_SERVER_LLM_MAX_INPUT_CHARS", "-4", -4),
        ("MEMORY_SERVER_LLM_CONFIDENCE_GATE", "0.25", 0.25),
    ],
)
def test_extraction_env_values_are_lenient_raw_strings(monkeypatch, name, value, expected):
    monkeypatch.setenv(name, value)
    got = Settings()
    assert getattr(got, name.removeprefix("MEMORY_SERVER_").lower()) == expected


@pytest.mark.parametrize(
    "name,value",
    [
        ("MEMORY_SERVER_LLM_TIMEOUT_SECONDS", "not-a-number"),
        ("MEMORY_SERVER_LLM_TIMEOUT_SECONDS", "nan"),
        ("MEMORY_SERVER_LLM_TIMEOUT_SECONDS", "+inf"),
        ("MEMORY_SERVER_LLM_TIMEOUT_SECONDS", "-inf"),
        ("MEMORY_SERVER_LLM_CONFIDENCE_GATE", "nan"),
        ("MEMORY_SERVER_LLM_CONFIDENCE_GATE", "+inf"),
        ("MEMORY_SERVER_LLM_CONFIDENCE_GATE", "-inf"),
        ("MEMORY_SERVER_LLM_MAX_INPUT_CHARS", "3.5"),
    ],
)
def test_malformed_extraction_env_falls_back_to_field_default(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    got = Settings()
    expected = {
        "MEMORY_SERVER_LLM_TIMEOUT_SECONDS": 15.0,
        "MEMORY_SERVER_LLM_CONFIDENCE_GATE": 0.7,
        "MEMORY_SERVER_LLM_MAX_INPUT_CHARS": 8000,
    }[name]
    assert getattr(got, name.removeprefix("MEMORY_SERVER_").lower()) == expected


@pytest.mark.parametrize(
    "field,expected",
    [
        ("llm_timeout_seconds", 15.0),
        ("llm_confidence_gate", 0.7),
    ],
)
def test_huge_integer_numeric_values_fall_back_to_field_default(field, expected):
    got = Settings(**{field: 10**1000})
    assert getattr(got, field) == expected


# ---------------------------------------------------------------------------
# Secrets / validation
# ---------------------------------------------------------------------------


class TestSecretsAndValidation:
    def test_secrets_absent_from_model_dump(self):
        dump = get_settings().model_dump()
        json_dump = get_settings().model_dump(mode="json")
        keys = {k.lower() for k in dump} | {k.lower() for k in json_dump}
        forbidden = {"openai_api_key", "qdrant_api_key"}
        assert not any(
            k in forbidden or "api_key" in k for k in keys
        ), keys

    def test_env_accessors_not_fields(self):
        assert "OPENAI_API_KEY" not in os.environ or get_openai_api_key() == os.environ[
            "OPENAI_API_KEY"
        ]
        assert "QDRANT_API_KEY" not in os.environ or get_qdrant_api_key() == os.environ[
            "QDRANT_API_KEY"
        ]
        assert "api_key" not in Settings.model_fields

    @pytest.mark.parametrize(
        ("kwargs", "label"),
        [
            ({"vector_backend": "bogus"}, "bogus vector_backend"),
            ({"vector_size": 0}, "zero vector_size"),
            ({"openai_embedding_vector_size": 0}, "zero openai vector size"),
            ({"outbox_max_retries": -1}, "negative retries"),
            ({"outbox_poll_batch_size": 0}, "zero poll batch size"),
            ({"outbox_poll_interval_seconds": 0}, "zero poll interval"),
            ({"sqlite_busy_timeout_ms": -1}, "negative busy timeout"),
            ({"min_belief_confidence": 1.5}, "confidence > 1"),
            ({"semantic_score_threshold": -0.1}, "threshold < 0"),
            ({"reflect_min_confidence": 1.1}, "reflect confidence > 1"),
            ({"decay_archive_threshold": 1.5}, "archive threshold > 1"),
            ({"decay_confidence_floor": -0.2}, "confidence floor < 0"),
            ({"admission_default_score": 1.1}, "admission score > 1"),
            ({"admission_ephemeral_score": -0.1}, "ephemeral score < 0"),
            ({"admission_important_score": 1.5}, "important score > 1"),
            ({"confidence_corroboration_boost": 1.2}, "boost > 1"),
            ({"confidence_conflict_penalty": -0.3}, "penalty < 0"),
            ({"ttl_days": {}}, "empty ttl_days"),
        ],
    )
    def test_validation_raises(self, kwargs, label):
        with pytest.raises(Exception):
            Settings(**kwargs)

    def test_dict_fields_parse_from_json_env(self, monkeypatch):
        monkeypatch.setenv("MEMORY_SERVER_TTL_DAYS", '{"fact": 30.0}')
        get_settings.cache_clear()
        assert get_settings().ttl_days == {"fact": 30.0}

    def test_dict_fields_parse_nested_env(self, monkeypatch):
        # nested-only env (no parent JSON) → PARTIAL dict {"belief": 30.0}
        monkeypatch.setenv("MEMORY_SERVER_TTL_DAYS__BELIEF", "30")
        get_settings.cache_clear()
        assert get_settings().ttl_days == {"belief": 30.0}

    def test_dict_fields_json_env_coexists_with_nested_delimiter(self, monkeypatch):
        # JSON-only env still parses with env_nested_delimiter set
        monkeypatch.setenv("MEMORY_SERVER_TTL_DAYS", '{"fact": 30.0}')
        get_settings.cache_clear()
        assert get_settings().ttl_days == {"fact": 30.0}

    def test_dict_fields_json_and_nested_env_merge(self, monkeypatch):
        # pydantic-settings 2.14.2: nested value MERGES into the parent JSON dict
        # (nested wins on conflict) — it does NOT replace the whole dict
        monkeypatch.setenv("MEMORY_SERVER_TTL_DAYS", '{"fact": 30.0}')
        monkeypatch.setenv("MEMORY_SERVER_TTL_DAYS__BELIEF", "30")
        get_settings.cache_clear()
        assert get_settings().ttl_days == {"fact": 30.0, "belief": 30.0}

    def test_path_fields_accept_env_and_string(self, monkeypatch):
        monkeypatch.setenv("MEMORY_SERVER_LANCEDB_PATH", "custom/lancedb")
        get_settings.cache_clear()
        assert get_settings().lancedb_path == Path("custom/lancedb")
        assert Settings(lancedb_path=Path("data/lancedb")).lancedb_path == Path(
            "data/lancedb"
        )


# ---------------------------------------------------------------------------
# SQLite / OpenAI constructor compatibility
# ---------------------------------------------------------------------------


class TestConstructorCompat:
    def test_sqlite_no_arg_default_preserved(self):
        """SQLiteProvider() keeps its no-arg compatibility default (no
        Settings influence on no-arg construction)."""
        from memory_server.providers.sqlite_provider import SQLiteProvider

        provider = SQLiteProvider()
        assert provider._url == "sqlite+aiosqlite:///memory.db"

    def test_sqlite_busy_timeout_default_preserved(self):
        """SQLiteProvider() keeps the 5000 ms busy-timeout default."""
        from memory_server.providers.sqlite_provider import SQLiteProvider

        assert SQLiteProvider()._busy_timeout_ms == 5000

    def test_sqlite_busy_timeout_explicit_arg_wins(self):
        """Explicit busy_timeout_ms is honored without Settings involvement."""
        from memory_server.providers.sqlite_provider import SQLiteProvider

        assert SQLiteProvider(busy_timeout_ms=1234)._busy_timeout_ms == 1234

    @pytest.mark.asyncio
    async def test_sqlite_initialize_applies_busy_timeout_pragma(self):
        """The effective PRAGMA busy_timeout on the initialized engine matches
        the configured value (review finding #2)."""
        from memory_server.providers.sqlite_provider import SQLiteProvider

        provider = SQLiteProvider(
            url="sqlite+aiosqlite:///:memory:",
            busy_timeout_ms=9000,
        )
        await provider.initialize()
        try:
            engine = provider.engine
            assert engine is not None
            async with engine.connect() as conn:
                row = (await conn.exec_driver_sql("PRAGMA busy_timeout")).first()
                assert row is not None
                assert row[0] == 9000
        finally:
            await provider.close()

    def test_openai_no_arg_vector_size_preserved(self):
        from memory_server.providers.embedding_provider import OpenAIEmbeddingProvider

        assert OpenAIEmbeddingProvider()._vector_size == 1536

    def test_openai_env_override_changes_vector_size(self, monkeypatch):
        from memory_server.providers.embedding_provider import OpenAIEmbeddingProvider

        monkeypatch.setenv("MEMORY_SERVER_OPENAI_EMBEDDING_VECTOR_SIZE", "768")
        get_settings.cache_clear()
        assert OpenAIEmbeddingProvider()._vector_size == 768

    def test_openai_explicit_arg_wins_over_settings(self, monkeypatch):
        from memory_server.providers.embedding_provider import OpenAIEmbeddingProvider

        monkeypatch.setenv("MEMORY_SERVER_OPENAI_EMBEDDING_VECTOR_SIZE", "768")
        get_settings.cache_clear()
        assert OpenAIEmbeddingProvider(vector_size=512)._vector_size == 512

    def test_sentence_provider_no_arg_uses_settings_defaults(self, monkeypatch):
        from memory_server.providers.embedding_provider import (
            SentenceTransformerEmbeddingProvider,
        )

        monkeypatch.setenv("MEMORY_SERVER_EMBEDDING_MODEL", "custom-model")
        get_settings.cache_clear()
        provider = SentenceTransformerEmbeddingProvider()
        assert provider._model_name == "custom-model"
        assert provider._batch_size == 32
        assert provider._device is None


# ---------------------------------------------------------------------------
# Server wiring
# ---------------------------------------------------------------------------


def _reset_server_singletons():
    import memory_server.server as server_module

    server_module._provider = None
    server_module._qdrant = None
    server_module._lancedb = None
    server_module._embedder = None
    server_module._router = None
    server_module._graph = None
    server_module._graph_router = None
    server_module._hybrid_router = None
    server_module._outbox_worker = None
    server_module._outbox_task = None


class TestServerWiring:
    @pytest.fixture(autouse=True)
    def _isolate_server_data(self, tmp_path, monkeypatch):
        """Keep server wiring tests off the repo's real data files."""
        monkeypatch.setenv(
            "MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///:memory:"
        )
        monkeypatch.setenv(
            "MEMORY_SERVER_GRAPH_SNAPSHOT_PATH", str(tmp_path / "graph.json")
        )
        monkeypatch.setenv(
            "MEMORY_SERVER_LANCEDB_PATH", str(tmp_path / "lancedb")
        )
        get_settings.cache_clear()
        _reset_server_singletons()
        yield
        _reset_server_singletons()

    def test_db_url_env_override(self, monkeypatch):
        import memory_server.server as server_module

        monkeypatch.setenv(
            "MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///custom.db"
        )
        get_settings.cache_clear()
        assert server_module._get_sqlite_db_url() == "sqlite+aiosqlite:///custom.db"

    def test_graph_snapshot_path_env_override(self, monkeypatch, tmp_path):
        import memory_server.server as server_module

        expected = tmp_path / "snapshots" / "graph.json"
        monkeypatch.setenv("MEMORY_SERVER_GRAPH_SNAPSHOT_PATH", str(expected))
        get_settings.cache_clear()
        assert server_module._get_graph_snapshot_path() == expected

    @pytest.mark.asyncio
    async def test_lancedb_constructor_args_follow_settings(self, monkeypatch):
        import memory_server.server as server_module

        monkeypatch.setenv("MEMORY_SERVER_VECTOR_COLLECTION", "col-x")
        monkeypatch.setenv("MEMORY_SERVER_VECTOR_METRIC", "dot")
        monkeypatch.setenv("MEMORY_SERVER_VECTOR_SIZE", "512")
        get_settings.cache_clear()

        provider = await server_module._get_lancedb_provider()
        assert provider._table_name == "col-x"
        assert provider._vector_size == 512
        assert provider._metric == "dot"

    @pytest.mark.asyncio
    async def test_qdrant_constructor_args_follow_settings(self, monkeypatch):
        import memory_server.providers.qdrant_provider as qdrant_module
        import memory_server.server as server_module

        captured: dict[str, object] = {}

        class FakeQdrant:
            def __init__(self, **kwargs):
                captured.update(kwargs)

        monkeypatch.setattr(qdrant_module, "QdrantProvider", FakeQdrant)
        monkeypatch.setenv("MEMORY_SERVER_QDRANT_LOCATION", "localhost")
        monkeypatch.setenv("MEMORY_SERVER_QDRANT_PORT", "6334")
        monkeypatch.setenv("MEMORY_SERVER_QDRANT_PREFER_GRPC", "true")
        monkeypatch.setenv("MEMORY_SERVER_VECTOR_COLLECTION", "col-q")
        monkeypatch.setenv("MEMORY_SERVER_VECTOR_SIZE", "256")
        monkeypatch.setenv("MEMORY_SERVER_VECTOR_METRIC", "cosine")
        get_settings.cache_clear()

        await server_module._get_qdrant_provider()
        assert captured["location"] == "localhost"
        assert captured["port"] == 6334
        assert captured["prefer_grpc"] is True
        assert captured["collection"] == "col-q"
        assert captured["vector_size"] == 256
        assert captured["distance"] == "cosine"

    def test_embedder_sentence_branch_passes_settings(self, monkeypatch):
        import memory_server.server as server_module

        monkeypatch.setenv("MEMORY_SERVER_EMBEDDING_MODEL", "m1")
        monkeypatch.setenv("MEMORY_SERVER_EMBEDDING_BATCH_SIZE", "8")
        get_settings.cache_clear()

        from memory_server.providers.embedding_provider import (
            SentenceTransformerEmbeddingProvider,
        )

        embedder = server_module._build_embedder()
        assert isinstance(embedder, SentenceTransformerEmbeddingProvider)
        assert embedder._model_name == "m1"
        assert embedder._batch_size == 8

    def test_embedder_openai_branch_selected_by_env(self, monkeypatch):
        import memory_server.server as server_module

        monkeypatch.setenv("MEMORY_SERVER_EMBEDDING_PROVIDER", "openai")
        monkeypatch.setenv("MEMORY_SERVER_OPENAI_EMBEDDING_MODEL", "text-embedding-3-large")
        monkeypatch.setenv("MEMORY_SERVER_OPENAI_EMBEDDING_VECTOR_SIZE", "768")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        get_settings.cache_clear()

        from memory_server.providers.embedding_provider import OpenAIEmbeddingProvider

        embedder = server_module._build_embedder()
        assert isinstance(embedder, OpenAIEmbeddingProvider)
        assert embedder._model == "text-embedding-3-large"
        assert embedder._vector_size == 768
        assert embedder._api_key == "sk-test"

    @pytest.mark.asyncio
    async def test_outbox_worker_kwargs_follow_settings(self, monkeypatch):
        import memory_server.server as server_module

        captured: dict[str, object] = {}

        class FakeOutboxWorker:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            async def initialize(self):
                pass

        monkeypatch.setattr(server_module, "OutboxWorker", FakeOutboxWorker)
        monkeypatch.setenv("MEMORY_SERVER_OUTBOX_MAX_RETRIES", "5")
        monkeypatch.setenv("MEMORY_SERVER_OUTBOX_POLL_INTERVAL_SECONDS", "2.5")
        monkeypatch.setenv("MEMORY_SERVER_OUTBOX_POLL_BATCH_SIZE", "77")
        monkeypatch.setenv("MEMORY_SERVER_OUTBOX_FACT_BATCH_CHUNK_SIZE", "11")
        monkeypatch.setenv("MEMORY_SERVER_OUTBOX_COMPACT_INTERVAL_SECONDS", "3600")
        monkeypatch.setenv("MEMORY_SERVER_OUTBOX_COMPACT_CLEANUP_HOURS", "2")
        monkeypatch.setenv("MEMORY_SERVER_OUTBOX_STALE_PROCESSING_SECONDS", "900")
        monkeypatch.setenv("MEMORY_SERVER_OUTBOX_PROCESS_PENDING_LIMIT", "99")
        monkeypatch.setenv("MEMORY_SERVER_SQLITE_BUSY_TIMEOUT_MS", "9000")
        get_settings.cache_clear()

        await server_module._get_outbox_worker()
        assert captured["max_retries"] == 5
        assert captured["poll_interval_seconds"] == 2.5
        assert captured["poll_batch_size"] == 77
        assert captured["fact_batch_chunk_size"] == 11
        assert captured["compact_interval_seconds"] == 3600
        assert captured["compact_cleanup_hours"] == 2
        assert captured["stale_processing_seconds"] == 900
        assert captured["process_pending_limit"] == 99
        assert captured["busy_timeout_ms"] == 9000

    @pytest.mark.asyncio
    async def test_graph_router_depth_follows_settings(self, monkeypatch):
        import memory_server.server as server_module

        monkeypatch.setenv("MEMORY_SERVER_GRAPH_MAX_PATH_DEPTH", "9")
        get_settings.cache_clear()

        router = await server_module._get_graph_router()
        assert router._max_path_depth == 9

    @pytest.mark.asyncio
    async def test_graph_search_fn_path_depth_follows_env_override(self, monkeypatch):
        """Behavior-level regression for review finding #1: the server
        ``graph_search`` tool must pathfind with the env-overridden depth, not a
        hardcoded literal 4. A 6-node chain (5 edges) is unreachable at the
        default depth 4 but reachable with MEMORY_SERVER_GRAPH_MAX_PATH_DEPTH=9.
        """
        import memory_server.server as server_module
        from memory_server.providers.graph_provider import SimpleGraph

        graph = SimpleGraph()
        for i in range(6):
            graph.add_node(id=f"n{i}", type="node", name=f"N{i}")
        for i in range(5):
            graph.add_edge(source_id=f"n{i}", target_id=f"n{i + 1}", relation="next")

        async def _fake_get_graph():
            return graph

        monkeypatch.setattr(server_module, "_get_graph", _fake_get_graph)
        monkeypatch.setattr(server_module, "_graph", None)
        monkeypatch.setattr(server_module, "_graph_router", None)

        # Default depth (4) cannot reach the 5-edge target.
        get_settings.cache_clear()
        result = json.loads(
            await server_module.graph_search_fn(source_id="n0", target_id="n5")
        )
        assert result["paths"] == []

        # Env override (9) reaches it through the same production function.
        monkeypatch.setenv("MEMORY_SERVER_GRAPH_MAX_PATH_DEPTH", "9")
        get_settings.cache_clear()
        monkeypatch.setattr(server_module, "_graph_router", None)
        result = json.loads(
            await server_module.graph_search_fn(source_id="n0", target_id="n5")
        )
        assert result["paths"], "graph_search must find the 5-edge path under the env override"
        assert result["paths"][0][-1]["id"] == "n5"

    @pytest.mark.asyncio
    async def test_sqlite_provider_constructor_follows_settings(self, monkeypatch):
        """The server SQLiteProvider singleton receives the Settings-configured
        busy timeout (review finding #2)."""
        import memory_server.server as server_module

        captured: dict[str, object] = {}

        class FakeProvider:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            async def initialize(self):
                pass

        monkeypatch.setattr(server_module, "SQLiteProvider", FakeProvider)
        monkeypatch.setenv("MEMORY_SERVER_SQLITE_BUSY_TIMEOUT_MS", "9000")
        get_settings.cache_clear()

        await server_module._get_provider()
        assert captured["busy_timeout_ms"] == 9000

    def test_hybrid_internal_graph_router_keeps_default_depth(self, tmp_path, monkeypatch):
        """Documented Card 1 exception (SPEC acceptance 7): HybridRouter's
        internal GraphRouter keeps max_path_depth=4 even when the Settings
        override is set; direct constructions DO change."""
        from memory_server.providers.embedding_provider import MockEmbeddingProvider
        from memory_server.providers.graph_provider import SimpleGraph
        from memory_server.providers.lancedb_provider import LanceDBProvider
        from memory_server.router.hybrid_router import HybridRouter

        monkeypatch.setenv("MEMORY_SERVER_GRAPH_MAX_PATH_DEPTH", "9")
        get_settings.cache_clear()

        hybrid = HybridRouter(
            vector_provider=LanceDBProvider(db_path=str(tmp_path / "h")),
            embedder=MockEmbeddingProvider(),
            graph=SimpleGraph(),
        )
        assert hybrid._graph_router._max_path_depth == 4

        # Sanity: the direct construction path DOES change.
        from memory_server.router.graph_router import GraphRouter

        direct = GraphRouter(graph=SimpleGraph(), max_path_depth=get_settings().graph_max_path_depth)
        assert direct._max_path_depth == 9


# ---------------------------------------------------------------------------
# Tool defaults evaluated at registration (import-time)
# ---------------------------------------------------------------------------


class TestToolDefaults:
    def test_registered_tool_defaults_follow_settings(self):
        """Set env BEFORE importing memory_server.server → registered MCP
        tool defaults are Settings-driven (evaluated at registration)."""
        env = os.environ.copy()
        env.update(
            {
                "MEMORY_SERVER_SEARCH_DEFAULT_LIMIT": "7",
                "MEMORY_SERVER_CONTEXT_DEFAULT_LIMIT": "3",
                "MEMORY_SERVER_MIN_BELIEF_CONFIDENCE": "0.4",
                "MEMORY_SERVER_SEMANTIC_TOP_K": "5",
                "MEMORY_SERVER_SEMANTIC_SCORE_THRESHOLD": "0.2",
                "MEMORY_SERVER_BELIEF_SEARCH_LIMIT": "2",
                "MEMORY_SERVER_REFLECT_MIN_CONFIDENCE": "0.1",
                "MEMORY_SERVER_REFLECT_LIMIT": "9",
            }
        )
        code = (
            "import inspect, json\n"
            "import memory_server.server as s\n"
            "print(json.dumps({\n"
            "    'search': inspect.signature(s.search_tool).parameters['limit'].default,\n"
            "    'get_context': inspect.signature(s.get_context_tool).parameters['max_results'].default,\n"
            "    'learn': inspect.signature(s.learn_tool).parameters['min_belief_confidence'].default,\n"
            "    'semantic_top_k': inspect.signature(s.semantic_search_tool).parameters['top_k'].default,\n"
            "    'semantic_threshold': inspect.signature("
            "s.semantic_search_tool).parameters['score_threshold'].default,\n"
            "    'route_top_k': inspect.signature(s.route_tool).parameters['top_k'].default,\n"
            "    'get_belief': inspect.signature(s.get_belief_tool).parameters['limit'].default,\n"
            "    'reflect_min': inspect.signature(s.reflect_tool).parameters['min_confidence'].default,\n"
            "    'reflect_limit': inspect.signature(s.reflect_tool).parameters['limit'].default,\n"
            "}))\n"
        )
        proc = subprocess.run(
            [sys.executable, "-c", code],
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert proc.returncode == 0, proc.stderr
        got = json.loads(proc.stdout)
        assert got == {
            "search": 7,
            "get_context": 3,
            "learn": 0.4,
            "semantic_top_k": 5,
            "semantic_threshold": 0.2,
            "route_top_k": 5,
            "get_belief": 2,
            "reflect_min": 0.1,
            "reflect_limit": 9,
        }

    def test_default_values_match_today(self):
        """With no overrides the registered defaults equal the old literals."""
        import memory_server.server as server_module

        assert inspect.signature(server_module.search_tool).parameters["limit"].default == 50
        assert (
            inspect.signature(server_module.get_context_tool).parameters["max_results"].default
            == 10
        )
        assert (
            inspect.signature(server_module.learn_tool).parameters[
                "min_belief_confidence"
            ].default
            == 0.6
        )
        assert (
            inspect.signature(server_module.reflect_tool).parameters["limit"].default == 50
        )
        assert (
            inspect.signature(server_module.get_belief_tool).parameters["limit"].default
            == 10
        )


# ---------------------------------------------------------------------------
# Direct API boundary
# ---------------------------------------------------------------------------


class TestDirectApiBoundary:
    async def test_search_none_resolves_from_settings(self, monkeypatch):
        from memory_server.api.search import search

        calls: dict[str, object] = {}

        class RecorderProvider:
            async def search_facts(self, **kwargs):
                calls.update(kwargs)
                return []

        monkeypatch.setenv("MEMORY_SERVER_SEARCH_DEFAULT_LIMIT", "3")
        get_settings.cache_clear()

        await search(RecorderProvider(), query="x", limit=None)
        assert calls["limit"] == 3

        await search(RecorderProvider(), query="x", limit=9)
        assert calls["limit"] == 9

    async def test_get_context_none_resolves_from_settings(self, monkeypatch):
        from memory_server.api.get_context import get_context

        calls: list[tuple[str, dict[str, object]]] = []

        class RecorderProvider:
            async def search_facts(self, **kwargs):
                calls.append(("facts", kwargs))
                return []

            async def search_decisions(self, **kwargs):
                calls.append(("decisions", kwargs))
                return []

        monkeypatch.setenv("MEMORY_SERVER_CONTEXT_DEFAULT_LIMIT", "5")
        monkeypatch.setenv("MEMORY_SERVER_CONTEXT_DEDUP_OVERFETCH_FACTOR", "2")
        get_settings.cache_clear()

        await get_context(RecorderProvider(), task="x", max_results=None)
        facts_call = [c for c in calls if c[0] == "facts"][0][1]
        decisions_call = [c for c in calls if c[0] == "decisions"][0][1]
        assert facts_call["limit"] == 5
        assert decisions_call["limit"] == 10  # 5 * overfetch 2

        calls.clear()
        await get_context(RecorderProvider(), task="x", max_results=4)
        facts_call = [c for c in calls if c[0] == "facts"][0][1]
        decisions_call = [c for c in calls if c[0] == "decisions"][0][1]
        assert facts_call["limit"] == 4
        assert decisions_call["limit"] == 8


# ---------------------------------------------------------------------------
# Outbox worker wiring
# ---------------------------------------------------------------------------


class TestOutboxWorkerWiring:
    async def test_constructor_kwargs_become_instance_attrs(self):
        from storage.outbox_worker import OutboxWorker

        worker = OutboxWorker(
            db_url="sqlite+aiosqlite:///:memory:",
            max_retries=4,
            poll_interval_seconds=0.5,
            poll_batch_size=7,
            fact_batch_chunk_size=3,
            compact_interval_seconds=999,
            compact_cleanup_hours=2,
            stale_processing_seconds=123,
            process_pending_limit=9,
            busy_timeout_ms=1234,
        )
        assert worker._max_retries == 4
        assert worker._poll_interval_seconds == 0.5
        assert worker._poll_batch_size == 7
        assert worker._fact_batch_chunk_size == 3
        assert worker._compact_interval_seconds == 999
        assert worker._compact_cleanup_hours == 2
        assert worker._stale_processing_seconds == 123
        assert worker._process_pending_limit == 9
        assert worker._busy_timeout_ms == 1234

    async def test_busy_timeout_default_preserved(self):
        """OutboxWorker keeps the 5000 ms default when not configured."""
        from storage.outbox_worker import OutboxWorker

        assert OutboxWorker(db_url="sqlite+aiosqlite:///:memory:")._busy_timeout_ms == 5000

    async def test_initialize_applies_configured_busy_timeout(self):
        """The effective PRAGMA busy_timeout on the worker-owned engine matches
        the configured value (review finding #2)."""
        from storage.outbox_worker import OutboxWorker

        worker = OutboxWorker(
            db_url="sqlite+aiosqlite:///:memory:",
            busy_timeout_ms=9000,
        )
        await worker.initialize()
        try:
            engine = worker._engine
            assert engine is not None
            async with engine.connect() as conn:
                row = (await conn.exec_driver_sql("PRAGMA busy_timeout")).first()
                assert row is not None
                assert row[0] == 9000
        finally:
            await worker.close()

    async def test_poll_once_uses_stale_threshold_and_batch_size(self, monkeypatch):
        """reset_stale_processing receives max_age_seconds explicitly and
        get_pending receives the configured batch size."""
        import storage.outbox_worker as outbox_worker_module
        from storage.outbox_worker import OutboxWorker

        calls: dict[str, object] = {}

        class FakeRepo:
            def __init__(self, session):
                pass

            async def reset_stale_processing(self, max_age_seconds=None):
                calls["stale"] = max_age_seconds
                return 0

            async def get_pending(self, limit=None):
                calls["pending_limit"] = limit
                return []

        worker = OutboxWorker(
            db_url="sqlite+aiosqlite:///:memory:",
            stale_processing_seconds=123,
            poll_batch_size=7,
        )
        await worker.initialize()
        monkeypatch.setattr(outbox_worker_module, "OutboxRepository", FakeRepo)
        await worker._poll_once()
        assert calls == {"stale": 123, "pending_limit": 7}
        await worker.close()

    async def test_process_all_pending_uses_process_pending_limit(self, monkeypatch):
        import storage.outbox_worker as outbox_worker_module
        from storage.outbox_worker import OutboxWorker

        calls: dict[str, object] = {}

        class FakeRepo:
            def __init__(self, session):
                pass

            async def get_pending(self, limit=None):
                calls["pending_limit"] = limit
                return []

        worker = OutboxWorker(
            db_url="sqlite+aiosqlite:///:memory:",
            process_pending_limit=9,
        )
        await worker.initialize()
        monkeypatch.setattr(outbox_worker_module, "OutboxRepository", FakeRepo)
        await worker.process_all_pending()
        assert calls["pending_limit"] == 9
        await worker.close()
