"""Central settings model for the CMMS memory server.

Single source of truth for every P0/P1 configuration value. Defaults are
byte-for-byte equal to the previously hardcoded values; this module is a
pure extraction — changing an env var changes behavior without code edits.

Precedence (Settings-resolved keys): canonical process env
(``MEMORY_SERVER_*``) > legacy alias env (``MEMORY_*``) > ``.env`` file >
field default. The Hermes plugin's ``db_url`` additionally consults the
YAML config inside ``HermesPluginConfig.from_dict`` before delegating here
(Context B: canonical env > YAML > legacy env > ``.env`` > default).

Secrets policy: API keys are NOT model fields. Providers read
``OPENAI_API_KEY`` / ``QDRANT_API_KEY`` from ``os.environ`` at use time via
the ``get_openai_api_key()`` / ``get_qdrant_api_key()`` accessors, so they
can never appear in ``model_dump()`` / JSON serialization.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Server-wide configuration, extracted from previously hardcoded values."""

    model_config = SettingsConfigDict(
        env_prefix="MEMORY_SERVER_",
        env_file=".env",
        extra="ignore",
        populate_by_name=True,
    )

    # --- Database ----------------------------------------------------------

    db_url: str = "sqlite+aiosqlite:///data/memory.db"

    graph_snapshot_path: Path = Field(
        default=Path("data/graph.json"),
        validation_alias=AliasChoices(
            "MEMORY_SERVER_GRAPH_SNAPSHOT_PATH",
            "MEMORY_GRAPH_SNAPSHOT_PATH",
        ),
    )

    # --- Vector store ------------------------------------------------------

    vector_backend: Literal["lancedb", "qdrant"] = Field(
        default="lancedb",
        validation_alias=AliasChoices(
            "MEMORY_SERVER_VECTOR_BACKEND",
            "MEMORY_VECTOR_BACKEND",
        ),
    )

    lancedb_path: Path = Path("data/lancedb")
    vector_collection: str = "memories"
    vector_size: int = 384
    vector_metric: str = "cosine"

    qdrant_location: str = Field(
        default=":memory:",
        validation_alias=AliasChoices(
            "MEMORY_SERVER_QDRANT_LOCATION",
            "MEMORY_QDRANT_URL",
        ),
    )
    qdrant_port: int = 6333
    qdrant_prefer_grpc: bool = False

    # --- Embeddings --------------------------------------------------------

    embedding_provider: Literal["sentence-transformers", "openai"] = (
        "sentence-transformers"
    )
    embedding_model: str = "all-MiniLM-L6-v2"
    embedding_device: str | None = None
    embedding_batch_size: int = 32
    openai_embedding_model: str = "text-embedding-3-small"
    openai_embedding_vector_size: int = 1536
    embedding_base_url: str | None = None

    # --- Outbox worker -----------------------------------------------------

    outbox_max_retries: int = 3
    outbox_poll_interval_seconds: float = 1.0
    outbox_poll_batch_size: int = 500
    outbox_fact_batch_chunk_size: int = 32
    outbox_compact_interval_seconds: int = 1800
    outbox_compact_cleanup_hours: int = 1
    outbox_process_pending_limit: int = 500
    outbox_stale_processing_seconds: int = 600
    sqlite_busy_timeout_ms: int = 5000

    # --- Beliefs / learning ------------------------------------------------

    max_active_beliefs: int = 500
    min_belief_confidence: float = 0.6

    # --- Search / context limits -------------------------------------------

    search_default_limit: int = 50
    context_default_limit: int = 10
    context_dedup_overfetch_factor: int = 4
    semantic_top_k: int = 10
    semantic_score_threshold: float = 0.0
    belief_search_limit: int = 10
    reflect_min_confidence: float = 0.0
    reflect_limit: int = 50
    max_contradiction_pairs: int = 100_000
    reflect_belief_cap: int = 10000
    graph_max_path_depth: int = 4

    # --- Decay engine -------------------------------------------------------

    ttl_days: dict[str, float] = Field(
        default_factory=lambda: {
            "fact": 90.0,
            "decision": 180.0,
            "skill": 365.0,
            "entity": 365.0,
            "belief": 180.0,
        }
    )
    decay_archive_threshold: float = 0.2
    decay_stale_ratio: float = 0.7
    decay_archive_ratio: float = 1.0
    decay_forgotten_ratio: float = 2.0
    decay_default_ttl_days: float = 90.0
    decay_confidence_floor: float = 0.1
    decay_base: float = 2.0
    decay_forecast_window_days: float = 7.0

    # --- Confidence engine --------------------------------------------------

    confidence_source_reliability: dict[str, float] = Field(
        default_factory=lambda: {
            "verified": 0.9,
            "admin": 0.85,
            "inferred": 0.7,
            "extracted": 0.6,
            "unknown": 0.3,
        }
    )
    confidence_ttl_days: float = 90.0
    confidence_lifecycle_multipliers: dict[str, float] = Field(
        default_factory=lambda: {
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
        }
    )
    confidence_corroboration_boost: float = 0.10
    confidence_boost_threshold: int = 2
    confidence_conflict_penalty: float = 0.20
    confidence_penalty_threshold: int = 1

    # --- Admission gate ------------------------------------------------------

    admission_default_ttl_days: int = 365
    admission_default_score: float = 0.65
    admission_ephemeral_ttl_days: int = 1
    admission_ephemeral_score: float = 0.05
    admission_important_score: float = 0.95

    # --- Validators ----------------------------------------------------------

    @field_validator(
        "vector_size",
        "openai_embedding_vector_size",
        "outbox_poll_batch_size",
        "outbox_poll_interval_seconds",
    )
    @classmethod
    def _strictly_positive(cls, v: int | float) -> int | float:
        if v <= 0:
            raise ValueError("must be > 0")
        return v

    @field_validator("outbox_max_retries", "sqlite_busy_timeout_ms")
    @classmethod
    def _non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("must be >= 0")
        return v

    @field_validator(
        "min_belief_confidence",
        "semantic_score_threshold",
        "reflect_min_confidence",
        "decay_archive_threshold",
        "decay_confidence_floor",
        "admission_default_score",
        "admission_ephemeral_score",
        "admission_important_score",
        "confidence_corroboration_boost",
        "confidence_conflict_penalty",
    )
    @classmethod
    def _unit_interval(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError("must be in [0, 1]")
        return v

    @field_validator(
        "ttl_days",
        "confidence_source_reliability",
        "confidence_lifecycle_multipliers",
    )
    @classmethod
    def _non_empty_str_float_dict(cls, v: dict[str, float]) -> dict[str, float]:
        if not v:
            raise ValueError("must not be empty")
        for key, value in v.items():
            if not isinstance(key, str):
                raise ValueError("keys must be strings")
            if not isinstance(value, (int, float)):
                raise ValueError("values must be numbers")
        return v


@lru_cache
def get_settings() -> Settings:
    """Return the process-global Settings instance (cached)."""
    return Settings()


def get_openai_api_key() -> str | None:
    """Return the OpenAI API key from the environment (never a model field)."""
    return os.environ.get("OPENAI_API_KEY")


def get_qdrant_api_key() -> str | None:
    """Return the Qdrant API key from the environment (never a model field)."""
    return os.environ.get("QDRANT_API_KEY")
