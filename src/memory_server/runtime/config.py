"""Deep-frozen configuration snapshot for the neutral CMMS runtime.

The snapshot is captured ONCE at ``create()`` and then never re-read: the runtime
builds every resource from these values, so mutating ``os.environ`` or the
process-global ``get_settings()`` cache (``settings.py:390-393``) afterwards
cannot change a dependency of an existing runtime.

Secrets policy (``settings.py:13-16``, accessors ``settings.py:396-408``): API
keys are never model fields. They stay in the environment, are read only through
``get_openai_api_key`` / ``get_qdrant_api_key`` / ``get_llm_api_key`` at the point
of use, and never enter this snapshot, a DTO derived from it, or any log/report
line.

Nested mappings are COPIED into ``MappingProxyType`` (the pattern of
``paths.py:389`` and ``plugins/hermes/config.py:476-497``), so the frozen DTO
cannot be mutated through the live ``Settings`` object or vice versa.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from memory_server.paths import serialize_layout_redacted
from memory_server.settings import get_settings


def _deep_freeze(value: Any) -> Any:
    """Copy and freeze a nested value; never alias the live object."""
    if isinstance(value, Mapping):
        return MappingProxyType({key: _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, (tuple, list, set, frozenset)):
        return tuple(_deep_freeze(item) for item in value)
    if isinstance(value, (str, bytes, int, float, bool, type(None), Path)):
        return value
    return value


def _thaw(value: Any) -> Any:
    """JSON-able rendering used by the secrets-free DTO."""
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_thaw(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


@dataclass(frozen=True)
class RuntimeConfig:
    """Immutable, deep-frozen recipe input captured from Settings + layout.

    ``layout`` is the already-frozen ``StorageLayout`` (``paths.py:107-119``); it
    is captured as-is, never re-resolved and never deep-copied.
    """

    sqlite_url: str
    sqlite_busy_timeout_ms: int
    vector_backend: str
    vector_local_path: Path | None
    vector_collection: str
    vector_metric: str
    vector_size: int
    qdrant_location: str
    qdrant_port: int
    qdrant_prefer_grpc: bool
    graph_snapshot_path: Path
    embedding: Mapping[str, Any]
    outbox: Mapping[str, Any]
    evaluation: Mapping[str, Any]
    routing: Mapping[str, Any]
    layout: Any

    def __post_init__(self) -> None:
        for name in ("embedding", "outbox", "evaluation", "routing"):
            object.__setattr__(self, name, _deep_freeze(getattr(self, name)))

    @classmethod
    def capture(cls, layout: Any, settings: Any = None) -> RuntimeConfig:
        """Capture the explicit snapshot; the ONLY ambient settings read."""
        resolved = settings if settings is not None else get_settings()
        return cls(
            sqlite_url=layout.sqlite.effective_url,
            sqlite_busy_timeout_ms=resolved.sqlite_busy_timeout_ms,
            vector_backend=layout.vector.backend,
            vector_local_path=layout.vector.local_path,
            vector_collection=resolved.vector_collection,
            vector_metric=resolved.vector_metric,
            vector_size=resolved.vector_size,
            qdrant_location=resolved.qdrant_location,
            qdrant_port=resolved.qdrant_port,
            qdrant_prefer_grpc=resolved.qdrant_prefer_grpc,
            graph_snapshot_path=layout.graph_snapshot_path,
            embedding={
                "provider": resolved.embedding_provider,
                "model": resolved.embedding_model,
                "device": resolved.embedding_device,
                "batch_size": resolved.embedding_batch_size,
                "openai_model": resolved.openai_embedding_model,
                "openai_vector_size": resolved.openai_embedding_vector_size,
                "base_url": resolved.embedding_base_url,
            },
            outbox={
                "max_retries": resolved.outbox_max_retries,
                "poll_interval_seconds": resolved.outbox_poll_interval_seconds,
                "poll_batch_size": resolved.outbox_poll_batch_size,
                "fact_batch_chunk_size": resolved.outbox_fact_batch_chunk_size,
                "compact_interval_seconds": resolved.outbox_compact_interval_seconds,
                "compact_cleanup_hours": resolved.outbox_compact_cleanup_hours,
                "stale_processing_seconds": resolved.outbox_stale_processing_seconds,
                "process_pending_limit": resolved.outbox_process_pending_limit,
                "busy_timeout_ms": resolved.sqlite_busy_timeout_ms,
            },
            evaluation={
                "ttl_days": dict(resolved.ttl_days),
                "decay": {
                    "archive_threshold": resolved.decay_archive_threshold,
                    "stale_ratio": resolved.decay_stale_ratio,
                    "archive_ratio": resolved.decay_archive_ratio,
                    "forgotten_ratio": resolved.decay_forgotten_ratio,
                    "default_ttl_days": resolved.decay_default_ttl_days,
                    "confidence_floor": resolved.decay_confidence_floor,
                    "base": resolved.decay_base,
                    "forecast_window_days": resolved.decay_forecast_window_days,
                },
                "confidence": {
                    "source_reliability": dict(resolved.confidence_source_reliability),
                    "ttl_days": resolved.confidence_ttl_days,
                    "lifecycle_multipliers": dict(resolved.confidence_lifecycle_multipliers),
                    "corroboration_boost": resolved.confidence_corroboration_boost,
                    "boost_threshold": resolved.confidence_boost_threshold,
                    "conflict_penalty": resolved.confidence_conflict_penalty,
                    "penalty_threshold": resolved.confidence_penalty_threshold,
                },
            },
            routing={"graph_max_path_depth": resolved.graph_max_path_depth},
            layout=layout,
        )

    def as_dict(self) -> dict[str, Any]:
        """Secrets-free DTO; the layout is redacted through ``paths.py``."""
        dto: dict[str, Any] = {}
        for field in fields(self):
            value = getattr(self, field.name)
            dto[field.name] = serialize_layout_redacted(value) if field.name == "layout" else _thaw(value)
        return dto

    def describe(self) -> dict[str, Any]:
        """Alias used by reports/logs: redacted and secret-free by construction."""
        return self.as_dict()
