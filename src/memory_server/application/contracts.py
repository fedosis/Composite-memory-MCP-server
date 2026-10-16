"""Frozen command DTOs and the single policy DTO for the application layer.

Every default below is taken FROM CODE, never from ARCHITECTURE_SOURCE §21
(``ARCHITECTURE_SOURCE.md:705-711``):

* ``RememberCommand.confidence = 1.0`` / ``source = "user"`` — the code defaults
  at ``api/remember.py:53``, ``:54``, matching
  ``services/ingestion_service.py:290-291`` and the C00 wire snapshot
  (``tests/fixtures/runtime_contracts.json``).
* ``LearnCommand.source = "user"``, ``extract_beliefs = False``,
  ``min_belief_confidence = 0.6`` — ``api/learn.py:16-18`` and
  ``services/ingestion_service.py:432-434``. The four ``llm_*`` fields default
  to ``None`` and mean "not supplied"; unconditional forwarding of ``None``
  stays forbidden (``api/learn.py:64-76``).
* ``BulkImportCommand.source = "MEMORY.md"`` — ``api/bulk_import.py:40-46``.

ARCHITECTURE_SOURCE §21 proposes a different remember default set (a lower
confidence, a different source string and an extra ``tags`` field). None of
those exists on the live path, so none of them is adopted here — this module
never spells those literals out, and a guard test fails if they leak back in.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from memory_server.application.errors import InvalidCommandError


@dataclass(frozen=True, slots=True)
class ReflectionPolicy:
    reflect_min_confidence: float = 0.0
    reflect_limit: int = 50
    reflect_belief_cap: int = 10000
    ttl_days: Mapping[str, float] = field(
        default_factory=lambda: {
            "fact": 90.0,
            "decision": 180.0,
            "skill": 365.0,
            "entity": 365.0,
            "belief": 180.0,
        }
    )
    decay_stale_ratio: float = 0.7
    decay_forgotten_ratio: float = 2.0
    decay_default_ttl_days: float = 90.0
    decay_forecast_window_days: float = 7.0

    @classmethod
    def capture(cls, settings: Any = None) -> ReflectionPolicy:
        if settings is None:
            from memory_server.settings import get_settings

            settings = get_settings()
        return cls(
            reflect_min_confidence=settings.reflect_min_confidence,
            reflect_limit=settings.reflect_limit,
            reflect_belief_cap=settings.reflect_belief_cap,
            ttl_days=dict(settings.ttl_days),
            decay_stale_ratio=settings.decay_stale_ratio,
            decay_forgotten_ratio=settings.decay_forgotten_ratio,
            decay_default_ttl_days=settings.decay_default_ttl_days,
            decay_forecast_window_days=settings.decay_forecast_window_days,
        )


@dataclass(frozen=True, slots=True)
class ReflectionCommand:
    mode: str = "overview"
    topic: str | None = None
    min_confidence: float | None = None
    limit: int | None = None
    context: str | None = None
    strict_same_context: bool = True


@dataclass(frozen=True, slots=True)
class RelationCommand:
    source_id: str
    target_id: str
    relation_type: str


@dataclass(frozen=True, slots=True)
class LifecycleCommand:
    memory_id: str
    memory_type: str
    to_state: str
    reason: str = ""
    triggered_by: str = "system"
    expected_version: str | int | None = None


@dataclass(frozen=True, slots=True)
class AuditCommand:
    audit_type: str = "full"


@dataclass(frozen=True, slots=True)
class RememberCommand:
    """Input DTO for the ``remember`` use case."""

    subject: str
    predicate: str
    object: str
    confidence: float = 1.0
    source: str = "user"
    metadata: dict[str, Any] | None = None
    admission: Any | None = None


@dataclass(frozen=True, slots=True)
class LearnCommand:
    """Input DTO for the ``learn`` use case."""

    text: str
    source: str = "user"
    extract_beliefs: bool = False
    min_belief_confidence: float = 0.6
    llm_extractor: Optional[Callable[[str], object]] = None
    llm_timeout_seconds: Optional[float] = None
    llm_max_input_chars: Optional[int] = None
    llm_confidence_gate: Optional[float] = None


@dataclass(frozen=True, slots=True)
class BulkImportCommand:
    """Input DTO for the ``bulk_import`` use case (library-only, not a tool)."""

    path: str | Path
    source: str = "MEMORY.md"
    gate: Any | None = None


@dataclass(frozen=True, slots=True)
class MemoryPolicy:
    """The single, narrow policy injection C06 introduces.

    Deliberately NOT ``RuntimeConfig`` (``runtime/config.py:52-75``): that DTO
    carries sqlite/vector/graph/embedding/outbox/evaluation/routing/layout and
    has no belief-policy field, and ``runtime/`` is outside this card's
    allowlist. ``min_belief_confidence`` stays a per-call parameter with the
    code default ``0.6``; the transport-level ambient default at
    ``server.py:536`` is C10's to move.
    """

    max_active_beliefs: int

    @classmethod
    def capture(cls, settings: Any = None) -> MemoryPolicy:
        """Capture the policy once; the only ambient settings read (``config.py:82-84``).

        The import is function-local and resolved at call time on purpose: a
        caller that patches ``memory_server.settings.get_settings`` must observe
        the read, which is what pins "captured once at construction" as a
        behavioural fact rather than an implementation detail.
        """
        if settings is None:
            from memory_server.settings import get_settings

            settings = get_settings()
        return cls(max_active_beliefs=settings.max_active_beliefs)


@dataclass(frozen=True, slots=True)
class SearchCommand:
    query: str = ""
    subject: str | None = None
    predicate: str | None = None
    limit: int | None = None
    include_inactive: bool = False


@dataclass(frozen=True, slots=True)
class ContextCommand:
    task: str
    subject: str | None = None
    max_results: int | None = None
    include_inactive: bool = False


@dataclass(frozen=True, slots=True)
class SemanticSearchCommand:
    query: str = ""
    top_k: int | None = None
    score_threshold: float | None = None


@dataclass(frozen=True, slots=True)
class RouteCommand:
    query: str = ""
    top_k: int | None = None
    score_threshold: float | None = None


@dataclass(frozen=True, slots=True)
class GraphSearchCommand:
    query: str = ""
    entity_id: str = ""
    source_id: str = ""
    target_id: str = ""
    relation_type: str = ""


@dataclass(frozen=True, slots=True)
class RetrievalPolicy:
    search_default_limit: int
    context_default_limit: int
    context_dedup_overfetch_factor: int
    semantic_top_k: int
    semantic_score_threshold: float

    @classmethod
    def capture(cls, settings: Any = None) -> RetrievalPolicy:
        if settings is None:
            from memory_server.settings import get_settings

            settings = get_settings()
        return cls(
            settings.search_default_limit,
            settings.context_default_limit,
            settings.context_dedup_overfetch_factor,
            settings.semantic_top_k,
            settings.semantic_score_threshold,
        )


@dataclass(frozen=True, slots=True)
class GraphPolicy:
    graph_max_path_depth: int

    @classmethod
    def capture(cls, settings: Any = None) -> GraphPolicy:
        if settings is None:
            from memory_server.settings import get_settings

            settings = get_settings()
        return cls(settings.graph_max_path_depth)


@dataclass(frozen=True, slots=True)
class BeliefPolicy:
    belief_search_limit: int

    @classmethod
    def capture(cls, settings: Any = None) -> BeliefPolicy:
        if settings is None:
            from memory_server.settings import get_settings

            settings = get_settings()
        return cls(settings.belief_search_limit)


@dataclass(frozen=True, slots=True)
class SetBeliefCommand:
    proposition: str
    confidence: float = 0.5
    sources: list[dict[str, Any]] | None = None
    tags: list[str] | None = None
    source: str = "system"
    replace_belief_id: str = ""


@dataclass(frozen=True, slots=True)
class GetBeliefCommand:
    proposition: str = ""
    lifecycle_state: str = "active"
    min_confidence: float = 0.0
    tags: list[str] | None = None
    source: str = ""
    creator: str = ""
    source_id: str = ""
    limit: int | None = None


@dataclass(frozen=True, slots=True)
class ResolveConflictCommand:
    belief_a_id: str
    belief_b_id: str
    resolution: str
    new_proposition: str = ""
    auto_resolve: bool = False


def validate_remember_command(command: RememberCommand) -> None:
    """Validate a ``RememberCommand``; mirrors ``ingestion_service.py:311-320``.

    The messages are byte-identical to the service's own validation so that
    ``pytest.raises(ValueError, match="subject")`` (``tests/test_remember.py:179``)
    and ``match="confidence"`` (``:188``) keep passing through the facade.
    """
    if not command.subject:
        raise InvalidCommandError("'subject' is required and cannot be empty")
    if not command.predicate:
        raise InvalidCommandError("'predicate' is required and cannot be empty")
    if not command.object:
        raise InvalidCommandError("'object' is required and cannot be empty")
    if command.confidence < 0.0 or command.confidence > 1.0:
        raise InvalidCommandError(
            f"'confidence' must be between 0.0 and 1.0, got {command.confidence}"
        )
