"""C0/C4 strict wire DTOs; C1 command/result projections. No runtime imports."""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Annotated, Generic, Literal, TypeVar, Union

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Discriminator,
    Field,
    JsonValue,
    Tag,
    create_model,
    model_validator,
)


def _utc(value: datetime) -> datetime:
    if value.utcoffset() != timedelta(0):
        raise ValueError("UTC timestamp required")
    return value


def _finite_json(value: JsonValue) -> JsonValue:
    def walk(item: JsonValue) -> None:
        if isinstance(item, float) and not math.isfinite(item):
            raise ValueError("finite JSON number required")
        if isinstance(item, list):
            for child in item:
                walk(child)
        if isinstance(item, dict):
            for child in item.values():
                walk(child)

    walk(value)
    return value


def _true(value: object) -> Literal[True]:
    # Pydantic Literal[True] otherwise also accepts integer 1 (C0 strict).
    if value is not True:
        raise ValueError("literal true required")
    return True


TrueLiteral = Annotated[Literal[True], BeforeValidator(_true)]
UTCDatetime = Annotated[datetime, AfterValidator(_utc)]
JSONValue = Annotated[JsonValue, AfterValidator(_finite_json)]
MemoryType = Literal["fact", "belief", "decision", "skill"]
Resolution = Literal["keep_a", "keep_b", "merge", "mark_contradicted", "discard_both", "defer"]
JobOperation = Literal["outbox_replay", "vector_rebuild", "graph_rebuild", "projections_rebuild"]


class DTO(BaseModel):
    """C0 strict, finite, closed wire object. No invented timestamp defaults."""

    model_config = ConfigDict(strict=True, extra="forbid", allow_inf_nan=False, populate_by_name=True)


T = TypeVar("T")

Permission = Literal[
    "audit.read",
    "belief.read",
    "belief.resolve",
    "belief.write",
    "graph.read",
    "index.manage",
    "memory.invalidate",
    "memory.read",
    "memory.reveal",
    "memory.transition",
    "memory.write",
    "profile.manage",
    "profile.read",
    "relation.read",
    "relation.write",
    "token.manage",
]

ErrorCode = Literal[
    "E_AUTH_REQUIRED",
    "E_PERMISSION_DENIED",
    "E_PROFILE_NOT_FOUND",
    "E_PROFILE_DISABLED",
    "E_WRITER_ACTIVE",
    "E_VERSION_CONFLICT",
    "E_VALIDATION",
    "E_ADMISSION_REJECTED",
    "E_NOT_FOUND",
    "E_STORAGE",
    "E_VECTOR_BACKEND",
    "E_GRAPH_BACKEND",
    "E_OUTBOX",
    "E_RATE_LIMIT",
    "E_REQUEST_TOO_LARGE",
    "E_IDEMPOTENCY_REQUIRED",
    "E_IDEMPOTENCY_CONFLICT",
    "E_REQUEST_IN_PROGRESS",
    "E_PRECONDITION_REQUIRED",
    "E_CURSOR_INVALID",
    "E_CURSOR_EXPIRED",
    "E_CAPABILITY_UNAVAILABLE",
    "E_RUNTIME_NOT_READY",
    "E_RUNTIME_CLOSING",
    "E_RUNTIME_FAILED",
    "E_PROFILE_BUSY",
    "E_AUDIT_UNAVAILABLE",
    "E_TIMEOUT",
    "E_CSRF",
    "E_UNSUPPORTED_MEDIA_TYPE",
    "E_METHOD_NOT_ALLOWED",
    "E_INTERNAL",
    "E_PROJECTION_UNAVAILABLE",
    "E_MAINTENANCE_CONFLICT",
    "E_JOB_INTERRUPTED",
    "E_EVENT_CURSOR_EXPIRED",
]

EventType = Literal[
    "memory.created",
    "memory.updated",
    "memory.transitioned",
    "memory.invalidated",
    "belief.conflict_detected",
    "relation.created",
    "relation.deleted",
    "outbox.pending",
    "outbox.applied",
    "index.degraded",
    "profile.lock_error",
    "audit.warning",
    "vector.indexed",
    "graph.indexed",
    "job.updated",
    "stream.reset",
]

Operation = Literal[
    "health",
    "ready",
    "profiles.list",
    "profiles.get",
    "profiles.enable",
    "profiles.disable",
    "tokens.create",
    "tokens.list",
    "tokens.revoke",
    "memories.list",
    "memories.get",
    "receipts.get",
    "history.list",
    "lookup",
    "memories.reveal",
    "search",
    "context",
    "relations.for_memory",
    "relations.list",
    "relations.get",
    "graph.neighborhood",
    "graph.path",
    "graph.entity",
    "audit",
    "indexes.status",
    "outbox.list",
    "overview",
    "reflect",
    "traces.get",
    "traces.list",
    "memories.create",
    "learn",
    "memories.transition",
    "memories.invalidate",
    "beliefs.list",
    "beliefs.get",
    "beliefs.set",
    "conflicts.list",
    "conflicts.resolve",
    "relations.create",
    "relations.delete",
    "admission.preview",
    "mutations.preview",
    "jobs.get",
    "outbox.replay",
    "indexes.rebuild",
    "events",
    "sessions.create",
    "sessions.get",
    "sessions.delete",
    "quality",
    "benchmarks.list",
    "benchmarks.get",
]


class Empty(DTO):
    pass


class Health(DTO):
    status: Literal["ok", "unavailable"]


class PageQuery(DTO):
    limit: int = Field(default=50, ge=1, le=200)
    cursor: str | None = Field(default=None)


class Page(DTO, Generic[T]):
    items: list[T]
    next_cursor: str | None
    has_more: bool
    request_id: str


class Profile(DTO):
    id: str
    display_name: str
    description: str | None
    enabled: bool
    generation: int
    runtime_state: Literal["not_started", "ready", "closing", "failed"]
    capabilities: list[str]
    storage_root: str | None


class ControlMutation(DTO):
    reason: str = Field(min_length=1, max_length=2048)
    confirmation: TrueLiteral
    expected_generation: int = Field(ge=1)


class TokenCreate(DTO):
    subject: str = Field(min_length=1, max_length=128)
    profiles: list[str] = Field(min_length=1, max_length=100)
    permissions: list[Permission]
    expires_at: UTCDatetime


class TokenInfo(DTO):
    id: str
    subject: str
    profiles: list[str]
    permissions: list[Permission]
    created_at: UTCDatetime
    expires_at: UTCDatetime
    last_used_at: UTCDatetime | None
    revoked_at: UTCDatetime | None
    generation: int


class TokenCreated(DTO):
    token: TokenInfo
    secret: str | None


class MemoryListQuery(DTO):
    limit: int = Field(default=50, ge=1, le=200)
    cursor: str | None
    memory_types: list[MemoryType] = Field(default_factory=list)
    lifecycle_states: list[str] = Field(default_factory=list)
    min_confidence: float | None = Field(ge=0, le=1)
    max_confidence: float | None = Field(ge=0, le=1)
    source: str | None
    tags: list[str] = Field(default_factory=list)
    authority_level: str | None
    epistemic_status: str | None
    created_from: UTCDatetime | None
    created_to: UTCDatetime | None
    updated_from: UTCDatetime | None
    updated_to: UTCDatetime | None
    valid_at: UTCDatetime | None
    risk_tags: list[str] = Field(default_factory=list)


class FactPayload(DTO):
    subject: str
    predicate: str
    object: str


class BeliefPayload(DTO):
    proposition: str
    tags: list[str]
    source_ids: list[str]


class DecisionPayload(DTO):
    context: str
    choice: str
    rejected_alternatives: list[str]
    reason: str


class SkillPayload(DTO):
    name: str
    purpose: str
    steps: list[str]
    constraints: list[str]
    validation: list[str]
    success_rate: float
    skill_version: str


class Memory(DTO):
    id: str
    memory_type: MemoryType
    content: str | None
    payload: FactPayload | BeliefPayload | DecisionPayload | SkillPayload | None
    confidence: float = Field(ge=0, le=1)
    lifecycle_state: str
    revision: int | None = Field(default=None)
    source_version: str | int
    source: str | None = Field(default=None)
    creator: str | None = Field(default=None)
    created_at: UTCDatetime
    updated_at: UTCDatetime
    valid_from: UTCDatetime | None = Field(default=None)
    valid_to: UTCDatetime | None = Field(default=None)
    epistemic_status: str | None = Field(default=None)
    authority_level: str | None = Field(default=None)
    risk_tags: list[str]
    receipt_id: str | None = Field(default=None)
    masked: bool
    projection: ItemProjection


class ItemProjection(DTO):
    vector: Literal["indexed", "pending", "missing", "unknown"]
    graph: Literal["indexed", "pending", "missing", "unknown"]
    outbox: Literal["pending", "processing", "applied", "failed", "unknown"]


class Receipt(DTO):
    id: str
    memory_type: MemoryType
    source: str
    created_by: str
    timestamp: UTCDatetime
    confidence: float = Field(ge=0, le=1)
    verification_status: str
    lifecycle_state: str
    source_version: str | int
    history: list[HistoryEvent]
    history_complete: bool


class HistoryEvent(DTO):
    id: str
    kind: Literal["receipt", "lifecycle", "relation", "confidence", "admission"]
    timestamp: UTCDatetime | None = Field(default=None)
    actor: str | None = Field(default=None)
    reason: str | None = Field(default=None)
    revision: int | None = Field(default=None)
    from_state: str | None = Field(default=None)
    to_state: str | None = Field(default=None)
    receipt_id: str | None = Field(default=None)
    evidence: EvidenceSummary | None = Field(default=None)


class EvidenceSummary(DTO):
    method: str | None = Field(default=None)
    sources: list[str]
    session_id: str | None = Field(default=None)
    confidence: float | None = Field(default=None, ge=0, le=1)
    source_date: str | None = Field(default=None)
    derived_from: list[str]
    claim_type: Literal["fact", "authority", "state"] | None = Field(default=None)


class LookupQuery(DTO):
    kind: Literal["memory", "receipt", "relation", "trace", "request"]
    id: str


class LookupResult(DTO):
    kind: str
    id: str
    target: Memory | Receipt | Relation | Trace | AccessAudit
    found: TrueLiteral


class RevealRequest(DTO):
    reason: str = Field(min_length=1, max_length=2048)
    memory_type: MemoryType
    expected_revision: Annotated[int, Field(ge=1)] | None


class SearchFilters(DTO):
    memory_types: list[MemoryType] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    lifecycle_states: list[str] = Field(default_factory=list)
    source: str | None
    min_confidence: float | None = Field(ge=0, le=1)


class SearchRequest(DTO):
    query: str = Field(min_length=1, max_length=4096)
    mode: Literal["keyword", "semantic", "graph", "hybrid"] = Field(default="hybrid")
    limit: int = Field(default=20, ge=1, le=100)
    include_inactive: bool = Field(default=False)
    filters: SearchFilters
    trace: bool = Field(default=False)


class SearchHit(DTO):
    memory: Memory
    source: Literal["fts", "semantic", "graph"]
    score: float
    rank: int = Field(ge=1)


class SearchResult(DTO):
    request_id: str
    results: list[SearchHit]
    total: int
    trace_id: str | None
    degraded: list[SourceFailure]


class SourceFailure(DTO):
    source: Literal["fts", "semantic", "graph"]
    code: ErrorCode


class ContextRequest(DTO):
    task: str = Field(min_length=1, max_length=4096)
    subject: str | None
    max_results: int = Field(default=20, ge=1, le=100)
    include_inactive: bool = Field(default=False)
    trace: bool = Field(default=False)


class ContextResult(DTO):
    request_id: str
    package: ContextPackage
    memories: list[Memory]
    trace_id: str | None
    estimated_tokens: int | None = Field(default=None)
    tokenizer: str | None = Field(default=None)
    token_count_is_estimate: bool


class ContextPackage(DTO):
    facts: list[LegacyFact]
    decisions: list[LegacyDecision]
    total: int


class Relation(DTO):
    id: str
    source_id: str
    target_id: str
    relation_type: Literal["supports", "contradicts", "derives", "derived_from"]
    canonical: TrueLiteral
    revision: int
    created_at: UTCDatetime


class NeighborhoodQuery(DTO):
    memory_id: str | None
    entity_id: str | None
    depth: int = Field(default=2, ge=1, le=4)
    limit: int = Field(default=100, ge=1, le=500)
    edge_limit: int = Field(default=200, ge=1, le=1000)
    relation_types: list[str] = Field(default_factory=list)
    memory_types: list[MemoryType] = Field(default_factory=list)
    lifecycle_states: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _one_root(self) -> NeighborhoodQuery:
        if (self.memory_id is None) == (self.entity_id is None):
            raise ValueError("exactly one root required")
        return self


class PathRequest(DTO):
    source_id: str
    target_id: str
    max_depth: int = Field(default=4, ge=1, le=4)
    relation_types: list[str] = Field(default_factory=list)
    node_limit: int = Field(default=500, ge=1, le=500)
    edge_limit: int = Field(default=1000, ge=1, le=1000)


class GraphNode(DTO):
    id: str
    kind: Literal["entity", "fact", "belief", "decision", "skill"]
    label: str
    lifecycle_state: str | None
    masked: bool


class GraphEdge(DTO):
    id: str
    source_id: str
    target_id: str
    relation_type: str
    canonical: bool
    canonical_relation_id: str | None
    revision: int | None


class GraphConsistency(DTO):
    canonical_store: Literal["sqlite"]
    projection: str
    outbox_lag: int | None = Field(default=None)
    canonical_edges: int | None = Field(default=None)
    projected_edges: int | None = Field(default=None)
    missing_projection: int | None = Field(default=None)
    extra_projection: int | None = Field(default=None)
    last_sync: UTCDatetime | None = Field(default=None)


class GraphResult(DTO):
    nodes: list[GraphNode]
    edges: list[GraphEdge]
    paths: list[list[str]]
    graph_consistency: GraphConsistency
    truncated: bool
    truncation_reason: Literal["nodes", "edges", "depth", "timeout"] | None
    request_id: str


class AuditQuery(DTO):
    audit_type: Literal["full", "lifecycle", "relations", "receipts", "indexes"] = Field(default="full")
    limit: int = Field(default=50, ge=1, le=200)
    cursor: str | None


class AuditFinding(DTO):
    id: str
    category: str
    severity: Literal["info", "warning", "error"]
    object_kind: str | None
    object_id: str | None
    code: str
    aggregate: bool


class AuditResult(DTO):
    findings: Page[AuditFinding]
    sections: dict[str, Literal["ok", "warning", "unavailable"]]
    sampled_at: UTCDatetime
    complete: bool


class IndexStatus(DTO):
    sqlite_records: int
    vector_records: int | None = Field(default=None)
    graph_relations: int | None = Field(default=None)
    vector_lag: int | None = Field(default=None)
    graph_lag: int | None = Field(default=None)
    outbox_pending: int
    outbox_failed: int
    graph_consistency: GraphConsistency
    last_success: UTCDatetime | None = Field(default=None)
    sampled_at: UTCDatetime


class OutboxQuery(DTO):
    limit: int = Field(default=50, ge=1, le=200)
    cursor: str | None
    status: Literal["pending", "processing", "applied", "failed"] | None
    memory_id: str | None


class OutboxItem(DTO):
    event_id: str
    memory_id: str
    record_type: MemoryType
    operation: str
    status: Literal["pending", "processing", "applied", "failed"]
    attempts: int
    target_backend: str | None = Field(default=None)
    last_error_code: str | None = Field(default=None)
    next_retry: UTCDatetime | None = Field(default=None)
    created_at: UTCDatetime
    processed_at: UTCDatetime | None = Field(default=None)


class Overview(DTO):
    profile: Profile
    facts: int
    beliefs: int
    decisions: int
    skills: int
    lifecycle_counts: dict[str, int]
    unresolved_conflicts: int | None = Field(default=None)
    audit_warnings: int | None = Field(default=None)
    indexes: IndexStatus
    schema_revision: str | None = Field(default=None)
    last_migration: UTCDatetime | None = Field(default=None)
    writer_owned: bool


class ReflectRequest(DTO):
    mode: Literal["overview", "contradictions", "decay", "topics", "evidence_audit", "confidence", "relations"] = Field(
        default="overview"
    )
    topic: str | None
    min_confidence: float | None = Field(ge=0, le=1)
    limit: int = Field(default=50, ge=1, le=100)
    context: str | None
    strict_same_context: bool = Field(default=True)


class ReflectResult(DTO):
    mode: str
    sections: dict[str, JSONValue]
    complete: bool
    request_id: str


class TraceContribution(DTO):
    memory_id: str | None = Field(default=None)
    source: Literal["fts", "semantic", "graph"]
    raw_score: float | None = Field(default=None)
    normalized_score: float | None = Field(default=None)
    rank_before_merge: int | None = Field(default=None)
    rank_after_merge: int | None = Field(default=None)
    matched_terms: list[str]
    relation_path: list[str]
    dedup_winner_id: str | None = Field(default=None)


class Trace(DTO):
    id: str
    request_id: str
    profile_id: str
    operation: Literal["search", "context"]
    query: str | None
    stages: dict[str, list[TraceContribution]]
    stage_status: dict[str, Literal["ran", "skipped", "failed"]]
    merge: list[TraceContribution]
    deduplicated: list[TraceContribution]
    final: list[TraceContribution]
    created_at: UTCDatetime
    expires_at: UTCDatetime
    settings_digest: str
    code_revision: str


class TraceSummary(DTO):
    id: str
    request_id: str
    operation: str
    created_at: UTCDatetime
    expires_at: UTCDatetime
    settings_digest: str
    code_revision: str


class RememberRequest(DTO):
    type: Literal["fact"]
    subject: str = Field(min_length=1, max_length=8192)
    predicate: str = Field(min_length=1, max_length=8192)
    object: str = Field(min_length=1, max_length=8192)
    confidence: float = Field(default=0.95, ge=0, le=1)
    source: str = Field(default="agent")
    tags: list[str] = Field(default_factory=list)
    evidence: EvidenceSummary | None
    expected_version: Annotated[int, Field(ge=1)] | None


class LearnRequest(DTO):
    text: str = Field(min_length=1, max_length=32768)
    source: str = Field(default="agent")
    extract_beliefs: bool = Field(default=False)
    min_belief_confidence: float = Field(default=0.6, ge=0, le=1)
    expected_versions: dict[str, Annotated[int, Field(ge=1)]] = Field(default_factory=dict)


class WriteResult(DTO):
    request_id: str
    memories: list[Memory]
    receipts: list[Receipt]
    admission: list[AdmissionResult]
    outbox_ids: list[str]
    reinforced_ids: list[str]


class TransitionRequest(DTO):
    memory_type: Literal["fact", "belief"]
    to_state: str
    reason: str = Field(min_length=1, max_length=2048)
    expected_version: Annotated[int, Field(ge=1)] = Field(ge=1)
    confirmation: TrueLiteral


class InvalidateRequest(DTO):
    memory_type: Literal["fact", "belief"]
    reason: str = Field(min_length=1, max_length=2048)
    expected_version: Annotated[int, Field(ge=1)] = Field(ge=1)
    confirmation: TrueLiteral


class Propagation(DTO):
    memory_id: str
    action: Literal["confidence_demoted", "transitioned"]
    reason: str
    revision: int


class TransitionResult(DTO):
    memory_id: str
    memory: Memory
    from_state: str
    to_state: str
    propagated: list[Propagation]
    receipt_id: str | None
    audit_id: str
    request_id: str


class BeliefListQuery(DTO):
    limit: int = Field(default=50, ge=1, le=200)
    cursor: str | None
    proposition: str | None
    lifecycle_state: str = Field(default="active")
    min_confidence: float = Field(default=0, ge=0, le=1)
    tags: list[str] = Field(default_factory=list)
    source: str | None
    creator: str | None
    source_id: str | None


class BeliefRequest(DTO):
    proposition: str = Field(min_length=1, max_length=2048)
    confidence: float = Field(default=0.5, ge=0, le=1)
    sources: list[EvidenceSummary] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    source: str = Field(default="agent")
    replace_belief_id: str | None
    expected_versions: dict[str, Annotated[int, Field(ge=1)]] = Field(default_factory=dict)
    reason: str = Field(min_length=1, max_length=2048)


class Conflict(DTO):
    id: str
    belief_a: Memory
    belief_b: Memory
    evidence_a: list[EvidenceSummary]
    evidence_b: list[EvidenceSummary]
    kind: Literal["belief", "contradiction", "duplicate_candidate", "supersession_candidate"]
    allowed_actions: list[Resolution]
    analysis_complete: bool


class ResolveRequest(DTO):
    belief_a_id: str
    belief_b_id: str
    resolution: Resolution
    new_proposition: str | None
    expected_versions: dict[str, Annotated[int, Field(ge=1)]]
    reason: str = Field(min_length=1, max_length=2048)
    confirmation: TrueLiteral

    @model_validator(mode="after")
    def _both_versions(self) -> ResolveRequest:
        if set(self.expected_versions) != {self.belief_a_id, self.belief_b_id}:
            raise ValueError("exactly both belief versions required")
        return self


class ConflictResult(DTO):
    conflict_id: str
    resolution: Resolution
    memories: list[Memory]
    receipt_ids: list[str]
    audit_id: str
    request_id: str


class RelationRequest(DTO):
    source_id: str
    target_id: str
    relation_type: Literal["supports", "contradicts", "derives"]
    expected_versions: dict[str, Annotated[int, Field(ge=1)]]
    reason: str = Field(min_length=1, max_length=2048)

    @model_validator(mode="after")
    def _endpoint_versions(self) -> RelationRequest:
        if set(self.expected_versions) != {self.source_id, self.target_id}:
            raise ValueError("both endpoint versions required")
        return self


class RelationDeleteRequest(DTO):
    expected_version: Annotated[int, Field(ge=1)] = Field(ge=1)
    expected_versions: dict[str, Annotated[int, Field(ge=1)]]
    reason: str = Field(min_length=1, max_length=2048)
    confirmation: TrueLiteral


class RelationResult(DTO):
    relation: Relation
    deleted: bool
    audit_id: str
    receipt_ids: list[str]
    outbox_ids: list[str]
    request_id: str


class AdmissionRequest(DTO):
    text: str = Field(min_length=1, max_length=32768)
    source_scope: Literal["user", "agent"] = Field(default="agent")


class AdmissionResult(DTO):
    admitted: bool
    tag: Literal["ephemeral", "durable", "important"]
    ttl_days: int | None
    score: float
    memory_kind: str | None = Field(default=None)
    authority_level: str | None = Field(default=None)
    epistemic_status: str | None = Field(default=None)
    risk_tags: list[str]
    reason_codes: list[str]


class PreviewRequest(DTO):
    operation: Literal["transition", "invalidate", "relation_delete", "resolve"]
    memory_type: Literal["fact", "belief"]
    to_state: str | None
    relation_id: str | None
    conflict_id: str | None


class PreviewResult(DTO):
    memory_id: str
    observed_versions: dict[str, Annotated[int, Field(ge=1)]]
    allowed_actions: list[str]
    affected_ids: list[str]
    affected_count: int
    count_exact: bool
    truncated: bool
    projections: ItemProjection
    expires_at: UTCDatetime
    preview_only: TrueLiteral


class MaintenanceRequest(DTO):
    reason: str = Field(min_length=1, max_length=2048)
    confirmation: TrueLiteral
    selection: Literal["failed", "pending"]
    event_ids: list[str] = Field(max_length=100)
    selection_digest: str
    expected_count: int = Field(ge=1, le=100)


class RebuildRequest(DTO):
    reason: str = Field(min_length=1, max_length=2048)
    confirmation: TrueLiteral
    targets: list[Literal["vector", "graph"]]
    selection_digest: str
    expected_count: int = Field(ge=0)
    allow_network_embedding: bool = Field(default=False)


class JobAccepted(DTO):
    job_id: str
    status: Literal["queued"]
    request_id: str


class Job(DTO):
    id: str
    profile_id: str
    token_id: str
    operation: Literal["outbox_replay", "vector_rebuild", "graph_rebuild", "projections_rebuild"]
    status: Literal["queued", "running", "succeeded", "failed", "interrupted"]
    progress: float | None = Field(default=None, ge=0, le=1)
    created_at: UTCDatetime
    started_at: UTCDatetime | None = Field(default=None)
    finished_at: UTCDatetime | None = Field(default=None)
    request_id: str
    result: JobResult | None = Field(default=None)
    error_code: ErrorCode | None = Field(default=None)


class JobResult(DTO):
    selected: int
    attempted: int
    completed: int
    failed: int
    projection_digest: str | None
    receipt_ids: list[str]


class EventQuery(DTO):
    profiles: list[str] = Field(min_length=1, max_length=100)
    types: list[EventType] = Field(default_factory=list)
    memory_id: str | None
    severity: Literal["info", "warning", "error"] | None


class Event(DTO):
    id: str
    type: EventType
    profile_id: str
    timestamp: UTCDatetime
    request_id: str | None
    memory_id: str | None
    job_id: str | None
    severity: Literal["info", "warning", "error"]
    data: EventData


class EventData(DTO):
    revision: int | None
    from_state: str | None
    to_state: str | None
    operation: str | None
    pending: int | None
    code: ErrorCode | None


class SessionExchange(DTO):
    client_id: str = Field(min_length=1, max_length=128)
    requested_profiles: list[str]
    requested_permissions: list[Permission]


class SessionInfo(DTO):
    id: str
    subject: str
    profiles: list[str]
    permissions: list[Permission]
    expires_at: UTCDatetime
    csrf_token: str


class QualityQuery(DTO):
    from_: UTCDatetime = Field(alias="from")
    to: UTCDatetime
    interval: Literal["hour", "day"] = Field(default="day")

    @model_validator(mode="after")
    def _window(self) -> QualityQuery:
        if not timedelta(0) <= self.to - self.from_ <= timedelta(days=31):
            raise ValueError("quality window must be ordered and at most 31 days")
        return self


class OperationalPoint(DTO):
    timestamp: UTCDatetime
    metric: Literal[
        "memory_growth",
        "active_stale_ratio",
        "contradiction_rate",
        "dedup_rate",
        "admission_reject_rate",
        "retrieval_age",
        "confidence",
        "index_lag",
        "outbox_failures",
    ]
    value: float | None
    numerator: float | None
    denominator: float | None
    sample_count: int


class QualityResult(DTO):
    points: list[OperationalPoint]
    complete_from: UTCDatetime
    sampled_at: UTCDatetime
    request_id: str


class BenchmarkReport(DTO):
    id: str
    profile_id: str
    dataset_version: str | None = Field(default=None)
    embedding_model: str | None = Field(default=None)
    backend: str | None = Field(default=None)
    top_k: int
    thresholds: dict[str, float]
    code_revision: str | None = Field(default=None)
    target: Literal["raw", "source", "canonical"]
    shared_subset_id: str | None = Field(default=None)
    recall_at_k: float | None = Field(default=None)
    ndcg_at_k: float | None = Field(default=None)
    mrr: float | None = Field(default=None)
    hit_rate: float | None = Field(default=None)
    latency_seconds: float | None = Field(default=None)
    complete_metadata: bool


class AccessAudit(DTO):
    id: str
    timestamp: UTCDatetime
    request_id: str
    token_id: str | None
    client_id: str | None
    profile_id: str | None
    operation: str
    memory_id: str | None
    result: str
    latency_ms: float
    remote_address: str | None


class LegacyFact(DTO):
    id: str
    subject: str
    predicate: str
    object: str
    dedup_key: str | None
    confidence: float = Field(ge=0, le=1)
    source: str | None
    creator: str
    created_at: UTCDatetime
    updated_at: UTCDatetime
    verification_status: str
    lifecycle_state: str
    version: int


class LegacyDecision(DTO):
    id: str
    context: str
    choice: str
    rejected_alternatives: list[str]
    reason: str
    source: str | None
    creator: str
    created_at: UTCDatetime
    updated_at: UTCDatetime
    confidence: float = Field(ge=0, le=1)
    verification_status: str
    lifecycle_state: str
    version: str


# Resolve only local DTO forward references (C4), never application/runtime types.
for _value in tuple(globals().values()):
    if isinstance(_value, type) and issubclass(_value, DTO):
        _value.model_rebuild()


class Command(DTO):
    """C1 frozen envelope; path validation is driven by the C3 catalogue."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True, allow_inf_nan=False)
    profile_id: str | None
    path_ids: dict[str, str]

    @model_validator(mode="after")
    def _path_shape(self) -> Command:
        expected = COMMAND_PATHS[self.operation]
        if set(self.path_ids) != set(expected):
            raise ValueError("path_ids must exactly match named non-profile placeholders")
        if COMMAND_SCOPED[self.operation] != (self.profile_id is not None):
            raise ValueError("profile_id must match route scope")
        return self


# C3 is projected here as types, not route implementations.
COMMAND_TYPES: dict[Operation, type[Command]] = {}
RESULT_TYPES: dict[Operation, type[DTO]] = {}
COMMAND_PATHS: dict[Operation, tuple[str, ...]] = {}
COMMAND_SCOPED: dict[Operation, bool] = {}

_COMMAND_SPECS = (
    ("health", Empty, Health, (), False),
    ("ready", Empty, Health, (), False),
    ("profiles.list", PageQuery, Page[Profile], (), False),
    ("profiles.get", Empty, Profile, (), True),
    ("profiles.enable", ControlMutation, Profile, (), True),
    ("profiles.disable", ControlMutation, Profile, (), True),
    ("tokens.create", TokenCreate, TokenCreated, (), False),
    ("tokens.list", PageQuery, Page[TokenInfo], (), False),
    ("tokens.revoke", ControlMutation, TokenInfo, ("id",), False),
    ("memories.list", MemoryListQuery, Page[Memory], (), True),
    ("memories.get", Empty, Memory, ("id",), True),
    ("receipts.get", Empty, Receipt, ("id",), True),
    ("history.list", PageQuery, Page[HistoryEvent], ("id",), True),
    ("lookup", LookupQuery, LookupResult, (), True),
    ("memories.reveal", RevealRequest, Memory, ("id",), True),
    ("search", SearchRequest, SearchResult, (), True),
    ("context", ContextRequest, ContextResult, (), True),
    ("relations.for_memory", PageQuery, Page[Relation], ("id",), True),
    ("relations.list", PageQuery, Page[Relation], (), True),
    ("relations.get", Empty, Relation, ("id",), True),
    ("graph.neighborhood", NeighborhoodQuery, GraphResult, (), True),
    ("graph.path", PathRequest, GraphResult, (), True),
    ("graph.entity", Empty, GraphResult, ("entity_id",), True),
    ("audit", AuditQuery, AuditResult, (), True),
    ("indexes.status", Empty, IndexStatus, (), True),
    ("outbox.list", OutboxQuery, Page[OutboxItem], (), True),
    ("overview", Empty, Overview, (), True),
    ("reflect", ReflectRequest, ReflectResult, (), True),
    ("traces.get", Empty, Trace, ("id",), True),
    ("traces.list", PageQuery, Page[TraceSummary], (), True),
    ("memories.create", RememberRequest, WriteResult, (), True),
    ("learn", LearnRequest, WriteResult, (), True),
    ("memories.transition", TransitionRequest, TransitionResult, ("id",), True),
    ("memories.invalidate", InvalidateRequest, TransitionResult, ("id",), True),
    ("beliefs.list", BeliefListQuery, Page[Memory], (), True),
    ("beliefs.get", Empty, Memory, ("id",), True),
    ("beliefs.set", BeliefRequest, WriteResult, (), True),
    ("conflicts.list", PageQuery, Page[Conflict], (), True),
    ("conflicts.resolve", ResolveRequest, ConflictResult, ("id",), True),
    ("relations.create", RelationRequest, RelationResult, (), True),
    ("relations.delete", RelationDeleteRequest, RelationResult, ("id",), True),
    ("admission.preview", AdmissionRequest, AdmissionResult, (), True),
    ("mutations.preview", PreviewRequest, PreviewResult, ("id",), True),
    ("jobs.get", Empty, Job, ("id",), False),
    ("outbox.replay", MaintenanceRequest, JobAccepted, (), True),
    ("indexes.rebuild", RebuildRequest, JobAccepted, (), True),
    ("events", EventQuery, Event, (), False),
    ("sessions.create", SessionExchange, SessionInfo, (), False),
    ("sessions.get", Empty, SessionInfo, (), False),
    ("sessions.delete", Empty, Health, (), False),
    ("quality", QualityQuery, QualityResult, (), True),
    ("benchmarks.list", PageQuery, Page[BenchmarkReport], (), True),
    ("benchmarks.get", Empty, BenchmarkReport, ("id",), True),
)
for _op, _request, _result, _paths, _scoped in _COMMAND_SPECS:
    _name = "".join(part.title() for part in _op.replace("_", ".").split(".")) + "Command"
    _command = create_model(
        _name,
        __base__=Command,
        __module__=__name__,
        operation=(Literal[_op], ...),
        payload=(_request, ...),
    )
    globals()[_name] = _command
    COMMAND_TYPES[_op] = _command
    RESULT_TYPES[_op] = _result
    COMMAND_PATHS[_op] = _paths
    COMMAND_SCOPED[_op] = _scoped


def _commands(operations: list[str]):
    return Annotated[Union[tuple(COMMAND_TYPES[op] for op in operations)], Field(discriminator="operation")]


def _result_tag(value: object) -> str | None:
    # C4 forbids adding a wire discriminator. Typed objects carry the type tag;
    # JSON is decoded through the operation-specific RESULT_TYPES adapter.
    return type(value).__name__ if isinstance(value, DTO) else None


def _results(operations: list[str]):
    types = tuple(dict.fromkeys(RESULT_TYPES[op] for op in operations))
    if len(types) == 1:
        return types[0]
    return Annotated[
        Union[tuple(Annotated[typ, Tag(typ.__name__)] for typ in types)],
        Discriminator(_result_tag),
    ]


GatewayCommand = _commands(list(COMMAND_TYPES))
GatewayResult = _results(list(RESULT_TYPES))

OWNER_OPERATIONS = {
    "MemoryRead": ["memories.list", "memories.get", "receipts.get", "history.list", "lookup", "memories.reveal"],
    "Retrieval": ["search", "context"],
    "GraphRead": [
        "relations.for_memory",
        "relations.list",
        "relations.get",
        "graph.neighborhood",
        "graph.path",
        "graph.entity",
    ],
    "Diagnostics": ["audit", "indexes.status", "outbox.list", "overview", "reflect"],
    "Trace": ["traces.get", "traces.list"],
    "Ingestion": ["memories.create", "learn"],
    "Lifecycle": ["memories.transition", "memories.invalidate"],
    "Beliefs": ["beliefs.list", "beliefs.get", "beliefs.set", "conflicts.list", "conflicts.resolve"],
    "RelationsWrite": ["relations.create", "relations.delete"],
    "Inspection": ["admission.preview", "mutations.preview"],
    "Quality": ["quality"],
    "Benchmark": ["benchmarks.list", "benchmarks.get"],
}
MemoryReadCommand = _commands(OWNER_OPERATIONS["MemoryRead"])
MemoryReadResult = _results(OWNER_OPERATIONS["MemoryRead"])
RetrievalCommand = _commands(OWNER_OPERATIONS["Retrieval"])
RetrievalResult = _results(OWNER_OPERATIONS["Retrieval"])
GraphReadCommand = _commands(OWNER_OPERATIONS["GraphRead"])
GraphReadResult = _results(OWNER_OPERATIONS["GraphRead"])
DiagnosticsCommand = _commands(OWNER_OPERATIONS["Diagnostics"])
DiagnosticsResult = _results(OWNER_OPERATIONS["Diagnostics"])
TraceCommand = _commands(OWNER_OPERATIONS["Trace"])
TraceResult = _results(OWNER_OPERATIONS["Trace"])
IngestionCommand = _commands(OWNER_OPERATIONS["Ingestion"])
LifecycleCommandV1 = _commands(OWNER_OPERATIONS["Lifecycle"])
BeliefsCommand = _commands(OWNER_OPERATIONS["Beliefs"])
BeliefsResult = _results(OWNER_OPERATIONS["Beliefs"])
RelationsWriteCommand = _commands(OWNER_OPERATIONS["RelationsWrite"])
InspectionCommand = _commands(OWNER_OPERATIONS["Inspection"])
InspectionResult = _results(OWNER_OPERATIONS["Inspection"])
QualityCommand = _commands(OWNER_OPERATIONS["Quality"])
BenchmarkCommand = _commands(OWNER_OPERATIONS["Benchmark"])
BenchmarkResult = _results(OWNER_OPERATIONS["Benchmark"])

PROFILE_MUTATIONS = [
    "memories.create",
    "learn",
    "memories.transition",
    "memories.invalidate",
    "beliefs.set",
    "conflicts.resolve",
    "relations.create",
    "relations.delete",
]
CONTROL_MUTATIONS = [
    "profiles.enable",
    "profiles.disable",
    "tokens.create",
    "tokens.revoke",
    "outbox.replay",
    "indexes.rebuild",
]
ProfileMutationCommand = _commands(PROFILE_MUTATIONS)
ControlMutationCommand = _commands(CONTROL_MUTATIONS)
MutationCommand = _commands(PROFILE_MUTATIONS + CONTROL_MUTATIONS)
MutationResult = _results(PROFILE_MUTATIONS + CONTROL_MUTATIONS)
