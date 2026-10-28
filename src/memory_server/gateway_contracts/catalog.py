"""C3 version-one route/permission catalogue, with C5 error vocabulary.

No HTTP mounting, authorization or port implementation belongs here.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal, Mapping

from . import dto as d

VOCABULARY_VERSION = 1


@dataclass(frozen=True)
class RouteEntry:
    owner: str
    method: Literal["GET", "POST", "DELETE"]
    route: str
    operation: d.Operation
    permission: str
    request_type: type[d.DTO]
    result_type: type[d.DTO]
    mutation: bool

    @property
    def path_parameters(self) -> tuple[str, ...]:
        return tuple(name for name in re.findall(r"{(.*?)}", self.route) if name != "profile")

    @property
    def streaming(self) -> bool:
        return self.operation == "events"


@dataclass(frozen=True)
class RouteCatalog:
    entries: tuple[RouteEntry, ...]
    by_operation: Mapping[d.Operation, RouteEntry] = field(init=False)
    by_route: Mapping[tuple[str, str], RouteEntry] = field(init=False)

    def __post_init__(self) -> None:
        operations = {}
        routes = {}
        for entry in self.entries:
            if entry.operation in operations or (entry.method, entry.route) in routes:
                raise ValueError("duplicate operation or route owner")
            if entry.operation not in d.COMMAND_TYPES:
                raise ValueError("unknown operation")
            command = d.COMMAND_TYPES[entry.operation]
            if command.model_fields["payload"].annotation is not entry.request_type:
                raise ValueError("request type mismatch")
            if d.RESULT_TYPES[entry.operation] is not entry.result_type:
                raise ValueError("result type mismatch")
            operations[entry.operation] = entry
            routes[entry.method, entry.route] = entry
        object.__setattr__(self, "by_operation", MappingProxyType(operations))
        object.__setattr__(self, "by_route", MappingProxyType(routes))

    def validate(self, operation: d.Operation, payload: d.JSONValue) -> d.GatewayCommand:
        # C1 does not supply route context separately: payload is the full command
        # JSON object, including operation/profile_id/path_ids/payload.
        if operation not in self.by_operation:
            raise ValueError("unknown operation")
        command = d.COMMAND_TYPES[operation].model_validate_json(json.dumps(payload, allow_nan=False))
        return command


# C3, in normative table order. SSE[Event] uses Event as the item DTO.
CATALOG = RouteCatalog(
    (
        RouteEntry("S06", "GET", "/healthz", "health", "PUBLIC", d.Empty, d.Health, False),
        RouteEntry("S06", "GET", "/readyz", "ready", "PUBLIC", d.Empty, d.Health, False),
        RouteEntry(
            "S06", "GET", "/api/v1/profiles", "profiles.list", "profile.read", d.PageQuery, d.Page[d.Profile], False
        ),
        RouteEntry(
            "S06", "GET", "/api/v1/profiles/{profile}", "profiles.get", "profile.read", d.Empty, d.Profile, False
        ),
        RouteEntry(
            "S06",
            "POST",
            "/api/v1/profiles/{profile}/enable",
            "profiles.enable",
            "profile.manage",
            d.ControlMutation,
            d.Profile,
            True,
        ),
        RouteEntry(
            "S06",
            "POST",
            "/api/v1/profiles/{profile}/disable",
            "profiles.disable",
            "profile.manage",
            d.ControlMutation,
            d.Profile,
            True,
        ),
        RouteEntry(
            "S03", "POST", "/api/v1/tokens", "tokens.create", "token.manage", d.TokenCreate, d.TokenCreated, True
        ),
        RouteEntry(
            "S03", "GET", "/api/v1/tokens", "tokens.list", "token.manage", d.PageQuery, d.Page[d.TokenInfo], False
        ),
        RouteEntry(
            "S03",
            "POST",
            "/api/v1/tokens/{id}/revoke",
            "tokens.revoke",
            "token.manage",
            d.ControlMutation,
            d.TokenInfo,
            True,
        ),
        RouteEntry(
            "S07",
            "GET",
            "/api/v1/profiles/{profile}/memories",
            "memories.list",
            "memory.read",
            d.MemoryListQuery,
            d.Page[d.Memory],
            False,
        ),
        RouteEntry(
            "S07",
            "GET",
            "/api/v1/profiles/{profile}/memories/{id}",
            "memories.get",
            "memory.read",
            d.Empty,
            d.Memory,
            False,
        ),
        RouteEntry(
            "S07",
            "GET",
            "/api/v1/profiles/{profile}/memories/{id}/receipt",
            "receipts.get",
            "memory.read",
            d.Empty,
            d.Receipt,
            False,
        ),
        RouteEntry(
            "S07",
            "GET",
            "/api/v1/profiles/{profile}/memories/{id}/history",
            "history.list",
            "memory.read",
            d.PageQuery,
            d.Page[d.HistoryEvent],
            False,
        ),
        RouteEntry(
            "S07",
            "GET",
            "/api/v1/profiles/{profile}/lookup",
            "lookup",
            "memory.read",
            d.LookupQuery,
            d.LookupResult,
            False,
        ),
        RouteEntry(
            "S07",
            "POST",
            "/api/v1/profiles/{profile}/memories/{id}/reveal",
            "memories.reveal",
            "memory.reveal",
            d.RevealRequest,
            d.Memory,
            False,
        ),
        RouteEntry(
            "S08",
            "POST",
            "/api/v1/profiles/{profile}/search",
            "search",
            "memory.read",
            d.SearchRequest,
            d.SearchResult,
            False,
        ),
        RouteEntry(
            "S08",
            "POST",
            "/api/v1/profiles/{profile}/context",
            "context",
            "memory.read",
            d.ContextRequest,
            d.ContextResult,
            False,
        ),
        RouteEntry(
            "S09",
            "GET",
            "/api/v1/profiles/{profile}/memories/{id}/relations",
            "relations.for_memory",
            "relation.read",
            d.PageQuery,
            d.Page[d.Relation],
            False,
        ),
        RouteEntry(
            "S09",
            "GET",
            "/api/v1/profiles/{profile}/relations",
            "relations.list",
            "relation.read",
            d.PageQuery,
            d.Page[d.Relation],
            False,
        ),
        RouteEntry(
            "S09",
            "GET",
            "/api/v1/profiles/{profile}/relations/{id}",
            "relations.get",
            "relation.read",
            d.Empty,
            d.Relation,
            False,
        ),
        RouteEntry(
            "S09",
            "GET",
            "/api/v1/profiles/{profile}/graph/neighborhood",
            "graph.neighborhood",
            "graph.read",
            d.NeighborhoodQuery,
            d.GraphResult,
            False,
        ),
        RouteEntry(
            "S09",
            "POST",
            "/api/v1/profiles/{profile}/graph/path",
            "graph.path",
            "graph.read",
            d.PathRequest,
            d.GraphResult,
            False,
        ),
        RouteEntry(
            "S09",
            "GET",
            "/api/v1/profiles/{profile}/graph/entity/{entity_id}",
            "graph.entity",
            "graph.read",
            d.Empty,
            d.GraphResult,
            False,
        ),
        RouteEntry(
            "S10", "GET", "/api/v1/profiles/{profile}/audit", "audit", "audit.read", d.AuditQuery, d.AuditResult, False
        ),
        RouteEntry(
            "S10",
            "GET",
            "/api/v1/profiles/{profile}/indexes/status",
            "indexes.status",
            "profile.read",
            d.Empty,
            d.IndexStatus,
            False,
        ),
        RouteEntry(
            "S10",
            "GET",
            "/api/v1/profiles/{profile}/outbox",
            "outbox.list",
            "audit.read",
            d.OutboxQuery,
            d.Page[d.OutboxItem],
            False,
        ),
        RouteEntry(
            "S10", "GET", "/api/v1/profiles/{profile}/overview", "overview", "profile.read", d.Empty, d.Overview, False
        ),
        RouteEntry(
            "S10",
            "POST",
            "/api/v1/profiles/{profile}/reflect",
            "reflect",
            "audit.read",
            d.ReflectRequest,
            d.ReflectResult,
            False,
        ),
        RouteEntry(
            "S11",
            "GET",
            "/api/v1/profiles/{profile}/retrieval/trace/{id}",
            "traces.get",
            "memory.read",
            d.Empty,
            d.Trace,
            False,
        ),
        RouteEntry(
            "S11",
            "GET",
            "/api/v1/profiles/{profile}/retrieval/traces",
            "traces.list",
            "memory.read",
            d.PageQuery,
            d.Page[d.TraceSummary],
            False,
        ),
        RouteEntry(
            "S13",
            "POST",
            "/api/v1/profiles/{profile}/memories",
            "memories.create",
            "memory.write",
            d.RememberRequest,
            d.WriteResult,
            True,
        ),
        RouteEntry(
            "S13",
            "POST",
            "/api/v1/profiles/{profile}/learn",
            "learn",
            "memory.write",
            d.LearnRequest,
            d.WriteResult,
            True,
        ),
        RouteEntry(
            "S14",
            "POST",
            "/api/v1/profiles/{profile}/memories/{id}/transition",
            "memories.transition",
            "memory.transition",
            d.TransitionRequest,
            d.TransitionResult,
            True,
        ),
        RouteEntry(
            "S14",
            "POST",
            "/api/v1/profiles/{profile}/memories/{id}/invalidate",
            "memories.invalidate",
            "memory.invalidate",
            d.InvalidateRequest,
            d.TransitionResult,
            True,
        ),
        RouteEntry(
            "S15",
            "GET",
            "/api/v1/profiles/{profile}/beliefs",
            "beliefs.list",
            "belief.read",
            d.BeliefListQuery,
            d.Page[d.Memory],
            False,
        ),
        RouteEntry(
            "S15",
            "GET",
            "/api/v1/profiles/{profile}/beliefs/{id}",
            "beliefs.get",
            "belief.read",
            d.Empty,
            d.Memory,
            False,
        ),
        RouteEntry(
            "S15",
            "POST",
            "/api/v1/profiles/{profile}/beliefs",
            "beliefs.set",
            "belief.write",
            d.BeliefRequest,
            d.WriteResult,
            True,
        ),
        RouteEntry(
            "S15",
            "GET",
            "/api/v1/profiles/{profile}/conflicts",
            "conflicts.list",
            "belief.read",
            d.PageQuery,
            d.Page[d.Conflict],
            False,
        ),
        RouteEntry(
            "S15",
            "POST",
            "/api/v1/profiles/{profile}/conflicts/{id}/resolve",
            "conflicts.resolve",
            "belief.resolve",
            d.ResolveRequest,
            d.ConflictResult,
            True,
        ),
        RouteEntry(
            "S16",
            "POST",
            "/api/v1/profiles/{profile}/relations",
            "relations.create",
            "relation.write",
            d.RelationRequest,
            d.RelationResult,
            True,
        ),
        RouteEntry(
            "S16",
            "DELETE",
            "/api/v1/profiles/{profile}/relations/{id}",
            "relations.delete",
            "relation.write",
            d.RelationDeleteRequest,
            d.RelationResult,
            True,
        ),
        RouteEntry(
            "S17",
            "POST",
            "/api/v1/profiles/{profile}/admission/preview",
            "admission.preview",
            "memory.write",
            d.AdmissionRequest,
            d.AdmissionResult,
            False,
        ),
        RouteEntry(
            "S17",
            "POST",
            "/api/v1/profiles/{profile}/memories/{id}/preview",
            "mutations.preview",
            "memory.read",
            d.PreviewRequest,
            d.PreviewResult,
            False,
        ),
        RouteEntry("S18", "GET", "/api/v1/jobs/{id}", "jobs.get", "index.manage", d.Empty, d.Job, False),
        RouteEntry(
            "S19",
            "POST",
            "/api/v1/profiles/{profile}/outbox/replay",
            "outbox.replay",
            "index.manage",
            d.MaintenanceRequest,
            d.JobAccepted,
            True,
        ),
        RouteEntry(
            "S20",
            "POST",
            "/api/v1/profiles/{profile}/indexes/rebuild",
            "indexes.rebuild",
            "index.manage",
            d.RebuildRequest,
            d.JobAccepted,
            True,
        ),
        RouteEntry("S21", "GET", "/api/v1/events", "events", "memory.read", d.EventQuery, d.Event, False),
        RouteEntry(
            "S22", "POST", "/api/v1/session", "sessions.create", "BEARER", d.SessionExchange, d.SessionInfo, False
        ),
        RouteEntry("S22", "GET", "/api/v1/session", "sessions.get", "SESSION", d.Empty, d.SessionInfo, False),
        RouteEntry("S22", "DELETE", "/api/v1/session", "sessions.delete", "SESSION+CSRF", d.Empty, d.Health, False),
        RouteEntry(
            "S27",
            "GET",
            "/api/v1/profiles/{profile}/quality",
            "quality",
            "audit.read",
            d.QualityQuery,
            d.QualityResult,
            False,
        ),
        RouteEntry(
            "S28",
            "GET",
            "/api/v1/profiles/{profile}/benchmarks",
            "benchmarks.list",
            "audit.read",
            d.PageQuery,
            d.Page[d.BenchmarkReport],
            False,
        ),
        RouteEntry(
            "S28",
            "GET",
            "/api/v1/profiles/{profile}/benchmarks/{id}",
            "benchmarks.get",
            "audit.read",
            d.Empty,
            d.BenchmarkReport,
            False,
        ),
    )
)

ERROR_HTTP_STATUS: Mapping[d.ErrorCode, int] = MappingProxyType(
    {
        "E_AUTH_REQUIRED": 401,
        "E_PERMISSION_DENIED": 403,
        "E_PROFILE_NOT_FOUND": 404,
        "E_PROFILE_DISABLED": 409,
        "E_WRITER_ACTIVE": 409,
        "E_VERSION_CONFLICT": 409,
        "E_VALIDATION": 422,
        "E_ADMISSION_REJECTED": 422,
        "E_NOT_FOUND": 404,
        "E_STORAGE": 503,
        "E_VECTOR_BACKEND": 503,
        "E_GRAPH_BACKEND": 503,
        "E_OUTBOX": 503,
        "E_RATE_LIMIT": 429,
        "E_REQUEST_TOO_LARGE": 413,
        "E_IDEMPOTENCY_REQUIRED": 400,
        "E_IDEMPOTENCY_CONFLICT": 409,
        "E_REQUEST_IN_PROGRESS": 409,
        "E_PRECONDITION_REQUIRED": 428,
        "E_CURSOR_INVALID": 400,
        "E_CURSOR_EXPIRED": 410,
        "E_CAPABILITY_UNAVAILABLE": 503,
        "E_RUNTIME_NOT_READY": 503,
        "E_RUNTIME_CLOSING": 503,
        "E_RUNTIME_FAILED": 503,
        "E_PROFILE_BUSY": 409,
        "E_AUDIT_UNAVAILABLE": 503,
        "E_TIMEOUT": 504,
        "E_CSRF": 403,
        "E_UNSUPPORTED_MEDIA_TYPE": 415,
        "E_METHOD_NOT_ALLOWED": 405,
        "E_INTERNAL": 500,
        "E_PROJECTION_UNAVAILABLE": 503,
        "E_MAINTENANCE_CONFLICT": 409,
        "E_JOB_INTERRUPTED": 409,
        "E_EVENT_CURSOR_EXPIRED": 410,
    }
)
