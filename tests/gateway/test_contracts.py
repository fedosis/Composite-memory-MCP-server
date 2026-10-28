"""C0-C9 executable contract checks against real DTO/catalog/Protocol boundaries."""

import dataclasses
import importlib
import inspect
import json
import subprocess
import sys
import typing
from pathlib import Path

import pytest
from pydantic import TypeAdapter, ValidationError

EXPECTED_FIELDS = {
    "Empty": [],
    "Health": ["status"],
    "PageQuery": ["limit", "cursor"],
    "Page": ["items", "next_cursor", "has_more", "request_id"],
    "Profile": [
        "id",
        "display_name",
        "description",
        "enabled",
        "generation",
        "runtime_state",
        "capabilities",
        "storage_root",
    ],
    "ControlMutation": ["reason", "confirmation", "expected_generation"],
    "TokenCreate": ["subject", "profiles", "permissions", "expires_at"],
    "TokenInfo": [
        "id",
        "subject",
        "profiles",
        "permissions",
        "created_at",
        "expires_at",
        "last_used_at",
        "revoked_at",
        "generation",
    ],
    "TokenCreated": ["token", "secret"],
    "MemoryListQuery": [
        "limit",
        "cursor",
        "memory_types",
        "lifecycle_states",
        "min_confidence",
        "max_confidence",
        "source",
        "tags",
        "authority_level",
        "epistemic_status",
        "created_from",
        "created_to",
        "updated_from",
        "updated_to",
        "valid_at",
        "risk_tags",
    ],
    "FactPayload": ["subject", "predicate", "object"],
    "BeliefPayload": ["proposition", "tags", "source_ids"],
    "DecisionPayload": ["context", "choice", "rejected_alternatives", "reason"],
    "SkillPayload": ["name", "purpose", "steps", "constraints", "validation", "success_rate", "skill_version"],
    "Memory": [
        "id",
        "memory_type",
        "content",
        "payload",
        "confidence",
        "lifecycle_state",
        "revision",
        "source_version",
        "source",
        "creator",
        "created_at",
        "updated_at",
        "valid_from",
        "valid_to",
        "epistemic_status",
        "authority_level",
        "risk_tags",
        "receipt_id",
        "masked",
        "projection",
    ],
    "ItemProjection": ["vector", "graph", "outbox"],
    "Receipt": [
        "id",
        "memory_type",
        "source",
        "created_by",
        "timestamp",
        "confidence",
        "verification_status",
        "lifecycle_state",
        "source_version",
        "history",
        "history_complete",
    ],
    "HistoryEvent": [
        "id",
        "kind",
        "timestamp",
        "actor",
        "reason",
        "revision",
        "from_state",
        "to_state",
        "receipt_id",
        "evidence",
    ],
    "EvidenceSummary": ["method", "sources", "session_id", "confidence", "source_date", "derived_from", "claim_type"],
    "LookupQuery": ["kind", "id"],
    "LookupResult": ["kind", "id", "target", "found"],
    "RevealRequest": ["reason", "memory_type", "expected_revision"],
    "SearchFilters": ["memory_types", "tags", "lifecycle_states", "source", "min_confidence"],
    "SearchRequest": ["query", "mode", "limit", "include_inactive", "filters", "trace"],
    "SearchHit": ["memory", "source", "score", "rank"],
    "SearchResult": ["request_id", "results", "total", "trace_id", "degraded"],
    "SourceFailure": ["source", "code"],
    "ContextRequest": ["task", "subject", "max_results", "include_inactive", "trace"],
    "ContextResult": [
        "request_id",
        "package",
        "memories",
        "trace_id",
        "estimated_tokens",
        "tokenizer",
        "token_count_is_estimate",
    ],
    "ContextPackage": ["facts", "decisions", "total"],
    "Relation": ["id", "source_id", "target_id", "relation_type", "canonical", "revision", "created_at"],
    "NeighborhoodQuery": [
        "memory_id",
        "entity_id",
        "depth",
        "limit",
        "edge_limit",
        "relation_types",
        "memory_types",
        "lifecycle_states",
    ],
    "PathRequest": ["source_id", "target_id", "max_depth", "relation_types", "node_limit", "edge_limit"],
    "GraphNode": ["id", "kind", "label", "lifecycle_state", "masked"],
    "GraphEdge": ["id", "source_id", "target_id", "relation_type", "canonical", "canonical_relation_id", "revision"],
    "GraphConsistency": [
        "canonical_store",
        "projection",
        "outbox_lag",
        "canonical_edges",
        "projected_edges",
        "missing_projection",
        "extra_projection",
        "last_sync",
    ],
    "GraphResult": ["nodes", "edges", "paths", "graph_consistency", "truncated", "truncation_reason", "request_id"],
    "AuditQuery": ["audit_type", "limit", "cursor"],
    "AuditFinding": ["id", "category", "severity", "object_kind", "object_id", "code", "aggregate"],
    "AuditResult": ["findings", "sections", "sampled_at", "complete"],
    "IndexStatus": [
        "sqlite_records",
        "vector_records",
        "graph_relations",
        "vector_lag",
        "graph_lag",
        "outbox_pending",
        "outbox_failed",
        "graph_consistency",
        "last_success",
        "sampled_at",
    ],
    "OutboxQuery": ["limit", "cursor", "status", "memory_id"],
    "OutboxItem": [
        "event_id",
        "memory_id",
        "record_type",
        "operation",
        "status",
        "attempts",
        "target_backend",
        "last_error_code",
        "next_retry",
        "created_at",
        "processed_at",
    ],
    "Overview": [
        "profile",
        "facts",
        "beliefs",
        "decisions",
        "skills",
        "lifecycle_counts",
        "unresolved_conflicts",
        "audit_warnings",
        "indexes",
        "schema_revision",
        "last_migration",
        "writer_owned",
    ],
    "ReflectRequest": ["mode", "topic", "min_confidence", "limit", "context", "strict_same_context"],
    "ReflectResult": ["mode", "sections", "complete", "request_id"],
    "TraceContribution": [
        "memory_id",
        "source",
        "raw_score",
        "normalized_score",
        "rank_before_merge",
        "rank_after_merge",
        "matched_terms",
        "relation_path",
        "dedup_winner_id",
    ],
    "Trace": [
        "id",
        "request_id",
        "profile_id",
        "operation",
        "query",
        "stages",
        "stage_status",
        "merge",
        "deduplicated",
        "final",
        "created_at",
        "expires_at",
        "settings_digest",
        "code_revision",
    ],
    "TraceSummary": ["id", "request_id", "operation", "created_at", "expires_at", "settings_digest", "code_revision"],
    "RememberRequest": [
        "type",
        "subject",
        "predicate",
        "object",
        "confidence",
        "source",
        "tags",
        "evidence",
        "expected_version",
    ],
    "LearnRequest": ["text", "source", "extract_beliefs", "min_belief_confidence", "expected_versions"],
    "WriteResult": ["request_id", "memories", "receipts", "admission", "outbox_ids", "reinforced_ids"],
    "TransitionRequest": ["memory_type", "to_state", "reason", "expected_version", "confirmation"],
    "InvalidateRequest": ["memory_type", "reason", "expected_version", "confirmation"],
    "Propagation": ["memory_id", "action", "reason", "revision"],
    "TransitionResult": [
        "memory_id",
        "memory",
        "from_state",
        "to_state",
        "propagated",
        "receipt_id",
        "audit_id",
        "request_id",
    ],
    "BeliefListQuery": [
        "limit",
        "cursor",
        "proposition",
        "lifecycle_state",
        "min_confidence",
        "tags",
        "source",
        "creator",
        "source_id",
    ],
    "BeliefRequest": [
        "proposition",
        "confidence",
        "sources",
        "tags",
        "source",
        "replace_belief_id",
        "expected_versions",
        "reason",
    ],
    "Conflict": [
        "id",
        "belief_a",
        "belief_b",
        "evidence_a",
        "evidence_b",
        "kind",
        "allowed_actions",
        "analysis_complete",
    ],
    "ResolveRequest": [
        "belief_a_id",
        "belief_b_id",
        "resolution",
        "new_proposition",
        "expected_versions",
        "reason",
        "confirmation",
    ],
    "ConflictResult": ["conflict_id", "resolution", "memories", "receipt_ids", "audit_id", "request_id"],
    "RelationRequest": ["source_id", "target_id", "relation_type", "expected_versions", "reason"],
    "RelationDeleteRequest": ["expected_version", "expected_versions", "reason", "confirmation"],
    "RelationResult": ["relation", "deleted", "audit_id", "receipt_ids", "outbox_ids", "request_id"],
    "AdmissionRequest": ["text", "source_scope"],
    "AdmissionResult": [
        "admitted",
        "tag",
        "ttl_days",
        "score",
        "memory_kind",
        "authority_level",
        "epistemic_status",
        "risk_tags",
        "reason_codes",
    ],
    "PreviewRequest": ["operation", "memory_type", "to_state", "relation_id", "conflict_id"],
    "PreviewResult": [
        "memory_id",
        "observed_versions",
        "allowed_actions",
        "affected_ids",
        "affected_count",
        "count_exact",
        "truncated",
        "projections",
        "expires_at",
        "preview_only",
    ],
    "MaintenanceRequest": ["reason", "confirmation", "selection", "event_ids", "selection_digest", "expected_count"],
    "RebuildRequest": [
        "reason",
        "confirmation",
        "targets",
        "selection_digest",
        "expected_count",
        "allow_network_embedding",
    ],
    "JobAccepted": ["job_id", "status", "request_id"],
    "Job": [
        "id",
        "profile_id",
        "token_id",
        "operation",
        "status",
        "progress",
        "created_at",
        "started_at",
        "finished_at",
        "request_id",
        "result",
        "error_code",
    ],
    "JobResult": ["selected", "attempted", "completed", "failed", "projection_digest", "receipt_ids"],
    "EventQuery": ["profiles", "types", "memory_id", "severity"],
    "Event": ["id", "type", "profile_id", "timestamp", "request_id", "memory_id", "job_id", "severity", "data"],
    "EventData": ["revision", "from_state", "to_state", "operation", "pending", "code"],
    "SessionExchange": ["client_id", "requested_profiles", "requested_permissions"],
    "SessionInfo": ["id", "subject", "profiles", "permissions", "expires_at", "csrf_token"],
    "QualityQuery": ["from", "to", "interval"],
    "OperationalPoint": ["timestamp", "metric", "value", "numerator", "denominator", "sample_count"],
    "QualityResult": ["points", "complete_from", "sampled_at", "request_id"],
    "BenchmarkReport": [
        "id",
        "profile_id",
        "dataset_version",
        "embedding_model",
        "backend",
        "top_k",
        "thresholds",
        "code_revision",
        "target",
        "shared_subset_id",
        "recall_at_k",
        "ndcg_at_k",
        "mrr",
        "hit_rate",
        "latency_seconds",
        "complete_metadata",
    ],
    "AccessAudit": [
        "id",
        "timestamp",
        "request_id",
        "token_id",
        "client_id",
        "profile_id",
        "operation",
        "memory_id",
        "result",
        "latency_ms",
        "remote_address",
    ],
    "LegacyFact": [
        "id",
        "subject",
        "predicate",
        "object",
        "dedup_key",
        "confidence",
        "source",
        "creator",
        "created_at",
        "updated_at",
        "verification_status",
        "lifecycle_state",
        "version",
    ],
    "LegacyDecision": [
        "id",
        "context",
        "choice",
        "rejected_alternatives",
        "reason",
        "source",
        "creator",
        "created_at",
        "updated_at",
        "confidence",
        "verification_status",
        "lifecycle_state",
        "version",
    ],
}
EXPECTED_ROUTES = [
    ["S06", "GET", "/healthz", "health", "PUBLIC", "Empty", "Health", "no"],
    ["S06", "GET", "/readyz", "ready", "PUBLIC", "Empty", "Health", "no"],
    ["S06", "GET", "/api/v1/profiles", "profiles.list", "profile.read", "PageQuery", "Page[Profile]", "no"],
    ["S06", "GET", "/api/v1/profiles/{profile}", "profiles.get", "profile.read", "Empty", "Profile", "no"],
    [
        "S06",
        "POST",
        "/api/v1/profiles/{profile}/enable",
        "profiles.enable",
        "profile.manage",
        "ControlMutation",
        "Profile",
        "yes",
    ],
    [
        "S06",
        "POST",
        "/api/v1/profiles/{profile}/disable",
        "profiles.disable",
        "profile.manage",
        "ControlMutation",
        "Profile",
        "yes",
    ],
    ["S03", "POST", "/api/v1/tokens", "tokens.create", "token.manage", "TokenCreate", "TokenCreated", "yes"],
    ["S03", "GET", "/api/v1/tokens", "tokens.list", "token.manage", "PageQuery", "Page[TokenInfo]", "no"],
    [
        "S03",
        "POST",
        "/api/v1/tokens/{id}/revoke",
        "tokens.revoke",
        "token.manage",
        "ControlMutation",
        "TokenInfo",
        "yes",
    ],
    [
        "S07",
        "GET",
        "/api/v1/profiles/{profile}/memories",
        "memories.list",
        "memory.read",
        "MemoryListQuery",
        "Page[Memory]",
        "no",
    ],
    ["S07", "GET", "/api/v1/profiles/{profile}/memories/{id}", "memories.get", "memory.read", "Empty", "Memory", "no"],
    [
        "S07",
        "GET",
        "/api/v1/profiles/{profile}/memories/{id}/receipt",
        "receipts.get",
        "memory.read",
        "Empty",
        "Receipt",
        "no",
    ],
    [
        "S07",
        "GET",
        "/api/v1/profiles/{profile}/memories/{id}/history",
        "history.list",
        "memory.read",
        "PageQuery",
        "Page[HistoryEvent]",
        "no",
    ],
    ["S07", "GET", "/api/v1/profiles/{profile}/lookup", "lookup", "memory.read", "LookupQuery", "LookupResult", "no"],
    [
        "S07",
        "POST",
        "/api/v1/profiles/{profile}/memories/{id}/reveal",
        "memories.reveal",
        "memory.reveal",
        "RevealRequest",
        "Memory",
        "no",
    ],
    [
        "S08",
        "POST",
        "/api/v1/profiles/{profile}/search",
        "search",
        "memory.read",
        "SearchRequest",
        "SearchResult",
        "no",
    ],
    [
        "S08",
        "POST",
        "/api/v1/profiles/{profile}/context",
        "context",
        "memory.read",
        "ContextRequest",
        "ContextResult",
        "no",
    ],
    [
        "S09",
        "GET",
        "/api/v1/profiles/{profile}/memories/{id}/relations",
        "relations.for_memory",
        "relation.read",
        "PageQuery",
        "Page[Relation]",
        "no",
    ],
    [
        "S09",
        "GET",
        "/api/v1/profiles/{profile}/relations",
        "relations.list",
        "relation.read",
        "PageQuery",
        "Page[Relation]",
        "no",
    ],
    [
        "S09",
        "GET",
        "/api/v1/profiles/{profile}/relations/{id}",
        "relations.get",
        "relation.read",
        "Empty",
        "Relation",
        "no",
    ],
    [
        "S09",
        "GET",
        "/api/v1/profiles/{profile}/graph/neighborhood",
        "graph.neighborhood",
        "graph.read",
        "NeighborhoodQuery",
        "GraphResult",
        "no",
    ],
    [
        "S09",
        "POST",
        "/api/v1/profiles/{profile}/graph/path",
        "graph.path",
        "graph.read",
        "PathRequest",
        "GraphResult",
        "no",
    ],
    [
        "S09",
        "GET",
        "/api/v1/profiles/{profile}/graph/entity/{entity_id}",
        "graph.entity",
        "graph.read",
        "Empty",
        "GraphResult",
        "no",
    ],
    ["S10", "GET", "/api/v1/profiles/{profile}/audit", "audit", "audit.read", "AuditQuery", "AuditResult", "no"],
    [
        "S10",
        "GET",
        "/api/v1/profiles/{profile}/indexes/status",
        "indexes.status",
        "profile.read",
        "Empty",
        "IndexStatus",
        "no",
    ],
    [
        "S10",
        "GET",
        "/api/v1/profiles/{profile}/outbox",
        "outbox.list",
        "audit.read",
        "OutboxQuery",
        "Page[OutboxItem]",
        "no",
    ],
    ["S10", "GET", "/api/v1/profiles/{profile}/overview", "overview", "profile.read", "Empty", "Overview", "no"],
    [
        "S10",
        "POST",
        "/api/v1/profiles/{profile}/reflect",
        "reflect",
        "audit.read",
        "ReflectRequest",
        "ReflectResult",
        "no",
    ],
    [
        "S11",
        "GET",
        "/api/v1/profiles/{profile}/retrieval/trace/{id}",
        "traces.get",
        "memory.read",
        "Empty",
        "Trace",
        "no",
    ],
    [
        "S11",
        "GET",
        "/api/v1/profiles/{profile}/retrieval/traces",
        "traces.list",
        "memory.read",
        "PageQuery",
        "Page[TraceSummary]",
        "no",
    ],
    [
        "S13",
        "POST",
        "/api/v1/profiles/{profile}/memories",
        "memories.create",
        "memory.write",
        "RememberRequest",
        "WriteResult",
        "yes",
    ],
    ["S13", "POST", "/api/v1/profiles/{profile}/learn", "learn", "memory.write", "LearnRequest", "WriteResult", "yes"],
    [
        "S14",
        "POST",
        "/api/v1/profiles/{profile}/memories/{id}/transition",
        "memories.transition",
        "memory.transition",
        "TransitionRequest",
        "TransitionResult",
        "yes",
    ],
    [
        "S14",
        "POST",
        "/api/v1/profiles/{profile}/memories/{id}/invalidate",
        "memories.invalidate",
        "memory.invalidate",
        "InvalidateRequest",
        "TransitionResult",
        "yes",
    ],
    [
        "S15",
        "GET",
        "/api/v1/profiles/{profile}/beliefs",
        "beliefs.list",
        "belief.read",
        "BeliefListQuery",
        "Page[Memory]",
        "no",
    ],
    ["S15", "GET", "/api/v1/profiles/{profile}/beliefs/{id}", "beliefs.get", "belief.read", "Empty", "Memory", "no"],
    [
        "S15",
        "POST",
        "/api/v1/profiles/{profile}/beliefs",
        "beliefs.set",
        "belief.write",
        "BeliefRequest",
        "WriteResult",
        "yes",
    ],
    [
        "S15",
        "GET",
        "/api/v1/profiles/{profile}/conflicts",
        "conflicts.list",
        "belief.read",
        "PageQuery",
        "Page[Conflict]",
        "no",
    ],
    [
        "S15",
        "POST",
        "/api/v1/profiles/{profile}/conflicts/{id}/resolve",
        "conflicts.resolve",
        "belief.resolve",
        "ResolveRequest",
        "ConflictResult",
        "yes",
    ],
    [
        "S16",
        "POST",
        "/api/v1/profiles/{profile}/relations",
        "relations.create",
        "relation.write",
        "RelationRequest",
        "RelationResult",
        "yes",
    ],
    [
        "S16",
        "DELETE",
        "/api/v1/profiles/{profile}/relations/{id}",
        "relations.delete",
        "relation.write",
        "RelationDeleteRequest",
        "RelationResult",
        "yes",
    ],
    [
        "S17",
        "POST",
        "/api/v1/profiles/{profile}/admission/preview",
        "admission.preview",
        "memory.write",
        "AdmissionRequest",
        "AdmissionResult",
        "no",
    ],
    [
        "S17",
        "POST",
        "/api/v1/profiles/{profile}/memories/{id}/preview",
        "mutations.preview",
        "memory.read",
        "PreviewRequest",
        "PreviewResult",
        "no",
    ],
    ["S18", "GET", "/api/v1/jobs/{id}", "jobs.get", "index.manage", "Empty", "Job", "no"],
    [
        "S19",
        "POST",
        "/api/v1/profiles/{profile}/outbox/replay",
        "outbox.replay",
        "index.manage",
        "MaintenanceRequest",
        "JobAccepted",
        "yes",
    ],
    [
        "S20",
        "POST",
        "/api/v1/profiles/{profile}/indexes/rebuild",
        "indexes.rebuild",
        "index.manage",
        "RebuildRequest",
        "JobAccepted",
        "yes",
    ],
    ["S21", "GET", "/api/v1/events", "events", "memory.read", "EventQuery", "SSE[Event]", "no"],
    ["S22", "POST", "/api/v1/session", "sessions.create", "BEARER", "SessionExchange", "SessionInfo", "no"],
    ["S22", "GET", "/api/v1/session", "sessions.get", "SESSION", "Empty", "SessionInfo", "no"],
    ["S22", "DELETE", "/api/v1/session", "sessions.delete", "SESSION+CSRF", "Empty", "Health", "no"],
    [
        "S27",
        "GET",
        "/api/v1/profiles/{profile}/quality",
        "quality",
        "audit.read",
        "QualityQuery",
        "QualityResult",
        "no",
    ],
    [
        "S28",
        "GET",
        "/api/v1/profiles/{profile}/benchmarks",
        "benchmarks.list",
        "audit.read",
        "PageQuery",
        "Page[BenchmarkReport]",
        "no",
    ],
    [
        "S28",
        "GET",
        "/api/v1/profiles/{profile}/benchmarks/{id}",
        "benchmarks.get",
        "audit.read",
        "Empty",
        "BenchmarkReport",
        "no",
    ],
]
EXPECTED_ERRORS = {
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


def modules():
    return tuple(
        importlib.import_module("memory_server.gateway_contracts." + name) for name in ("dto", "ports", "catalog")
    )


def example(model):
    schema = model.model_json_schema()
    refs = schema.get("$defs", {})

    def value(spec):
        if "$ref" in spec:
            return value(refs[spec["$ref"].split("/")[-1]])
        if "const" in spec:
            return spec["const"]
        if "enum" in spec:
            return spec["enum"][0]
        if "anyOf" in spec:
            return value(next((v for v in spec["anyOf"] if v.get("type") == "null"), spec["anyOf"][0]))
        kind = spec.get("type")
        if kind == "object":
            return {key: value(spec["properties"][key]) for key in spec.get("required", [])}
        if kind == "array":
            return [value(spec["items"]) for _ in range(spec.get("minItems", 0))]
        if kind == "string":
            if spec.get("format") == "date-time":
                return "2026-10-28T00:00:00Z"
            return "x" * max(1, spec.get("minLength", 0))
        if kind in ("number", "integer"):
            return spec.get("minimum", 0)
        if kind == "boolean":
            return False
        return None

    result = value(schema)
    if model.__name__ == "NeighborhoodQuery":
        result.update(memory_id="x", entity_id=None)
    if model.__name__ == "ResolveRequest":
        result["expected_versions"] = {result["belief_a_id"]: 1, result["belief_b_id"]: 1}
    if model.__name__ == "RelationRequest":
        result["expected_versions"] = {result["source_id"]: 1, result["target_id"]: 1}
    return result


def test_import_graph_in_fresh_process():
    script = """
import json, sys
import memory_server.gateway_contracts
import memory_server.gateway_contracts.dto
import memory_server.gateway_contracts.ports
import memory_server.gateway_contracts.catalog
blocked = sorted(name for name in sys.modules if
    name == 'memory_server.server' or
    name.startswith(('memory_server.transports', 'memory_server.providers',
                     'memory_server.plugins', 'memory_server.runtime',
                     'memory_server.application', 'storage', 'sqlalchemy')))
print(json.dumps({'blocked_modules': blocked,
                  'loaded_contracts': sorted(n for n in sys.modules
                    if n.startswith('memory_server.gateway_contracts'))}))
assert not blocked, blocked
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["blocked_modules"] == []
    print(result.stdout.strip())


@pytest.mark.parametrize("name", EXPECTED_FIELDS)
def test_exact_dto_fields_and_closed_strict_finite_models(name):
    dto, _, _ = modules()
    model = getattr(dto, name)
    expected = set(EXPECTED_FIELDS[name])
    actual = {field.alias or key for key, field in model.model_fields.items()}
    assert actual == expected
    assert model.model_config["strict"] is True
    assert model.model_config["extra"] == "forbid"
    assert model.model_config["allow_inf_nan"] is False
    data = example(model)
    if name == "Page":
        model = model[dto.Memory]
    obj = model.model_validate_json(json.dumps(data))
    assert model.model_validate_json(obj.model_dump_json(by_alias=True)) == obj
    with pytest.raises(ValidationError):
        model.model_validate_json(json.dumps({**data, "unexpected": True}))


@pytest.mark.parametrize("row", EXPECTED_ROUTES, ids=lambda r: r[3])
def test_catalogue_command_result_roundtrip_and_discriminators(row):
    dto, _, catalog = modules()
    owner, method, route, operation, permission, request, response, mutation = row
    entry = catalog.CATALOG.by_operation[operation]
    assert (entry.owner, entry.method, entry.route, entry.permission, entry.mutation) == (
        owner,
        method,
        route,
        permission,
        mutation == "yes",
    )
    assert entry.request_type.__name__ == request
    assert entry.result_type.__name__ == response.replace("SSE[Event]", "Event")
    assert catalog.CATALOG.by_route[(method, route)] is entry
    payload = example(entry.request_type)
    command_data = dict(
        operation=operation,
        profile_id="alpha" if "{profile}" in route else None,
        path_ids={name: "x" for name in entry.path_parameters},
        payload=payload,
    )
    command = TypeAdapter(dto.GatewayCommand).validate_json(json.dumps(command_data))
    assert command.operation == operation
    assert command.model_config["frozen"] is True
    assert TypeAdapter(dto.GatewayCommand).validate_json(command.model_dump_json()) == command
    assert catalog.CATALOG.validate(operation, command_data) == command
    with pytest.raises(ValidationError):
        command.operation = "health"
    for bad in (
        {**command_data, "operation": "unknown"},
        {**command_data, "extra": 1},
        {**command_data, "path_ids": {"unknown": "x"}},
        {**command_data, "profile_id": None if command_data["profile_id"] else "alpha"},
    ):
        with pytest.raises(ValidationError):
            TypeAdapter(dto.GatewayCommand).validate_json(json.dumps(bad))
    result = entry.result_type.model_validate_json(json.dumps(example(entry.result_type)))
    assert TypeAdapter(dto.GatewayResult).validate_python(result) is result
    assert entry.result_type.model_validate_json(result.model_dump_json()) == result


def test_catalogue_rejects_duplicates_and_unknown_operation():
    dto, _, catalog = modules()
    first = catalog.CATALOG.entries[0]
    with pytest.raises(ValueError, match="duplicate"):
        catalog.RouteCatalog((first, first))
    with pytest.raises(ValueError, match="duplicate"):
        catalog.RouteCatalog((first, dataclasses.replace(first, operation="ready")))
    with pytest.raises(ValueError, match="duplicate"):
        catalog.RouteCatalog((first, dataclasses.replace(first, route="/other")))
    with pytest.raises(ValueError):
        catalog.CATALOG.validate("unknown", {})
    assert set(typing.get_args(dto.Operation)) == set(catalog.CATALOG.by_operation)
    assert len(catalog.CATALOG.entries) == len(EXPECTED_ROUTES)
    assert catalog.ERROR_HTTP_STATUS == EXPECTED_ERRORS
    assert set(typing.get_args(dto.ErrorCode)) == set(EXPECTED_ERRORS)


@pytest.mark.parametrize("bad", ["1", "1.0.0", True, 0, -1, 1.1])
def test_integer_cas_distinct_from_skill_version(bad):
    dto, _, _ = modules()
    data = example(dto.TransitionRequest)
    data["expected_version"] = bad
    with pytest.raises(ValidationError):
        dto.TransitionRequest.model_validate(data)
    skill = dto.SkillPayload.model_validate({**example(dto.SkillPayload), "skill_version": "1.2.3"})
    assert skill.skill_version == "1.2.3"
    assert "expected_version" not in dto.SkillPayload.model_fields


def test_unknown_provenance_null_and_fresh_defaults():
    dto, _, _ = modules()
    for model in (
        dto.Memory,
        dto.HistoryEvent,
        dto.EvidenceSummary,
        dto.GraphConsistency,
        dto.IndexStatus,
        dto.BenchmarkReport,
        dto.AdmissionResult,
        dto.Job,
    ):
        data = example(model)
        instance = model.model_validate_json(json.dumps(data))
        for name, field in model.model_fields.items():
            if not field.is_required() and field.default is None:
                assert getattr(instance, name) is None
                assert instance.model_dump()[name] is None
    left = dto.LearnRequest(text="test")
    right = dto.LearnRequest(text="test")
    left.expected_versions["x"] = 1
    assert right.expected_versions == {}
    left = dto.PageQuery()
    assert left.cursor is None


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_numbers_rejected(bad):
    dto, _, _ = modules()
    with pytest.raises(ValidationError):
        dto.ReflectResult(mode="overview", sections={"nested": [bad]}, complete=True, request_id="req_x")
    with pytest.raises(ValidationError):
        dto.OperationalPoint.model_validate({**example(dto.OperationalPoint), "value": bad})


@pytest.mark.parametrize("stamp", ["2026-10-28T00:00:00", "2026-10-28T00:00:00+03:00", 0])
def test_utc_dates_only(stamp):
    dto, _, _ = modules()
    with pytest.raises(ValidationError):
        dto.TokenCreate.model_validate_json(json.dumps({**example(dto.TokenCreate), "expires_at": stamp}))


def test_token_creation_replay_nullable_secret():
    dto, _, _ = modules()
    data = {**example(dto.TokenCreated), "secret": None}
    assert dto.TokenCreated.model_validate_json(json.dumps(data)).secret is None
    with pytest.raises(ValidationError):
        dto.TokenCreated.model_validate_json(json.dumps({"token": data["token"]}))


def test_context_result_requires_nullable_wire_fields():
    # C4:425 — estimated_tokens/tokenizer are required-nullable, not optional.
    dto, _, _ = modules()
    data = {**example(dto.ContextResult), "estimated_tokens": None, "tokenizer": None}
    parsed = dto.ContextResult.model_validate_json(json.dumps(data))
    assert parsed.estimated_tokens is None and parsed.tokenizer is None
    for missing in ("estimated_tokens", "tokenizer"):
        payload = {k: v for k, v in data.items() if k != missing}
        with pytest.raises(ValidationError):
            dto.ContextResult.model_validate_json(json.dumps(payload))


def test_result_discriminator_requires_operation_for_wire_decode():
    dto, _, catalog = modules()
    # C1 result union has no C4 wire tag; ambiguous Page JSON is not guessed.
    data = example(dto.Page[dto.Memory])
    with pytest.raises(ValidationError):
        TypeAdapter(dto.GatewayResult).validate_json(json.dumps(data))
    entry = catalog.CATALOG.by_operation["memories.list"]
    assert entry.result_type.model_validate_json(json.dumps(data)).items == []


def test_constraints_and_cas_maps():
    dto, _, _ = modules()
    invalid = [
        (dto.PageQuery, {"limit": "1"}),
        (dto.PageQuery, {"limit": 201}),
        (dto.SearchRequest, {"query": ""}),
        (dto.SearchRequest, {"limit": 101}),
        (dto.LearnRequest, {"text": "x" * 32769}),
        (dto.LearnRequest, {"expected_versions": {"x": "1"}}),
        (dto.ControlMutation, {"confirmation": 1}),
        (dto.NeighborhoodQuery, {"memory_id": None, "entity_id": None}),
        (dto.NeighborhoodQuery, {"memory_id": "a", "entity_id": "b"}),
        (dto.ResolveRequest, {"expected_versions": {}}),
        (dto.RelationRequest, {"expected_versions": {}}),
        (dto.QualityQuery, {"to": "2026-12-01T00:00:00Z"}),
    ]
    for model, override in invalid:
        with pytest.raises(ValidationError):
            model.model_validate_json(json.dumps({**example(model), **override}))


def test_port_protocols_and_context_dataclasses():
    dto, ports, _ = modules()
    names = (
        "ContractCatalog ProfileRegistry TokenService ProfileLeases AccessSink HttpFactory MemoryReadPort "
        "RetrievalPort GraphReadPort DiagnosticsPort TracePort MutationExecutor IngestionPort LifecyclePort "
        "BeliefsPort RelationsWritePort InspectionPort JobPort ReplayRunner RebuildRunner EventSource SessionService "
        "QualityPort BenchmarkPort BetaComposition MCPHttpFactory GatewayPort "
        "MutationHandler TraceObserver MetricsSink "
        "StudioClient StudioMemory StudioGraph StudioRetrieval StudioOperations"
    ).split()
    for name in names:
        protocol = getattr(ports, name)
        assert protocol._is_protocol
        with pytest.raises(TypeError):
            protocol()
    for name in (
        "Principal RequestContext ProfileRecord ProfileBinding ProfileSession "
        "AuditTicket AuditOutcome GatewayDependencies HttpConfig"
    ).split():
        cls = getattr(ports, name)
        assert dataclasses.is_dataclass(cls)
        assert cls.__dataclass_params__.frozen
    assert "self" not in inspect.signature(ports.StudioMemory.mount_memory).parameters
    assert set(inspect.signature(ports.MutationExecutor.run).parameters) == {"self", "ctx", "command", "handler"}
    assert "ProfileMutationCommand" in str(inspect.signature(ports.MutationExecutor.run))
    assert inspect.isasyncgenfunction(ports.EventSource.subscribe) is False
    assert inspect.iscoroutinefunction(ports.EventSource.subscribe) is False
    assert inspect.iscoroutinefunction(ports.TracePort.execute)
    assert not inspect.iscoroutinefunction(ports.TracePort.observe)
    assert set(typing.get_args(dto.Permission)) == {r[4] for r in EXPECTED_ROUTES if "." in r[4]}


def test_closed_vocabularies():
    dto, _, _ = modules()
    for model, updates in (
        (dto.SourceFailure, {"code": "E_MADE_UP"}),
        (dto.Event, {"type": "conflict.detected"}),
        (dto.TokenCreate, {"permissions": ["admin"]}),
        (dto.RelationRequest, {"relation_type": "derived_from"}),
    ):
        with pytest.raises(ValidationError):
            model.model_validate_json(json.dumps({**example(model), **updates}))
    assert "belief.conflict_detected" in typing.get_args(dto.EventType)


def test_no_concrete_neighbour_imports():
    _, ports, _ = modules()
    import ast

    for filename in ("dto.py", "ports.py", "catalog.py", "__init__.py"):
        tree = ast.parse((Path(ports.__file__).parent / filename).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                assert not node.module.startswith(
                    (
                        "memory_server.runtime",
                        "memory_server.application",
                        "memory_server.transports",
                        "memory_server.providers",
                        "storage",
                    )
                )


EXPECTED_PORT_SIGNATURES = {
    "MetricsSink": {
        "http_completed": (
            "self, ctx: RequestContext, operation: Operation, status: int, duration_seconds: float",
            "None",
            False,
        ),
        "retrieval_completed": (
            "self, ctx: RequestContext, stage: Literal['fts', 'semantic', 'graph', 'merge', 'context'], "
            "duration_seconds: float, error_code: ErrorCode | None",
            "None",
            False,
        ),
        "projection_snapshot": (
            "self, profile_id: str, pending: int, vector_lag: int | None, graph_lag: int | None",
            "None",
            False,
        ),
    },
    "MutationHandler": {
        "apply": ("self, session: AsyncSession, command: ProfileMutationCommand", "MutationResult", True)
    },
    "TraceObserver": {
        "stage": (
            "self, name: Literal['fts', 'semantic', 'graph', 'context'], items: list[TraceContribution], "
            "status: Literal['ran', 'skipped', 'failed']",
            "None",
            False,
        ),
        "merged": ("self, items: list[TraceContribution], deduplicated: list[TraceContribution]", "None", False),
        "final": ("self, items: list[TraceContribution]", "None", False),
    },
    "GatewayPort": {"execute": ("self, ctx: RequestContext, command: GatewayCommand", "GatewayResult", True)},
    "ContractCatalog": {"validate": ("self, operation: Operation, payload: JSONValue", "GatewayCommand", False)},
    "ProfileRegistry": {"resolve": ("self, principal: Principal, profile_id: str", "ProfileBinding", True)},
    "TokenService": {"authenticate": ("self, bearer: str, *, now: datetime", "Principal", True)},
    "ProfileLeases": {"acquire": ("self, ctx: RequestContext", "AsyncContextManager[ProfileSession]", False)},
    "AccessSink": {
        "begin": ("self, ctx: RequestContext, operation: Operation", "AuditTicket", True),
        "finish": ("self, ticket: AuditTicket, outcome: AuditOutcome", "None", True),
    },
    "HttpFactory": {
        "create_http_app": ("self, config: HttpConfig, dependencies: GatewayDependencies", "ASGIApp", False)
    },
    "MemoryReadPort": {"execute": ("self, ctx: RequestContext, command: MemoryReadCommand", "MemoryReadResult", True)},
    "RetrievalPort": {"execute": ("self, ctx: RequestContext, command: RetrievalCommand", "RetrievalResult", True)},
    "GraphReadPort": {"execute": ("self, ctx: RequestContext, command: GraphReadCommand", "GraphReadResult", True)},
    "DiagnosticsPort": {
        "execute": ("self, ctx: RequestContext, command: DiagnosticsCommand", "DiagnosticsResult", True)
    },
    "TracePort": {
        "observe": (
            "self, ctx: RequestContext, operation: Literal['search', 'context']",
            "AsyncContextManager[TraceObserver]",
            False,
        ),
        "execute": ("self, ctx: RequestContext, command: TraceCommand", "TraceResult", True),
    },
    "MutationExecutor": {
        "run": (
            "self, ctx: RequestContext, command: ProfileMutationCommand, handler: MutationHandler",
            "MutationResult",
            True,
        )
    },
    "IngestionPort": {"execute": ("self, ctx: RequestContext, command: IngestionCommand", "WriteResult", True)},
    "LifecyclePort": {"execute": ("self, ctx: RequestContext, command: LifecycleCommandV1", "TransitionResult", True)},
    "BeliefsPort": {"execute": ("self, ctx: RequestContext, command: BeliefsCommand", "BeliefsResult", True)},
    "RelationsWritePort": {
        "execute": ("self, ctx: RequestContext, command: RelationsWriteCommand", "RelationResult", True)
    },
    "InspectionPort": {"execute": ("self, ctx: RequestContext, command: InspectionCommand", "InspectionResult", True)},
    "JobPort": {
        "enqueue": (
            "self, ctx: RequestContext, operation: JobOperation, payload: MaintenanceRequest | RebuildRequest",
            "JobAccepted",
            True,
        ),
        "get": ("self, ctx: RequestContext, job_id: str", "Job", True),
    },
    "ReplayRunner": {
        "run": ("self, session: ProfileSession, job: Job, request: MaintenanceRequest", "JobResult", True)
    },
    "RebuildRunner": {"run": ("self, session: ProfileSession, job: Job, request: RebuildRequest", "JobResult", True)},
    "EventSource": {
        "subscribe": (
            "self, ctx: RequestContext, query: EventQuery, last_event_id: str | None",
            "AsyncIterator[Event]",
            False,
        ),
        "publish": ("self, event: Event", "None", True),
    },
    "SessionService": {
        "exchange": ("self, bearer: str, request: SessionExchange, *, origin: str", "SessionInfo", True),
        "authenticate": ("self, cookie: str, *, origin: str, csrf_token: str | None", "Principal", True),
    },
    "StudioMemory": {
        "mount_memory": ("api: StudioClient, root: Element, capabilities: frozenset[str]", "Callable[[], None]", False)
    },
    "StudioGraph": {
        "mount_graph": ("api: StudioClient, root: Element, capabilities: frozenset[str]", "Callable[[], None]", False)
    },
    "StudioRetrieval": {
        "mount_retrieval": (
            "api: StudioClient, root: Element, capabilities: frozenset[str]",
            "Callable[[], None]",
            False,
        )
    },
    "StudioOperations": {
        "mount_operations": (
            "api: StudioClient, root: Element, capabilities: frozenset[str]",
            "Callable[[], None]",
            False,
        )
    },
    "QualityPort": {"execute": ("self, ctx: RequestContext, command: QualityCommand", "QualityResult", True)},
    "BenchmarkPort": {"execute": ("self, ctx: RequestContext, command: BenchmarkCommand", "BenchmarkResult", True)},
    "BetaComposition": {"assemble": ("self, config: HttpConfig, dependencies: GatewayDependencies", "ASGIApp", False)},
    "MCPHttpFactory": {"create_mcp_http_app": ("self, dependencies: GatewayDependencies", "ASGIApp", False)},
}


@pytest.mark.parametrize("name", EXPECTED_PORT_SIGNATURES)
def test_all_stage_port_signatures_match_c1(name):
    import ast

    _, ports, _ = modules()
    tree = ast.parse(Path(ports.__file__).read_text())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == name)
    actual = {}
    for method in cls.body:
        if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
            assert len(method.body) == 1
            assert isinstance(method.body[0], ast.Expr) and method.body[0].value.value is Ellipsis
            actual[method.name] = (
                ast.unparse(method.args).replace("d.", ""),
                ast.unparse(method.returns).replace("d.", ""),
                isinstance(method, ast.AsyncFunctionDef),
            )
    assert actual == EXPECTED_PORT_SIGNATURES[name]


@pytest.mark.parametrize(
    "group",
    [
        "MemoryRead",
        "Retrieval",
        "GraphRead",
        "Diagnostics",
        "Trace",
        "Ingestion",
        "Lifecycle",
        "Beliefs",
        "RelationsWrite",
        "Inspection",
        "Quality",
        "Benchmark",
    ],
)
def test_owner_command_subsets(group):
    dto, _, catalog = modules()
    command_type = getattr(dto, "LifecycleCommandV1" if group == "Lifecycle" else group + "Command")
    adapter = TypeAdapter(command_type)
    for operation in dto.OWNER_OPERATIONS[group]:
        entry = catalog.CATALOG.by_operation[operation]
        data = dict(
            operation=operation,
            profile_id="alpha",
            payload=example(entry.request_type),
            path_ids={key: "x" for key in entry.path_parameters},
        )
        assert adapter.validate_json(json.dumps(data)).operation == operation
    with pytest.raises(ValidationError):
        adapter.validate_json(json.dumps(dict(operation="health", profile_id=None, path_ids={}, payload={})))


def test_profile_control_mutation_separation():
    dto, _, _ = modules()
    assert set(dto.PROFILE_MUTATIONS) == {
        "memories.create",
        "learn",
        "memories.transition",
        "memories.invalidate",
        "beliefs.set",
        "conflicts.resolve",
        "relations.create",
        "relations.delete",
    }
    assert set(dto.CONTROL_MUTATIONS) == {
        "tokens.create",
        "tokens.revoke",
        "profiles.enable",
        "profiles.disable",
        "outbox.replay",
        "indexes.rebuild",
    }
    assert not set(dto.PROFILE_MUTATIONS) & set(dto.CONTROL_MUTATIONS)
    control = dict(
        operation="profiles.enable",
        profile_id="alpha",
        path_ids={},
        payload=dict(reason="test", confirmation=True, expected_generation=1),
    )
    with pytest.raises(ValidationError):
        TypeAdapter(dto.ProfileMutationCommand).validate_json(json.dumps(control))
    assert TypeAdapter(dto.ControlMutationCommand).validate_json(json.dumps(control)).operation == "profiles.enable"


def test_real_legacy_context_serialization(utc_timestamp):
    from memory_server.models.decision import Decision
    from memory_server.models.fact import Fact

    dto, _, _ = modules()
    fact = Fact(id="fact", subject="s", predicate="p", object="o", created_at=utc_timestamp, updated_at=utc_timestamp)
    decision = Decision(
        id="decision", context="c", choice="a", reason="r", created_at=utc_timestamp, updated_at=utc_timestamp
    )
    payload = dict(facts=[fact.model_dump(mode="json")], decisions=[decision.model_dump(mode="json")], total=2)
    assert dto.ContextPackage.model_validate_json(json.dumps(payload)).model_dump(mode="json") == payload
