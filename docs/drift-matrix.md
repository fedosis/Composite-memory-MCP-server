# Contract Drift Matrix — v0.6 Phase 0

Audit date: 2026-07-07
Source files: README.md, docs/ADR.md, docs/architecture.md, src/memory_server/server.py, src/memory_server/api/*.py, src/memory_server/models/*.py, src/memory_server/evaluation/*.py, src/memory_server/providers/*.py

## Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Match |
| ⚠️ | Minor drift (cosmetic / docs only) |
| ❌ | Missing (in code or in docs) |
| ➕ | Extra (in code but undocumented) |

---

## 1. ping

### README
- **Args**: None
- **Response**: `{"status": "ok"}`
- **Error format**: N/A

### Code (server.py:68-69)
- **Args**: None (`ping() -> str`)
- **Response**: `{"status": "ok"}`
- **Error format**: MCP SDK handles transport errors

### Status: ✅ Match

---

## 2. remember

### README
- **Args**: subject (str, required), predicate (str, required), object (str, required), confidence (float, optional, default 1.0), source (str, optional, default "manual"), tags (list, optional)
- **Response**: `{receipt: {id, memory_type, confidence, source, verification_status, timestamp}, fact: {id, subject, predicate, object, confidence, source, created_at, updated_at}}`
- **Error format**: JSON error message

### Code (server.py:117-159 + api/remember.py)
- **Args**: subject (str, required), predicate (str, required), object (str, required), confidence (float, optional, default 1.0), source (str, optional, default "user")
- **❌ tags param missing** — README lists it but code does not accept it
- **⚠️ source default mismatch** — README says "manual", code says "user"
- **Response**: `{receipt: {id, memory_type, source, created_by, timestamp, confidence, verification_status, history}, fact: {id, subject, predicate, object, confidence, source, created_at}}`
- **⚠️ receipt.updated_at missing** — spec requires updated_at but receipt model doesn't have it
- **⚠️ receipt.created_by** exists in code response but not in README
- **⚠️ receipt.history** exists in code response but not in README
- **⚠️ fact.updated_at missing** — Fact model has only created_at, no updated_at

### Status: ⚠️ Multiple drifts

---

## 3. search

### README
- **Args**: query (str, required), subject (str, optional), predicate (str, optional), object (str, optional), source (str, optional), limit (int, optional, default 10)
- **Response**: `{total: int, results: [{id, subject, predicate, object, confidence, source, created_at, updated_at}], query: string}`
- **❌ Default limit**: README says 10, code says 50
- **➕ Extra args in README**: object, source — accepted by SQLiteProvider.search_facts but **not** exposed at MCP tool level (server.py:72-95 has query/subject/predicate/limit only)

### Code (server.py:72-95 + api/search.py)
- **Args**: query (str), subject (str), predicate (str), limit (int, default 50)
- **❌ object, source params not exposed** at MCP tool layer (server.py)
- **Response**: `{results: [{id, subject, predicate, object, confidence, source, created_at}], total: int}`
- **❌ query field missing** from response (README shows it, code does not)
- **⚠️ fact.updated_at missing** — Fact model has only created_at

### Status: ⚠️ Multiple drifts (args, default limit, response fields)

---

## 4. get_context

### README
- **Args**: task (str, required), subject (str, optional), max_results (int, optional, default 10)
- **Response**: `{total: int, facts: [{id, subject, predicate, object, confidence, source, created_at, updated_at}], task: string}`

### Code (server.py:97-114 + api/get_context.py)
- **Args**: task (str, required), subject (str), max_results (int, default 10)
- **Response**: `{facts: [{id, subject, predicate, object, confidence, source, created_at}], decisions: [], total: int}`
- **❌ task field missing** from response
- **⚠️ decisions: []** is returned (empty list) but README doesn't mention it
- **⚠️ fact.updated_at missing** — Fact model has no updated_at

### Status: ⚠️ Minor drift (missing response field, extra field)

---

## 5. semantic_search

### README
- **Args**: query (str, required), top_k (int, optional, default 10), score_threshold (float, optional, default 0.0)
- **Response (rule match)**: `{rule_match: {route, rule_name, matched_keyword}}`
- **Response (semantic)**: `{semantic_results: [{id, score, payload}], total: int}`

### Code (server.py:233-256 + router/embedding_router.py)
- **Args**: query (str), top_k (int, default 10), score_threshold (float, default 0.0)
- **Response**: Same as README — `{rule_match: ...}` or `{semantic_results: [...], total: int}` or `{error: "Empty query"}`
- **⚠️ Error response undocumented** — `{error: "Empty query"}` returned for empty query but not documented

### Status: ⚠️ Minor drift (undocumented error case)

---

## 6. learn

### README
- **Args**: text (str, required), source (str, optional, default "user")
- **Response**: `{facts: [{receipt, item}], decisions: [{receipt, item}], skills: [{receipt, item}], receipts: [{receipt...}]}`

### Code (server.py:162-230 + api/learn.py)
- **Args**: text (str, required), source (str, optional, default "user")
- **Response**: Same structure, but:
  - **⚠️ facts: item format** — README shows item with fact fields (subject, predicate, object), code returns full Fact.model_dump(mode="json") which also includes id, confidence, source, created_at
  - **⚠️ decisions: item format** — README shows {context, choice, reason, source}, code returns full Decision.model_dump
  - **⚠️ skills: item format** — README shows {purpose, steps, success_rate}, code returns full Skill.model_dump including name, version, constraints, validation
  - **⚠️ Auto-indexing side effect** — directly writes to Qdrant + graph (not documented in README) — will be replaced by outbox in Phase 4

### Status: ⚠️ Response field detail mismatch

---

## 7. graph_search

### README
- **Args**: query (str, optional), entity_id (str, optional), source_id (str, optional), target_id (str, optional)
- **Response**: `{nodes: [{id, name, type, attributes}], edges: [{source_id, target_id, relation, attributes}], paths: [...]}`

### Code (server.py:267-335 + router/graph_router.py)
- **Args**: query (str), entity_id (str), source_id (str), target_id (str)
- **Response nodes**: `{id, name, type, attributes}`
- **Response edges**: `{source_id, target_id, relation, attributes}`
- **Response paths**: `[{id, name, type}]`
- **⚠️ Graph router returns extra fields**: For `query` mode, edges include `source_name`, `target_name`, `target_type` (beyond README)
- **⚠️ Path response**: Path nodes have `{id, name, type}` without `attributes` (README shows attributes field)

### Status: ⚠️ Edge/path field mismatch between direct lookup and query mode

---

## 8. route

### README
- **Args**: query (str, required), top_k (int, optional, default 10), score_threshold (float, optional, default 0.0)
- **Response (stage 1)**: `{stage: 1, route: "rules", rule_match: {route, rule_name, matched_keyword}}`
- **Response (stage 2)**: `{stage: 2, route: "semantic", semantic_results, total}`
- **Response (stage 3)**: `{stage: 3, route: "graph", graph_result: {entities, relations, paths}}`
- **Response (stage 4)**: `{stage: 4, route: "llm_fallback", message}`

### Code (server.py:413-435 + router/hybrid_router.py)
- **Args**: query (str), top_k (int, default 10), score_threshold (float, default 0.0)
- **Response**: Matches README for all 4 stages
- **Response graph_result**: `{entities, relations, paths}` — matches

### Status: ✅ Match

---

## 9. audit

### README
- **Args**: `audit_type` (optional, default "full", choices: "full", "consistency", "orphans", "confidence")
- **Response**: `{audit_type, warnings, errors, stats}`

### Code (server.py:438-463 + evaluation/auditor.py)
- **Args**: audit_type (str, default "full")
- **Response**: `{audit_type, warnings, errors, stats}` — matches
- **⚠️ Audit depth**: Current audit checks consistency, orphans, and confidence only. Missing: lifecycle violations, SQL/vector drift, SQL/graph drift, missing receipts (to be expanded in Phase 7)

### Status: ⚠️ Audit scope narrower than spec (Phase 7 target)

---

## Data Models — Required Fields Audit

Per spec, ALL models must have these fields:
```
id, type, content, source, creator, created_at, updated_at,
confidence, verification_status, lifecycle_state, version
```

### Entity (src/memory_server/models/entity.py)
| Field | Present | Notes |
|-------|---------|-------|
| id | ✅ | |
| type | ✅ | |
| content | ❌ | Uses `name` + `attributes` instead |
| source | ❌ | Missing |
| creator | ❌ | Missing |
| created_at | ✅ | |
| updated_at | ✅ | |
| confidence | ❌ | Missing |
| verification_status | ❌ | Missing |
| lifecycle_state | ❌ | Missing |
| version | ❌ | Missing |

### Fact (src/memory_server/models/fact.py)
| Field | Present | Notes |
|-------|---------|-------|
| id | ✅ | |
| type | ❌ | Not explicitly stored; derived from memory_type in receipt |
| content | ❌ | Uses `subject`, `predicate`, `object` |
| source | ✅ | Optional |
| creator | ❌ | Not on Fact model (exists on MemoryReceipt as `created_by`) |
| created_at | ✅ | |
| updated_at | ❌ | Missing |
| confidence | ✅ | |
| verification_status | ❌ | On MemoryReceipt, not on Fact |
| lifecycle_state | ❌ | Not on Fact model |
| version | ❌ | Missing |

### Decision (src/memory_server/models/decision.py)
| Field | Present | Notes |
|-------|---------|-------|
| id | ✅ | |
| type | ❌ | Not explicitly stored |
| content | ❌ | Uses `context`, `choice`, `reason` |
| source | ✅ | Optional |
| creator | ❌ | Not on Decision model |
| created_at | ✅ | |
| updated_at | ❌ | Missing |
| confidence | ❌ | Not on Decision model |
| verification_status | ❌ | Missing |
| lifecycle_state | ❌ | Missing |
| version | ❌ | Missing |

### Skill (src/memory_server/models/skill.py)
| Field | Present | Notes |
|-------|---------|-------|
| id | ✅ | |
| type | ❌ | Not explicitly stored |
| content | ❌ | Uses `purpose`, `steps`, `constraints`, `validation` |
| source | ❌ | Missing |
| creator | ❌ | Missing |
| created_at | ✅ | |
| updated_at | ❌ | Missing |
| confidence | ❌ | Uses `success_rate` (related but different name) |
| verification_status | ❌ | Missing |
| lifecycle_state | ❌ | Missing |
| version | ✅ | Present as `version` (str "1.0.0" default) |

### MemoryReceipt (src/memory_server/models/receipt.py)
| Field | Present | Notes |
|-------|---------|-------|
| id | ✅ | |
| type | ✅ | As `memory_type` |
| content | ❌ | N/A for receipt |
| source | ✅ | |
| creator | ✅ | As `created_by` |
| created_at | ✅ | As `timestamp` |
| updated_at | ❌ | Missing |
| confidence | ✅ | |
| verification_status | ✅ | |
| lifecycle_state | ❌ | Missing |
| version | ❌ | Missing |

---

## Lifecycle State

### README Verification Statuses
```
candidate → validated → trusted → deprecated → archived
```

### Code (VerificationStatus enum in models/receipt.py)
```
UNVERIFIED, CANDIDATE, VALIDATED, TRUSTED, DEPRECATED, ARCHIVED
```

- **⚠️ UNVERIFIED** extra status in code, not in README
- **⚠️ lifecycle_state (spec)**: Per v0.6 spec, lifecycle has 6 states: candidate → validated → active → stale → archived → forgotten. The current code uses different states (unverified, candidate, validated, trusted, deprecated, archived).
- **⚠️ No "forgotten" state** anywhere
- **⚠️ No "stale" state** anywhere
- **⚠️ Validator in-memory only** — Validator._store is a dict, not persisted to SQL

---

## Missing Components

| Component | Needed For | Phase |
|-----------|-----------|-------|
| JSON Schema contracts (contracts/*.schema.json) | Contract stability | Phase 1 |
| `updated_at` on all models | Provenance | Phase 2 |
| `lifecycle_state` on all models | Lifecycle engine | Phase 2, 5 |
| `verification_status` on Entity, Decision, Skill | Canonical data model | Phase 2 |
| `source` on Entity, Skill | Canonical data model | Phase 2 |
| `confidence` on Entity, Decision | Canonical data model | Phase 2 |
| `creator` on Entity, Fact, Decision, Skill | Provenance | Phase 2 |
| Alembic migrations | Storage layer | Phase 3 |
| WAL mode for SQLite | Performance | Phase 3 |
| Outbox pattern (transactional indexing) | Data integrity | Phase 4 |
| FTS5 for keyword search | Retrieval | Phase 6 |
| Multi-source ranking layer | Retrieval | Phase 6 |
| Enhanced audit checks | Data integrity | Phase 7 |
| OpenTelemetry + Prometheus metrics | Observability | Phase 8 |
| CI/CD pipeline | Release quality | Phase 9 |

---

## Summary

| Tool | Status |
|------|--------|
| ping | ✅ Match |
| remember | ⚠️ 7 drifts |
| search | ⚠️ 5 drifts |
| get_context | ⚠️ 3 drifts |
| semantic_search | ⚠️ 1 drift (undocumented error) |
| learn | ⚠️ 4 drifts |
| graph_search | ⚠️ 2 drifts |
| route | ✅ Match |
| audit | ⚠️ Scope needs expansion |

**Canonical behavior selected**: Code is the source of truth for contracts. README and docs will be updated post-freeze to match.

**Total inconsistencies documented**: 24+
**Canonical behavior anchor**: The MCP tool implementations in server.py + api/*.py define the actual contract.
