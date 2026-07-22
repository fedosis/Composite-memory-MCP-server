# PLAN.md — Composite Memory MCP Server

## Phase: v0.1a (current)
Goal: MCP API + SQLite backend + get_context/search/remember

| # | Card | ID | Status | Review | Tests | Merged |
|---|------|----|--------|--------|-------|--------|
| 001 | Project skeleton | t_ed422770 | ✅ done | ✅ approve | ✅ pass | ✅ v0.1a-alpha.1 |
| 002 | Pydantic data models | t_da4fd8bb | ✅ done | ✅ approve | ✅ pass | ✅ v0.1a-alpha.2 |
| 003 | SQLite provider | — | ✅ done | ✅ approve | ✅ pass | ✅ v0.1a-alpha.3 |
| 004 | MCP tool: get_context | — | ✅ done | ✅ approve | ✅ pass | ✅ v0.1a-alpha.4 |
| 005 | MCP tool: search | — | ✅ done | — | ✅ pass | ✅ v0.1a-alpha.5 |
| 006 | MCP tool: remember | — | ✅ done | — | ✅ pass | ✅ v0.1a-alpha.6 |
| 007 | Integration tests + docs | — | ✅ done | — | ✅ pass | ✅ v0.1a-alpha.7 |

## Phase: v0.2 (done)
Qdrant + embeddings + semantic router

| # | Card | ID | Status | Review | Tests | Merged |
|---|------|----|--------|--------|-------|--------|
| 008 | Qdrant integration | — | ✅ done | — | ✅ pass | ✅ v0.2-alpha.8 |
| 009 | Embedding provider | — | ✅ done | — | ✅ pass | ✅ v0.2-alpha.9 |
| 010 | Semantic router | — | ✅ done | — | ✅ pass | ✅ v0.2-alpha.10 |
| 011 | Router integration tests | — | ✅ done | — | ✅ pass | ✅ v0.2-alpha.11 |

## Phase: v0.3 (done)
LLM extractors + learn()

| # | Card | ID | Status | Review | Tests | Merged |
|---|------|----|--------|--------|-------|--------|
| 012 | FactExtractor | — | ✅ done | — | ✅ pass | ✅ v0.3-alpha.12 |
| 013 | DecisionExtractor | — | ✅ done | — | ✅ pass | ✅ v0.3-alpha.13 |
| 014 | SkillExtractor | — | ✅ done | — | ✅ pass | ✅ v0.3-alpha.14 |
| 015 | learn() MCP tool | — | ✅ done | — | ✅ pass | ✅ v0.3-alpha.15 |
| 016 | Integration tests + docs | — | ✅ done | — | ✅ pass | ✅ v0.3-alpha.16 |

## Phase: v0.4 (done)
Graph DB + entity relations

| # | Card | ID | Status | Review | Tests | Merged |
|---|------|----|--------|--------|-------|--------|
| 017 | In-memory graph engine | — | ✅ done | — | ✅ pass | ✅ v0.4-alpha.17 |
| 018 | Entity relation linker | — | ✅ done | — | ✅ pass | ✅ v0.4-alpha.18 |
| 019 | graph_search MCP tool | — | ✅ done | — | ✅ pass | ✅ v0.4-alpha.19 |
| 020 | Hybrid router (rules→embeddings→graph→LLM) | — | ✅ done | — | ✅ pass | ✅ v0.4-alpha.20 |
| 021 | Graph integration tests + docs | — | ✅ done | — | ✅ pass | ✅ v0.4-alpha.21 |

## Phase: v0.5 (done)
Confidence engine + validation + decay + memory auditor

| # | Card | ID | Status | Review | Tests | Merged |
|---|------|----|--------|--------|-------|--------|
| 022 | Confidence engine | — | ✅ done | — | ✅ pass | ✅ v0.5-alpha.22 |
| 023 | Validation/conflict detection | — | ✅ done | — | ✅ pass | ✅ v0.5-alpha.23 |
| 024 | Decay + archival | — | ✅ done | — | ✅ pass | ✅ v0.5-alpha.24 |
| 025 | Memory auditor | — | ✅ done | — | ✅ pass | ✅ v0.5-alpha.25 |
| 026 | Auto-indexing (remember+learn -> Qdrant+graph) | — | ✅ done | — | ✅ pass | ✅ v0.5-alpha.26 |
| 027 | Final integration + composite MCP | — | ✅ done | — | ✅ pass | ✅ v0.5.0 |

## Phase: v0.6 (done)
Stabilization: contract freeze → canonical data model → storage layer → outbox ingestion pipeline → lifecycle engine → FTS5 retrieval → audit system → observability → CI/CD

| # | Phase | Status | Tags |
|---|-------|--------|------|
| 000 | Contract audit (drift-matrix.md) | ✅ done | v0.6.0-phase0 |
| 001 | MCP contract freeze (JSON schemas) | ✅ done | v0.6.0-phase1 |
| 002 | Canonical data model (SQLAlchemy) | ✅ done | v0.6.0-phase2 |
| 003 | Storage layer (Alembic migrations) | ✅ done | v0.6.0-phase3 |
| 004 | Outbox ingestion pipeline | ✅ done | v0.6.0-phase4 |
| 005 | Lifecycle engine | ✅ done | v0.6.0-phase5 |
| 006 | Retrieval system (FTS5) | ✅ done | v0.6.0-phase6 |
| 007 | Audit system | ✅ done | v0.6.0-phase7 |
| 008 | Observability (OpenTelemetry + Prometheus) | ✅ done | v0.6.0-phase8 |
| 009 | CI/CD pipeline (lint, tests, container, SBOM) | ✅ done | v0.6.0-phase9 |

Released: **v0.6.0** (latest: v0.6.0-1-gadedff5 — README update)

## Phase: v0.7 (done)
Belief Store + Reflection

| # | Card | Status | Notes |
|---|------|--------|-------|
| 001 | Belief Model | ✅ **v0.7.0-alpha.28** | Merged. 4 arch reviews + 4 code reviews — все Approve с 0 замечаний. 116 тестов ✅ |
| 002 | reflect() tool | ✅ **v0.7.0-alpha.29** | Merged. 2 code reviews — Approve с 0 замечаний. 173 теста ✅ |
| 003 | Learn-to-belief bridge | ✅ **v0.7.0-alpha.30** | Merged. 2 code reviews — Approve с 0 замечаний. 199 тестов ✅ |
| 004 | Belief conflict resolution | ✅ **v0.7.0-alpha.31** | Merged. 1 code review — Approve с 0 замечаний. 228 тестов ✅ |
| 005 | Integration tests + docs | ✅ **v0.7.0** | Merged. 240 тестов, 5 ADRs (007-012), полная документация |

## Phase: v0.8 (done)
Hermes Native MemoryProvider Integration

| # | Card | Status | Notes |
|---|------|--------|-------|
|| 001 | Native MemoryProvider Plugin | ✅ **v0.8.0-alpha.1** | MemoryProvider ABC, lifecycle hooks, writer queue |
|| 002 | CLI + auto-discovery + docs | ✅ **v0.8.0-alpha.2** | install-hermes-plugin, config.yaml management, documentation |

### Справочные материалы (curiosity worker)

| # | Исследование | Суть |
|---|-------------|------|
| CUR-CMMS-PLUGIN-001 | MemoryProvider Plugin API | Hindsight — эталон. Обязательны: name, is_available, initialize, get_tool_schemas, handle_tool_call. Lifecycle: prefetch, sync_turn, on_session_end, on_session_switch, shutdown. |
| CUR-CMMS-HINDSIGHT-001 | Hindsight как MemoryProvider | Hindsight — native provider, не MCP. Жизненно важно: MCP-only теряет lifecycle hooks, auto-recall, auto-sync, session rotation. |

## Phase: v0.9 (done)
Belief System v2 — Ternary Relation Classification

| # | Card | Status | Notes |
|---|------|--------|-------|
| 001 | Ternary Relation Classifier | ✅ **v0.9.0-alpha.1** | contradiction|entailment|neutral, same_context gate, ADR-014 |
| 002 | same_context gate | ✅ (включено в Card 001) | Реализован в RelationClassifier._has_same_subject() |
| 003 | Migrate reflect() + resolve_conflict() | ✅ (включено в Card 001) | reflect.py обновлён, resolve_conflict работает с ternary |
| 004 | Fix tests + docs | ✅ (включено в Card 001) | 136 тестов, ADR-014, spec |

### Справочные материалы (curiosity worker)

| # | Исследование | Суть |
|---|-------------|------|
| CUR-CMMS-LLM-CONFLICT-001 | LLM contradiction detection | Нужен ternary relation classifier, не binary conflict detector. Рекомендован `contradiction|entailment|neutral` с same_context gate |
| CUR-CMMS-RELATION-001 | Belief relation taxonomy | Миграция reflect()/resolve_conflict() обязательна. Текущий binary conflict — architectural mismatch. Тесты закрепляют ложный positive |

## Phase: v0.10 (done)
LanceDB Vector Store

| # | Card | Status | Notes |
|---|------|--------|-------|
| 001 | LanceDB Vector Store | ✅ **v0.10.0-alpha.1** | LanceDBProvider, env MEMORY_VECTOR_BACKEND, ADR-015, 18+ тестов |

## Phase: v0.11 (current)
LongMemEval-S Benchmark Harness

| # | Card | ID | Status | Review | Tests | Merged |
|---|------|----|--------|--------|-------|--------|
| 001 | LongMemEval Benchmark Harness | t_ee797b52 | ✅ done | ✅ approve | ✅ pass | ✅ v0.11.0b1 |
| 002 | Memory Admission Gate + Tagging | t_855a2392 | ✅ done | ✅ approve | ✅ pass | ✅ v0.11.0b1 |

Scope: lineage-aware LongMemEval-S harness with Raw/Source/Canonical retrieval scoring and a deterministic Hermes built-in lexical baseline. Memory Admission Gate with rule-based write-time tagging, TTL-aware lifecycle, and structured admission metadata.

---

## ⚡ Priority Research Direction: Multi-Agent Memory Sharing & Access Control

**Status:** Research/planning only — NOT implemented. This section captures architectural reasoning for post-v0.11 investigation.

### Problem

CMMS is currently a single-agent memory server. As Hermes evolves into multi-agent/multi-tenant setups (subagents, cron workers, delegated tasks, shared project memory), the memory server must support **controlled memory sharing across agents** without leaking private context, corrupting shared state, or allowing unauthorized reads.

### Design Principles (draft, for validation)

1. **Default-private** — every memory write is private to the originating agent by default. Sharing is opt-in.
2. **Visibility levels** — a hierarchy of scope: `private → agent-shared → project-shared → team-shared → public`.
3. **Permission primitives** — `read`, `write`, `derive` (create derived memory from shared), `share` (grant access), `revoke` (withdraw granted access).
4. **Auth-before-retrieval** — permission check must precede every retrieval access. Shared read should fail closed if no explicit grant.
5. **Provenance + Confidence + Lifecycle** — every shared memory item carries provenance (who wrote it), confidence (how reliable), lifecycle (TTL, staleness).
6. **Guard against shared hallucinations** — shared memory derived by agent A should not be treated as truth by agent B without confidence/evidence anchoring.

### Key Research Questions

- What existing approaches exist for access control / tenant isolation inside a single memory server (not database-level, but semantic/application-level)?
- Current standards/patterns for multi-agent memory sharing?
- How to enforce ACL at the retrieval level, not just the storage level?
- Derived-memory leakage: when agent B reads a belief derived from agent A's private raw data, what leaks?
- Revocation semantics: what happens to derived memories after the source is revoked?
- Audit trail for shared memory access — who read what, when, with what derived memory?

### Canonical Findings Location

All CMMS research findings live under:

```
~/.hermes/workspace/cmms/research/<branch>/
```

Branches: `memory-architecture/`, `evaluation/`, `storage/`, `provider-integration/`, `access-control/`.

See `~/.hermes/workspace/cmms/README.md` for full catalog.
