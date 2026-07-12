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

## Phase: v0.7 (current)
Belief Store + Reflection

| # | Card | Status | Notes |
|---|------|--------|-------|
| 001 | Belief Model | ✅ **v0.7.0-alpha.28** | Merged. 4 arch reviews + 4 code reviews — все Approve с 0 замечаний. 116 тестов ✅ |
| 002 | reflect() tool | ✅ **v0.7.0-alpha.29** | Merged. 2 code reviews — Approve с 0 замечаний. 173 теста ✅ |
| 003 | Learn-to-belief bridge | ✅ **v0.7.0-alpha.30** | Merged. 2 code reviews — Approve с 0 замечаний. 199 тестов ✅ |
| 004 | Belief conflict resolution | ✅ **v0.7.0-alpha.31** | Merged. 1 code review — Approve с 0 замечаний. 228 тестов ✅ |
| 005 | Integration tests + docs | ✅ **v0.7.0** | Merged. 240 тестов, 5 ADRs (007-012), полная документация |
