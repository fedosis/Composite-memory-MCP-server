# Changelog

All notable changes to Composite Memory MCP Server (CMMS) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [PEP 440](https://peps.python.org/pep-0440/)
versioning with SemVer-like semantics.

## 0.11.0b1 — 2026-07-22

First CMMS beta release. Shifts from the `alpha` pre-release track to `beta`,
marking API stability sufficient for early integration testing.

This release candidate is prepared for reproducible validation only: it is not tagged and not published to PyPI or GitHub Releases yet.

### Added

- **LongMemEval-S Benchmark Harness** (Card 001, t_ee797b52)
  - Lineage-aware retrieval evaluation with three scoring targets:
    `raw`, `source`, `canonical`.
  - `memory-server benchmark-longmemeval` CLI command.
  - `BuiltInMemoryBaseline` — deterministic Hermes built-in lexical overlap
    scorer (model-free, no API keys).
  - `rescore_trace()` for re-scoring saved retrieval traces against any target.
  - Shared-subset pairwise comparison (`raw_vs_source`, `raw_vs_canonical`).
  - Full documentation in `docs/longmemeval-harness.md`.

- **Memory Admission Gate + Tagging** (Card 002, t_855a2392)
  - `MemoryAdmissionGate` — rule-based write-time admission filter that
    classifies memory text as `EPHEMERAL`, `DURABLE`, or `IMPORTANT`.
  - TTL-aware lifecycle: ephemeral (1 day), durable (365 days), important
    (no expiry).
  - Structured admission metadata: `memory_kind`, `epistemic_status`,
    `authority_level`, `risk_tags`, `admission_tags`.
  - `force=True` override admits low-signal text while preserving its
    tag and TTL.
  - `prune_expired_memories()` — batch archive for expired receipts and
    facts.
  - `import_memory_md()` — MEMORY.md bulk import with automatic
    ephemeral/durable filtering.
  - CLI integration — `remember()` accepts `admission=` parameter.
  - 8+ admission gate tests, 3+ integration tests covering TTL prune,
    bulk import, and admission metadata persistence.

- **Hermes v0.19 Compatibility Fix**
  - Restored CMMS provider discovery under Hermes v0.19 plugin shim.

### Changed

- Package metadata version from `0.1.0` → `0.11.0b1`.
- README: restructured with clean quickstart section at top for new users.

### Known limitations

- **LongMemEval-S requires an external dataset** — the harness does not bundle
  the LongMemEval JSON. Users must download `longmemeval_s_cleaned.json`
  separately.
- **No retrieval plug-in API** — the built-in baseline is the only retriever
  in this release. Custom retrievers require subclassing `BuiltInMemoryBaseline`.
- **No full suite green**: Unit tests pass (~240+ tests), but integration,
  e2e, and benchmark tests require Qdrant or external services; they are not
  part of the CI unit-test gate.
- **Memory Admission Gate is rule-first** — it uses deterministic heuristics,
  not an ML model. Edge cases (mixed-language input, novel preference forms)
  may be misclassified. The `force` flag provides an escape hatch.
- **Graph cleanup on fact deletion** — graph nodes and edges persist when a
  fact is deleted from SQLite. This is a known limitation (documented in
  `docs/metrics.md` §3.4).

### Test Status

- **Unit tests**: ✅ ~240+ passing (`pytest tests/ -q -k "not integration and not e2e and not benchmark and not loadtest and not migration"`)
- **Integration + e2e + benchmark**: 🔄 requires Qdrant container (CI-only)
- **Contract tests**: ✅ JSON Schema validation + schema/contract tests
- **Migration tests**: ✅ `alembic upgrade head && alembic downgrade -1`
- **Lint**: ✅ `ruff check src/` clean

## [0.10.0-alpha.1] — 2026-07-XX

LanceDB Vector Store as default local-first backend.

- LanceDBProvider with env `MEMORY_VECTOR_BACKEND=lancedb`
- ADR-015: LanceDB local-first architecture decision
- 18+ tests for LanceDB integration

## [0.9.0-alpha.1] — 2026-07-XX

Belief System v2 — Ternary Relation Classification.

- Ternary Relation Classifier (`contradiction|entailment|neutral`)
- `same_context` gate via `RelationClassifier._has_same_subject()`
- Migrated `reflect()` and `resolve_conflict()` to ternary semantics
- ADR-014, 136 tests

## [0.8.0-alpha.1] — 2026-07-XX

Hermes Native MemoryProvider Integration.

- MemoryProvider ABC, lifecycle hooks, writer queue
- `install-hermes-plugin` CLI + auto-discovery + docs

## [0.7.0] — 2026-07-XX

Belief Store + Reflection.

- Belief Model with confidence, evidence provenance, lifecycle states
- `reflect()` tool with 6 analysis modes
- Learn-to-belief bridge
- Conflict resolution (manual + auto)
- 240 tests, 5 ADRs (007-012)

## [0.6.0] — 2026-07-XX

Stabilization: contract freeze, canonical data model, outbox ingestion,
lifecycle engine, FTS5 retrieval, audit system, observability, CI/CD.

## [0.5.0] — 2026-07-XX

Confidence engine + validation + decay + memory auditor + auto-indexing.

## [0.4.0] — 2026-07-XX

Graph DB + entity relations + hybrid router.

## [0.3.0] — 2026-07-XX

LLM extractors + `learn()` MCP tool.

## [0.2.0] — 2026-07-XX

Qdrant + embeddings + semantic router.

## [0.1a] — 2026-07-XX

Initial MCP API + SQLite provider + `get_context`/`search`/`remember` tools.


