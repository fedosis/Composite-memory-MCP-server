# Changelog

All notable changes to Composite Memory MCP Server (CMMS) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [PEP 440](https://peps.python.org/pep-0440/)
versioning with SemVer-like semantics.

## 0.12.0 — 2026-09-14

Profile isolation series (cards S1..S5 plus the S5 fix loop): storage is
resolved once per profile and rooted under that profile's own data root, the
migration to that layout is crash-safe and reversible, a second writer is
refused, and the doctor reports the raw/effective model. Reviewed end to end by
an independent cross-provider reviewer (`VERDICT: APPROVE`, 57 commits,
`da1cd0e..5ec25ad`).

### Added

- **Profile-owned storage layout** — `StorageLayout` is resolved once and
  frozen; every local store constructor receives its values from that frozen
  object, and profile mode roots all stores under the profile's own data root.
- **Profile migration with resume and rollback** — plan / apply / resume /
  rollback with a durable backup taken before the target is touched,
  checkpointed publication, and an exact raw-symlink referent restored on
  rollback. `E_RESUME_CONFIG_CHANGED` guards resuming against a changed config
  digest; `E_SQLITE_INTEGRITY` rejects a corrupt target before any mutation.
- **Storage locks** — a root lock plus old-writer detection that checks both
  SQLite activity and graph activity, refusing with `E_WRITER_ACTIVE` /
  `E_OLD_WRITER_ACTIVE` instead of signalling success.
- **Doctor raw/effective model** — per-profile status, severity, canonical
  origins and exit codes, with root and profile homes inspected in one run.

### Fixed

- **Canonical Qdrant endpoint was discarded at the wiring boundary** — the
  server passed the frozen-layout `vector.qdrant_location` (which is `None`
  whenever the vector backend is the default `lancedb`) instead of the
  resolved `settings.qdrant_location`, so `MEMORY_SERVER_QDRANT_LOCATION` never
  reached the provider. The canonical environment now wins over YAML, an
  existing legacy alias, `.env`/`Settings` and the field default, as DETAIL
  §3.2 specifies.
- **Settings and layout were not hermetic inside the plugin path** — cached
  server state (`_storage_settings`, `_storage_layout`, root lock, cleanup flag)
  is now reset per wiring test, so env overrides are honoured deterministically.
- **Rust logger panic across in-process LanceDB tests** — the collection-time
  import probe in `tests/test_ping.py` popped already-loaded native modules out
  of `sys.modules` and never restored them; restoring them in a `finally`
  removes a 53-node shared-process failure cohort.
- **Outbox harness wrote into sandbox-denied scratch paths** — the harness now
  routes its databases and the migration INI into `GATE_TMP`.

### Changed

- **Doctor coverage** — six legacy doctor assertions were replaced by the
  rewritten raw/effective model and remain deliberately unasserted; the
  id-by-id disposition (successor citation or accepted LOST) is recorded in
  `S5-02C_DISPOSITION.md`. A vanished assertion is treated as lost coverage,
  never as an improvement.
- **Packaging** — wheel and sdist carry the new modules; the release-candidate
  check no longer masks a missing `CHANGELOG.md` in the sdist.

### Notes

- Upgrading from an earlier layout **runs a migration**: take a backup, inspect
  with the dry-run, then apply while no other writer is active.
- 28 tests in the certified offline environment fail identically at the
  pre-series baseline and at this release (no network for the embedder, no
  `memory-server` console script on `PATH`, read-only fixture tree); they are
  environment-conditioned, not regressions — see `S5-02E_ACCEPTANCE.md`.

## 0.12.0b1 — 2026-09-02

Major CMMS series hardening: 30 cards across the fact-extraction,
dedup, migration, and review-gate pipeline (cmms series A1..B3d).

### Added

- **LLM extraction contracts + service boundary** (Cards A1/A2)
  - `llm_response.py` — strict validated LLM response contract
    (exact-one-fence JSON grammar, non-string rejection before
    stringification, total confidence handling; never raises on
    malformed numeric/structural input).
  - `noise_filter.py` — facts noise filter (stopwords, fragment
    enders, demonstrative prefixes, em-dash predicate reject).
  - `learn()` gains `llm_extractor` / `llm_timeout_seconds` /
    `llm_max_input_chars` / `llm_confidence_gate` kwargs with
    timeout + fallback boundary; LLM-mode confidence gate >= 0.7.
- **Fact dedup + copy migration** (Cards B1/B2)
  - `dedup_key` on facts with partial unique active index;
    ORM/migration parity; copy-only migration with child-row
    cleanup and ZERO deleted-id references post-upgrade.
- **Real-boundary regression gates** (cmms-series-fixes)
  - Interleaving-agnostic race invariants (no exact-outcome
    counts); pre-confirmation receipt-history proofs;
    fail-closed F6 guard matrix oracles on actual guard text
    (normal and `python -O`).
- **Outbox worker on plugin path** — `busy_timeout_ms=60000`
  passed explicitly to OutboxWorker in the Hermes plugin path
  (was silently downgraded to 5000 on the shared engine).

### Fixed

- OverflowError on integer confidence > 1.7e308 and uncaught
  ValueError/RecursionError from `json.loads` — validator is now
  total over malformed input (returns None per contract).
- Race-test flakiness (~20-40% full-file failure on green code):
  exact loser/winner outcome counts replaced with invariants that
  hold for every legitimate interleaving.

## 0.11.0b1 — 2026-07-22

First CMMS beta release. Shifts from the `alpha` pre-release track to `beta`,
marking API stability sufficient for early integration testing.

This beta is published as a GitHub prerelease tag. It is not published to PyPI,
the official MCP Registry, Smithery, or Glama yet; install from source or
explicit GitHub release artifacts until package publication is verified.

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
- **Public directory/package publication is pending** — `server.json` and
  directory text are draft metadata only until explicit publication to PyPI,
  the official MCP Registry, Smithery, or Glama is verified.
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

### Storage isolation warning

Profile storage now resolves to profile-owned roots. An existing escaping
vector symlink is deliberately not followed; vector recall is unavailable
until explicit migration. Non-empty SQLite WAL/SHM blocks migration apply;
only a clean external runtime shutdown may checkpoint it.

### S4-05 operator documentation reconciliation

The migration documentation now matches the committed CLI and implementation:
SQLite remains the source of truth; `rebuild-from-profile-sql` rebuilds rather
than partitions; legacy projections are preserve-only; and an escaping vector
symlink deliberately leaves recall unavailable until migration. Apply is blocked
by WAL/SHM/journal until normal external shutdown; operators must not manually
checkpoint, delete, or copy sidecars. Mixed-version runtimes must all be
stopped and independently verified before attestation. Dry-run unknown
lock/schema/cost values are not approval, and embedding backend availability,
duration, network, quota, and API-cost authorization are explicit. Backup,
quarantine, resume, and rollback preserve source and avoid writing through
symlinks. Configuration activation remains a separate operator step after data
verification. See [USAGE.md](docs/USAGE.md) for exact flags, JSON shape,
manifest states, and exit codes `0`–`6`.


