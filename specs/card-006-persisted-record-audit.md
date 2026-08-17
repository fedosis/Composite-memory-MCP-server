# Card 006: Persisted record-level audit for CMMS

## Context
The previous fix corrected persisted totals and backend wiring for `audit`, but alter-ego review found that `MemoryAuditor` still performs record-level checks against an empty in-memory `Validator`. As a result, `audit_type="full"` can report correct top-level totals while silently missing persisted issues such as low-confidence facts, lifecycle anomalies, and receipt mismatches.

## Goal
Make `audit` perform record-level analysis against persisted CMMS data, not only persisted counts, so `full` audit can surface real low-confidence, lifecycle, and receipt-related problems from SQLite-backed records.

## Acceptance Criteria
- [ ] A fact written with `confidence < 0.3` is reported by `server.audit_tool(audit_type="full")` and `HermesProvider.handle_tool_call("audit", {"audit_type":"full"})`.
- [ ] `stats.confidence.total` reflects persisted records under audit, not just validator entries.
- [ ] Receipt-related checks (`check_orphan_records`, `check_missing_receipts`) operate on persisted records and receipts, not an empty validator fallback.
- [ ] Lifecycle checks operate on persisted lifecycle state when SQLite is configured.
- [ ] Existing persisted total counters remain correct.
- [ ] Existing audit warnings about missing SQLite/receipt stores stay gone when the stores are present.
- [ ] Add regression tests for low-confidence persisted facts and persisted receipt/lifecycle checks.

## Non-Goals
- [ ] Redesign the whole validator/confidence subsystem.
- [ ] Remove eventual-consistency drift warnings in this card unless a minimal wording fix falls out naturally.
- [ ] Rename `qdrant_*` telemetry fields in this card; treat naming cleanup as a follow-up unless trivial.

## Root Cause
1. `src/memory_server/server.py::_build_auditor()` and `src/memory_server/plugins/hermes/provider.py::_build_auditor()` pass real SQLite/vector/graph state but still provide an empty `Validator`.
2. `src/memory_server/evaluation/auditor.py` uses `self._validator.get_all()` for:
   - `audit_consistency()`
   - `audit_confidence()`
   - `check_orphan_records()`
   - `check_missing_receipts()`
   - `check_lifecycle_violations()`
3. Persisted SQL records therefore influence only counts/drift, not record-level findings.

## Approach
1. Add a persisted audit snapshot loader that extracts the minimal record-level fields the auditor needs from SQLite.
2. Feed `MemoryAuditor` both persisted counts and persisted record snapshots.
3. Make record-level checks prefer persisted snapshot data whenever SQLite-backed audit state is available.
4. Keep validator-based behavior only as a fallback when persisted audit state is unavailable.
5. Keep the regression focus fact-first; decisions/skills may share the same plumbing, but this card is accepted only if fact-backed persisted lifecycle/receipt/confidence audit works end-to-end.

## Implementation Targets
- `src/memory_server/evaluation/auditor.py`
- `src/memory_server/providers/sqlite_provider.py`
- `src/memory_server/server.py`
- `src/memory_server/plugins/hermes/provider.py`
- `tests/test_audit_persistence_alignment.py`
- optionally a new focused test module if coverage is cleaner there

## Required Data Shape
Add a persisted audit snapshot structure with the minimal fields needed per record:
- `fact_id`
- `memory_type` (`fact`, `decision`, `skill`)
- `status` (normalized lifecycle/status field consumed by the auditor)
- `confidence`
- `lifecycle_state`
- `verification_status`
- `receipt_id` or `has_receipt`

Normalization rule: persisted snapshot entries should be converted at the auditor boundary into a validator-like shape (`fact_id`, `memory_type`, `status`, `confidence`, `verification_status`, `has_receipt`/`receipt_id`, optional raw `lifecycle_state`) so all record-level audit methods consume one common input contract.

Persisted audit loading for this card must honor `include_inactive=True` so archived/stale/forgotten rows remain visible to lifecycle auditing.

Receipt semantics for this card are intentionally scoped to **record -> receipt linkage** only:
- in scope: persisted record exists but expected receipt linkage is missing
- out of scope: receipt exists without a corresponding record (treat as follow-up work unless trivial fallout)

The snapshot should be cheap to load and not require hydrating every ORM relation.

## Suggested File-Level Changes
### 1. `src/memory_server/providers/sqlite_provider.py`
Add one focused method for audit hydration, e.g.:
- `list_audit_records(include_inactive: bool = True) -> list[dict[str, Any]]`

This method should gather facts/decisions/skills plus enough receipt linkage to support:
- confidence distribution
- missing receipt detection
- lifecycle validation

### 2. `src/memory_server/evaluation/auditor.py`
- Extend `collect_persisted_audit_state()` to include `records` (or similarly named field).
- Extend `MemoryAuditor.__init__()` with persisted record snapshot input.
- Add an internal helper like `_audit_entries()` that returns:
  1. persisted snapshot records when available
  2. validator entries otherwise
- Update the record-level audit methods to use `_audit_entries()` instead of raw `self._validator.get_all()`.
- Preserve current fallback behavior when no persisted snapshot exists.

### 3. `src/memory_server/server.py`
Pass persisted record snapshot into `MemoryAuditor` from `_build_auditor()`.

### 4. `src/memory_server/plugins/hermes/provider.py`
Pass the same persisted record snapshot into `MemoryAuditor` from plugin `_build_auditor()`.

## Test Plan
### Red/Green cases
1. Persisted low-confidence fact
   - write a fact with `confidence=0.1`
   - assert `stats.confidence.total >= 1`
   - assert low-confidence finding is surfaced

2. Persisted receipt linkage
   - write a fact through the real ingestion path
   - assert it is not incorrectly reported as missing a receipt

3. Persisted lifecycle visibility
   - seed or update a record with a lifecycle state that current audit logic considers problematic
   - assert the audit reports it from persisted state
   - prefer fact-scoped mutation through existing update paths rather than introducing new decision/skill lifecycle APIs in this card

4. Fallback safety
   - instantiate `MemoryAuditor` without SQLite snapshot data
   - assert validator-based behavior still works

## Verification Commands
Run in `/home/shtorm/memory-server`:

```bash
pytest -q tests/test_audit_persistence_alignment.py tests/test_hermes_provider.py tests/test_auditor.py
```

If a new focused test file is added, include it in the command and keep the existing targeted suite green.

## Done Definition
This card is done only when:
1. spec review passes,
2. coder implementation lands,
3. targeted tests pass,
4. independent alter-ego code review reports no critical issues for the card scope.
