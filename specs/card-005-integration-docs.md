# Card 005: Integration Tests & Documentation

## Objective

Complete v0.7 by adding end-to-end integration tests covering the full belief pipeline (Cards 001-004) and updating documentation to reflect the current state.

## Scope

### 1. Integration Tests

Cover the full pipeline: `learn()` → beliefs → `reflect()` → `resolve_conflict()`.

**Test scenarios:**

| # | Scenario | Pipe |
|---|----------|------|
| 1 | Learn text → extract beliefs → reflect overview shows them | learn → belief → reflect |
| 2 | Duplicate learn call → belief reinforced (not duplicated) | learn → learn → belief |
| 3 | Learn contradictory statements → reflect detects contradiction | learn → learn → reflect(contradictions) |
| 4 | Auto-resolve contradiction → belief superseded | reflect → resolve_conflict(auto) |
| 5 | Manual merge → new belief + evidence linked | resolve_conflict(merge) |
| 6 | Evidence audit after learn → correct evidence counts | learn → reflect(evidence_audit) |
| 7 | 500+ active beliefs → soft limit stops extraction | learn × N → soft limit |
| 8 | Empty store → all reflect modes return graceful results | reflect(overview/decay/etc) |
| 9 | get_belief with source_id filter → finds belief by evidence | learn → get_belief |
| 10 | Full lifecycle: create → reinforce → supersede → reflect | Belief lifecycle |

**Integration test file:** `tests/test_belief_integration.py`

### 2. Documentation Update

Update key docs to reflect v0.7 state:

| Doc | Action |
|-----|--------|
| `docs/USAGE.md` | Add belief tools section: set_belief, get_belief, resolve_conflict, reflect, learn(extract_beliefs) |
| `README.md` | Update API Reference with all 9+ MCP tools, add v0.7 features |
| `docs/ADR.md` | Add ADR-011: Belief Model and ADR-012: Reflection |
| `docs/metrics.md` | Update benchmark baselines with belief operations |
| `PLAN.md` | Already up to date — no changes needed |

### 3. Minor Cleanup

- Remove unused imports (ruff-clean after all changes)
- Ensure consistent naming across all belief files (plural `beliefs` in output keys)
- Verify all JSON Schema contracts are self-consistent

## Files

| File | Action | Description |
|------|--------|-------------|
| `tests/test_belief_integration.py` | **Create** | 10 end-to-end test scenarios |
| `docs/USAGE.md` | Modify | Add belief tools section |
| `README.md` | Modify | Update API reference |
| `docs/ADR.md` | Modify | Add ADR-011, ADR-012 |

## Acceptance Criteria

1. All 10 integration scenarios pass with real SQLite in-memory
2. No regressions — all existing 228 belief tests still pass
3. README lists all belief MCP tools with brief descriptions
4. USAGE.md documents learn(extract_beliefs=True) with example
5. ADR-011 documents Belief Model rationale and design decisions
6. ADR-012 documents Reflection architecture

## Non-goals (v0.7)

- Performance benchmarks (deferred to v0.8)
- Production deployment docs (v1.0)
- Client library docs (v1.0)
