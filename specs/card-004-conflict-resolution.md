# Card 004: Conflict Resolution Enhancement

## Objective

Enhance the conflict detection and resolution system for beliefs. Currently, Card 001 provides manual `resolve_conflict` and Card 002 provides keyword-based `reflect(contradictions)`. Card 004 adds **confidence-weighted auto-detection**, **source-overlap detection**, and **auto-resolution** for low-risk conflicts.

## Motivation

Three gaps exist after Cards 001–003:

1. **No auto-detection**: `reflect(contradictions)` must be called manually. Conflicts can accumulate unnoticed.
2. **Limited heuristic**: Keyword-based detection misses structural contradictions (e.g., "Docker > Podman" vs "Podman > Docker" both use "better").
3. **No auto-resolution**: Even trivial conflicts (confidence 0.95 vs 0.3) require manual `resolve_conflict`.

## Design

### 1. Enhanced Contradiction Detection

Extend the contradiction detection from Card 002 with two additional heuristics:

**A. Confidence-weighted detection:** Two beliefs on the same topic with confidence difference > 0.4 are likely contradictory. The detection score combines keyword overlap with confidence disparity:

```
overlap_score = |tokens_a & tokens_b| / |tokens_a | tokens_b|
confidence_diff_weight = min(|c1 - c2| × 2, 1.0)
detection_score = overlap_score × confidence_diff_weight
```

**B. Source-overlap detection:** Two beliefs sharing ≥ 2 evidence source_ids but with opposing propositions (detected via `OPPOSITE_SENTIMENT` from Card 002). This catches cases where the same facts support contradictory conclusions.

**Detection threshold:** A pair is reported as a contradiction when `detection_score >= 0.3` AND either:
- Keyword overlap ≥ 2 tokens with opposite sentiment (Card 002 heuristic), OR
- Confidence difference > 0.4, OR
- Source overlap ≥ 2 shared evidence IDs

**Output format** (extended from Card 002):
```json
{
  "belief_a_id": "uuid",
  "proposition_a": "...",
  "proposition_b": "...",
  "overlap_score": 0.75,
  "detection_score": 0.68,
  "detection_method": "confidence_weighted",  // "keyword" | "confidence_weighted" | "source_overlap"
  "detected_at": "2026-07-12T16:00:00Z"
}
```

### 2. Auto-Resolution for Low-Risk Conflicts

Add an optional `auto_resolve: bool = False` parameter to `resolve_conflict`. When `auto_resolve=True`:

| Condition | Action | Rationale |
|-----------|--------|-----------|
| Confidence diff > 0.5 | Keep higher-confidence belief. Lower-confidence → `superseded` (not `discarded` — reversible via decay) | High confidence gap = clear winner |
| Both < 0.3 | Mark both as `contradicted` for manual review | Too uncertain to auto-resolve |
| Otherwise | Mark both as `contradicted` for manual review | Need human/agent judgment |

**Why `superseded` not `discarded`:** `discarded` is terminal — no recovery. `superseded` allows the belief to decay naturally (`superseded → stale → archived → forgotten`) and can be manually reinstated. If the higher-confidence belief turns out to be wrong, the `superseded` one is still recoverable.

No `auto_resolve` by default (`False`). Manual `resolve_conflict` always takes priority.

### 3. Conflict Report in Overview

Add `conflicts` section to `reflect(overview)` output:
```json
"conflicts": {
  "total": 3,
  "unresolved": 2,
  "auto_resolvable": 1,
  "age_hours_max": 48
}
```

## MCP API Changes

### Modified `reflect(mode="contradictions")`

Enhanced detection with three heuristics (keyword, confidence-weighted, source-overlap). Same input/output format as Card 002, with additional field per contradiction:

```json
{
  "detection_method": "keyword|confidence_weighted|source_overlap",
  "detection_score": 0.68
}
```

### Modified `resolve_conflict`

New optional parameter:
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `auto_resolve` | bool | no | `false` | Auto-resolve by confidence threshold |

When `auto_resolve=True`, returns:
```json
{
  "result": "auto_superseded_b",
  "belief_a": { "...", "lifecycle_state": "active" },
  "belief_b": { "...", "lifecycle_state": "superseded" },
  "reason": "Belief B (confidence 0.3) auto-superseded: Belief A (confidence 0.9) has significantly higher confidence",
  "auto_resolved": true
}
```

Note: auto-resolution uses `superseded` lifecycle state (NOT `discarded`) to allow recovery via natural decay.

### Modified `reflect(overview)`

Additional `conflicts` section in overview output (see section 3).

## Implementation

### Files

| File | Action | Description |
|------|--------|-------------|
| `src/memory_server/api/reflect.py` | Modify | Enhanced contradictions: confidence-weighted + source-overlap + detection_score |
| `src/memory_server/server.py` | Modify | `resolve_conflict` `auto_resolve` param, `reflect` overview conflicts |
| `contracts/resolve_conflict.schema.json` | Modify | Add `auto_resolve` to schema |
| `contracts/reflect.schema.json` | Modify | Add `detection_method` and `detection_score` |
| `tests/test_belief_conflict.py` | Create | Tests for enhanced conflict detection + auto-resolution |

### Implementation Order

1. Enhanced contradiction detection (confidence-weighted + source-overlap)
2. Auto-resolution in resolve_conflict (`superseded`, not `discarded`)
3. Conflict report in overview
4. JSON Schema updates
5. Tests

## Acceptance Criteria

1. `reflect(mode="contradictions")` detects confidence-weighted contradictions (confidence diff > 0.4)
2. `reflect(mode="contradictions")` detects source-overlap contradictions (shared evidence → opposing conclusions)
3. `reflect(mode="contradictions")` only reports pairs with `detection_score >= 0.3`
4. `resolve_conflict(auto_resolve=True)` sets lower-confidence belief to `superseded` when diff > 0.5
5. `resolve_conflict(auto_resolve=True)` marks both as `contradicted` when confidences are close or both < 0.3
6. `reflect(overview)` includes conflict section with counts
7. All existing reflect/resolve_conflict tests continue to pass
8. Auto-resolve never runs by default (`auto_resolve=False`)
9. Superseded auto-resolution is recoverable (belief can be restored)
10. Auto-resolve never uses `discarded` state

## Non-goals (v0.7)

- LLM-based semantic contradiction detection (v0.8+)
- Proactive conflict notification (requires event system)
- Conflict clustering (group >2 related conflicts)
- Auto-merge conflicting beliefs (manual only)
