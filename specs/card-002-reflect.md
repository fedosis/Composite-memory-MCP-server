# Card 002: reflect() Tool — Belief Analysis & Reflection

## Objective

Add a `reflect()` MCP tool that analyzes the belief store and produces actionable insights. The agent uses it to understand *what it currently believes*, *how confident it is*, *what conflicts exist*, and *what should change*.

## Motivation

Card 001 (Belief Model) created the storage layer for beliefs — propositions with confidence, evidence, and lifecycle state. Card 002 adds the *reflection* layer: instead of manually querying for each belief, the agent can run `reflect()` to get a structured overview of its own knowledge state.

Use cases:
- **Health check:** What do I believe with high confidence? What am I unsure about?
- **Contradiction scan:** Which active beliefs contradict each other? (Beyond exact-match — requires new logic)
- **Decay analysis:** Which beliefs are stale or about to be archived?
- **Topic clustering:** What topics do my beliefs cover? How many per topic?
- **Evidence audit:** Which beliefs have strong/weak/no evidence?
- **Confidence distribution:** Histogram of confidence scores across all active beliefs

## MCP Tool: `reflect`

### Input

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `mode` | string | no | `"overview"` | Analysis mode: `"overview"`, `"contradictions"`, `"decay"`, `"topics"`, `"evidence_audit"`, `"confidence"` |
| `topic` | string | no | — | Filter analysis to a specific topic/tag |
| `min_confidence` | float | no | 0.0 | Minimum confidence threshold for inclusion |
| `limit` | int | no | 50 | Max beliefs to analyze (0 = all) |

### Modes

#### `overview` (default)
High-level summary of the belief store.

**Output:**
```json
{
  "mode": "overview",
  "total_beliefs": 45,
  "by_lifecycle_state": {
    "active": 30,
    "superseded": 5,
    "contradicted": 2,
    "discarded": 3,
    "stale": 3,
    "archived": 2
  },
  "by_topics": {
    "user-preference": 12,
    "infra": 8,
    "development": 6,
    "other": 14
  },
  "confidence": {
    "high_0.8_1.0": 15,
    "medium_0.5_0.8": 18,
    "low_0.0_0.5": 7,
    "average": 0.72
  },
  "contradiction_count": 1,
  "stale_count": 3,
  "decaying_next_7d": 2,
  "no_evidence_count": 4,
  "oldest_belief_days": 180,
  "newest_belief_days": 0
}
```

#### `contradictions`
Find beliefs that semantically conflict. Beyond Card 001's exact-match reinforcement, this mode uses heuristics to detect contradictions:
- Same subject, opposite predicates ("Docker > Podman" vs "Podman > Docker")
- Confidence-based: two active beliefs with overlapping source_ids but opposing propositions
- Lists all unresolved contradictions with both belief IDs and confidence values

**Output:**
```json
{
  "mode": "contradictions",
  "total": 2,
  "contradictions": [
    {
      "belief_a": {"id": "...", "proposition": "Docker is better than Podman", "confidence": 0.8},
      "belief_b": {"id": "...", "proposition": "Podman is better than Docker", "confidence": 0.6},
      "overlap_score": 0.75,
      "detected_at": "2026-07-12T09:00:00Z"
    }
  ],
  "recommendation": "run resolve_conflict(belief_a_id, belief_b_id, 'keep_a')"
}
```

**Contradiction detection heuristic (v0.7):**
1. Fetch all `active` and `contradicted` beliefs
2. Tokenize propositions into keywords (remove stopwords: "is", "the", "a", "an", "better", "worse" etc.)
3. Find pairs sharing ≥ 2 significant keywords but with opposite sentiment indicators ("better"/"worse", "prefer"/"dislike", "recommend"/"avoid")
4. Score by keyword overlap × confidence difference
5. Return pairs above threshold (default: overlap_score ≥ 0.5)

This is intentionally simple for v0.7. Semantic/LLM-based detection is v0.8+.

#### `decay`
Analyze which beliefs are approaching lifecycle transitions.

**Output:**
```json
{
  "mode": "decay",
  "stale_now": 3,
  "stale_7d": 5,
  "archived_7d": 2,
  "forgotten_7d": 1,
  "by_tag_stale": {
    "user-preference": 2,
    "infra": 1
  },
  "recommendation": "Review 3 stale beliefs: run get_belief(lifecycle_state='stale')"
}
```

#### `topics`
Cluster beliefs by tags/topics. Uses existing `tags` field from Belief model.

**Output:**
```json
{
  "mode": "topics",
  "topics": [
    {"tag": "user-preference", "count": 12, "avg_confidence": 0.78, "stale": 1},
    {"tag": "infra", "count": 8, "avg_confidence": 0.85, "stale": 0},
    {"tag": "development", "count": 6, "avg_confidence": 0.65, "stale": 2}
  ],
  "untagged_count": 5
}
```

#### `evidence_audit`
Audit evidence quality across beliefs.

**Output:**
```json
{
  "mode": "evidence_audit",
  "total": 45,
  "with_evidence": 35,
  "without_evidence": 10,
  "avg_evidence_per_belief": 2.3,
  "by_source_type": {
    "fact": 45,
    "observation": 30,
    "user_statement": 12
  },
  "zero_weight_entries": 3,
  "recommendation": "Add evidence to 10 beliefs with no sources"
}
```

#### `confidence`
Detailed confidence histogram.

**Output:**
```json
{
  "mode": "confidence",
  "beliefs": [
    {"proposition": "Docker runs on OMV8", "confidence": 0.95, "evidence_count": 5},
    {"proposition": "My server is Ubuntu 24.04", "confidence": 0.9, "evidence_count": 3}
  ],
  "histogram": {
    "0.9_1.0": 5,
    "0.7_0.9": 12,
    "0.5_0.7": 10,
    "0.3_0.5": 5,
    "0.0_0.3": 3
  },
  "lowest_count": 3,
  "recommendation": "Review 3 beliefs with confidence < 0.3"
}
```

## Implementation

### Dependencies
Card 001 must be complete (Belief model, SQLiteProvider CRUD, lifecycle engine). Already satisfied in v0.7.0-alpha.28.

### Files

| File | Action | Description |
|------|--------|-------------|
| `src/memory_server/server.py` | Modify | Register `reflect` MCP tool |
| `src/memory_server/api/reflect.py` | Create | Core reflection logic |
| `contracts/reflect.schema.json` | Create | Input/output JSON Schema |
| `tests/test_belief_reflect.py` | Create | Tests for reflect tool |

### Core Logic (`api/reflect.py`)

```python
import logging
from datetime import datetime, timezone
from typing import Any

class ReflectEngine:
    def __init__(self, provider: SQLiteProvider):
        self._provider = provider

    async def _fetch_beliefs(self, topic=None, min_confidence=0.0, limit=0) -> list[Belief]:
        """Fetch beliefs with filters. limit=0 means no limit."""
        return await self._provider.search_beliefs(
            tags=[topic] if topic else None,
            min_confidence=min_confidence if min_confidence > 0 else None,
            lifecycle_state=None,  # all states
            limit=limit if limit > 0 else 10000,  # effectively unlimited
        )

    async def overview(self, topic=None, min_confidence=0.0, limit=0) -> dict
    async def contradictions(self, topic=None, min_confidence=0.0, limit=0) -> dict
    async def decay_analysis(self, topic=None, min_confidence=0.0, limit=0) -> dict
    async def topics(self, topic=None, min_confidence=0.0, limit=0) -> dict
    async def evidence_audit(self, topic=None, min_confidence=0.0, limit=0) -> dict
    async def confidence_histogram(self, topic=None, min_confidence=0.0, limit=0) -> dict
```

Note: `limit=0` means "fetch all". The provider's `search_beliefs(limit=0)` should return all matching beliefs without a cap.

### Contradiction Detection (v0.7 heuristic)

```python
# Stopwords for keyword overlap computation.
# Note: "better"/"worse" ARE stopwords — they're uninformative for keyword
# overlap but remain in the raw proposition text for _has_opposite_sentiment().
STOPWORDS = {"is", "the", "a", "an", "be", "to", "of", "in", "it",
             "and", "or", "for", "on", "with", "as", "at", "by",
             "better", "worse", "more", "less", "very", "most"}

# Sentiment opposition pairs for contradiction detection
# Note: this heuristic does NOT catch structural contradictions where
# both propositions use the same favorable word.
# Example NOT detected: "Docker is better than Podman" vs
# "Podman is better than Docker" — both use "better", no opposite found.
# Full LLM-based detection is v0.8+.
OPPOSITE_SENTIMENT = {
    "better": "worse", "prefer": "avoid", "recommend": "against",
    "like": "dislike", "good": "bad", "fast": "slow", "stable": "unstable"
}

def _tokenize(proposition: str) -> set[str]:
    """Extract significant keywords from a proposition."""
    words = proposition.lower().split()
    return {w.strip(".,!?;:'\"()") for w in words if w not in STOPWORDS and len(w) > 2}

def _has_opposite_sentiment(a: str, b: str) -> bool:
    """Check if two propositions express opposing views on the same topic."""
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())
    for pos, neg in OPPOSITE_SENTIMENT.items():
        if (pos in words_a and neg in words_b) or (neg in words_a and pos in words_b):
            return True
    return False

# Timeout guard for large contradiction scans
# O(n²) pairwise comparison: 447 beliefs ≈ 100K pairs, ~1s at Python speed
MAX_CONTRADICTION_PAIRS = 100_000
_MAX_BELIEFS_FOR_CONTRADICTION = 447  # derived: sqrt(2 * MAX_CONTRADICTION_PAIRS)

def detect_contradictions(beliefs: list) -> list[dict]:
    """Find pairs of beliefs with significant keyword overlap and opposite sentiment.
    
    For >447 beliefs, logs a warning — caller can sample or accept O(n²).
    """
    if len(beliefs) > _MAX_BELIEFS_FOR_CONTRADICTION:
        logger = logging.getLogger(__name__)
        logger.warning(
            "Large contradiction scan: %s beliefs, may be slow", len(beliefs)
        )
    now = datetime.now(timezone.utc).isoformat()
    results = []
    for i in range(len(beliefs)):
        for j in range(i + 1, len(beliefs)):
            a, b = beliefs[i], beliefs[j]
            tokens_a = _tokenize(a.proposition)
            tokens_b = _tokenize(b.proposition)
            overlap = tokens_a & tokens_b
            if len(overlap) >= 2 and _has_opposite_sentiment(a.proposition, b.proposition):
                score = len(overlap) / max(len(tokens_a | tokens_b), 1)
                results.append({
                    "belief_a_id": a.id,
                    "proposition_a": a.proposition,
                    "confidence_a": a.confidence,
                    "belief_b_id": b.id,
                    "proposition_b": b.proposition,
                    "confidence_b": b.confidence,
                    "overlap_score": round(score, 2),
                    "detected_at": now,
                })
    return results
```

### Required Provider Changes

Before implementing reflect(), add to `EvidenceRepository`:

```python
async def aggregate_stats(self, belief_ids: list[str] | None = None) -> dict[str, dict]:
    """Return {belief_id: {count, avg_weight, by_source_type}} for all or specified beliefs.
    Aggregation at SQL level to avoid N+1 queries at scale."""
```

And ensure `search_beliefs(limit=0)` returns all results without capping (currently defaults to 10).

### MCP Tool Registration

```python
@mcp.tool(name="reflect")
async def reflect_tool(
    mode: str = "overview",
    topic: str = "",
    min_confidence: float = 0.0,
    limit: int = 50,
) -> str:
    """Analyze the belief store and produce insights.

    Args:
        mode: Analysis mode — overview, contradictions, decay, topics, evidence_audit, confidence.
        topic: Optional topic/tag filter.
        min_confidence: Minimum confidence threshold.
        limit: Max beliefs to analyze (0 = all).
    """
    ...
```

## Acceptance Criteria

1. `reflect(mode="overview")` returns correct counts by lifecycle_state, tags, confidence buckets
2. `reflect(mode="contradictions")` detects keyword overlap contradictions without false positives
3. `reflect(mode="decay")` correctly identifies stale/archiving/forgotten beliefs using actual TTL
4. `reflect(mode="topics")` clusters beliefs by tags with accurate averages
5. `reflect(mode="evidence_audit")` counts evidence per belief correctly (using Evidence table)
6. `reflect(mode="confidence")` returns sorted belief list + histogram
7. All modes respect `topic`, `min_confidence`, `limit` filters
8. Empty belief store returns graceful empty results (not errors)
9. Invalid mode returns clear error message
10. All 6 modes produce valid JSON matching the schema

## Non-goals (v0.7)

- LLM-based contradiction detection (v0.8+)
- Auto-resolve contradictions (manual via `resolve_conflict` only)
- Belief evolution timeline / trend analysis (v0.8+)
- Belief export/import (v1.0)
- Active learning / belief suggestion (v1.0+)
