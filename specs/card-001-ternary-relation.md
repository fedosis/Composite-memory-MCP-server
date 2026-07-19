# Card 001: Ternary Relation Classifier

**Phase:** v0.9
**Status:** Design
**Depends on:** v0.8 (MemoryProvider), ADR-011, ADR-012, ADR-014

## Objective

Replace binary keyword-based contradiction detection with a ternary
classifier: `contradiction | entailment | neutral`.

## Curiousity Worker Findings

- **CUR-CMMS-LLM-CONFLICT-001**: Current binary detection has false
  positive — "Docker is better than Podman" vs "Podman is worse than
  Docker" flagged as contradiction but is entailment.
- **CUR-CMMS-RELATION-001**: Ternary relation model needed with explicit
  `same_context` gate.

## Specification

### 1. RelationClassifier

**File:** `src/memory_server/evaluation/relation.py`

```python
class RelationClassifier:
    def classify_pair(
        belief_a: Belief,
        belief_b: Belief,
        context_a: str | None = None,
        context_b: str | None = None,
        strict_same_context: bool = True,
    ) -> RelationResult:
        ...
```

**Input:**
- `belief_a` / `belief_b` — two Belief instances to compare
- `context_a` / `context_b` — optional context identifiers (tags, source domains, conversation IDs)
- `strict_same_context` — if True (default), different contexts → neutral

**Output (`RelationResult`):**

| Field | Type | Description |
|-------|------|-------------|
| `relation` | str | `contradiction` | `entailment` | `neutral` |
| `confidence` | float 0-1 | Confidence in the classification |
| `same_context` | bool | Whether beliefs share the same context |
| `overlap_score` | float | Keyword Jaccard overlap score |
| `detection_method` | str | `keyword` | `entailment_keyword` | `neutral` |
| `detected_at` | str | ISO timestamp |

### 2. same_context Gate

- If `context_a` and `context_b` are provided and differ:
  - `same_context = false`
  - `strict_same_context=True` → relation = `neutral`, confidence capped at 0.3
  - `strict_same_context=False` → relation computed normally, confidence reduced 50%
- If contexts match or are not provided: `same_context = true`, classification normal
- Context is compared by string equality, then tag intersection fallback

### 3. Classification Algorithm (v0.9 Heuristic)

1. **Tokenize:** lowercase, strip punctuation, filter STOPWORDS, keep tokens >2 chars
2. **Overlap score:** `len(intersection) / max(len(union), 1)` (Jaccard)
3. **Sentiment analysis:**
   - `_has_opposite_sentiment(a, b)` — OPPOSITE_SENTIMENT pairs present
   - `_has_same_sentiment(a, b)` — both use positive words OR both use negative words
4. **Classification:**
   - Overlap ≥ 2 + opposite sentiment → `contradiction`, confidence from overlap × sentiment_weight
   - Overlap ≥ 1 + same sentiment on same topic → `entailment`, confidence from overlap + sentiment match
   - Overlap ≥ 1 + neutral → borderline entailment, confidence < 0.4
   - Otherwise → `neutral`, confidence = 0.0

### 4. Integration

#### ReflectEngine changes

```python
# New: ternary relations mode
async def relations(self, topic=None, min_confidence=0.0, limit=0,
                     context=None, strict_same_context=True) -> dict:
    """Classify all belief pairs as contradiction|entailment|neutral."""

# Updated: contradictions() delegates to RelationClassifier with filter
async def contradictions(self, topic=None, min_confidence=0.0, limit=0) -> dict:
    """Backward-compatible wrapper returning only contradiction results."""

# New helper
def find_relations(beliefs, contexts=None, strict_same_context=True) -> list[dict]:
    """Find all ternary relations between belief pairs."""
```

#### Schema additions

- reflect input: `mode` enum extended with `"relations"`
- reflect input: new params `context` and `strict_same_context`
- reflect output for mode=relations: array of RelationResult objects

### 5. Test Updates

#### Fix false positive
- "Docker is better than Podman" vs "Podman is worse than Docker"
  → should be `entailment`, NOT `contradiction`

#### New test cases
- Contradiction: "Docker is better than Podman" vs "Docker is worse than Podman"
- Entailment: "Docker is better than Podman" vs "Podman is worse than Docker"
- Entailment: "Python is great for AI" vs "Python is excellent for ML"
- Neutral: "Docker is good" vs "Caddy is a web server"
- same_context: different contexts → neutral when strict=True
- same_context: different contexts → lowered confidence when strict=False
- Empty input → empty results

### 6. Files to Modify/Create

| File | Action |
|------|--------|
| `src/memory_server/evaluation/relation.py` | CREATE — RelationClassifier |
| `src/memory_server/api/reflect.py` | MODIFY — add relations(), update contradictions() |
| `contracts/reflect.schema.json` | MODIFY — add relations mode |
| `tests/test_belief_conflict.py` | MODIFY — fix false positive, add ternary tests |
| `tests/test_relation.py` | CREATE — dedicated ternary tests |
| `docs/ADR.md` | MODIFY — append ADR-014 |
| `specs/card-001-ternary-relation.md` | CREATE — this document |
| `PLAN.md` | MODIFY — update Card 001 status |

## Acceptance Criteria

1. `RelationClassifier.classify_pair()` returns correct relation for all three classes
2. same_context gate works (different contexts → neutral or lowered confidence)
3. Existing `mode=contradictions` still works (backward compatible)
4. "Docker is better than Podman" vs "Podman is worse than Docker" → entailment, not contradiction
5. All tests pass
6. ADR-014 documents the architectural decision
