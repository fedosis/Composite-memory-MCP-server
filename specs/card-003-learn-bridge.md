# Card 003: Learn-to-Belief Bridge

## Objective

Bridge the existing `learn()` tool with the belief store (Card 001). When the `learn()` tool extracts facts, decisions, and skills from text, it can optionally promote extracted knowledge into beliefs — propositions the agent holds as true with explicit confidence and evidence chains.

## Motivation

Card 001 created the belief store (set_belief, get_belief, resolve_conflict). Card 002 created reflection (reflect). What's missing is **automatic belief creation from extracted knowledge**.

Currently, `learn()` extracts facts (SPO triples), decisions, and skills — but these remain as separate entity types. The agent can't answer "what do you believe about X?" without manually calling `set_belief` after each `learn()`.

The bridge connects two existing pipelines:
```
Text → learn() → FactExtractor → fact (stored as-is)
                                    ↓
Text → learn() → BeliefExtractor → belief (confidence computed, evidence linked)
```

## Design

### Option A: New `BeliefExtractor` + `learn_to_belief` parameter

Add a `BeliefExtractor` to the extractor family that analyzes text and extracts belief propositions (free-form statements, not SPO triples). The existing `learn()` tool gets a new parameter `extract_beliefs: bool = False`.

When `extract_beliefs=True`:
1. Run existing extractors (FactExtractor, DecisionExtractor, SkillExtractor) as before
2. Run new `BeliefExtractor` on the same text
3. For each extracted belief with confidence ≥ `min_belief_confidence` (default: 0.6):
   - Call `set_belief` logic (create or reinforce)
   - Link evidence back to the extracted facts that support this belief
4. Include beliefs in the response

### Option B: Standalone `learn_to_belief` MCP tool

Create a separate MCP tool `learn_from_text` that only extracts beliefs (no facts/decisions/skills). Simpler but duplicates learn() infrastructure.

**Decision: Option A** — lower overhead, reuses existing extraction pipeline.

## Data Model

### BeliefExtractor (Pydantic)

```python
class ExtractedBelief(BaseModel):
    proposition: str          # The belief statement (free-form)
    confidence: float         # 0.0-1.0 — LLM-estimated confidence
    source_ids: list[str]     # IDs of extracted facts supporting this belief
    tags: list[str]           # Extracted topics/tags
    reasoning: str | None     # Why this belief was extracted from the text
```

### BeliefExtractor interface

```python
class BeliefExtractor:
    """Extract belief propositions from natural language text using LLM."""

    MODEL = "gpt-5.4-mini"  # Cheap model, beliefs are approximate

    SYSTEM_PROMPT = """You are a belief extraction system. Given a text, extract
    belief propositions — statements the author appears to hold as true.
    
    For each belief:
    1. proposition: concise declarative statement (1 sentence)
    2. confidence: 0.0-1.0 based on how explicitly stated vs inferred
       - 0.9-1.0: explicitly stated ("I use Docker for everything")
       - 0.6-0.8: strongly implied ("Docker makes deployment easy")
       - 0.3-0.5: weakly implied ("I've heard Docker is good")
       - 0.0-0.2: speculative
    3. tags: relevant categories (max 3)
    4. reasoning: why this proposition is considered a belief

    Extract 0-5 beliefs per text. Return JSON array.
    """

    async def extract(self, text: str) -> list[ExtractedBelief]: ...
```

### Evidence linking (content-based)

LLM экстрактор не может предсказать UUID фактов (они генерируются в транзакции). Вместо этого `source_refs` хранит proposition тексты фактов, которые маппятся на реальные ID после создания фактов:

```python
# 1. Collect mapping from proposition text → fact ID
proposition_to_fact_id = {
    f"{f.subject} {f.predicate} {f.object}": f.id
    for f in created_facts
}

# 2. Link evidence using content-based matching
for eb in extracted:
    if eb.confidence < min_belief_confidence:
        continue
    actual_source_ids = [
        proposition_to_fact_id[ref]
        for ref in eb.source_refs
        if ref in proposition_to_fact_id
    ]
    evidence = [
        Evidence(
            belief_id=belief.id,
            source_type="fact",
            source_id=fid,
            weight=eb.confidence,
        )
        for fid in actual_source_ids
    ]
```

### Reinforcement formula

When `set_belief` finds an existing active belief with the same proposition (case-insensitive exact match):
```
new_confidence = (old_belief.confidence * old_belief.version + confidence) / (old_belief.version + 1)
```
Where `old_belief.version` is the current revision count. This gives progressively less weight to new evidence as the belief matures.

### Soft limit on active beliefs

To prevent uncontrolled belief growth:
```python
MAX_ACTIVE_BELIEFS = 500  # Soft limit per v0.7

if extract_beliefs:
    active_count = await provider.search_beliefs(
        lifecycle_state="active", limit=0
    )
    if len(active_count) >= MAX_ACTIVE_BELIEFS:
        logger.warning(
            "Active beliefs (%s) at limit (%s): skipping belief extraction",
            len(active_count), MAX_ACTIVE_BELIEFS,
        )
        # Skip creation but don't fail — existing learn() results are still returned
    else:
        # Normal extraction flow
        extractor = BeliefExtractor(llm_extractor)
        ...
```

### Ingestion Service Changes

### Modified `learn()` tool

**New input parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `extract_beliefs` | bool | no | `false` | Also extract beliefs from text |
| `min_belief_confidence` | float | no | `0.6` | Minimum confidence to create a belief |

**Extended output (when extract_beliefs=true):**
```json
{
  "facts": [...],
  "decisions": [...],
  "skills": [...],
  "beliefs": [
    {
      "belief": { /* full Belief object */ },
      "extracted": { /* ExtractedBelief with proposition, confidence, reasoning */ }
    }
  ],
  "receipts": [...]
}
```

## Implementation

### Files

| File | Action | Description |
|------|--------|-------------|
| `src/memory_server/extractors/belief_extractor.py` | **Create** | BeliefExtractor class using LLM for extraction |
| `src/memory_server/services/ingestion_service.py` | **Modify** | Add belief extraction to learn() pipeline |
| `src/memory_server/server.py` | **Modify** | Add `extract_beliefs` + `min_belief_confidence` params |
| `src/memory_server/providers/sqlite_provider.py` | **Modify** | May need set_belief exposed for ingestion service |
| `contracts/learn.schema.json` | **Modify** | Add new params + beliefs output |
| `tests/test_belief_bridge.py` | **Create** | Tests for learn-to-belief bridge |

### Sequence

```
learn(text, extract_beliefs=True)
  │
  ├── FactExtractor.extract(text)       → facts
  ├── DecisionExtractor.extract(text)   → decisions
  ├── SkillExtractor.extract(text)      → skills
  └── BeliefExtractor.extract(text)     → extracted_beliefs
        │
        for each belief with confidence ≥ min_belief_confidence:
          ├── Check existing active belief (case-insensitive exact match)
          ├── If exists → reinforce (weighted average)
          ├── If new → create belief
          └── Create Evidence entries for each linked fact
```

### BeliefExtractor Implementation

Uses the same DI pattern as existing extractors (FactExtractor, DecisionExtractor, SkillExtractor). Accepts `llm_extractor: Callable | None`.

```python
class BeliefExtractor:
    """Extract belief propositions from natural language text using LLM."""

    SYSTEM_PROMPT = """You are a belief extraction system. Given a text, extract
    belief propositions — statements the author appears to hold as true.

    For each belief:
    1. proposition: concise declarative statement (1 sentence)
    2. confidence: 0.0-1.0 based on how explicitly stated vs inferred
       - 0.9-1.0: explicitly stated ("I use Docker for everything")
       - 0.6-0.8: strongly implied ("Docker makes deployment easy")
       - 0.3-0.5: weakly implied ("I've heard Docker is good")
       - 0.0-0.2: speculative
    3. source_refs: array of fact proposition texts that support this belief
       (e.g. "I use Docker", "Docker runs on OMV8")
    4. tags: relevant categories (max 3)
    5. reasoning: why this proposition is considered a belief

    Extract 0-5 beliefs per text. Return JSON array.
    """

    def __init__(self, llm_extractor: Callable | None = None):
        """
        Args:
            llm_extractor: Async callable taking (text, system_prompt) and
                returning list of ExtractedBelief. When None — used in
                test mode (regex-based fallback or empty result).
        """
        self._llm = llm_extractor

    async def extract(self, text: str) -> list[ExtractedBelief]:
        if not text or not text.strip():
            return []
        if self._llm is None:
            return []  # Test mode: no LLM available
        result = await self._llm(text, self.SYSTEM_PROMPT)
        return [ExtractedBelief(**item) for item in result]
```

### Ingestion Service Changes

After extracting facts/decisions/skills, if `extract_beliefs=True`:

```python
# In MemoryIngestionService.learn()
belief_results = []
if extract_beliefs:
    # Check soft limit
    active_count = await provider.search_beliefs(lifecycle_state="active", limit=0)
    if len(active_count) < MAX_ACTIVE_BELIEFS:
        extractor = BeliefExtractor(llm_extractor)
        extracted = await extractor.extract(text)
        for eb in extracted:
            if eb.confidence < min_belief_confidence:
                continue
            # Content-based evidence linking
            proposition_to_fact_id = {
                f"{f.subject} {f.predicate} {f.object}": f.id
                for f in created_facts
            }
            actual_source_ids = [
                proposition_to_fact_id[ref]
                for ref in eb.source_refs
                if ref in proposition_to_fact_id
            ]
            evidence = [
                Evidence(belief_id=..., source_type="fact", source_id=fid, weight=eb.confidence)
                for fid in actual_source_ids
            ]
            # Create or reinforce belief via provider
            ...
```

### MCP API Changes

### Modified `learn()` tool

## Acceptance Criteria

1. `learn(text="I prefer Docker over Podman for development", extract_beliefs=True)` extracts a belief with proposition containing "Docker" / "prefer"
2. Extracted belief has confidence ≥ 0.6 and correct tags
3. Evidence entries link back to extracted facts (if any)
4. Running learn() twice with similar text reinforces (doesn't duplicate) the belief
5. `extract_beliefs=False` (default) — no beliefs created, no performance impact
6. Empty text returns empty results gracefully
7. Belief with confidence < threshold is not created
8. All existing learn() tests continue to pass without modification
9. Integration test: full pipeline text → belief → reflect(overview) shows belief

## Non-goals (v0.7)

- Multi-turn belief extraction (requires conversation context)
- Learning from structured data (JSON, tables) — text only
- Belief conflict auto-resolution (manual via resolve_conflict)
- Custom extraction prompts (fixed SYSTEM_PROMPT)
- Streaming extraction (batch only)
