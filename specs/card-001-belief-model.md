# Card 001: Belief Model — v3 (post-review v2)

## Objective

Add a belief store to the Composite Memory MCP Server. A belief is a proposition the agent holds as true about the user, system, or world, with explicit confidence, source attribution, and revision history.

## Motivation

The existing memory model (facts, decisions, skills) stores *what happened* and *what was decided*. It does not model *what the agent believes to be true* — a fundamentally different concept:

- A **fact** is a recorded observation (`Docker runs on OMV8`).
- A **belief** is an inferred or asserted proposition with confidence and evidence (`The user prefers Docker Compose over Podman — confidence 0.85, based on 12 observations since March 2026`).

Beliefs enable the agent to:
- Track evolving understanding of the user and system
- Detect contradictions (conflicting beliefs)
- Revise beliefs when new evidence arrives
- Explain *why* it holds a position

## Data Model

### Belief (Pydantic)

Uses `pydantic.BaseModel` with `ConfigDict(from_attributes=True)` — consistent with existing Fact/Decision/Skill models. ADR-008 compliant.

```python
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from uuid import uuid4

class Belief(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str = Field(default_factory=lambda: str(uuid4()))
    proposition: str = Field(..., min_length=1, max_length=2048)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    source: str = Field(default="system")
    creator: str = Field(default="system")
    source_ids: list[str] = Field(default_factory=list)  # denormalized from Evidence entries
    tags: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_reinforced_at: datetime = Field(default_factory=datetime.utcnow)
    version: int = Field(default=1, ge=1)   # integer revision counter (unlike Fact/Decision semver)
    verification_status: str = "candidate"
    lifecycle_state: str = "active"
```

**Design rationale:** Belief does NOT define a parallel `BeliefStatus` enum. Uses `lifecycle_state` extended from v0.6 lifecycle engine. All state transitions go through `LifecycleRepository.record_event()`. Version is `int` (revision counter) because belief supersession is a linear chain, not semantic versioning.

### Evidence (Pydantic)

```python
class Evidence(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str = Field(default_factory=lambda: str(uuid4()))
    belief_id: str = Field(...)
    source_type: str = Field(...)          # "fact" | "decision" | "observation" | "user_statement"
    source_id: str = Field(...)
    weight: float = Field(default=0.5, ge=0.0, le=1.0)
    contributor: str = Field(default="system")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    note: str | None = None
```

**Confidence aggregation:** `Belief.confidence = weighted_average(e.weight for e in active_evidence)`, normalized to sum = 1.0. Implemented as a new method `ConfidenceEngine.score_belief(evidence: list[Evidence]) -> float` — NOT the existing `score_fact()` which uses a different methodology (source reliability, age decay, corroboration boost, conflict penalty, lifecycle multiplier).

## Lifecycle State Machine

### Valid states

`active | superseded | contradicted | discarded | stale | archived | forgotten`

### Transition matrix

| From → To | Trigger | Notes |
|-----------|---------|-------|
| `active → superseded` | `set_belief(replace_belief_id=)` | Old belief superseded by new version |
| `active → contradicted` | Conflict detected (manual via `resolve_conflict` or future auto-detect) | Two contradictory beliefs coexist |
| `contradicted → active` | `resolve_conflict(keep_a)` | One belief retained |
| `contradicted → discarded` | `resolve_conflict(discard_both)` or `resolve_conflict(keep_b)` | The other belief discarded |
| `superseded → discarded` | Manual cleanup / future auto-cleanup | Superseded beliefs can be cleaned up |
| `active → stale` | DecayEngine.tick() | Standard lifecycle decay |
| `superseded → stale` | DecayEngine.tick() | Superseded beliefs decay too |
| `contradicted → stale` | DecayEngine.tick() | Unresolved contradictions decay |
| `discarded → archived` | DecayEngine.tick() | Discarded → archived after cooldown |
| `stale → archived` | DecayEngine.tick() | Standard lifecycle |
| `archived → forgotten` | DecayEngine.tick() | Standard lifecycle |

**Terminal states:** `forgotten`. No transitions out.
- `discarded` is NOT terminal — decay moves it to `archived` then `forgotten`

**Implementation requirements:**
1. Add `"superseded"`, `"contradicted"`, `"discarded"` to `LifecycleState` enum in `storage/models/lifecycle.py` or `storage/base.py`
2. Extend `_VALID_TRANSITIONS` in `evaluation/validator.py` with all entries from the matrix above
3. Add entries for `"superseded"`, `"contradicted"`, `"discarded"` to `ConfidenceEngine.LIFECYCLE_MULTIPLIER` (suggested: 0.3 for superseded/contradicted, 0.0 for discarded)
4. Set `"belief"` TTL in `PER_TYPE_TTL` in `evaluation/decay.py` to 180 days
5. All transitions logged via `LifecycleRepository.record_event()`

## Storage

### SQLAlchemy ORM models

```python
# storage/models/belief.py
class BeliefORM(Base):
    __tablename__ = "beliefs"
    id = Column(String, primary_key=True, default=uuid4)
    proposition = Column(String(2048), nullable=False, index=True)
    confidence = Column(Float, nullable=False, default=0.5)
    source = Column(String(128), nullable=False, default="system")
    creator = Column(String(128), nullable=False, default="system")
    source_ids = Column(JSON, nullable=False, default=list)  # denormalized, rebuilt from Evidence
    tags = Column(JSON, nullable=False, default=list)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    last_reinforced_at = Column(DateTime, nullable=False, default=func.now())
    version = Column(Integer, nullable=False, default=1)
    verification_status = Column(String(32), nullable=False, default="candidate")
    lifecycle_state = Column(String(32), nullable=False, default="active", index=True)

class EvidenceORM(Base):
    __tablename__ = "evidence"
    id = Column(String, primary_key=True, default=uuid4)
    belief_id = Column(String, ForeignKey("beliefs.id"), nullable=False, index=True)
    source_type = Column(String(32), nullable=False)
    source_id = Column(String, nullable=False)
    weight = Column(Float, nullable=False, default=0.5)
    contributor = Column(String(128), nullable=False, default="system")
    created_at = Column(DateTime, nullable=False, default=func.now())
    note = Column(Text, nullable=True)
```

### Alembic migration

Add tables `beliefs` and `evidence`, plus `beliefs_fts` FTS5 virtual table on `proposition` (analogous to existing `facts_fts`):

```sql
CREATE VIRTUAL TABLE beliefs_fts USING fts5(proposition, content='beliefs', content_rowid='rowid');
-- Trigger to keep FTS5 in sync with beliefs table
```

### Repositories

```python
# storage/repositories/belief_repo.py
class BeliefRepository:
    def create(self, belief: BeliefORM) -> BeliefORM
    def get_by_id(self, belief_id: str) -> BeliefORM | None
    def search(self, proposition: str | None = None, tags: list[str] | None = None,
               lifecycle_state: str | None = "active", min_confidence: float | None = None,
               source: str | None = None, creator: str | None = None,
               limit: int = 10) -> list[BeliefORM]
    def update_confidence(self, belief_id: str, new_confidence: float)
    def update_lifecycle_state(self, belief_id: str, new_state: str)

class EvidenceRepository:
    def create(self, evidence: EvidenceORM) -> EvidenceORM
    def get_by_belief_id(self, belief_id: str) -> list[EvidenceORM]
    def get_active_weights(self, belief_id: str) -> list[float]
```

### SQLiteProvider CRUD

```python
# providers/sqlite_provider.py — new methods
def create_belief(self, belief: Belief, evidence: list[Evidence] | None = None) -> Belief
def get_belief(self, belief_id: str) -> Belief | None
def search_beliefs(self, proposition: str | None = None, tags: list[str] | None = None,
                   lifecycle_state: str | None = None, min_confidence: float | None = None,
                   source: str | None = None, creator: str | None = None,
                   limit: int = 10) -> list[Belief]
def update_belief_confidence(self, belief_id: str, new_confidence: float) -> Belief
def update_belief_lifecycle(self, belief_id: str, new_state: str) -> Belief
def create_in_transaction(self, belief: Belief, evidence: list[Evidence]) -> tuple[Belief, list[Evidence]]
```

## MCP Tools

### `set_belief`

Create, reinforce, or supersede a belief.

**Input:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `proposition` | string | yes | The belief statement |
| `confidence` | float | no | Initial/override confidence (default: 0.5) |
| `sources` | string[] | no | IDs of facts/decisions supporting this belief |
| `tags` | string[] | no | Optional grouping tags |
| `source` | string | no | How this belief was created (default: "system") |
| `replace_belief_id` | string | no | UUID of belief to supersede |

**Behaviour:**
- If `replace_belief_id` is provided: mark old belief `lifecycle_state=superseded` via LifecycleRepository, create new with `version=old_version + 1`
- If proposition already exists with `lifecycle_state=active`: reinforcement — weighted average confidence, update `last_reinforced_at`
- Reinforcement match is **case-insensitive trimmed exact match** on proposition text
- If new: create with `version=1`, `lifecycle_state=active`

**Output (full Belief object + receipt — consistent with remember/learn):**
```json
{
  "belief": {
    "id": "uuid", "proposition": "...", "confidence": 0.85,
    "source": "inference", "creator": "system",
    "tags": ["user-preference", "docker"],
    "source_ids": ["fact-uuid-1", "fact-uuid-2"],
    "version": 2, "verification_status": "candidate",
    "lifecycle_state": "active",
    "created_at": "2026-07-12T09:00:00Z",
    "updated_at": "2026-07-12T09:00:00Z",
    "last_reinforced_at": "2026-07-12T09:00:00Z"
  },
  "receipt": {
    "id": "receipt-uuid",
    "operation": "set_belief",
    "entity_type": "belief",
    "entity_id": "uuid",
    "timestamp": "2026-07-12T09:00:00Z"
  },
  "superseded": "previous-belief-uuid | null"
}
```

### `get_belief`

Retrieve beliefs.

**Input:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `proposition` | string | no | FTS5 match against belief text (via beliefs_fts) |
| `source_id` | string | no | Find beliefs supported by this entity |
| `lifecycle_state` | string | no | Filter by state (default: "active") |
| `min_confidence` | float | no | Confidence threshold |
| `tags` | string[] | no | Filter by tags |
| `source` | string | no | Filter by creation source |
| `creator` | string | no | Filter by creator |
| `limit` | int | no | Max results (default: 10) |

**Output:**
```json
{
  "total": 2,
  "beliefs": [ { /* full Belief object */ } ],
  "query": { "lifecycle_state": "active", "limit": 10 }
}
```

### `resolve_conflict`

Explicitly resolve a contradiction between two beliefs.

**Input:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `belief_a_id` | string | yes | One conflicting belief |
| `belief_b_id` | string | yes | The other conflicting belief |
| `resolution` | string | yes | "keep_a" | "keep_b" | "merge" | "discard_both" |
| `new_proposition` | string | no | Required if resolution="merge" |

**Behaviour (transition matrix):**
- `keep_a`: belief_b → `discarded`, belief_a stays `active` (or `contradicted → active`)
- `keep_b`: belief_a → `discarded`, belief_b stays `active`
- `discard_both`: both → `discarded`
- `merge`: both → `discarded`, create new belief with `new_proposition`

All transitions logged via `LifecycleRepository.record_event()`.

**Output:**
```json
{
  "belief_a": { /* full Belief object after update */ },
  "belief_b": { /* full Belief object after update */ },
  "resolution": "keep_a",
  "created": null,
  "events": ["lifecycle-event-uuid-1", "lifecycle-event-uuid-2"]
}
```

If `resolution="merge"`, also returns:
```json
{
  "created": { /* full new Belief object */ }
}
```

## Outbox Integration

1. **OutboxEntryORM** — add `"belief"` as a valid `record_type`
2. **OutboxWorker** — add `_process_index_belief()`:
   ```python
   payload = {
       "proposition": "...",
       "confidence": 0.85,
       "tags": ["user-preference"],
       "source": "inference",
       "memory_type": "belief",
       "point_uuid": uuid5('belief:{record_id}')
   }
   ```
   - Indexes proposition in Qdrant (semantic search via existing embedding provider)
   - Creates/updates graph node for the belief (if `GraphRouter.sync_belief()` exists; otherwise deferred)
3. **SQLiteProvider.create_in_transaction()** — belief + evidence + outbox in single transaction

## Conflict Detection (v0.7)

**Exact match only:** when `set_belief` creates a new active belief with the same proposition text (case-insensitive trimmed comparison) as an existing active belief → reinforce (update confidence). No semantic conflict detection.

`resolve_conflict` is **manual** — called by the agent or user.

## Integration with Existing Tools

### remember() → belief bridge (Card 003, v0.7)

Defined but not implemented in Card 001. Requires new `BeliefExtractor` or modifications to existing extractors.

### learn() → belief bridge (Card 003, v0.7)

Similar to remember bridge. Extracted knowledge with confidence > 0.7 → optional belief creation.

## Implementation Order

```
Step 1: Pydantic models (Belief, Evidence, BeliefORM, EvidenceORM)
Step 2: Alembic migration (beliefs + evidence tables, beliefs_fts FTS5 index)
Step 3: Repository layer (BeliefRepository, EvidenceRepository)
Step 4: SQLiteProvider CRUD (create, get, search, update + create_in_transaction)
Step 5: Outbox integration (record_type="belief", _process_index_belief)
Step 6: Lifecycle integration (transition matrix, PER_TYPE_TTL, model registration,
        Validator, LifecycleState enum, LIFECYCLE_MULTIPLIER)
Step 7: MCP tools + JSON Schema contracts
Step 8: Tests (unit → integration → edge cases)
```

JSON Schema contracts: `contracts/set_belief.schema.json`, `get_belief.schema.json`, `resolve_conflict.schema.json` in v0.7.0 format, referencing `common.schema.json#/definitions/`.

## Acceptance Criteria

1. `set_belief` creates new belief, returns full object + receipt
2. `set_belief` with existing proposition (case-insensitive trimmed match) reinforces confidence (weighted average via `ConfidenceEngine.score_belief()`)
3. `set_belief` with `replace_belief_id` supersedes old version (`lifecycle_state→superseded`, version incremented)
4. `get_belief` returns matching beliefs by proposition (FTS5 via beliefs_fts), lifecycle_state, tags, source, creator, min_confidence
5. `resolve_conflict` correctly updates lifecycle_state per transition matrix
6. All changes survive server restart (SQLite + WAL)
7. Alembic migration creates beliefs, evidence, beliefs_fts tables
8. Outbox worker indexes belief in Qdrant
9. Lifecycle engine processes "belief" type (decay, archive, forget) via lifecycle_state
10. Validator accepts all valid state transitions from the transition matrix

## Non-goals (v0.7)

- Semantic conflict detection (LLM-based) — v0.8+
- Automatic belief extraction from conversation — Card 003
- Belief propagation across agent instances — v1.0
- Graphical belief network — v1.0+
- `ConflictRecord` entity — use lifecycle_state + LifecycleEvent instead
