# Card 002: Pydantic Data Models

## Context
v0.1a — foundation for all data storage. Define the core data types per ADR-006
(Entity/Fact/Decision/Skill) plus MemoryReceipt per ADR-008 (mandatory provenance).

## Goal
Create Pydantic v2 models for all five knowledge types in `src/memory_server/models/`.

## Acceptance Criteria
- [ ] All models use Pydantic v2 (`BaseModel`, `ConfigDict`, validators)
- [ ] Entity model: id, type, name, attributes (dict), created_at, updated_at
- [ ] Fact model: id, subject, predicate, object, confidence (float 0-1), source, created_at
- [ ] Decision model: id, context, choice, rejected_alternatives (list[str]), reason, source, created_at
- [ ] Skill model: id, name, version, purpose, steps (list[str]), constraints (list[str]), validation (list[str]), success_rate (float 0-1), created_at
- [ ] MemoryReceipt: id, memory_type, source, created_by, timestamp, confidence, verification_status, history (list of previous states)
- [ ] All models have `model_config = ConfigDict(from_attributes=True)` for SQLAlchemy compatibility
- [ ] `pytest tests/ -v` passes (model validation tests)
- [ ] `ruff check src/` passes

## Approach
1. Create `src/memory_server/models/__init__.py` — re-export all models
2. Create `src/memory_server/models/entity.py`
3. Create `src/memory_server/models/fact.py`
4. Create `src/memory_server/models/decision.py`
5. Create `src/memory_server/models/skill.py`
6. Create `src/memory_server/models/receipt.py`
7. Write `tests/test_models.py` — validate construction, serialization, edge cases
8. Run tests + lint

## Tests
- Each model: valid construction, JSON serialization round-trip, field validation
- Fact: confidence must be 0.0-1.0
- Decision: empty rejected_alternatives allowed (no alternatives considered)
- Skill: steps must be non-empty, version defaults to "1.0.0"
- MemoryReceipt: verification_status enum (unverified, candidate, validated, trusted, deprecated, archived)
- Edge cases: Optional/default fields, empty strings, None values

## Dependencies
- pydantic>=2.0.0 — already in pyproject.toml
