# Card 003: SQLite Provider (CRUD)

## Context
v0.1a — SQLite storage backend for facts and receipts. Per ADR-002 (Composite Memory),
facts use SQLite/PostgreSQL. Per ADR-010 (Incremental), v0.1a uses SQLite only.

## Goal
Create an async SQLite provider using SQLAlchemy + aiosqlite that supports CRUD
operations for facts and memory receipts.

## Acceptance Criteria
- [ ] Async SQLAlchemy engine with aiosqlite
- [ ] SQLAlchemy ORM models for Fact and MemoryReceipt tables
- [ ] SQLiteProvider class with create_fact, get_fact, search_facts, update_fact, delete_fact
- [ ] SQLiteProvider class with create_receipt, get_receipt
- [ ] search_facts supports: by subject, predicate, object, source, confidence range, text search
- [ ] In-memory SQLite for tests (not file-based)
- [ ] `pytest tests/ -v` passes (SQLite provider tests)
- [ ] `ruff check src/` passes

## Approach
1. Create `src/memory_server/providers/__init__.py`
2. Create `src/memory_server/providers/sqlite_provider.py` with:
   - SQLAlchemy declarative ORM models for Fact and MemoryReceipt
   - SQLiteProvider class wrapping async CRUD
3. Write `tests/test_sqlite_provider.py` — test each CRUD operation with real SQLite
4. Run tests + lint

## Tests
- create_fact: insert a fact, verify it exists
- get_fact: retrieve by id, return None if not found
- search_facts: by subject exact match, by predicate, by text search (LIKE on subject/predicate/object)
- update_fact: modify fields, verify updated
- delete_fact: remove, verify gone
- create_receipt: insert receipt, verify retrieval
- Edge cases: empty results, missing fields

## Dependencies
- sqlalchemy>=2.0.0 — already in pyproject.toml
- aiosqlite>=0.20.0 — already in pyproject.toml
