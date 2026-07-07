# Card 005: MCP tool — search

## Context
v0.1a — agents need to search stored facts by keyword. The SQLite provider already supports LIKE-based text search.

## Goal
Create a `search` MCP tool that wraps the SQLite provider's search_facts with keyword search (SQL LIKE), returning matched facts.

## Acceptance Criteria
- [ ] `search` tool accepts `query` (str, required) plus optional filters: `subject`, `predicate`, `limit`
- [ ] Performs LIKE-based search on fact subject/predicate/object
- [ ] Returns JSON with `results` (list) and `total` (int)
- [ ] Tool registered in server.py and available via MCP
- [ ] `pytest tests/ -v` passes
- [ ] `ruff check src/` passes

## Approach
1. Create `src/memory_server/api/search.py` — implements the search logic
2. Update `src/memory_server/server.py` — register search_tool
3. Write `tests/test_search.py`
4. Run tests + lint

## Tests
- Exact match query returns results
- Partial match (substring) returns results
- No results case returns empty array
- Optional subject filter narrows results
- limit parameter respected
