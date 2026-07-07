# Card 004: MCP tool — get_context

## Context
v0.1a — MCP tools that agents call to retrieve structured context about tasks and agents.

## Goal
Create a `get_context` MCP tool that reads `task` and optional `agent` parameters, searches facts and decisions via the SQLite provider, and returns structured context.

## Acceptance Criteria
- [ ] `get_context` tool accepts `task` (str, required) and optional filters (`subject`, `max_results`)
- [ ] Searches facts by text match against subject/predicate/object
- [ ] Returns structured dict: `{"facts": [...], "decisions": [...], "total": int}`
- [ ] Tool registered in server.py and available via MCP
- [ ] `pytest tests/ -v` passes
- [ ] `ruff check src/` passes

## Approach
1. Create `src/memory_server/api/__init__.py`
2. Create `src/memory_server/api/get_context.py` — implements the get_context logic
3. Update `src/memory_server/server.py` — register get_context tool
4. Write `tests/test_get_context.py`
5. Run tests + lint

## Tests
- Basic call with task, verify returned structure
- Call with subject filter
- Call with no results
- Verify facts and decisions are returned separately
