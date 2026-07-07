# Card 001: Project Skeleton

## Context
First card of v0.1a. Need a working MCP server entry point, package structure,
virtual environment, and CI/commit hooks. This is the foundation all other cards build on.

## Goal
Create a functioning Python package (`memory-server`) with an MCP server that
starts, announces itself via stdio transport, and exposes a single `ping` tool
for connectivity verification.

## Acceptance Criteria

- [ ] `pip install -e ".[dev]"` installs without errors
- [ ] `memory-server --help` prints CLI usage
- [ ] MCP server starts and handles a `ping` request via stdio transport
- [ ] `pytest tests/` passes (at minimum the ping test)
- [ ] `ruff check src/` passes (no lint errors)
- [ ] `git commit` hook runs lint + test (pre-commit or simple script)
- [ ] Project README updated with dev setup instructions

## Approach

1. Create `src/memory_server/cli.py` with `typer` entry point
2. Wire `mcp.server.Server` with a `ping` tool tool
3. Add `[tool.ruff]` to pyproject.toml
4. Set up virtual env + install
5. Write `tests/test_ping.py` — starts server, sends ping, expects pong
6. Set up pre-commit hook (`.githooks/pre-commit` → `ruff check src/ && pytest tests/`)
7. Update README.md with dev quickstart

## Architecture Review Required
Yes — validate MCP server lifecycle and tool registration pattern.

## Tests
- `test_server_lifecycle` — server starts, creates transport, shuts down cleanly
- `test_ping_tool` — calls `ping`, receives `{"status": "ok"}`

## Dependencies
- `mcp>=1.0.0` — already in pyproject.toml
- `typer>=0.12.0` — add to pyproject.toml
