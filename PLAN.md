# PLAN.md — Composite Memory MCP Server

## Phase: v0.1a (current)
Goal: MCP API + SQLite backend + get_context/search/remember

| # | Card | ID | Status | Review | Tests | Merged |
|---|------|----|--------|--------|-------|--------|
| 001 | Project skeleton | t_ed422770 | ✅ done | ✅ approve | ✅ pass | ✅ v0.1a-alpha.1 |
| 002 | Pydantic data models | t_da4fd8bb | ✅ done | ✅ approve | ✅ pass | ✅ v0.1a-alpha.2 |
| 003 | SQLite provider | — | ✅ done | ✅ approve | ✅ pass | ✅ v0.1a-alpha.3 |
| 004 | MCP tool: get_context | — | ✅ done | ✅ approve | ✅ pass | ✅ v0.1a-alpha.4 |
| 005 | MCP tool: search | — | ✅ done | — | ✅ pass | ✅ v0.1a-alpha.5 |
| 006 | MCP tool: remember | — | ⏳ | — | — | — |
| 007 | Integration tests + docs | — | ⏳ | — | — | — |

## Phase: v0.2 (next)
Qdrant + embeddings + semantic router

## Phase: v0.3
LLM extractors + learn()

## Phase: v0.4
Graph DB + entity relations

## Phase: v0.5+
Confidence engine + validation + decay + memory auditor
