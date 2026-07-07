# Composite Memory MCP Server (CMMS)

Independent MCP memory service for AI agents. Agent-independent.

## Docs

- [ADR](docs/ADR.md) — Architecture Decision Records (10 ADRs)
- [Agent Spec](docs/agent-spec.md) — Implementation specification
- [Technical Design](docs/technical-design.md) — Tech stack + roadmap

## Stack

Python 3.12+, MCP SDK, Pydantic, SQLAlchemy, Qdrant, Neo4j, GitPython

## Roadmap

| Phase | Milestone |
|-------|-----------|
| v0.1a | MCP API + SQLite provider + get_context/search/remember |
| v0.2  | Qdrant + embeddings + semantic router |
| v0.3  | LLM extractors + learn() |
| v0.4  | Graph DB + entity relations |
| v0.5+ | Confidence engine + validation + decay + auditor |
