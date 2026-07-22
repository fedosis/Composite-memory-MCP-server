# Composite Memory MCP Server

## Agent Implementation Specification

> **Status note for v0.11.0b1:** this file is an implementation specification
> plus historical roadmap. Current public facts: GitHub prerelease is published;
> PyPI, official MCP Registry, Smithery, and Glama are not published. Current
> runtime uses SQLite/FTS5 as the durable and keyword-search base, LanceDB by
> default or Qdrant optionally for semantic search, and in-memory SimpleGraph
> for graph lookup. Neo4j remains future/optional and is not wired in v0.11.
> Hermes support is an optional `[hermes]` integration; see `docs/INTEGRATION.md`.

> **Status:** Historical/spec reference document. Core architectural principles are current; the Required Stack and Storage Mapping have been updated for v0.11 (LanceDB by default, Qdrant optional, SimpleGraph in-memory graph). Development Roadmap reflects original planned milestones with current-technology annotations.

## Mission

Implement an independent MCP memory service for AI agents.

The server MUST NOT depend on a specific agent.

## Repository Structure

    memory-server/

    ├── api/
    │   MCP protocol implementation
    ├── core/
    │   memory orchestrator
    │   context builder
    │   routing engine
    ├── models/
    │   entity.py
    │   fact.py
    │   decision.py
    │   skill.py
    │   receipt.py
    ├── router/
    │   rules.py
    │   embedding_router.py
    │   graph_router.py
    ├── extractors/
    │   fact_extractor.py
    │   decision_extractor.py
    │   skill_extractor.py
    ├── providers/
    │   sqlite_provider.py
    │   qdrant_provider.py
    │   lancedb_provider.py
    │   graph_provider.py
    │   git_provider.py
    └── evaluation/
        confidence.py
        validator.py
        decay.py

## Required Stack

Language: Python 3.11+

Libraries: - MCP SDK - Pydantic - SQLAlchemy - aiosqlite - LanceDB/Qdrant
client extras - SimpleGraph currently, Neo4j driver only for future/optional
graph work - GitPython optional

## MCP Tools

### get_context()

Purpose: Prepare context for agent reasoning.

Input: - task - agent - project

Output: - facts - decisions - skills - warnings

### search()

Purpose: Explicit memory search.

Routing:

Query -\> Rules -\> Embedding Router -\> Graph Lookup

### remember()

Purpose: Directly store explicit knowledge.

### learn()

Purpose: Extract reusable knowledge from sessions.

Pipeline:

Raw session -\> Extractors -\> Classification -\> Validation -\> Storage

## Data Model

### Entity

Fields: - id - type - name - attributes

### Fact

Fields: - subject - predicate - object - confidence - source

### Decision

Fields: - context - choice - rejected alternatives - reason - confidence

### Skill

Fields: - name - version - purpose - steps - validation - success rate

## Routing Strategy

Priority:

1.  Rules engine
2.  Embedding similarity
3.  Graph traversal
4.  LLM fallback

LLM must not be the first routing layer.

## Storage Mapping (v0.11)

*Note: This section reflects the originally designed architecture. Current v0.11 implementation uses the technologies noted.*

| Layer        | v0.11 Implementation                    | Future / Optional             |
|-------------|----------------------------------------|------------------------------|
| Facts       | SQLite via SQLAlchemy async            | PostgreSQL                    |
| Semantic    | LanceDB (default); Qdrant optional    | —                            |
| Relations   | SimpleGraph (in-memory dict+set)      | Neo4j / Graphiti (declared dependency, not wired) |
| Skills      | Git repository                        | —                            |

## Lifecycle

States:

candidate validated active stale archived forgotten

Compatibility note: older docs/specs may say `trusted`/`deprecated`; v0.11 maps
those concepts to `active`/`stale`.

## Development Roadmap (historical record)

*Note: These were the original planned milestones. Current v0.11 implementation uses LanceDB (default) with Qdrant optional for vector, and SimpleGraph for in-memory graph relations.*

### v0.1a

Implement: - MCP interface - schemas - SQLite provider - get_context -
search - remember

### v0.2

Implement: - embeddings - LanceDB/Qdrant - semantic routing

### v0.3

Implement: - extractors - learn()

### v0.4

Implement: - graph layer

### v0.5+

Implement: - confidence - validation - decay - memory auditor

## Principles

1.  Memory must be agent-independent.
2.  Every memory item requires provenance.
3.  Unvalidated knowledge must not become trusted memory.
4.  Preserve history.
5.  Prefer structured knowledge over raw text.
