# Composite Memory MCP Server

## Agent Implementation Specification

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
    │   postgres_provider.py
    │   qdrant_provider.py
    │   graph_provider.py
    │   git_provider.py
    └── evaluation/
        confidence.py
        validator.py
        decay.py

## Required Stack

Language: Python 3.12+

Libraries: - MCP SDK - Pydantic - SQLAlchemy - Qdrant client - Neo4j
driver - GitPython

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

## Storage Mapping

Facts: SQLite/PostgreSQL

Semantic: Qdrant

Relations: Graph DB

Skills: Git repository

## Lifecycle

States:

candidate validated trusted deprecated archived

## Development Roadmap

### v0.1a

Implement: - MCP interface - schemas - SQLite provider - get_context -
search - remember

### v0.2

Implement: - embeddings - Qdrant - semantic routing

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
