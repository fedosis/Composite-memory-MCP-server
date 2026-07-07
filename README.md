# Composite Memory MCP Server (CMMS)

Independent MCP memory service for AI agents. Agent-independent.

## Dev Setup

```bash
# Clone and enter
git clone git@github.com:fedosis/Composite-memory-MCP-server.git
cd memory-server

# Create venv and install
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest tests/ -x -q

# Lint
ruff check src/

# CLI
memory-server --help
memory-server ping
```

## Docs

- [ADR](docs/ADR.md) — Architecture Decision Records (10 ADRs)
- [Agent Spec](docs/agent-spec.md) — Implementation specification
- [Technical Design](docs/technical-design.md) — Tech stack + roadmap

## API Reference

The server exposes eight MCP tools:

### ping

Health check — verifies the server is running and responsive.

**Arguments:** None

**Response:**
```json
{"status": "ok"}
```

### search

Search for stored facts by query text, subject, predicate, or object.

**Arguments:**
| Parameter  | Type   | Required | Description |
|-----------|--------|----------|-------------|
| `query`   | string | yes      | Text to search across all fact fields |
| `subject` | string | no       | Filter by subject |
| `predicate` | string | no    | Filter by predicate |
| `object`  | string | no       | Filter by object |
| `source`  | string | no       | Filter by source |
| `limit`   | int    | no       | Max results (default: 10) |

**Response:**
```json
{
  "total": 2,
  "results": [
    {"id": "uuid", "subject": "Docker", "predicate": "runs_on", "object": "OMV8", "confidence": 1.0, "source": "test", "created_at": "2025-01-01T00:00:00Z", "updated_at": "2025-01-01T00:00:00Z"}
  ],
  "query": "Docker"
}
```

### remember

Store a new fact in the memory server.

**Arguments:**
| Parameter    | Type   | Required | Description |
|-------------|--------|----------|-------------|
| `subject`   | string | yes      | Subject entity |
| `predicate` | string | yes      | Relation/predicate |
| `object`    | string | yes      | Object entity |
| `confidence` | float | no       | Confidence score 0.0–1.0 (default: 0.5) |
| `source`    | string | no       | Source identifier (default: "manual") |
| `tags`      | list   | no       | Optional tags |

**Response:**
```json
{
  "receipt": {
    "id": "uuid",
    "memory_type": "fact",
    "confidence": 1.0,
    "source": "test",
    "verification_status": "candidate",
    "timestamp": "2025-01-01T00:00:00Z"
  },
  "fact": {
    "id": "uuid",
    "subject": "Docker",
    "predicate": "runs_on",
    "object": "OMV8",
    "confidence": 1.0,
    "source": "test",
    "created_at": "2025-01-01T00:00:00Z",
    "updated_at": "2025-01-01T00:00:00Z"
  }
}
```

### get_context

Retrieve relevant context facts for a given task or subject.

**Arguments:**
| Parameter    | Type   | Required | Description |
|-------------|--------|----------|-------------|
| `task`      | string | yes      | Task description or subject to find context for |
| `subject`   | string | no       | Optional subject filter |
| `max_results` | int  | no       | Max results (default: 10) |

**Response:**
```json
{
  "total": 2,
  "facts": [
    {"id": "uuid", "subject": "Caddy", "predicate": "uses", "object": "Port 443", "confidence": 1.0, "source": "test", "created_at": "2025-01-01T00:00:00Z", "updated_at": "2025-01-01T00:00:00Z"}
  ],
  "task": "Caddy"
}
```

### semantic_search

Semantic search — embed a query, find similar facts via vector similarity, and return ranked results with similarity scores.

Per ADR-005, routing rules (keyword-based exact matches) are evaluated **before** the embedding search. If a rule matches, the result indicates which route should handle the query (e.g., `"route": "sql"`). Otherwise, semantically ranked results are returned.

**Arguments:**
| Parameter        | Type   | Required | Default | Description |
|-----------------|--------|----------|---------|-------------|
| `query`         | string | yes      | —       | Natural language query text |
| `top_k`         | int    | no       | 10      | Maximum number of results |
| `score_threshold` | float | no      | 0.0     | Minimum similarity score 0.0–1.0 |

**Response (rule match):**
```json
{
  "rule_match": {
    "route": "sql",
    "rule_name": "ip_address_query",
    "matched_keyword": "ip of"
  }
}
```

**Response (semantic results):**
```json
{
  "semantic_results": [
    {
      "id": "uuid",
      "score": 0.92,
      "payload": {
        "subject": "Docker",
        "predicate": "runs_on",
        "object": "OMV8",
        "content": "Docker runs on OMV8"
      }
    }
  ],
  "total": 1
}
```

### learn

Extract and store facts, decisions, and skills from natural language text. Runs all three extractors (FactExtractor, DecisionExtractor, SkillExtractor) on the input text, stores extracted items in the memory database, and returns structured results with receipts per item type.

**Arguments:**
| Parameter | Type   | Required | Default | Description |
|-----------|--------|----------|---------|-------------|
| `text`    | string | yes      | —       | Natural language text to extract knowledge from |
| `source`  | string | no       | "user"  | Source identifier for provenance tracking |

**Response:**
```json
{
  "facts": [
    {
      "receipt": {"id": "uuid", "memory_type": "fact", "source": "user", "confidence": 0.5, "verification_status": "candidate"},
      "item": {"id": "uuid", "subject": "Docker", "predicate": "is", "object": "container", "confidence": 0.5, "source": "user"}
    }
  ],
  "decisions": [
    {
      "receipt": {"id": "uuid", "memory_type": "decision", "source": "user", "confidence": 0.5, "verification_status": "candidate"},
      "item": {"id": "uuid", "context": "", "choice": "use Caddy", "reason": "it is simpler", "source": "user"}
    }
  ],
  "skills": [
    {
      "receipt": {"id": "uuid", "memory_type": "skill", "source": "user", "confidence": 0.5, "verification_status": "candidate"},
      "item": {"id": "uuid", "purpose": "deploy docker", "steps": ["pull image", "run container"], "success_rate": 0.5}
    }
  ],
  "receipts": [
    {"id": "uuid", "memory_type": "fact", "source": "user", "verification_status": "candidate"},
    {"id": "uuid", "memory_type": "decision", "source": "user", "verification_status": "candidate"}
  ]
}
```

### graph_search

Search the knowledge graph for entities, relations, and paths between entities.

Supports three search modes depending on which parameters are provided:

**Mode 1 — Query (entity lookup):** Pass a `query` string. The server extracts entity references from
the query and returns matching entities plus their neighbors and the relations between them.

**Mode 2 — Direct node lookup:** Pass an `entity_id` to look up a specific graph node by its ID
and get its neighbors and edges.

**Mode 3 — Pathfinding:** Pass `source_id` and `target_id` to find paths between two entities
in the graph (max depth 4).

**Arguments:**
| Parameter   | Type   | Required | Description |
|-------------|--------|----------|-------------|
| `query`     | string | no       | Text to extract entity references from |
| `entity_id` | string | no       | Direct node ID lookup |
| `source_id` | string | no       | Source entity for pathfinding |
| `target_id` | string | no       | Target entity for pathfinding |

**Response:**
```json
{
  "nodes": [
    {"id": "docker", "name": "Docker", "type": "entity", "attributes": {}},
    {"id": "omv8", "name": "OMV8", "type": "entity", "attributes": {}}
  ],
  "edges": [
    {"source_id": "docker", "target_id": "omv8", "relation": "runs_on", "attributes": {}}
  ],
  "paths": []
}
```

**Pathfinding response:**
```json
{
  "nodes": [],
  "edges": [],
  "paths": [
    [
      {"id": "serveralpha", "name": "ServerAlpha", "type": "entity"},
      {"id": "webapp", "name": "WebApp", "type": "entity"},
      {"id": "postgresql", "name": "PostgreSQL", "type": "entity"}
    ]
  ]
}
```

### route

Route a query through the 4-stage hybrid router (rules → embeddings → graph → LLM fallback).

Per ADR-005, each stage is evaluated in priority order:
1. **Rules:** Keyword-based exact match rules.
2. **Semantic:** Embedding similarity search via Qdrant.
3. **Graph:** Entity relation lookup in the knowledge graph.
4. **LLM fallback:** Placeholder for future LLM-based routing.

Returns the result from the highest-priority stage that produces meaningful output.

**Arguments:**
| Parameter        | Type   | Required | Default | Description |
|------------------|--------|----------|---------|-------------|
| `query`          | string | yes      | —       | Natural language query text |
| `top_k`          | int    | no       | 10      | Maximum semantic search results |
| `score_threshold` | float | no       | 0.0     | Minimum similarity score 0.0–1.0 |

**Response (rule match — stage 1):**
```json
{
  "stage": 1,
  "route": "rules",
  "rule_match": {
    "route": "sql",
    "rule_name": "ip_address_query",
    "matched_keyword": "ip of"
  }
}
```

**Response (semantic — stage 2):**
```json
{
  "stage": 2,
  "route": "semantic",
  "semantic_results": [
    {"id": "uuid", "score": 0.92, "payload": {"subject": "Docker", "predicate": "runs_on", "object": "OMV8"}}
  ],
  "total": 1
}
```

**Response (graph — stage 3):**
```json
{
  "stage": 3,
  "route": "graph",
  "graph_result": {
    "entities": [{"id": "docker", "name": "Docker", "type": "entity"}],
    "relations": [{"source_id": "docker", "target_id": "omv8", "relation": "runs_on"}],
    "paths": []
  }
}
```

**Response (LLM fallback — stage 4):**
```json
{
  "stage": 4,
  "route": "llm_fallback",
  "message": "LLM fallback not configured"
}
```

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
