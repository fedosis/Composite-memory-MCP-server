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

The server exposes five MCP tools:

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
