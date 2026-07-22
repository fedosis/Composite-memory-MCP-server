# Composite Memory MCP Server (CMMS)

Composite Memory MCP Server (CMMS) is a beta local-first memory service for AI
agents. It exposes an MCP stdio server plus an optional Hermes native
MemoryProvider integration, with structured storage for facts, beliefs,
provenance receipts, audit data, optional vector retrieval, and graph lookup.

CMMS is meant for agents that need more than a generic vector-only memory layer:
explicit facts remain queryable in SQLite, receipts preserve provenance, vector
and graph retrieval are separate optional layers, and v0.11 adds deterministic
memory admission plus LongMemEval-S benchmark tooling.

[![Hermes Native Provider](https://img.shields.io/badge/Hermes-Native_MemoryProvider-blueviolet)](docs/INTEGRATION.md)
[![version](https://img.shields.io/badge/version-0.11.0b1-blue)]()
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue)]()

## Publication status and channel order

Current public source of truth: the GitHub repository and the GitHub prerelease
tag `v0.11.0b1`.

Publication boundary for `v0.11.0b1`:

1. **GitHub** — published source and prerelease artifacts.
2. **PyPI** — not published yet; add package-manager install instructions only
   after verifying package publication.
3. **Official MCP Registry** — not published; `server.json` is conservative
   metadata for future submission, not a registry listing.
4. **Smithery / Glama** — not published; `docs/DIRECTORY_SUBMISSIONS.md` is
   draft copy only.
5. **Hermes community** — optional integration documentation only; not a
   distribution channel for this release.

## First-run install

### Prerequisites

- **Python 3.11+**
- **SQLite** (bundled with Python)
- Optional: LanceDB/Qdrant extras for vector search; graph lookup uses the current SimpleGraph layer

### Install

```bash
# Clone the repository
git clone https://github.com/fedosis/Composite-memory-MCP-server.git
cd Composite-memory-MCP-server

# Create and activate a virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install base package exactly as a clean user would
pip install .

# Or install with all extras (recommended for development/full functionality)
pip install -e ".[dev]"
```

### Run the server

```bash
# Start the MCP server (stdio transport)
memory-server serve
```

### Use the tools

The server exposes MCP tools over stdio. For a local MCP client, point the
server command at the virtualenv executable:

```json
{
  "mcpServers": {
    "memory-server": {
      "command": "/absolute/path/to/memory-server/.venv/bin/memory-server",
      "args": ["serve"]
    }
  }
}
```

For a quick local smoke check, run `memory-server --help`. To exercise the MCP
tools, connect through an MCP-compatible client and call the `ping` tool.

### What you can do

| Tool | Purpose |
|------|---------|
| `ping` | Health check / connectivity test |
| `remember` | Store a fact with provenance |
| `search` | Keyword search over stored facts |
| `semantic_search` | Vector similarity search (LanceDB default; Qdrant optional) |
| `get_context` | Retrieve context for a task |
| `learn` | Extract knowledge from natural language |
| `graph_search` | Entity lookup + pathfinding through the current SimpleGraph layer |
| `route` | 4-stage hybrid router |
| `audit` | Memory health report |
| `metrics` | Prometheus metrics |
| `set_belief` / `get_belief` | Belief store management |
| `resolve_conflict` | Resolve belief conflicts |
| `reflect` | 6-mode belief store analysis |

### Run tests and lint

```bash
pytest tests/ -q
ruff check src/
```

---

## Hermes Integration

CMMS can optionally run as a native Hermes MemoryProvider plugin via the
`[hermes]` extra. Hermes is not a base runtime dependency of the MCP stdio
server. The implemented integration enables Hermes lifecycle hooks such as
auto-recall, auto-retain, and session-boundary flushing. It is a separate
in-process Hermes path, not a remote MCP transport.

```bash
pip install -e ".[hermes]"
memory-server install-hermes-plugin --hermes-home ~/.hermes/profiles/coder
hermes gateway restart
```

See [Hermes Integration Guide](docs/INTEGRATION.md) for supported Hermes setup
and v0.19 compatibility notes.

## Dev Setup

```bash
# Clone and enter
git clone https://github.com/fedosis/Composite-memory-MCP-server.git
cd Composite-memory-MCP-server

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
```

## Docs

- [Agent Discovery](docs/AGENT_DISCOVERY.md) — concise MCP/client/directory metadata, capabilities, transports, and limitations
- [Directory Submission Text](docs/DIRECTORY_SUBMISSIONS.md) — Smithery/Glama-ready draft copy; no external publication performed
- [ADR](docs/ADR.md) — Architecture Decision Records (13 ADRs)
- [Changelog](CHANGELOG.md) — Release notes and known limitations
- [Integration Guide](docs/INTEGRATION.md) — Hermes MemoryProvider plugin
- [Agent Spec](docs/agent-spec.md) — Implementation specification
- [Technical Design](docs/technical-design.md) — Tech stack + roadmap
- [Architecture](docs/architecture.md) — Mermaid architecture diagram
- [Usage](docs/USAGE.md) — Full usage reference
- [Metrics & Benchmarking](docs/metrics.md) — Metrics and benchmark framework
- [LongMemEval Harness](docs/longmemeval-harness.md) — v0.11 raw/source/canonical retrieval scoring
- [Comparative Analysis](docs/comparative-metrics.md) — Comparative analysis with ChromaDB/SQLite
- [Drift Matrix](docs/drift-matrix.md) — Contract audit
- [Contracts](contracts/) — JSON Schema 2020-12 tool contracts

## API Reference

The server exposes fourteen MCP tools in v0.11.0b1:

| # | Tool | v0.7 | Description |
|---|------|------|-------------|
| 1 | `ping` | — | Health check |
| 2 | `search` | — | Keyword search over facts |
| 3 | `remember` | — | Store a fact with provenance |
| 4 | `get_context` | — | Retrieve context for a task |
| 5 | `semantic_search` | — | Vector similarity search |
| 6 | `learn` | ✓ | Extract knowledge; optionally extract beliefs |
| 7 | `graph_search` | — | Entity lookup + pathfinding |
| 8 | `route` | — | 4-stage hybrid router |
| 9 | `audit` | — | Memory health report |
| 10 | `metrics` | — | Prometheus metrics |
| 11 | `set_belief` | ✓ | Create, reinforce, or supersede a belief |
| 12 | `get_belief` | ✓ | Search beliefs with filters |
| 13 | `resolve_conflict` | ✓ | Resolve belief conflicts (manual + auto) |
| 14 | `reflect` | ✓ | 6-mode belief store analysis |

### v0.7 New Features

- **Belief Model** — Propositional knowledge with confidence, evidence provenance,
  tags, lifecycle states (active, superseded, contradicted, discarded), and
  version tracking.
- **Reflection** — The `reflect()` tool provides 6 analysis modes: overview,
  contradictions, decay, topics, evidence_audit, and confidence histogram.
- **Learn-to-Belief** — `learn(extract_beliefs=True)` automatically extracts
  beliefs from natural language text with evidence linked to extracted facts.
- **Conflict Resolution** — `resolve_conflict()` supports manual resolution
  (keep_a, keep_b, merge, discard_both) and auto-resolution via confidence
  threshold rules.
- **Reinforcement** — `set_belief()` automatically reinforces existing beliefs
  with the same proposition via weighted average confidence.
- **Evidence Audit** — `reflect(mode="evidence_audit")` reports evidence quality
  across all beliefs, detecting beliefs with missing or zero-weight evidence.
- **Decay Analysis** — `reflect(mode="decay")` forecasts lifecycle transitions
  (stale, archived, forgotten) within the next 7 days using belief-specific TTL.

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
2. **Semantic:** Embedding similarity search via the configured vector backend.
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

### audit

Run a structured memory audit covering consistency, orphan detection, confidence analysis, lifecycle validation, and index drift detection.

Supports focused sub-audits via the `audit_type` parameter, or a comprehensive `"full"` report.

**Arguments:**
| Parameter    | Type   | Required | Default | Description |
|-------------|--------|----------|---------|-------------|
| `audit_type` | string | no       | "full"  | One of `"full"`, `"consistency"`, `"orphans"`, `"confidence"` |

**Audit checks (full mode):**

| # | Check | Description |
|---|-------|-------------|
| 1 | **Orphan records** | Items in the validator store with no corresponding `MemoryReceipt` |
| 2 | **Missing receipts** | Validator entries referencing receipts that don't exist |
| 3 | **Lifecycle violations** | Items in an invalid lifecycle state or that skipped a required transition |
| 4 | **Confidence issues** | Confidence scores that conflict with current lifecycle state |
| 5 | **SQL/vector drift** | Consistency gaps between SQLite fact storage and the vector index |
| 6 | **SQL/graph drift** | Consistency gaps between SQLite fact storage and the knowledge graph |

When `audit_type` is set to a specific sub-audit (`"consistency"`, `"orphans"`, or `"confidence"`), only the corresponding analysis is returned:

- **consistency** — Checks for deprecated facts with active receipts, zero-confidence facts not marked stale/archived/forgotten, and stale facts with full confidence.
- **orphans** — Scans the graph for nodes with no incoming edges (unlinked facts).
- **confidence** — Analyzes the confidence score distribution, bucket counts, and lists low-confidence items (< 0.3).

**Response:**
```json
{
  "audit_type": "full",
  "warnings": [],
  "errors": [
    "Found 2 items without MemoryReceipt: fact_001, fact_002"
  ],
  "stats": {
    "confidence": {
      "total": 150,
      "buckets": {"0.0-0.3": 5, "0.3-0.5": 20, "0.5-0.7": 45, "0.7-0.85": 50, "0.85-1.0": 30},
      "low_confidence": ["fact_001", "fact_002"]
    },
    "sql_vector_drift": {"drift_pct": 0.0, "sql_count": 150, "vector_count": 150},
    "sql_graph_drift": {"drift_pct": 2.0, "sql_count": 150, "graph_count": 147}
  }
}
```

### metrics

Return a Prometheus-formatted snapshot of all observability metrics. Compatible with any Prometheus scraper or `curl | grep` workflows.

**Arguments:** None

**Response:** Plaintext Prometheus exposition format:
```
# HELP tool_calls_total Total MCP tool calls
# TYPE tool_calls_total counter
tool_calls_total{tool="search",status="success"} 42.0
tool_calls_total{tool="remember",status="success"} 17.0
# HELP search_latency_ms Search latency in ms
# TYPE search_latency_ms histogram
search_latency_ms_bucket{le="1.0"} 0.0
search_latency_ms_bucket{le="5.0"} 5.0
...
```

## Stack

Python 3.11+, MCP SDK, Pydantic, SQLAlchemy, LanceDB/Qdrant vector providers,
SimpleGraph, GitPython, Prometheus Client, OpenTelemetry

## Storage

The server uses a multi-tier storage architecture with SQLite/FTS5 as the primary durable and keyword-search store, backed by optional vector indexes (LanceDB by default for semantic search, Qdrant optional via `MEMORY_VECTOR_BACKEND=qdrant`) and the current in-memory SimpleGraph graph layer. Neo4j is declared only as a future/optional graph dependency and is not wired into the v0.11 runtime.

### SQLite with WAL Mode

The primary fact store uses SQLite in **WAL (Write-Ahead Logging)** mode for concurrent read performance during background indexing operations:

```sql
PRAGMA journal_mode=WAL;
```

WAL mode allows simultaneous reads while a single writer is active, which is critical for the outbox pattern and background indexing without blocking the MCP tool handler.

### Alembic Migrations

Database schema migrations are managed via Alembic. To apply pending migrations:

```bash
alembic upgrade head
```

Migrations live in `migrations/` and are automatically tested in CI (upgrade then downgrade -1).

### FTS5 Full-Text Search

The `facts_fts` virtual table provides fast keyword search across fact content:

```sql
CREATE VIRTUAL TABLE facts_fts USING fts5(
  subject, predicate, object, content='facts', content_rowid='id'
);
```

FTS5 enables the `search` tool's full-text capabilities with ranking, prefix queries, and snippet generation.

### Outbox Pattern for Reliable Indexing

Facts are written to the SQLite store first, then queued through an **outbox pattern** for background indexing. In v0.11.0b1 the server keeps the SQLite write durable even if optional vector indexing or in-memory SimpleGraph indexing is unavailable:

1. Fact is inserted into SQLite (single write transaction)
2. An outbox record is created in a dedicated table or queue
3. A background worker picks up outbox entries and indexes available secondary paths (Qdrant only when that provider/embedder has been initialized in the worker path, plus SimpleGraph relations)
4. On success, the outbox record is marked as processed
5. On failure, the outbox record is retried — the fact is never lost

This ensures that even if vector or graph indexing fails, the fact data is durably stored and can be re-indexed on the next retry.

## Lifecycle

Every fact and extracted memory item passes through a 6-stage lifecycle.
The lifecycle determines how a fact moves from raw ingestion to trusted knowledge and eventual retirement.

### Lifecycle States (v0.6)

| State        | Description |
|-------------|-------------|
| `candidate` | Initial state after ingestion via `remember()` or `learn()`. Low confidence (0.5 default). |
| `validated` | Confidence >= 0.7 — fact has passed an internal quality check. |
| `active`    | Confidence >= 0.85 AND corroboration >= 2 sources. High-reliability knowledge. |
| `stale`     | Confidence has decayed below threshold — fact may be outdated. |
| `archived`  | Stale fact moved to cold storage. Retained for audit but excluded from active queries. |
| `forgotten` | Permanently removed from indexes. Only receipt/provenance metadata preserved. |

### Lifecycle Flow

```
candidate → validated → active → stale → archived → forgotten
```

Transitions are **forward-only** — once promoted, an item can only move forward through the lifecycle. Backward compatibility maps the v0.5 states `"trusted"` → `"active"` and `"deprecated"` → `"stale"`.

### Confidence Scoring

Confidence scores (0.0–1.0) are computed heuristically from:

- **Source reliability**: `verified` (0.9), `admin` (0.85), `inferred` (0.7),
  `extracted` (0.6), `unknown` (0.3)
- **Age decay**: Exponential decay over TTL (default 90 days), minimum 0.3
- **Corroboration boost**: +0.05 for 2 sources, +0.10 for 3+
- **Conflict penalty**: -0.10 for 1 conflict, -0.20 for 2+

### Decay Engine

The `DecayEngine` applies time-based decay to all stored facts:
- TTL-based confidence reduction (default 90 days)
- Archive threshold: facts below 0.3 confidence are flagged for archiving
- Runs on-demand at audit time, not as a background process

### Auto-Indexing

When a fact is stored via `remember()` or `learn()`, the server automatically:

1. **Embeds** the fact text using SentenceTransformer (`all-MiniLM-L6-v2`) when an embedder is initialized
2. **Upserts** the embedding into the initialized outbox vector path (currently Qdrant when available; `semantic_search` itself uses LanceDB by default or Qdrant if configured)
3. **Syncs** to the in-memory SimpleGraph knowledge graph (creates entity nodes + relation edges)

This is best-effort — failures during auto-indexing never crash the caller.

## Observability

The server exposes structured observability through Prometheus metrics and OpenTelemetry instrumentation.

### Prometheus Metrics

A dedicated `/metrics` tool returns Prometheus-formatted output on demand. The `MetricsCollector` singleton tracks key performance indicators across all tool operations.

**Key metrics:**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `tool_calls_total` | Counter | `tool`, `status` | Total MCP tool calls by name and success/error status |
| `tool_error_total` | Counter | `tool` | Total errors per tool |
| `search_latency_ms` | Histogram | — | Search latency buckets (1–500 ms) |
| `semantic_search_latency_ms` | Histogram | — | Semantic search latency buckets (10–1000 ms) |
| `remember_latency_ms` | Histogram | — | Remember latency buckets (5–500 ms) |
| `derived_index_drift` | Gauge | — | SQL/vector index drift count (updated on each audit) |
| `reindex_repair_total` | Counter | — | Reindex repairs triggered |
| `sqlite_busy_events_total` | Counter | — | SQLite WAL busy events |

### OpenTelemetry Hooks

Every tool call is wrapped with OpenTelemetry tracing:

```python
tracer = trace.get_tracer(__name__)
```

Spans are created per tool invocation, capturing duration and status. The `tool_call()` context manager on `MetricsCollector` automatically records:
- Start time and duration
- Success/error status
- Exception propagation for error counting

## Development

### Makefile Targets

| Target      | Description |
|------------|-------------|
| `make install` | Install package with dev dependencies (`pip install -e ".[dev]"`) |
| `make test`    | Run unit tests (`pytest tests/ -q`) |
| `make lint`    | Run Ruff linter (`ruff check src/`) |
| `make all`     | Run lint + test sequentially |
| `make migrate` | Apply Alembic migrations (`alembic upgrade head`) |
| `make build`   | Build Python package (`python3 -m build`) |

### CI/CD

GitHub Actions (`.github/workflows/ci.yml`) runs on push/PR to `main`:

| Job | What it runs |
|-----|-------------|
| **lint** | `ruff check src/` |
| **unit-tests** | `pytest tests/ -q` |
| **integration-tests** | `pytest tests/ -q -k "integration or e2e or benchmark"` |
| **contract-tests** | JSON Schema validation + `pytest tests/ -q -k "schema or contract"` |
| **migration-tests** | `alembic upgrade head && alembic downgrade -1` |

## Roadmap

| Phase | Milestone |
|-------|-----------|
| v0.1a | MCP API + SQLite provider + get_context/search/remember |
| v0.2  | Qdrant + embeddings + semantic router |
| v0.3  | LLM extractors + learn() |
| v0.4  | Graph DB + entity relations |
| v0.5  | Confidence engine + validation + decay + auditor + auto-indexing |
| v0.6  | 6-state lifecycle, audit tool, metrics/observability, outbox indexing, storage docs, CI/CD |

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

## Authors

See [AUTHORS.md](AUTHORS.md) for contributor and attribution information.
