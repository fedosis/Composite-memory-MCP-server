# Agent Discovery: Composite Memory MCP Server

Composite Memory MCP Server (CMMS) is a beta local-first memory server for MCP-capable AI agents. It stores explicit facts, derived knowledge, beliefs, receipts, and audit metadata in a local SQLite-backed service, with optional vector and graph retrieval layers.

This document is intentionally concise and machine-readable by agents and directory maintainers. The README remains the human entry point.

## Current release

- Version: `0.11.0b1`
- Stage: beta / early integration testing
- Source repository: `https://github.com/fedosis/Composite-memory-MCP-server`
- GitHub release: `https://github.com/fedosis/Composite-memory-MCP-server/releases/tag/v0.11.0b1`
- PyPI package: not published at the time this metadata was written; install from source or an explicit release artifact only.
- Official MCP Registry: not published; root `server.json` is schema-valid draft metadata only.
- Smithery / Glama: not published; submission copy is drafted in `docs/DIRECTORY_SUBMISSIONS.md` only.

Publication channel order for public docs is: GitHub source/release first, PyPI only after verified package publication, official MCP Registry only after accepted registry submission, Smithery/Glama only after explicit directory publication, then Hermes community documentation for optional Hermes users.

## Implemented transports and integration paths

| Path | Status | Notes |
|------|--------|-------|
| MCP stdio | Implemented | `memory-server serve` runs `mcp.run(transport="stdio")`. |
| Hermes native MemoryProvider | Implemented | `memory-server install-hermes-plugin` registers an in-process Hermes provider. This is not an MCP transport. |
| Remote HTTP/SSE MCP server | Not implemented in v0.11.0b1 | Do not advertise `remotes` in registry metadata until a remote endpoint exists. |

## Install from source

```bash
git clone https://github.com/fedosis/Composite-memory-MCP-server.git
cd Composite-memory-MCP-server
python3.11 -m venv .venv
source .venv/bin/activate
pip install .
memory-server serve
```

Development/full optional install:

```bash
pip install -e ".[dev]"
```

Optional extras are declared for Qdrant, LanceDB, Hermes integration, sentence-transformers, and OpenAI clients. The base package uses SQLite/FTS5 and MCP stdio. The `[hermes]` extra is an optional integration path, not a base runtime dependency.

## MCP client configuration

```json
{
  "mcpServers": {
    "composite-memory": {
      "command": "/absolute/path/to/Composite-memory-MCP-server/.venv/bin/memory-server",
      "args": ["serve"]
    }
  }
}
```

## Hermes native provider

```bash
pip install -e ".[hermes]"
memory-server install-hermes-plugin --hermes-home ~/.hermes/profiles/coder
# Restart Hermes separately after reviewing config changes.
```

Hermes native mode provides lifecycle hooks (`prefetch`, `sync_turn`, session boundary flush) through `memory_server.plugins.hermes.provider.HermesProvider`. It is optional, documented in `docs/INTEGRATION.md`, separate from MCP stdio, and should not be represented as a registry `transport`.

## Tool inventory

CMMS v0.11.0b1 registers these 14 MCP tools in `src/memory_server/server.py`:

| Tool | Purpose |
|------|---------|
| `ping` | Connectivity check. |
| `search` | Keyword/FTS-backed fact search with subject and predicate filters. |
| `get_context` | Retrieve relevant facts for a task, optional subject filter, and max result count. |
| `remember` | Store an explicit fact with confidence and provenance receipt. |
| `learn` | Extract facts, decisions, skills, and optionally beliefs from text. |
| `semantic_search` | Vector similarity retrieval through the configured vector backend. |
| `graph_search` | Entity lookup, neighbor traversal, and pathfinding through the graph layer. |
| `route` | Four-stage routing: rules, semantic, graph, then LLM fallback placeholder. |
| `audit` | Memory health/audit report. |
| `metrics` | Prometheus-formatted metrics snapshot. |
| `set_belief` | Create, reinforce, or supersede a belief proposition. |
| `get_belief` | Search/filter stored beliefs. |
| `resolve_conflict` | Resolve belief conflicts manually or by deterministic confidence threshold. |
| `reflect` | Analyze the belief store in overview, contradictions, decay, topics, evidence audit, or confidence modes. |

## Architecture summary

- Local-first durable store: SQLite via SQLAlchemy async/aiosqlite.
- Provenance: memory receipts track source, confidence, timestamp, and lifecycle state.
- Composite memory: facts, decisions, skills, beliefs, vector retrieval, graph relations, metrics, and audit layers are kept distinct instead of using a single vector-only store.
- Vector backend: LanceDB is the default code path in server mode; Qdrant remains optional/configurable with `MEMORY_VECTOR_BACKEND=qdrant`.
- Graph backend: current server graph path uses the in-memory `SimpleGraph` implementation.
- Indexing: remember/learn write durably to SQL and enqueue best-effort secondary indexing work; current outbox vector indexing is Qdrant-path-specific when initialized, while semantic search can use the configured vector backend.
- Admission/retention: v0.11 adds deterministic memory admission tags (`EPHEMERAL`, `DURABLE`, `IMPORTANT`) and TTL-aware pruning.
- Benchmarking: v0.11 adds a lineage-aware LongMemEval-S harness with `raw`, `source`, and `canonical` scoring targets plus a deterministic built-in lexical baseline.

## Limitations and boundaries

- Beta release: suitable for early integration testing, not a compatibility guarantee for all clients.
- PyPI package metadata is intentionally omitted from `server.json` until package publication is verified.
- LongMemEval-S data is external and not bundled; users must provide the cleaned dataset.
- Remote HTTP/SSE MCP endpoints are not implemented in v0.11.0b1.
- Some integration/e2e/benchmark tests require external services such as Qdrant or benchmark data.
- The LLM fallback route exists as a routing stage placeholder; do not claim a configured LLM fallback unless one is added.
- Comparisons should be framed against generic vector-only memory architectures, not as unverified claims about specific vendors.

## Support, contribution, and security boundaries

- Use GitHub issues/PRs in the public repository for support and contributions.
- Do not include secrets in memory values, docs examples, issue reports, or registry metadata.
- Treat registry metadata as public and cacheable.
- Do not publish to MCP Registry, Smithery, Glama, PyPI, or other directories without repository owner approval and package/namespace verification.
