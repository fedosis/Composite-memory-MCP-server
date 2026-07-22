# Directory Submission Text

Prepared copy for downstream MCP/server directories. This file is a drafting aid only; it does not publish CMMS anywhere.

Publication status for `v0.11.0b1`: GitHub prerelease is published; PyPI, the official MCP Registry, Smithery, and Glama are not published. Use this copy only after explicit maintainer approval and channel-specific verification. Channel order is GitHub source/release → PyPI after verified package publication → official MCP Registry → Smithery/Glama → Hermes community documentation.

## Short description

Local-first composite memory MCP server for agent facts, beliefs, recall, provenance, and audits.

## Long description

Composite Memory MCP Server (CMMS) is a beta MCP stdio server for AI-agent memory. It stores explicit facts and extracted knowledge in a local SQLite-backed service with provenance receipts, lifecycle/audit metadata, optional vector retrieval, and an in-memory graph layer. It also includes an optional `[hermes]` native MemoryProvider integration for Hermes users who want in-process lifecycle hooks instead of MCP stdio. Hermes is not a base runtime dependency.

CMMS is designed for agents that need more structure than generic vector-only memory: keyword facts, semantic retrieval, graph/path lookup, belief tracking, confidence/lifecycle metadata, deterministic admission tags, and benchmark tooling are separate layers with explicit limitations.

## Feature bullets

- MCP stdio server: run with `memory-server serve`.
- 14 registered tools: `ping`, `search`, `get_context`, `remember`, `learn`, `semantic_search`, `graph_search`, `route`, `audit`, `metrics`, `set_belief`, `get_belief`, `resolve_conflict`, `reflect`.
- Local-first SQLite storage for facts, beliefs, receipts, lifecycle, and audit data.
- Provenance receipts with source, timestamp, confidence, and lifecycle status.
- Optional vector retrieval: LanceDB default code path, Qdrant configurable with `MEMORY_VECTOR_BACKEND=qdrant`.
- Graph lookup/pathfinding through the current in-memory `SimpleGraph` implementation.
- Hermes native MemoryProvider plugin for auto-recall/auto-retain lifecycle hooks.
- v0.11 deterministic admission gate: ephemeral/durable/important classification and TTL-aware pruning.
- v0.11 LongMemEval-S harness: raw/source/canonical retrieval scoring and deterministic built-in baseline.

## Install and run from source

```bash
git clone https://github.com/fedosis/Composite-memory-MCP-server.git
cd Composite-memory-MCP-server
python3.11 -m venv .venv
source .venv/bin/activate
pip install .
memory-server serve
```

## MCP config snippet

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

## Smithery-style fields

- Name: Composite Memory MCP Server
- Slug: composite-memory-mcp-server
- Repository: https://github.com/fedosis/Composite-memory-MCP-server
- License: no license file detected in the repository at the time of drafting
- Runtime: Python 3.11+
- Transport: stdio
- Package status: PyPI package not published for v0.11.0b1; use source install until package publication is verified
- Environment variables: optional `MEMORY_VECTOR_BACKEND=qdrant` to use Qdrant instead of the default LanceDB vector path

## Glama-style summary

CMMS is a beta local-first MCP memory server. It exposes tools for storing and searching facts, deriving knowledge from text, retrieving task context, semantic/vector search, graph search, routing, auditing, metrics, and belief management. It is best described as a composite memory stack rather than a vector-only memory server: facts, beliefs, receipts, vector search, graph lookup, admission tagging, and evaluation are separate layers.

## Limitations to include in listings

- Beta release for early integration testing.
- MCP transport implemented as stdio only; no remote HTTP/SSE endpoint in v0.11.0b1.
- PyPI package is not published at the time of drafting; do not advertise package-manager install until verified.
- LongMemEval-S dataset is external and not bundled.
- Some integration/e2e/benchmark workflows require external services or data.
- The LLM fallback route is a placeholder unless an LLM backend is configured in future code.

## Security/support boundaries

- Do not put API keys, passwords, or private memory contents in public issue reports or directory metadata.
- Registry/directory text is public; keep examples synthetic.
- Use the GitHub repository for issues and pull requests.
- External publication to MCP Registry, Smithery, Glama, PyPI, or other directories requires explicit maintainer action outside this task.
