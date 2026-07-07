# Composite Memory MCP Server — Usage Reference

## Quick Start

```bash
# Setup
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run server
memory-server serve

# In another terminal — test
source .venv/bin/activate
python3 -c "
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import asyncio, json

async def test():
    params = StdioServerParameters(command='memory-server', args=['serve'])
    async with stdio_client(params) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            r = await s.call_tool('ping', {})
            print(r.content[0].text)

asyncio.run(test())
"
```

## MCP Tools Reference

### ping
Connectivity check.

```
→ ping()
← {"status": "ok"}
```

### remember
Store a fact with provenance.

| Param | Type | Required | Default |
|-------|------|----------|---------|
| subject | str | yes | — |
| predicate | str | yes | — |
| object | str | yes | — |
| confidence | float | no | 0.8 |
| source | str | no | "user" |

```
→ remember(subject="Docker", predicate="runs_on", object="OMV8", confidence=0.95)
← {"receipt": {"id": "uuid...", "timestamp": "2026-07-07T...", 
     "verification_status": "candidate"}, "fact": {...}}
```

Auto-indexes into Qdrant + graph on store.

### search
Keyword search over facts (SQL LIKE on subject/predicate/object).

| Param | Type | Required |
|-------|------|----------|
| query | str | yes |
| subject | str | no |
| predicate | str | no |
| object | str | no |

```
→ search(query="Docker")
← {"results": [{"subject": "Docker", "predicate": "runs_on", "object": "OMV8", ...}], "total": 1}
```

### semantic_search
Vector similarity search via Qdrant + sentence-transformers.

| Param | Type | Required | Default |
|-------|------|----------|---------|
| query | str | yes | — |
| top_k | int | no | 5 |
| score_threshold | float | no | 0.0 |

```
→ semantic_search(query="container platform", top_k=3)
← {"results": [{"content": "Docker runs_on OMV8", "score": 0.87, "source": "fact"}, ...]}
```

### learn
Extract knowledge from raw text. Runs all extractors, stores results, auto-indexes.

| Param | Type | Required | Default |
|-------|------|----------|---------|
| text | str | yes | — |
| source | str | no | "learn" |

```
→ learn(text="Server IP 192.168.1.100 is assigned to NAS. We decided to use Caddy.")
← {"facts": [...], "decisions": [...], "skills": [...], "facts_count": 1, "decisions_count": 1, ...}
```

### get_context
Retrieve relevant context for a task. Searches facts + graph.

| Param | Type | Required |
|-------|------|----------|
| task | str | yes |
| agent | str | no |

```
→ get_context(task="deploy database")
← {"facts": [...], "decisions": [...], "entities": [...], "warnings": []}
```

### graph_search
Entity lookup, neighbor traversal, and pathfinding.

| Param | Type | Required |
|-------|------|----------|
| query | str | no |
| entity_id | str | no |
| source_id | str | no |
| target_id | str | no |

```
→ graph_search(query="Docker")
← {"nodes": [{"id": "Docker", "type": "software", ...}], 
    "edges": [{"source": "Docker", "target": "OMV8", "relation": "runs_on"}], 
    "paths": []}

→ graph_search(source_id="Docker", target_id="192.168.1.100")
← {"paths": [["Docker", "runs_on", "OMV8", "ip_address", "192.168.1.100"]]}
```

### route
Hybrid 4-stage router: RulesEngine → SemanticRouter → GraphRouter → LLM fallback.

| Param | Type | Required |
|-------|------|----------|
| query | str | yes |

```
→ route(query="deploy PostgreSQL")
← {"stage": "graph_router", "result": {...}, "confidence": 0.75, 
    "fallthrough_path": ["rules", "semantic", "graph"]}
```

### audit
Memory health report.

| Param | Type | Required | Default |
|-------|------|----------|---------|
| audit_type | str | no | "full" |

Values: "consistency", "orphans", "confidence", "full"

```
→ audit(audit_type="full")
← {"warnings": [...], "errors": [...], 
    "stats": {"total_facts": 10, "total_decisions": 3, "total_skills": 1, 
              "total_entities": 8, "orphan_nodes": 0, "avg_confidence": 0.82}}
```

## Configuration

Server is configured via constructor params in `memory_server.server.create_server()`:
- `sqlite_path`: path to SQLite DB (default: `memory.db`)
- `qdrant_url`: Qdrant HTTP URL (default: `None` = in-memory)
- `qdrant_api_key`: Qdrant API key (default: `None`)
- `embedding_model`: sentence-transformers model name (default: `all-MiniLM-L6-v2`)
- Use `None` for any of these to use in-memory/test defaults

## Data Lifecycle

```
remember()/learn()
    ↓
[confidence=0.8, status=candidate]
    ↓ (automated or manual)
validate(fact_id) — requires confidence ≥ 0.7
    ↓
[status=validated]
    ↓
trust(fact_id) — requires confidence ≥ 0.85 + corroboration ≥ 2
    ↓
[status=trusted]
    ↓ (conflict detected)
deprecate(fact_id)
    ↓
[status=deprecated]
    ↓ (TTL expired)
archive(fact_id)
    ↓
[status=archived, removed from Qdrant + graph]
```

## File Structure

```
~/memory-server/
├── src/memory_server/
│   ├── __init__.py
│   ├── server.py        # FastMCP server + 9 tools
│   ├── cli.py            # Typer CLI entry point
│   ├── models/          # Pydantic data models
│   │   ├── entity.py, fact.py, decision.py, skill.py, receipt.py
│   ├── providers/       # Storage backends
│   │   ├── sqlite_provider.py
│   │   ├── qdrant_provider.py
│   │   ├── embedding_provider.py
│   │   └── graph_provider.py
│   ├── router/          # Routing layers
│   │   ├── rules.py, embedding_router.py, graph_router.py, hybrid_router.py
│   ├── api/             # MCP tool implementations
│   │   ├── remember.py, search.py, learn.py, get_context.py
│   ├── extractors/      # Knowledge extractors
│   │   ├── fact_extractor.py, decision_extractor.py, skill_extractor.py
│   └── evaluation/      # Memory lifecycle
│       ├── confidence.py, validator.py, decay.py, auditor.py
├── docs/
│   ├── ADR.md, architecture.md, QUICKSTART.md, metrics.md
├── specs/
├── tests/
│   ├── test_ping.py, test_search.py, test_remember.py ...
│   └── test_v05_integration.py
├── pyproject.toml
├── README.md
└── PLAN.md
```
