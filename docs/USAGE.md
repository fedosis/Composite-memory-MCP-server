# Composite Memory MCP Server — Usage Reference

## Quick Start

```bash
# Setup
python3.11 -m venv .venv
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
| confidence | float | no | 1.0 |
| source | str | no | "user" |

```
→ remember(subject="Docker", predicate="runs_on", object="OMV8", confidence=0.95)
← {"receipt": {"id": "uuid...", "timestamp": "2026-07-07T...", 
     "verification_status": "candidate"}, "fact": {...}}
```

Queues best-effort secondary indexing after the durable SQLite write.

### search
Keyword search over facts (SQLite FTS5 first, with LIKE fallback on
subject/predicate/object).

| Param | Type | Required |
|-------|------|----------|
| query | str | yes |
| subject | str | no |
| predicate | str | no |

```
→ search(query="Docker")
← {"results": [{"subject": "Docker", "predicate": "runs_on", "object": "OMV8", ...}], "total": 1}
```

### semantic_search
Vector similarity search via the configured vector backend + sentence-transformers.

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
Extract knowledge from raw text. Runs all extractors, stores results, and queues best-effort secondary indexing.

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
| subject | str | no |
| max_results | int | no |

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

## Belief Tools (v0.7)

Belief tools allow the agent to store, search, resolve, and reflect on
propositional knowledge extracted from interactions. The belief subsystem
provides a structured model for what the agent "knows" with quantifiable
confidence, evidence provenance, and lifecycle management.

### set_belief

Create, reinforce, or supersede a belief proposition with evidence.

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `proposition` | string | yes | — | The belief proposition text |
| `confidence` | float | no | 0.5 | Confidence score 0.0–1.0 |
| `sources` | string | no | `"[]"` | JSON array of evidence sources: `[{"source_type": "fact", "source_id": "uuid", "weight": 0.9}]` |
| `tags` | string | no | `"[]"` | JSON array of tag strings (stored as JSON string for now, see note below) |
| `source` | string | no | `"system"` | Source identifier |
| `replace_belief_id` | string | no | `""` | If set, supersede the referenced belief and link this new one |

> **Note on `tags` type:** The MCP contract declares `tags` as an array type,
> but the server currently accepts it as a JSON-encoded string for consistency
> with other tools. Pass tags as a JSON array string: `'["tag1", "tag2"]'`.

```json
→ set_belief(
    proposition="Docker is the container runtime on OMV8",
    confidence=0.9,
    sources='[{"source_type":"fact","source_id":"f1","weight":0.9}]',
    tags='["docker","infra"]'
)
← {
  "belief": {"id": "uuid", "proposition": "Docker is the container runtime on OMV8", ...},
  "receipt": {"id": "uuid", "memory_type": "belief", ...},
  "superseded": null
}
```

**Reinforcement:** If a belief with the same proposition (case-insensitive)
already exists in `active` state, `set_belief` averages the confidence scores
and updates `reinforced_at` instead of creating a duplicate.

**Supersede:** When `replace_belief_id` is set, the referenced belief is
transitioned to `superseded` and the new belief inherits `version + 1`.

### get_belief

Search beliefs with optional filters.

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `proposition` | string | no | `""` | Search proposition text (FTS5 full-text search) |
| `lifecycle_state` | string | no | `"active"` | Filter by lifecycle state (pass `""` for all) |
| `min_confidence` | float | no | 0.0 | Minimum confidence threshold |
| `tags` | string | no | `""` | JSON array of tag strings to filter by |
| `source` | string | no | `""` | Filter by source identifier |
| `creator` | string | no | `""` | Filter by creator identifier |
| `source_id` | string | no | `""` | Filter by source_id in the belief's evidence |
| `limit` | int | no | 10 | Maximum number of results (max 100) |

```json
→ get_belief(proposition="Docker", lifecycle_state="active", limit=5)
← {
  "total": 1,
  "beliefs": [
    {"id": "uuid", "proposition": "Docker is the container runtime on OMV8", "confidence": 0.9, ...}
  ],
  "query": {"proposition": "Docker", "lifecycle_state": "active", ...}
}
```

### resolve_conflict

Resolve a conflict between two beliefs using a transition matrix.

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `belief_a_id` | string | yes | — | UUID of the first belief |
| `belief_b_id` | string | yes | — | UUID of the second belief |
| `resolution` | string | yes | — | Strategy: `keep_a`, `keep_b`, `merge`, `discard_both` |
| `new_proposition` | string | no | `""` | Proposition for new merged belief (required for `merge`) |
| `auto_resolve` | bool | no | `false` | When True, auto-resolve by confidence threshold (never uses `discarded` state) |

**Auto-resolve rules:**
- Confidence diff > 0.5 → lower-confidence belief → `superseded`
- Confidence diff ≤ 0.5 → both beliefs → `contradicted`

```json
→ resolve_conflict(
    belief_a_id="uuid-a",
    belief_b_id="uuid-b",
    resolution="merge",
    new_proposition="Both tools have trade-offs"
)
← {
  "belief_a": {...}, "belief_b": {...},
  "resolution": "merge",
  "created": {"id": "uuid-merged", "proposition": "Both tools have trade-offs"},
  "events": [...],
  "receipt": {...}
}
```

### reflect

Analyse the belief store and produce structured insights. Provides 6
analysis modes.

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `mode` | string | no | `"overview"` | Mode: `overview`, `contradictions`, `decay`, `topics`, `evidence_audit`, `confidence` |
| `topic` | string | no | `""` | Optional topic/tag filter |
| `min_confidence` | float | no | 0.0 | Minimum confidence threshold |
| `limit` | int | no | 50 | Max beliefs to analyse (0 = all) |

**Modes:**

- **overview** — High-level summary with counts, confidence distribution,
  lifecycle breakdown, conflict stats, and decaying-next-7d estimates.
- **contradictions** — Find semantically conflicting beliefs using keyword
  heuristic (token overlap + opposite sentiment) and confidence-weighted
  detection.
- **decay** — Analyse which beliefs are approaching lifecycle transitions
  (stale, archived, forgotten) within 7 days.
- **topics** — Cluster beliefs by tags with counts, avg confidence, and
  stale breakdown.
- **evidence_audit** — Audit evidence quality: with/without evidence counts,
  avg evidence per belief, zero-weight entries.
- **confidence** — Detailed confidence histogram with sorted belief list
  and evidence counts.

```json
→ reflect(mode="overview")
← {
  "mode": "overview",
  "total_beliefs": 5,
  "by_lifecycle_state": {"active": 4, "superseded": 1},
  "confidence": {"average": 0.74, "high_0.8_1.0": 2, ...},
  "contradiction_count": 2,
  "conflicts": {"total": 2, "unresolved": 1, "auto_resolvable": 1, "age_hours_max": 48.5},
  "stale_count": 0,
  "decaying_next_7d": 0,
  "oldest_belief_days": 30.2,
  ...
}
```

### learn (updated)

Extract and store facts, decisions, skills, and optionally beliefs from text.

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `text` | string | yes | — | Natural language text to extract knowledge from |
| `source` | string | no | `"user"` | Source identifier |
| `extract_beliefs` | bool | no | `false` | When True, also extract and store beliefs |
| `min_belief_confidence` | float | no | 0.6 | Minimum confidence to create a belief |

When `extract_beliefs=True`, `learn()` runs the BeliefExtractor after the
main extraction transaction. Extracted beliefs are created (or reinforced
if they match existing ones) with evidence linked to the extracted facts.

```json
→ learn(
    text="Server IP 192.168.1.100 is assigned to NAS. We decided to use Caddy.",
    extract_beliefs=True
)
← {
  "facts": [...],
  "decisions": [...],
  "beliefs": [
    {
      "belief": {"id": "uuid", "proposition": "192.168.1.100 is the NAS IP", ...},
      "extracted": true,
      "reinforced": false
    }
  ],
  ...
}
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
[status=archived, removed from secondary indexes where supported]
```

## File Structure

```
~/memory-server/
├── src/memory_server/
│   ├── __init__.py
│   ├── server.py        # FastMCP server + 14 tools
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
