# Composite Memory MCP Server — Architecture Overview

> **Current status:** updated for `v0.11.0b1`. GitHub prerelease is published;
> PyPI, the official MCP Registry, Smithery, and Glama are not published. This
> document describes the current MCP stdio runtime plus optional `[hermes]`
> integration; Neo4j is not wired into the v0.11 runtime.

```mermaid
flowchart TB
    subgraph Clients["AI Agents / MCP Clients"]
        HC["Hermes Agent"]
        CC["Claude Code"]
        OC["OpenClaw"]
    end

    subgraph CMMS["Composite Memory MCP Server"]
        direction TB
        
        MCP["MCP Interface\n(stdio transport)"]
        
        subgraph Tools["MCP Tools (14)"]
            PING["ping 🔊"]
            REM["remember 💾"]
            SRCH["search 🔍"]
            SEM["semantic_search 🧠"]
            LEARN["learn 📖"]
            CTX["get_context 📋"]
            GRPH["graph_search 🕸️"]
            ROUTE["route 🧭"]
            AUDIT["audit 📊"]
            METRICS["metrics 📈"]
            BELIEF["set_belief / get_belief"]
            CONFLICT["resolve_conflict"]
            REFLECT["reflect"]
        end

        subgraph Router["Hybrid Router (ADR-005)"]
            direction LR
            R1["① RulesEngine\nexact match"]
            R2["② SemanticRouter\nembeddings → LanceDB default / Qdrant optional"]
            R3["③ GraphRouter\nentity relations"]
            R4["④ LLM fallback\n(placeholder)"]
        end

        subgraph Providers["Storage Backends"]
            SQL["SQLite\n(Facts, Decisions,\nSkills, Receipts)"]
            VECTOR["LanceDB default /\nQdrant optional\n(Vectors, Semantic Search)"]
            GRAPH["SimpleGraph\n(in-memory Python dict+set,\nEntity Relations)"]
        end

        subgraph Extractors["Knowledge Extractors (v0.3)"]
            FE["FactExtractor\nSPO triples"]
            DE["DecisionExtractor\ncontext + choice"]
            SE["SkillExtractor\nsteps + constraints"]
        end

        subgraph Eval["Evaluation Engine (v0.5+)"]
            CONF["ConfidenceEngine\nsource + age +\ncorroboration"]
            VAL["Validator\ncandidate→validated\n→trusted→archived"]
            DECAY["DecayEngine\nper-type TTL\nexponential decay"]
            AUD["MemoryAuditor\nconsistency +\norphans + stats"]
        end

        subgraph AutoIndex["Outbox Indexing Bridge (v0.11)"]
            AI_REM["remember() →\nSQLite + outbox"]
            AI_LEARN["learn() →\nSQLite + outbox"]
            AI_DECAY["decay/archive →\nSQLite lifecycle state"]
        end
    end

    Clients -->|MCP stdio| MCP
    MCP --> Tools
    
    REM --> SQL
    REM --> AI_REM
    AI_REM --> VECTOR
    AI_REM --> GRAPH
    
    SRCH --> SQL
    SEM --> R2
    LEARN --> Extractors
    LEARN --> AI_LEARN
    AI_LEARN --> VECTOR
    AI_LEARN --> GRAPH
    CTX --> SQL
    CTX --> GRAPH
    GRPH --> GRAPH
    ROUTE --> Router
    
    R1 --> SQL
    R2 --> VECTOR
    R3 --> GRAPH
    R4 -->|"not configured"| ROUTE
    
    Extractors --> SQL
    Extractors --> Eval
    Eval --> SQL
    Eval --> DECAY
    DECAY --> AI_DECAY
```

## Data Model (ADR-006)

```
Entity       → {id, type, name, attributes}
Fact         → {subject, predicate, object, confidence, source}
Decision     → {context, choice, rejected_alternatives, reason, source}
Skill        → {name, version, purpose, steps, constraints, validation}
MemoryReceipt→ {id, memory_type, source, created_by, timestamp, confidence,
                verification_status, history}
```

## Verification Lifecycle (current compatibility view)

```
candidate ──→ validated ──→ active ──→ stale ──→ archived ──→ forgotten
  (new)    (conf≥0.7)  (conf≥0.85   (decayed)     (cold)       (index removal)
                         + corr≥2)
```

Backward compatibility maps older terminology: `trusted` is now `active`, and
`deprecated` is now `stale`.

## Routing Priority (ADR-005)

```
Query → RulesEngine → SemanticRouter → GraphRouter → LLM fallback
         (exact)      (embeddings)     (relations)    (future)
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11+ |
| Transport | MCP SDK (stdio) |
| CLI | Typer |
| Data models | Pydantic v2 |
| Facts storage | SQLAlchemy async + aiosqlite |
| Vector search | LanceDB by default; Qdrant optional via `MEMORY_VECTOR_BACKEND=qdrant` |
| Embeddings | sentence-transformers (all-MiniLM-L6-v2) |
| Graph | `SimpleGraph` in-memory Python dict+set engine; Neo4j not wired in v0.11 runtime |
| Testing | pytest + pytest-asyncio |
| Linting | ruff |
