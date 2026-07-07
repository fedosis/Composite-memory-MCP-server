# Composite Memory MCP Server — Architecture Overview

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
        
        subgraph Tools["MCP Tools (9)"]
            PING["ping 🔊"]
            REM["remember 💾"]
            SRCH["search 🔍"]
            SEM["semantic_search 🧠"]
            LEARN["learn 📖"]
            CTX["get_context 📋"]
            GRPH["graph_search 🕸️"]
            ROUTE["route 🧭"]
            AUDIT["audit 📊"]
        end

        subgraph Router["Hybrid Router (ADR-005)"]
            direction LR
            R1["① RulesEngine\nexact match"]
            R2["② SemanticRouter\nembeddings → Qdrant"]
            R3["③ GraphRouter\nentity relations"]
            R4["④ LLM fallback\n(placeholder)"]
        end

        subgraph Providers["Storage Backends"]
            SQL["SQLite\n(Facts, Decisions,\nSkills, Receipts)"]
            QDRANT["Qdrant\n(Vectors,\nSemantic Search)"]
            GRAPH["GraphEngine\n(Python dict+set,\nEntity Relations)"]
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

        subgraph AutoIndex["Auto-Indexing Bridge (v0.5)"]
            AI_REM["remember() →\nQdrant + Graph"]
            AI_LEARN["learn() →\nQdrant + Graph"]
            AI_DECAY["decay archive →\nremove from\nQdrant + Graph"]
        end
    end

    Clients -->|MCP stdio| MCP
    MCP --> Tools
    
    REM --> SQL
    REM --> AI_REM
    AI_REM --> QDRANT
    AI_REM --> GRAPH
    
    SRCH --> SQL
    SEM --> R2
    LEARN --> Extractors
    LEARN --> AI_LEARN
    AI_LEARN --> QDRANT
    AI_LEARN --> GRAPH
    CTX --> SQL
    CTX --> GRAPH
    GRPH --> GRAPH
    ROUTE --> Router
    
    R1 --> SQL
    R2 --> QDRANT
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

## Verification Lifecycle (v0.5)

```
candidate ──→ validated ──→ trusted ──→ deprecated ──→ archived
  (new)    (conf≥0.7)  (conf≥0.85   (conflict      (TTL expired)
                         + corr≥2)    resolved)
```

## Routing Priority (ADR-005)

```
Query → RulesEngine → SemanticRouter → GraphRouter → LLM fallback
         (exact)      (embeddings)     (relations)    (future)
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.12+ |
| Transport | MCP SDK (stdio) |
| CLI | Typer |
| Data models | Pydantic v2 |
| Facts storage | SQLAlchemy async + aiosqlite |
| Vector search | Qdrant (in-memory / HTTP) |
| Embeddings | sentence-transformers (all-MiniLM-L6-v2) |
| Graph | Pure Python dict+set engine |
| Testing | pytest + pytest-asyncio |
| Linting | ruff |
