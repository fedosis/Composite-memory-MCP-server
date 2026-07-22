# Comparative Benchmark Framework: CMMS vs External Memory Providers

> Historical v0.5.3 benchmark baseline. For v0.11.0b1, read CMMS backend
> references as SQLite/FTS5 plus optional LanceDB/Qdrant vectors and in-memory
> SimpleGraph unless a scenario explicitly initializes Qdrant.

## Overview

This document defines the quality and performance metrics used to benchmark the
Composite Memory MCP Server (CMMS) against two baseline external memory providers:

| Provider | Description | Strengths |
|----------|-------------|-----------|
| **CMMS** (hybrid) | SQLite/FTS5 + optional LanceDB/Qdrant vectors + in-memory SimpleGraph + 4-stage router | Hybrid routing, entity relations, knowledge extraction |
| **ChromaDB** (pure vector) | Vector-only semantic retrieval | Fast similarity search, simple API |
| **SQLite-only** (pure keyword) | LIKE-based exact/keyword matching | Deterministic, no dependencies |

---

## A. QUALITY METRICS (Primary)

These capture the **user-visible retrieval quality** — what matters most when
an agent asks the memory system a question.

### 1. Context Precision

**Definition:** Given a set of stored facts and a specific query, what fraction
of retrieved results are relevant?

**Test Dataset:** 50 facts across 5 topics (10 each): Docker, Databases,
Networking, Applications, Configurations.

**Probe Queries:**
1. `"Docker container setup"` — relevant: all 10 Docker facts → precision@5, precision@10
2. `"database connection"` — relevant: database + networking cross-topic facts
3. `"web server port configuration"` — relevant: apps + configs + networking
4. `"PostgreSQL backup"` — relevant: database facts about PostgreSQL
5. `"Nginx reverse proxy"` — relevant: apps + networking cross-topic facts

**Measurement:**
```
precision@N = (# relevant results in top-N) / N
```

For each probe query, measure precision@5 and precision@10 across all 3 systems.

### 2. Context Recall

**Definition:** Of all relevant facts for a query, how many are retrieved?

**Measurement:**
```
recall@10 = (# relevant facts in top-10) / (total # relevant facts in dataset)
```

The multi-fact queries (e.g. `"Docker networking"`) are especially informative
because the correct answer set spans two topics (Docker AND networking), testing
whether the system can cross topic boundaries.

### 3. Multi-hop Retrieval

**Definition:** Can the system connect indirectly related facts through entity
relations?

**Scenario:**
```
Stored facts:
  ("Docker", "runs_on", "server-alpha")
  ("server-alpha", "hosts", "PostgreSQL")
  ("server-alpha", "ip_address", "10.0.0.42")

Query: "What runs on the machine that hosts Docker?"
```

**Expected multi-hop resolution:**
- Docker → runs_on → server-alpha
- server-alpha → hosts → PostgreSQL
- Answer: PostgreSQL (and ideally the IP 10.0.0.42 as additional context)

**CMMS** uses the GraphRouter stage to find this path.
**ChromaDB** relies on embedding overlap — may or may not connect the dots.
**SQLite** relies on keyword matching — cannot follow entity relations.

**Score:** 0–3 points based on retrieval depth:
- 0: No relevant results
- 1: Returns Docker-related facts only (no hops)
- 2: Returns server-alpha facts (1 hop)
- 3: Returns PostgreSQL + IP (2+ hops, full chain)

### 4. Noise Resilience

**Definition:** With 100 irrelevant facts added, does precision degrade?

**Procedure:**
1. Measure baseline precision for each probe query on the clean 50-fact dataset.
2. Add 100 noise facts (random system logs, unrelated software, generic config).
3. Re-measure precision for each probe query.
4. Compute `precision_drop = baseline_precision - noise_precision`.

**Expected behavior:**
- Vector systems (CMMS, ChromaDB) should be moderately resilient since
  irrelevant facts have different semantic embeddings.
- SQLite may show false positives if noise facts happen to share keywords.

### 5. Hybrid Routing Value

**Definition:** Do rules catch exact-match queries before reaching vector search?

**Precondition:** CMMS has a RoutingRuleSet with an `ip_address_query` rule that
matches queries containing "IP of" or "what is the IP".

**Scenario:**
```
Stored fact: ("server-alpha", "ip_address", "10.0.0.42")

Query: "What is the IP of server-alpha?"
```

**Expected routing:**
- CMMS: Stage 1 (rules engine) catches the query → routes to SQL → exact answer.
- ChromaDB: Must go through full vector search → may or may not rank it #1.
- SQLite: Direct LIKE match on `ip_address` → exact answer.

**Measurements:**
- Correctness: Does the top result contain "10.0.0.42"?
- Latency: Time to first result (rules should be microsecond-fast vs ms for vector).

---

## B. PERFORMANCE METRICS (Secondary)

These measure the operational characteristics of each system.

### 6. Indexing Throughput

**Procedure:** Insert 1000 facts as fast as possible. Measure elapsed time.

```
throughput = 1000 / elapsed_seconds  (facts/second)
```

**Systems:**
- CMMS: `remember()` — writes SQLite + queues best-effort secondary vector/graph indexing
- ChromaDB: `collection.add()` — pure vector upsert
- SQLite: `INSERT INTO facts ...` — bare SQL write

### 7. Query Latency (p50 / p95)

**Procedure:** Execute 100 random queries (mix of exact-match and semantic), measure
wall-clock time for each. Report p50 and p95 latencies in milliseconds.

### 8. Memory Usage (RSS)

**Procedure:** Measure `VmRSS` from `/proc/self/status` after indexing 1000 facts.

- **Baseline:** Memory before any facts loaded.
- **After 1000 facts:** Additional RSS consumed.

### 9. Cold Start Time

**Procedure:** Time from system instantiation to first successful query.

For CMMS + ChromaDB this includes:
- Loading SentenceTransformer model (≈2–5 sec on first load)
- Creating the active vector collection (LanceDB by default, Qdrant if configured, ChromaDB for the external baseline)

For SQLite-only, this is near-instant.

---

## C. COMPARATIVE FEATURE MATRIX

| Feature | CMMS | ChromaDB | SQLite-only |
|---------|------|----------|-------------|
| Exact keyword match | ✅ rules engine (Stage 1) | ❌ vector only | ✅ LIKE |
| Semantic similarity | ✅ LanceDB/Qdrant (Stage 2) | ✅ default | ❌ |
| Entity relations | ✅ graph engine (Stage 3) | ❌ | ❌ |
| Pathfinding / multi-hop | ✅ graph BFS traversal | ❌ | ❌ |
| Knowledge extraction | ✅ learn() with 3 extractors | ❌ | ❌ |
| Validation lifecycle | ✅ candidate→trusted | ❌ | ❌ |
| Audit trail | ✅ receipts + history | ❌ | ❌ |
| MCP interface | ✅ native FastMCP | ❌ | ❌ |
| Secondary indexing | ✅ remember/learn queue vector+graph indexing where configured | ❌ manual only | ❌ |
| LLM fallback | ✅ Stage 4 (placeholder) | ❌ | ❌ |
| Configurable rules | ✅ RoutingRuleSet | ❌ | ❌ |
| In-process Python API | ✅ mcp.call_tool() | ✅ collection API | ✅ raw SQL |

---

## D. TEST HARNESS

Defined in `tests/test_comparative.py` with the following structure:

```
tests/test_comparative.py
├── Shared Test Dataset (50 facts × 5 topics)
├── Provider Wrappers
│   ├── CMMSProvider    — MCP stdio/in-process client
│   ├── ChromaDBProvider — direct ChromaDB embeddings
│   └── SQLiteOnlyProvider — raw SQLite LIKE queries
├── Quality Metric Tests
│   ├── test_context_precision
│   ├── test_context_recall
│   ├── test_multi_hop_retrieval
│   ├── test_noise_resilience
│   └── test_hybrid_routing_value
├── Performance Metric Tests
│   ├── test_indexing_throughput
│   ├── test_query_latency
│   └── test_memory_usage
└── Comparison Tables (stdout)
```

---

## E. INTERPRETATION GUIDE

| If CMMS wins on… | It means… |
|------------------|-----------|
| Precision | Hybrid routing effectively filters irrelevant results |
| Recall | Cross-backend federation finds more relevant facts |
| Multi-hop | The graph engine adds genuine value over pure vector/keyword |
| Noise resilience | The multi-stage pipeline degrades gracefully |
| Hybrid routing | Rules-based pre-routing is faster for structured queries |

| If ChromaDB wins on… | It means… |
|----------------------|-----------|
| Precision | Pure vector search with good embedding quality is hard to beat |
| Throughput | CMMS multi-backend overhead is significant |
| Latency | Single-backend lookup is faster than multi-stage routing |

| If SQLite wins on… | It means… |
|--------------------|-----------|
| Exact-match queries | For trivial queries, fancy routing is unnecessary overhead |
| Memory | Zero dependencies = minimal footprint |
| Cold start | Model loading is the bottleneck |
