# Composite Memory MCP Server — Metrics & Benchmark Suite

> **Version:** 1.0
> **Applies to:** Historical CMMS v0.5 baseline. For v0.11.0b1, treat Qdrant
> examples as optional vector-backend scenarios; current server mode uses
> SQLite/FTS5 as the base, LanceDB by default or Qdrant optionally for semantic
> search, and in-memory SimpleGraph for graph lookup.
> **Purpose:** Define what to measure, how to measure it, and how to interpret results for adequacy, performance, and stability.

---

## Table of Contents

1. [Adequacy Metrics](#1-adequacy-metrics-does-it-work-correctly)
2. [Performance Metrics](#2-performance-metrics-how-fastcheap-is-it)
3. [Stability Metrics](#3-stability-metrics-does-it-stay-up)
4. [Appendix: Measuring Framework](#appendix-measuring-framework)
5. [Appendix: Test Infrastructure Requirements](#appendix-test-infrastructure-requirements)

---

## 1. Adequacy Metrics (does it work correctly?)

### 1.1 `remember()` → `search()` — Precision & Recall at Volume

| Property | Detail |
|----------|--------|
| **What** | Fact storage followed by keyword search retrieval |
| **Why** | Core read-after-write contract for the SQLite-backed fact store |
| **How** | Insert N facts with known subject/predicate/object via `remember()`, then `search()` with keywords matching each fact's text. Compute **precision** = retrieved_relevant / retrieved_total and **recall** = retrieved_relevant / N. Repeat at volumes N = {10, 100, 1000}. Because `search()` uses SQL `LIKE` across subject/predicate/object, a fact is "retrieved" if any keyword from the fact text appears in the query. |
| **Expected baseline** | Precision = 1.0, Recall = 1.0 at all volumes. SQLite `LIKE` is exact substring matching — no false positives *unless* the query substring happens to match multiple facts (in which case precision < 1.0 is an honest reflection of ambiguous keywords, not a bug). |
| **Threshold** | Precision ≥ 0.95, Recall ≥ 0.95. Below 0.95 at N=1000 → investigate query generation or SQLite indexing. |
| **Python pattern** | ```python
import asyncio, json, random, string
from memory_server.server import mcp

async def bench_remember_search(N: int):
    stored = []
    for i in range(N):
        subj = f"metric-entity-{i}"
        pred = "has_property"
        obj = f"value-{random.randint(0, 1000)}"
        resp = await mcp.call_tool("remember", {
            "subject": subj, "predicate": pred, "object": obj,
            "confidence": 1.0, "source": "bench"
        })
        stored.append((subj, pred, obj))

    # Search for each fact by subject keyword
    hits, misses, total = 0, 0, len(stored)
    for subj, pred, obj in stored:
        kw = subj.replace("metric-entity-", "metric")  # partial keyword
        result = json.loads(await mcp.call_tool("search", {"query": kw}))
        found = any(
            r.get("subject") == subj
            for r in (result if isinstance(result, list) else result.get("results", []))
        )
        if found: hits += 1
        else: misses += 1

    recall = hits / total
    # Precision: count results that contain at least one stored subject
    return {"volume": N, "recall": recall, "misses": misses}
``` |
| **Interpretation** | Recall < 1.0 means facts written by `remember()` are not found by `search()` — a data-integrity bug. Precision < 1.0 means the query is too broad (acceptable; fine-tune query). The volume sweep (10→1000) checks whether SQLite performance degrades retrieval correctness. |

---

### 1.2 `learn()` Extraction Accuracy — Known Facts Injection

| Property | Detail |
|----------|--------|
| **What** | Regex-based `FactExtractor` (default mode) extracting known SPO triples from controlled text |
| **Why** | The `learn()` tool is the primary ingestion pipeline; extraction rate determines how much knowledge survives ingestion |
| **How** | Craft sentences of the form `"X is Y."` (matches FactExtractor's regex), inject known SPO triples, call `learn()`, count how many are extracted. Compute **extraction_rate** = extracted / injected. |
| **Expected baseline** | Extraction rate ≥ 0.95 for simple `"X is Y"` sentences (regex mode). The regex `(\w[\w\s]*?)\s+is\s+(\w[\w\s]*)` captures single-word subjects/objects reliably. Multi-word subjects without whitespace breaks (e.g. `"my-server-01"`) also work. Compound objects like `"fast and reliable"` cause partial captures — acceptable, score separately. |
| **Threshold** | ≥ 0.95 for simple SPOs; ≥ 0.80 for compound objects. |
| **Python pattern** | ```python
async def bench_learn_accuracy():
    # Inject 20 known facts
    sentences = "\n".join([
        f"{chr(65+i)} is {chr(97+i)}." for i in range(20)
    ])
    result = json.loads(await mcp.call_tool("learn", {
        "text": sentences, "source": "bench"
    }))
    extracted = result.get("facts", [])
    rate = len(extracted) / 20
    return {"injected": 20, "extracted": len(extracted), "rate": rate}
``` |
| **Interpretation** | Rate < 0.95 indicates either the regex doesn't match the injected patterns or the extractor is silently failing. Investigate FactExtractor regex first. Compound objects naturally lower the rate — document this as a known limitation, not a bug. |

---

### 1.3 `semantic_search()` Ranking — Top-K Relevance

| Property | Detail |
|----------|--------|
| **What** | Vector search ranking quality: stored facts with known similarity, verify top-K contains the most relevant fact |
| **Why** | Semantic search is the core of the embedding-based retrieval path; if ranking is poor, `route()` stage 2 and `get_context()` produce noisy results |
| **How** | Store facts A, B, C where A is semantically closest to query Q. Call `semantic_search(query=Q, top_k=3)`. Verify fact A appears in the result set and is ranked #1 (highest score). Repeat with 10 fact clusters of 3 facts each (30 facts total). Compute **precision@K** and **mean-reciprocal-rank (MRR)**. |
| **Expected baseline** | For clearly distinct facts (e.g., "Python is a programming language" vs "Cats are mammals"), MRR ≥ 0.95. For confusable facts (e.g., "PostgreSQL is a database" vs "MySQL is a database"), MRR ≥ 0.70 — the all-MiniLM-L6-v2 model has 384 dimensions and limited disambiguation for closely related concepts. |
| **Threshold** | MRR ≥ 0.85 (mixed), Precision@3 ≥ 0.80 |
| **Python pattern** | ```python
async def bench_semantic_ranking():
    clusters = [
        ("python programming language", "Python is a popular programming language"),
        ("python snake species", "Python is a genus of snakes"),
    ]
    for topic, fact_text in clusters:
        subj, _, obj = fact_text.partition(" is ")
        obj = obj.rstrip(".")
        await mcp.call_tool("remember", {
            "subject": subj, "predicate": "is", "object": obj,
            "confidence": 1.0, "source": "bench"
        })

    result = json.loads(await mcp.call_tool("semantic_search", {
        "query": "programming language",
        "top_k": 5
    }))
    results = result if isinstance(result, list) else result.get("results", [])
    ranks = [r["score"] for r in results if "python programming" in str(r).lower()]
    return {"results_with_relevant": len(ranks) > 0, "top_score": ranks[0] if ranks else 0}
``` |
| **Interpretation** | Low MRR means the embedding model can't distinguish facts well, OR the secondary indexing pipeline is not populating the active vector backend correctly. First verify secondary indexing/outbox behavior (see §1.7), then inspect embedding quality directly. |

---

### 1.4 `graph_search()` Pathfinding — Shortest-Path Correctness

| Property | Detail |
|----------|--------|
| **What** | Graph pathfinding via BFS between known entities |
| **Why** | The graph engine is used by `graph_search()` and `route()` stage 3; wrong paths produce wrong context |
| **How** | Build a known graph topology (e.g., A→B→C→D, A→D direct). Query `graph_search(source_id=A, target_id=D)`. Verify the returned path is the shortest (A→D, length 1 edge, not A→B→C→D, length 3). Test path absence between disconnected nodes. Test self-loop path. |
| **Expected baseline** | Shortest path correctly returned for all connected node pairs up to `max_depth=4`. Path of length 0 (source=target) returns `[[source]]`. No path between disconnected nodes returns `[]`. |
| **Threshold** | 100% correctness on known topologies |
| **Python pattern** | ```python
async def bench_graph_pathfinding():
    # Direct: A→D (short) vs indirect: A→B→C→D (long)
    # Build via remember() → auto-index creates graph nodes
    facts = [
        ("entity_a", "connects_to", "entity_d"),      # short path
        ("entity_a", "links_to", "entity_b"),
        ("entity_b", "links_to", "entity_c"),
        ("entity_c", "links_to", "entity_d"),          # long path
    ]
    for s, p, o in facts:
        await mcp.call_tool("remember", {
            "subject": s, "predicate": p, "object": o,
            "confidence": 1.0, "source": "bench"
        })

    result = json.loads(await mcp.call_tool("graph_search", {
        "source_id": "entity-a", "target_id": "entity-d"
    }))
    paths = result.get("paths", [])
    shortest_edge_count = min(len(p) - 1 for p in paths) if paths else None
    return {
        "paths_found": len(paths),
        "shortest_edges": shortest_edge_count,
        "correct": shortest_edge_count == 1  # should be A→D direct
    }
``` |
| **Interpretation** | Shortest path not returned → BFS implementation bug or graph construction issue. Check `SimpleGraph.find_path()` depth parameter. If shortest path has more edges than expected, verify the direct edge was created during secondary indexing. |

---

### 1.5 `route()` Fallthrough — RulesEngine Priority

| Property | Detail |
|----------|--------|
| **What** | The 4-stage hybrid router correctly matches rule-triggered queries at stage 1, and falls through to embeddings/graph/LLM for non-rule queries |
| **Why** | ADR-005 defines routing priority: rules → semantic → graph → LLM. Wrong routing breaks the memory quality contract |
| **How** | (a) Query with a known keyword from default rules (e.g., `"what is the IP of server"`) → verify `stage=1`. (b) Query with unknown text that has a semantically similar fact in the vector backend (LanceDB default, Qdrant optional) → verify `stage=2`. (c) Query with entity name from graph → verify `stage=3`. (d) Query with gibberish / no matches → verify `stage=4` (LLM fallback). |
| **Expected baseline** | 100% correct stage selection for all four categories |
| **Threshold** | 100% |
| **Python pattern** | ```python
async def bench_route_fallthrough():
    # Preconditions: store a fact + build graph
    await mcp.call_tool("remember", {
        "subject": "database-server", "predicate": "runs_on", "object": "port-5432",
        "confidence": 1.0, "source": "bench"
    })

    tests = [
        ("what is the IP of main server", 1, "rules"),
        ("database server", 2, "semantic"),
        ("database-server", 3, "graph"),
        ("xyznonexistent gibberish", 4, "llm_fallback"),
    ]
    results = []
    for query, expected_stage, expected_route in tests:
        resp = json.loads(await mcp.call_tool("route", {"query": query}))
        ok = resp.get("stage") == expected_stage and resp.get("route") == expected_route
        results.append({"query": query, "expected": expected_stage, "got": resp.get("stage"), "ok": ok})
    return results
``` |
| **Interpretation** | Wrong stage assignment → hybrid router logic defect. Rules stage misclassification is most common (keyword matching is case-sensitive, or rules not loaded). Stage 2→3 misclassification happens when embeddings produce no results but graph has a match — verify the vector backend (LanceDB default, Qdrant optional) is populated. |

---

### 1.6 Validation Lifecycle — Candidate → Validated → Trusted

| Property | Detail |
|----------|--------|
| **What** | Facts registered as `candidate` (confidence=0.5) transition to `validated` at confidence ≥ 0.7, and to `trusted` at confidence ≥ 0.85 with corroboration ≥ 2 |
| **Why** | The Validator is the core quality gate for the memory evaluation engine; wrong thresholds compromise memory reliability |
| **How** | (a) Register a fact at confidence 0.5 → status is `candidate`. (b) Update confidence to 0.75 → call `validate()` → status becomes `validated`. (c) Update confidence to 0.9, set corroboration count to 3 → call `trust()` → status becomes `trusted`. (d) Test boundary conditions: confidence exactly 0.7→validated, 0.69 stays candidate; trust threshold 0.85 with corroboration 1 stays validated. |
| **Expected baseline** | All transitions match the hard-coded thresholds in `Validator.__init__()`: `validate_threshold=0.7`, `trust_threshold=0.85`, `trust_corroboration_min=2`. |
| **Threshold** | 100% of transitions correct |
| **Python pattern** | ```python
from memory_server.evaluation.validator import Validator

def bench_validation_lifecycle():
    v = Validator()
    fid = "test-fact-1"

    # Stage 1: candidate
    v.register(fid, confidence=0.5)
    s1 = v.get_status(fid)["status"]
    assert s1 == "candidate", f"Expected candidate, got {s1}"

    # Stage 2: validated (confidence ≥ 0.7)
    v.set_confidence(fid, 0.75)
    v.validate(fid)
    s2 = v.get_status(fid)["status"]
    assert s2 == "validated", f"Expected validated, got {s2}"

    # Stage 3: trusted (confidence ≥ 0.85 + corroboration ≥ 2)
    v.set_confidence(fid, 0.9)
    v.set_corroboration_count(fid, 3)
    v.trust(fid)
    s3 = v.get_status(fid)["status"]
    assert s3 == "trusted", f"Expected trusted, got {s3}"

    return {"candidate": s1, "validated": s2, "trusted": s3}
``` |
| **Interpretation** | If candidate→validated fails at 0.75, either the threshold constant changed or `validate()` logic is wrong. If validated→trusted fails at 0.9+corroboration 3, check `trust_corroboration_min` (default 2). The *deprecated→archived* path should also be tested but is a secondary concern. |

---

### 1.7 Secondary Indexing — Fact Is Safely Queued After `remember()`

| Property | Detail |
|----------|--------|
| **What** | After `remember()`, the fact is durably written to SQLite and an outbox entry is queued for best-effort secondary indexing. Vector indexing occurs only when the active vector provider (LanceDB default, Qdrant optional) and embedder are initialized; graph sync uses the available `SimpleGraph` path. |
| **Why** | The outbox bridge keeps the primary write reliable: a secondary index failure must not lose the fact. Later `semantic_search()` / `graph_search()` quality depends on the outbox worker processing the relevant secondary paths. |
| **How** | Call `remember()`, verify the fact exists in SQLite, verify an outbox entry was created/processed, then inspect the active vector provider and graph only for providers initialized in that test setup. |
| **Expected baseline** | 100% durable SQLite writes; 100% queued outbox entries. Secondary index coverage depends on initialized providers and should be reported separately. |
| **Threshold** | 100% primary write + outbox creation; provider-specific secondary index thresholds documented per benchmark setup. |
| **Python pattern** | ```python
async def bench_auto_index():
    from memory_server.server import _get_graph_router, _get_provider
    from memory_server.api.remember import remember
    from memory_server.providers.embedding_provider import MockEmbeddingProvider

    provider = await _get_provider()
    result = await remember(provider, "test-subj", "test-pred", "test-obj")
    fact_id = result["fact"].id

    # Check provider-specific secondary indexes only when initialized.
    # Qdrant vector indexing is optional in v0.11.0b1 server wiring.
    in_vector = None

    # Check graph
    graph_router = await _get_graph_router()
    subj_node = graph_router.graph.get_node("test-subj")
    obj_node = graph_router.graph.get_node("test-obj")
    edge = graph_router.graph.get_edge("test-subj", "test-obj")
    in_graph = subj_node is not None and obj_node is not None and edge is not None

    return {"fact_id": fact_id, "in_vector": in_vector, "in_graph": in_graph}
``` |
| **Interpretation** | If the primary SQLite write succeeds but no outbox entry is created, check `remember_tool` / ingestion wiring. If the outbox entry exists but provider-specific indexes are empty, check whether the relevant provider/embedder was initialized and inspect outbox worker logs/retry state. |

---

## 2. Performance Metrics (how fast/cheap is it?)

### 2.1 `remember()` Latency — p50/p95/p99 vs Fact Volume

| Property | Detail |
|----------|--------|
| **What** | End-to-end latency for storing a single fact via `remember()`, including durable SQLite write and outbox creation, not full secondary index completion. |
| **Why** | `remember()` is the synchronous primary write path — high latency blocks the MCP event loop (stdio, single-threaded) |
| **How** | Measure wall-clock time for N consecutive `remember()` calls (warming up providers first). Compute p50, p95, p99 latencies at volumes N = {10, 100, 1000}. The volume parameter changes the *pre-existing* fact count before measurement (not batch size). |
| **Expected baseline** | With an initialized optional vector backend (LanceDB default, Qdrant optional) and MockEmbeddingProvider: p50 < 50ms, p95 < 100ms, p99 < 200ms. With real SentenceTransformer (all-MiniLM-L6-v2, CPU): p50 < 200ms, p95 < 500ms, p99 < 1000ms. |
| **Threshold** | p99 < 2s (real model), p99 < 500ms (mock). If p99 exceeds 5s with real model, consider thread-pool offloading for embedding. |
| **Python pattern** | ```python
import time, statistics, json
from memory_server.server import mcp

async def bench_remember_latency(N: int, warm_up: bool = True):
    # Warm up: ensure providers are initialized
    if warm_up:
        await mcp.call_tool("remember", {
            "subject": "warmup", "predicate": "is", "object": "ready",
            "confidence": 1.0, "source": "bench"
        })

    latencies = []
    for i in range(N):
        t0 = time.perf_counter()
        await mcp.call_tool("remember", {
            "subject": f"perf-entity-{i}",
            "predicate": "has_value",
            "object": f"val-{i}",
            "confidence": 1.0, "source": "bench"
        })
        latencies.append(time.perf_counter() - t0)

    sorted_lat = sorted(latencies)
    n = len(sorted_lat)
    return {
        "volume": N,
        "p50": sorted_lat[int(n * 0.50)] * 1000,
        "p95": sorted_lat[int(n * 0.95)] * 1000,
        "p99": sorted_lat[int(n * 0.99)] * 1000,
        "mean": statistics.mean(latencies) * 1000,
    }
``` |
| **Interpretation** | High p50 (≥1s with real model) suggests the embedding call is the bottleneck — it runs synchronously in `asyncio.to_thread()`. High p95/p99 spread suggests GC pauses or vector-backend work. Compare mock vs real model to isolate embedding overhead. |

---

### 2.2 `search()` Latency — p50/p95 vs Database Size

| Property | Detail |
|----------|--------|
| **What** | SQLite keyword search latency as the fact table grows |
| **Why** | Historical v0.5 `search()` used SQL `LIKE` across three text columns; v0.11.0b1 also maintains SQLite FTS5 indexes for fact and belief search paths. Performance still needs volume checks because broad substring searches can degrade when they cannot use FTS5 effectively. |
| **How** | Pre-populate SQLite with N facts (N = {10, 100, 1000, 5000}). Measure `search()` latency with a query that matches *every* row (worst-case scan) and one that matches 1 row (best-case). |
| **Expected baseline** | With SQLite in-memory (no disk I/O): p50 < 10ms at N=1000, p50 < 50ms at N=5000 for exact matches. FULL TABLE SCAN at N=5000: p50 < 200ms. |
| **Threshold** | p50 < 100ms at N=5000 for exact match; if >500ms on v0.11.0b1, verify the SQLite FTS5 tables/triggers are populated and inspect the query plan. |
| **Python pattern** | ```python
async def bench_search_latency(N: int):
    # Pre-populate N facts
    for i in range(N):
        await mcp.call_tool("remember", {
            "subject": f"search-entity-{i}",
            "predicate": "has_attr",
            "object": f"value-{i}",
            "confidence": 1.0, "source": "bench"
        })

    # Worst-case: query that matches many rows
    t0 = time.perf_counter()
    await mcp.call_tool("search", {"query": "search-entity"})
    full_scan_ms = (time.perf_counter() - t0) * 1000

    # Best-case: exact match
    t0 = time.perf_counter()
    await mcp.call_tool("search", {"query": "search-entity-0"})
    exact_ms = (time.perf_counter() - t0) * 1000

    return {"volume": N, "full_scan_ms": full_scan_ms, "exact_ms": exact_ms}
``` |
| **Interpretation** | Full-scan latency growing linearly with N is expected for `LIKE '%keyword%'` — SQLite cannot use a normal B-tree index for that pattern, and the query must explicitly use the FTS5 path to avoid scans. Exact-match latency should be near-constant (fast). A spike indicates the SQL query plan changed, FTS5 maintenance broke, or the table has too many columns. |

---

### 2.3 `semantic_search()` Latency — Cold vs Warm Start

| Property | Detail |
|----------|--------|
| **What** | Time for the first `semantic_search()` call (cold start — model not loaded) vs subsequent calls (warm). Also measured with `MockEmbeddingProvider` vs real `SentenceTransformer`. |
| **Why** | The SentenceTransformer model is lazy-loaded on first use (see `SentenceTransformerEmbeddingProvider._get_model()`). The first call pays a ~1–3s load penalty on CPU. |
| **How** | Start a fresh server process. Measure time for the first `semantic_search()` call. Then measure the second call. Compare. Also compare cold-start with `MockEmbeddingProvider` (instant, no model load). |
| **Expected baseline** | Cold start (real model): 1000–3000ms (model download + load + warmup). Warm start (real model): 50–200ms per query. Cold start (mock): < 5ms. Warm start (mock): < 1ms. |
| **Threshold** | Cold start < 5s. Warm start p50 < 300ms. If cold > 5s, consider pre-loading the model at server init. |
| **Python pattern** | ```python
async def bench_semantic_cold_warm():
    import subprocess, sys, time, json, asyncio

    # Cold: fresh subprocess calling semantic_search once
    code = """
import asyncio, json
from memory_server.server import mcp
t0 = time.perf_counter()
result = await mcp.call_tool("semantic_search", {"query": "cold test", "top_k": 3})
elapsed = (time.perf_counter() - t0) * 1000
print(json.dumps({"cold_ms": elapsed}))
"""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", code,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await proc.communicate()
    result_cold = json.loads(stdout.decode())

    # Warm: second call in same process
    t0 = time.perf_counter()
    await mcp.call_tool("semantic_search", {"query": "warm test", "top_k": 3})
    warm_ms = (time.perf_counter() - t0) * 1000

    return {"cold_ms": result_cold["cold_ms"], "warm_ms": warm_ms}
``` |
| **Interpretation** | Cold-warm delta is dominated by SentenceTransformer model load (torch import + model download + tokenizer init). If cold start is > 5s, check whether the model is already cached locally. Under stdio transport the server stays alive; the cold penalty is paid once per server lifetime. |

---

### 2.4 `learn()` Throughput — Facts Extracted Per Second

| Property | Detail |
|----------|--------|
| **What** | Number of facts extracted and stored per second from bulk text input |
| **Why** | `learn()` is used for batch knowledge ingestion; low throughput limits the server's ability to catch up on large text corpora |
| **How** | Generate synthetic text with N `"X is Y"` sentences (N = {10, 100, 500}). Call `learn()` on the full text. Measure total wall-clock time. Compute throughput = N / seconds. Include any queued secondary indexing overhead visible in the tool call. |
| **Expected baseline** | Regex extraction (default): 50–200 facts/second (CPU-bound on regex + SQLite writes). LLM extraction: 0.5–2 facts/second (network-bound on LLM API call). The bottleneck is typically SQLite `create_fact` + `create_receipt` per extracted item. |
| **Threshold** | Regex mode: ≥ 20 facts/second at N=500. LLM mode: ≥ 0.5 facts/second. Below these, examine the extraction loop overhead. |
| **Python pattern** | ```python
async def bench_learn_throughput(N: int):
    sentences = "\n".join([
        f"entity-{i} is value-{i}." for i in range(N)
    ])
    t0 = time.perf_counter()
    result = json.loads(await mcp.call_tool("learn", {
        "text": sentences, "source": "bench"
    }))
    elapsed = time.perf_counter() - t0
    extracted = len(result.get("facts", []))
    return {
        "injected": N,
        "extracted": extracted,
        "seconds": round(elapsed, 3),
        "facts_per_sec": round(extracted / elapsed, 1),
    }
``` |
| **Interpretation** | Low throughput (< 20 facts/sec regex) is likely caused by extraction plus per-fact SQL/receipt/outbox writes. Secondary vector/graph indexing should be measured separately through the outbox worker; if that path becomes a bottleneck, batch embedding/upsert can be evaluated for the provider-specific worker path. |

---

### 2.5 Memory Usage — Baseline vs 1000 Facts vs Loaded Model

| Property | Detail |
|----------|--------|
| **What** | Resident memory (RSS) of the server process under three conditions: (a) baseline (server started, no facts stored), (b) 1000 facts with initialized secondary indexes populated, (c) embedding model loaded (after first `semantic_search`) |
| **Why** | The server runs as a long-lived stdio process; cumulative memory growth means eventual OOM |
| **How** | Measure RSS via `/proc/self/status` (Linux) or `psutil` after each phase. Record: baseline at process start, after 1000 `remember()` calls, and after one `semantic_search()` call (which loads the SentenceTransformer model). |
| **Expected baseline** | Baseline (empty, no model): ~30–60 MB (Python interpreter + aiosqlite plus imported optional clients). After 1000 facts: ~80–150 MB when an optional vector collection plus SimpleGraph nodes/edges are populated. After model load: ~200–400 MB (all-MiniLM-L6-v2 is ~90 MB model weights, but PyTorch + CUDA/cpu adds overhead). |
| **Threshold** | < 500 MB total under all conditions. If > 500 MB, investigate memory growth in the initialized vector backend or unreleased SimpleGraph references. |
| **Python pattern** | ```python
import psutil, os

def get_rss_mb() -> float:
    proc = psutil.Process(os.getpid())
    return proc.memory_info().rss / (1024 * 1024)

async def bench_memory_usage():
    snapshot = {"baseline_mb": get_rss_mb()}
    for i in range(1000):
        await mcp.call_tool("remember", {
            "subject": f"mem-entity-{i}",
            "predicate": "has_val",
            "object": f"v-{i}",
            "confidence": 1.0, "source": "bench"
        })
    snapshot["after_1000_facts_mb"] = get_rss_mb()

    await mcp.call_tool("semantic_search", {"query": "memory test", "top_k": 3})
    snapshot["after_model_load_mb"] = get_rss_mb()

    return snapshot  # expecting ~50 → ~150 → ~350 MB
``` |
| **Interpretation** | Large baseline (> 100 MB) → investigate imports (sentence-transformers may be imported eagerly). Large growth after 1000 facts (> 200 MB) → the initialized vector backend or SimpleGraph may be using excessive storage per record. Large growth after model load (> 500 MB) → PyTorch CUDA memory allocator may be pre-allocating (set `PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:128`). |

---

### 2.6 Startup Time — Cold Start vs Warm Start

| Property | Detail |
|----------|--------|
| **What** | Time from process start to accepting the first tool call |
| **Why** | MCP clients (Hermes, Claude Code) spawn the server as a subprocess; slow startup impacts user experience |
| **How** | Measure time from `python -m memory_server` to `ping()` returning `{"status": "ok"}`. Cold start: first launch (no model cached, no SQLite DB). Warm start: relaunch against the same persistent SQLite DB; in-memory SimpleGraph and any in-memory vector backend state must be rebuilt. |
| **Expected baseline** | Cold start: 200–500 ms (Python imports + FastMCP init + provider lazy-init). Warm start: same (in-memory state is not persisted). If SQLite were file-backed, warm would be faster. |
| **Threshold** | Cold start < 2s. If > 2s, check for eager imports of heavy libraries (sentence-transformers, torch) at module level. |
| **Python pattern** | ```python
import subprocess, sys, time, json, asyncio

async def bench_startup_time():
    t0 = time.perf_counter()
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "memory_server",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    # Send a ping via MCP protocol (or wait for process to be ready)
    # Simplified: measure time until process accepts stdin
    # Real implementation would use the MCP JSON-RPC handshake
    startup_ms = (time.perf_counter() - t0) * 1000
    proc.kill()
    return {"startup_ms": startup_ms}
``` |
| **Interpretation** | Most startup time is Python import overhead. The `SentenceTransformerEmbeddingProvider` uses lazy init, so model load doesn't affect startup. If startup > 2s, profile with `python -X importtime` to find expensive imports. |

---

## 3. Stability Metrics (does it stay up?)

### 3.1 Concurrent Client Connections (Known Limitation)

| Property | Detail |
|----------|--------|
| **What** | Document the stdio transport limitation: MCP stdio is a single-client, single-process protocol. The server reads from stdin and writes to stdout — only one client can be connected at a time. |
| **Why** | This is not a bug; it's a design constraint of the MCP stdio transport chosen for CMMS. Multiple concurrent clients require the HTTP/SSE transport or a gateway. |
| **How** | Attempt to connect two client processes to the same server process. Verify the second connection fails or blocks (document the failure mode). |
| **Expected baseline** | Second client receives broken pipe / EOF. Or stdin is consumed by the first client and the second never gets responses. |
| **Threshold** | N/A — document as known limitation |
| **Python pattern** | ```python
import subprocess, sys, os

def test_single_client_limitation():
    """Documentation test — not expected to pass."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "memory_server"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
    )
    # First client works
    proc.stdin.write(b'{"jsonrpc":"2.0","method":"tools/call","params":...}')
    # Second client can't connect (same stdin/stdout)
    # Document: use MCP HTTP/SSE transport for multi-client
``` |
| **Interpretation** | For multi-client scenarios, the project should add MCP SSE/HTTP transport or use Hermes Agent's process manager to spawn per-client server instances. |

---

### 3.2 Error Recovery — Secondary Vector Index Unavailable

| Property | Detail |
|----------|--------|
| **What** | When the secondary vector index is unavailable, the `remember()` / `learn()` call still succeeds because SQLite is the primary durable write and secondary indexing is best-effort/outbox-driven. |
| **Why** | Vector search is a secondary index; the primary fact store is SQLite. Losing the vector provider should not lose data. |
| **How** | Run with the target vector provider disabled or mocked to fail. Call `remember()`. Verify the fact is stored in SQLite (via `search()`), and then inspect outbox worker retry/failure state plus provider-specific search results. |
| **Expected baseline** | `remember()` returns success with receipt + fact. `search()` finds the fact. Provider-specific semantic retrieval may miss it until the secondary index is available and the outbox is replayed. Warnings/retry state are visible. |
| **Threshold** | No crash, no data loss. |
| **Python pattern** | ```python
async def test_qdrant_unavailable():
    # Temporarily override the selected vector provider to an unhealthy state.
    # Exact setup depends on MEMORY_VECTOR_BACKEND and test fixtures.
    import memory_server.server as server
    old_lancedb, old_qdrant = server._lancedb, server._qdrant
    server._lancedb = None  # Simulate LanceDB init failure in a fixture-specific way
    server._qdrant = None   # Simulate Qdrant init failure in a fixture-specific way

    try:
        result = json.loads(await mcp.call_tool("remember", {
            "subject": "crash-test", "predicate": "is", "object": "resilient",
            "confidence": 1.0, "source": "bench"
        }))
        fact_ok = "fact" in result and result["fact"].get("subject") == "crash-test"

        search_result = json.loads(await mcp.call_tool("search", {"query": "crash-test"}))
        search_ok = len(search_result.get("results", search_result)) > 0

        return {"remember_succeeded": fact_ok, "search_found": search_ok}
    finally:
        server._lancedb, server._qdrant = old_lancedb, old_qdrant
``` |
| **Interpretation** | If `remember()` fails when a secondary vector provider is down, the primary write path is incorrectly coupled to secondary indexing. If SQLite storage also fails, inspect shared initialization/session wiring rather than provider-specific search code. |

---

### 3.3 Resource Leaks — Connection Cleanup

| Property | Detail |
|----------|--------|
| **What** | After each tool call, all provider connections (aiosqlite connection and any initialized vector client) should be properly reference-counted or closed. No file descriptors should accumulate. |
| **Why** | Long-running server with open FDs eventually hits the ulimit (typically 1024), causing `EMFILE` errors. |
| **How** | Before and after a burst of 1000 tool calls, check open file descriptors via `proc_fd_count = len(os.listdir('/proc/self/fd'))`. Also check SQLite connection count and initialized vector-client count if accessible. |
| **Expected baseline** | FD count difference of 0–2 (minor GC-related fluctuation). SQLite uses `aiosqlite` which manages a single connection; optional Qdrant in-memory uses gRPC-in-process (no external FDs). |
| **Threshold** | < 5 FD increase after 1000 calls. |
| **Python pattern** | ```python
import os

def count_fds() -> int:
    return len(os.listdir(f"/proc/{os.getpid()}/fd"))

async def bench_fd_leak():
    initial_fds = count_fds()
    for i in range(1000):
        await mcp.call_tool("ping", {})
    for i in range(100):
        await mcp.call_tool("remember", {
            "subject": f"fd-entity-{i}", "predicate": "is", "object": f"val-{i}",
            "confidence": 1.0, "source": "bench"
        })
    final_fds = count_fds()
    return {"initial": initial_fds, "final": final_fds, "delta": final_fds - initial_fds}
``` |
| **Interpretation** | A large delta (> 10) suggests unclosed file handles. Common culprits: vector provider client re-initialization or `asyncio.to_thread` threads that hold references. Because providers are singletons (lazy-init once), this should be stable. |

---

### 3.4 Data Integrity — 1000 Remember+Delete Cycles

| Property | Detail |
|----------|--------|
| **What** | After 1000 cycles of `remember()` followed by deleting the fact (via SQLite provider directly), no orphaned graph nodes remain. An orphan is a graph node with no incoming or outgoing edges and no corresponding fact in SQLite. |
| **Why** | The graph engine (`SimpleGraph`) adds nodes during secondary indexing but has no corresponding deletion path — `remember()` has no `delete()` tool, and the graph nodes persist even if the SQLite fact is removed. |
| **How** | Use the `SQLiteProvider` directly to delete facts after `remember()`. Then run `MemoryAuditor.audit_orphans()` to find graph nodes with no neighbors. |
| **Expected baseline** | Currently, the graph does NOT delete nodes when facts are removed. Expect **orphans** = total facts created × 2 (subject + object nodes). This is a known gap, not a regression. |
| **Threshold** | Document the current behavior. A fix would add `graph.delete_node()` calls to the deletion path. For now, the metric tracks whether orphan count stays *bounded* (no additional orphans beyond the 2-nodes-per-fact baseline). |
| **Python pattern** | ```python
async def bench_data_integrity():
    from memory_server.server import _get_graph_router, _get_provider
    provider = await _get_provider()

    for i in range(1000):
        result = json.loads(await mcp.call_tool("remember", {
            "subject": f"integrity-entity-{i}", "predicate": "is",
            "object": f"val-{i}", "confidence": 1.0, "source": "bench"
        }))
        fact_id = result["fact"]["id"]
        await provider.delete_fact(fact_id)

    graph_router = await _get_graph_router()
    all_nodes = graph_router.graph.get_all_nodes()
    orphans = [n for n in all_nodes if not graph_router.graph.get_neighbors(n.id)]

    return {"total_nodes": len(all_nodes), "orphan_nodes": len(orphans)}
``` |
| **Interpretation** | Each indexed `remember()` creates 2 graph nodes (subject, object) + 1 edge. Deleting the SQLite fact does NOT remove graph nodes. Expected orphan count depends on how many outbox/secondary-index entries processed. If the count is higher, nodes are accumulating from another secondary indexing path. |
| **Mitigation** | Add graph cleanup to the fact deletion path, or mark this as a known limitation and use `MemoryAuditor` for periodic cleanup. |

---

### 3.5 Long-Running Test — 1 Hour Continuous Operation

| Property | Detail |
|----------|--------|
| **What** | Run the server continuously for 1 hour, issuing periodic tool calls (remember, search, semantic_search, learn, route, audit) every 10 seconds. Monitor for crashes, latency regression, and memory growth. |
| **Why** | Long-lived stdio processes must remain stable across hours of operation. Memory leaks, thread leaks, or vector collection bloat only manifest over time. |
| **How** | Spawn the server process. Every 10 seconds for 1 hour (360 cycles), call a rotating tool sequence. Record: response success/fail, per-call latency, RSS memory, FD count. Report max, min, and trend. |
| **Expected baseline** | 0 crashes. Memory growth < 50 MB over the hour. Latency p95 does not increase by more than 20% from start to end. No more than 1 transient error from the initialized vector provider or embedding stack. |
| **Threshold** | 0 crashes. Memory growth < 100 MB. Latency p95 stable (±20%). FD count stable. |
| **Python pattern** | ```python
async def bench_long_running():
    import psutil, time, os

    pid = os.getpid()
    proc = psutil.Process(pid)

    tool_cycle = [
        ("ping", {}),
        ("remember", {"subject": "longrun", "predicate": "cycle", "object": "test",
                       "confidence": 1.0, "source": "bench"}),
        ("search", {"query": "longrun"}),
        ("semantic_search", {"query": "long run test", "top_k": 3}),
        ("learn", {"text": "longrun-server is stable."}),
        ("route", {"query": "long run test"}),
    ]

    snapshots = []
    for cycle in range(360):  # 360 * 10s = 3600s = 1h
        tool_name, params = tool_cycle[cycle % len(tool_cycle)]
        t0 = time.perf_counter()
        try:
            await mcp.call_tool(tool_name, params)
            ok = True
        except Exception:
            ok = False
        elapsed_ms = (time.perf_counter() - t0) * 1000

        snapshots.append({
            "cycle": cycle, "tool": tool_name, "ok": ok,
            "latency_ms": round(elapsed_ms, 1),
            "rss_mb": proc.memory_info().rss / (1024 * 1024),
            "fd_count": len(os.listdir(f"/proc/{pid}/fd")),
        })
        await asyncio.sleep(10)

    return snapshots  # Analyze trends offline
``` |
| **Interpretation** | **Memory growth**: the initialized vector collection grows with every indexed `remember()` call — each fact adds a 384-dimensional vector + payload. Expected growth is small for the 360-fact endurance scenario. If RSS grows > 50 MB, check for Python object accumulation in Validator, DecayEngine, vector provider state, or SimpleGraph stores. **Latency creep**: Embedding model may trigger JIT compilation or cache thrashing. **FD growth**: vector-client handle leakage is the primary risk. |

---

## Appendix: Measuring Framework

### Orchestration Script Structure

All benchmarks should follow this pattern:

```python
"""
bench_<metric>.py — standalone benchmark for <metric>.

Usage:
    python bench_<metric>.py [--volume 100] [--warmup]

Outputs JSON to stdout.
"""
import asyncio, json, time, statistics, sys, argparse

# 1. Parse args
parser = argparse.ArgumentParser()
parser.add_argument("--volume", type=int, default=100)
parser.add_argument("--warmup", action="store_true")
args = parser.parse_args()

# 2. Import server (this starts the MCP server)
from memory_server.server import mcp

async def main():
    # 3. Warmup / precondition
    if args.warmup:
        await mcp.call_tool("ping", {})
    # 4. Run benchmark
    results = await bench_function(args.volume)
    # 5. Output JSON
    print(json.dumps(results, indent=2))

asyncio.run(main())
```

### Common Baseline Setup

Before running any benchmark, ensure:

1. **Python 3.11+** with `pip install -e ".[dev,sentence]"`
2. **Clean state**: no pre-existing SQLite data, vector-backend data (LanceDB default or Qdrant optional), or graph data
3. **Mock embedding**: For latency benchmarks that should isolate embedding overhead, set `USE_MOCK_EMBEDDING=1` env var (swap `SentenceTransformerEmbeddingProvider` for `MockEmbeddingProvider`)
4. **Warmup**: Call `ping()` and one `remember()` to force lazy-init of all providers before timed measurements

### Reporting Format

```
## Metric: <name>
- Result: <value>
- Threshold: <value>
- Pass/Fail: <bool>
- Notes: <any anomalies or observations>
```

For benchmark runs, output as JSON for CI integration:
```json
{
  "metric": "remember_latency",
  "volume": 100,
  "p50_ms": 42.1,
  "p95_ms": 87.3,
  "p99_ms": 156.2,
  "pass": true,
  "threshold_p99": 200.0
}
```

---

## Appendix: Test Infrastructure Requirements

| Requirement | Detail |
|-------------|--------|
| Python | 3.11+ |
| Packages | `pytest>=8.0`, `pytest-asyncio`, `psutil`, `memory_server[dev]` |
| CI Runner | Linux x86_64 (CPU-only; no CUDA needed for all-MiniLM-L6-v2) |
| Memory | ≥ 2 GB RAM (for model load plus initialized vector backend) |
| Disk | ≥ 500 MB (for sentence-transformers cache at ~/.cache/huggingface/) |
| Duration | Adequacy: ~2 min. Performance: ~5 min. Stability (1h): 60 min. |
| Repeatability | All benchmarks must be runnable back-to-back without server restart (in-memory state resets each run if process is fresh) |

### Benchmark Execution Matrix

| Priority | Metric | Frequency | Time |
|----------|--------|-----------|------|
| P0 | 1.1 remember→search recall | Every PR | < 30s |
| P0 | 1.6 Validation lifecycle | Every PR | < 10s |
| P0 | 1.7 Auto-indexing | Every PR | < 10s |
| P0 | 2.1 remember latency | Every PR | < 30s |
| P1 | 1.2 learn extraction rate | Daily | < 20s |
| P1 | 1.3 semantic search ranking | Daily | < 30s |
| P1 | 1.4 graph pathfinding | Daily | < 20s |
| P1 | 1.5 route fallthrough | Daily | < 20s |
| P1 | 2.2 search latency vs volume | Weekly | < 60s |
| P1 | 2.5 Memory usage | Weekly | < 30s |
| P2 | 2.3 semantic cold/warm | Weekly | < 10s |
| P2 | 2.4 learn throughput | Weekly | < 30s |
| P2 | 2.6 Startup time | Weekly | < 10s |
| P2 | 3.2 Error recovery | Weekly | < 10s |
| P2 | 3.3 Resource leaks | Weekly | < 30s |
| P2 | 3.4 Data integrity | Weekly | < 30s |
| P3 | 3.5 Long-running (1h) | Per release | 60 min |

**P0** = CI gate (must pass before merge)  
**P1** = Daily regression suite  
**P2** = Weekly performance regression  
**P3** = Per-release endurance check
