"""Comparative benchmark: CMMS vs ChromaDB vs SQLite-only.

Tests quality metrics (precision, recall, multi-hop, noise resilience,
hybrid routing) and performance metrics (throughput, latency, memory).

Run: pytest tests/test_comparative.py -v -s
"""

import asyncio
import json
import logging
import os
import statistics
import time
from typing import Any
from uuid import uuid4

import pytest

pytestmark = pytest.mark.asyncio

# ─── Dataset ────────────────────────────────────────────────────────────

TOPICS = {
    "docker": [
        ("Docker", "version", "24.0.7"),
        ("Docker", "runs_on", "server-alpha"),
        ("Docker", "compose_version", "2.24"),
        ("Docker", "registry", "docker.io/nousresearch"),
        ("Docker", "storage_driver", "overlay2"),
        ("Docker", "network_driver", "bridge"),
        ("Docker", "log_driver", "json-file"),
        ("Docker", "cgroup_driver", "systemd"),
        ("Docker", "buildkit", "enabled"),
        ("Docker", "swarm_mode", "inactive"),
    ],
    "databases": [
        ("PostgreSQL", "version", "16.1"),
        ("PostgreSQL", "port", "5432"),
        ("PostgreSQL", "data_dir", "/var/lib/postgresql/16/main"),
        ("PostgreSQL", "backup_tool", "pg_dump"),
        ("PostgreSQL", "host", "server-alpha"),
        ("Redis", "version", "7.2.4"),
        ("Redis", "port", "6379"),
        ("Redis", "persistence", "RDB+AOF"),
        ("Redis", "max_memory", "4GB"),
        ("Redis", "eviction_policy", "allkeys-lru"),
    ],
    "networking": [
        ("server-alpha", "ip_address", "10.0.0.42"),
        ("server-alpha", "subnet", "10.0.0.0/24"),
        ("server-alpha", "gateway", "10.0.0.1"),
        ("server-alpha", "dns", "10.0.0.53"),
        ("server-alpha", "mac_address", "00:1a:2b:3c:4d:5e"),
        ("proxy", "type", "nginx"),
        ("proxy", "port", "443"),
        ("proxy", "ssl_cert", "/etc/nginx/ssl/cert.pem"),
        ("proxy", "upstream", "localhost:3000"),
        ("proxy", "ssl_protocols", "TLSv1.2 TLSv1.3"),
    ],
    "applications": [
        ("nginx", "config_path", "/etc/nginx/nginx.conf"),
        ("nginx", "worker_processes", "4"),
        ("nginx", "access_log", "/var/log/nginx/access.log"),
        ("nginx", "error_log", "/var/log/nginx/error.log"),
        ("nginx", "server_name", "example.com"),
        ("Grafana", "version", "10.2.3"),
        ("Grafana", "port", "3000"),
        ("Grafana", "auth_mode", "oauth"),
        ("Grafana", "datasource", "Prometheus"),
        ("Grafana", "dashboard_dir", "/var/lib/grafana/dashboards"),
    ],
    "configs": [
        ("system", "timezone", "UTC"),
        ("system", "locale", "en_US.UTF-8"),
        ("system", "kernel", "6.5.0-15-generic"),
        ("system", "hostname", "server-alpha"),
        ("system", "uptime", "47 days"),
        ("backup", "schedule", "daily 02:00"),
        ("backup", "retention", "30 days"),
        ("backup", "destination", "s3://backups/server-alpha"),
        ("backup", "encryption", "AES-256"),
        ("backup", "verify", "enabled"),
    ],
}

ALL_FACTS: list[tuple[str, str, str]] = []
for _facts in TOPICS.values():
    ALL_FACTS.extend(_facts)

ALL_FACTS_KEYS = {
    i: f"{s}|{p}|{o}" for i, (s, p, o) in enumerate(ALL_FACTS)
}

PROBE_QUERIES: list[dict[str, Any]] = [
    {"query": "Docker container setup", "expected_facts": set(range(0, 10))},
    {"query": "database connection PostgreSQL", "expected_facts": set(range(10, 20))},
    {"query": "web server port configuration", "expected_facts": set(list(range(20, 30)) + list(range(40, 50)) + list(range(30, 40)))},
    {"query": "postgreSQL backup", "expected_facts": set(list(range(10, 20)) + list(range(45, 50)))},
    {"query": "nginx reverse proxy", "expected_facts": set(list(range(30, 35)) + list(range(20, 30)))},
]

NOISE_FACTS: list[tuple[str, str, str]] = [
    (f"syslog-{i}", "level", "info") for i in range(50)
] + [
    (f"tempfile-{i}", "owner", "root") for i in range(50)
]


# ─── Helpers ────────────────────────────────────────────────────────────

def _fact_key_from_dict(r: dict) -> str:
    return f"{r.get('subject', '')}|{r.get('predicate', '')}|{r.get('object', '')}"


def _precision_at_n(results: list[dict], expected_ids: set[int]) -> float:
    if not results:
        return 0.0
    expected_keys = {ALL_FACTS_KEYS[i] for i in expected_ids}
    relevant = sum(1 for r in results if _fact_key_from_dict(r) in expected_keys)
    return relevant / len(results)


def _recall_at_n(results: list[dict], expected_ids: set[int]) -> float:
    if not expected_ids:
        return 1.0
    expected_keys = {ALL_FACTS_KEYS[i] for i in expected_ids}
    found = sum(1 for r in results if _fact_key_from_dict(r) in expected_keys)
    return found / len(expected_ids)


def _get_vmrss_kb() -> int:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except FileNotFoundError:
        return 0
    return 0


# ─── Provider Wrappers ──────────────────────────────────────────────────

class CMMSProvider:
    """In-process CMMS via mcp.call_tool using file-based DB."""

    def __init__(self):
        from memory_server.server import mcp
        self._mcp = mcp
        self._db_path = f"/tmp/cmms_benchmark_{uuid4().hex}.db"

    async def initialize(self):
        # Override the global provider singleton with a file-based DB
        # (avoiding :memory: pool issues with aiosqlite)
        from memory_server.providers.sqlite_provider import SQLiteProvider
        import memory_server.server as srv

        p = SQLiteProvider(url=f"sqlite+aiosqlite:///{self._db_path}")
        await p.initialize()
        srv._provider = p

    async def close(self):
        from memory_server.server import _provider, _qdrant
        if _provider:
            await _provider.close()
        if _qdrant:
            try:
                _qdrant.close()
            except Exception:
                pass
        if os.path.exists(self._db_path):
            try:
                os.unlink(self._db_path)
            except OSError:
                pass

    async def _call(self, tool: str, args: dict) -> dict:
        content_list, _ = await self._mcp.call_tool(tool, args)
        return json.loads(content_list[0].text)

    async def remember(self, subject: str, predicate: str, object_: str) -> dict:
        return await self._call("remember", {
            "subject": subject, "predicate": predicate, "object": object_,
        })

    async def search(self, query: str, limit: int = 10) -> list[dict]:
        """CMMS hybrid search via semantic_search (rules + embeddings)."""
        result = await self._call("semantic_search", {"query": query, "top_k": limit, "score_threshold": 0.0})

        # Stage 1: Rule matched — route to SQL; fetch via keyword search
        if result.get("stage") == 1:
            sql_result = await self._call("search", {"query": query, "limit": limit})
            return sql_result.get("results", [])[:limit]

        # Stage 2: Semantic results from Qdrant
        semantic = result.get("semantic_results", [])
        if semantic:
            return [
                {"subject": s.get("payload", {}).get("subject", ""),
                 "predicate": s.get("payload", {}).get("predicate", ""),
                 "object": s.get("payload", {}).get("object", ""),
                 "score": s.get("score", 0.0)}
                for s in semantic
            ]
        return []

    async def route_search(self, query: str, limit: int = 10) -> list[dict]:
        """Full 4-stage hybrid route and fetch results."""
        result = await self._call("route", {"query": query, "top_k": limit})
        # Stage 1 (rules): fallback to keyword
        if result.get("stage") == 1 or result.get("route") == "rules":
            sql_result = await self._call("search", {"query": query, "limit": limit})
            return sql_result.get("results", [])[:limit]
        # Stage 2 (semantic): parse payloads
        semantic = result.get("semantic_results", [])
        if semantic:
            return [
                {"subject": s.get("payload", {}).get("subject", ""),
                 "predicate": s.get("payload", {}).get("predicate", ""),
                 "object": s.get("payload", {}).get("object", "")}
                for s in semantic
            ]
        # Stage 3 (graph): parse entities/relations
        graph_result = result.get("graph_result", {})
        if graph_result:
            entities = graph_result.get("entities", [])
            relations = graph_result.get("relations", [])
            out = []
            for e in entities:
                out.append({"subject": e.get("name", ""), "predicate": "entity", "object": e.get("type", "")})
            for r in relations:
                out.append({"subject": r.get("source_id", ""), "predicate": r.get("relation", ""), "object": r.get("target_id", "")})
            return out[:limit]
        return []


class ChromaDBProvider:
    """ChromaDB + sentence-transformers for pure vector retrieval."""

    def __init__(self):
        self._collection = None
        self._embedder = None
        self._client = None
        self._next_id = 0

    async def initialize(self):
        import chromadb
        from sentence_transformers import SentenceTransformer
        loop = asyncio.get_event_loop()
        self._embedder = await loop.run_in_executor(
            None, lambda: SentenceTransformer("all-MiniLM-L6-v2")
        )
        self._client = await loop.run_in_executor(
            None,
            lambda: chromadb.Client(chromadb.Settings(anonymized_telemetry=False)),
        )
        try:
            await loop.run_in_executor(None, self._client.delete_collection, "memories")
        except Exception:
            pass
        self._collection = await loop.run_in_executor(
            None, lambda: self._client.create_collection("memories")
        )

    async def close(self):
        pass

    async def remember(self, subject: str, predicate: str, object_: str) -> dict:
        text = f"{subject} {predicate} {object_}"
        loop = asyncio.get_event_loop()
        emb = await loop.run_in_executor(None, self._embedder.encode, text)
        self._next_id += 1
        doc_id = str(self._next_id)
        metadata = {"subject": subject, "predicate": predicate, "object": object_}
        await loop.run_in_executor(
            None,
            lambda: self._collection.add(
                ids=[doc_id], embeddings=[emb.tolist()], metadatas=[metadata], documents=[text],
            ),
        )
        return {"id": doc_id, "subject": subject, "predicate": predicate, "object": object_}

    async def search(self, query: str, limit: int = 10) -> list[dict]:
        loop = asyncio.get_event_loop()
        emb = await loop.run_in_executor(None, self._embedder.encode, query)
        result = await loop.run_in_executor(
            None,
            lambda: self._collection.query(
                query_embeddings=[emb.tolist()], n_results=min(limit, 100),
            ),
        )
        results = []
        metadatas = result.get("metadatas", [[]])[0]
        if metadatas:
            for m in metadatas:
                results.append({
                    "subject": m.get("subject", ""),
                    "predicate": m.get("predicate", ""),
                    "object": m.get("object", ""),
                })
        return results


class SQLiteOnlyProvider:
    """Pure SQLite LIKE — CMMS without Qdrant/graph/rules."""

    def __init__(self):
        self._provider = None

    async def initialize(self):
        from memory_server.providers.sqlite_provider import SQLiteProvider
        self._provider = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
        await self._provider.initialize()

    async def close(self):
        if self._provider:
            await self._provider.close()

    async def remember(self, subject: str, predicate: str, object_: str) -> dict:
        from datetime import datetime, timezone
        from memory_server.models import Fact
        fact = Fact(
            id=str(uuid4()),
            subject=subject,
            predicate=predicate,
            object=object_,
            confidence=1.0,
            source="benchmark",
            created_at=datetime.now(timezone.utc),
        )
        stored = await self._provider.create_fact(fact)
        return {"id": stored.id, "subject": stored.subject,
                "predicate": stored.predicate, "object": stored.object}

    async def search(self, query: str, limit: int = 10) -> list[dict]:
        facts = await self._provider.search_facts(text=query, limit=limit)
        return [{"subject": f.subject, "predicate": f.predicate, "object": f.object}
                for f in facts]


# ─── Provider factory ───────────────────────────────────────────────────

async def _make_provider(name: str):
    if name == "cmms":
        p = CMMSProvider()
    elif name == "chromadb":
        p = ChromaDBProvider()
    elif name == "sqlite":
        p = SQLiteOnlyProvider()
    else:
        raise ValueError(f"Unknown provider: {name}")
    await p.initialize()
    return p


async def _load_facts(p, facts: list[tuple[str, str, str]]) -> None:
    for s, pr, o in facts:
        await p.remember(s, pr, o)


# =====================================================================
# TESTS
# =====================================================================


async def _run_quality_comparison():
    """Run all quality metrics across all 3 providers and print table."""
    out = {}

    for prov_name in ["cmms", "chromadb", "sqlite"]:
        p = await _make_provider(prov_name)
        try:
            await _load_facts(p, ALL_FACTS)

            # Precision@5 and @10
            p5_scores, p10_scores, r10_scores = [], [], []
            for probe in PROBE_QUERIES:
                facts5 = await p.search(probe["query"], limit=5)
                facts10 = await p.search(probe["query"], limit=10)
                p5_scores.append(_precision_at_n(facts5, probe["expected_facts"]))
                p10_scores.append(_precision_at_n(facts10, probe["expected_facts"]))
                r10_scores.append(_recall_at_n(facts10, probe["expected_facts"]))

            out[prov_name] = {
                "p5": statistics.mean(p5_scores),
                "p10": statistics.mean(p10_scores),
                "r10": statistics.mean(r10_scores),
            }
        finally:
            await p.close()

    print()
    print("╔════════════════════════════════════════════════════════╗")
    print("║        QUALITY COMPARISON TABLE                      ║")
    print("╠════════╦════════════╦══════════════╦══════════════╣")
    print("║ Provider ║ Precision@5 ║ Precision@10 ║ Recall@10    ║")
    print("╠════════╬════════════╬══════════════╬══════════════╣")
    for prov_name in ["cmms", "chromadb", "sqlite"]:
        m = out[prov_name]
        print(f"║ {prov_name:<7}║ {m['p5']:<10.3f} ║ {m['p10']:<12.3f} ║ {m['r10']:<12.3f} ║")
    print("╚════════╩════════════╩══════════════╩══════════════╝")
    return out


async def _run_multi_hop():
    """Multi-hop: Docker→server-alpha→PostgreSQL."""
    print("\n--- Multi-hop Retrieval ---")
    print("Query: 'What runs on the machine that hosts Docker?'")
    print("Chain: Docker→runs_on→server-alpha→hosts→PostgreSQL\n")

    multi_facts = [
        ("Docker", "runs_on", "server-alpha"),
        ("server-alpha", "hosts", "PostgreSQL"),
        ("server-alpha", "ip_address", "10.0.0.42"),
        ("Docker", "version", "24.0.7"),
        ("PostgreSQL", "version", "16.1"),
    ]

    results = {}
    for prov_name in ["cmms", "chromadb", "sqlite"]:
        p = await _make_provider(prov_name)
        try:
            await _load_facts(p, multi_facts)
            if prov_name == "cmms":
                # Use full 4-stage hybrid route including graph
                facts = await p.route_search("What runs on the machine that hosts Docker?", limit=10)
            else:
                facts = await p.search("What runs on the machine that hosts Docker?", limit=10)

            result_text = " ".join(
                f"{f.get('subject','')} {f.get('predicate','')} {f.get('object','')}"
                for f in facts
            ).lower()
            has_postgres = "postgresql" in result_text or "16.1" in result_text
            has_ip = "10.0.0.42" in result_text
            has_server = any(
                "server" in str(f.get("subject","")).lower()
                or "server" in str(f.get("object","")).lower()
                or "server-alpha" in result_text
                for f in facts
            )
            has_docker = any("docker" in str(f.get("subject","")).lower() for f in facts)

            if has_postgres or has_ip:
                score = 3
            elif has_server:
                score = 2
            elif has_docker:
                score = 1
            else:
                score = 0

            label = {3: "✅ 2+ hops (PostgreSQL/IP found)",
                     2: "⚠  1 hop (server-alpha found)",
                     1: "❌ Docker only (0 hops)",
                     0: "❌ No results"}[score]
            print(f"  {prov_name:<8} score={score}/3  {label}")
            for f in facts[:5]:
                print(f"             {f.get('subject','')} → {f.get('predicate','')} → {f.get('object','')}")
            results[prov_name] = score
        finally:
            await p.close()
    return results


async def _run_noise_resilience():
    """Add 100 noise facts, measure precision drop."""
    print("\n--- Noise Resilience ---")
    print("100 irrelevant facts added to 50 real facts\n")

    results = {}
    for prov_name in ["cmms", "chromadb", "sqlite"]:
        p = await _make_provider(prov_name)
        try:
            await _load_facts(p, ALL_FACTS)
            base_scores = []
            for probe in PROBE_QUERIES:
                facts = await p.search(probe["query"], limit=5)
                base_scores.append(_precision_at_n(facts, probe["expected_facts"]))
            baseline = statistics.mean(base_scores)

            await _load_facts(p, NOISE_FACTS)
            noise_scores = []
            for probe in PROBE_QUERIES:
                facts = await p.search(probe["query"], limit=5)
                noise_scores.append(_precision_at_n(facts, probe["expected_facts"]))
            noised = statistics.mean(noise_scores)

            drop = baseline - noised
            print(f"  {prov_name:<8} P@5: {baseline:.3f} → {noised:.3f}  drop={drop:+.3f}")
            results[prov_name] = {"baseline": baseline, "noised": noised, "drop": drop}
        finally:
            await p.close()
    return results


async def _run_hybrid_routing():
    """Exact IP query — tests whether CMMS rules catch it before vector search.
    
    CMMS should route to Stage 1 (rules engine, microsecond-fast) for queries
    matching the ip_address_query rule, avoiding vector search entirely.
    """
    print("\n--- Hybrid Routing Value ---")
    print("Query: 'What is the IP of server-alpha?'")
    print("Expected routing: CMMS→stage1(rules), ChromaDB→vector, SQLite→keyword\n")

    results = {}
    for prov_name in ["cmms", "chromadb", "sqlite"]:
        p = await _make_provider(prov_name)
        try:
            await _load_facts(p, [("server-alpha", "ip_address", "10.0.0.42")])

            t0 = time.perf_counter()
            if prov_name == "cmms":
                # Direct route check — should hit stage 1 (rules engine)
                route_result = await p._call("semantic_search", {"query": "What is the IP of server-alpha?"})
                route_latency = (time.perf_counter() - t0) * 1000
                stage = route_result.get("stage")
                rule_match = route_result.get("rule_match", {})
                rule_name = rule_match.get("rule_name", "")
                print(f"  {prov_name:<8} stage={stage} rule='{rule_name}' latency={route_latency:.1f}ms")
                # Also check we can find the answer with keyword search after rule routes to SQL
                kw_facts = await p._call("search", {"query": "ip_address", "limit": 5})
                kw_results = kw_facts.get("results", [])
                correct = any("10.0.0.42" in f.get("object", "") for f in kw_results)
                print(f"             SQL fallback: {'✅ found' if correct else '❌ not found'} via keyword 'ip_address'")
                results[prov_name] = {"stage": stage, "latency_ms": route_latency, "correct": correct}
            else:
                facts = await p.search("What is the IP of server-alpha?", limit=5)
                latency_ms = (time.perf_counter() - t0) * 1000
                correct = any("10.0.0.42" in f.get("object", "") for f in facts)
                check = "✅" if correct else "❌"
                print(f"  {prov_name:<8} correct={correct} latency={latency_ms:.1f}ms {check}")
                results[prov_name] = {"correct": correct, "latency_ms": latency_ms}
        finally:
            await p.close()
    return results


async def _run_performance():
    """Throughput, latency, memory across all providers."""
    print("\n╔════════════════════════════════════════════════════════════════╗")
    print("║              PERFORMANCE COMPARISON TABLE                    ║")
    print("╠════════╦══════════════╦═══════════╦═══════════╦═══════════╣")
    print("║ Provider ║ Index (f/s)   ║ p50 (ms)   ║ p95 (ms)   ║ RSS (MB)   ║")
    print("╠════════╬══════════════╬═══════════╬═══════════╬═══════════╣")

    results = {}
    for prov_name in ["cmms", "chromadb", "sqlite"]:
        p = await _make_provider(prov_name)
        try:
            # ── Throughput ──
            N = 100
            t0 = time.perf_counter()
            for i in range(N):
                await p.remember(f"perf-{i}", "attr", f"val-{i}")
            elapsed = time.perf_counter() - t0
            fps = N / elapsed if elapsed > 0 else 0

            # ── Latency ──
            queries = ["Docker", "PostgreSQL", "nginx", "Redis", "backup",
                       "server-alpha", "Grafana", "proxy", "system config",
                       "database port"]
            lats = []
            for _ in range(100):
                q = queries[_ % len(queries)]
                t0 = time.perf_counter()
                await p.search(q, limit=5)
                lats.append((time.perf_counter() - t0) * 1000)
            sorted_lats = sorted(lats)
            p50 = sorted_lats[len(sorted_lats) // 2]
            p95 = sorted_lats[int(len(sorted_lats) * 0.95) - 1]

            # ── Memory ──
            rss = _get_vmrss_kb() / 1024

            print(f"║ {prov_name:<7}║ {fps:<13.1f}║ {p50:<10.1f}║ {p95:<10.1f}║ {rss:<10.1f}║")
            results[prov_name] = {"fps": fps, "p50_ms": p50, "p95_ms": p95, "rss_mb": rss}
        finally:
            await p.close()

    print("╚════════╩══════════════╩═══════════╩═══════════╩═══════════╝")
    return results


# ─── Main comparison test ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_comparative_benchmark():
    """Run all quality + performance comparisons and print summary tables."""
    print("\n" + "=" * 70)
    print("  COMPOSITE MEMORY MCP SERVER — COMPARATIVE BENCHMARK")
    print("  CMMS (hybrid) vs ChromaDB (pure vector) vs SQLite (pure keyword)")
    print("=" * 70)

    quality = await _run_quality_comparison()
    await _run_multi_hop()
    await _run_noise_resilience()
    await _run_hybrid_routing()
    perf = await _run_performance()

    # ── Final summary ──
    print("\n" + "=" * 70)
    print("COMPARATIVE ANALYSIS COMPLETE")
    print("=" * 70)
    print("Quality Table:")
    print(f"  {'Provider':<10} {'Prec@5':<10} {'Prec@10':<10} {'Recall@10':<10}")
    for pn in ["cmms", "chromadb", "sqlite"]:
        m = quality[pn]
        print(f"  {pn:<10} {m['p5']:<10.3f} {m['p10']:<10.3f} {m['r10']:<10.3f}")
    print("Performance Table:")
    print(f"  {'Provider':<10} {'Idx f/s':<10} {'p50 ms':<10} {'p95 ms':<10} {'RSS MB':<10}")
    for pn in ["cmms", "chromadb", "sqlite"]:
        m = perf[pn]
        print(f"  {pn:<10} {m['fps']:<10.1f} {m['p50_ms']:<10.1f} {m['p95_ms']:<10.1f} {m['rss_mb']:<10.1f}")

    print("\nComparative analysis complete — all metrics collected for review.")
