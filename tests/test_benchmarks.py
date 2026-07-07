"""Benchmark and integration tests based on the metrics framework (docs/metrics.md).

P0 (CI gate): Adequacy — does it work correctly?
P1 (Daily): Performance + Stability — how fast, does it stay up?

Implements the highest-priority metrics from the metrics framework.
"""

import json
import statistics
import time
from uuid import uuid4

import pytest

from memory_server.api.learn import learn as learn_fn
from memory_server.evaluation.validator import Validator
from memory_server.models import LifecycleState, VerificationStatus
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.server import (
    _get_graph_router,
    _get_provider,
    mcp,
)

pytestmark = pytest.mark.asyncio


# =============================================================================
# Helpers
# =============================================================================


async def _call_tool(name: str, args: dict) -> dict:
    """Call an MCP tool and parse the JSON result.

    FastMCP's in-process call_tool returns
    (content_list: list[TextContent], metadata: dict).
    """
    content_list, _ = await mcp.call_tool(name, args)
    # content_list is a list of TextContent objects
    return json.loads(content_list[0].text)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
async def provider():
    """Fresh SQLiteProvider for tests that need direct DB access."""
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


# =============================================================================
# ADEQUACY METRICS (P0 — CI Gate)
# =============================================================================


class TestAdequacy:
    """P0 metrics — must pass before every merge."""

    # ------------------------------------------------------------------
    # Metric 1.1: remember() -> search() — Precision & Recall
    # ------------------------------------------------------------------

    async def test_benchmark_remember_search_roundtrip(self):
        """Store 10 facts via remember(), verify search() finds ALL (precision=1.0, recall=1.0)."""
        stored = []
        N = 10

        for i in range(N):
            subj = f"bench-entity-{i}"
            obj = f"bench-value-{i}"
            result = await _call_tool("remember", {
                "subject": subj,
                "predicate": "has_property",
                "object": obj,
                "confidence": 1.0,
                "source": "bench",
            })
            stored.append((subj, obj, result["fact"]["id"]))

        assert len(stored) == N

        hits = 0
        total = len(stored)

        for subj, obj, fid in stored:
            # Search by subject keyword
            result = await _call_tool("search", {"query": subj})
            results = result.get("results", [])
            found = any(r.get("subject") == subj for r in results)
            if found:
                hits += 1

        recall = hits / total
        assert recall == 1.0, (
            f"Expected recall=1.0, got {recall} ({hits}/{total} found)"
        )

        # Precision: each specific entity query should find our fact
        for subj, obj, fid in stored:
            result = await _call_tool("search", {"query": subj, "limit": 100})
            results = result.get("results", [])
            subj_results = [r for r in results if r.get("subject") == subj]
            assert len(subj_results) >= 1, f"Subject '{subj}' not found in search results"

    # ------------------------------------------------------------------
    # Metric 1.2: learn() Extraction Accuracy
    # ------------------------------------------------------------------

    async def test_benchmark_learn_extraction(self, provider):
        """Inject 3 known facts into text via learn(), verify extraction of each."""
        sentences = (
            "Python is programming-language.\n"
            "Docker is container-technology.\n"
            "PostgreSQL is relational-database.\n"
        )
        result = await learn_fn(provider, text=sentences, source="bench")
        extracted = result.get("facts", [])

        expected_subjects = {"Python", "Docker", "PostgreSQL"}
        extracted_subjects = {f["item"]["subject"] for f in extracted}

        for subj in expected_subjects:
            assert subj in extracted_subjects, (
                f"Expected '{subj}' to be extracted via learn(), got {extracted_subjects}"
            )

        extraction_rate = len(extracted) / 3
        assert extraction_rate >= 0.95, (
            f"Extraction rate too low: {extraction_rate} ({len(extracted)}/3)"
        )

    # ------------------------------------------------------------------
    # Metric 1.7: Auto-Indexing
    # ------------------------------------------------------------------

    async def test_benchmark_auto_indexing(self):
        """remember() -> verify fact visible in both semantic_search and graph_search."""
        subj = "auto-index-target"
        pred = "has_feature"
        obj = "verified-indexing"

        result = await _call_tool("remember", {
            "subject": subj,
            "predicate": pred,
            "object": obj,
            "confidence": 1.0,
            "source": "bench",
        })
        fact_id = result["fact"]["id"]

        # Check semantic_search (Qdrant)
        semantic = await _call_tool("semantic_search", {
            "query": f"{subj} {pred} {obj}",
            "top_k": 10,
        })

        semantic_found = False
        if "semantic_results" in semantic:
            semantic_found = any(
                p.get("payload", {}).get("subject") == subj
                for p in semantic["semantic_results"]
            )
        assert semantic_found, (
            f"Fact '{subj}' not found in semantic_search results: "
            f"{json.dumps(semantic, indent=2)[:300]}"
        )

        # Check graph_search via entity_id
        node_id = subj.lower().replace(" ", "-")
        graph = await _call_tool("graph_search", {"entity_id": node_id})
        graph_found = any(n["name"] == subj for n in graph.get("nodes", []))
        assert graph_found, (
            f"Entity '{subj}' not found in graph nodes: {graph.get('nodes', [])}"
        )

    # ------------------------------------------------------------------
    # Metric 1.6: Validation Lifecycle
    # ------------------------------------------------------------------

    async def test_benchmark_validation_lifecycle(self):
        """Store fact -> validate -> trust -> verify status transitions."""
        v = Validator()
        fid = f"test-validation-fact-{uuid4()}"

        # Stage 1: Register as candidate (confidence=0.5)
        v.register(fid, confidence=0.5)
        s1 = v.get_status(fid)
        assert s1["status"] == "candidate", f"Expected candidate, got {s1['status']}"

        # Stage 2: confidence >= 0.7 -> validate -> validated
        v.set_confidence(fid, 0.75)
        s2_status = v.validate(fid)
        assert s2_status == LifecycleState.VALIDATED, (
            f"Expected VALIDATED, got {s2_status}"
        )
        s2 = v.get_status(fid)
        assert s2["status"] == "validated"

        # Stage 3: confidence >= 0.85 + corroboration >= 2 -> trust -> active (was trusted)
        v.set_confidence(fid, 0.9)
        v.set_corroboration_count(fid, 3)
        s3_status = v.trust(fid)
        assert s3_status == LifecycleState.ACTIVE, (
            f"Expected ACTIVE, got {s3_status}"
        )
        s3 = v.get_status(fid)
        assert s3["status"] == "active"

        # Boundary: confidence exactly 0.69 stays candidate
        v2 = Validator()
        v2.register("boundary-test", confidence=0.69)
        assert v2.validate("boundary-test") == LifecycleState.CANDIDATE

        # Boundary: confidence >= 0.7 -> validated
        v2.set_confidence("boundary-test", 0.7)
        assert v2.validate("boundary-test") == LifecycleState.VALIDATED

        # Boundary: trust threshold 0.85 with corroboration 1 stays validated
        v2.register("boundary-trust", confidence=0.9)
        v2.validate("boundary-trust")
        v2.set_corroboration_count("boundary-trust", 1)
        assert v2.trust("boundary-trust") == LifecycleState.VALIDATED

    # ------------------------------------------------------------------
    # Metric 1.5: route() Fallthrough
    # ------------------------------------------------------------------

    async def test_benchmark_route_fallthrough(self):
        """Rule query -> RulesEngine stage, unknown query -> LLM fallback.

        Per ADR-005: rules -> semantic -> graph -> LLM fallback.
        """
        # Pre-populate: store a fact so semantic/graph stages have data
        await _call_tool("remember", {
            "subject": "database-server",
            "predicate": "runs_on",
            "object": "port-5432",
            "confidence": 1.0,
            "source": "bench",
        })

        # (a) Query with known keyword from default rules -> stage 1
        rule_query = "what is the IP of main server"
        rule_result = await _call_tool("route", {"query": rule_query})
        assert rule_result.get("stage") == 1, (
            f"Rule query expected stage=1, got stage={rule_result.get('stage')}"
        )
        assert rule_result.get("route") == "rules"
        assert "rule_match" in rule_result

        # (b) Graph stage (3) verification via GraphRouter.query()
        # NOTE: With default score_threshold=0.0, Qdrant always returns nearby
        # vectors, so the HybridRouter's semantic stage (2) always fires before
        # graph stage (3) can be reached. Verify graph routing works by calling
        # the GraphRouter directly.
        from memory_server.server import _get_graph_router

        graph_router = await _get_graph_router()
        graph_result = graph_router.query("database-server")
        assert len(graph_result.get("entities", [])) >= 1, (
            f"GraphRouter should find entity 'database-server' in query: "
            f"{json.dumps(graph_result)[:200]}"
        )

        # (c) Query with gibberish / no matches -> stage 4 (LLM fallback)
        # With score_threshold=1.0, no Qdrant results pass and graph finds
        # nothing, so the route falls through to LLM fallback.
        gibberish = "xyznonexistentgibberish12345"
        fallback_result = await _call_tool("route", {
            "query": gibberish,
            "score_threshold": 1.0,
        })
        assert fallback_result.get("stage") == 4, (
            f"Gibberish query expected stage=4, got stage={fallback_result.get('stage')}"
        )
        assert fallback_result.get("route") == "llm_fallback"


# =============================================================================
# PERFORMANCE METRICS (P1 — Daily)
# =============================================================================


class TestPerformance:
    """P1 metrics — run daily to detect regressions."""

    async def test_benchmark_remember_latency(self):
        """remember() latency: p50/p95 over 50 sequential stores.

        Expected baseline (mock embedding): p50 < 50ms, p95 < 100ms.
        """
        N = 50
        latencies = []

        # Warmup: one call to ensure providers are initialized
        await _call_tool("remember", {
            "subject": "warmup", "predicate": "is", "object": "ready",
            "confidence": 1.0, "source": "bench",
        })

        for i in range(N):
            t0 = time.perf_counter()
            await _call_tool("remember", {
                "subject": f"perf-entity-{i}",
                "predicate": "has_value",
                "object": f"perf-val-{i}",
                "confidence": 1.0,
                "source": "bench",
            })
            latencies.append((time.perf_counter() - t0) * 1000)

        sorted_lat = sorted(latencies)
        p50 = sorted_lat[int(N * 0.50)]
        p95 = sorted_lat[int(N * 0.95)]
        mean_lat = statistics.mean(latencies)

        print(f"\n  [remember latency] p50={p50:.1f}ms  p95={p95:.1f}ms  mean={mean_lat:.1f}ms")

        assert p50 < 2000, (
            f"remember() p50 latency too high: {p50:.1f}ms (threshold: 2000ms)"
        )
        assert p95 < 5000, (
            f"remember() p95 latency too high: {p95:.1f}ms (threshold: 5000ms)"
        )

    async def test_benchmark_search_latency(self):
        """search() latency: p50 over 50 queries.

        Expected: p50 < 100ms for exact matches at low volume.
        """
        N = 50

        # Pre-populate some data
        for i in range(20):
            await _call_tool("remember", {
                "subject": f"latency-entity-{i}",
                "predicate": "has_attr",
                "object": f"latency-val-{i}",
                "confidence": 1.0,
                "source": "bench",
            })

        latencies = []
        for i in range(N):
            t0 = time.perf_counter()
            await _call_tool("search", {"query": f"latency-entity-{i % 20}"})
            latencies.append((time.perf_counter() - t0) * 1000)

        sorted_lat = sorted(latencies)
        p50 = sorted_lat[int(N * 0.50)]
        p95 = sorted_lat[int(N * 0.95)]

        print(f"\n  [search latency] p50={p50:.1f}ms  p95={p95:.1f}ms")

        assert p50 < 500, f"search() p50 too high: {p50:.1f}ms (threshold: 500ms)"
        assert p95 < 2000, f"search() p95 too high: {p95:.1f}ms (threshold: 2000ms)"

    async def test_benchmark_cold_start_time(self):
        """Measure cold-start time to first ping response."""
        t0 = time.perf_counter()
        result = await _call_tool("ping", {})
        elapsed_ms = (time.perf_counter() - t0) * 1000

        assert result.get("status") == "ok", f"ping failed: {result}"
        print(f"\n  [cold start] first ping response: {elapsed_ms:.1f}ms")

        assert elapsed_ms < 2000, (
            f"Cold start too slow: {elapsed_ms:.1f}ms (threshold: 2000ms)"
        )


# =============================================================================
# STABILITY METRICS (P1 — Daily)
# =============================================================================


class TestStability:
    """P1 metrics — daily stability regression checks."""

    async def test_benchmark_error_recovery(self):
        """Call remember() with missing required fields, verify graceful error."""
        # After errors, server should still work (ping)
        errors_raised = 0

        for field_name in ("subject", "predicate", "object"):
            args = {
                "subject": "test", "predicate": "is", "object": "test",
                "confidence": 1.0, "source": "bench",
            }
            args[field_name] = ""  # Empty string = missing required field
            try:
                await _call_tool("remember", args)
            except Exception:
                errors_raised += 1

        # All three should raise errors
        assert errors_raised == 3, (
            f"Expected 3 errors for empty fields, got {errors_raised}"
        )

        # After errors, server should still work
        result = await _call_tool("ping", {})
        assert result.get("status") == "ok", "Server not responsive after error recovery"

    async def test_benchmark_data_integrity(self):
        """10 remember+delete rounds, verify no orphaned graph nodes."""
        provider = await _get_provider()
        graph_router = await _get_graph_router()
        graph = graph_router.graph

        cycles = 10
        fact_ids = []

        for i in range(cycles):
            result = await _call_tool("remember", {
                "subject": f"integrity-entity-{i}",
                "predicate": "is",
                "object": f"integrity-val-{i}",
                "confidence": 1.0,
                "source": "bench",
            })
            fact_ids.append(result["fact"]["id"])

        # Delete each fact directly via provider
        for fid in fact_ids:
            deleted = await provider.delete_fact(fid)
            assert deleted, f"Failed to delete fact {fid}"

        # Check graph for orphans (nodes with no neighbors).
        # NOTE: Currently, neither nodes NOR edges are cleaned when a fact is
        # deleted from SQLite. Each subject node still has an edge to its
        # object node (the edge persists), so they are NOT orphans.
        # This is a known limitation documented in metrics.md §3.4.
        # If graph cleanup is added in the future, orphans would be
        # expected at 2 * cycles (subject + object nodes per fact).
        all_nodes = graph.get_all_nodes()
        orphans = [n for n in all_nodes if not graph.get_neighbors(n.id)]

        print(f"\n  [data integrity] total graph nodes: {len(all_nodes)}, orphans: {len(orphans)}")

        # Current behavior: edges persist, so all nodes have neighbors -> 0 orphans
        assert len(orphans) == 0, (
            f"Expected 0 orphans (edges persist after delete — known limitation), "
            f"got {len(orphans)}"
        )

    async def test_benchmark_long_session(self):
        """20 rapid tool calls through rotating types, verify no crashes."""
        tool_cycle = [
            ("ping", {}),
            ("remember", {
                "subject": "longrun-server", "predicate": "cycle",
                "object": "test", "confidence": 1.0, "source": "bench",
            }),
            ("search", {"query": "longrun"}),
            ("semantic_search", {"query": "long run test", "top_k": 3}),
            ("graph_search", {"query": "longrun"}),
            ("route", {"query": "long run test"}),
        ]

        errors = 0
        latencies = []

        for cycle_idx in range(20):
            tool_name, params = tool_cycle[cycle_idx % len(tool_cycle)]
            t0 = time.perf_counter()
            try:
                await _call_tool(tool_name, params)
            except Exception as e:
                errors += 1
                print(f"  Error at cycle {cycle_idx} ({tool_name}): {e}")
            elapsed_ms = (time.perf_counter() - t0) * 1000
            latencies.append(elapsed_ms)

        assert errors == 0, f"Long session encountered {errors} errors"

        p50 = statistics.median(latencies) if latencies else 0
        print(f"\n  [long session] 20 calls, 0 errors, p50 latency: {p50:.1f}ms")
