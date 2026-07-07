"""Heavy load and stress test for the Composite Memory MCP Server.

Scenarios (all in one session):
1. VOLUME: 1000 facts via remember() — latency, search verification, memory
2. LEARN THROUGHPUT: 100 learn() calls — extraction rate
3. SEARCH STRESS: 500 search() calls — sustained load
4. MIXED WORKLOAD: 1000 iterations across all 9 tools — per-tool latency
5. DATA INTEGRITY: SQLite / Qdrant / graph consistency after load
"""

import asyncio
import json
import os
import re
import statistics
import time
from uuid import uuid4

import pytest

from memory_server.server import (
    _get_graph_router,
    _get_provider,
    _get_router,
    mcp,
)

pytestmark = pytest.mark.asyncio

# ─── helpers ────────────────────────────────────────────────────────────


async def _call_tool(name: str, args: dict) -> dict:
    """Call an MCP tool and parse the JSON result."""
    content_list, _ = await mcp.call_tool(name, args)
    return json.loads(content_list[0].text)


def _get_vmrss_kb() -> int:
    """Return current VmRSS in KB from /proc/self/status."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except FileNotFoundError:
        return 0
    return 0


def _percentile(sorted_data: list[float], p: float) -> float:
    """Compute the p-th percentile of a sorted list."""
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = int(k)
    c = k - f
    if f + 1 < len(sorted_data):
        return sorted_data[f] * (1 - c) + sorted_data[f + 1] * c
    return sorted_data[-1]


# ─── Scenario 1: Volume — 1000 facts ────────────────────────────────────


async def _scenario_volume() -> dict:
    """Store 1000 unique facts, measure latency, verify retrieval."""
    report = {}
    N = 1000
    batch_size = 50

    facts = []
    for i in range(N):
        facts.append({
            "subject": f"subject_{i}",
            "predicate": "has_property",
            "object": f"object_{i}",
            "confidence": 1.0,
            "source": "loadtest",
        })

    latencies = []
    stored_ids = []
    stored_subject_obj = []

    t0 = time.perf_counter()

    # Store in batches
    for batch_start in range(0, N, batch_size):
        batch = facts[batch_start:batch_start + batch_size]
        for fact in batch:
            t_start = time.perf_counter()
            result = await _call_tool("remember", fact)
            latencies.append((time.perf_counter() - t_start) * 1000)
            stored_ids.append(result["fact"]["id"])
            stored_subject_obj.append((fact["subject"], fact["object"]))

    total_time = time.perf_counter() - t0
    sorted_lat = sorted(latencies)

    report["total_time_s"] = round(total_time, 2)
    report["total_facts"] = len(stored_ids)
    report["avg_latency_ms"] = round(statistics.mean(latencies), 2)
    report["p50_ms"] = round(_percentile(sorted_lat, 50), 2)
    report["p95_ms"] = round(_percentile(sorted_lat, 95), 2)
    report["p99_ms"] = round(_percentile(sorted_lat, 99), 2)

    print(f"\n  [volume] {N} facts stored in {total_time:.2f}s")
    print(f"    avg={report['avg_latency_ms']:.1f}ms p50={report['p50_ms']:.1f}ms "
          f"p95={report['p95_ms']:.1f}ms p99={report['p99_ms']:.1f}ms")

    # Verify 10 random facts via search
    import random
    random.seed(42)
    search_indices = random.sample(range(N), 10)
    search_hits = 0
    for idx in search_indices:
        subj, obj = stored_subject_obj[idx]
        result = await _call_tool("search", {"query": subj, "limit": 10})
        found = any(r.get("subject") == subj for r in result.get("results", []))
        if found:
            search_hits += 1
    report["search_verification_rate"] = round(search_hits / 10, 2)
    print(f"    search verification: {search_hits}/10 found")

    # Verify semantic_search for a known pattern
    semantic_pivot = random.randint(0, N - 1)
    sem_subj = f"subject_{semantic_pivot}"
    sem_result = await _call_tool("semantic_search", {
        "query": sem_subj,
        "top_k": 10,
    })
    sem_found = False
    if "semantic_results" in sem_result:
        sem_found = any(
            p.get("payload", {}).get("subject") == sem_subj
            for p in sem_result["semantic_results"]
        )
    report["semantic_found"] = sem_found
    print(f"    semantic_search for '{sem_subj}': {'FOUND' if sem_found else 'NOT FOUND'}")

    return report


# ─── Scenario 2: Learn throughput — 100 calls ────────────────────────────


async def _scenario_learn() -> dict:
    """Call learn() 100 times with synthetic text, measure extraction rate."""
    N = 100
    facts_per_text = 3  # 2-3 facts per snippet
    total_injected = 0
    total_extracted = 0
    latencies = []

    t0 = time.perf_counter()

    for i in range(N):
        # Build a text snippet with 3 "X is Y" facts and 1 decision
        prefix = f"project_{i}_{uuid4().hex[:6]}"
        snippet = (
            f"{prefix}_alpha is feature_{i}_a. "
            f"{prefix}_beta is feature_{i}_b. "
            f"{prefix}_gamma is feature_{i}_c. "
            f"Decision: I chose {prefix}_option because it scales better."
        )
        total_injected += 3  # 3 facts per snippet

        t_start = time.perf_counter()
        result = await _call_tool("learn", {"text": snippet, "source": "loadtest"})
        latencies.append((time.perf_counter() - t_start) * 1000)

        extracted = result.get("facts", [])
        total_extracted += len(extracted)

    total_time = time.perf_counter() - t0
    sorted_lat = sorted(latencies)

    report = {
        "total_time_s": round(total_time, 2),
        "learn_calls": N,
        "total_injected_facts": total_injected,
        "total_extracted_facts": total_extracted,
        "extraction_rate_pct": round(total_extracted / total_injected * 100, 1),
        "extraction_facts_per_sec": round(total_extracted / total_time, 1),
        "avg_latency_ms": round(statistics.mean(latencies), 2),
        "p50_ms": round(_percentile(sorted_lat, 50), 2),
        "p95_ms": round(_percentile(sorted_lat, 95), 2),
    }

    print(f"\n  [learn] {N} calls in {total_time:.2f}s")
    print(f"    extracted {total_extracted}/{total_injected} facts "
          f"({report['extraction_rate_pct']}%) at {report['extraction_facts_per_sec']}/s")
    print(f"    avg={report['avg_latency_ms']:.1f}ms p50={report['p50_ms']:.1f}ms "
          f"p95={report['p95_ms']:.1f}ms")

    assert total_extracted >= total_injected * 0.70, (
        f"Extraction rate too low: {total_extracted}/{total_injected} "
        f"({report['extraction_rate_pct']}%)"
    )

    return report


# ─── Scenario 3: Search stress — 500 calls ──────────────────────────────


async def _scenario_search_stress() -> dict:
    """500 search() calls with random queries, mix of hit/partial/no-match."""
    N = 500
    queries = []
    expected_hits = []

    # Queries: 40% exact-match, 30% partial-match, 30% no-match
    for i in range(N):
        if i < 200:
            # Exact match: existing subject
            idx = i % 1000
            queries.append({"query": f"subject_{idx}", "limit": 5})
            expected_hits.append(True)
        elif i < 350:
            # Partial match
            queries.append({"query": f"loadtest", "limit": 5})
            expected_hits.append(True)
        else:
            # No-match
            queries.append({"query": f"xyznonexistent_{uuid4().hex}", "limit": 5})
            expected_hits.append(False)

    latencies = []
    errors = 0
    zero_results = 0

    t0 = time.perf_counter()

    for i in range(N):
        t_start = time.perf_counter()
        try:
            result = await _call_tool("search", queries[i])
            latencies.append((time.perf_counter() - t_start) * 1000)
            total = result.get("total", 0)
            if total == 0:
                zero_results += 1
        except Exception:
            errors += 1
            latencies.append(0)

    total_time = time.perf_counter() - t0
    sorted_lat = sorted([l for l in latencies if l > 0])

    report = {
        "total_calls": N,
        "total_time_s": round(total_time, 2),
        "throughput_calls_per_sec": round(N / total_time, 1),
        "p50_ms": round(_percentile(sorted_lat, 50), 2),
        "p95_ms": round(_percentile(sorted_lat, 95), 2),
        "p99_ms": round(_percentile(sorted_lat, 99), 2),
        "errors": errors,
        "error_rate_pct": round(errors / N * 100, 2),
        "zero_result_count": zero_results,
        "zero_result_rate_pct": round(zero_results / N * 100, 2),
    }

    print(f"\n  [search-stress] {N} calls in {total_time:.2f}s "
          f"({report['throughput_calls_per_sec']}/s)")
    print(f"    p50={report['p50_ms']:.1f}ms p95={report['p95_ms']:.1f}ms "
          f"p99={report['p99_ms']:.1f}ms")
    print(f"    errors={errors} ({report['error_rate_pct']}%), "
          f"zero-results={zero_results} ({report['zero_result_rate_pct']}%)")

    assert errors == 0, f"Search stress had {errors} errors!"
    return report


# ─── Scenario 4: Mixed workload — 9 tools ───────────────────────────────


async def _scenario_mixed_workload() -> dict:
    """Rotate through all 9 tools for 1000 iterations, measure per-tool latency."""
    MAX_ITERATIONS = 1000
    MAX_DURATION = 600  # 10 minutes in seconds

    # Tool distribution: remember=20%, search=20%, semantic_search=10%,
    # learn=10%, graph_search=10%, route=10%, get_context=10%, audit=5%, ping=5%
    tool_weights = [
        ("remember", 0.20),
        ("search", 0.20),
        ("semantic_search", 0.10),
        ("learn", 0.10),
        ("graph_search", 0.10),
        ("route", 0.10),
        ("get_context", 0.10),
        ("audit", 0.05),
        ("ping", 0.05),
    ]

    # Build a cyclic schedule
    schedule = []
    for tool_name, weight in tool_weights:
        count = int(weight * MAX_ITERATIONS)
        schedule.extend([tool_name] * count)
    # Fill remaining slots with ping
    while len(schedule) < MAX_ITERATIONS:
        schedule.append("ping")

    import random
    random.seed(42)
    random.shuffle(schedule)

    # Per-tool stats
    per_tool: dict[str, list[float]] = {}
    per_tool_errors: dict[str, int] = {}
    for t, _ in tool_weights:
        per_tool[t] = []
        per_tool_errors[t] = 0

    fact_counter = [0, 2000]  # continue from where volume left off

    def _next_fact_pair():
        fact_counter[0] += 1
        return f"mixed_subj_{fact_counter[0]}", f"mixed_obj_{fact_counter[0]}"

    iterations_done = 0
    t_start = time.perf_counter()

    for idx, tool_name in enumerate(schedule):
        if time.perf_counter() - t_start >= MAX_DURATION:
            iterations_done = idx
            break
        iterations_done = idx + 1

        args = {}
        if tool_name == "remember":
            s, o = _next_fact_pair()
            args = {"subject": s, "predicate": "is", "object": o,
                    "confidence": 1.0, "source": "mixed"}
        elif tool_name == "search":
            args = {"query": f"subject_{random.randint(0, 1999)}", "limit": 5}
        elif tool_name == "semantic_search":
            args = {"query": f"test query {random.randint(0, 100)}", "top_k": 5}
        elif tool_name == "learn":
            args = {"text": f"mixed_{idx} is test-fact-{idx}.", "source": "mixed"}
        elif tool_name == "graph_search":
            args = {"query": f"subject_{random.randint(0, 1999)}"}
        elif tool_name == "route":
            args = {"query": f"route query {random.randint(0, 100)}"}
        elif tool_name == "get_context":
            args = {"task": f"subject_{random.randint(0, 1999)}"}
        elif tool_name == "audit":
            args = {"audit_type": "full"}
        elif tool_name == "ping":
            args = {}

        t0 = time.perf_counter()
        try:
            await _call_tool(tool_name, args)
            per_tool[tool_name].append((time.perf_counter() - t0) * 1000)
        except Exception as e:
            per_tool_errors[tool_name] += 1
            per_tool[tool_name].append((time.perf_counter() - t0) * 1000)

    total_time = time.perf_counter() - t_start

    # Build report
    report = {
        "iterations": iterations_done,
        "total_time_s": round(total_time, 2),
        "throughput_calls_per_sec": round(iterations_done / total_time, 1),
        "per_tool": {},
    }

    print(f"\n  [mixed] {iterations_done} iterations in {total_time:.2f}s "
          f"({report['throughput_calls_per_sec']}/s)")

    for tool_name, _ in tool_weights:
        lats = per_tool.get(tool_name, [])
        errs = per_tool_errors.get(tool_name, 0)
        if lats:
            sorted_l = sorted(lats)
            t_report = {
                "calls": len(lats),
                "p50_ms": round(_percentile(sorted_l, 50), 2),
                "p95_ms": round(_percentile(sorted_l, 95), 2),
                "errors": errs,
                "error_rate_pct": round(errs / len(lats) * 100, 2),
            }
        else:
            t_report = {"calls": 0, "p50_ms": 0, "p95_ms": 0,
                        "errors": errs, "error_rate_pct": 0}
        report["per_tool"][tool_name] = t_report
        print(f"    {tool_name:20s}: calls={t_report['calls']:4d}  "
              f"p50={t_report['p50_ms']:8.1f}ms  p95={t_report['p95_ms']:8.1f}ms  "
              f"errors={t_report['errors']}")

    return report


# ─── Scenario 5: Data integrity after load ──────────────────────────────


async def _scenario_data_integrity() -> dict:
    """Count SQLite facts, graph nodes, Qdrant points — verify consistency."""
    provider = await _get_provider()
    router = await _get_router()
    graph_router = await _get_graph_router()
    graph = graph_router.graph

    # SQLite fact count
    sqlite_facts = await provider.search_facts(text="", limit=100000)
    sqlite_count = len(sqlite_facts)
    print(f"\n  [integrity] SQLite facts: {sqlite_count}")

    # Graph node count
    graph_nodes = graph.get_all_nodes()
    graph_count = len(graph_nodes)
    print(f"    Graph nodes: {graph_count}")

    # Qdrant point count via scroll
    qdrant = router._qdrant
    qdrant_points = await qdrant.scroll(limit=100000)
    qdrant_count = len(qdrant_points)
    print(f"    Qdrant points: {qdrant_count}")

    # Consistency check
    sqlite_qdrant_ok = sqlite_count == qdrant_count
    discrepancies = []

    if not sqlite_qdrant_ok:
        discrepancies.append(
            f"SQLite count ({sqlite_count}) != Qdrant count ({qdrant_count})"
        )

    report = {
        "sqlite_facts": sqlite_count,
        "graph_nodes": graph_count,
        "qdrant_points": qdrant_count,
        "sqlite_eq_qdrant": sqlite_qdrant_ok,
        "discrepancies": discrepancies,
    }

    print(f"    SQLite == Qdrant: {sqlite_qdrant_ok}")
    if discrepancies:
        for d in discrepancies:
            print(f"    DISCREPANCY: {d}")

    return report


# ─── Memory measurement helper ──────────────────────────────────────────


async def _measure_memory() -> dict:
    """Get current memory usage."""
    return {"vmrss_kb": _get_vmrss_kb()}


# ═══════════════════════════════════════════════════════════════════════
# Main test — runs all 5 scenarios sequentially
# ═══════════════════════════════════════════════════════════════════════


class TestLoadTest:
    """Full load test suite — all scenarios in sequence."""

    async def test_full_loadtest(self):
        """Run all 5 load test scenarios sequentially and report results."""
        print("\n" + "=" * 72)
        print("  COMPOSITE MEMORY MCP SERVER — LOAD TEST")
        print("=" * 72)

        results = {}

        # ── Warmup ──
        print("\n--- Warmup (cold start + ping) ---")
        t0 = time.perf_counter()
        await _call_tool("ping", {})
        warmup_ms = (time.perf_counter() - t0) * 1000
        print(f"  First ping: {warmup_ms:.1f}ms (includes model load)")

        mem_before = await _measure_memory()
        print(f"  VmRSS before: {mem_before['vmrss_kb']} KB")

        # ── Scenario 1: Volume ──
        print("\n" + "-" * 72)
        print("  SCENARIO 1: VOLUME — 1000 facts via remember()")
        print("-" * 72)
        mem_vol_before = await _measure_memory()
        results["volume"] = await _scenario_volume()
        mem_vol_after = await _measure_memory()
        vol_delta = mem_vol_after["vmrss_kb"] - mem_vol_before["vmrss_kb"]
        print(f"  VmRSS delta after volume: {vol_delta:+d} KB")

        # ── Scenario 2: Learn throughput ──
        print("\n" + "-" * 72)
        print("  SCENARIO 2: LEARN THROUGHPUT — 100 learn() calls")
        print("-" * 72)
        mem_learn_before = await _measure_memory()
        results["learn"] = await _scenario_learn()
        mem_learn_after = await _measure_memory()
        learn_delta = mem_learn_after["vmrss_kb"] - mem_learn_before["vmrss_kb"]
        print(f"  VmRSS delta after learn: {learn_delta:+d} KB")

        # ── Scenario 3: Search stress ──
        print("\n" + "-" * 72)
        print("  SCENARIO 3: SEARCH STRESS — 500 search() calls")
        print("-" * 72)
        mem_search_before = await _measure_memory()
        results["search_stress"] = await _scenario_search_stress()
        mem_search_after = await _measure_memory()
        search_delta = mem_search_after["vmrss_kb"] - mem_search_before["vmrss_kb"]
        print(f"  VmRSS delta after search-stress: {search_delta:+d} KB")

        # ── Scenario 4: Mixed workload ──
        print("\n" + "-" * 72)
        print("  SCENARIO 4: MIXED WORKLOAD — 1000 iterations across 9 tools")
        print("-" * 72)
        mem_mixed_before = await _measure_memory()
        results["mixed"] = await _scenario_mixed_workload()
        mem_mixed_after = await _measure_memory()
        mixed_delta = mem_mixed_after["vmrss_kb"] - mem_mixed_before["vmrss_kb"]
        print(f"  VmRSS delta after mixed: {mixed_delta:+d} KB")

        # ── Memory summary ──
        mem_after = await _measure_memory()
        total_delta = mem_after["vmrss_kb"] - mem_before["vmrss_kb"]
        results["memory"] = {
            "vmrss_before_kb": mem_before["vmrss_kb"],
            "vmrss_after_kb": mem_after["vmrss_kb"],
            "vmrss_delta_kb": total_delta,
            "volume_delta_kb": vol_delta,
            "learn_delta_kb": learn_delta,
            "search_delta_kb": search_delta,
            "mixed_delta_kb": mixed_delta,
        }
        print(f"\n  Total VmRSS delta: {total_delta:+d} KB "
              f"({mem_before['vmrss_kb']} -> {mem_after['vmrss_kb']})")

        # ── Scenario 5: Data integrity ──
        print("\n" + "-" * 72)
        print("  SCENARIO 5: DATA INTEGRITY AFTER LOAD")
        print("-" * 72)
        results["integrity"] = await _scenario_data_integrity()

        # ── Final summary ──
        print("\n" + "=" * 72)
        print("  LOAD TEST COMPLETE — SUMMARY")
        print("=" * 72)

        summary_parts = []

        # Volume
        v = results["volume"]
        summary_parts.append(
            f"Volume: {v['total_facts']} facts in {v['total_time_s']}s "
            f"(avg {v['avg_latency_ms']}ms, p50 {v['p50_ms']}ms, "
            f"p95 {v['p95_ms']}ms, p99 {v['p99_ms']}ms)"
        )

        # Learn
        l = results["learn"]
        summary_parts.append(
            f"Learn: {l['learn_calls']} calls in {l['total_time_s']}s, "
            f"{l['total_extracted_facts']}/{l['total_injected_facts']} facts "
            f"({l['extraction_rate_pct']}%) at {l['extraction_facts_per_sec']}/s"
        )

        # Search stress
        ss = results["search_stress"]
        summary_parts.append(
            f"Search-stress: {ss['total_calls']} calls in {ss['total_time_s']}s "
            f"({ss['throughput_calls_per_sec']}/s), p50 {ss['p50_ms']}ms, "
            f"p95 {ss['p95_ms']}ms, p99 {ss['p99_ms']}ms, "
            f"errors {ss['error_rate_pct']}%"
        )

        # Mixed
        mx = results["mixed"]
        summary_parts.append(
            f"Mixed: {mx['iterations']} iterations in {mx['total_time_s']}s "
            f"({mx['throughput_calls_per_sec']}/s)"
        )
        # Per-tool in mixed
        for tname, trep in mx["per_tool"].items():
            summary_parts.append(
                f"  {tname}: {trep['calls']} calls, p50 {trep['p50_ms']}ms, "
                f"p95 {trep['p95_ms']}ms, errors {trep['error_rate_pct']}%"
            )

        # Memory
        mem = results["memory"]
        summary_parts.append(
            f"Memory: {mem['vmrss_before_kb']} -> {mem['vmrss_after_kb']} KB "
            f"(Δ {mem['vmrss_delta_kb']:+d} KB)"
        )

        # Integrity
        ig = results["integrity"]
        summary_parts.append(
            f"Integrity: SQLite {ig['sqlite_facts']}, "
            f"Graph {ig['graph_nodes']} nodes, "
            f"Qdrant {ig['qdrant_points']} points, "
            f"SQLite==Qdrant: {ig['sqlite_eq_qdrant']}"
        )
        if ig["discrepancies"]:
            summary_parts.append(f"  DISCREPANCIES: {'; '.join(ig['discrepancies'])}")

        summary_text = " | ".join(summary_parts)
        print(f"\n  Load test complete — {summary_text}")

        # Store summary as class attribute for external access
        TestLoadTest.summary = summary_text
        TestLoadTest.results = results

        # No crash assertions
        assert v["search_verification_rate"] >= 0.9, (
            f"Search verification rate too low: {v['search_verification_rate']}"
        )
        assert ss["errors"] == 0, (
            f"Search stress had {ss['errors']} errors!"
        )
        assert ig["sqlite_eq_qdrant"] or abs(ig["sqlite_facts"] - ig["qdrant_points"]) < 100, (
            f"Data integrity violation: SQLite {ig['sqlite_facts']} != Qdrant {ig['qdrant_points']}"
        )

        # Return results so pytest can print them
        return results
