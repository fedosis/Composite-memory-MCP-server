"""OpenTelemetry + Prometheus metrics collection for the MCP memory server.

Provides a MetricsCollector singleton that tracks tool call counts,
latency histograms, error rates, and drift detection gauges.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Generator

import prometheus_client
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

# Singleton registry shared across the module
metrics_registry = prometheus_client.CollectorRegistry()


class MetricsCollector:
    """Collects and exposes Prometheus metrics for MCP tool observability.

    All metric objects are lazily created on the module-level registry.
    """

    def __init__(self, registry: prometheus_client.Registry = metrics_registry) -> None:
        self._registry = registry

        # --- Counters ---
        self.tool_calls_total = prometheus_client.Counter(
            "tool_calls_total",
            "Total MCP tool calls",
            ["tool", "status"],
            registry=registry,
        )
        self.tool_error_total = prometheus_client.Counter(
            "tool_error_total",
            "Total tool errors",
            ["tool"],
            registry=registry,
        )
        self.reindex_repair_total = prometheus_client.Counter(
            "reindex_repair_total",
            "Reindex repairs triggered",
            registry=registry,
        )
        self.sqlite_busy_events_total = prometheus_client.Counter(
            "sqlite_busy_events_total",
            "SQLite WAL busy events",
            registry=registry,
        )

        # --- Histograms for latency ---
        self.search_latency_ms = prometheus_client.Histogram(
            "search_latency_ms",
            "Search latency in ms",
            buckets=[1, 5, 10, 25, 50, 100, 250, 500],
            registry=registry,
        )
        self.semantic_search_latency_ms = prometheus_client.Histogram(
            "semantic_search_latency_ms",
            "Semantic search latency in ms",
            buckets=[10, 25, 50, 100, 250, 500, 1000],
            registry=registry,
        )
        self.remember_latency_ms = prometheus_client.Histogram(
            "remember_latency_ms",
            "Remember latency in ms",
            buckets=[5, 10, 25, 50, 100, 250, 500],
            registry=registry,
        )

        # --- Gauges ---
        self.derived_index_drift = prometheus_client.Gauge(
            "derived_index_drift",
            "SQL/vector index drift count",
            registry=registry,
        )

    # ------------------------------------------------------------------
    # Context manager for timing and recording tool calls
    # ------------------------------------------------------------------

    @contextmanager
    def tool_call(
        self, tool_name: str
    ) -> Generator[dict[str, Any], None, None]:
        """Context manager that records tool call metrics.

        Usage::

            with collector.tool_call("search") as ctx:
                result = await search_fn(...)
                ctx["status"] = "success"

        Sets *status* and *latency_ms* on the context dict automatically.
        On exception, sets status to "error" and increments error counter.
        """
        start = time.perf_counter()
        ctx: dict[str, Any] = {"status": "success", "latency_ms": 0.0}
        try:
            yield ctx
        except Exception:
            ctx["status"] = "error"
            self.tool_error_total.labels(tool=tool_name).inc()
            # Re-raise so the caller can handle it normally
            raise
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            ctx["latency_ms"] = elapsed_ms
            self.tool_calls_total.labels(
                tool=tool_name, status=ctx["status"]
            ).inc()

            # Record latency in the appropriate histogram
            self._record_latency(tool_name, elapsed_ms)

    def _record_latency(self, tool_name: str, elapsed_ms: float) -> None:
        """Route latency recording to the correct histogram."""
        if tool_name == "search":
            self.search_latency_ms.observe(elapsed_ms)
        elif tool_name == "semantic_search":
            self.semantic_search_latency_ms.observe(elapsed_ms)
        elif tool_name == "remember":
            self.remember_latency_ms.observe(elapsed_ms)

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def update_drift(self, drift_count: float) -> None:
        """Update the derived_index_drift gauge with the latest count."""
        self.derived_index_drift.set(drift_count)

    def inc_busy_event(self) -> None:
        """Increment the SQLite busy events counter."""
        self.sqlite_busy_events_total.inc()

    def inc_reindex_repair(self) -> None:
        """Increment the reindex repair counter."""
        self.reindex_repair_total.inc()

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------

    def generate_latest(self) -> bytes:
        """Return the registry content in Prometheus text format."""
        return prometheus_client.generate_latest(self._registry)


# Module-level singleton for easy importing
_collector: MetricsCollector | None = None


def get_collector() -> MetricsCollector:
    """Return the module-level MetricsCollector singleton."""
    global _collector
    if _collector is None:
        _collector = MetricsCollector()
    return _collector


def generate_latest() -> bytes:
    """Convenience: generate latest metrics from the singleton collector."""
    return get_collector().generate_latest()
