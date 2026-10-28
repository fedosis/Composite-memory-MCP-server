"""Bounded C9 metrics and an unmounted, permission-checked exposition seam.

No default/process registry, auto-describe collectors or dynamic label keys.
The containment count is local diagnostic state, not an extra metric family.
S06 authenticates credentials and selects/mounts the management bind.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable
from threading import Lock
from typing import Literal, get_args

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

from memory_server.gateway_contracts.dto import ErrorCode, Operation
from memory_server.gateway_contracts.ports import RequestContext

_OPERATIONS = frozenset(get_args(Operation))
_STAGES = frozenset({"fts", "semantic", "graph", "merge", "context"})
_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


class TelemetryError(Exception):
    def __init__(self, code: ErrorCode) -> None:
        self.code = code
        super().__init__(code)


class _GatewayRegistry(CollectorRegistry):
    def collect(self):
        # Prometheus client adds optional creation-time series; C9 does not.
        for family in super().collect():
            family.samples = [sample for sample in family.samples if not sample.name.endswith("_created")]
            yield family


class BoundedMetricsSink:
    """C1 MetricsSink, with at most 100 configured profiles plus _unknown.

    Invalid operation/stage/duration is rejected without recording a series.
    Production collector errors never escape sink methods. failure_count counts
    contained calls once; it is not a new MetricsSink method or metric family.
    Unknown projection lag removes a previously known gauge instead of lying 0.
    """

    def __init__(self, profiles: Iterable[str]) -> None:
        configured = frozenset(profiles)
        if len(configured) > 100 or any(
            not isinstance(p, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", p) or p == "_unknown"
            for p in configured
        ):
            raise TelemetryError("E_VALIDATION")
        self.profiles = configured
        self.registry_name = "cmms_gateway"
        self.registry = _GatewayRegistry()
        self._lock = Lock()
        self.failure_count = 0
        self.requests = Counter("cmms_http_requests", "Completed HTTP requests", (
            "operation", "status_class", "profile"), registry=self.registry)
        self.http_duration = Histogram("cmms_http_request_duration_seconds", "HTTP latency", (
            "operation", "status_class", "profile"), buckets=_BUCKETS, registry=self.registry)
        self.retrieval_duration = Histogram("cmms_retrieval_duration_seconds", "Retrieval latency", (
            "stage", "profile"), buckets=_BUCKETS, registry=self.registry)
        self.pending = Gauge("cmms_outbox_pending", "Pending projection events", ("profile",), registry=self.registry)
        self.vector_lag = Gauge("cmms_vector_index_lag", "Vector lag", ("profile",), registry=self.registry)
        self.graph_lag = Gauge("cmms_graph_index_lag", "Graph lag", ("profile",), registry=self.registry)
        self.conflicts = Counter("cmms_version_conflicts", "Version conflicts", (
            "operation", "profile"), registry=self.registry)

    def _profile(self, value: str | None) -> str:
        return value if value in self.profiles else "_unknown"

    def _failed(self) -> None:
        with self._lock:
            self.failure_count += 1

    @staticmethod
    def _duration(value: float) -> bool:
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0

    def http_completed(
        self, ctx: RequestContext, operation: Operation, status: int, duration_seconds: float,
    ) -> None:
        try:
            if operation not in _OPERATIONS or type(status) is not int or not 100 <= status <= 599:
                return
            if not self._duration(duration_seconds):
                return
            labels = (operation, f"{status // 100}xx", self._profile(ctx.profile_id))
            self.requests.labels(*labels).inc()
            self.http_duration.labels(*labels).observe(duration_seconds)
        except Exception:
            self._failed()

    def retrieval_completed(
        self, ctx: RequestContext, stage: Literal["fts", "semantic", "graph", "merge", "context"],
        duration_seconds: float, error_code: ErrorCode | None,
    ) -> None:
        try:
            if stage not in _STAGES or not self._duration(duration_seconds):
                return
            self.retrieval_duration.labels(stage, self._profile(ctx.profile_id)).observe(duration_seconds)
        except Exception:
            self._failed()

    def projection_snapshot(
        self, profile_id: str, pending: int, vector_lag: int | None, graph_lag: int | None,
    ) -> None:
        try:
            if any(type(v) is not int or v < 0 for v in (pending, vector_lag, graph_lag) if v is not None):
                return
            if pending is None:
                return
            profile = self._profile(profile_id)
            self.pending.labels(profile).set(pending)
            for gauge, value in ((self.vector_lag, vector_lag), (self.graph_lag, graph_lag)):
                if value is not None:
                    gauge.labels(profile).set(value)
                else:
                    # Public API: drop the labelled child when the lag is unknown.
                    gauge.remove(profile)
        except Exception:
            self._failed()


def metrics_handler(ctx: RequestContext, sink: BoundedMetricsSink) -> tuple[str, bytes]:
    """GET /metrics body seam; ctx must come from S06 authentication, not wire input."""
    if "token.manage" not in ctx.principal.permissions:
        raise TelemetryError("E_PERMISSION_DENIED")
    try:
        return CONTENT_TYPE, generate_latest(sink.registry)
    except Exception:
        raise TelemetryError("E_INTERNAL") from None
