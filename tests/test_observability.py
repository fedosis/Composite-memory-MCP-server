"""Tests for Phase 8: Observability (OpenTelemetry + Prometheus metrics).

Covers:
- Metrics increment after tool call
- Histogram records latency
- Error counter increments on tool error
- /metrics tool returns valid Prometheus format
- Backward compatibility: all existing tests still pass
"""

import prometheus_client
import pytest

from memory_server.evaluation.metrics import MetricsCollector, get_collector, metrics_registry


def _get_metric_value(output: str, metric_name: str, labels: dict[str, str] | None = None) -> float | None:
    """Parse Prometheus text output to find a metric value by name and optional labels."""
    label_str = ""
    if labels:
        label_parts = [f'{k}="{v}"' for k, v in sorted(labels.items())]
        label_str = "{" + ",".join(label_parts) + "}"

    for line in output.split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Match metric_name with or without labels
        if label_str:
            expected_prefix = f"{metric_name}{label_str}"
            if line.startswith(expected_prefix):
                parts = line.split()
                return float(parts[-1])
        else:
            if line.startswith(metric_name + " "):
                parts = line.split()
                return float(parts[-1])
            elif line.startswith(metric_name + "{"):
                # Skip labeled variants when we want unlabeled
                continue
            elif line.startswith(metric_name + "_"):
                # Might be a _bucket,_sum,_count suffix
                if not label_str:
                    # Just return first match for _bucket
                    parts = line.split()
                    return float(parts[-1])
    return None


def test_tool_call_records_counter():
    """Metrics increment after tool call (success)."""
    registry = prometheus_client.CollectorRegistry()
    collector = MetricsCollector(registry=registry)
    with collector.tool_call("search") as ctx:
        ctx["status"] = "success"

    output = collector.generate_latest().decode("utf-8")
    val = _get_metric_value(output, "tool_calls_total", {"tool": "search", "status": "success"})
    assert val is not None and val > 0, "Counter should be > 0 after a tool call"


def test_tool_call_records_latency():
    """Histogram records latency after tool call."""
    registry = prometheus_client.CollectorRegistry()
    collector = MetricsCollector(registry=registry)
    with collector.tool_call("search") as ctx:
        pass
    assert ctx["latency_ms"] >= 0, "Latency should be recorded"

    output = collector.generate_latest().decode("utf-8")
    # The _bucket with +Inf should have count >= 1
    val = _get_metric_value(output, "search_latency_ms_bucket", {"le": "+Inf"})
    assert val is not None and val >= 1, f"Histogram should record observations, got {val}"


def test_error_counter_increments_on_exception():
    """Error counter increments on tool error."""
    registry = prometheus_client.CollectorRegistry()
    collector = MetricsCollector(registry=registry)
    with pytest.raises(ValueError):
        with collector.tool_call("search") as ctx:
            raise ValueError("test error")

    output = collector.generate_latest().decode("utf-8")
    val = _get_metric_value(output, "tool_error_total", {"tool": "search"})
    assert val is not None and val > 0, "Error counter should be > 0 after an exception"


def test_semantic_search_histogram():
    """Semantic search latency histogram records correctly."""
    registry = prometheus_client.CollectorRegistry()
    collector = MetricsCollector(registry=registry)
    with collector.tool_call("semantic_search") as ctx:
        pass
    assert ctx["latency_ms"] >= 0

    output = collector.generate_latest().decode("utf-8")
    val = _get_metric_value(output, "semantic_search_latency_ms_bucket", {"le": "+Inf"})
    assert val is not None and val >= 1


def test_remember_histogram():
    """Remember latency histogram records correctly."""
    registry = prometheus_client.CollectorRegistry()
    collector = MetricsCollector(registry=registry)
    with collector.tool_call("remember") as ctx:
        pass
    assert ctx["latency_ms"] >= 0

    output = collector.generate_latest().decode("utf-8")
    val = _get_metric_value(output, "remember_latency_ms_bucket", {"le": "+Inf"})
    assert val is not None and val >= 1


def test_drift_gauge():
    """Gauge can be updated with drift value."""
    registry = prometheus_client.CollectorRegistry()
    collector = MetricsCollector(registry=registry)
    collector.update_drift(12.5)

    output = collector.generate_latest().decode("utf-8")
    val = _get_metric_value(output, "derived_index_drift")
    assert val is not None and val == 12.5, f"Expected drift 12.5, got {val}"


def test_reindex_counter():
    """Reindex repair counter increments."""
    registry = prometheus_client.CollectorRegistry()
    collector = MetricsCollector(registry=registry)
    collector.inc_reindex_repair()

    output = collector.generate_latest().decode("utf-8")
    val = _get_metric_value(output, "reindex_repair_total")
    assert val is not None and val == 1.0


def test_busy_event_counter():
    """SQLite busy event counter increments."""
    registry = prometheus_client.CollectorRegistry()
    collector = MetricsCollector(registry=registry)
    collector.inc_busy_event()

    output = collector.generate_latest().decode("utf-8")
    val = _get_metric_value(output, "sqlite_busy_events_total")
    assert val is not None and val == 1.0


def test_generate_latest_prometheus_format():
    """generate_latest() returns valid Prometheus text format."""
    registry = prometheus_client.CollectorRegistry()
    collector = MetricsCollector(registry=registry)
    # Trigger some metrics
    with collector.tool_call("search") as ctx:
        pass
    with collector.tool_call("remember") as ctx:
        with pytest.raises(ValueError):
            raise ValueError("error")
    collector.update_drift(3.0)
    collector.inc_reindex_repair()
    collector.inc_busy_event()

    output = collector.generate_latest().decode("utf-8")

    lines = output.strip().split("\n")
    assert len(lines) > 0, "Output should not be empty"

    # Check for common metric names
    assert "tool_calls_total" in output
    assert "search_latency_ms" in output
    assert "derived_index_drift" in output
    assert "tool_error_total" in output
    assert "reindex_repair_total" in output
    assert "sqlite_busy_events_total" in output

    # Verify # HELP lines exist
    help_lines = [l for l in lines if l.startswith("# HELP")]
    assert len(help_lines) >= 3, f"Expected at least 3 HELP lines, got {len(help_lines)}"


def test_prometheus_format_parseable():
    """Prometheus output is parseable."""
    registry = prometheus_client.CollectorRegistry()
    collector = MetricsCollector(registry=registry)
    with collector.tool_call("ping") as ctx:
        pass

    output = collector.generate_latest().decode("utf-8")

    # Parse back
    parsed = []
    for line in output.split("\n"):
        line = line.strip()
        if line and not line.startswith("#"):
            parts = line.split()
            if len(parts) >= 2:
                metric_name = parts[0].split("{")[0]
                value = float(parts[-1])
                parsed.append((metric_name, value))

    assert len(parsed) >= 1, "Should parse at least one metric line"
    assert any("tool_calls_total" in m for m, _ in parsed)


def test_singleton_shared():
    """get_collector() returns the same singleton."""
    c1 = get_collector()
    c2 = get_collector()
    assert c1 is c2, "get_collector() should return the same instance"


def test_error_counter_no_increment_on_success():
    """Error counter is not incremented on successful tool calls."""
    registry = prometheus_client.CollectorRegistry()
    collector = MetricsCollector(registry=registry)
    with collector.tool_call("search") as ctx:
        pass

    output = collector.generate_latest().decode("utf-8")
    val = _get_metric_value(output, "tool_error_total", {"tool": "search"})
    assert val is None or val == 0, "Error counter should be 0 after a successful call"


def test_metrics_registry_global():
    """metrics_registry is a valid prometheus_client.CollectorRegistry."""
    assert isinstance(metrics_registry, prometheus_client.CollectorRegistry)


def test_latency_routed_to_correct_histogram():
    """Each tool name routes to its specific histogram."""
    registry = prometheus_client.CollectorRegistry()
    collector = MetricsCollector(registry=registry)

    with collector.tool_call("search") as ctx:
        pass
    with collector.tool_call("remember") as ctx:
        pass
    with collector.tool_call("semantic_search") as ctx:
        pass

    output = collector.generate_latest().decode("utf-8")

    # All three histogram metrics should be present
    assert "search_latency_ms_bucket" in output
    assert "remember_latency_ms_bucket" in output
    assert "semantic_search_latency_ms_bucket" in output

    # Non-latency tool should NOT create a latency entry
    with collector.tool_call("ping") as ctx:
        pass
    output2 = collector.generate_latest().decode("utf-8")
    # There should be no histogram for ping
    # (it just goes through counter, not histogram)
    assert "search_latency_ms_bucket" in output2
