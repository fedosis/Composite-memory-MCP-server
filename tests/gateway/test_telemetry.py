"""Synthetic local SQLite integration; no listener, live store or soft imports."""

import asyncio
import inspect
import sqlite3
import stat
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from threading import Barrier
from typing import get_args
from uuid import uuid4

import pytest
from prometheus_client import REGISTRY
from prometheus_client.parser import text_string_to_metric_families

from memory_server.control.access_audit import AccessAuditStore, AuditError
from memory_server.control.store import ControlStore
from memory_server.gateway_contracts.dto import AccessAudit, Operation, SearchRequest
from memory_server.gateway_contracts.ports import AuditOutcome, Principal, ProfileRecord, RequestContext
from memory_server.transports.http.telemetry import BoundedMetricsSink, TelemetryError, metrics_handler

CANARY_QUERY = "CANARY_QUERY_S05_93c8"
CANARY_SECRET = "CANARY_BEARER_S05_46fa"
FIELDS = {"id", "timestamp", "request_id", "token_id", "client_id", "profile_id", "operation",
          "memory_id", "result", "latency_ms", "remote_address"}
SUCCESS = AuditOutcome(200, None, (), 999999.0)


def run(coro):
    return asyncio.run(coro)


class Clock:
    def __init__(self):
        self.wall = datetime(2026, 10, 28, tzinfo=UTC)
        self.mono = 10.0

    def now(self):
        return self.wall

    def monotonic(self):
        return self.mono


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def ctx():
    principal = Principal("a" * 32, "synthetic", "test-client", frozenset({"alpha"}),
                          frozenset({"audit.read", "memory.read"}), 1, datetime(2099, 1, 1, tzinfo=UTC))
    return RequestContext("req_" + str(uuid4()), principal, "alpha", None, 1000.0)


@pytest.fixture
def store(tmp_path, clock):
    value = AccessAuditStore(tmp_path / "audit", clock=clock.now, monotonic=clock.monotonic)
    yield value
    value.close()


def begin(store, ctx):
    return run(store.begin(ctx, "search"))


def rows(store):
    return store._db.execute("SELECT * FROM access_audit ORDER BY seq").fetchall()


def retain_elapsed(store, clock, *, days):
    # Normal periodic retention, not an unbounded forward-clock step.
    removed = 0
    for _ in range(days * 24):
        clock.wall += timedelta(hours=1)
        removed += store.retain()
    return removed


def test_f1_denial_flood_isolated_and_dropped(tmp_path, ctx, clock):
    store = AccessAuditStore(tmp_path / "audit", clock=clock.now, segment_rows=2, max_segments=2,
                             denial_segment_rows=2, denial_max_segments=1)
    try:
        for _ in range(2):
            assert run(store.record_unauthenticated(ctx.request_id, "E_AUTH_REQUIRED")) is not None
        assert store.saturation()["denials"] is True
        for _ in range(100):
            assert run(store.record_unauthenticated(ctx.request_id, "E_AUTH_REQUIRED")) is None
        assert store.dropped_denials == 100
        assert store.saturation()["authenticated"] is False
        for _ in range(4):
            run(store.finish(begin(store, ctx), SUCCESS))
        assert [r["segment"] for r in rows(store)] == [0, 0, 1, 1]
        assert store.saturation()["authenticated"] is True
        with pytest.raises(AuditError, match="^E_AUDIT_UNAVAILABLE$"):
            begin(store, ctx)
        assert store._denials._db.execute("SELECT COUNT(*) FROM access_audit").fetchone()[0] == 2
        assert retain_elapsed(store, clock, days=31) == 6
        assert store.saturation() == {"authenticated": False, "denials": False}
    finally:
        store.close()


def test_f1_denials_do_not_consume_authenticated_segments(tmp_path, ctx):
    store = AccessAuditStore(tmp_path / "audit", segment_rows=2, max_segments=2)
    try:
        for _ in range(20):
            run(store.record_unauthenticated(ctx.request_id, "E_AUTH_REQUIRED"))
        assert begin(store, ctx).id
        assert len(rows(store)) == 1
    finally:
        store.close()


def test_f1_defaults_follow_retention(tmp_path, clock):
    stores = [AccessAuditStore(tmp_path / str(days), clock=clock.now, ttl_seconds=days * 86400)
              for days in (30, 60)]
    try:
        assert stores[0].max_segments == 27
        assert stores[1].max_segments == 53
        assert stores[0].segment_rows * (stores[0].max_segments - 1) >= 30 * 86400 / 10
    finally:
        for store in stores:
            store.close()


def test_intent_before_real_sqlite_effect_and_insert_failure(store, ctx, tmp_path):
    domain = sqlite3.connect(tmp_path / "domain.sqlite")
    domain.execute("CREATE TABLE effects (id INTEGER PRIMARY KEY)")
    ticket = begin(store, ctx)
    # Read through another connection, proving durability before the effect.
    with sqlite3.connect(tmp_path / "audit" / "access.sqlite") as reader:
        assert reader.execute("SELECT state FROM access_audit WHERE id=?", (ticket.id,)).fetchone() == ("intent",)
    domain.execute("INSERT INTO effects VALUES (1)")
    domain.commit()
    run(store.finish(ticket, SUCCESS))
    store._db.set_authorizer(lambda action, *args: sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_INSERT
                             else sqlite3.SQLITE_OK)
    with pytest.raises(AuditError, match="^E_AUDIT_UNAVAILABLE$"):
        begin(store, replace(ctx, request_id="req_" + str(uuid4())))
        domain.execute("INSERT INTO effects VALUES (2)")
        domain.commit()
    domain.close()
    with sqlite3.connect(tmp_path / "domain.sqlite") as reader:
        assert reader.execute("SELECT * FROM effects").fetchall() == [(1,)]


def test_dml_shapes_cas_and_terminal_immutability(store, ctx, clock):
    statements = []
    store._db.set_trace_callback(statements.append)
    ticket = begin(store, ctx)
    run(store.finish(ticket, SUCCESS))
    before = tuple(rows(store)[0])
    changes = store._db.total_changes
    with pytest.raises(AuditError, match="^E_VERSION_CONFLICT$"):
        run(store.finish(ticket, AuditOutcome(500, "E_INTERNAL", (), 0.0)))
    assert store._db.total_changes == changes  # second UPDATE changed zero rows
    assert tuple(rows(store)[0]) == before
    store.list(ctx.principal, "alpha")
    assert retain_elapsed(store, clock, days=31) == 1
    dml = [s for s in statements if s.split()[0].upper() in {"INSERT", "UPDATE", "DELETE"}]
    assert {s.split()[0] for s in dml} == {"INSERT", "UPDATE", "DELETE"}
    assert all("WHERE id=" in s and "AND state='intent'" in s for s in dml if s.startswith("UPDATE"))
    assert all("DELETE FROM access_audit WHERE created_at < " in s for s in dml if s.startswith("DELETE"))


def test_public_projection_and_search_canaries(store, ctx, tmp_path, caplog):
    # The S08 port is absent: a real synthetic SQLite FTS search, not runtime proof.
    request = SearchRequest(query=CANARY_QUERY, filters={"source": None, "min_confidence": None})
    domain = sqlite3.connect(tmp_path / "search.sqlite")
    domain.execute("CREATE VIRTUAL TABLE documents USING fts5(content)")
    domain.execute("INSERT INTO documents VALUES (?)", (CANARY_QUERY,))
    ticket = begin(store, ctx)
    assert domain.execute("SELECT rowid FROM documents WHERE documents MATCH ?", (request.query,)).fetchall() == [(1,)]
    run(store.finish(ticket, SUCCESS))
    listed = store.list(ctx.principal, "alpha")
    assert len(listed) == 1
    assert set(AccessAudit.model_fields) == FIELDS
    assert set(listed[0].model_dump()) == FIELDS
    assert listed[0].remote_address is None
    assert {"seq", "created_at", "state", "segment"} <= set(rows(store)[0].keys()) - FIELDS
    raw = (tmp_path / "audit" / "access.sqlite").read_bytes()
    for value in (CANARY_QUERY, CANARY_SECRET):
        assert value.encode() not in raw
        assert value not in repr([tuple(r) for r in rows(store)])
        assert value not in caplog.text
    domain.close()


def test_unauthenticated_has_no_identity(store, ctx, caplog, tmp_path):
    assert set(inspect.signature(store.record_unauthenticated).parameters) == {"request_id", "reason"}
    ticket = run(store.record_unauthenticated(ctx.request_id, "E_AUTH_REQUIRED"))
    row = rows(store._denials)[0]
    assert row["id"] == ticket.id
    assert row["request_id"] == ctx.request_id
    assert row["result"] == "E_AUTH_REQUIRED"
    assert all(row[key] is None for key in ("token_id", "client_id", "profile_id", "memory_id", "remote_address"))
    # The submitted bearer never crosses the helper's boundary.
    assert CANARY_SECRET not in caplog.text
    assert CANARY_SECRET.encode() not in (tmp_path / "audit" / "access.sqlite").read_bytes()


def test_listing_scope_permission_and_sql_predicate(store, ctx):
    alpha = begin(store, ctx)
    other = replace(ctx, principal=replace(ctx.principal, profiles=frozenset({"beta"})), profile_id="beta")
    beta = begin(store, other)
    statements = []
    store._db.set_trace_callback(statements.append)
    assert [r.id for r in store.list(ctx.principal, "alpha", limit=1)] == [alpha.id]
    assert beta.id not in [r.id for r in store.list(ctx.principal, "alpha", limit=200)]
    assert all("WHERE profile_id='alpha' AND seq>" in s and "LIMIT " in s
               for s in statements if s.startswith("SELECT"))
    with pytest.raises(AuditError, match="E_PERMISSION_DENIED"):
        store.list(ctx.principal, "beta")
    with pytest.raises(AuditError, match="E_PERMISSION_DENIED"):
        store.list(replace(ctx.principal, permissions=frozenset({"memory.read"})), "alpha")
    with pytest.raises(AuditError, match="E_VALIDATION"):
        store.list(ctx.principal, "alpha", limit=201)


def test_metrics_family_labels_cardinality_and_registry(ctx):
    previous = set(REGISTRY._names_to_collectors)
    sink = BoundedMetricsSink(["alpha"])
    assert sink.registry_name == "cmms_gateway"
    assert sink._registry is not REGISTRY
    assert set(REGISTRY._names_to_collectors) == previous
    for op in get_args(Operation):
        sink.http_completed(ctx, op, 200, 0.1)
    sink.retrieval_completed(ctx, "fts", 0.25, "E_INTERNAL")
    sink.projection_snapshot("alpha", 1, 2, 3)
    for i in range(500):
        unknown = replace(ctx, profile_id=f"{CANARY_QUERY}_{i}")
        sink.http_completed(unknown, "search", 500, 0.1)
        with pytest.raises(TelemetryError, match="^E_VALIDATION$"):
            sink.http_completed(unknown, CANARY_SECRET, 500, 0.1)
        with pytest.raises(TelemetryError, match="^E_VALIDATION$"):
            sink.retrieval_completed(unknown, CANARY_SECRET, 0.1, "E_INTERNAL")
    # C9 version conflict family exists without adding a Protocol method.
    sink.record_conflict("search", "alpha")
    admin = replace(ctx, principal=replace(ctx.principal, permissions=frozenset({"token.manage"})))
    content_type, body = metrics_handler(admin, sink)
    assert content_type.startswith("text/plain; version=0.0.4")
    text = body.decode()
    assert CANARY_SECRET not in text and CANARY_QUERY not in text
    families = list(text_string_to_metric_families(text))
    expected = {
        "cmms_http_requests": {"operation", "status_class", "profile"},
        "cmms_http_request_duration_seconds": {"operation", "status_class", "profile"},
        "cmms_retrieval_duration_seconds": {"stage", "profile"},
        "cmms_outbox_pending": {"profile"}, "cmms_vector_index_lag": {"profile"},
        "cmms_graph_index_lag": {"profile"}, "cmms_version_conflicts": {"operation", "profile"},
    }
    assert {f.name for f in families} == set(expected)
    for family in families:
        base = family.name.removesuffix("_created")
        for sample in family.samples:
            assert set(sample.labels) - {"le"} == expected[base]
            assert sample.labels["profile"] in {"alpha", "_unknown"}
    assert len(sink._requests._metrics) == len(get_args(Operation)) + 1
    assert len(sink._retrieval_duration._metrics) == 1


def test_f3_public_surface_cannot_inject_labels(ctx):
    sink = BoundedMetricsSink(["alpha"])
    for name in ("requests", "http_duration", "retrieval_duration", "pending", "vector_lag",
                 "graph_lag", "conflicts", "registry"):
        assert not hasattr(sink, name)
    assert not any(hasattr(value, "labels") or hasattr(value, "register")
                   for name, value in vars(sink).items() if not name.startswith("_"))
    unknown = replace(ctx, profile_id=CANARY_QUERY)
    with pytest.raises(TelemetryError, match="^E_VALIDATION$"):
        sink.http_completed(unknown, CANARY_SECRET, 200, 0.1)
    with pytest.raises(TelemetryError, match="^E_VALIDATION$"):
        sink.retrieval_completed(unknown, CANARY_SECRET, 0.1, None)
    with pytest.raises(TelemetryError, match="^E_VALIDATION$"):
        sink.record_conflict(CANARY_SECRET, CANARY_QUERY)
    sink.record_conflict("search", CANARY_QUERY)
    sink.http_completed(unknown, "search", 200, 0.1)
    sink.retrieval_completed(unknown, "fts", 0.1, None)
    sink.projection_snapshot(CANARY_QUERY, 0, 0, 0)
    admin = replace(ctx, principal=replace(ctx.principal, permissions=frozenset({"token.manage"})))
    text = metrics_handler(admin, sink)[1].decode()
    assert CANARY_QUERY not in text and CANARY_SECRET not in text
    samples = [s for f in text_string_to_metric_families(text) for s in f.samples]
    assert samples and all(s.labels["profile"] == "_unknown" for s in samples)


def test_metrics_cap_and_unknown_lag(ctx):
    with pytest.raises(TelemetryError, match="E_VALIDATION"):
        BoundedMetricsSink([f"p{i}" for i in range(101)])
    sink = BoundedMetricsSink([f"p{i}" for i in range(100)])
    for i in range(1000):
        sink.projection_snapshot(f"p{i}", 1, 1, 1)
    assert len(sink._pending._metrics) == 101
    sink.projection_snapshot("p0", 0, None, None)
    assert ("p0",) not in sink._vector_lag._metrics
    assert ("p0",) not in sink._graph_lag._metrics


def test_production_sink_failure_is_contained(ctx, monkeypatch):
    sink = BoundedMetricsSink(["alpha"])

    def fail(*args, **kwargs):
        raise RuntimeError(CANARY_SECRET)

    monkeypatch.setattr(sink._requests, "labels", fail)
    result = {"success": True}
    sink.http_completed(ctx, "search", 200, 0.25)
    assert result == {"success": True}
    assert sink.failure_count == 1


def test_metrics_permission_and_text(ctx):
    sink = BoundedMetricsSink(["alpha"])
    with pytest.raises(TelemetryError, match="E_PERMISSION_DENIED"):
        metrics_handler(ctx, sink)
    admin = replace(ctx, principal=replace(ctx.principal, permissions=frozenset({"token.manage"})))
    sink.http_completed(admin, "search", 200, 0.1)
    _, body = metrics_handler(admin, sink)
    assert list(text_string_to_metric_families(body.decode()))
    assert not body.startswith(b"{")


def test_restart_retention_strict_boundary_and_sequence(tmp_path, ctx, clock):
    root = tmp_path / "audit"
    store = AccessAuditStore(root, clock=clock.now, monotonic=clock.monotonic, ttl_seconds=10)
    first = begin(store, ctx)
    run(store.finish(first, SUCCESS))
    store.close()
    clock.wall += timedelta(seconds=10)
    for _ in range(2):
        store = AccessAuditStore(root, clock=clock.now, monotonic=clock.monotonic, ttl_seconds=10)
        assert store.list(ctx.principal, "alpha")[0].id == first.id
        assert store.retain() == 0  # strict <, equality retained
        store.close()
    store = AccessAuditStore(root, clock=clock.now, monotonic=clock.monotonic, ttl_seconds=10)
    second = begin(store, ctx)
    clock.wall += timedelta(seconds=1)
    assert store.retain() == 1
    assert [r["id"] for r in rows(store)] == [second.id]
    old_sequence = rows(store)[0]["seq"]
    clock.wall += timedelta(seconds=11)
    assert store.retain() == 1
    third = begin(store, ctx)
    assert rows(store)[0]["seq"] > old_sequence
    assert rows(store)[0]["id"] == third.id
    store.close()


def test_finish_once_and_exception_outcome(store, ctx):
    ticket = begin(store, ctx)
    try:
        raise RuntimeError(CANARY_QUERY)
    except RuntimeError:
        run(store.finish(ticket, AuditOutcome(500, "E_INTERNAL", (), -1.0)))
    assert rows(store)[0]["result"] == "E_INTERNAL"
    for _ in range(10):
        with pytest.raises(AuditError, match="E_VERSION_CONFLICT"):
            run(store.finish(ticket, SUCCESS))
    assert len(rows(store)) == 1


def test_monotonic_measurement_ignores_caller_and_wall_clock(store, ctx, clock):
    ticket = begin(store, ctx)
    clock.wall -= timedelta(days=10)
    clock.mono += 0.25
    run(store.finish(ticket, SUCCESS))
    assert rows(store)[0]["latency_ms"] == 250.0
    assert set(inspect.signature(store.finish).parameters) == {"ticket", "outcome"}
    ticket = begin(store, ctx)
    clock.mono -= 1
    run(store.finish(ticket, SUCCESS))
    assert rows(store)[1]["latency_ms"] == 0.0


def test_rotation_full_fail_closed_and_reclaim(tmp_path, ctx, clock):
    store = AccessAuditStore(tmp_path / "audit", clock=clock.now, segment_rows=2, max_segments=2, ttl_seconds=10)
    for _ in range(4):
        run(store.finish(begin(store, ctx), SUCCESS))
    assert [r["segment"] for r in rows(store)] == [0, 0, 1, 1]
    with pytest.raises(AuditError, match="E_AUDIT_UNAVAILABLE"):
        begin(store, ctx)
    assert len(rows(store)) == 4
    clock.wall += timedelta(seconds=11)
    assert store.retain() == 4
    begin(store, ctx)
    assert rows(store)[0]["segment"] == 2
    assert (tmp_path / "audit" / "access.sqlite").stat().st_size <= 64 * 1024 * 1024
    store.close()


def test_sqlite_full_is_sanitized(store, ctx, caplog):
    pages = store._db.execute("PRAGMA page_count").fetchone()[0]
    store._db.execute(f"PRAGMA max_page_count={pages}")
    completed = 0
    with pytest.raises(AuditError, match="^E_AUDIT_UNAVAILABLE$"):
        for _ in range(1000):
            begin(store, ctx)
            completed += 1
    assert completed == len(rows(store))
    assert CANARY_SECRET not in caplog.text


@pytest.mark.parametrize("column", ["timestamp", "latency_ms"])
def test_f5_corrupt_row_is_c5_only(store, ctx, tmp_path, column):
    begin(store, ctx)
    with sqlite3.connect(tmp_path / "audit" / "access.sqlite") as writer:
        writer.execute(f"UPDATE access_audit SET {column}=?", (CANARY_SECRET,))
    with pytest.raises(AuditError, match="^E_AUDIT_UNAVAILABLE$") as caught:
        store.list(ctx.principal, "alpha")
    assert caught.value.code == "E_AUDIT_UNAVAILABLE"
    assert CANARY_SECRET not in str(caught.value)
    assert CANARY_SECRET not in repr(caught.value)
    assert caught.value.__suppress_context__ is True


@pytest.mark.parametrize("stream", ["authenticated", "denials"])
def test_f4_retention_scrubs_file_bytes(store, ctx, clock, tmp_path, stream):
    target = store if stream == "authenticated" else store._denials
    if stream == "authenticated":
        run(store.finish(begin(store, ctx), SUCCESS))
    else:
        run(store.record_unauthenticated(ctx.request_id, "E_AUTH_REQUIRED"))
    root = tmp_path / "audit"
    if stream == "denials":
        root /= "denials"
    canaries = [ctx.request_id.encode()]
    if stream == "authenticated":
        canaries.append(ctx.principal.token_id.encode())
    raw = (root / "access.sqlite").read_bytes()
    assert all(canary in raw for canary in canaries)
    assert retain_elapsed(store, clock, days=31) == 1
    assert rows(target) == []
    assert all(canary not in (root / "access.sqlite").read_bytes() for canary in canaries)
    assert not (root / "access.sqlite-journal").exists()


def test_f2_rollback_before_first_retain(store, ctx, clock):
    watermark = clock.wall
    clock.wall -= timedelta(days=40)
    ticket = begin(store, ctx)
    assert rows(store)[0]["created_at"] == clock.wall.timestamp()
    with pytest.raises(AuditError, match="^E_VALIDATION$"):
        store.retain()
    assert store.rollback_refusals == 1
    assert rows(store)[0]["id"] == ticket.id
    clock.wall = watermark
    assert store.retain() == 0
    assert rows(store)[0]["id"] == ticket.id


@pytest.mark.parametrize("operation", ["begin", "finish", "list", "reopen"])
def test_f2_missing_watermark_with_rows_fails_closed(tmp_path, ctx, clock, operation):
    root = tmp_path / "audit"
    store = AccessAuditStore(root, clock=clock.now)
    try:
        ticket = begin(store, ctx)
        (root / "retention.watermark").unlink(missing_ok=True)
        with pytest.raises(AuditError, match="^E_AUDIT_UNAVAILABLE$") as caught:
            if operation == "reopen":
                store.close()
                AccessAuditStore(root, clock=clock.now)
            elif operation == "begin":
                begin(store, ctx)
            elif operation == "finish":
                run(store.finish(ticket, SUCCESS))
            else:
                store.list(ctx.principal, "alpha")
        assert caught.value.code == "E_AUDIT_UNAVAILABLE"
    finally:
        store.close()


def test_f2_first_insert_seeds_empty_store_and_advances(store, ctx, clock, tmp_path):
    sidecar = tmp_path / "audit" / "retention.watermark"
    sidecar.unlink(missing_ok=True)
    clock.wall += timedelta(days=2)
    begin(store, ctx)
    expected = clock.wall.timestamp()
    assert float(sidecar.read_text()) == expected
    clock.wall += timedelta(days=2)
    begin(store, ctx)
    assert rows(store)[-1]["created_at"] == clock.wall.timestamp()
    assert float(sidecar.read_text()) == expected
    assert store.retain() == 0
    assert float(sidecar.read_text()) == expected + 3600


def test_rollback_watermark_survives_restart(tmp_path, ctx, clock):
    root = tmp_path / "audit"
    store = AccessAuditStore(root, clock=clock.now, ttl_seconds=10)
    ticket = begin(store, ctx)
    assert store.retain() == 0
    store.close()
    clock.wall -= timedelta(seconds=1)
    store = AccessAuditStore(root, clock=clock.now, ttl_seconds=10)
    with pytest.raises(AuditError, match="E_VALIDATION"):
        store.retain()
    assert store.rollback_refusals == 1
    assert rows(store)[0]["id"] == ticket.id
    store.close()


def test_fresh_intent_during_clock_rollback_is_not_expired(tmp_path, ctx, clock):
    store = AccessAuditStore(tmp_path / "audit", clock=clock.now, ttl_seconds=10)
    assert store.retain() == 0
    watermark = clock.wall
    clock.wall -= timedelta(seconds=100)
    ticket = begin(store, ctx)
    clock.wall = watermark
    assert store.retain() == 0
    assert rows(store)[0]["id"] == ticket.id
    store.close()


def test_monotonic_across_connections(tmp_path, ctx, clock):
    root = tmp_path / "audit"
    first = AccessAuditStore(root, clock=clock.now, monotonic=clock.monotonic)
    second = AccessAuditStore(root, clock=clock.now, monotonic=clock.monotonic)
    ticket = begin(first, ctx)
    clock.mono += 0.25
    run(second.finish(ticket, SUCCESS))
    assert rows(first)[0]["latency_ms"] == 250.0
    first.close()
    second.close()


def test_concurrent_same_ticket_cas_two_connections(tmp_path, ctx):
    root = tmp_path / "audit"
    first, second = AccessAuditStore(root), AccessAuditStore(root)
    ticket = begin(first, ctx)
    barrier = Barrier(2)

    def finish(store):
        barrier.wait(timeout=5)
        try:
            run(store.finish(ticket, SUCCESS))
            return "won"
        except AuditError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(finish, (first, second)))
    assert sorted(results) == ["E_VERSION_CONFLICT", "won"]
    assert len(rows(first)) == 1 and rows(first)[0]["state"] == "terminal"
    with pytest.raises(AuditError, match="E_VERSION_CONFLICT"):
        run(first.finish(ticket, SUCCESS))
    first.close()
    second.close()


def test_concurrent_sequence_unique(tmp_path, ctx):
    root = tmp_path / "audit"
    stores = [AccessAuditStore(root) for _ in range(4)]
    barrier = Barrier(4)

    def write(store):
        barrier.wait(timeout=5)
        return [begin(store, ctx).id for _ in range(10)]

    with ThreadPoolExecutor(max_workers=4) as pool:
        tickets = sum(pool.map(write, stores), [])
    assert len(set(tickets)) == 40
    assert [r["seq"] for r in rows(stores[0])] == list(range(1, 41))
    for store in stores:
        store.close()


def test_separate_control_journal_and_outcome_dedup(tmp_path, store, ctx):
    control = ControlStore(tmp_path / "control")
    control._configure((ProfileRecord("alpha", tmp_path / "profile", True, "Alpha", None, {}, 1),))
    control._mutate("alpha", expected_generation=1, operation="enabled", enabled=False)
    assert control._connection.execute("SELECT COUNT(*) FROM control_journal").fetchone()[0] == 1
    ticket = begin(store, ctx)
    assert run(store.publish_committed(ticket, SUCCESS)) is True
    assert run(store.publish_committed(ticket, SUCCESS)) is True
    assert len(rows(store)) == 1 and rows(store)[0]["id"] == ticket.id
    assert control._connection.execute("SELECT COUNT(*) FROM control_journal").fetchone()[0] == 1
    assert not store._db.execute("SELECT name FROM sqlite_master WHERE name='control_journal'").fetchall()
    control.close()


def test_postcommit_failure_pending_marker_then_reconcile(tmp_path, store, ctx):
    canonical = sqlite3.connect(tmp_path / "canonical.sqlite")
    canonical.execute("CREATE TABLE effects (id INTEGER PRIMARY KEY)")
    canonical.execute("CREATE TABLE outcomes (ticket TEXT PRIMARY KEY, pending INTEGER)")
    ticket = begin(store, ctx)
    with canonical:
        canonical.execute("INSERT INTO effects VALUES (1)")
        canonical.execute("INSERT INTO outcomes VALUES (?,1)", (ticket.id,))
    # Fail inside real SQLite outcome UPDATE, after the canonical transaction.
    store._db.set_authorizer(lambda action, *args: sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_UPDATE
                             else sqlite3.SQLITE_OK)
    result = {"status": 201}
    assert run(store.publish_committed(ticket, SUCCESS)) is False
    assert result == {"status": 201}
    assert rows(store)[0]["state"] == "intent"
    canonical.close()
    with sqlite3.connect(tmp_path / "canonical.sqlite") as readback:
        assert readback.execute("SELECT * FROM effects").fetchall() == [(1,)]
        assert readback.execute("SELECT * FROM outcomes").fetchall() == [(ticket.id, 1)]
    store._db.set_authorizer(None)
    assert run(store.publish_committed(ticket, SUCCESS)) is True
    assert run(store.publish_committed(ticket, SUCCESS)) is True
    assert rows(store)[0]["state"] == "terminal"


def test_private_modes_and_reopen_compatibility(tmp_path, ctx):
    root = tmp_path / "audit"
    store = AccessAuditStore(root)
    begin(store, ctx)
    store.retain()
    store.close()
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    for name in ("access.sqlite", "retention.watermark"):
        assert stat.S_IMODE((root / name).stat().st_mode) == 0o600
    # Additive schema; a previous S02 control store can still open its own DB.
    control = ControlStore(root)
    assert control.journal(profile_ids=("alpha",)) == ()
    control.close()
    reopened = AccessAuditStore(root)
    assert len(rows(reopened)) == 1
    reopened.close()


@pytest.mark.parametrize("name", ["access.sqlite", "retention.watermark"])
def test_symlink_files_rejected(tmp_path, name):
    root = tmp_path / "audit"
    root.mkdir(mode=0o700)
    target = tmp_path / "canary"
    target.write_text(CANARY_SECRET)
    (root / name).symlink_to(target)
    with pytest.raises(AuditError, match="E_AUDIT_UNAVAILABLE"):
        AccessAuditStore(root)
    assert target.read_text() == CANARY_SECRET


@pytest.mark.parametrize("duration", [-1, float("nan"), float("inf")])
def test_invalid_metric_duration_no_series(ctx, duration):
    sink = BoundedMetricsSink(["alpha"])
    sink.http_completed(ctx, "search", 200, duration)
    sink.retrieval_completed(ctx, "fts", duration, None)
    assert not sink._requests._metrics and not sink._retrieval_duration._metrics


@pytest.mark.parametrize("field,value", [("request_id", CANARY_QUERY), ("profile_id", "/absolute/path")])
def test_untrusted_metadata_rejected_without_content(store, ctx, field, value, tmp_path):
    with pytest.raises(AuditError, match="E_VALIDATION"):
        begin(store, replace(ctx, **{field: value}))
    assert rows(store) == []
    assert value.encode() not in (tmp_path / "audit" / "access.sqlite").read_bytes()


def test_n1_pinned_denial_does_not_delay_authenticated(store, ctx, tmp_path):
    import time
    from threading import Event

    entered = Event()
    blocker = sqlite3.connect(tmp_path / "audit" / "denials" / "access.sqlite")
    blocker.execute("BEGIN IMMEDIATE")
    store._denials._db.set_trace_callback(
        lambda sql: entered.set() if sql == "BEGIN IMMEDIATE" else None)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            denial = pool.submit(run, store.record_unauthenticated(ctx.request_id, "E_AUTH_REQUIRED"))
            assert entered.wait(2)
            started = time.monotonic()
            auth = pool.submit(lambda: run(store.finish(begin(store, ctx), SUCCESS)))
            try:
                auth.result(timeout=0.2)
                elapsed = time.monotonic() - started
                print(f"N1 pinned denial: begin+finish={elapsed:.6f}s bound=0.200s")
                assert not denial.done()
            finally:
                blocker.rollback()
            assert denial.result(timeout=2) is not None
    finally:
        blocker.close()


def test_n1_concurrent_denial_and_authenticated_writes(store, ctx):
    barrier = Barrier(4)

    def write(denial):
        barrier.wait(timeout=5)
        for _ in range(25):
            if denial:
                assert run(store.record_unauthenticated(ctx.request_id, "E_AUTH_REQUIRED")) is not None
            else:
                run(store.finish(begin(store, ctx), SUCCESS))

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(write, (True, False, True, False)))
    assert len(rows(store)) == len(rows(store._denials)) == 50
    assert all(r["state"] == "terminal" for target in (store, store._denials) for r in rows(target))


def test_n2_forward_step_bounded_and_converges(store, ctx, clock, tmp_path):
    original = clock.wall
    first = begin(store, ctx)
    clock.wall += timedelta(days=40)
    assert store.retain() == 0
    assert rows(store)[0]["id"] == first.id
    assert store.clock_step_refusals == 1
    watermark = tmp_path / "audit" / "retention.watermark"
    assert float(watermark.read_text()) == original.timestamp() + 3600
    # A permanently skewed clock still accepts exact wall-clock inserts.
    for _ in range(3):
        run(store.finish(begin(store, ctx), SUCCESS))
    assert all(r["created_at"] == clock.wall.timestamp() for r in rows(store)[1:])
    removed = 0
    for step in range(1, 960):
        removed += store.retain()
        assert float(watermark.read_text()) == original.timestamp() + (step + 1) * 3600
    assert removed == 1
    assert len(rows(store)) == 3
    assert store.clock_step_refusals == 959
    assert store.retain() == 0
    clock.wall += timedelta(seconds=1)
    assert store.retain() == 0
    print("N2 +40d: first deleted=0; cap=3600s; catchup=960 calls; refusals=959; skewed inserts=3")


@pytest.mark.parametrize("value", [0, 0.5, -1, float("nan"), float("inf"), True, "3600", None])
def test_n2_invalid_max_clock_step(tmp_path, value):
    with pytest.raises(AuditError, match="^E_VALIDATION$"):
        AccessAuditStore(tmp_path / "audit", max_clock_step=value)


def test_n2_configured_step_and_normal_expiry(tmp_path, ctx, clock):
    store = AccessAuditStore(tmp_path / "audit", clock=clock.now, ttl_seconds=10, max_clock_step=1)
    try:
        begin(store, ctx)
        clock.wall += timedelta(seconds=11)
        assert [store.retain() for _ in range(11)] == [0] * 10 + [1]
        assert store.clock_step_refusals == 10
        assert rows(store) == []
    finally:
        store.close()


def test_n2_backward_clock_refuses_without_deleting_fresh(store, ctx, clock):
    store.retain()
    clock.wall -= timedelta(days=40)
    fresh = begin(store, ctx)
    assert rows(store)[0]["created_at"] == clock.wall.timestamp()
    with pytest.raises(AuditError, match="^E_VALIDATION$"):
        store.retain()
    assert store.rollback_refusals == 1
    assert rows(store)[0]["id"] == fresh.id
