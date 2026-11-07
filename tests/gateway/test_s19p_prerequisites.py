"""S19-P public seams across real control SQLite and runtime composition."""

from __future__ import annotations

import importlib
import importlib.util
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from time import monotonic
from types import SimpleNamespace
from uuid import uuid4

import pytest


def public_surface():
    names = ("memory_server.control.profiles", "memory_server.application.job_service")
    assert all(importlib.util.find_spec(name) is not None for name in names), "S19P_SURFACE"
    profiles, service = (importlib.import_module(name) for name in names)
    seam = getattr(profiles.ProfileRegistry, "control_store", None)
    assert isinstance(seam, property) and callable(seam.fget) and callable(seam.fset), "S19P_SURFACE"
    assert callable(getattr(service.JobService, "authorize", None)), "S19P_SURFACE"
    return profiles.ProfileRegistry, service.JobService


@pytest.fixture
async def seam_owner(tmp_path):
    from memory_server.application.job_service import JobService
    from memory_server.application.profile_bundle import BundlePolicies
    from memory_server.control.access_audit import AccessAuditStore
    from memory_server.control.jobs import JobMetadata
    from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
    from memory_server.control.store import ControlStore
    from memory_server.control.tokens import TokenService
    from memory_server.gateway_contracts import dto as d
    from memory_server.gateway_contracts.ports import RequestContext
    from memory_server.runtime.daemon import Daemon
    from memory_server.runtime.runtime_manager import RuntimeManager
    from memory_server.settings import Settings
    from tests.gateway.test_daemon import factory, make_binding

    now = datetime.now(UTC)
    settings = Settings(_env_file=None, outbox_poll_interval_seconds=0.01)
    bindings = tuple(make_binding(tmp_path / name, name, settings) for name in ("alpha", "beta"))
    with ControlStore(tmp_path / "control") as store:
        access = AccessAuditStore(tmp_path / "access")
        tokens = TokenService(store)
        registry = ProfileRegistry(store, RegistryConfiguration(bindings, access, tokens))

        async def principal(subject, profiles, permissions):
            created = tokens._bootstrap_create(
                d.TokenCreate(
                    subject=subject,
                    profiles=profiles,
                    permissions=permissions,
                    expires_at=now + timedelta(hours=1),
                ),
                now=now,
            )
            return await tokens.authenticate(f"cmms_{created.token.id}.{created.secret}", now=now)

        worker_a = await principal("worker-A", ["alpha"], ["index.manage"])
        worker_b = await principal("worker-B", ["beta"], ["index.manage"])
        reader = await principal("reader", ["alpha"], ["memory.read"])
        administrator = await principal("administrator", ["alpha", "beta"], ["token.manage", "profile.manage"])
        ctx = RequestContext("req_s19p", worker_a, "alpha", uuid4(), monotonic() + 60)
        metadata = JobMetadata(store)
        service = JobService(metadata, registry, tokens, access.begin, access.publish_committed, clock=lambda: now)
        daemon = Daemon(
            registry, policies=BundlePolicies.capture(settings), manager=RuntimeManager(factory_builder=factory)
        )
        assert store.root.stat().st_mode & 0o777 == 0o700
        assert (store.root / "control.sqlite").stat().st_mode & 0o777 == 0o600
        try:
            yield SimpleNamespace(**locals())
        finally:
            await daemon.close_all()
            access.close()


def test_s19p_surface_required():
    public_surface()


async def test_s19p_public_seams_compatibility(seam_owner, tmp_path, monkeypatch):
    registry_type, service_type = public_surface()
    from memory_server.control.jobs import JobError
    from memory_server.control.store import ControlStore
    from memory_server.gateway_contracts import dto as d

    h = seam_owner
    original = h.store
    with ControlStore(tmp_path / "other-control") as other:
        for value in (other, None):
            with pytest.raises(AttributeError, match="^control_store is read-only$"):
                h.registry.control_store = value
            assert h.registry.control_store is original
        h.registry.__dict__["control_store"] = other
        assert h.registry.control_store is original
        del h.registry.__dict__["control_store"]
    assert registry_type.control_store.fget(h.registry) is original

    order, statements = [], []
    verify, resolve = h.tokens.verify_principal, h.registry.resolve

    def verified(*args, **kwargs):
        order.append("verify")
        return verify(*args, **kwargs)

    async def resolved(*args, **kwargs):
        order.append("resolve")
        return await resolve(*args, **kwargs)

    monkeypatch.setattr(h.tokens, "verify_principal", verified)
    monkeypatch.setattr(h.registry, "resolve", resolved)
    # Keep statement shape only: no expanded token/subject/payload binds in evidence.
    h.store._connection.set_trace_callback(
        lambda sql: statements.append((sql.split()[0].upper(), "jobs" in sql or "job_requests" in sql))
    )

    async def outcome(method, ctx, job_id):
        order.clear()
        statements.clear()
        try:
            assert await method(ctx, job_id) is None
            code = None
        except JobError as exc:
            code = exc.code
        assert not any(metadata for _, metadata in statements), "S19P_AUTH_METADATA"
        return code, tuple(order)

    cases = [
        (h.ctx, None, None, ("verify", "resolve")),
        (h.ctx, "a" * 48, None, ("verify", "resolve")),
        (replace(h.ctx, principal=h.worker_b, profile_id="beta"), None, None, ("verify", "resolve")),
        (replace(h.ctx, principal=None), "bad", "E_AUTH_REQUIRED", ()),
        (replace(h.ctx, principal=replace(h.worker_a, token_id="0" * 32)), "bad", "E_AUTH_REQUIRED", ("verify",)),
        (replace(h.ctx, principal=h.reader), "bad", "E_PERMISSION_DENIED", ("verify",)),
        (replace(h.ctx, profile_id="beta"), "bad", "E_PERMISSION_DENIED", ("verify",)),
        (replace(h.ctx, profile_id=None), "bad", "E_PERMISSION_DENIED", ("verify",)),
        (h.ctx, "bad", "E_VALIDATION", ("verify",)),
        (h.ctx, False, "E_VALIDATION", ("verify",)),
    ]
    # A genuine token may carry an unknown scope; registry denies only after ID validation.
    unknown = await h.principal("unknown", ["missing"], ["index.manage"])
    cases.append(
        (replace(h.ctx, principal=unknown, profile_id="missing"), None, "E_PROFILE_NOT_FOUND", ("verify", "resolve"))
    )
    for ctx, job_id, code, expected_order in cases:
        for method in (h.service.authorize, h.service._authorize):
            assert await outcome(method, ctx, job_id) == (code, expected_order), "S19P_AUTH_ORDER"

    await h.registry.set_enabled(h.administrator, "alpha", enabled=False, expected_generation=1)
    for method in (h.service.authorize, h.service._authorize):
        assert await outcome(method, h.ctx, None) == ("E_PROFILE_DISABLED", ("verify", "resolve"))
    await h.registry.set_enabled(h.administrator, "alpha", enabled=True, expected_generation=2)
    late = replace(h.service, clock=lambda: h.now + timedelta(hours=1))
    for method in (late.authorize, late._authorize):
        assert await outcome(method, h.ctx, "bad") == ("E_AUTH_REQUIRED", ("verify",))
    await h.tokens.revoke(
        replace(h.ctx, principal=h.administrator, profile_id=None),
        h.worker_a.token_id,
        d.ControlMutation(reason="synthetic revoke", confirmation=True, expected_generation=1),
        now=h.now,
    )
    for method in (h.service.authorize, h.service._authorize):
        assert await outcome(method, h.ctx, "bad") == ("E_AUTH_REQUIRED", ("verify",))

    calls = []

    async def public(self, ctx, job_id=None):
        calls.append((self, ctx, job_id))

    monkeypatch.setattr(service_type, "authorize", public)
    assert await h.service._authorize(h.ctx, "a" * 48) is None
    assert calls == [(h.service, h.ctx, "a" * 48)], "S19P_DELEGATE"


async def test_s19p_no_second_engine_and_legacy_default(seam_owner, monkeypatch):
    public_surface()
    import sqlalchemy.ext.asyncio as async_sql
    from storage.outbox_worker import OutboxWorker

    from memory_server.control.jobs import JobError
    from memory_server.control.store import ControlStore
    from memory_server.gateway_contracts import dto as d
    from memory_server.gateway_contracts.ports import GatewayDependencies, HttpConfig
    from memory_server.providers import sqlite_provider
    from memory_server.runtime.read_composition import compose_job_ports
    from memory_server.transports.http.routes.jobs import create_job_routes
    from memory_server.transports.http.telemetry import BoundedMetricsSink

    h = seam_owner
    async with (
        h.daemon.acquire(h.ctx) as alpha,
        h.daemon.acquire(replace(h.ctx, principal=h.worker_b, profile_id="beta")) as beta,
    ):
        runtimes = (alpha.runtime, beta.runtime)
        engines = tuple(runtime.dependencies.sqlite.engine for runtime in runtimes)
        workers = tuple(runtime.dependencies.outbox_worker for runtime in runtimes)
        assert engines[0] is not engines[1]
        assert all(worker._engine is engine for worker, engine in zip(workers, engines, strict=True))

        def forbidden(*args, **kwargs):
            pytest.fail("S19P_SECOND_OWNER")

        # Install guards after real factory/manager/runtime initialization, never replace ownership.
        monkeypatch.setattr(ControlStore, "__init__", forbidden)
        monkeypatch.setattr(async_sql, "create_async_engine", forbidden)
        monkeypatch.setattr(sqlite_provider, "create_async_engine", forbidden)
        monkeypatch.setattr(OutboxWorker, "__init__", forbidden)
        existing = {"health": object()}
        deps = GatewayDependencies(
            h.registry, h.tokens, h.access, BoundedMetricsSink(frozenset({"alpha", "beta"})), existing, leases=h.daemon
        )
        statements = []
        h.store._connection.set_trace_callback(
            lambda sql: statements.append((sql.split()[0].upper(), "jobs" in sql or "job_requests" in sql))
        )
        composed = compose_job_ports(deps, HttpConfig(control_path=h.store.root / "control.sqlite"))
        assert type(composed) is tuple and len(composed) == 2
        wired, specs = composed
        assert specs == create_job_routes() and tuple(spec.operation for spec in specs) == ("jobs.get",)
        assert set(wired.ports) == set(existing) | {"jobs.get"}
        assert deps.ports is existing and set(existing) == {"health"}
        assert wired.ports["health"] is existing["health"]
        assert wired.ports["jobs.get"].jobs is wired.jobs
        assert wired.jobs.attach is None and wired.jobs.metadata.store is h.store
        assert wired.registry is h.registry and h.registry.control_store is h.store
        assert wired.leases is h.daemon
        payloads = (
            (
                "outbox_replay",
                d.MaintenanceRequest(
                    reason="synthetic",
                    confirmation=True,
                    selection="pending",
                    event_ids=[],
                    selection_digest="d" * 64,
                    expected_count=1,
                ),
            ),
            (
                "vector_rebuild",
                d.RebuildRequest(
                    reason="synthetic",
                    confirmation=True,
                    targets=["vector"],
                    selection_digest="d" * 64,
                    expected_count=1,
                ),
            ),
        )
        for operation, payload in payloads:
            with pytest.raises(JobError, match="^E_CAPABILITY_UNAVAILABLE$"):
                await wired.jobs.enqueue(h.ctx, operation, payload)
        assert not any(metadata for _, metadata in statements), "S19P_LEGACY_EFFECT"
        assert not any(verb in {"INSERT", "UPDATE", "DELETE"} for verb, _ in statements), "S19P_LEGACY_EFFECT"
        assert h.access._db.execute("SELECT count(*) FROM access_audit").fetchone()[0] == 0
        for runtime, engine, worker in zip(runtimes, engines, workers, strict=True):
            assert runtime.dependencies.sqlite.engine is engine and runtime.dependencies.outbox_worker is worker
            async with engine.connect() as connection:
                result = await connection.exec_driver_sql("SELECT count(*) FROM outbox_entries")
                assert result.scalar_one() == 0


def scoped_surface():
    import inspect

    from memory_server.application.job_runner import JobCoordinator
    from memory_server.control.jobs import JobMetadata

    assert callable(getattr(JobMetadata, "scan_reconcile_scoped", None)), "S19P_SURFACE"
    parameters = inspect.signature(JobCoordinator.reconcile_startup).parameters
    assert {"profile_id", "custody"} <= parameters.keys(), "S19P_SURFACE"
    assert "startup_custody" in inspect.signature(JobCoordinator).parameters, "S19P_SURFACE"


def p2_reserve(h, *, profile="alpha", running=True):
    from memory_server.control.jobs import request_digest
    from memory_server.gateway_contracts.ports import AuditTicket
    from tests.gateway.test_job_shutdown import payload

    principal = h.worker_a if profile == "alpha" else h.worker_b
    ctx = replace(h.ctx, principal=principal, profile_id=profile, idempotency_key=uuid4())
    reservation = h.metadata.enqueue(
        ctx,
        "outbox_replay",
        payload(),
        digest=request_digest("outbox_replay", payload()),
        ticket=AuditTicket("ticket", ctx.request_id),
        now=h.now,
    )
    if not running:
        return reservation
    return h.metadata.claim(reservation.job_id, profile, principal.subject, 0, now=h.now)


async def test_s19p_scoped_scan_contract(seam_owner, monkeypatch):
    scoped_surface()
    from memory_server.control.jobs import JobError

    h = seam_owner
    beta = p2_reserve(h, profile="beta")
    alpha = p2_reserve(h)
    statements = []
    h.store._connection.set_trace_callback(lambda sql: statements.append(sql.split()[0].upper()))
    for profile in (None, False, 1, "", "a" * 65, "é", "a\n", "a.b", "a b", "_alpha"):
        with pytest.raises(JobError, match="^E_VALIDATION$"):
            h.metadata.scan_reconcile_scoped(profile_id=profile, deadline=monotonic() + 2)
    for limit in (False, 0, 1001, 1.0, "1", None):
        with pytest.raises(JobError, match="^E_VALIDATION$"):
            h.metadata.scan_reconcile_scoped(profile_id="alpha", limit=limit, deadline=monotonic() + 2)
    for deadline in (True, None, "1", float("nan"), float("inf"), -float("inf")):
        with pytest.raises(JobError, match="^E_VALIDATION$"):
            h.metadata.scan_reconcile_scoped(profile_id="alpha", deadline=deadline)
    with pytest.raises(TypeError):
        h.metadata.scan_reconcile_scoped(deadline=monotonic() + 2)
    assert h.metadata.scan_reconcile_scoped(profile_id="alpha", deadline=monotonic() - 1) == ()
    assert statements == [], "S19P_SCOPE"
    # Choose the lexically later ID as target: LIMIT must follow SQL scope filtering.
    target, foreign = sorted((alpha, beta), key=lambda claim: claim.job.id, reverse=True)
    seen = []
    transaction = h.store.borrow_transaction

    from contextlib import contextmanager

    @contextmanager
    def borrowed():
        with transaction() as tx:
            fetchmany = tx.fetchmany

            def fetched(sql, params, *, limit):
                seen.append((sql, params, limit))
                return fetchmany(sql, params, limit=limit)

            monkeypatch.setattr(tx, "fetchmany", fetched)
            yield tx

    monkeypatch.setattr(h.store, "borrow_transaction", borrowed)
    records = h.metadata.scan_reconcile_scoped(profile_id=target.job.profile_id, limit=1, deadline=monotonic() + 2)
    assert tuple(r.claim.job.id for r in records) == (target.job.id,), "S19P_SCOPE"
    assert seen == [
        (
            "SELECT * FROM jobs WHERE profile_id=? AND status='running' ORDER BY id LIMIT ?",
            (target.job.profile_id, 1),
            1,
        )
    ], "S19P_SCOPE"
    assert h.metadata.get(foreign.job.id, foreign.job.profile_id, foreign.subject).status == "running"
    for profile in ("a", "A0_-", "z" * 64):
        assert h.metadata.scan_reconcile_scoped(profile_id=profile, limit=1000, deadline=monotonic() + 2) == ()


async def test_s19p_recovery_predicates(seam_owner):
    from memory_server.application.job_service import JobService

    assert callable(getattr(JobService, "authorize", None)), "S19P_SURFACE"
    from memory_server.application.job_runner import JobCoordinator
    from memory_server.control.jobs import JobError

    h = seam_owner
    statements = []
    h.store._connection.set_trace_callback(lambda sql: statements.append(sql.split()[0].upper()))
    coordinator = JobCoordinator(h.metadata, h.daemon, authorize=h.service.authorize)
    try:
        await coordinator.reconcile_startup(deadline=monotonic() + 2)
    except JobError as exc:
        assert exc.code == "E_RUNTIME_NOT_READY", "S19P_SCOPE"
    else:
        pytest.fail("S19P_SCOPE: empty legacy snapshot allowed recovery")
    assert statements == [], "S19P_SCOPE"
    scoped_surface()
    for kwargs in ({"profile_id": "alpha"}, {"custody": object()}, {"profile_id": "alpha", "custody": object()}):
        with pytest.raises(JobError, match="^E_RUNTIME_NOT_READY$"):
            await coordinator.reconcile_startup(deadline=monotonic() + 2, **kwargs)
    with pytest.raises(JobError, match="^E_TIMEOUT$"):
        await coordinator.reconcile_startup(profile_id="alpha", deadline=monotonic() - 1)
    assert statements == [], "S19P_SCOPE"
    # Actual READY runtime is not startup authority, even while unrelated beta is owned.
    async with h.daemon.acquire(h.ctx) as session:
        with pytest.raises(JobError, match="^E_RUNTIME_NOT_READY$"):
            await coordinator.reconcile_startup(profile_id="alpha", custody=session.runtime, deadline=monotonic() + 2)


@pytest.mark.parametrize(
    "kind",
    [
        "E_TIMEOUT",
        "E_STORAGE",
        "E_VECTOR_BACKEND",
        "E_GRAPH_BACKEND",
        "E_OUTBOX",
        "E_PROJECTION_UNAVAILABLE",
        "E_CAPABILITY_UNAVAILABLE",
        "E_MAINTENANCE_CONFLICT",
        "E_VERSION_CONFLICT",
        "E_RUNTIME_NOT_READY",
        "E_RUNTIME_CLOSING",
        "E_RUNTIME_FAILED",
        "E_JOB_INTERRUPTED",
        "E_INTERNAL",
        "unknown",
        "forged",
        "bool",
        "unhashable",
        "timeout",
    ],
)
async def test_s19p_typed_c5_forwarding(seam_owner, kind):
    from memory_server.application.job_service import JobService

    assert callable(getattr(JobService, "authorize", None)), "S19P_SURFACE"
    from memory_server.application.job_runner import JobCoordinator
    from memory_server.control.jobs import JobError
    from tests.gateway.test_job_shutdown import reserve

    h = seam_owner
    canary = "PRIVATE_S19P_C5_EXCEPTION"
    expected = "E_INTERNAL"
    if kind.startswith("E_"):
        error = JobError(kind)
        expected = kind
    elif kind == "timeout":
        error = TimeoutError(canary)
        expected = "E_TIMEOUT"
    elif kind == "forged":
        error = RuntimeError(canary)
        error.code = "E_STORAGE"
    else:
        error = JobError({"unknown": "E_VALIDATION", "bool": True, "unhashable": []}[kind])
    error.args = (canary,)
    events = []

    async def event(value):
        events.append(value)

    class Runner:
        async def run(self, session, job, payload):
            assert session.runtime.state == "READY"
            raise error

    coordinator = JobCoordinator(h.metadata, h.daemon, authorize=h.service.authorize, replay=Runner(), events=event)
    reservation = reserve(h)
    try:
        await coordinator.attach(h.ctx, reservation)
        await coordinator.close(deadline=monotonic() + 5)
        job = h.metadata.get(reservation.job_id, "alpha", h.worker_a.subject)
        assert job.error_code == expected, "S19P_C5"
        assert job.status == ("interrupted" if expected == "E_JOB_INTERRUPTED" else "failed"), "S19P_C5"
        assert len(events) == 1 and events[0].data.code == expected, "S19P_C5"
        assert canary not in job.model_dump_json() + events[0].model_dump_json(), "S19P_C5"
        with h.store.borrow_transaction() as tx:
            row = tx.fetchone("SELECT dto_snapshot FROM jobs WHERE id=?", (reservation.job_id,))
        assert canary not in row["dto_snapshot"], "S19P_C5"
    finally:
        await coordinator.close(deadline=monotonic() + 5)


async def test_s19p_recovery_guard_and_cas(seam_owner):
    """Metadata CAS proof only; actual startup-window L/A/R integration awaits P5."""
    scoped_surface()
    from memory_server.control.jobs import JobMetadata, TerminalUpdate
    from memory_server.control.store import ControlStore

    h = seam_owner
    claim = p2_reserve(h)
    queued = p2_reserve(h, profile="beta", running=False)
    with ControlStore(h.store.root) as other:
        competitor = JobMetadata(other)
        progressed = competitor.progress(claim, 0.5, now=h.now)
        assert progressed.revision == claim.revision + 1
        update = TerminalUpdate("interrupted", None, "E_JOB_INTERRUPTED", h.now, phase="reconcile")
        assert h.metadata.finish(claim.job.id, "alpha", claim.subject, claim.revision, "running", update) is None
        completed = competitor.complete(
            progressed,
            importlib.import_module("memory_server.gateway_contracts.dto").JobResult(
                selected=1, attempted=1, completed=1, failed=0, projection_digest=None, receipt_ids=[]
            ),
            now=h.now,
        )
        assert h.metadata.finish(claim.job.id, "alpha", claim.subject, claim.revision, "running", update) == completed
    assert h.metadata.scan_reconcile_scoped(profile_id="alpha", deadline=monotonic() + 2) == ()
    assert h.metadata.get(queued.job_id, "beta", h.worker_b.subject).status == "queued"


def test_s19p_legacy_scan_frozen_and_production_scoped_only():
    import ast
    import subprocess
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    path = "src/memory_server/control/jobs.py"
    baseline = subprocess.run(["git", "show", f"ca49f13:{path}"], cwd=root, capture_output=True, check=True).stdout

    def method(source):
        text = source.decode()
        node = next(
            n for n in ast.walk(ast.parse(text)) if isinstance(n, ast.FunctionDef) and n.name == "scan_reconcile"
        )
        return "".join(text.splitlines(keepends=True)[node.lineno - 1 : node.end_lineno])

    assert method((root / path).read_bytes()) == method(baseline), "S19P_LEGACY"
    for folder in (root / "src", root / "storage"):
        for source in folder.rglob("*.py"):
            tree = ast.parse(source.read_text())
            for node in ast.walk(tree):
                # Reject attribute references, string/getattr aliases, imported/direct aliases,
                # including constant fragments that could reconstruct the legacy name.
                if isinstance(node, ast.Attribute):
                    assert node.attr != "scan_reconcile", f"S19P_SCOPE: {source}:{node.lineno}"
                if isinstance(node, ast.Name):
                    assert node.id != "scan_reconcile", f"S19P_SCOPE: {source}:{node.lineno}"
                if isinstance(node, ast.alias):
                    assert node.name != "scan_reconcile", f"S19P_SCOPE: {source}"
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    assert "scan_reconcile" not in node.value, f"S19P_SCOPE: {source}:{node.lineno}"


async def test_s19p_c6_request_digest(seam_owner):
    import asyncio
    import hashlib
    import json

    from memory_server.control.jobs import request_digest
    from memory_server.gateway_contracts import dto as d
    from tests.gateway.test_job_shutdown import payload, reserve

    h = seam_owner
    requests = (
        ("outbox_replay", payload(), "outbox.replay"),
        (
            "vector_rebuild",
            d.RebuildRequest(
                reason="synthetic", confirmation=True, targets=["vector"], selection_digest="d" * 64, expected_count=1
            ),
            "indexes.rebuild",
        ),
    )
    for operation, request, route in requests:
        golden = json.dumps(
            {"method": "POST", "route": route, "path_ids": {}, "payload": request.model_dump(mode="json")},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
        assert request_digest(operation, request) == hashlib.sha256(golden).hexdigest(), "S19P_C6"
    first = reserve(h)
    for _ in range(10):
        assert reserve(h).accepted == first.accepted, "S19P_C6"

    async def duplicate():
        from memory_server.gateway_contracts.ports import AuditTicket

        return h.metadata.enqueue(
            h.ctx,
            "outbox_replay",
            payload(),
            digest=request_digest("outbox_replay", payload()),
            ticket=AuditTicket("ticket", h.ctx.request_id),
            now=h.now,
        )

    copies = await asyncio.gather(*(duplicate() for _ in range(10)))
    assert all(r.replayed and r.accepted == first.accepted for r in copies), "S19P_C6"
    beta = reserve(h, replace(h.ctx, principal=h.worker_b, profile_id="beta"))
    assert beta.job_id != first.job_id, "S19P_C6"
    with h.store.borrow_transaction() as tx:
        rows = tx.fetchmany("SELECT accepted_snapshot FROM job_requests ORDER BY job_id LIMIT ?", (10,), limit=10)
    assert len(rows) == 2, "S19P_C6"


async def test_s19p_p6_drain_contract(tmp_path):
    import asyncio
    import sys
    import threading

    from memory_server.runtime.runtime import CMMSRuntime

    runtime = await CMMSRuntime(tmp_path / "native").start()
    release, parked = threading.Event(), threading.Event()

    def native():
        def profile(frame, event, arg):
            if event == "return" and frame.f_code is CMMSRuntime._native_target.__code__:
                sys.setprofile(None)
                parked.set()
                release.wait(30)

        sys.setprofile(profile)
        return 7

    try:
        handle = await runtime.submit_native(native, kind="s19p-p6-contract")
        assert await handle.result() == 7, "S19P_P6_DRAIN"
        assert await asyncio.to_thread(parked.wait, 5), "S19P_P6_DRAIN"
        assert callable(getattr(handle, "drain", None)), "S19P_P6_DRAIN"
        for invalid in (True, None, "1", float("nan"), float("inf"), -float("inf")):
            with pytest.raises(ValueError, match="invalid drain deadline"):
                await handle.drain(invalid)
        assert not await handle.drain(monotonic() + 0.05), "S19P_P6_DRAIN"
        assert not handle.terminated and handle.thread.is_alive(), "S19P_P6_DRAIN"
        assert runtime.lock is not None, "S19P_P6_DRAIN"
        release.set()
        assert await handle.drain(monotonic() + 5), "S19P_P6_DRAIN"
        assert handle.terminated and not handle.thread.is_alive(), "S19P_P6_DRAIN"
    finally:
        release.set()
        await runtime.close()


async def test_s19p_p6_projection_result_forwarding(seam_owner, monkeypatch):
    import asyncio
    import sys
    import threading
    from dataclasses import fields

    import memory_server.runtime.daemon as daemon_module
    from memory_server.runtime.daemon import _ProjectionResult
    from memory_server.runtime.runtime import CMMSRuntime, NativeSettlementTimeout

    release, parked = threading.Event(), threading.Event()

    def native():
        def profile(frame, event, arg):
            if event == "return" and frame.f_code is CMMSRuntime._native_target.__code__:
                sys.setprofile(None)
                parked.set()
                release.wait(30)

        sys.setprofile(profile)
        return "projection-value"

    try:
        empty = _ProjectionResult("projection-value")
        assert not any(hasattr(empty, name) for name in ("drain", "terminated")), "S19P_P6_DRAIN"
        assert await empty.result() == "projection-value", "S19P_P6_DRAIN"
        async with seam_owner.daemon.acquire(seam_owner.ctx) as session:
            runtime = session.runtime
            handle = await runtime.submit_native(native, kind="s19p-p6-forward")
            assert await handle.result() == "projection-value", "S19P_P6_DRAIN"
            assert await asyncio.to_thread(parked.wait, 5), "S19P_P6_DRAIN"
            result = _ProjectionResult("projection-value", handle)
            assert result == empty and repr(result) == repr(empty), "S19P_P6_DRAIN"
            private = next(field for field in fields(result) if field.name == "_handle")
            assert not private.repr and not private.compare, "S19P_P6_DRAIN"
            assert not any(hasattr(result, name) for name in ("handle", "thread", "native_future", "_record")), (
                "S19P_P6_DRAIN"
            )
            monkeypatch.setattr(daemon_module, "_DRAIN_SECONDS", 0.05)
            with pytest.raises(NativeSettlementTimeout, match="^native settlement timeout$"):
                await result.result()
            assert not handle.terminated, "S19P_P6_DRAIN"
            release.set()
            monkeypatch.setattr(daemon_module, "_DRAIN_SECONDS", 5)
            assert await result.result() == "projection-value" and handle.terminated, "S19P_P6_DRAIN"
    finally:
        release.set()


async def test_s19p_p6_result_drains_before_value(seam_owner, monkeypatch):
    import asyncio
    import sys
    import threading

    import memory_server.runtime.daemon as daemon_module
    from memory_server.runtime.runtime import CMMSRuntime, NativeSettlementTimeout

    release, parked = threading.Event(), threading.Event()

    def native():
        def profile(frame, event, arg):
            if event == "return" and frame.f_code is CMMSRuntime._native_target.__code__:
                sys.setprofile(None)
                parked.set()
                release.wait(30)

        sys.setprofile(profile)
        return "result-value"

    try:
        monkeypatch.setattr(daemon_module, "_DRAIN_SECONDS", 0.05, raising=False)
        async with seam_owner.daemon.acquire(seam_owner.ctx) as session:
            submit = session.runtime.dependencies.outbox_worker._native_submitter
            result = await submit(native, kind="s19p-p6-result")
            assert await asyncio.to_thread(parked.wait, 5), "S19P_P6_DRAIN"
            timed_out = False
            try:
                await result.result()
            except NativeSettlementTimeout as exc:
                timed_out = str(exc) == "native settlement timeout"
            assert timed_out, "S19P_P6_DRAIN"
            assert not result._handle.terminated, "S19P_P6_DRAIN"
            release.set()
            monkeypatch.setattr(daemon_module, "_DRAIN_SECONDS", 5)
            assert await result.result() == "result-value", "S19P_P6_DRAIN"
    finally:
        release.set()


async def test_s19p_p6_worker_blocks_until_native_dead(seam_owner, monkeypatch):
    import asyncio
    import sys
    import threading

    import memory_server.runtime.daemon as daemon_module
    from memory_server.runtime.runtime import CMMSRuntime, NativeSettlementTimeout

    release, parked = threading.Event(), threading.Event()
    task = None

    def native():
        def profile(frame, event, arg):
            if event == "return" and frame.f_code is CMMSRuntime._native_target.__code__:
                sys.setprofile(None)
                parked.set()
                release.wait(60)

        sys.setprofile(profile)
        return "worker-value"

    try:
        async with seam_owner.daemon.acquire(seam_owner.ctx) as session:
            runtime = session.runtime
            worker = runtime.dependencies.outbox_worker
            # Exercise the installed production projection submitter, not a replacement.
            assert worker._native_submitter != runtime.submit_native, "S19P_P6_DRAIN"
            monkeypatch.setattr(daemon_module, "_DRAIN_SECONDS", 0.05, raising=False)
            task = asyncio.create_task(worker._blocking(native, kind="s19p-p6-worker-timeout"))
            assert await asyncio.to_thread(parked.wait, 5), "S19P_P6_DRAIN"
            timed_out = False
            try:
                await asyncio.wait_for(asyncio.shield(task), 5)
            except NativeSettlementTimeout as exc:
                timed_out = str(exc) == "native settlement timeout"
            assert timed_out, "S19P_P6_DRAIN"
            record = next(r for r in runtime.owned_snapshot()[2] if r.kind == "s19p-p6-worker-timeout")
            assert record.thread.is_alive() and runtime.lock is not None, "S19P_P6_DRAIN"
            release.set()
            async with asyncio.timeout(5):
                while record.state != "TERMINATED":
                    await asyncio.sleep(0)
            release.clear()
            parked.clear()
            monkeypatch.setattr(daemon_module, "_DRAIN_SECONDS", 30.0)
            task = asyncio.create_task(worker._blocking(native, kind="s19p-p6-worker-success"))
            assert await asyncio.to_thread(parked.wait, 5), "S19P_P6_DRAIN"
            record = next(r for r in runtime.owned_snapshot()[2] if r.kind == "s19p-p6-worker-success")
            assert not task.done() and record.thread.is_alive(), "S19P_P6_DRAIN"
            release.set()
            assert await asyncio.wait_for(task, 5) == "worker-value", "S19P_P6_DRAIN"
            assert record.state == "TERMINATED" and not record.thread.is_alive(), "S19P_P6_DRAIN"
            release.clear()
            parked.clear()
            monkeypatch.setattr(daemon_module, "_DRAIN_SECONDS", 0.05)
            task = asyncio.create_task(worker._blocking(native, kind="s19p-p6-worker-retained"))
            assert await asyncio.to_thread(parked.wait, 5), "S19P_P6_DRAIN"
            with pytest.raises(NativeSettlementTimeout, match="^native settlement timeout$"):
                await asyncio.wait_for(task, 5)
            record = next(r for r in runtime.owned_snapshot()[2] if r.kind == "s19p-p6-worker-retained")
        report = await runtime.close()
        assert report.lock_retained and record.thread.is_alive(), "S19P_P6_DRAIN"
        release.set()
        async with asyncio.timeout(5):
            while record.state != "TERMINATED":
                await asyncio.sleep(0)
        assert not (await runtime.close()).lock_retained, "S19P_P6_DRAIN"
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)


async def p6c_owned_claim(h, monkeypatch, *, operation, method):
    import asyncio
    import sys
    import threading

    from sqlalchemy import event, text
    from storage.models.belief import BeliefORM
    from storage.models.relation import ClaimRelationORM
    from storage.models.relation_revision import RelationRevisionORM
    from storage.outbox import OutboxRepository

    import memory_server.runtime.daemon as daemon_module
    from memory_server.runtime.runtime import CMMSRuntime

    release, parked = threading.Event(), threading.Event()
    async with h.daemon.acquire(h.ctx) as scope:
        runtime = scope.runtime
        worker = runtime.dependencies.outbox_worker
        # Stop only the automatic consumer; all calls below use the real worker.
        worker.stop()
        async with asyncio.timeout(5):
            while any(r.kind == "outbox-worker" for r in runtime.owned_snapshot()[1]):
                await asyncio.sleep(0)
        source, target, event_id = str(uuid4()), str(uuid4()), str(uuid4())
        async with worker._session_factory() as session:
            if operation == "relation_sync":
                session.add_all([BeliefORM(id=value, proposition=value) for value in (source, target)])
                session.add(
                    ClaimRelationORM(
                        source_id=source,
                        target_id=target,
                        relation_type="supports",
                        created_at=h.now,
                    )
                )
                session.add(
                    RelationRevisionORM(
                        source_id=source,
                        target_id=target,
                        relation_type="supports",
                        revision=1,
                        deleted=0,
                        relation_created_at=h.now,
                        changed_at=h.now,
                        event_id=event_id,
                    )
                )
                payload = dict(
                    v=1, source_id=source, target_id=target, relation_type="supports", revision=1, event_id=event_id
                )
            else:
                payload = dict(subject="s", predicate="p", object="o")
            entry = await OutboxRepository(session).add_entry(
                record_type="belief" if operation == "relation_sync" else "fact",
                record_id=source,
                operation=operation,
                payload=payload,
            )
            entry_id = entry.id
            await session.commit()

        owner = worker._graph_router.graph if operation == "relation_sync" else worker._embedder
        name = "reconcile_claim_relation" if operation == "relation_sync" else "embed_batch"
        original = getattr(owner, name)

        def barrier(*args, **kwargs):
            value = original(*args, **kwargs)

            def profile(frame, event, arg):
                if event == "return" and frame.f_code is CMMSRuntime._native_target.__code__:
                    sys.setprofile(None)
                    parked.set()
                    release.wait(30)

            sys.setprofile(profile)
            return value

        monkeypatch.setattr(owner, name, barrier)
        monkeypatch.setattr(daemon_module, "_DRAIN_SECONDS", 0.05)
        commits = []

        def committed(connection):
            commits.append(True)

        event.listen(worker._engine.sync_engine, "commit", committed)
        task = None
        try:
            task = asyncio.create_task(getattr(worker, method)())
            assert await asyncio.to_thread(parked.wait, 5), "S19P_P6C_NO_COMMIT"
            async with worker._engine.connect() as connection:
                claimed_at = (
                    await connection.execute(
                        text("SELECT processed_at FROM outbox_entries WHERE id=:id"), {"id": entry_id}
                    )
                ).scalar_one()
            assert claimed_at is not None, "S19P_P6C_NO_COMMIT"
            result = await asyncio.wait_for(asyncio.shield(task), 5)
            expected = 0 if method == "_poll_once" else {"processed": 0, "failed": 0}
            assert result == expected, "S19P_P6C_NO_COMMIT"
            assert len(commits) == 1, "S19P_P6C_NO_COMMIT: work commit after claim"
            async with worker._engine.connect() as connection:
                row = (
                    await connection.execute(
                        text("SELECT status,retry_count,error,processed_at FROM outbox_entries WHERE id=:id"),
                        {"id": entry_id},
                    )
                ).one()
            assert tuple(row) == ("processing", 0, None, claimed_at), "S19P_P6C_NO_COMMIT"
            records = [r for r in runtime.owned_snapshot()[2] if r.thread.is_alive()]
            assert records and runtime.lock is not None, "S19P_P6C_NO_COMMIT"
            print("S19P_P6C_NO_COMMIT", operation, method, tuple(row), "claim_commits=1 native_alive EX_retained")
        finally:
            event.remove(worker._engine.sync_engine, "commit", committed)
            release.set()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            async with asyncio.timeout(5):
                while any(r.thread.is_alive() for r in runtime.owned_snapshot()[2]):
                    await asyncio.sleep(0)


@pytest.mark.parametrize("method", ["_poll_once", "process_all_pending"])
async def test_s19p_p6c_signal_survives_wrappers(seam_owner, monkeypatch, method):
    await p6c_owned_claim(seam_owner, monkeypatch, operation="relation_sync", method=method)


async def test_s19p_p6c_fact_batch_no_commit(seam_owner, monkeypatch):
    await p6c_owned_claim(seam_owner, monkeypatch, operation="index_fact", method="_poll_once")


@pytest.mark.parametrize("method", ["_poll_once", "process_all_pending"])
async def test_s19p_p6d_exception_path_no_commit(seam_owner, monkeypatch, method):
    import asyncio
    import sys
    import threading

    from sqlalchemy import event, text
    from storage.outbox import OutboxRepository

    import memory_server.runtime.daemon as daemon_module
    from memory_server.runtime.runtime import CMMSRuntime, NativeSettlementTimeout

    release, parked = threading.Event(), threading.Event()
    failure = RuntimeError("S19P_SYNTHETIC_NATIVE_FAILURE")
    async with seam_owner.daemon.acquire(seam_owner.ctx) as scope:
        runtime = scope.runtime
        worker = runtime.dependencies.outbox_worker
        worker.stop()
        async with asyncio.timeout(5):
            while any(r.kind == "outbox-worker" for r in runtime.owned_snapshot()[1]):
                await asyncio.sleep(0)
        async with worker._session_factory() as session:
            entry = await OutboxRepository(session).add_entry(
                record_type="fact",
                record_id=str(uuid4()),
                operation="index_fact",
                payload=dict(subject="s", predicate="p", object="o"),
            )
            entry_id = entry.id
            await session.commit()

        def failing_embedder(*args, **kwargs):
            def profile(frame, event, arg):
                if event == "return" and frame.f_code is CMMSRuntime._native_target.__code__:
                    sys.setprofile(None)
                    parked.set()
                    release.wait(30)

            sys.setprofile(profile)
            raise failure

        monkeypatch.setattr(worker._embedder, "embed_batch", failing_embedder)
        monkeypatch.setattr(worker._embedder, "embed", failing_embedder)
        monkeypatch.setattr(daemon_module, "_DRAIN_SECONDS", 0.05)
        commits = []
        settlements = []
        submit = worker._native_submitter

        async def observed_submit(*args, **kwargs):
            try:
                return await submit(*args, **kwargs)
            except NativeSettlementTimeout as exc:
                settlements.append(exc)
                raise

        monkeypatch.setattr(worker, "_native_submitter", observed_submit)

        def committed(connection):
            commits.append(True)

        event.listen(worker._engine.sync_engine, "commit", committed)
        task = None
        try:
            task = asyncio.create_task(getattr(worker, method)())
            assert await asyncio.to_thread(parked.wait, 5), "S19P_P6D_EXCEPTION_NO_COMMIT"
            result = await asyncio.wait_for(asyncio.shield(task), 5)
            assert len(settlements) == 1, "S19P_P6D_EXCEPTION_NO_COMMIT: missing call-level signal"
            assert settlements[0].__cause__ is failure, "S19P_P6D_EXCEPTION_NO_COMMIT"
            expected = 0 if method == "_poll_once" else {"processed": 0, "failed": 0}
            assert result == expected, "S19P_P6D_EXCEPTION_NO_COMMIT"
            assert len(commits) == 1, "S19P_P6D_EXCEPTION_NO_COMMIT: work commit after claim"
            async with worker._engine.connect() as connection:
                row = (
                    await connection.execute(
                        text("SELECT status,retry_count,error,processed_at FROM outbox_entries WHERE id=:id"),
                        {"id": entry_id},
                    )
                ).one()
            assert tuple(row[:3]) == ("processing", 0, None), "S19P_P6D_EXCEPTION_NO_COMMIT"
            assert row.processed_at is not None, "S19P_P6D_EXCEPTION_NO_COMMIT"
            assert any(r.thread.is_alive() for r in runtime.owned_snapshot()[2]), "S19P_P6D_EXCEPTION_NO_COMMIT"
            print("S19P_P6D_EXCEPTION_NO_COMMIT", method, tuple(row), "commits=1 native_alive")
        finally:
            event.remove(worker._engine.sync_engine, "commit", committed)
            release.set()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            async with asyncio.timeout(5):
                while any(r.thread.is_alive() for r in runtime.owned_snapshot()[2]):
                    await asyncio.sleep(0)


async def test_s19p_p6d_exception_after_termination_preserved(seam_owner, monkeypatch):
    from memory_server.runtime.runtime import NativeCallHandle

    failure = RuntimeError("S19P_SYNTHETIC_NATIVE_FAILURE")
    original = NativeCallHandle.result

    async def settled_result(handle):
        try:
            return await original(handle)
        finally:
            assert await handle.drain(monotonic() + 5), "S19P_P6D_EXCEPTION_PRESERVED"
            assert handle.terminated, "S19P_P6D_EXCEPTION_PRESERVED"

    def failing_native():
        raise failure

    monkeypatch.setattr(NativeCallHandle, "result", settled_result)
    async with seam_owner.daemon.acquire(seam_owner.ctx) as session:
        submit = session.runtime.dependencies.outbox_worker._native_submitter
        with pytest.raises(RuntimeError) as caught:
            await submit(failing_native, kind="s19p-p6d-dead-exception")
        assert caught.value is failure, "S19P_P6D_EXCEPTION_PRESERVED"
        print("S19P_P6D_EXCEPTION_PRESERVED original_exception_identity native_terminated")


async def test_s19p_p6c_not_exception(seam_owner, monkeypatch):
    import asyncio
    import threading

    import memory_server.runtime.daemon as daemon_module
    import memory_server.runtime.runtime as runtime_module

    signal = getattr(runtime_module, "NativeSettlementTimeout", None)
    assert signal is not None, "S19P_P6C_NO_COMMIT: settlement signal missing"
    assert issubclass(signal, BaseException) and not issubclass(signal, Exception), "S19P_P6C_NO_COMMIT"
    release, entered = threading.Event(), threading.Event()

    def native():
        entered.set()
        release.wait(30)

    async with seam_owner.daemon.acquire(seam_owner.ctx) as session:
        handle = await session.runtime.submit_native(native, kind="s19p-p6c-type")
        try:
            assert await asyncio.to_thread(entered.wait, 5), "S19P_P6C_NO_COMMIT"
            monkeypatch.setattr(daemon_module, "_DRAIN_SECONDS", 0.05)
            result = daemon_module._ProjectionResult(None, handle)
            with pytest.raises(signal, match="^native settlement timeout$"):
                await result.result()
            assert not handle.terminated and session.runtime.lock is not None, "S19P_P6C_NO_COMMIT"
        finally:
            release.set()
            assert await handle.drain(monotonic() + 5), "S19P_P6C_NO_COMMIT"


@pytest.fixture
async def p3_worker(tmp_path):
    from sqlalchemy.ext.asyncio import create_async_engine
    from storage.outbox_worker import OutboxWorker

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'outbox.sqlite'}")
    worker = OutboxWorker(engine=engine, compact_interval_seconds=0)
    await worker.initialize()
    try:
        yield worker
    finally:
        await engine.dispose()


def p3_surface(worker):
    import storage.outbox_worker as module

    for name in ("claim_scope", "process_claimed"):
        assert callable(getattr(worker, name, None)), "S19P_SURFACE"
    assert isinstance(getattr(module, "OutboxClaimError", None), type), "S19P_SURFACE"
    return module.OutboxClaimError


async def p3_add(worker, *, operation="index_decision", payload=None):
    from storage.outbox import OutboxRepository

    async with worker._session_factory() as session:
        entry = await OutboxRepository(session).add_entry(
            record_type="fact",
            record_id=str(uuid4()),
            operation=operation,
            payload=payload or {"choice": "local", "reason": "test"},
        )
        await session.commit()
        return entry


async def p3_rows(worker):
    from sqlalchemy import text

    async with worker._engine.connect() as connection:
        return [
            tuple(row)
            for row in (
                await connection.execute(
                    text("SELECT id,status,retry_count,error,processed_at FROM outbox_entries ORDER BY id")
                )
            ).all()
        ]


@pytest.mark.parametrize(
    "site",
    [
        "pending",
        "exhausted",
        "chunk",
        "poll",
        "compact",
        "lance_single",
        "lance_batch",
        "lance_ensure",
        "lance_optimize",
        "qdrant_single",
        "qdrant_batch",
        "graph_replace",
        "graph_read",
        "graph_lock",
        "graph_resync",
        "graph_publish",
    ],
)
async def test_s19p_worker_secret_canary(p3_worker, monkeypatch, caplog, site):
    import asyncio
    import logging

    from storage.outbox import OutboxRepository

    from memory_server.providers.embedding_provider import MockEmbeddingProvider

    worker = p3_worker
    secret = "S19P_PRIVATE_WORKER /private/path payload=canary"
    failure = RuntimeError(secret)
    caplog.set_level(logging.DEBUG)
    entry = await p3_add(worker, operation="index_fact", payload={"subject": "s", "predicate": "p", "object": "o"})
    before = await p3_rows(worker)

    def fail(*args, **kwargs):
        raise failure

    if site.startswith(("lance_", "qdrant_", "graph_")):
        await p3_provider_canary(worker, monkeypatch, caplog, site, fail, failure, secret)
        return
    if site == "poll":
        # Fault below the real repository method, at SQL execution.
        from sqlalchemy.ext.asyncio import AsyncSession

        monkeypatch.setattr(AsyncSession, "execute", fail)
        worker.stop()
        worker._stop_requested = False
        original_sleep = asyncio.sleep

        async def stopped(delay):
            worker.stop()
            await original_sleep(0)

        monkeypatch.setattr(asyncio, "sleep", stopped)
        await worker.run()
        monkeypatch.undo()
    elif site == "compact":
        from memory_server.providers.lancedb_provider import LanceDBProvider
        from memory_server.settings import Settings

        provider = LanceDBProvider(
            db_path=str(worker._engine.url.database) + ".lance", vector_size=3, settings=Settings(_env_file=None)
        )
        table = await provider._get_table()
        monkeypatch.setattr(type(table), "optimize", fail)
        worker._qdrant = provider
        tasks = []

        def spawn(factory, *, kind):
            task = asyncio.create_task(factory())
            tasks.append(task)
            return task

        worker._task_spawner = spawn
        await worker._maybe_compact()
        await asyncio.gather(*tasks)
    else:
        worker._embedder = MockEmbeddingProvider(vector_size=3)
        monkeypatch.setattr(worker._embedder, "embed", fail)
        monkeypatch.setattr(worker._embedder, "embed_batch", fail)
        worker._max_retries = 1 if site == "exhausted" else 3
        if site == "chunk":
            await worker._poll_once()
        else:
            async with worker._session_factory() as session:
                repo = OutboxRepository(session)
                claimed = await repo.claim_pending()
                await session.commit()
                await worker._process_entry(session, repo, claimed[0])
                await session.commit()
    rows = await p3_rows(worker)
    if site in ("poll", "compact"):
        assert rows == before, "S19P_PRIVACY"
    else:
        assert rows[0][0] == entry.id
        assert rows[0][1:4] == ("failed" if site == "exhausted" else "pending", 1, "E_OUTBOX"), "S19P_PRIVACY"
    records = [r for r in caplog.records if r.name.startswith(("storage.", "memory_server."))]
    assert records, "S19P_PRIVACY: missing exercised sink"
    for record in records:
        assert secret not in repr((record.msg, record.args, record.exc_info, record.stack_info)), "S19P_PRIVACY"
        assert secret not in record.getMessage() and not record.exc_info and not record.stack_info, "S19P_PRIVACY"


async def test_s19p_process_claimed_contract(p3_worker):
    from storage.outbox import OutboxEntry, OutboxRepository

    worker = p3_worker
    error = p3_surface(worker)
    await p3_add(worker)
    for deadline in (None, True, float("inf"), float("nan"), -1):
        with pytest.raises(error) as caught:
            async with worker.claim_scope(deadline=deadline):
                pytest.fail("S19P_CLAIM: invalid deadline entered")
        assert caught.value.code == "E_VALIDATION", "S19P_CLAIM"
    async with worker.claim_scope(deadline=monotonic() + 5) as claim:
        async with worker._session_factory() as session:
            repo = OutboxRepository(session)
            entries = await repo.claim_pending()
            claim.register_claimed(session, entries)
            await session.commit()
            entry = entries[0]
            forged = OutboxEntry(**{**entry.__dict__, "payload_json": "{}"})
            with pytest.raises(error) as caught:
                await worker.process_claimed(session, repo, forged, claim=claim)
            assert caught.value.code == "E_MAINTENANCE_CONFLICT", "S19P_CLAIM"
            assert await worker.process_claimed(session, repo, entry, claim=claim) == "completed"
            with pytest.raises(error) as caught:
                await worker.process_claimed(session, repo, entry, claim=claim)
            assert caught.value.code == "E_MAINTENANCE_CONFLICT", "S19P_CLAIM"
            await session.commit()
    assert (await p3_rows(worker))[0][1:4] == ("completed", 0, None), "S19P_CLAIM"
    async with worker._session_factory() as session:
        with pytest.raises(error):
            await worker.process_claimed(session, OutboxRepository(session), entry, claim=claim)


async def test_s19p_worker_claim_fence(p3_worker):
    import asyncio

    from storage.outbox import OutboxRepository

    worker = p3_worker
    p3_surface(worker)
    await p3_add(worker)
    async with worker.claim_scope(deadline=monotonic() + 5) as claim:
        async with worker._session_factory() as session:
            repo = OutboxRepository(session)
            entries = await repo.claim_pending()
            claim.register_claimed(session, entries)
            await session.commit()
            worker._stale_processing_seconds = -1
            before = await p3_rows(worker)
            poll = asyncio.create_task(worker._poll_once())
            all_pending = asyncio.create_task(worker.process_all_pending())
            try:
                await asyncio.sleep(0)
                assert not poll.done() and not all_pending.done(), "S19P_FENCE"
                assert await p3_rows(worker) == before, "S19P_FENCE"
                assert await worker.process_claimed(session, repo, entries[0], claim=claim) == "completed"
                await session.commit()
            finally:
                if session.in_transaction():
                    await session.rollback()
    assert await asyncio.wait_for(poll, 5) == 0
    assert await asyncio.wait_for(all_pending, 5) == {"processed": 0, "failed": 0}
    assert (await p3_rows(worker))[0][1:4] == ("completed", 0, None), "S19P_FENCE"


async def test_s19p_claim_scope_one_winner(p3_worker, monkeypatch):
    import asyncio

    from storage.outbox import OutboxRepository

    worker = p3_worker
    error = p3_surface(worker)
    entry = await p3_add(worker)
    winners = []
    schedules = []

    async def between(ids):
        schedules.append(tuple(ids))
        await asyncio.sleep(0)

    monkeypatch.setattr(OutboxRepository, "claim_between_select_and_update", between)

    async def consume():
        async with worker.claim_scope(deadline=monotonic() + 5) as claim:
            with pytest.raises(error):
                async with worker.claim_scope(deadline=monotonic() + 5):
                    pytest.fail("S19P_FENCE: nested scope")
            async with worker._session_factory() as session:
                repo = OutboxRepository(session)
                entries = await repo.claim_pending()
                claim.register_claimed(session, entries)
                await session.commit()
                for value in entries:
                    winners.append(value.id)
                    assert await worker.process_claimed(session, repo, value, claim=claim) == "completed"
                await session.commit()

    engine = worker._engine
    await asyncio.gather(consume(), consume())
    assert winners == [entry.id] and schedules == [(entry.id,)], "S19P_CLAIM"
    assert worker._engine is engine, "S19P_CLAIM"


async def p3_provider_canary(worker, monkeypatch, caplog, site, fail, failure, secret):
    import asyncio
    import logging
    from pathlib import Path

    from memory_server.providers.embedding_provider import MockEmbeddingProvider
    from memory_server.providers.exceptions import ProviderWriteError
    from memory_server.settings import Settings

    worker._embedder = MockEmbeddingProvider(vector_size=3)
    worker._max_retries = 1
    original_rows = await p3_rows(worker)
    probes = []
    calls = []
    thread = None
    release = None

    def injected(*args, **kwargs):
        calls.append(True)
        return fail(*args, **kwargs)

    if site.startswith("lance_"):
        import lancedb

        from memory_server.providers.lancedb_provider import LanceDBProvider

        provider = LanceDBProvider(
            db_path=str(worker._engine.url.database) + ".lance", vector_size=3, settings=Settings(_env_file=None)
        )
        if site == "lance_ensure":
            db = lancedb.connect(provider._db_path)
            monkeypatch.setattr(type(db), "create_table", injected)
            expected = "failed to ensure lancedb table 'memories': E_VECTOR_BACKEND"
        else:
            table = await provider._get_table()
            if site == "lance_optimize":
                monkeypatch.setattr(type(table), "optimize", injected)
            else:
                builder = table.merge_insert("id")
                monkeypatch.setattr(type(builder), "execute", injected)
            expected = {
                "lance_single": "lancedb upsert failed: E_VECTOR_BACKEND",
                "lance_batch": "lancedb upsert_batch failed: E_VECTOR_BACKEND",
                "lance_optimize": "lancedb optimize failed: E_VECTOR_BACKEND",
            }[site]
        worker._qdrant = provider
    elif site.startswith("qdrant_"):
        from memory_server.providers.qdrant_provider import QdrantProvider

        provider = QdrantProvider(location=":memory:", vector_size=3, settings=Settings(_env_file=None))
        monkeypatch.setattr(provider._client, "upsert", injected)
        expected = (
            "qdrant upsert_batch failed: E_VECTOR_BACKEND"
            if site.endswith("batch")
            else "qdrant upsert failed: E_VECTOR_BACKEND"
        )
        worker._qdrant = provider
    else:
        import sys
        import threading

        import memory_server.providers.graph_provider as gp
        from memory_server.router.graph_router import GraphRouter

        path = Path(str(worker._engine.url.database) + ".graph.json")
        graph = gp.SimpleGraph(snapshot_path=path)
        worker._graph_router = GraphRouter(graph)
        if site == "graph_resync":
            graph.add_node(id="s", type="entity", name="s")
            graph.add_node(id="o", type="entity", name="o")
            worker._graph_router.sync_fact("s", "p", "o")
        if site == "graph_replace":
            monkeypatch.setattr(gp.os, "replace", injected)
        elif site in ("graph_lock", "graph_resync", "graph_publish"):
            monkeypatch.setattr(gp._fcntl, "flock", injected)
        elif site == "graph_read":
            path.write_text("{}")
            read = Path.read_text

            def read_failure(self, *args, **kwargs):
                if self == path:
                    return injected()
                return read(self, *args, **kwargs)

            monkeypatch.setattr(Path, "read_text", read_failure)
            with pytest.raises(gp._MalformedSnapshotError) as caught:
                graph._read_state_file(path)
            assert caught.value.__cause__ is failure, "S19P_PRIVACY"
            assert str(caught.value) == f"{path}: E_GRAPH_BACKEND", "S19P_PRIVACY"
        if site == "graph_publish":
            # Real external from_dict interleaving, paused after its flag assignment.
            entered, release = threading.Event(), threading.Event()

            def external():
                def trace(frame, event, arg):
                    if (
                        event == "line"
                        and frame.f_code is gp.SimpleGraph.from_dict.__code__
                        and graph._full_publish_pending
                    ):
                        sys.settrace(None)
                        entered.set()
                        release.wait(10)
                    return trace

                sys.settrace(trace)
                try:
                    graph.from_dict({"nodes": [], "edges": []})
                finally:
                    sys.settrace(None)

            thread = threading.Thread(target=external)
            thread.start()
            assert await asyncio.to_thread(entered.wait, 5), "S19P_PRIVACY: conditional interleaving"
    try:
        if not site.startswith("graph_"):
            if site == "lance_optimize":
                operation = provider.optimize
            elif site.endswith("batch"):

                async def operation():
                    return await provider.upsert_batch([{"id": str(uuid4()), "vector": [1.0, 0.0, 0.0], "payload": {}}])
            else:

                async def operation():
                    return await provider.upsert(point_id=str(uuid4()), vector=[1.0, 0.0, 0.0], payload={})

            with pytest.raises(ProviderWriteError) as caught:
                await operation()
            assert type(caught.value) is ProviderWriteError and caught.value.__cause__ is failure, "S19P_PRIVACY"
            assert str(caught.value) == expected, "S19P_PRIVACY"
            probes.append(str(caught.value))
        if site == "lance_optimize":
            tasks = []

            def spawn(factory, *, kind):
                task = asyncio.create_task(factory())
                tasks.append(task)
                return task

            worker._task_spawner = spawn
            await worker._maybe_compact()
            await asyncio.gather(*tasks)
            assert await p3_rows(worker) == original_rows, "S19P_PRIVACY"
        elif site == "graph_resync":
            await worker._poll_once()
        elif site.endswith("batch"):
            await worker._poll_once()
        else:
            if site.startswith("graph_"):
                await p3_add(worker)
            await worker.process_all_pending()
        rows = await p3_rows(worker)
        if site.startswith("graph_"):
            assert all(row[1:4] == ("completed", 0, None) for row in rows), "S19P_PRIVACY"
        elif site != "lance_optimize":
            assert all(row[1:4] == ("failed", 1, "E_VECTOR_BACKEND") for row in rows), "S19P_PRIVACY"
        assert calls, "S19P_PRIVACY: I/O boundary not crossed"
        records = [r for r in caplog.records if r.name.startswith(("storage.", "memory_server."))]
        assert any(r.levelno >= logging.WARNING for r in records), "S19P_PRIVACY: catch not crossed"
        for record in records:
            assert secret not in repr((record.msg, record.args, record.exc_info, record.stack_info)), "S19P_PRIVACY"
            assert secret not in logging.Formatter().format(record), "S19P_PRIVACY"
            assert not record.exc_info and not record.stack_info, "S19P_PRIVACY"
        assert secret not in repr((rows, probes)), "S19P_PRIVACY"
    finally:
        if release is not None:
            release.set()
        if thread is not None:
            await asyncio.to_thread(thread.join, 5)
            assert not thread.is_alive()
        if site.startswith("qdrant_"):
            provider._client.close()


async def p3_native_claim(h, monkeypatch, *, cancel):
    import asyncio
    import subprocess
    import sys
    import threading

    from storage.outbox import OutboxRepository

    entered, release = threading.Event(), threading.Event()
    consumer = contender = None
    async with h.daemon.acquire(h.ctx) as lease:
        runtime = lease.runtime
        worker = runtime.dependencies.outbox_worker
        p3_surface(worker)
        worker.stop()
        async with asyncio.timeout(5):
            while any(r.kind == "outbox-worker" for r in runtime.owned_snapshot()[1]):
                await asyncio.sleep(0)
        # The shared S04 vector recipe is search-only; use graph native I/O.
        await p3_add(worker, operation="index_decision")
        original = worker._graph_router.sync_decision

        def native(*args, **kwargs):
            entered.set()
            release.wait(20)
            return original(*args, **kwargs)

        monkeypatch.setattr(worker._graph_router, "sync_decision", native)
        worker._stale_processing_seconds = -1

        async def apply():
            async with worker.claim_scope(deadline=monotonic() + 10) as claim:
                async with worker._session_factory() as session:
                    repo = OutboxRepository(session)
                    entries = await repo.claim_pending()
                    claim.register_claimed(session, entries)
                    await session.commit()
                    try:
                        result = await worker.process_claimed(session, repo, entries[0], claim=claim)
                        await session.commit()
                        return result
                    except BaseException:
                        await session.rollback()
                        raise

        try:
            consumer = asyncio.create_task(apply())
            assert await asyncio.to_thread(entered.wait, 5), "S19P_FENCE"
            before = await p3_rows(worker)
            assert before[0][1:4] == ("processing", 0, None), "S19P_FENCE"
            contender = asyncio.create_task(worker._poll_once())
            await asyncio.sleep(0)
            assert not contender.done(), "S19P_FENCE"
            if cancel:
                consumer.cancel()
                await asyncio.sleep(0)
                consumer.cancel()
                await asyncio.sleep(0)
                assert not consumer.done(), "S19P_FENCE: cancelled before native settlement"
                try:
                    report = await runtime.close()
                except asyncio.CancelledError:
                    # The real shutdown cancels this test's admitted request too.
                    report = await runtime.close()
                assert report.lock_retained and runtime.lock is not None, "S19P_FENCE"
                code = (
                    "import pathlib,sys; from memory_server.storage_lock import RuntimeStorageLock,StorageLockError; "
                    "root=pathlib.Path(sys.argv[1]);\n"
                    "try: RuntimeStorageLock.acquire_exclusive(root,timeout=0.01)\n"
                    "except StorageLockError as e: print(e.code)\n"
                    "else: raise AssertionError('S19P_FENCE: EX released')"
                )
                result = await asyncio.to_thread(
                    subprocess.run,
                    [sys.executable, "-c", code, str(runtime.root)],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                assert result.returncode == 0 and result.stdout.strip() == "E_LOCK_TIMEOUT", "S19P_FENCE"
            else:
                from dataclasses import replace

                async with h.daemon.acquire(replace(h.ctx, profile_id="beta", principal=h.worker_b)) as beta:
                    other = beta.runtime.dependencies.outbox_worker
                    other.stop()
                    async with asyncio.timeout(5):
                        while any(r.kind == "outbox-worker" for r in beta.runtime.owned_snapshot()[1]):
                            await asyncio.sleep(0)
                    await p3_add(other)
                    assert await other.process_all_pending() == {"processed": 1, "failed": 0}, "S19P_FENCE"
            assert await p3_rows(worker) == before, "S19P_FENCE"
            release.set()
            if cancel:
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(consumer, 5)
                assert (await p3_rows(worker))[0][1:4] == ("processing", 0, None), "S19P_FENCE"
            else:
                assert await asyncio.wait_for(consumer, 5) == "completed", "S19P_FENCE"
            await asyncio.wait_for(contender, 5)
            assert not any(r.thread.is_alive() for r in runtime.owned_snapshot()[2]), "S19P_FENCE"
        finally:
            release.set()
            for task in (consumer, contender):
                if task is not None:
                    await asyncio.gather(task, return_exceptions=True)


async def test_s19p_live_claim_survives_stale_reset(seam_owner, monkeypatch):
    from storage.outbox_worker import OutboxWorker

    p3_surface(OutboxWorker())
    await p3_native_claim(seam_owner, monkeypatch, cancel=False)


async def test_s19p_cancel_retains_fence_until_settle(seam_owner, monkeypatch):
    from storage.outbox_worker import OutboxWorker

    p3_surface(OutboxWorker())
    await p3_native_claim(seam_owner, monkeypatch, cancel=True)


@pytest.mark.parametrize("failure", ["storage", "timeout", "cancel"])
async def test_s19p_p3_claim_lock_released_on_finish_error(p3_worker, failure):
    import asyncio

    from sqlalchemy import event
    from sqlalchemy.exc import SQLAlchemyError
    from storage.outbox import OutboxRepository

    worker = p3_worker
    error = p3_surface(worker)
    await p3_add(worker)
    worker._stale_processing_seconds = -1
    faults = {"storage": SQLAlchemyError, "timeout": TimeoutError, "cancel": asyncio.CancelledError}
    calls = []

    def fail_read(connection, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT") and "outbox_entries" in statement:
            calls.append(statement)
            raise faults[failure]()

    try:
        expected = asyncio.CancelledError if failure == "cancel" else error
        with pytest.raises(expected):
            async with worker.claim_scope(deadline=monotonic() + 5) as claim:
                async with worker._session_factory() as session:
                    entries = await OutboxRepository(session).claim_pending()
                    claim.register_claimed(session, entries)
                    await session.commit()
                event.listen(worker._engine.sync_engine, "before_cursor_execute", fail_read)
    finally:
        event.remove(worker._engine.sync_engine, "before_cursor_execute", fail_read)
    assert calls, "S19P_FINISH: real readback boundary not crossed"
    assert not worker._claim_lock.locked(), "S19P_FINISH: claim lock leaked"
    assert worker._active_claim is None and not claim.active, "S19P_FINISH"
    if failure != "cancel":
        assert claim in worker._retained_claims, "S19P_FINISH: custody marker lost"
    assert await asyncio.wait_for(worker._poll_once(), 1) == 1, "S19P_FINISH: next poll blocked"
    assert not worker._retained_claims, "S19P_FINISH"
    assert (await p3_rows(worker))[0][1:4] == ("completed", 0, None), "S19P_FINISH"


async def test_s19p_p3_ownerless_retain_pruned(p3_worker):
    import asyncio

    from storage.outbox import OutboxRepository

    worker = p3_worker
    await p3_add(worker)
    worker._stale_processing_seconds = -1
    async with worker.claim_scope(deadline=monotonic() + 5) as claim:
        async with worker._session_factory() as session:
            entries = await OutboxRepository(session).claim_pending()
            claim.register_claimed(session, entries)
            await session.commit()
    assert claim.owner is None and not claim.natives
    assert claim in worker._retained_claims, "S19P_FINISH"
    assert await asyncio.wait_for(worker._poll_once(), 1) == 1, "S19P_FINISH: stale reset disabled"
    assert not worker._retained_claims, "S19P_FINISH"
    assert (await p3_rows(worker))[0][1:4] == ("completed", 0, None), "S19P_FINISH"


async def test_s19p_retry_batch_native_parity(p3_worker, monkeypatch):
    from memory_server.providers.embedding_provider import MockEmbeddingProvider

    worker = p3_worker
    p3_surface(worker)
    worker._embedder = MockEmbeddingProvider(vector_size=3)
    original = worker._embedder.embed_batch
    batches = []

    def embed_batch(texts):
        batches.append(tuple(texts))
        return original(texts)

    monkeypatch.setattr(worker._embedder, "embed_batch", embed_batch)
    for _ in range(33):
        await p3_add(worker, operation="index_fact", payload={"subject": "s", "predicate": "p", "object": "o"})
    await p3_add(worker)
    assert worker._fact_batch_chunk_size == 32 and worker._poll_batch_size == 500
    assert worker._process_pending_limit == 500 and worker._max_retries == 3
    submitter, spawner, engine = worker._native_submitter, worker._task_spawner, worker._engine
    assert await worker._poll_once() == 34, "S19P_PARITY"
    assert [len(batch) for batch in batches] == [32, 1], "S19P_PARITY"
    assert all(row[1:4] == ("completed", 0, None) for row in await p3_rows(worker)), "S19P_PARITY"
    assert (worker._native_submitter, worker._task_spawner, worker._engine) == (submitter, spawner, engine)
    await p3_add(worker, operation="unknown")
    assert await worker.process_all_pending() == {"processed": 0, "failed": 0}, "S19P_PARITY"
    assert await worker.process_all_pending() == {"processed": 0, "failed": 0}, "S19P_PARITY"
    assert await worker.process_all_pending() == {"processed": 0, "failed": 1}, "S19P_PARITY"


async def test_s19p_process_claimed_contract_negative_matrix(p3_worker, tmp_path):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from storage.outbox import OutboxRepository
    from storage.outbox_worker import OutboxWorker

    worker = p3_worker
    error = p3_surface(worker)
    entry = await p3_add(worker)
    foreign_engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'foreign.sqlite'}")
    try:
        async with worker.claim_scope(deadline=monotonic() + 5) as claim:
            async with worker._session_factory() as session, worker._session_factory() as other:
                repo = OutboxRepository(session)
                entries = await repo.claim_pending()
                await session.commit()
                with pytest.raises(error):
                    await worker.process_claimed(session, repo, entries[0], claim=claim)
                claim.register_claimed(session, entries)
                async with async_sessionmaker(foreign_engine)() as foreign:
                    for target, borrowed, repository, token in (
                        (worker, None, repo, claim),
                        (worker, other, OutboxRepository(other), claim),
                        (worker, foreign, OutboxRepository(foreign), claim),
                        (worker, session, OutboxRepository(other), claim),
                        (worker, session, repo, None),
                        (OutboxWorker(engine=worker._engine), session, repo, claim),
                    ):
                        with pytest.raises(error) as caught:
                            await target.process_claimed(borrowed, repository, entries[0], claim=token)
                        assert caught.value.code == "E_MAINTENANCE_CONFLICT", "S19P_CLAIM"
                with pytest.raises(error):
                    await asyncio.create_task(worker.process_claimed(session, repo, entries[0], claim=claim))
                assert (await p3_rows(worker))[0][1:4] == ("processing", 0, None), "S19P_CLAIM"
                # A real competing repository mutation makes the registered stamp/status stale.
                await OutboxRepository(other).mark_completed(entry.id)
                await other.commit()
                with pytest.raises(error):
                    await worker.process_claimed(session, repo, entries[0], claim=claim)
                await session.rollback()
        assert (await p3_rows(worker))[0][1:4] == ("completed", 0, None), "S19P_CLAIM"
    finally:
        await foreign_engine.dispose()


async def test_s19p_live_claim_survives_stale_reset_after_timeout(seam_owner, monkeypatch):
    import asyncio
    import sys
    import threading

    import memory_server.runtime.daemon as daemon_module
    from memory_server.runtime.runtime import CMMSRuntime

    release, parked = threading.Event(), threading.Event()
    task = None
    async with seam_owner.daemon.acquire(seam_owner.ctx) as lease:
        runtime = lease.runtime
        worker = runtime.dependencies.outbox_worker
        p3_surface(worker)
        worker.stop()
        async with asyncio.timeout(5):
            while any(r.kind == "outbox-worker" for r in runtime.owned_snapshot()[1]):
                await asyncio.sleep(0)
        await p3_add(worker)
        original = worker._graph_router.sync_decision
        calls = []

        def native(*args, **kwargs):
            calls.append(True)
            value = original(*args, **kwargs)

            def profile(frame, event, arg):
                if event == "return" and frame.f_code is CMMSRuntime._native_target.__code__:
                    sys.setprofile(None)
                    parked.set()
                    release.wait(20)

            sys.setprofile(profile)
            return value

        monkeypatch.setattr(worker._graph_router, "sync_decision", native)
        monkeypatch.setattr(daemon_module, "_DRAIN_SECONDS", 0.05)
        worker._stale_processing_seconds = -1
        try:
            task = asyncio.create_task(worker._poll_once())
            assert await asyncio.to_thread(parked.wait, 5), "S19P_FENCE"
            assert await asyncio.wait_for(task, 5) == 0, "S19P_FENCE"
            before = await p3_rows(worker)
            assert before[0][1:4] == ("processing", 0, None), "S19P_FENCE"
            assert any(r.thread.is_alive() for r in runtime.owned_snapshot()[2]), "S19P_FENCE"
            assert runtime.lock is not None, "S19P_FENCE"
            assert await asyncio.wait_for(worker._poll_once(), 5) == 0, "S19P_FENCE"
            assert await asyncio.wait_for(worker.process_all_pending(), 5) == {"processed": 0, "failed": 0}
            assert await p3_rows(worker) == before and len(calls) == 1, "S19P_FENCE"
        finally:
            release.set()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            async with asyncio.timeout(5):
                while any(r.thread.is_alive() for r in runtime.owned_snapshot()[2]):
                    await asyncio.sleep(0)
        monkeypatch.setattr(worker._graph_router, "sync_decision", original)
        assert await worker._poll_once() == 1, "S19P_FENCE: terminated native left a permanent fence"
        assert (await p3_rows(worker))[0][1:4] == ("completed", 0, None), "S19P_FENCE"


async def test_s19p_preview_contract_roundtrip(seam_owner):
    import hashlib
    import json

    from pydantic import ValidationError

    from memory_server.gateway_contracts import dto as d
    from memory_server.gateway_contracts.ports import Principal, RequestContext

    name = "memory_server.application.maintenance_preview"
    assert importlib.util.find_spec(name) is not None, "S19P_SURFACE"
    m = importlib.import_module(name)
    assert callable(getattr(m, "MaintenancePlanCodec", None)), "S19P_SURFACE"
    now = datetime(2026, 11, 7, tzinfo=UTC)
    principal = Principal(
        "synthetic", "worker-A", None, frozenset({"alpha"}), frozenset({"index.manage"}), 1, now + timedelta(hours=1)
    )
    ctx = RequestContext("req_p4", principal, "alpha", None, monotonic() + 30)
    ident = str(uuid4())
    row = m.MaintenanceRow(ident, str(uuid4()), "fact", "index_fact", "failed", 1, now, None)
    canonical = json.dumps(
        [[row.id, row.record_id, "fact", "index_fact", "failed", 1, "2026-11-07T00:00:00.000000Z", None]],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()
    material = m.MaintenancePlanMaterial(
        1,
        "cmms.outbox.maintenance-plan.v1",
        "alpha",
        1,
        "worker-A",
        "failed",
        (ident,),
        1,
        (row,),
        hashlib.sha256(canonical).hexdigest(),
        now,
        now + timedelta(seconds=900),
    )
    codec = m.MaintenancePlanCodec(bytes(range(32)))
    token = codec.issue(material)
    plan = d.MaintenancePlan(
        selection_digest=token,
        selected_ids=[ident],
        selected_count=1,
        expires_at=material.expiry,
        generation=material.generation,
        selection="failed",
    )
    wire = plan.model_dump_json()
    assert d.MaintenancePlan.model_validate_json(wire) == plan
    assert set(json.loads(wire)) == {
        "selection_digest",
        "selected_ids",
        "selected_count",
        "expires_at",
        "generation",
        "selection",
    }
    assert (
        codec.verify(
            token,
            ctx=ctx,
            selection="failed",
            selected_ids=(ident,),
            expected_count=1,
            registry_generation=1,
            rows=(row,),
            now=now,
        )
        == material
    ), "S19P_PLAN_TOKEN"
    for changes in ({"selected_count": True}, {"extra": 1}, {"selected_ids": [ident, ident]}, {"selected_count": 0}):
        with pytest.raises(ValidationError):
            d.MaintenancePlan.model_validate({**plan.model_dump(), **changes})
    assert "maintenance_plan" not in d.Page.model_fields
    assert (
        d.OutboxMaintenancePage(items=[], next_cursor=None, has_more=False, request_id="req_p4").maintenance_plan
        is None
    )

    from pathlib import Path

    for forbidden in ("diagnostics_service", "DiagnosticsReadWork", "mutation_uow", "S10"):
        assert forbidden not in Path(m.__file__).read_text(), "S19P_SURFACE"
    async with seam_owner.daemon.acquire(seam_owner.ctx) as lease:
        runtime = lease.runtime
        engine = runtime.dependencies.sqlite.engine
        assert runtime.dependencies.outbox_worker._engine is engine
        await p4_stop_worker(runtime)
        await p4_seed(runtime, ("failed", "failed", "pending", "completed"))
        await p4_preview_on_engine(m, runtime, seam_owner.ctx, limit=100, count=2)
    beta_ctx = replace(seam_owner.ctx, principal=seam_owner.worker_b, profile_id="beta")
    async with seam_owner.daemon.acquire(beta_ctx) as lease:
        await p4_stop_worker(lease.runtime)
        empty = await p4_preview_on_engine(m, lease.runtime, beta_ctx, limit=100, count=0)
        assert empty.generation == hashlib.sha256(b"[]").hexdigest()
        await p4_seed(lease.runtime, ("failed", "failed"))
        await p4_preview_on_engine(m, lease.runtime, beta_ctx, limit=1, count=1)

    rows = tuple(replace(row, id=str(uuid4())) for _ in range(100))
    rows = tuple(sorted(rows, key=lambda row: (row.created_at, row.id)))
    now = datetime.max.replace(tzinfo=UTC) - timedelta(seconds=900)
    long_ctx = replace(ctx, profile_id="p" * 90, principal=replace(principal, subject="s" * 200))
    large = p4_material(m, long_ctx, rows, now, registry_generation=9223372036854775807)
    token = codec.issue(large)
    import base64

    assert token.isascii() and len(token) <= 512
    assert len(base64.urlsafe_b64decode(token.split(".")[0] + "==")) <= 384
    assert p4_verify(codec, token, long_ctx, large) == large


async def p4_stop_worker(runtime):
    import asyncio

    runtime.dependencies.outbox_worker.stop()
    async with asyncio.timeout(5):
        while any(record.kind == "outbox-worker" for record in runtime.owned_snapshot()[1]):
            await asyncio.sleep(0)


async def p4_seed(runtime, statuses):
    from storage.outbox import OutboxRepository

    async with runtime.dependencies.outbox_worker._session_factory() as session:
        repo = OutboxRepository(session)
        for status in statuses:
            entry = await repo.add_entry("fact", str(uuid4()), "index_fact", {})
            await session.commit()
            if status == "failed":
                assert await repo.mark_failed(entry.id, "E_STORAGE")
            elif status == "completed":
                assert await repo.mark_completed(entry.id)
            await session.commit()


async def p4_counts(engine):
    from sqlalchemy import text

    async with engine.connect() as connection:
        statuses = tuple(
            (
                await connection.execute(
                    text("SELECT status, COUNT(*) FROM outbox_entries GROUP BY status ORDER BY status")
                )
            ).all()
        )
        total = (await connection.execute(text("SELECT COUNT(*) FROM outbox_entries"))).scalar_one()
    return statuses, total


def p4_generation(rows):
    import hashlib
    import json

    values = [
        [
            row.id,
            row.record_id,
            row.record_type,
            row.operation,
            row.status,
            row.retry_count,
            row.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            None if row.processed_at is None else row.processed_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        ]
        for row in rows
    ]
    return hashlib.sha256(
        json.dumps(values, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    ).hexdigest()


def p4_material(m, ctx, rows, now, *, registry_generation=1):
    return m.MaintenancePlanMaterial(
        1,
        "cmms.outbox.maintenance-plan.v1",
        ctx.profile_id,
        registry_generation,
        ctx.principal.subject,
        "failed",
        tuple(row.id for row in rows),
        len(rows),
        rows,
        p4_generation(rows),
        now,
        now + timedelta(seconds=900),
    )


def p4_verify(codec, token, ctx, material, **changes):
    args = dict(
        ctx=ctx,
        selection=material.selection,
        selected_ids=material.selected_ids,
        expected_count=material.expected_count,
        registry_generation=material.registry_generation,
        rows=material.rows,
        now=material.issued_at,
    )
    return codec.verify(token, **(args | changes))


async def p4_preview_on_engine(m, runtime, ctx, *, limit, count):
    from sqlalchemy import DateTime, event, text

    engine = runtime.dependencies.sqlite.engine
    assert runtime.dependencies.outbox_worker._engine is engine
    sql = (
        "SELECT id,record_id,record_type,operation,status,retry_count,created_at,processed_at\n"
        "FROM outbox_entries WHERE status=:selection\n"
        "ORDER BY created_at,id LIMIT :fetch_limit"
    )
    records, snapshots = [], []

    def recorder(connection, cursor, statement, parameters, context, executemany):
        if "outbox_entries" in statement:
            records.append((statement, parameters, context.compiled, context.compiled_parameters))

    async def read_rows(ctx, *, selection, limit):
        async with engine.connect() as connection:
            result = await connection.execute(
                text(sql).columns(created_at=DateTime(), processed_at=DateTime()),
                {"selection": selection, "fetch_limit": limit + 1},
            )
            rows = []
            for value in result.mappings():
                data = dict(value)
                for field in ("created_at", "processed_at"):
                    if data[field] is not None:
                        assert data[field].tzinfo is None
                        data[field] = data[field].replace(tzinfo=UTC)
                rows.append(m.MaintenanceRow(**data))
        snapshots.append(tuple(rows))
        return m.MaintenanceReadSnapshot(ctx.profile_id, 1, tuple(rows))

    async def authorize(ctx):
        return None

    codec = m.MaintenancePlanCodec(bytes(range(32)))
    provider = m.MaintenancePreviewProvider(
        authorize=authorize, read_rows=read_rows, codec=codec, clock=lambda: datetime.now(UTC)
    )
    before = await p4_counts(engine)
    event.listen(engine.sync_engine, "before_cursor_execute", recorder)
    try:
        plan = await provider.preview(ctx, selection="failed", limit=limit)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", recorder)
    assert await p4_counts(engine) == before
    assert len(records) == 1
    statement, parameters, compiled, binds = records[0]
    assert " ".join(str(compiled.statement).split()) == " ".join(sql.split())
    assert " ".join(statement.split()) == " ".join(sql.replace(":selection", "?").replace(":fetch_limit", "?").split())
    assert binds == [{"selection": "failed", "fetch_limit": limit + 1}]
    assert dict(zip(compiled.positiontup, parameters, strict=True)) == binds[0]
    assert binds[0]["fetch_limit"] <= 101
    rows = snapshots[0][:limit]
    assert len(snapshots[0]) == (limit + 1 if limit == 1 else count)
    assert plan.selected_count == count
    assert plan.selected_ids == [row.id for row in rows]
    assert plan.generation == p4_generation(rows)
    now = plan.expires_at - timedelta(seconds=900)
    material = p4_material(m, ctx, rows, now)
    assert codec.issue(material) == plan.selection_digest
    assert p4_verify(codec, plan.selection_digest, ctx, material) == material
    assert plan.selection_digest.isascii() and len(plan.selection_digest) <= 512
    return plan


@pytest.mark.parametrize(
    "case",
    [
        "key-none",
        "key-short",
        "key-str",
        "key-empty",
        "ttl-zero",
        "ttl-large",
        "ttl-bool",
        "file-valid",
        "file-root",
        "file-mode",
        "file-symlink",
        "file-short",
        "file-long",
        "file-hardlink",
        "file-missing",
        "rotation",
        "auth-required",
        "auth-denied",
        "none-authorize",
        "none-read_rows",
        "none-codec",
        "none-clock",
        "selection-done",
        "selection-none",
        "limit-zero",
        "limit-large",
        "limit-bool",
        "deadline",
        "read-timeout",
        "read-storage",
        "snapshot-profile",
        "duplicate-rows",
        "row-status",
        "row-bound",
        "row-retry-bool",
        "token-duplicate",
        "token-padding",
        "token-noncanonical",
        "token-long",
        "token-unicode",
        "token-version",
        "token-mac",
        "token-expired",
        "token-future",
        "token-ttl",
        "token-r",
        "token-g",
        "token-ids",
        "token-count",
        "token-retry",
        "token-subject",
        "uuid-uppercase",
        "uuid-lowercase",
    ],
)
async def test_s19p_preview_negative_matrix(seam_owner, tmp_path, case):
    import base64
    import json
    import os

    assert importlib.util.find_spec("memory_server.application.maintenance_preview") is not None, "S19P_SURFACE"
    from memory_server.application import maintenance_preview as m
    from memory_server.control.jobs import JobError
    from memory_server.control.store import ControlError

    h = seam_owner
    codec = m.MaintenancePlanCodec(bytes(range(32)))
    row = m.MaintenanceRow(
        "abcdefab-1234-1234-1234-123456abcdef", str(uuid4()), "fact", "index_fact", "failed", 0, h.now, None
    )
    material = p4_material(m, h.ctx, (row,), h.now)
    token = codec.issue(material)
    expected = "E_VALIDATION"

    if case.startswith("key-") or case.startswith("ttl-"):
        options = {
            "key-none": dict(key=None),
            "key-short": dict(key=b"x" * 31),
            "key-str": dict(key="x" * 32),
            "key-empty": dict(key=b""),
            "ttl-zero": dict(key=b"x" * 32, ttl_seconds=0),
            "ttl-large": dict(key=b"x" * 32, ttl_seconds=901),
            "ttl-bool": dict(key=b"x" * 32, ttl_seconds=True),
        }
        if case == "key-none":
            expected = "E_CAPABILITY_UNAVAILABLE"
        with pytest.raises(JobError) as exc:
            m.MaintenancePlanCodec(**options[case])
    elif case.startswith("file-"):
        root = tmp_path / "key-root"
        root.mkdir(mode=0o700)
        path = root / "maintenance-plan-v1.key"
        if case != "file-missing":
            path.write_bytes(b"k" * {"file-short": 31, "file-long": 33}.get(case, 32))
            os.chmod(path, 0o600)
        if case == "file-root":
            os.chmod(root, 0o755)
        elif case == "file-mode":
            os.chmod(path, 0o644)
        elif case == "file-hardlink":
            os.link(path, root / "hardlink")
            assert path.stat().st_nlink == 2
        elif case == "file-symlink":
            target = root / "target"
            path.rename(target)
            os.symlink(target, path)
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            if case == "file-valid":
                assert m.load_maintenance_plan_key(fd) == b"k" * 32
                return
            expected = "E_STORAGE"
            with pytest.raises(JobError) as exc:
                m.load_maintenance_plan_key(fd)
        finally:
            os.close(fd)
    elif case in {"rotation", "uuid-uppercase", "uuid-lowercase"} or case.startswith("token-"):
        args = {}
        if case == "uuid-lowercase":
            assert p4_verify(codec, token, h.ctx, material) == material
            assert material.selected_ids == ("abcdefab-1234-1234-1234-123456abcdef",)
            return
        if case == "uuid-uppercase":
            args = dict(selected_ids=(row.id.upper(),), rows=(replace(row, id=row.id.upper()),))
        elif case in {"token-duplicate", "token-version", "token-g"}:
            header_segment, mac = token.split(".")
            raw = base64.urlsafe_b64decode(header_segment + "==")
            header = json.loads(raw)
            if case == "token-duplicate":
                raw = b'{"v":1,' + raw[1:]
            else:
                header["v" if case == "token-version" else "g"] = 2 if case == "token-version" else "b" * 64
                raw = json.dumps(header, sort_keys=True, separators=(",", ":")).encode()
            token = base64.urlsafe_b64encode(raw).rstrip(b"=").decode() + "." + mac
        elif case == "token-padding":
            token = token.replace(".", "=.")
        elif case == "token-noncanonical":
            head, mac = token.split(".")
            alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
            index = alphabet.index(mac[-1])
            assert index % 4 == 0
            token = head + "." + mac[:-1] + alphabet[index + 1]
        elif case == "token-long":
            token = "a" * 513
        elif case == "token-unicode":
            token = "é"
        else:
            expected = "E_MAINTENANCE_CONFLICT"
            if case == "rotation":
                codec = m.MaintenancePlanCodec(b"b" * 32)
            elif case == "token-mac":
                head, mac = token.split(".")
                token = head + "." + ("A" if mac[0] != "A" else "B") + mac[1:]
            elif case == "token-expired":
                args["now"] = material.expiry
            elif case == "token-future":
                args["now"] = material.issued_at - timedelta(microseconds=1)
            elif case == "token-ttl":
                codec = m.MaintenancePlanCodec(bytes(range(32)), ttl_seconds=60)
            elif case == "token-r":
                args["registry_generation"] = 2
            elif case == "token-ids":
                args["selected_ids"] = (str(uuid4()),)
            elif case == "token-count":
                args.update(expected_count=0, selected_ids=())
            elif case == "token-retry":
                # Change a supported persisted field, then reconstruct the actual current row.
                from sqlalchemy import DateTime, text
                from storage.outbox import OutboxRepository

                async with h.daemon.acquire(h.ctx) as lease:
                    runtime = lease.runtime
                    await p4_stop_worker(runtime)
                    async with runtime.dependencies.outbox_worker._session_factory() as session:
                        repo = OutboxRepository(session)
                        entry = await repo.add_entry("fact", str(uuid4()), "index_fact", {})
                        await session.commit()
                        await repo.mark_failed(entry.id, "E_STORAGE")
                        await session.commit()
                        statement = text(
                            "SELECT id,record_id,record_type,operation,status,retry_count,created_at,processed_at "
                            "FROM outbox_entries WHERE id=:id"
                        ).columns(created_at=DateTime(), processed_at=DateTime())

                        async def persisted():
                            value = dict((await session.execute(statement, {"id": entry.id})).mappings().one())
                            for field in ("created_at", "processed_at"):
                                if value[field] is not None:
                                    value[field] = value[field].replace(tzinfo=UTC)
                            return m.MaintenanceRow(**value)

                        material = p4_material(m, h.ctx, (await persisted(),), h.now)
                        token = codec.issue(material)
                        await repo.increment_retry(entry.id, "E_STORAGE")
                        await session.commit()
                        args["rows"] = (await persisted(),)
                        assert args["rows"][0].retry_count == material.rows[0].retry_count + 1
            elif case == "token-subject":
                args["ctx"] = replace(h.ctx, principal=replace(h.worker_a, subject="worker-B"))
        if case == "token-g":
            expected = "E_MAINTENANCE_CONFLICT"
        target_ctx = args.pop("ctx", h.ctx)
        with pytest.raises(JobError) as exc:
            p4_verify(codec, token, target_ctx, material, **args)
    else:
        async with h.daemon.acquire(h.ctx) as lease:
            runtime = lease.runtime
            await p4_stop_worker(runtime)
            await p4_seed(runtime, ("failed", "pending", "completed"))
            engine = runtime.dependencies.sqlite.engine
            before = await p4_counts(engine)
            reads = []

            async def authorize(ctx):
                if case in {"auth-required", "auth-denied"}:
                    raise JobError("E_AUTH_REQUIRED" if case == "auth-required" else "E_PERMISSION_DENIED")

            async def read_rows(ctx, *, selection, limit):
                reads.append(True)
                if case == "read-timeout":
                    raise TimeoutError
                if case == "read-storage":
                    raise ControlError("E_STORAGE")
                rows = (row,)
                if case == "duplicate-rows":
                    rows = (row, row)
                elif case == "row-status":
                    rows = (replace(row, status="pending"),)
                elif case == "row-retry-bool":
                    rows = (replace(row, retry_count=True),)
                elif case == "row-bound":
                    rows = tuple(
                        sorted((replace(row, id=str(uuid4())) for _ in range(3)), key=lambda r: (r.created_at, r.id))
                    )
                return m.MaintenanceReadSnapshot("beta" if case == "snapshot-profile" else ctx.profile_id, 1, rows)

            options = dict(authorize=authorize, read_rows=read_rows, codec=codec, clock=lambda: h.now)
            preview_args = dict(selection="failed", limit=1)
            ctx = h.ctx
            if case.startswith("none-"):
                options[case.removeprefix("none-")] = None
                expected = "E_CAPABILITY_UNAVAILABLE"
            elif case.startswith("auth-"):
                expected = "E_AUTH_REQUIRED" if case == "auth-required" else "E_PERMISSION_DENIED"
            elif case.startswith("selection-"):
                preview_args["selection"] = "done" if case == "selection-done" else None
            elif case.startswith("limit-"):
                preview_args["limit"] = {"limit-zero": 0, "limit-large": 101, "limit-bool": True}[case]
            elif case == "deadline":
                ctx = replace(ctx, deadline=monotonic() - 1)
                expected = "E_TIMEOUT"
            elif case.startswith("read-"):
                expected = "E_TIMEOUT" if case == "read-timeout" else "E_STORAGE"
            else:
                expected = "E_INTERNAL"
            try:
                with pytest.raises(JobError) as exc:
                    provider = m.MaintenancePreviewProvider(**options)
                    await provider.preview(ctx, **preview_args)
            finally:
                assert await p4_counts(engine) == before, "S19P_PREVIEW_READ_ONLY"
            if case.startswith(("none-", "auth-", "selection-", "limit-")) or case == "deadline":
                assert reads == []
    assert exc.value.code == expected, case


async def test_s19p_c6_plan_token_journal(seam_owner):
    import hashlib
    import json

    assert importlib.util.find_spec("memory_server.application.maintenance_preview") is not None, "S19P_SURFACE"
    from memory_server.application import maintenance_preview as m
    from memory_server.control.jobs import JobError, request_digest
    from memory_server.gateway_contracts import dto as d
    from memory_server.gateway_contracts.ports import AuditTicket

    h = seam_owner
    for legacy_digest in ("a" * 64, "legacy-digest"):
        request = d.MaintenanceRequest(
            reason="synthetic",
            confirmation=True,
            selection="failed",
            event_ids=[str(uuid4())],
            selection_digest=legacy_digest,
            expected_count=1,
        )
        assert d.MaintenanceRequest.model_validate_json(request.model_dump_json()) == request
        ctx = replace(h.ctx, idempotency_key=uuid4())
        golden = json.dumps(
            {"method": "POST", "route": "outbox.replay", "path_ids": {}, "payload": request.model_dump(mode="json")},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
        digest = request_digest("outbox_replay", request)
        assert digest == hashlib.sha256(golden).hexdigest(), "S19P_C6"

        def enqueue():
            return h.metadata.enqueue(
                ctx,
                "outbox_replay",
                request,
                digest=digest,
                ticket=AuditTicket("ticket", ctx.request_id),
                now=h.now,
            )

        first = enqueue()

        def snapshot():
            with h.store.borrow_transaction() as tx:
                value = tx.fetchone("SELECT accepted_snapshot FROM job_requests WHERE job_id=?", (first.job_id,))
                count = tx.fetchone("SELECT COUNT(*) AS n FROM job_requests", ())
                jobs = tx.fetchone("SELECT COUNT(*) AS n FROM jobs", ())
            return value["accepted_snapshot"], count["n"], jobs["n"]

        before = snapshot()
        # Complete the existing journal lifecycle before exercising C6 replay.
        from memory_server.gateway_contracts.ports import AuditOutcome

        first = h.metadata.finalize_request(first, AuditOutcome(202, None, (), 0.0), now=h.now)
        h.metadata.mark_published(first, now=h.now)
        assert snapshot() == before
        assert isinstance(before[0], (bytes, str))
        for _ in range(10):
            duplicate = enqueue()
            assert duplicate.replayed and duplicate.accepted == first.accepted
            assert snapshot() == before, "S19P_C6"
        codec = m.MaintenancePlanCodec(bytes(range(32)))
        material = p4_material(m, ctx, (), h.now)
        with pytest.raises(JobError) as exc:
            p4_verify(codec, legacy_digest, ctx, material)
        assert exc.value.code == "E_VALIDATION"
        assert snapshot() == before, "S19P_C6"


def test_s19p_p4_m4_mutant():
    import hashlib
    from pathlib import Path

    assert importlib.util.find_spec("memory_server.application.maintenance_preview") is not None, "S19P_SURFACE"
    from memory_server.application import maintenance_preview as m
    from memory_server.control.jobs import JobError
    from memory_server.gateway_contracts.ports import Principal, RequestContext

    path = Path(m.__file__)
    original = path.read_bytes()
    saved = hashlib.sha256(original).hexdigest()
    anchor = b"canonical(projection(material))"
    assert original.count(anchor) == 1, "S19P_PLAN_TOKEN"
    now = datetime(2026, 11, 7, tzinfo=UTC)
    principal = Principal(
        "synthetic", "worker-A", None, frozenset({"alpha"}), frozenset({"index.manage"}), 1, now + timedelta(hours=1)
    )
    ctx = RequestContext("req_m4", principal, "alpha", None, monotonic() + 30)
    foreign = replace(ctx, principal=replace(principal, subject="worker-B"))
    codec = m.MaintenancePlanCodec(bytes(range(32)))
    material = p4_material(m, ctx, (), now)
    pristine = codec.issue(material)
    try:
        path.write_bytes(original.replace(anchor, b"canonical(self._material(material))"))
        importlib.invalidate_caches()
        importlib.reload(m)
        material = p4_material(m, ctx, (), now)
        codec = m.MaintenancePlanCodec(bytes(range(32)))
        mutant = codec.issue(material)
        assert p4_verify(codec, mutant, foreign, material).subject == "worker-B", "S19P_PLAN_TOKEN"
        # The actual regression node must turn RED, not merely observe the mutant here.
        import os
        import subprocess
        import sys

        red = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "tests/gateway/test_s19p_prerequisites.py::test_s19p_preview_negative_matrix[token-subject]",
                "--basetemp=/home/shtorm/.hermes/cache/scratch/s19p-p4-fix2/m4-red",
                "-p",
                "no:cacheprovider",
            ],
            cwd=path.parents[3],
            env={**os.environ, "PYTHONPATH": "src", "PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert red.returncode == 1, red.stdout + red.stderr
        assert "DID NOT RAISE" in red.stdout and "1 failed" in red.stdout, red.stdout
        print("S19P_PLAN_TOKEN M4 RED: token-subject, 1 failed (DID NOT RAISE)")
    finally:
        path.write_bytes(original)
        importlib.invalidate_caches()
        importlib.reload(m)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == saved, "S19P_PLAN_TOKEN"
    material = p4_material(m, ctx, (), now)
    codec = m.MaintenancePlanCodec(bytes(range(32)))
    with pytest.raises(JobError) as exc:
        p4_verify(codec, codec.issue(material), foreign, material)
    assert exc.value.code == "E_MAINTENANCE_CONFLICT", "S19P_PLAN_TOKEN"
    assert codec.issue(material) == pristine
    assert p4_verify(codec, pristine, ctx, material) == material


async def test_s19p_startup_callback_before_ready(tmp_path, seam_owner, monkeypatch):
    from memory_server.runtime import read_composition
    from memory_server.runtime.runtime import CMMSRuntime

    assert callable(getattr(CMMSRuntime, "configure_startup_recovery", None)) and callable(
        getattr(CMMSRuntime, "check_startup_custody", None)
    ), "S19P_SURFACE"
    assert callable(getattr(read_composition, "create_job_recovery_builder", None)), "S19P_SURFACE"
    await p5_owner_startup(seam_owner, monkeypatch)
    import asyncio

    from memory_server.runtime.errors import RuntimeNotReadyError
    from memory_server.runtime.runtime import CMMSRuntime
    from memory_server.storage_lock import RuntimeStorageLock, StorageLockError

    root = tmp_path / "alpha"
    runtime = CMMSRuntime(root, profile_id="alpha")
    calls = []

    async def callback(current, *, deadline):
        calls.append(current)
        assert current is runtime and current.state == "STARTING"
        assert deadline > asyncio.get_running_loop().time()
        current.check_startup_custody(profile_id="alpha", canonical_root=root)
        for profile, other_root in (("beta", root), ("alpha", tmp_path / "beta")):
            with pytest.raises(RuntimeNotReadyError):
                current.check_startup_custody(profile_id=profile, canonical_root=other_root)
        assert current._admitted == set()
        with pytest.raises(RuntimeNotReadyError):
            current.spawn_owned_task(lambda: asyncio.sleep(0), kind="startup-probe")
        with pytest.raises(RuntimeNotReadyError):
            current.admit()
        assert current._admitted == set()
        with pytest.raises((StorageLockError, TimeoutError)):
            contender = await asyncio.wait_for(RuntimeStorageLock.acquire_exclusive_async(root, timeout=0.2), timeout=1)
            contender.release_exclusive()
        current.check_startup_custody(profile_id="alpha", canonical_root=root)
        assert current.state == "STARTING"

    runtime.configure_startup_recovery(callback, timeout_seconds=2)
    try:
        assert await runtime.start() is runtime
        assert runtime.state == "READY" and runtime.started is True
        assert calls == [runtime]
        assert await runtime.start() is runtime
        assert calls == [runtime]
        with pytest.raises(RuntimeNotReadyError):
            runtime.check_startup_custody(profile_id="alpha", canonical_root=root)
    finally:
        await runtime.close()


async def test_s19p_startup_failure_closed(tmp_path, seam_owner, monkeypatch):
    from memory_server.runtime import read_composition
    from memory_server.runtime.runtime import CMMSRuntime

    assert callable(getattr(CMMSRuntime, "configure_startup_recovery", None)) and callable(
        getattr(CMMSRuntime, "check_startup_custody", None)
    ), "S19P_SURFACE"
    assert callable(getattr(read_composition, "create_job_recovery_builder", None)), "S19P_SURFACE"
    await p5_owner_failure(seam_owner, monkeypatch)
    import asyncio
    from functools import partial

    from memory_server.control.jobs import JobError
    from memory_server.runtime.errors import RuntimeNotReadyError, StorageOwnershipError
    from memory_server.runtime.runtime import CMMSRuntime
    from memory_server.storage_lock import RuntimeStorageLock

    for mode, error in (("job", JobError), ("timeout", TimeoutError), ("cancel", asyncio.CancelledError)):
        root = tmp_path / mode
        runtime = CMMSRuntime(root, profile_id="alpha")
        calls = []

        async def callback(current, *, deadline):
            calls.append(current)
            assert current.state == "STARTING"
            current.check_startup_custody(profile_id="alpha", canonical_root=root)
            if mode == "job":
                raise JobError("E_TIMEOUT")
            if mode == "cancel":
                raise asyncio.CancelledError()
            await asyncio.sleep(60)
            pytest.fail("startup timeout did not cancel callback")

        runtime.configure_startup_recovery(callback, timeout_seconds=0.05)
        try:
            with pytest.raises(error) as exc:
                await asyncio.wait_for(runtime.start(), timeout=2)
            if mode == "job":
                assert exc.value.code == "E_TIMEOUT"
            if mode == "timeout":
                assert isinstance(runtime._start_error, TimeoutError)
            assert calls == [runtime]
            assert runtime.state != "READY" and runtime.started is False
            assert runtime._startup_window is False
            assert runtime._cleanup_task is not None
            await asyncio.wait_for(asyncio.shield(runtime._cleanup_task), timeout=2)
            assert runtime.lock is None
            assert runtime._cleanup_task.result().lock_retained is False
            with pytest.raises(RuntimeNotReadyError):
                runtime.check_startup_custody(profile_id="alpha", canonical_root=root)
            contender = await asyncio.wait_for(RuntimeStorageLock.acquire_exclusive_async(root, timeout=0.2), timeout=1)
            contender.release_exclusive()
        finally:
            await runtime.close()

    calls = []

    async def callback(current, *, deadline):
        calls.append(current)

    root = tmp_path / "occupied"
    holder = await RuntimeStorageLock.acquire_exclusive_async(root, timeout=0.2)
    runtime = CMMSRuntime(
        root,
        profile_id="alpha",
        acquire_lock=partial(RuntimeStorageLock.acquire_exclusive_async, timeout=0.2),
    )
    runtime.configure_startup_recovery(callback, timeout_seconds=1)
    try:
        with pytest.raises(StorageOwnershipError):
            await asyncio.wait_for(runtime.start(), timeout=2)
        assert calls == []
        assert runtime.started is False and runtime.lock is None
    finally:
        holder.release_exclusive()
        await runtime.close()

    runtime = CMMSRuntime(tmp_path / "registration", profile_id="alpha")
    with pytest.raises(ValueError):
        runtime.configure_startup_recovery(None, timeout_seconds=1)
    for timeout in (0, -1, True, float("inf"), "1"):
        with pytest.raises(ValueError):
            runtime.configure_startup_recovery(callback, timeout_seconds=timeout)
    runtime.configure_startup_recovery(callback, timeout_seconds=1)
    with pytest.raises(ValueError):
        runtime.configure_startup_recovery(callback, timeout_seconds=1)
    try:
        await runtime.start()
        assert runtime.started is True and calls == [runtime]
        with pytest.raises(ValueError):
            runtime.configure_startup_recovery(callback, timeout_seconds=1)
    finally:
        await runtime.close()
    with pytest.raises(ValueError):
        runtime.configure_startup_recovery(callback, timeout_seconds=1)

    late = CMMSRuntime(tmp_path / "late-registration", profile_id="alpha")
    try:
        await late.start()
        with pytest.raises(ValueError):
            late.configure_startup_recovery(callback, timeout_seconds=1)
    finally:
        await late.close()


def p5_assembly(h, *, builder=None, timeout=5):
    from memory_server.runtime import read_composition
    from memory_server.runtime.runtime import CMMSRuntime

    assert callable(getattr(CMMSRuntime, "configure_startup_recovery", None)) and callable(
        getattr(CMMSRuntime, "check_startup_custody", None)
    ), "S19P_SURFACE"
    assert callable(getattr(read_composition, "create_job_recovery_builder", None)), "S19P_SURFACE"
    from memory_server.application.profile_bundle import BundlePolicies
    from memory_server.runtime.daemon import Daemon
    from memory_server.runtime.read_composition import create_job_recovery_builder
    from memory_server.runtime.runtime_manager import RuntimeManager
    from tests.gateway.test_daemon import factory

    owner = create_job_recovery_builder(
        h.registry, None, bindings=h.bindings, factory_builder=builder or factory, timeout_seconds=timeout
    )
    manager = RuntimeManager(factory_builder=owner.factory_builder)
    daemon = Daemon(h.registry, policies=BundlePolicies.capture(h.settings), manager=manager)
    owner.bind_leases(daemon)
    return owner, daemon


async def p5_owner_startup(h, monkeypatch):
    import asyncio

    from memory_server.application.job_runner import JobCoordinator
    from memory_server.runtime.errors import RuntimeNotReadyError
    from memory_server.runtime.runtime import CMMSRuntime
    from tests.gateway.test_daemon import factory

    built, calls, spawned = [], [], []

    def builder(layout, **kwargs):
        result = factory(layout, **kwargs)
        built.append(result.runtime)
        return result

    owner, daemon = p5_assembly(h, builder=builder)
    original = JobCoordinator.reconcile_startup
    spawn = CMMSRuntime.spawn_owned_task

    def recorded_spawn(current, *args, **kwargs):
        spawned.append((current, current.state))
        return spawn(current, *args, **kwargs)

    async def reconcile(coordinator, *, profile_id, custody, deadline):
        calls.append((coordinator, custody))
        assert custody is built[-1] and custody.state == "STARTING", "S19P_STARTUP"
        binding = next(b for b in h.bindings if b.profile.id == profile_id)
        custody.check_startup_custody(profile_id=profile_id, canonical_root=binding.layout.data_root)
        observation = daemon.snapshot()[profile_id]
        assert observation.state == "not_started" and observation.retained and not observation.owned
        assert observation.admissions == 0 and not spawned
        assert custody.dependencies.outbox_worker is not None
        with pytest.raises(RuntimeNotReadyError):
            custody.admit()
        with pytest.raises(ValueError):
            owner.factory_builder(binding.layout, config=binding.config)
        assert len(built) == len(calls), "S19P_REPLACEMENT"
        with pytest.raises(ValueError):
            custody.configure_startup_recovery(owner._startup_recovery, timeout_seconds=5)
        validate = coordinator._startup_custody
        foreign = CMMSRuntime(binding.layout.data_root, profile_id=profile_id)
        with pytest.raises(RuntimeNotReadyError):
            validate(foreign, profile_id=profile_id)
        with pytest.raises(RuntimeNotReadyError):
            validate(custody, profile_id="missing")
        with pytest.raises(RuntimeNotReadyError):
            custody.check_startup_custody(profile_id=profile_id, canonical_root=h.store.root)

        async def wrong_task():
            with pytest.raises(RuntimeNotReadyError):
                validate(custody, profile_id=profile_id)

        await asyncio.create_task(wrong_task())
        result = await original(coordinator, profile_id=profile_id, custody=custody, deadline=deadline)
        assert custody.state == "STARTING" and not spawned
        return result

    try:
        with monkeypatch.context() as patch:
            patch.setattr(JobCoordinator, "reconcile_startup", reconcile)
            patch.setattr(CMMSRuntime, "spawn_owned_task", recorded_spawn)
            async with daemon.acquire(h.ctx) as session:
                assert session.runtime is built[0] and session.runtime.state == "READY"
                assert spawned and all(state == "READY" for _, state in spawned)
                assert await session.runtime.start() is session.runtime
                assert len(calls) == 1
            # One coordinator must resolve beta through the same closure, not capture alpha.
            spawned.clear()
            async with daemon.acquire(replace(h.ctx, principal=h.worker_b, profile_id="beta")) as beta:
                assert beta.runtime is built[1] and beta.runtime.state == "READY"
            assert len(calls) == 2 and calls[0][0] is calls[1][0]
        with pytest.raises(ValueError):
            owner.bind_leases(daemon)
        with pytest.raises(ValueError):
            owner.factory_builder(h.bindings[0].layout, config=h.bindings[0].config)
        await daemon.close_all()
        assert built[0].state == "CLOSED" and built[0].lock is None
        replacement = owner.factory_builder(h.bindings[0].layout, config=h.bindings[0].config)
        assert replacement.runtime is not built[0]
        await replacement.runtime.close()
    finally:
        await daemon.close_all()
        await owner.close(deadline=monotonic() + 5)


async def p5_owner_failure(h, monkeypatch):
    from memory_server.application.job_runner import JobCoordinator
    from memory_server.control.jobs import JobError
    from memory_server.runtime.daemon import LeaseError
    from memory_server.runtime.runtime import CMMSRuntime
    from memory_server.storage_lock import RuntimeStorageLock
    from tests.gateway.test_daemon import factory

    for mode in ("job", "timeout"):
        built, calls, spawns = [], [], []

        def builder(layout, **kwargs):
            result = factory(layout, **kwargs)
            built.append(result.runtime)
            return result

        owner, daemon = p5_assembly(h, builder=builder)

        async def failed(coordinator, *, profile_id, custody, deadline):
            coordinator._startup_custody(custody, profile_id=profile_id)
            calls.append(custody)
            assert custody.state == "STARTING" and not custody._admitted
            with pytest.raises(ValueError):
                owner.factory_builder(h.bindings[0].layout, config=h.bindings[0].config)
            with pytest.raises(ValueError):
                owner.bind_leases(daemon)
            if mode == "job":
                raise JobError("E_STORAGE")
            raise TimeoutError()

        def forbidden_spawn(*args, **kwargs):
            spawns.append(args)
            pytest.fail("S19P_STARTUP: worker spawned after failed recovery")

        try:
            with monkeypatch.context() as patch:
                patch.setattr(JobCoordinator, "reconcile_startup", failed)
                patch.setattr(CMMSRuntime, "spawn_owned_task", forbidden_spawn)
                with pytest.raises(LeaseError) as exc:
                    async with daemon.acquire(h.ctx):
                        pytest.fail("S19P_STARTUP: failed recovery admitted")
            assert exc.value.code == ("E_STORAGE" if mode == "job" else "E_TIMEOUT")
            assert calls == built and len(built) == 1 and not spawns
            runtime = built[0]
            assert runtime.state != "READY" and not runtime._admitted and runtime.lock is None
            assert daemon.snapshot()["alpha"].admissions == 0
            contender = await RuntimeStorageLock.acquire_exclusive_async(h.bindings[0].layout.data_root, timeout=0.2)
            contender.release_exclusive()
        finally:
            await daemon.close_all()
            await owner.close(deadline=monotonic() + 5)


def p5_rows(store, profile):
    """Ordered values of every column, preserving SQLite blobs and scalar types."""
    import pickle
    import sqlite3

    with sqlite3.connect(f"file:{store.root / 'control.sqlite'}?mode=ro", uri=True) as connection:
        rows = []
        for table, order in (("jobs", "id"), ("job_requests", "job_id,subject,key")):
            cursor = connection.execute(f"SELECT * FROM {table} WHERE profile_id=? ORDER BY {order}", (profile,))
            rows.append((tuple(column[0] for column in cursor.description), cursor.fetchall()))
    return pickle.dumps(rows, protocol=5)


async def p5_wait_file(path, child=None, log=None):
    import asyncio

    deadline = monotonic() + 20
    while not path.exists():
        if child is not None and child.poll() is not None:
            pytest.fail("S19P_PROCESS: child exited: " + log.read_text())
        assert monotonic() < deadline, "S19P_PROCESS: barrier timeout"
        await asyncio.sleep(0.01)


async def p5_child(root, profile):
    """Executed in a disposable OS process, not an in-process fake owner."""
    from unittest.mock import patch

    from memory_server.control.access_audit import AccessAuditStore
    from memory_server.control.jobs import JobMetadata
    from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
    from memory_server.control.store import ControlStore
    from memory_server.control.tokens import TokenService
    from memory_server.settings import Settings
    from tests.gateway.test_daemon import ctx, make_binding, principal

    settings = Settings(_env_file=None, outbox_poll_interval_seconds=0.01)
    bindings = tuple(make_binding(root / name, name, settings) for name in ("alpha", "beta"))
    with ControlStore(root / "control") as store:
        access = AccessAuditStore(root / "child-access")
        registry = ProfileRegistry(store, RegistryConfiguration(bindings, access, TokenService(store)))
        h = SimpleNamespace(
            registry=registry,
            bindings=bindings,
            settings=settings,
            metadata=JobMetadata(store),
            worker_a=principal("alpha"),
            worker_b=principal("beta"),
            ctx=ctx(profile),
            now=datetime.now(UTC),
        )
        owner, daemon = p5_assembly(h)
        try:
            async with daemon.acquire(h.ctx) as session:
                assert session.runtime.state == "READY" and session.runtime.lock is not None
                with patch(
                    "memory_server.control.jobs.secrets.token_hex",
                    return_value=("0" if profile == "beta" else "f") * 48,
                ):
                    claim = p2_reserve(h, profile=profile)
                claim = h.metadata.progress(claim, 0.25, now=datetime.now(UTC))
                (root / "child-ready").write_text(claim.job.id)
                await p5_wait_file(root / "resume")
                claim = h.metadata.progress(claim, 0.5, now=datetime.now(UTC))
                assert claim is not None
                (root / "child-progressed").write_text(claim.job.id)
                await p5_wait_file(root / "stop")
        finally:
            await daemon.close_all()
            await owner.close(deadline=monotonic() + 5)
            access.close()


def p5_launch(root, profile, log):
    import os
    import subprocess
    import sys

    code = (
        "import asyncio,sys; from pathlib import Path; "
        "from tests.gateway.test_s19p_prerequisites import p5_child; "
        "asyncio.run(p5_child(Path(sys.argv[1]), sys.argv[2]))"
    )
    return subprocess.Popen(
        [sys.executable, "-c", code, str(root), profile],
        env=dict(os.environ, PYTHONPATH="src", PYTHONDONTWRITEBYTECODE="1"),
        stdout=log,
        stderr=subprocess.STDOUT,
    )


async def test_s19p_scoped_recovery_two_processes(seam_owner, monkeypatch):
    from memory_server.runtime import read_composition
    from memory_server.runtime.runtime import CMMSRuntime

    assert callable(getattr(CMMSRuntime, "configure_startup_recovery", None)) and callable(
        getattr(CMMSRuntime, "check_startup_custody", None)
    ), "S19P_SURFACE"
    assert callable(getattr(read_composition, "create_job_recovery_builder", None)), "S19P_SURFACE"
    import asyncio

    from memory_server.control.jobs import JobError
    from memory_server.storage_lock import RuntimeStorageLock, StorageLockError

    h = seam_owner
    log_path = h.tmp_path / "child.log"
    with log_path.open("wb") as log:
        child = p5_launch(h.tmp_path, "beta", log)
        owner = daemon = None
        try:
            await p5_wait_file(h.tmp_path / "child-ready", child, log_path)
            assert child.pid != __import__("os").getpid()
            with pytest.raises((StorageLockError, TimeoutError)):
                contender = await RuntimeStorageLock.acquire_exclusive_async(
                    h.bindings[1].layout.data_root, timeout=0.2
                )
                contender.release_exclusive()
            before = p5_rows(h.store, "beta")
            with monkeypatch.context() as patch:
                patch.setattr("memory_server.control.jobs.secrets.token_hex", lambda n: "f" * 48)
                alpha = p2_reserve(h)
            assert (h.tmp_path / "child-ready").read_text() < alpha.job.id
            assert (
                h.metadata.scan_reconcile_scoped(profile_id="alpha", limit=1, deadline=monotonic() + 5)[0].claim
                == alpha
            )
            statements = []
            h.store._connection.set_trace_callback(statements.append)
            try:
                for profile in (None, False, "", "bad.profile"):
                    with pytest.raises(JobError, match="^E_VALIDATION$"):
                        h.metadata.scan_reconcile_scoped(profile_id=profile, deadline=monotonic() + 5)
                assert h.metadata.scan_reconcile_scoped(profile_id="alpha", deadline=monotonic() - 1) == ()
                assert statements == [], "S19P_SCOPE: rejected scans issued SQL"
                owner, daemon = p5_assembly(h)
                async with daemon.acquire(h.ctx) as session:
                    assert session.runtime.state == "READY" and session.runtime.lock is not None
                    assert h.metadata.get(alpha.job.id, "alpha", alpha.subject).status == "interrupted"
            finally:
                h.store._connection.set_trace_callback(None)
            assert any("FROM jobs WHERE profile_id='alpha' AND status='running'" in sql for sql in statements)
            assert p5_rows(h.store, "beta") == before, "S19P_SCOPE: B rows changed at barrier"
            (h.tmp_path / "resume").touch()
            await p5_wait_file(h.tmp_path / "child-progressed", child, log_path)
            assert p5_rows(h.store, "beta") != before, "S19P_PROCESS: B did not resume writes"
            (h.tmp_path / "stop").touch()
            assert await asyncio.to_thread(child.wait, timeout=10) == 0, log_path.read_text()
        finally:
            if child.poll() is None:
                child.kill()
                await asyncio.to_thread(child.wait, timeout=5)
            if daemon is not None:
                await daemon.close_all()
                await owner.close(deadline=monotonic() + 5)


async def test_s19p_restart_no_dispatch(seam_owner, monkeypatch):
    from memory_server.runtime import read_composition
    from memory_server.runtime.runtime import CMMSRuntime

    assert callable(getattr(CMMSRuntime, "configure_startup_recovery", None)) and callable(
        getattr(CMMSRuntime, "check_startup_custody", None)
    ), "S19P_SURFACE"
    assert callable(getattr(read_composition, "create_job_recovery_builder", None)), "S19P_SURFACE"
    import asyncio

    from memory_server.application.job_runner import JobCoordinator
    from memory_server.gateway_contracts import dto as d

    h = seam_owner
    queued = p2_reserve(h, profile="beta", running=False)
    interrupted = p2_reserve(h, profile="beta")
    h.metadata.fail(interrupted, "E_JOB_INTERRUPTED", now=h.now)
    succeeded = p2_reserve(h, profile="beta")
    h.metadata.complete(
        succeeded,
        d.JobResult(selected=1, attempted=1, completed=1, failed=0, projection_digest=None, receipt_ids=[]),
        now=h.now,
    )
    assert h.metadata.get(queued.job_id, "beta", h.worker_b.subject).status == "queued"
    before = p5_rows(h.store, "beta")
    log_path = h.tmp_path / "crash-child.log"
    with log_path.open("wb") as log:
        child = p5_launch(h.tmp_path, "alpha", log)
        try:
            await p5_wait_file(h.tmp_path / "child-ready", child, log_path)
            with h.store.borrow_transaction() as tx:
                assert tx.fetchone("SELECT status FROM jobs WHERE id=?", ("f" * 48,))["status"] == "running"
            child.kill()
            assert await asyncio.to_thread(child.wait, timeout=5) == -9
        finally:
            if child.poll() is None:
                child.kill()
                await asyncio.to_thread(child.wait, timeout=5)
    dispatches = []

    async def forbidden_dispatch(*args, **kwargs):
        dispatches.append(args)
        pytest.fail("S19P_DISPATCH: recovery dispatched a job")

    owner, daemon = p5_assembly(h)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(JobCoordinator, "attach", forbidden_dispatch)
            patch.setattr(JobCoordinator, "_child", forbidden_dispatch)
            async with daemon.acquire(h.ctx) as session:
                assert session.runtime.state == "READY" and session.runtime.lock is not None
                from tests.gateway.test_daemon import principal

                job = h.metadata.get("f" * 48, "alpha", principal("alpha").subject)
                assert job.status == "interrupted" and job.error_code == "E_JOB_INTERRUPTED"
                assert owner._coordinator.replay is None and owner._coordinator.rebuild is None
        assert dispatches == []
        assert p5_rows(h.store, "beta") == before, "S19P_SCOPE: restart changed B rows"
    finally:
        await daemon.close_all()
        await owner.close(deadline=monotonic() + 5)
