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


async def test_s19p_p6_projection_result_forwarding(seam_owner):
    import asyncio
    import sys
    import threading
    from dataclasses import fields

    from memory_server.runtime.daemon import _ProjectionResult
    from memory_server.runtime.runtime import CMMSRuntime

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
        assert callable(getattr(empty, "drain", None)), "S19P_P6_DRAIN"
        assert await empty.drain(monotonic()) and empty.terminated, "S19P_P6_DRAIN"
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
            assert not await result.drain(monotonic() + 0.05) and not result.terminated, "S19P_P6_DRAIN"
            release.set()
            assert await result.drain(monotonic() + 5) and result.terminated, "S19P_P6_DRAIN"
    finally:
        release.set()


async def test_s19p_p6_result_drains_before_value(seam_owner, monkeypatch):
    import asyncio
    import sys
    import threading

    import memory_server.runtime.daemon as daemon_module
    from memory_server.runtime.runtime import CMMSRuntime

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
            except TimeoutError as exc:
                timed_out = str(exc) == "native drain deadline"
            assert timed_out, "S19P_P6_DRAIN"
            assert not result.terminated, "S19P_P6_DRAIN"
            release.set()
            assert await result.drain(monotonic() + 5), "S19P_P6_DRAIN"
            assert await result.result() == "result-value", "S19P_P6_DRAIN"
    finally:
        release.set()


async def test_s19p_p6_worker_blocks_until_native_dead(seam_owner, monkeypatch):
    import asyncio
    import sys
    import threading

    import memory_server.runtime.daemon as daemon_module
    from memory_server.runtime.runtime import CMMSRuntime

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
            except TimeoutError as exc:
                timed_out = str(exc) == "native drain deadline"
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
            with pytest.raises(TimeoutError, match="^native drain deadline$"):
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
