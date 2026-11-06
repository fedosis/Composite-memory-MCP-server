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
