"""S18-C2 real daemon, runtime ownership and disk-backed H01 integration."""

import ast
import asyncio
import hashlib
import subprocess
import sys
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic
from types import SimpleNamespace
from uuid import uuid4

import pytest


@pytest.fixture
async def harness(tmp_path):
    from memory_server.application.job_runner import JobCoordinator
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

    settings = Settings(_env_file=None, outbox_poll_interval_seconds=0.01)
    bindings = tuple(make_binding(tmp_path / name, name, settings) for name in ("alpha", "beta"))
    store = ControlStore(tmp_path / "control")
    access = AccessAuditStore(tmp_path / "access")
    tokens = TokenService(store)
    registry = ProfileRegistry(store, RegistryConfiguration(bindings, access, tokens))
    created = tokens._bootstrap_create(
        d.TokenCreate(
            subject="worker",
            profiles=["alpha", "beta"],
            permissions=["index.manage"],
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        ),
        now=datetime.now(UTC),
    )
    principal = await tokens.authenticate(f"cmms_{created.token.id}.{created.secret}", now=datetime.now(UTC))
    ctx = RequestContext("req_" + str(uuid4()), principal, "alpha", uuid4(), monotonic() + 20)
    daemon = Daemon(
        registry, policies=BundlePolicies.capture(settings), manager=RuntimeManager(factory_builder=factory)
    )
    events = []

    async def event(value):
        events.append(value)

    class Runner:
        def __init__(self):
            self.started = asyncio.Event()
            self.release = threading.Event()
            self.calls = []
            self.value = d.JobResult(
                selected=1, attempted=1, completed=1, failed=0, projection_digest=None, receipt_ids=[]
            )

        async def run(self, session, job, request):
            self.calls.append((session, job, request, asyncio.current_task()))
            self.started.set()
            handle = await session.runtime.submit_native(lambda: (self.release.wait(), self.value)[1], kind="c2-job")
            return await handle.result()

    runner = Runner()
    metadata = JobMetadata(store)
    service = JobService(metadata, registry, tokens, access.begin, access.publish_committed, events=event)
    coordinator = JobCoordinator(
        metadata, daemon, authorize=service._authorize, replay=runner, rebuild=runner, events=event
    )
    service = replace(service, attach=coordinator.attach)
    h = SimpleNamespace(**locals())
    try:
        yield h
    finally:
        runner.release.set()
        await coordinator.close(deadline=monotonic() + 10)
        await daemon.close_all()
        access.close()
        store.close()


def payload():
    from memory_server.gateway_contracts import dto as d

    return d.MaintenanceRequest(
        reason="PRIVATE_C2_CANARY",
        confirmation=True,
        selection="pending",
        event_ids=[],
        selection_digest="d" * 64,
        expected_count=1,
    )


def reserve(h, ctx=None):
    from memory_server.control.jobs import request_digest
    from memory_server.gateway_contracts.ports import AuditOutcome, AuditTicket

    ctx = ctx or h.ctx
    r = h.metadata.enqueue(
        ctx,
        "outbox_replay",
        payload(),
        digest=request_digest("outbox_replay", payload()),
        ticket=AuditTicket("ticket", ctx.request_id),
        now=datetime.now(UTC),
        ttl_seconds=86400,
    )
    r = h.metadata.finalize_request(r, AuditOutcome(202, None, (), 0.0), now=datetime.now(UTC))
    h.metadata.mark_published(r, now=datetime.now(UTC))
    return r


async def settled(h):
    await h.coordinator.close(deadline=monotonic() + 5)


async def test_s18c2_durable_dispatch_and_claim(harness):
    from memory_server.control.jobs import JobError

    h = harness
    with pytest.raises(JobError, match="E_IDEMPOTENCY_REQUIRED"):
        await h.service.enqueue(replace(h.ctx, idempotency_key=None), "outbox_replay", payload())
    assert not h.runner.calls, "S18C2_DURABLE_CAS"
    accepted = await h.service.enqueue(h.ctx, "outbox_replay", payload())
    await asyncio.wait_for(h.runner.started.wait(), 3)
    for _ in range(10):
        assert await h.service.enqueue(h.ctx, "outbox_replay", payload()) == accepted, "S18C2_DURABLE_CAS"
    r = h.metadata.pending_outcomes(limit=100, deadline=monotonic() + 1)
    assert not r and len(h.runner.calls) == 1, "S18C2_DURABLE_CAS"
    job = await h.service.get(h.ctx, accepted.job_id)
    assert job.status == "running", "S18C2_DURABLE_CAS"
    # Competing attachments cross the same real runtime and H01 claim CAS.
    reservation = reserve(h, replace(h.ctx, idempotency_key=uuid4(), profile_id="beta"))
    beta = replace(h.ctx, profile_id="beta")
    await asyncio.gather(*(h.coordinator.attach(beta, reservation) for _ in range(10)))
    assert len(h.runner.calls) == 2, "S18C2_DURABLE_CAS"
    h.runner.release.set()
    await settled(h)
    assert (await h.service.get(h.ctx, accepted.job_id)).status == "succeeded", "S18C2_DURABLE_CAS"
    calls = len(h.runner.calls)
    late = replace(h.ctx, idempotency_key=uuid4())
    with pytest.raises(JobError, match="E_RUNTIME_CLOSING"):
        await h.service.enqueue(late, "outbox_replay", payload())
    with h.store.borrow_transaction() as tx:
        row = tx.fetchone("SELECT job_id FROM job_requests WHERE key=?", (str(late.idempotency_key),))
    assert h.metadata.get(row["job_id"], "alpha", late.principal.subject).status == "queued", "S18C2_DURABLE_CAS"
    assert len(h.runner.calls) == calls, "S18C2_DURABLE_CAS"


async def test_s18_cancel_keeps_owned_work(harness, monkeypatch):
    h = harness
    reservation = reserve(h)
    entered, allow = asyncio.Event(), asyncio.Event()
    authorize = h.coordinator.authorize

    async def paused(ctx):
        await authorize(ctx)
        if asyncio.current_task() is not request:
            entered.set()
            await allow.wait()

    monkeypatch.setattr(h.coordinator, "authorize", paused)
    request = asyncio.create_task(h.coordinator.attach(h.ctx, reservation))
    await asyncio.wait_for(entered.wait(), 3)
    runtime = h.daemon._entries["alpha"].runtime
    assert h.daemon.snapshot()["alpha"].admissions >= 1, "S18C2_CANCEL_CUSTODY"
    request.cancel()
    await asyncio.sleep(0)
    request.cancel()
    allow.set()
    with pytest.raises(asyncio.CancelledError):
        await request
    await asyncio.wait_for(h.runner.started.wait(), 3)
    session, job, _, child = h.runner.calls[0]
    assert child is not request and session.runtime is runtime, "S18C2_CANCEL_CUSTODY"
    assert child in h.daemon._held and h.daemon._held[child]["alpha"].token in runtime._admitted, "S18C2_CANCEL_CUSTODY"
    assert any(r.kind == "job" and r.task is child for r in runtime.owned_snapshot()[1]), "S18C2_CANCEL_CUSTODY"
    assert (await h.service.get(h.ctx, job.id)).status == "running", "S18C2_CANCEL_CUSTODY"
    h.runner.release.set()
    await settled(h)
    assert (await h.service.get(h.ctx, job.id)).status == "succeeded", "S18C2_CANCEL_CUSTODY"


async def retained(h):
    accepted = await h.service.enqueue(h.ctx, "outbox_replay", payload())
    await asyncio.wait_for(h.runner.started.wait(), 3)
    runtime = h.runner.calls[0][0].runtime
    await h.coordinator.close(deadline=monotonic() + 0.02)
    return accepted, runtime


async def test_s18_shutdown_retains_native_owner(harness):
    from memory_server.control.jobs import JobError

    h = harness
    accepted, runtime = await retained(h)
    try:
        await h.coordinator.attach(h.ctx, reserve(h, replace(h.ctx, idempotency_key=uuid4())))
    except JobError as exc:
        assert exc.code == "E_RUNTIME_CLOSING", "S18C2_NATIVE_RETAINED"
    else:
        assert False, "S18C2_NATIVE_RETAINED: shutdown admitted another job"
    report = await runtime.close()
    assert report.lock_retained and report.retained_resources, "S18C2_NATIVE_RETAINED"
    assert (await h.service.get(h.ctx, accepted.job_id)).status == "interrupted", "S18C2_NATIVE_RETAINED"
    contender_denied(h, "S18C2_NATIVE_RETAINED")


def contender_denied(h, marker):
    # Separate process: real flock exclusion, before any contender provider construction.
    code = "import fcntl,sys; f=open(sys.argv[1],'r+'); fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)"
    path = h.bindings[0].layout.data_root / ".cmms-storage.lock"
    result = subprocess.run([sys.executable, "-c", code, str(path)], capture_output=True, text=True)
    assert result.returncode != 0 and "BlockingIOError" in result.stderr, marker


async def test_s18_shutdown_retry_after_native_settle(harness):
    h = harness
    _, runtime = await retained(h)
    report = await runtime.close()
    assert report.lock_retained, "S18C2_SHUTDOWN"
    h.runner.release.set()
    await settled(h)
    report = await runtime.close()
    assert not report.lock_retained and not report.retained_resources, "S18C2_SHUTDOWN"
    path = h.bindings[0].layout.data_root / ".cmms-storage.lock"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import fcntl,sys; f=open(sys.argv[1],'r+'); fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)",
            str(path),
        ],
        capture_output=True,
    )
    assert result.returncode == 0, "S18C2_SHUTDOWN"


async def test_s18_restart_interrupted_reconciliation(harness):
    from memory_server.application.job_runner import JobCoordinator
    from memory_server.control.jobs import JobError, JobMetadata
    from memory_server.control.store import ControlStore

    h = harness
    r = reserve(h)
    h.metadata.claim(r.job_id, "alpha", h.ctx.principal.subject, 0, now=datetime.now(UTC))
    queued = reserve(h, replace(h.ctx, profile_id="beta", idempotency_key=uuid4()))
    # A distinct process owns the startup locks; no persisted bearer is supplied.
    script = """
import asyncio, sys
from pathlib import Path
from time import monotonic
from memory_server.application.job_runner import JobCoordinator
from memory_server.application.profile_bundle import BundlePolicies
from memory_server.control.jobs import JobMetadata
from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
from memory_server.control.store import ControlStore
from memory_server.runtime.daemon import Daemon
from memory_server.settings import Settings
from memory_server.storage_lock import RuntimeStorageLock
from tests.gateway.test_daemon import make_binding
async def main():
    settings = Settings(_env_file=None)
    root = Path(sys.argv[1])
    bindings = tuple(make_binding(root / name, name, settings) for name in ('alpha', 'beta'))
    locks = [RuntimeStorageLock.acquire_exclusive(b.layout.data_root) for b in bindings]
    try:
        with ControlStore(root / 'control') as store:
            registry = ProfileRegistry(store, RegistryConfiguration(bindings))
            daemon = Daemon(registry, policies=BundlePolicies.capture(settings))
            fresh = JobCoordinator(JobMetadata(store), daemon, authorize=None)
            print(await fresh.reconcile_startup(deadline=monotonic() + 3))
            await daemon.close_all()
    finally:
        for lock in locks:
            lock.release_exclusive()
asyncio.run(main())
"""
    process = subprocess.run(
        [sys.executable, "-c", script, str(h.tmp_path)], capture_output=True, text=True, timeout=10
    )
    assert process.returncode == 0, "S18C2_RECOVERY: " + process.stderr
    assert process.stdout.strip() == "1", "S18C2_RECOVERY"
    with ControlStore(h.store.root) as reopened:
        fresh = JobCoordinator(JobMetadata(reopened), h.daemon, authorize=h.service._authorize)
        assert await fresh.reconcile_startup(deadline=monotonic() + 3) == 0, "S18C2_RECOVERY"
        with pytest.raises(JobError, match="E_TIMEOUT"):
            await fresh.reconcile_startup(deadline=monotonic() - 1)
    job = await h.service.get(h.ctx, r.job_id)
    assert job.status == "interrupted" and job.error_code == "E_JOB_INTERRUPTED", "S18C2_RECOVERY"
    assert h.metadata.get(queued.job_id, "beta", h.ctx.principal.subject).status == "queued", "S18C2_RECOVERY"
    assert not h.runner.calls, "S18C2_RECOVERY"
    with h.store.borrow_transaction() as tx:
        assert tx.fetchone("SELECT phase FROM jobs WHERE id=?", (r.job_id,))["phase"] == "reconcile", "S18C2_RECOVERY"


async def test_s18_restart_does_not_rerun_native_job(harness):
    from memory_server.application.job_runner import JobCoordinator

    h = harness
    accepted, _ = await retained(h)
    h.runner.release.set()
    await settled(h)
    await h.daemon.close_all()
    fresh = JobCoordinator(h.metadata, h.daemon, authorize=h.service._authorize, replay=h.runner)
    assert await fresh.reconcile_startup(deadline=monotonic() + 2) == 0, "S18C2_NO_RERUN"
    assert len(h.runner.calls) == 1, "S18C2_NO_RERUN"
    assert (await h.service.get(h.ctx, accepted.job_id)).status == "interrupted", "S18C2_NO_RERUN"


async def test_s18_get_bounded_and_late_response(harness):
    from memory_server.control.jobs import JobError
    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.routes.jobs import JobReadAdapter

    h = harness
    accepted = await h.service.enqueue(h.ctx, "outbox_replay", payload())
    await asyncio.wait_for(h.runner.started.wait(), 3)
    claim = h.metadata.scan_reconcile(deadline=monotonic() + 2)[0].claim
    first = h.metadata.fail(claim, "E_JOB_INTERRUPTED", now=datetime.now(UTC))
    adapter = JobReadAdapter(h.service)
    command = d.COMMAND_TYPES["jobs.get"](
        operation="jobs.get", profile_id=None, path_ids={"id": accepted.job_id}, payload=d.Empty()
    )
    with pytest.raises(JobError, match="E_NOT_FOUND"):
        await adapter.execute(replace(h.ctx, profile_id="beta"), command)
    with pytest.raises(JobError, match="E_TIMEOUT"):
        await adapter.execute(replace(h.ctx, deadline=monotonic() - 1), command)
    assert await adapter.execute(h.ctx, command) == first, "S18C2_LATE_ISOLATION"
    h.runner.release.set()
    await settled(h)
    assert await adapter.execute(h.ctx, command) == first, "S18C2_LATE_ISOLATION"
    assert all(event.profile_id == "alpha" and event.job_id == accepted.job_id for event in h.events), (
        "S18C2_LATE_ISOLATION"
    )


async def test_s18c2_verified_result_and_event(harness):
    h = harness
    h.runner.value = object()  # A completed future is NOT a verified JobResult.
    accepted = await h.service.enqueue(h.ctx, "outbox_replay", payload())
    await asyncio.wait_for(h.runner.started.wait(), 3)
    h.runner.release.set()
    await settled(h)
    job = await h.service.get(h.ctx, accepted.job_id)
    assert job.status == "failed" and job.error_code == "E_INTERNAL", "S18C2_RESULT_EVENT"
    terminal = h.events[-1]
    assert terminal.data.to_state == job.status and terminal.profile_id == "alpha", "S18C2_RESULT_EVENT"
    assert "PRIVATE_C2_CANARY" not in terminal.model_dump_json(), "S18C2_RESULT_EVENT"


def test_s18c2_lifecycle_import_boundary():
    import memory_server.application.job_runner as runner

    source = Path(runner.__file__).read_text()
    tree = ast.parse(source)
    imports = [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
    assert not any(
        x in source.lower() for x in ("select ", "insert ", "update jobs", "control.store", "transports.http")
    ), "S18C2_LAYERS"
    assert not any(x.startswith(("memory_server.providers", "memory_server.runtime.daemon")) for x in imports), (
        "S18C2_LAYERS"
    )
    root = Path(__file__).resolve().parents[2]
    for path, expected in (
        ("src/memory_server/control/jobs.py", "bfb9d877bbc3225b8b293d2ec377573b4a1e2a7fc345d197ad2bbd2891be4853"),
        (
            "src/memory_server/application/job_service.py",
            "2f0b640fb8a868bd72c6593d94bf9e759659cfe6bdb8838c50d1139358353861",
        ),
        ("tests/gateway/test_jobs.py", "b0a8f1cb9d0c91ee167ac9bf0f966f360911935ee3dcde1b169304e93225126a"),
    ):
        assert hashlib.sha256((root / path).read_bytes()).hexdigest() == expected, "S18C2_LAYERS"
