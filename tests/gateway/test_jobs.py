"""S18-C1: real disk-backed metadata and direct-port contracts."""

from __future__ import annotations

import ast
import asyncio
import importlib
import math
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from time import monotonic
from uuid import uuid4

import pytest

from memory_server.control.store import ControlStore
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import AuditOutcome, AuditTicket, Principal, ProfileRecord, RequestContext

NOW = datetime(2026, 1, 1, tzinfo=UTC)
OUTCOME = AuditOutcome(202, None, (), 0.0)


def modules():
    return importlib.import_module("memory_server.control.jobs"), importlib.import_module(
        "memory_server.application.job_service"
    )


def context(**changes):
    principal = Principal(
        "creator", "subject", None, frozenset({"p"}), frozenset({"index.manage"}), 1, NOW + timedelta(days=1)
    )
    return replace(RequestContext("request", principal, "p", uuid4(), monotonic() + 60), **changes)


def payload():
    return d.MaintenanceRequest(
        reason="PRIVATE_CANARY",
        confirmation=True,
        selection="pending",
        event_ids=[],
        selection_digest="d" * 64,
        expected_count=1,
    )


@pytest.fixture
def owner(tmp_path):
    with ControlStore(tmp_path / "control") as store:
        store._configure((ProfileRecord("p", tmp_path / "profile", True, "p", None, {}, 1),))
        yield store


def enqueue(owner, ctx=None):
    m, _ = modules()
    ctx = context() if ctx is None else ctx
    return m.JobMetadata(owner).enqueue(
        ctx,
        "outbox_replay",
        payload(),
        digest=m.request_digest("outbox_replay", payload()),
        ticket=AuditTicket("ticket", ctx.request_id),
        now=NOW,
        ttl_seconds=86400,
    )


def final(owner, reservation):
    m, _ = modules()
    metadata = m.JobMetadata(owner)
    result = metadata.finalize_request(reservation, OUTCOME, now=NOW)
    metadata.mark_published(result, now=NOW)
    return result


def assert_code(code):
    m, _ = modules()
    return pytest.raises(m.JobError, match="^" + code + "$")


def result():
    return d.JobResult(selected=1, attempted=1, completed=1, failed=0, projection_digest=None, receipt_ids=[])


def test_s18c1_surface_required():
    m, s = modules()  # Missing modules must fail inside the call, never collection.
    assert all(
        callable(getattr(m.JobMetadata, name, None))
        for name in (
            "enqueue",
            "get",
            "claim",
            "finish",
            "complete",
            "fail",
            "progress",
            "scan_reconcile",
            "cleanup_expired",
            "finalize_request",
            "pending_outcomes",
            "mark_published",
        )
    ), "S18C1_SURFACE"
    assert callable(s.JobService.enqueue) and callable(s.JobService.get), "S18C1_SURFACE"


def test_s18_duplicate_ten_and_concurrent_claim(owner):
    m, _ = modules()
    ctx = context()
    r = enqueue(owner, ctx)
    with assert_code("E_REQUEST_IN_PROGRESS"):
        enqueue(owner, ctx)
    with assert_code("E_IDEMPOTENCY_CONFLICT"):
        m.JobMetadata(owner).enqueue(
            ctx,
            "outbox_replay",
            payload(),
            digest="f" * 64,
            ticket=AuditTicket("new", "new"),
            now=NOW,
            ttl_seconds=86400,
        )
    r = final(owner, r)
    for _ in range(10):
        replay = enqueue(owner, ctx)
        assert replay.accepted == r.accepted and replay.replayed, "S18C1_DEDUP_CAS"
    barrier = Barrier(2)

    def contender():
        with ControlStore(owner.root) as other:
            barrier.wait()
            return m.JobMetadata(other).claim(r.job_id, "p", "subject", 0, now=NOW)

    with ThreadPoolExecutor(2) as pool:
        claims = list(pool.map(lambda _: contender(), range(2)))
    assert sum(c is not None for c in claims) == 1, "S18C1_DEDUP_CAS"
    with owner.borrow_transaction() as tx:
        assert tx.fetchone("SELECT count(*) AS n FROM jobs WHERE slot_held=1")["n"] == 1, "S18C1_DEDUP_CAS"


def test_s18_late_completion(owner):
    m, _ = modules()
    md = m.JobMetadata(owner)
    r = enqueue(owner)
    c = md.claim(r.job_id, "p", "subject", 0, now=NOW)
    first = md.fail(c, "E_JOB_INTERRUPTED", now=NOW)
    with owner.borrow_transaction() as tx:
        before = dict(tx.fetchone("SELECT * FROM jobs WHERE id=?", (r.job_id,)))
    assert md.complete(c, result(), now=NOW + timedelta(seconds=1)) == first, "S18C1_LATE"
    with owner.borrow_transaction() as tx:
        assert dict(tx.fetchone("SELECT * FROM jobs WHERE id=?", (r.job_id,))) == before, "S18C1_LATE"


def test_s18_ttl_inflight(owner):
    m, _ = modules()
    md = m.JobMetadata(owner)
    r = enqueue(owner)
    late = NOW + timedelta(days=2)
    assert md.cleanup_expired(now=late, limit=10, deadline=monotonic() + 5) == 0, "S18C1_TTL"
    r = md.finalize_request(r, OUTCOME, now=NOW)
    c = md.claim(r.job_id, "p", "subject", 0, now=NOW)
    md.complete(c, result(), now=NOW)
    assert md.cleanup_expired(now=late, limit=10, deadline=monotonic() + 5) == 0, "S18C1_TTL"
    md.mark_published(r, now=NOW)
    assert md.cleanup_expired(now=late, limit=10, deadline=monotonic() + 5) == 1, "S18C1_TTL"
    assert md.get(r.job_id, "p", "subject").status == "succeeded", "S18C1_TTL"
    with owner.borrow_transaction() as tx:
        assert (
            tx.fetchone("SELECT validated_request FROM jobs WHERE id=?", (r.job_id,))["validated_request"] == '{"v":1}'
        ), "S18C1_TTL"


def test_s18_queue_and_profile_budget(owner):
    m, _ = modules()
    md = m.JobMetadata(owner)
    reservations = [enqueue(owner) for _ in range(128)]
    with assert_code("E_RATE_LIMIT"):
        enqueue(owner)
    assert md.claim(reservations[0].job_id, "p", "subject", 0, now=NOW), "S18C1_BOUNDED"
    assert md.claim(reservations[1].job_id, "p", "subject", 0, now=NOW) is None, "S18C1_BOUNDED"


def test_s18_progress_monotonic_or_null(owner):
    m, _ = modules()
    md = m.JobMetadata(owner)
    r = enqueue(owner)
    c = md.claim(r.job_id, "p", "subject", 0, now=NOW)
    assert c.job.progress is None, "S18C1_PROGRESS"
    c = md.progress(c, 0.5, now=NOW)
    for value in (0.4, None, math.nan, math.inf, -0.1, 1.1):
        with assert_code("E_VALIDATION"):
            md.progress(c, value, now=NOW)
    assert md.get(r.job_id, "p", "subject").progress == 0.5, "S18C1_PROGRESS"
    md.complete(c, result(), now=NOW)
    assert md.progress(c, 0.9, now=NOW) is None, "S18C1_PROGRESS"


def test_s18c1_scan_and_reconcile_cas(owner):
    m, _ = modules()
    r = enqueue(owner)
    md = m.JobMetadata(owner)
    c = md.claim(r.job_id, "p", "subject", 0, now=NOW)
    with ControlStore(owner.root) as reopened:
        recovered = m.JobMetadata(reopened).scan_reconcile(limit=1, deadline=monotonic() + 5)
    assert len(recovered) == 1 and recovered[0].claim == c, "S18C1_RECOVERY"
    assert md.scan_reconcile(limit=1, deadline=monotonic() - 1) == (), "S18C1_RECOVERY"
    updated = md.finish(
        r.job_id,
        "p",
        "subject",
        c.revision,
        "running",
        m.TerminalUpdate("interrupted", None, "E_JOB_INTERRUPTED", NOW, "reconcile"),
    )
    assert updated.status == "interrupted", "S18C1_RECOVERY"
    assert md.complete(c, result(), now=NOW) == updated, "S18C1_RECOVERY"


def test_s18c1_get_bounded(owner):
    m, _ = modules()
    r = enqueue(owner)
    md = m.JobMetadata(owner)
    assert md.get(r.job_id, "p", "foreign") is None, "S18C1_READ"
    assert md.get(r.job_id, "foreign", "subject") is None, "S18C1_READ"
    assert md.get(r.job_id, "p", "subject").id == r.job_id, "S18C1_READ"
    source = Path(m.__file__).read_text()
    assert "WHERE id=? AND profile_id=? AND subject=? LIMIT 1" in source, "S18C1_READ"


def test_s18_job_dto_and_error_mapping(owner):
    m, _ = modules()
    r = enqueue(owner)
    job = m.JobMetadata(owner).get(r.job_id, "p", "subject")
    assert set(job.model_dump()) == set(d.Job.model_fields), "S18C1_CONTRACT"
    assert job.result is job.error_code is job.progress is job.started_at is job.finished_at is None, "S18C1_CONTRACT"
    assert len(job.id) == 48 and all(c in "0123456789abcdef" for c in job.id), "S18C1_CONTRACT"
    assert str(m.JobError("E_STORAGE")) == "E_STORAGE", "S18C1_CONTRACT"


def test_s18_wire_limits_and_privacy(owner):
    m, _ = modules()
    r = enqueue(owner)
    assert "PRIVATE_CANARY" not in m.canonical_json(m.JobMetadata(owner).get(r.job_id, "p", "subject")), "S18C1_PRIVACY"
    with assert_code("E_REQUEST_TOO_LARGE"):
        m.canonical_json("я" * 65536)
    with owner.borrow_transaction() as tx:
        tx.execute("UPDATE jobs SET dto_snapshot=? WHERE id=?", ("{}", r.job_id))
    with assert_code("E_STORAGE"):
        m.JobMetadata(owner).get(r.job_id, "p", "subject")


def service(owner, **kwargs):
    m, s = modules()
    return s.JobService(m.JobMetadata(owner), **kwargs)


def test_s18_job_durable_before_dispatch(owner):
    # Callback doubles are outside the real commit/readback boundary.
    calls = []

    async def attach(ctx, reservation):
        with sqlite3.connect(owner.root / "control.sqlite") as conn:
            calls.append(conn.execute("SELECT id FROM jobs WHERE id=?", (reservation.job_id,)).fetchone()[0])

    svc, ctx = assembled(owner, attach=attach)
    accepted = asyncio.run(svc.enqueue(ctx, "outbox_replay", payload()))
    assert calls == [accepted.job_id], "S18C1_DURABLE"


def assembled(owner, *, attach=None, publish=None, events=None):
    from test_registry import binding

    from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
    from memory_server.control.tokens import TokenService

    root = owner.root.parent / "profile"
    root.mkdir(exist_ok=True)
    registry = ProfileRegistry(owner, RegistryConfiguration((binding(root, "p"),)))
    tokens = TokenService(owner)
    issuer = replace(context().principal, permissions=frozenset({"token.manage", "index.manage"}))
    created = asyncio.run(
        tokens.create(
            context(principal=issuer),
            d.TokenCreate(
                subject="subject", profiles=["p"], permissions=["index.manage"], expires_at=NOW + timedelta(hours=1)
            ),
            now=NOW,
        )
    )
    principal = asyncio.run(tokens.authenticate(f"cmms_{created.token.id}.{created.secret}", now=NOW))

    async def intent(ctx, operation):
        return AuditTicket("ticket", ctx.request_id)

    async def published(ticket, outcome):
        return None

    async def attached(ctx, reservation):
        return None

    return service(
        owner,
        registry=registry,
        tokens=tokens,
        intent=intent,
        publish=publish or published,
        attach=attach or attached,
        events=events,
        clock=lambda: NOW,
    ), context(principal=principal)


def test_s18_pre_postcommit_failure(owner):
    m, _ = modules()
    r = enqueue(owner)
    md = m.JobMetadata(owner)
    assert len(md.pending_outcomes(limit=10, deadline=monotonic() + 5)) == 1, "S18C1_COMMIT"
    r = md.finalize_request(r, OUTCOME, now=NOW)
    assert md.pending_outcomes(limit=10, deadline=monotonic() + 5)[0].outcome_snapshot == OUTCOME, "S18C1_COMMIT"
    md.mark_published(r, now=NOW)
    assert md.pending_outcomes(limit=10, deadline=monotonic() + 5) == (), "S18C1_COMMIT"


def test_s18_get_scope_and_principal(owner):
    svc, ctx = assembled(owner)
    r = enqueue(owner, ctx)
    assert asyncio.run(svc.get(ctx, r.job_id)).token_id == ctx.principal.token_id, "S18C1_SCOPE"
    with assert_code("E_PERMISSION_DENIED"):
        asyncio.run(svc.get(replace(ctx, profile_id=None), r.job_id))
    with assert_code("E_VALIDATION"):
        asyncio.run(svc.get(ctx, r.job_id.upper()))


def test_s18_credentials_permissions(owner):
    svc, ctx = assembled(owner)
    with assert_code("E_PERMISSION_DENIED"):
        asyncio.run(svc.get(replace(ctx, principal=replace(ctx.principal, permissions=frozenset())), "a" * 48))
    with assert_code("E_AUTH_REQUIRED"):
        asyncio.run(svc.get(replace(ctx, principal=replace(ctx.principal, credential_generation=0)), "a" * 48))
    assert True, "S18C1_AUTH"


def test_s18_job_updated_after_commit(owner):
    seen = []

    async def events(event):
        with sqlite3.connect(owner.root / "control.sqlite") as conn:
            assert conn.execute("SELECT id FROM jobs WHERE id=?", (event.job_id,)).fetchone(), "S18C1_EVENT_COMMIT"
        assert "PRIVATE_CANARY" not in event.model_dump_json(), "S18C1_EVENT_COMMIT"
        seen.append(event)

    svc, ctx = assembled(owner, events=events)
    asyncio.run(svc.enqueue(ctx, "outbox_replay", payload()))
    assert len(seen) == 1, "S18C1_EVENT_COMMIT"


def test_s18_job_layer_import_boundaries():
    m, s = modules()
    for module in (m, s):
        tree = ast.parse(Path(module.__file__).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            assert not any(
                any(part in name for part in ("starlette", "fastapi", "transports", "http.server")) for name in names
            ), "S18C1_LAYERS"
    assert not any(
        isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in ("eval", "exec", "__import__")
        for n in ast.walk(tree)
    ), "S18C1_LAYERS"
