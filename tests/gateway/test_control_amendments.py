"""S06 amendments: real control storage boundaries, no HTTP shell."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import time
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
from memory_server.control.store import ControlError, ControlStore
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import AuditTicket, ProfileRecord, RequestContext
from tests.gateway.test_registry import binding, principal


@pytest.fixture
def configured(tmp_path):
    bindings = tuple(binding(tmp_path / name, name, enabled=name != "beta") for name in ("alpha", "beta", "gamma"))
    with ControlStore(tmp_path / "control") as store:
        registry = ProfileRegistry(store, RegistryConfiguration(bindings))
        yield registry, store, bindings


async def test_q3_page_scoped_keyset_and_disabled_record(configured):
    registry, store, bindings = configured
    actor = principal("alpha", "beta")
    statements = []
    store._connection.set_trace_callback(statements.append)
    page, upper = await registry.page(actor, limit=1, after=None, upper=None)
    assert [item.id for item in page] == ["alpha"]
    assert upper == "beta"
    assert page[0].storage_root is None
    other, same_upper = await registry.page(actor, limit=1, after="alpha", upper=upper)
    assert [item.id for item in other] == ["beta"] and same_upper == upper
    assert not other[0].enabled and other[0].storage_root is None
    record = await registry.record(actor, "beta", permission="profile.manage")
    assert not record.enabled
    with pytest.raises(ControlError, match="Profile disabled"):
        await registry.resolve(actor, "beta")
    select = [s for s in statements if s.startswith("SELECT")]
    assert any("LIMIT 2" in s and "ORDER BY id" in s and " IN (" in s for s in select)
    assert statements[0] == "BEGIN"
    assert statements.index("COMMIT") > next(i for i, s in enumerate(statements) if "LIMIT 2" in s)
    assert not any(b.layout.data_root.exists() for b in bindings)


async def test_q3_scope_before_lookup_and_position_validation(configured, monkeypatch):
    registry, store, _ = configured
    actor = principal("alpha")

    def forbidden(*args, **kwargs):
        pytest.fail("lookup before scope/validation")

    monkeypatch.setattr(store, "_get", forbidden)
    for target in ("beta", "absent"):
        with pytest.raises(ControlError) as exc:
            await registry.record(actor, target, permission="profile.read")
        assert exc.value.code == "E_PERMISSION_DENIED"
    with pytest.raises(ControlError) as exc:
        await registry.record(actor, "alpha", permission="token.manage")
    assert exc.value.code == "E_VALIDATION"
    for options in (
        dict(limit=0, after=None, upper=None),
        dict(limit=True, after=None, upper=None),
        dict(limit=201, after=None, upper=None),
        dict(limit=1, after="../alpha", upper="beta"),
        dict(limit=1, after="alpha", upper=None),
        dict(limit=1, after="beta", upper="alpha"),
    ):
        with pytest.raises(ControlError) as exc:
            await registry.page(actor, **options)
        assert exc.value.code == "E_VALIDATION"
    with pytest.raises(ControlError) as exc:
        await registry.page(replace(actor, permissions=frozenset()), limit=1, after=None, upper=None)
    assert exc.value.code == "E_PERMISSION_DENIED"
    empty, upper = await registry.page(principal(), limit=1, after=None, upper=None)
    assert empty == () and upper is None


async def test_q3_read_only_and_no_filesystem_after_configuration(configured, monkeypatch):
    from pathlib import Path

    registry, store, _ = configured

    def forbidden(*args, **kwargs):
        pytest.fail("read path opened filesystem")

    before = store._connection.total_changes
    with monkeypatch.context() as patch:
        patch.setattr(Path, "stat", forbidden)
        patch.setattr(sqlite3, "connect", forbidden)
        await registry.page(principal("alpha"), limit=200, after=None, upper=None)
        await registry.record(principal("beta"), "beta", permission="profile.read")
    assert store._connection.total_changes == before


class ObservedLeases:
    """Injected lease boundary; storage and registry orchestration stay real."""

    def __init__(self):
        self.observations = {}
        self.calls = 0
        self.entered = asyncio.Event()
        self.release = None
        self.report = None

    def snapshot(self):
        return dict(self.observations)

    async def stop_profile(self, profile_id):
        self.calls += 1
        self.entered.set()
        if self.release is not None:
            await self.release.wait()
        if self.report is not None and not self.report.lock_retained and not self.report.unsettled:
            self.observations.pop(profile_id, None)
        return self.report


def control_input(profile="alpha", *, key=None, generation=1, enable=False, reason="test control"):
    who = replace(principal("alpha", "beta", "gamma"), token_id=uuid4().hex)
    ctx = RequestContext("req_" + str(uuid4()), who, profile, key or uuid4(), time.monotonic() + 10)
    cls = d.ProfilesEnableCommand if enable else d.ProfilesDisableCommand
    command = cls(
        operation="profiles.enable" if enable else "profiles.disable",
        profile_id=profile,
        path_ids={},
        payload=d.ControlMutation(
            reason=reason,
            confirmation=True,
            expected_generation=generation,
        ),
    )
    return ctx, command, AuditTicket(str(uuid4()), ctx.request_id)


async def publish_ok(ticket, outcome):
    return True


async def run_control(registry, args, leases, *, publish=publish_ok, now=None):
    ctx, command, ticket = args
    return await registry.control(
        ctx,
        command,
        ticket=ticket,
        leases=leases,
        publish=publish,
        now=now or datetime.now(timezone.utc),
        ttl_seconds=60,
    )


async def test_q4_atomic_replay_scope_digest_and_real_audit(configured, tmp_path):
    from memory_server.control.access_audit import AccessAuditStore

    registry, store, _ = configured
    leases = ObservedLeases()
    args = control_input()
    audit = AccessAuditStore(tmp_path / "audit")
    statements = []
    audit._db.set_trace_callback(statements.append)
    try:
        ticket = await audit.begin(args[0], args[1].operation)
        args = (args[0], args[1], ticket)
        first = await run_control(registry, args, leases, publish=audit.publish_committed)
        assert first.status == 200 and not first.replayed and not first.body.enabled
        assert first.body.storage_root is None and first.body.generation == 2
        retry = replace(args[0], request_id="req_" + str(uuid4()))
        replay = await run_control(registry, (retry, args[1], AuditTicket("unused", retry.request_id)), leases)
        assert replay.replayed and replay.body == first.body
        assert replay.original_request_id == args[0].request_id and leases.calls == 1
        changed = control_input(key=args[0].idempotency_key, reason="changed")
        with pytest.raises(ControlError) as exc:
            await run_control(registry, changed, leases)
        assert exc.value.code == "E_IDEMPOTENCY_CONFLICT"
        denied = replace(retry, principal=replace(retry.principal, profiles=frozenset()))
        with pytest.raises(ControlError) as exc:
            await run_control(registry, (denied, args[1], AuditTicket("x", denied.request_id)), leases)
        assert exc.value.code == "E_PERMISSION_DENIED"
        # Scope key includes profile, so the same key is independent on beta.
        await run_control(registry, control_input("beta", key=args[0].idempotency_key, enable=True), leases)
        assert len(store.journal(profile_ids=["alpha", "beta"])) == 2
        rows = audit.list(replace(args[0].principal, permissions=frozenset({"audit.read"})), "alpha")
        assert len(rows) == 1
        assert audit._db.execute("SELECT COUNT(*) FROM access_audit").fetchone()[0] == 1
        assert sum(query.startswith("INSERT INTO access_audit") for query in statements) == 1
        assert sum(query.startswith("UPDATE access_audit") for query in statements) == 1
        assert store.control_requests(limit=1) == ()
    finally:
        audit.close()


async def test_q4_ten_replays_and_concurrent_cas(configured):
    registry, store, bindings = configured
    leases = ObservedLeases()
    args = control_input()
    first = await run_control(registry, args, leases)
    replies = await asyncio.gather(*(run_control(registry, args, leases) for _ in range(10)))
    assert all(reply.replayed and reply.body == first.body for reply in replies)
    assert len(store.journal(profile_ids=["alpha"])) == 1 and leases.calls == 1
    with ControlStore(store.root) as second:
        other = ProfileRegistry(second, RegistryConfiguration(bindings))
        results = await asyncio.gather(
            run_control(registry, control_input("beta", enable=True), leases),
            run_control(other, control_input("beta", enable=True), leases),
            return_exceptions=True,
        )
    assert sum(not isinstance(result, Exception) for result in results) == 1
    assert [result.code for result in results if isinstance(result, ControlError)] == ["E_VERSION_CONFLICT"]
    assert len(store.journal(profile_ids=["beta"])) == 1


async def test_q4_live_two_connections_pending_and_bounded_recovery(configured):
    registry, store, bindings = configured
    leases = ObservedLeases()
    leases.release = asyncio.Event()
    args = control_input()
    assert callable(getattr(registry, "control", None))
    task = asyncio.create_task(run_control(registry, args, leases))
    await leases.entered.wait()
    try:
        rows = store.control_requests(limit=1)
        assert rows[0]["phase"] == "committed" and rows[0]["pending_publication"] == 1
        assert json.loads(rows[0]["response_snapshot"])["storage_root"] is None
        with ControlStore(store.root) as second:
            other = ProfileRegistry(second, RegistryConfiguration(bindings))
            with pytest.raises(ControlError) as exc:
                await run_control(other, args, leases)
            assert exc.value.code == "E_REQUEST_IN_PROGRESS"
            changed = control_input(key=args[0].idempotency_key, reason="different")
            with pytest.raises(ControlError) as exc:
                await run_control(other, changed, leases)
            assert exc.value.code == "E_IDEMPOTENCY_CONFLICT"
            before_calls = leases.calls
            assert not await other.reconcile_control(leases=leases, publish=publish_ok, limit=1)
            assert leases.calls == before_calls
            assert (await run_control(other, control_input("beta", enable=True), leases)).status == 200
            # Probe/read paths are not gated by pending control work.
            assert (await other.page(principal("alpha"), limit=1, after=None, upper=None))[0][0].id == "alpha"
    finally:
        leases.release.set()
    assert (await task).status == 200
    assert len(store.journal(profile_ids=["alpha"])) == 1


async def test_q4_publish_failure_preserves_snapshot_original_ticket_and_reopen(configured):
    registry, store, bindings = configured
    leases = ObservedLeases()
    args = control_input()

    async def unavailable(ticket, outcome):
        return False

    first = await run_control(registry, args, leases, publish=unavailable)
    assert first.status == 200 and len(store.control_requests(limit=1)) == 1
    # Legacy local changes cannot rewrite the original response on recovery.
    await registry.set_metadata(
        args[0].principal, "alpha", display_name="later", description=None, expected_generation=2
    )
    root = store.root
    store.close()
    seen = []

    async def published(ticket, outcome):
        seen.append((ticket, outcome))
        return True

    with ControlStore(root) as reopened:
        again = ProfileRegistry(reopened, RegistryConfiguration(bindings))
        sql = []
        reopened._connection.set_trace_callback(sql.append)
        assert await again.reconcile_control(leases=leases, publish=published, limit=1)
        assert any("LIMIT 1" in query for query in sql)
        assert seen[0][0] == args[2]
        replay = await run_control(again, args, leases)
        assert replay.body == first.body and replay.replayed and leases.calls == 1
        assert len(reopened.journal(profile_ids=["alpha"])) == 2  # one control + one local metadata effect


@pytest.mark.parametrize("cancel", [False, True])
async def test_q4_timeout_cancel_retains_committed_state_then_reconciles(configured, cancel):
    from memory_server.runtime.runtime import ShutdownReport

    registry, store, bindings = configured
    leases = ObservedLeases()
    leases.observations["alpha"] = SimpleNamespace(
        state="ready",
        owned=True,
        admissions=1,
        stopping=False,
        retained=False,
    )
    leases.release = asyncio.Event()
    args = control_input()
    args = (replace(args[0], deadline=time.monotonic() + (10 if cancel else 0.02)), args[1], args[2])
    assert callable(getattr(registry, "control", None))
    task = asyncio.create_task(run_control(registry, args, leases))
    await leases.entered.wait()
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(ControlError) as exc:
            await task
        assert exc.value.code == "E_PROFILE_BUSY"
    assert not store._get("alpha").enabled and store._get("alpha").generation == 2
    assert store.control_requests(limit=1)[0]["phase"] == "committed"
    with pytest.raises(ControlError) as exc:
        await run_control(registry, control_input(key=args[0].idempotency_key), leases)
    assert exc.value.code == "E_REQUEST_IN_PROGRESS"
    # None from stop is NOT proof of safe drain when an entry is still owned.
    leases.release.set()
    assert not await registry.reconcile_control(leases=leases, publish=publish_ok, limit=1)
    leases.report = ShutdownReport()
    assert await registry.reconcile_control(leases=leases, publish=publish_ok, limit=1)
    retry = replace(args[0], deadline=time.monotonic() + 10)
    assert (await run_control(registry, (retry, args[1], args[2]), leases)).replayed


async def test_q4_enable_does_not_bypass_retained_owner(configured):
    registry, store, _ = configured
    leases = ObservedLeases()
    leases.observations["beta"] = SimpleNamespace(
        state="closing",
        owned=True,
        admissions=0,
        stopping=True,
        retained=True,
    )
    with pytest.raises(ControlError) as exc:
        await run_control(registry, control_input("beta", enable=True), leases)
    assert exc.value.code == "E_PROFILE_BUSY"
    assert store._get("beta").generation == 1 and not store._get("beta").enabled
    assert store.journal(profile_ids=["beta"]) == ()


@pytest.mark.parametrize("phase", ["after_control_cas", "after_control_journal", "after_control_commit"])
async def test_q4_crash_checkpoints_reopen(configured, phase):
    registry, store, bindings = configured

    def crash(at):
        if at == phase:
            raise RuntimeError("synthetic crash boundary")

    store._checkpoint = crash
    with pytest.raises(RuntimeError):
        await run_control(registry, control_input(), ObservedLeases())
    root = store.root
    store.close()
    with ControlStore(root) as reopened:
        again = ProfileRegistry(reopened, RegistryConfiguration(bindings))
        committed = phase == "after_control_commit"
        assert reopened._get("alpha").generation == (2 if committed else 1)
        assert len(reopened.journal(profile_ids=["alpha"])) == int(committed)
        assert await again.reconcile_control(leases=ObservedLeases(), publish=publish_ok, limit=1)


def test_q4_existing_deafea8_store_migration_rehearsal(tmp_path):
    # Execute the real base ControlStore, not a hand-maintained schema surrogate.
    from pathlib import Path

    repository = Path(__file__).resolve().parents[2]
    source = subprocess.check_output(
        ["git", "show", "deafea8:src/memory_server/control/store.py"],
        cwd=repository,
        text=True,
    )
    namespace = {"__name__": "legacy_control_store"}
    exec(compile(source, "deafea8/control/store.py", "exec"), namespace)
    root = tmp_path / "legacy-control"
    record = ProfileRecord("alpha", tmp_path / "alpha", True, "original", None, {}, 1)
    with namespace["ControlStore"](root) as legacy:
        legacy._configure((record,))
        legacy._mutate("alpha", expected_generation=1, operation="enabled", enabled=False)
        old_profiles = [tuple(row) for row in legacy._connection.execute("SELECT * FROM profiles")]
        old_journal = [tuple(row) for row in legacy._connection.execute("SELECT * FROM control_journal")]
    with ControlStore(root) as migrated:
        columns = {row[1] for row in migrated._connection.execute("PRAGMA table_info(control_journal)")}
        assert {"phase", "response_snapshot", "pending_publication", "idem_key"} <= columns
        assert [tuple(row) for row in migrated._connection.execute("SELECT * FROM profiles")] == old_profiles
        assert [tuple(row)[:7] for row in migrated._connection.execute("SELECT * FROM control_journal")] == old_journal
        assert migrated.control_requests(limit=1) == ()
        assert migrated._get("alpha").generation == 2


def test_q5_availability_is_bounded_read_only_and_fail_closed(configured, monkeypatch):
    from memory_server.control.tokens import TokenService

    registry, store, _ = configured
    tokens = TokenService(store)
    now = datetime.now(timezone.utc)
    from datetime import timedelta

    tokens._bootstrap_create(
        d.TokenCreate(
            subject="readiness", profiles=["alpha"], permissions=["profile.read"],
            expires_at=now + timedelta(hours=1),
        ), now=now,
    )
    before = [tuple(row) for row in store._connection.execute("SELECT * FROM tokens")]
    changes = store._connection.total_changes
    statements = []
    store._connection.set_trace_callback(statements.append)

    def forbidden(*args, **kwargs):
        pytest.fail("readiness must not authenticate or open anything")

    monkeypatch.setattr(tokens, "authenticate", forbidden)
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    assert registry.available() and tokens.available()
    assert len(statements) == 4 and all(query.startswith("SELECT") and "LIMIT 1" in query for query in statements)
    assert store._connection.total_changes == changes
    assert [tuple(row) for row in store._connection.execute("SELECT * FROM tokens")] == before
    store._connection.execute("DROP TABLE token_journal")
    store._connection.execute("DROP TABLE tokens")
    assert not tokens.available() and registry.available()
    store.close()
    assert not tokens.available() and not registry.available()


def test_q5_dependencies_readiness_defaults_unavailable():
    from dataclasses import FrozenInstanceError

    from memory_server.gateway_contracts.ports import GatewayDependencies

    dependencies = GatewayDependencies(registry=None, tokens=None, access=None, metrics=None, ports={})
    assert dependencies.readiness is None
    with pytest.raises(FrozenInstanceError):
        dependencies.readiness = publish_ok


@pytest.fixture
async def observed_daemon(tmp_path):
    from memory_server.application.profile_bundle import BundlePolicies
    from memory_server.runtime.daemon import Daemon
    from memory_server.runtime.runtime_manager import RuntimeManager
    from memory_server.settings import Settings
    from tests.gateway.test_daemon import factory, make_binding

    settings = Settings(_env_file=None, outbox_poll_interval_seconds=0.01)
    bindings = tuple(make_binding(tmp_path / name, name, settings) for name in ("alpha", "beta"))
    with ControlStore(tmp_path / "control") as store:
        registry = ProfileRegistry(store, RegistryConfiguration(bindings))
        daemon = Daemon(registry, policies=BundlePolicies.capture(settings),
                        manager=RuntimeManager(factory_builder=factory))
        try:
            yield daemon, registry, store
        finally:
            await daemon.close_all()


async def test_q5_snapshot_real_runtime_and_stop_without_lazy_open(observed_daemon, monkeypatch):
    from dataclasses import FrozenInstanceError

    daemon, _, _ = observed_daemon
    assert callable(getattr(daemon, "snapshot", None))
    assert dict(daemon.snapshot()) == {}
    assert await daemon.stop_profile("beta") is None
    assert not daemon.manager._entries
    ctx, _, _ = control_input()
    async with daemon.acquire(ctx) as session:
        snapshot = daemon.snapshot()
        observed = snapshot["alpha"]
        assert observed.state == "ready" and observed.owned and observed.admissions == 1
        assert not observed.stopping and not observed.retained and "beta" not in snapshot
        with pytest.raises(FrozenInstanceError):
            observed.owned = False
        with pytest.raises(TypeError):
            snapshot["beta"] = observed

        def forbidden(*args, **kwargs):
            pytest.fail("snapshot must not call manager.get/acquire")

        with monkeypatch.context() as patch:
            patch.setattr(daemon.manager, "get", forbidden)
            patch.setattr(daemon, "acquire", forbidden)
            assert daemon.snapshot()["alpha"] == observed
        assert await daemon.stop_profile("alpha") is None
        stopped = daemon.snapshot()["alpha"]
        assert stopped.state == "closing" and stopped.stopping and stopped.owned
        assert session.runtime.lock is not None
    assert dict(daemon.snapshot()) == {}
    assert session.runtime.lock is None
    assert observed.admissions == 1  # detached, not a live view


async def test_q5_stop_closes_admission_before_lock_await(observed_daemon):
    daemon, _, _ = observed_daemon
    ctx, _, _ = control_input()
    async with daemon.acquire(ctx):
        async with daemon._lock:
            task = asyncio.create_task(daemon.stop_profile("alpha"))
            try:
                await asyncio.sleep(0)
                assert daemon._entries["alpha"].stopping
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass


async def test_q5_failed_close_snapshot_retains_real_owner(observed_daemon, monkeypatch):
    daemon, _, _ = observed_daemon
    assert callable(getattr(daemon, "snapshot", None))
    ctx, _, _ = control_input()
    async with daemon.acquire(ctx) as session:
        pass
    close = session.runtime.dependencies.sqlite.close

    async def fail_close():
        raise RuntimeError("synthetic close failure")

    monkeypatch.setattr(session.runtime.dependencies.sqlite, "close", fail_close)
    try:
        report = await daemon.stop_profile("alpha")
        observation = daemon.snapshot()["alpha"]
        assert report.lock_retained and session.runtime.lock is not None
        assert observation.state == "failed" and observation.owned
        assert observation.stopping and observation.retained and observation.admissions == 0
    finally:
        monkeypatch.setattr(session.runtime.dependencies.sqlite, "close", close)
        await daemon.manager.stop_profile("alpha")


async def test_q5_control_real_daemon_retains_pending_until_drain(observed_daemon):
    daemon, registry, store = observed_daemon
    assert callable(getattr(daemon, "snapshot", None))
    args = control_input()
    async with daemon.acquire(args[0]):
        with pytest.raises(ControlError) as exc:
            await run_control(registry, args, daemon)
        assert exc.value.code == "E_PROFILE_BUSY"
        assert not store._get("alpha").enabled
        assert daemon.snapshot()["alpha"].stopping
        assert not await registry.reconcile_control(leases=daemon, publish=publish_ok, limit=1)
    assert await registry.reconcile_control(leases=daemon, publish=publish_ok, limit=1)
    assert (await run_control(registry, args, daemon)).replayed
    assert len(store.journal(profile_ids=["alpha"])) == 1


def test_q8_wire_ceiling_defaults_and_bounds(tmp_path):
    from memory_server.gateway_contracts.ports import HttpConfig

    config = HttpConfig(control_path=tmp_path)
    assert (config.max_header_bytes, config.header_timeout_seconds, config.max_connections) == (16384, 5.0, 128)
    bounded = HttpConfig(control_path=tmp_path, max_header_bytes=1048576,
                         header_timeout_seconds=300.0, request_timeout_seconds=300.0, max_connections=65536)
    assert bounded.max_header_bytes == 1048576 and bounded.max_connections == 65536
    for name, values in {
        "max_header_bytes": (0, -1, 1048577, True, 1.0, "16384", None),
        "header_timeout_seconds": (0, -1, 300.001, True, float("nan"), float("inf"), "5", None),
        "max_connections": (0, -1, 65537, True, 1.0, "128", None),
    }.items():
        for value in values:
            with pytest.raises(ValueError):
                HttpConfig(control_path=tmp_path, **{name: value})
    smallest = HttpConfig(control_path=tmp_path, max_header_bytes=1,
                          header_timeout_seconds=0.001, max_connections=1)
    assert smallest.header_timeout_seconds == 0.001


def test_q9_audit_eligibility_policy_is_closed_and_immutable():
    from memory_server.gateway_contracts import ports

    assert dict(ports.AUDIT_ELIGIBILITY) == {
        "pre_auth_reject": "none",
        "authentication_failure": "denial",
        "authentication_storage_failure": "none",
        "authenticated_authz_reject": "none",
        "authenticated_validation_reject": "none",
        "authenticated_rate_reject": "none",
        "eligible_execution": "intent_outcome",
        "public_probe": "none",
    }
    with pytest.raises(TypeError):
        ports.AUDIT_ELIGIBILITY["pre_auth_reject"] = "intent_outcome"


async def test_q9_existing_denial_binding_no_submitted_identity(tmp_path):
    from memory_server.control.access_audit import AccessAuditStore, AuditError
    from memory_server.gateway_contracts import ports

    assert ports.AUDIT_ELIGIBILITY["authentication_failure"] == "denial"
    audit = AccessAuditStore(tmp_path / "audit")
    try:
        request_id = "req_" + str(uuid4())
        ticket = await audit.record_unauthenticated(request_id, "E_AUTH_REQUIRED")
        assert ticket is not None and ticket.request_id == request_id
        assert audit._db.execute("SELECT COUNT(*) FROM access_audit").fetchone()[0] == 0
        rows = audit._denials._db.execute("SELECT * FROM access_audit").fetchall()
        assert len(rows) == 1 and rows[0]["state"] == "terminal"
        assert rows[0]["token_id"] is None and rows[0]["profile_id"] is None and rows[0]["client_id"] is None
        assert rows[0]["operation"] == "authentication" and rows[0]["result"] == "E_AUTH_REQUIRED"
        with pytest.raises(AuditError) as exc:
            await audit.record_unauthenticated("req_" + str(uuid4()), "E_PERMISSION_DENIED")
        assert exc.value.code == "E_VALIDATION"
        assert audit._denials._db.execute("SELECT COUNT(*) FROM access_audit").fetchone()[0] == 1
    finally:
        audit.close()


async def test_f1_pending_is_profile_scoped_and_recovers_without_restart(configured):
    registry, store, _ = configured
    leases = ObservedLeases()

    async def failed_publication(ticket, outcome):
        raise OSError("publication unavailable")

    a = control_input()
    await run_control(registry, a, leases, publish=failed_publication)
    assert store.control_requests(limit=1)[0]["pending_publication"] == 1
    b = await run_control(registry, control_input("beta", enable=True), leases)
    assert b.status == 200
    b_before = tuple(store._connection.execute("SELECT * FROM profiles WHERE id='beta'").fetchone())
    b_journal = store.journal(profile_ids=["beta"])
    seen = []

    async def recover(ticket, outcome):
        seen.append(ticket)
        return True

    assert await registry.reconcile_control(leases=leases, publish=recover, limit=1)
    assert seen == [a[2]] and leases.calls == 1
    assert tuple(store._connection.execute("SELECT * FROM profiles WHERE id='beta'").fetchone()) == b_before
    assert store.journal(profile_ids=["beta"]) == b_journal
    assert store.control_requests(limit=1) == ()


async def test_f1_bounded_recovery_rotates_past_failed_profile(configured):
    registry, store, _ = configured
    leases = ObservedLeases()

    async def unavailable(ticket, outcome):
        return False

    a = control_input()
    b = control_input("beta", enable=True)
    await run_control(registry, a, leases, publish=unavailable)
    await run_control(registry, b, leases, publish=unavailable)
    seen = []

    async def recover_b(ticket, outcome):
        seen.append(ticket.id)
        return ticket.id == b[2].id

    assert not await registry.reconcile_control(leases=leases, publish=recover_b, limit=1)
    assert not await registry.reconcile_control(leases=leases, publish=recover_b, limit=1)
    assert seen == [a[2].id, b[2].id]
    assert [row["profile_id"] for row in store.control_requests(limit=10)] == ["alpha"]


async def test_f2_pending_readiness_is_false_and_read_only(configured, monkeypatch):
    from datetime import timedelta

    from memory_server.control.tokens import TokenService
    from memory_server.gateway_contracts.ports import GatewayDependencies

    registry, store, _ = configured
    tokens = TokenService(store)
    now = datetime.now(timezone.utc)
    tokens._bootstrap_create(d.TokenCreate(subject="probe", profiles=["alpha"],
        permissions=["profile.read"], expires_at=now + timedelta(hours=1)), now=now)

    async def unavailable(ticket, outcome):
        return False

    await run_control(registry, control_input(), ObservedLeases(), publish=unavailable)
    before = [tuple(row) for row in store._connection.execute("SELECT * FROM tokens")]
    changes = store._connection.total_changes
    statements = []
    store._connection.set_trace_callback(statements.append)

    def forbidden(*args, **kwargs):
        pytest.fail("readiness authenticated or opened storage")

    monkeypatch.setattr(tokens, "authenticate", forbidden)
    monkeypatch.setattr(tokens, "verify_principal", forbidden)
    monkeypatch.setattr(sqlite3, "connect", forbidden)

    async def ready():
        return registry.available() and tokens.available()

    deps = GatewayDependencies(registry=registry, tokens=tokens, access=None, metrics=None, ports={}, readiness=ready)
    assert not registry.available()
    assert not tokens.available()
    assert not await deps.readiness()
    assert statements and all(q.startswith("SELECT") and "LIMIT 1" in q for q in statements)
    assert store._connection.total_changes == changes
    assert [tuple(row) for row in store._connection.execute("SELECT * FROM tokens")] == before


@pytest.mark.parametrize("overrides", [
    {"max_header_bytes": 10**9},
    {"max_connections": 10**9},
    {"header_timeout_seconds": 10**6},
    {"header_timeout_seconds": 6, "request_timeout_seconds": 5},
    {"request_timeout_seconds": float("inf")},
    {"request_timeout_seconds": float("nan")},
    {"request_timeout_seconds": 0},
])
def test_f4_use_time_validation_rejects_subclass_bypass(tmp_path, overrides):
    from memory_server.gateway_contracts import ports

    class Bypass(ports.HttpConfig):
        def __post_init__(self):
            pass

    config = Bypass(control_path=tmp_path, **overrides)
    with pytest.raises(ValueError, match="^E_VALIDATION:"):
        ports.validate_http_config(config)


def test_f4_header_timeout_cannot_exceed_request_timeout(tmp_path):
    from memory_server.gateway_contracts.ports import HttpConfig

    with pytest.raises(ValueError, match="^E_VALIDATION:"):
        HttpConfig(control_path=tmp_path, header_timeout_seconds=300, request_timeout_seconds=30)


async def test_f3_page_and_record_roots_are_frozen_null(configured):
    registry, store, _ = configured
    actor = principal("alpha")
    page, _ = await registry.page(actor, limit=1, after=None, upper=None)
    for permission in ("profile.read", "profile.manage"):
        record = await registry.record(actor, "alpha", permission=permission)
        assert record.canonical_root is page[0].storage_root is None
    assert store._get("alpha").canonical_root is not None
