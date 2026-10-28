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
            with pytest.raises(ControlError) as exc:
                await run_control(other, control_input("beta", enable=True), leases)
            assert exc.value.code == "E_PROFILE_BUSY"
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
