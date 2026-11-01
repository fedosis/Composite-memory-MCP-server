"""Real HTTP concurrency, transactional fault schedules and cancellation."""

from __future__ import annotations

import asyncio
import threading
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction
from storage.repositories.lifecycle_repo import LifecycleRepository

from tests.gateway.test_http_lifecycle import (
    CHILD,
    OTHER,
    ROOT,
    edges,
    payload,
    post,
    seed,
    snapshot,
)
from tests.gateway.test_http_lifecycle import lifecycle as lifecycle
from tests.gateway.test_http_shell import code

pytestmark = pytest.mark.asyncio


async def retry(h, key, **kwargs):
    for _ in range(20):
        r = await post(h, key=key, **kwargs)
        if r.status != 409 or r.json["error"]["code"] != "E_REQUEST_IN_PROGRESS":
            return r
        await asyncio.sleep(0.01)
    pytest.fail("transient busy did not settle to a semantic outcome")


async def test_two_clients_barrier_same_observed_version(lifecycle):
    h = lifecycle
    await seed(h)
    await seed(h, CHILD)
    await seed(h, profile="beta")
    await edges(h, [(CHILD, ROOT, "derived_from")])
    barrier = asyncio.Barrier(2)

    async def client():
        observed = (await snapshot(h))["facts"][0]["version"]
        assert observed == "1"
        await barrier.wait()
        return await retry(h, str(uuid4()), op="invalidate")

    first, second, independent = await asyncio.gather(client(), client(), post(h, profile="beta"))
    assert independent.status == 200, independent.body
    assert sorted([first.status, second.status]) == [200, 409], [first.body, second.body]
    loser = first if first.status == 409 else second
    code(loser, "E_VERSION_CONFLICT", 409)
    assert loser.json["error"]["details"] == dict(expected=1, actual=2, id=ROOT)
    s = await snapshot(h)
    assert len(s["lifecycle_events"]) == len(s["outbox_entries"]) == 2
    assert len(s["http_request_journal"]) == 1
    assert {f["version"] for f in s["facts"]} == {"2"}


async def test_writer_busy_at_journal_purge_retries_to_typed_conflict(lifecycle, monkeypatch):
    """Real second writer, not an injected exception or a probabilistic race."""
    import sqlite3
    import traceback

    from sqlalchemy.exc import OperationalError

    h = lifecycle
    await seed(h)
    before = await snapshot(h)
    original, failures = AsyncSession.execute, []

    async def no_wait(session, statement, *args, **kwargs):
        if str(statement).startswith("DELETE FROM http_request_journal"):
            await original(session, text("PRAGMA busy_timeout=0"))
            try:
                return await original(session, statement, *args, **kwargs)
            except OperationalError as exc:
                failures.append(exc)
                print("S14_WRITER_ORIGIN", type(exc).__name__, type(exc.orig).__name__,
                      str(exc.orig), exc.orig.sqlite_errorcode, exc.orig.sqlite_errorname)
                traceback.print_exc()
                raise
        return await original(session, statement, *args, **kwargs)

    key = str(uuid4())
    with monkeypatch.context() as m:
        m.setattr(AsyncSession, "execute", no_wait)
        async with h.factories["alpha"]() as writer, writer.begin():
            await writer.execute(text("UPDATE facts SET version=version WHERE id=:id"), {"id": ROOT})
            response = await post(h, key=key)
            assert len(failures) == 1
            assert isinstance(failures[0].orig, sqlite3.OperationalError)
            assert failures[0].orig.sqlite_errorcode == sqlite3.SQLITE_BUSY
            code(response, "E_REQUEST_IN_PROGRESS", 409)
    assert await snapshot(h) == before
    winner = await post(h)
    assert winner.status == 200, winner.body
    loser = await retry(h, key)
    code(loser, "E_VERSION_CONFLICT", 409)
    assert loser.json["error"]["details"] == dict(expected=1, actual=2, id=ROOT)
    after = await snapshot(h)
    assert len(after["http_request_journal"]) == len(after["lifecycle_events"]) == 1


async def test_inflight_audit_publication_is_not_a_sqlite_busy_error(lifecycle, monkeypatch):
    """Characterize the distinct 503 path; do not relabel an audit outage busy."""
    import traceback

    from memory_server.application.mutation_uow import MutationUoW
    from memory_server.application.remote_lifecycle import RemoteLifecyclePort
    from memory_server.transports.http.errors import HttpError

    h = lifecycle
    await seed(h)
    reached, release = asyncio.Event(), asyncio.Event()
    tail, attempt, failures = RemoteLifecyclePort._tail, MutationUoW._attempt, []

    async def pause(port, *args, **kwargs):
        reached.set()
        await release.wait()
        return await tail(port, *args, **kwargs)

    async def observe(uow, *args, **kwargs):
        try:
            return await attempt(uow, *args, **kwargs)
        except HttpError as exc:
            failures.append((exc.code, traceback.format_exc()))
            raise

    with monkeypatch.context() as m:
        m.setattr(RemoteLifecyclePort, "_tail", pause)
        m.setattr(MutationUoW, "_attempt", observe)
        winner = asyncio.create_task(post(h))
        try:
            await asyncio.wait_for(reached.wait(), 5)
            committed = await snapshot(h)
            assert committed["http_request_journal"][0]["pending_publication"] == 1
            loser = await post(h)
            code(loser, "E_AUDIT_UNAVAILABLE", 503)
            assert await snapshot(h) == committed
            assert len(failures) == 1 and failures[0][0] == "E_AUDIT_UNAVAILABLE"
            assert 'mutation_uow.py", line 214' in failures[0][1]
            print("S14_PENDING_AUDIT_ORIGIN", failures[0][1])
        finally:
            release.set()
            response = await winner
        assert response.status == 200, response.body
    code(await post(h), "E_VERSION_CONFLICT", 409)


@pytest.mark.parametrize("boundary", ["journal", "handler"])
@pytest.mark.parametrize("failure", ["readonly", "missing_table"])
async def test_non_busy_sqlite_fault_is_storage_not_transient(lifecycle, monkeypatch, boundary, failure):
    import sqlite3

    from sqlalchemy.exc import OperationalError

    h = lifecycle
    await seed(h)
    before = await snapshot(h)
    original, failures = AsyncSession.execute, []

    async def fault(session, statement, *args, **kwargs):
        sql = str(statement)
        matches = (
            sql.startswith("DELETE FROM http_request_journal")
            if boundary == "journal"
            else sql.startswith("UPDATE facts SET")
        )
        if matches:
            if failure == "readonly":
                await original(session, text("PRAGMA query_only=ON"))
            else:
                statement = text("UPDATE s14_missing_table SET value=1")
            try:
                return await original(session, statement, *args, **kwargs)
            except OperationalError as exc:
                failures.append(exc.orig)
                raise
            finally:
                await original(session, text("PRAGMA query_only=OFF"))
        return await original(session, statement, *args, **kwargs)

    with monkeypatch.context() as m:
        m.setattr(AsyncSession, "execute", fault)
        code(await post(h), "E_STORAGE", 503)
    assert len(failures) == 1
    assert failures[0].sqlite_errorcode == (sqlite3.SQLITE_READONLY if failure == "readonly" else sqlite3.SQLITE_ERROR)
    assert await snapshot(h) == before


@pytest.mark.parametrize("schedule", ["first_select", "conditional_update"])
async def test_dependent_cas_conflict_rolls_back_parent_and_prior_dependents(lifecycle, monkeypatch, schedule):
    h = lifecycle
    await seed(h)
    await seed(h, CHILD)
    await seed(h, OTHER, version=2)
    await edges(h, [(CHILD, ROOT, "derived_from"), (OTHER, ROOT, "derived_from")])
    before = await snapshot(h)
    staged, original = [], LifecycleRepository.transition
    execute = AsyncSession.execute

    async def wrong_expected(repo, repository, **kwargs):
        staged.append(kwargs["memory_id"])
        if kwargs["memory_id"] == OTHER and schedule == "first_select":
            kwargs["expected_version"] -= 1
        return await original(repo, repository, **kwargs)

    async def between(session, statement, *args, **kwargs):
        sql = str(statement)
        compiled = statement.compile().params
        if schedule == "conditional_update" and sql.startswith("UPDATE facts SET") and compiled.get("id_1") == OTHER:
            await execute(session, text("UPDATE facts SET version='3' WHERE id=:id"), {"id": OTHER})
        return await execute(session, statement, *args, **kwargs)

    with monkeypatch.context() as m:
        m.setattr(LifecycleRepository, "transition", wrong_expected)
        m.setattr(AsyncSession, "execute", between)
        r = await post(h, op="invalidate")
        code(r, "E_VERSION_CONFLICT", 409)
        assert r.json["error"]["details"]["id"] == OTHER
        assert r.json["error"]["details"]["actual"] == (2 if schedule == "first_select" else 3)
    assert staged == [ROOT, CHILD, OTHER]
    assert await snapshot(h) == before


async def test_same_key_concurrent_x10_one_effect(lifecycle):
    h = lifecycle
    await seed(h)
    await seed(h, CHILD)
    await edges(h, [(CHILD, ROOT, "derived_from")])
    barrier, key = asyncio.Barrier(10), str(uuid4())

    async def client():
        await barrier.wait()
        return await retry(h, key, op="invalidate")

    results = await asyncio.gather(*(client() for _ in range(10)))
    assert all(r.status == 200 for r in results), [r.body for r in results]
    assert len({r.body for r in results}) == 1
    s = await snapshot(h)
    assert len(s["http_request_journal"]) == 1
    assert len(s["lifecycle_events"]) == len(s["outbox_entries"]) == 2


async def test_cancel_before_commit_rolls_back_all_effects(lifecycle, monkeypatch):
    from memory_server.application.remote_lifecycle import LifecycleHandler

    h = lifecycle
    await seed(h)
    before = await snapshot(h)
    staged, release = asyncio.Event(), asyncio.Event()
    original = LifecycleHandler.apply

    async def pause(handler, *args, **kwargs):
        result = await original(handler, *args, **kwargs)
        staged.set()
        await release.wait()
        return result

    with monkeypatch.context() as m:
        m.setattr(LifecycleHandler, "apply", pause)
        task = asyncio.create_task(post(h))
        await asyncio.wait_for(staged.wait(), 5)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert await snapshot(h) == before
    assert (await post(h)).status == 200


@pytest.mark.parametrize("window", ["before", "after"])
async def test_cancel_commit_window_resolves_by_original_key(lifecycle, monkeypatch, window):
    h = lifecycle
    await seed(h)
    key, reached, release = str(uuid4()), asyncio.Event(), asyncio.Event()
    original = AsyncSessionTransaction.__aexit__
    intercepted = []

    async def pause(transaction, typ, value, trace):
        # Only the request transaction which has staged its durable journal.
        session = transaction.session
        candidates = [
            obj
            for obj in session.sync_session.identity_map.values()
            if obj.__class__.__name__ == "HttpRequestJournalORM"
        ]
        if not intercepted and typ is None:
            # Journal is flushed and may no longer be held by the identity map.
            count = await session.scalar(text("SELECT count(*) FROM http_request_journal"))
            candidates = count
        if candidates and not intercepted and typ is None:
            intercepted.append(True)
            if window == "after":
                result = await original(transaction, typ, value, trace)
            reached.set()
            await release.wait()
            if window == "after":
                return result
        return await original(transaction, typ, value, trace)

    with monkeypatch.context() as m:
        m.setattr(AsyncSessionTransaction, "__aexit__", pause)
        task = asyncio.create_task(post(h, key=key))
        await asyncio.wait_for(reached.wait(), 5)
        task.cancel()
        release.set()
        if window == "before":
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            recovered = await task
            assert recovered.status == 200, recovered.body
    replay = await retry(h, key)
    assert replay.status == 200, replay.body
    s = await snapshot(h)
    assert len(s["lifecycle_events"]) == len(s["http_request_journal"]) == 1
    assert s["http_request_journal"][0]["pending_publication"] == 0
    if window == "after":
        assert replay.body == recovered.body
        rows = [dict(r) for r in h.deps.access._db.execute("SELECT * FROM access_audit")]
        assert all(row["result"] == "success" for row in rows)


async def test_cancelled_native_waiter_retains_owner(lifecycle):
    runtime = lifecycle.deps.leases._entries["alpha"].runtime
    hold, started = threading.Event(), threading.Event()
    handle = await runtime.submit_native(lambda: (started.set(), hold.wait(), 7)[2], kind="s14-cancel")
    try:
        assert await asyncio.to_thread(started.wait, 3)
        waiter = asyncio.create_task(handle.result())
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        rec = next(n for n in runtime.owned_snapshot()[2] if n.kind == "s14-cancel")
        assert rec.state != "TERMINATED" and rec.capacity_held and handle.thread.is_alive()
        import fcntl

        lock_path = lifecycle.deps.leases._entries["alpha"].binding.layout.data_root / ".cmms-storage.lock"
        with lock_path.open("r+") as contender:
            with pytest.raises(BlockingIOError):
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert lifecycle.deps.leases._entries["alpha"].runtime is runtime
        closer = asyncio.create_task(runtime.close())
        await asyncio.sleep(0.02)
        assert not closer.done() and handle.thread.is_alive()
    finally:
        hold.set()
        await handle.result()
    report = await closer
    assert report.code is None and not report.lock_retained and not report.retained_resources


async def test_missing_cas_428_never_mutates_under_load(lifecycle):
    h = lifecycle
    await seed(h)
    missing = payload()
    del missing["expected_version"]
    rejected, success = await asyncio.gather(asyncio.gather(*(post(h, body=missing) for _ in range(10))), post(h))
    assert success.status == 200, success.body
    for r in rejected:
        code(r, "E_PRECONDITION_REQUIRED", 428)
        assert r.json["error"]["details"] == {}
    s = await snapshot(h)
    assert len(s["lifecycle_events"]) == len(s["http_request_journal"]) == len(s["outbox_entries"]) == 1
    rows = list(h.deps.access._db.execute("SELECT * FROM access_audit"))
    assert len(rows) == 1
