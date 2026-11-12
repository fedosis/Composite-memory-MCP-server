"""S20A1 owner fence regressions, using real runtime admission and EX locks."""

import asyncio
import inspect
import time
import traceback
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from memory_server.runtime.runtime import CMMSRuntime


@asynccontextmanager
async def projection_owner(root, embedder=None):
    from sqlalchemy import text

    from memory_server import projection_rebuild as p
    from memory_server.providers.graph_provider import SimpleGraph
    from memory_server.providers.sqlite_provider import SQLiteProvider
    from memory_server.runtime.dependencies import RuntimeDependencies

    async def sqlite(owner):
        provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{root / 'memory.db'}")
        await provider.initialize()
        async with provider.engine.begin() as connection:
            await connection.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)"))
            await connection.execute(text("INSERT INTO alembic_version VALUES ('0007')"))
            await connection.execute(text(
                "INSERT INTO facts(id,subject,predicate,object,dedup_key,confidence,creator,source,"
                "created_at,updated_at,verification_status,lifecycle_state,version) VALUES "
                "('f','subject','predicate','object','f',0.8,'test','test','2026-10-30 00:00:00.000000',"
                "'2026-10-30 00:00:00.000000','verified','active','1')"
            ))
        return provider

    required(p, "snapshot_owned_projection_records")
    layout = SimpleNamespace(data_root=root, vector=SimpleNamespace(backend="lancedb", local_path=root / "vector"),
                             graph_snapshot_path=root / "graph.json")
    runtime = CMMSRuntime(layout, profile_id="a", dependencies=RuntimeDependencies(
        embedder=embedder, recipes=(("sqlite", sqlite),),
    ))
    await runtime.start()
    root.chmod(0o700)
    (root / "vector").mkdir(mode=0o700)
    SimpleGraph().save_snapshot(root / "graph.json")
    (root / "graph.json").chmod(0o600)
    token = runtime.admit()
    try:
        begin = required(runtime, "begin_maintenance")
        custody = begin(profile_id="a", canonical_root=root, deadline=time.monotonic() + 30)
        await runtime.drain_maintenance(custody)
        yield runtime, custody
    finally:
        runtime.release_admitted(token)
        assert (await runtime.close()).code is None


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["snapshot", "rebuild"])
async def test_s20a_owner_oserror_no_absolute_path(tmp_path, caplog, phase):
    from memory_server import projection_rebuild as p

    canary = tmp_path / "ABSOLUTE_PATH_CANARY"
    async with projection_owner(canary) as (runtime, custody):
        if phase == "rebuild":
            snapshot = await p.snapshot_owned_projection_records(runtime, custody=custody)
        # Real FS failures: snapshot os.open has an absolute canary filename;
        # rebuild stat fails inside its publication preflight, after native read.
        moved = canary.with_name("held-root")
        if phase == "snapshot":
            canary.rename(moved)
        else:
            (canary / "graph.json").unlink()
        caught = None
        try:
            if phase == "snapshot":
                await p.snapshot_owned_projection_records(runtime, custody=custody)
            else:
                await p.rebuild_owned_projections(snapshot, custody=custody, targets=frozenset({"graph"}))
        except Exception as exc:
            caught = exc
        finally:
            if phase == "snapshot":
                moved.rename(canary)
        assert caught is not None, "S20A_CANARY_ERROR_REQUIRED"
        public = repr(caught.args) + str(caught) + ''.join(traceback.format_exception(caught))
        assert str(tmp_path) not in public and "ABSOLUTE_PATH_CANARY" not in public, "S20A_CANARY_PUBLIC_PATH"
        assert getattr(caught, "code", None) == "E_STORAGE", "S20A_CANARY_STORAGE_CODE"
        assert caught.__context__ is None and caught.__cause__ is None, "S20A_CANARY_EXCEPTION_CHAIN"
        for record in caplog.records:
            rendered = repr(record.__dict__)
            assert str(tmp_path) not in rendered and "ABSOLUTE_PATH_CANARY" not in rendered, "S20A_CANARY_LOG_PATH"
        assert runtime._maintenance_state == "blocked", "S20A_CANARY_FAIL_CLOSED"


@pytest.mark.asyncio
async def test_s20a_backup_driver_owned_registry(tmp_path, monkeypatch):
    from memory_server import projection_rebuild as p

    async with projection_owner(tmp_path / "a") as (runtime, custody):
        spawn = runtime.spawn_owned_task
        attached = []

        def observed_spawn(factory, *, kind):
            task = spawn(factory, kind=kind)
            records = runtime.owned_snapshot()[1]
            assert any(r.task is task and r.kind == kind for r in records), "S20A_BACKUP_REGISTERED"
            attached.append(kind)
            return task

        monkeypatch.setattr(runtime, "spawn_owned_task", observed_spawn)
        await p.snapshot_owned_projection_records(runtime, custody=custody)
        assert attached == ["projection-backup", "projection-backup-driver"], "S20A_BACKUP_DRIVER_OWNED"


@pytest.mark.asyncio
@pytest.mark.parametrize("remote", [False, True])
async def test_s20a_network_false_local_remote(tmp_path, remote):
    from memory_server import projection_rebuild as p
    from memory_server.providers.embedding_provider import MockEmbeddingProvider, OpenAIEmbeddingProvider

    embedder = (OpenAIEmbeddingProvider(base_url="https://not-contacted.invalid/v1")
                if remote else MockEmbeddingProvider())
    async with projection_owner(tmp_path / "a", embedder) as (runtime, custody):
        snapshot = await p.snapshot_owned_projection_records(runtime, custody=custody)
        if remote:
            with pytest.raises(Exception) as rejected:
                await p.rebuild_owned_projections(snapshot, custody=custody, targets=frozenset({"vector"}),
                                                  allow_network_embedding=False)
            assert rejected.value.code == "E_CAPABILITY_UNAVAILABLE", "S20A_NETWORK_REMOTE_REFUSED"
            assert embedder._client is None, "S20A_NETWORK_ZERO_CLIENT"
        else:
            result = await p.rebuild_owned_projections(snapshot, custody=custody, targets=frozenset({"vector"}),
                                                       allow_network_embedding=False)
            assert result is not None and embedder._cache, "S20A_NETWORK_LOCAL_WORKS"
            assert runtime.dependencies.embedder is embedder, "S20A_NETWORK_CAPTURED_PROVIDER"


async def worker_runtime(root):
    from storage.outbox_worker import OutboxWorker

    from memory_server.providers.sqlite_provider import SQLiteProvider
    from memory_server.runtime.dependencies import RuntimeDependencies

    async def sqlite(owner):
        provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{owner.root / 'memory.db'}")
        await provider.initialize()
        return provider

    async def worker(owner):
        result = OutboxWorker(
            engine=owner.dependencies.sqlite.engine,
            task_spawner=owner.spawn_owned_task,
            native_submitter=owner.submit_native,
            poll_interval_seconds=0.01,
        )
        await result.initialize()
        return result

    runtime = CMMSRuntime(
        root, profile_id="a",
        dependencies=RuntimeDependencies(recipes=(("sqlite", sqlite), ("outbox_worker", worker))),
    )
    await runtime.start()
    return runtime


@pytest.mark.asyncio
async def test_s20a_pause_requires_custody(tmp_path):
    runtime = await worker_runtime(tmp_path / "a")
    worker = runtime.dependencies.outbox_worker
    token = runtime.admit()
    try:
        hook = getattr(worker, "_pause_for_maintenance", None)
        assert callable(hook), "S20A_PAUSE_CUSTODY: private hook missing"
        for invalid in (None, object()):
            with pytest.raises(Exception):
                hook(custody=invalid)
            assert not worker._maintenance_paused, "S20A_PAUSE_CUSTODY"
        assert not hasattr(worker, "pause") and not hasattr(worker, "resume"), "S20A_PAUSE_CUSTODY"
        custody = runtime.begin_maintenance(
            profile_id="a", canonical_root=runtime.root, deadline=time.monotonic() + 10,
        )

        async def foreign_task():
            with pytest.raises(Exception):
                hook(custody=custody)
            assert not worker._maintenance_paused, "S20A_PAUSE_CUSTODY: foreign task"

        await asyncio.create_task(foreign_task())
        hook(custody=custody)
        assert worker._maintenance_paused, "S20A_PAUSE_CUSTODY"
        with pytest.raises(Exception) as rejected:
            async with worker.claim_scope(deadline=time.monotonic() + 1):
                pytest.fail("S20A_PAUSE_CUSTODY: paused claim entered")
        assert rejected.value.code == "E_MAINTENANCE_CONFLICT"
    finally:
        runtime.release_admitted(token)
        assert (await runtime.close()).code is None


@pytest.mark.asyncio
async def test_s20a_pause_same_loop_queued_claim(tmp_path):
    runtime = await worker_runtime(tmp_path / "a")
    worker = runtime.dependencies.outbox_worker
    token = runtime.admit()
    engine, sessions = worker._engine, worker._session_factory
    queued = asyncio.Event()
    rejected = asyncio.Event()

    async def waiter():
        queued.set()
        try:
            async with worker.claim_scope(deadline=time.monotonic() + 5):
                pytest.fail("S20A_LOOP: queued claim crossed pause")
        except Exception as exc:
            assert exc.code == "E_MAINTENANCE_CONFLICT", "S20A_LOOP"
            rejected.set()

    parent = pending = None
    try:
        hook = getattr(worker, "_pause_for_maintenance", None)
        assert callable(hook), "S20A_LOOP: private hook missing"
        async with worker.claim_scope(deadline=time.monotonic() + 5):
            pending = asyncio.create_task(waiter())
            await queued.wait()
            custody = runtime.begin_maintenance(
                profile_id="a", canonical_root=runtime.root, deadline=time.monotonic() + 10,
            )
            hook(custody=custody)
            assert worker._active_claim is not None, "S20A_LOOP: active claim interrupted"
        await pending
        assert rejected.is_set() and not worker._claim_lock.locked(), "S20A_LOOP"
        parent = runtime.spawn_owned_task(worker.run, kind="daemon-worker-parent")
        await runtime.drain_maintenance(custody, worker_task=parent)
        assert worker._maintenance_pause_ack and not parent.done(), "S20A_WORKER_IDENTITY"
        assert worker._engine is engine and worker._session_factory is sessions, "S20A_LOOP"
        worker.stop()
        await asyncio.wait_for(asyncio.shield(parent), 1)
        assert worker._maintenance_paused, "S20A_LOOP: terminal stop opened gate"
    finally:
        worker.stop()
        if pending is not None:
            await asyncio.gather(pending, return_exceptions=True)
        if parent is not None:
            await asyncio.gather(parent, return_exceptions=True)
        runtime.release_admitted(token)
        assert (await runtime.close()).code is None


def required(runtime, name):
    method = getattr(runtime, name, None)
    assert callable(method), f"S20A_SURFACE: {name}"
    return method


@pytest.mark.asyncio
async def test_s20a_ready_maintenance_admission(tmp_path):
    first = CMMSRuntime(tmp_path / "a", profile_id="a")
    second = CMMSRuntime(tmp_path / "b", profile_id="b")
    await first.start()
    await second.start()
    token = first.admit()
    lock = first.lock
    try:
        begin = required(first, "begin_maintenance")
        assert "job_token" not in inspect.signature(begin).parameters
        custody = begin(profile_id="a", canonical_root=first.root, deadline=time.monotonic() + 10)
        assert first.state == "READY" and first.lock is lock, "S20A_FENCE"
        assert custody.self_task is asyncio.current_task(), "S20A_DRAIN"
        with pytest.raises(Exception) as denied:
            first.admit()
        assert denied.value.code == "E_MAINTENANCE_CONFLICT", "S20A_FENCE"
        other = second.admit()
        second.release_admitted(other)
        await first.drain_maintenance(custody)
        first.check_maintenance(custody)
        for _ in range(10):
            with pytest.raises(Exception) as denied:
                first.finish_maintenance(custody)
            assert denied.value.code == "E_MAINTENANCE_CONFLICT", "S20A_FINISH_NO_UNLOCK"
        with pytest.raises(Exception) as denied:
            first.admit()
        assert denied.value.code == "E_MAINTENANCE_CONFLICT", "S20A_FINISH_NO_UNLOCK"
    finally:
        first.release_admitted(token)
        assert (await first.close()).code is None
        assert (await second.close()).code is None


@pytest.mark.asyncio
async def test_s20a_two_maintenance_contenders(tmp_path):
    runtime = CMMSRuntime(tmp_path / "a", profile_id="a")
    await runtime.start()
    go = asyncio.Event()
    admitted = asyncio.Queue()
    outcomes = []

    async def contender():
        token = runtime.admit()
        await admitted.put(token)
        try:
            await go.wait()
            begin = required(runtime, "begin_maintenance")
            try:
                custody = begin(profile_id="a", canonical_root=runtime.root, deadline=time.monotonic() + 10)
            except Exception as exc:
                outcomes.append(exc.code)
            else:
                outcomes.append(custody)
        finally:
            runtime.release_admitted(token)

    tasks = [asyncio.create_task(contender()) for _ in range(2)]
    try:
        await admitted.get()
        await admitted.get()
        go.set()
        await asyncio.gather(*tasks)
        assert len(outcomes) == 2, "S20A_ONE_WINNER"
        assert sum(value == "E_MAINTENANCE_CONFLICT" for value in outcomes) == 1, "S20A_ONE_WINNER"
        assert sum(not isinstance(value, str) for value in outcomes) == 1, "S20A_ONE_WINNER"
    finally:
        go.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        assert (await runtime.close()).code is None


@pytest.mark.asyncio
async def test_s20a_drain_exact_admitted_self(tmp_path):
    runtime = CMMSRuntime(tmp_path / "a", profile_id="a")
    await runtime.start()
    release = asyncio.Event()
    entered = asyncio.Event()
    drained = asyncio.Event()
    custody_ready = asyncio.Event()

    async def prior_operation():
        token = runtime.admit()
        entered.set()
        try:
            await release.wait()
        finally:
            runtime.release_admitted(token)

    async def maintenance():
        token = runtime.admit()
        try:
            begin = required(runtime, "begin_maintenance")
            custody = begin(profile_id="a", canonical_root=runtime.root, deadline=time.monotonic() + 10)
            custody_ready.set()
            await runtime.drain_maintenance(custody)
            drained.set()
        finally:
            runtime.release_admitted(token)

    prior = asyncio.create_task(prior_operation())
    await entered.wait()
    job = asyncio.create_task(maintenance())
    try:
        ready = asyncio.create_task(custody_ready.wait())
        done, _ = await asyncio.wait({job, ready}, return_when=asyncio.FIRST_COMPLETED)
        if job in done:
            await job
        assert custody_ready.is_set(), "S20A_DRAIN"
        await asyncio.sleep(0)
        assert not drained.is_set(), "S20A_DRAIN: other admitted operation excluded"
        release.set()
        await asyncio.gather(prior, job)
        assert drained.is_set(), "S20A_DRAIN"
    finally:
        release.set()
        if not ready.done():
            ready.cancel()
        await asyncio.gather(prior, job, ready, return_exceptions=True)
        assert (await runtime.close()).code is None
