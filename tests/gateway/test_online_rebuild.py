"""S20A1 owner fence regressions, using real runtime admission and EX locks."""

import asyncio
import contextlib
import inspect
import json
import logging
import os
import shutil
import time
from types import SimpleNamespace

import pytest

from memory_server import projection_rebuild as projections
from memory_server.models import Fact
from memory_server.providers.embedding_provider import MockEmbeddingProvider, OpenAIEmbeddingProvider
from memory_server.runtime import runtime as runtime_module
from memory_server.runtime.runtime import CMMSRuntime


@contextlib.asynccontextmanager
async def projection_owner(root, embedder=None):
    assert callable(getattr(projections, "snapshot_owned_projection_records", None)), "S20A_SURFACE: snapshot owner"
    from memory_server.providers.sqlite_provider import SQLiteProvider
    from memory_server.runtime.dependencies import RuntimeDependencies

    async def sqlite(owner):
        provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{owner.root / 'memory.db'}")
        await provider.initialize()
        return provider

    from alembic import command
    from alembic.config import Config

    root.mkdir(mode=0o700, parents=True)
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", f"sqlite:///{root / 'memory.db'}")
    command.upgrade(config, "head")
    runtime = CMMSRuntime(root, profile_id="a", dependencies=RuntimeDependencies(recipes=(("sqlite", sqlite),)))
    await runtime.start()
    runtime.layout = SimpleNamespace(vector=SimpleNamespace(backend="lancedb", local_path=root / "vector"),
                                     graph_snapshot_path=root / "graph.json")
    runtime.dependencies = runtime.dependencies.with_resource("embedder", embedder or MockEmbeddingProvider())
    (root / "vector").mkdir(mode=0o700)
    projections.SimpleGraph().save_snapshot(root / "graph.json")
    (root / "graph.json").chmod(0o600)
    await runtime.dependencies.sqlite.create_fact(Fact(
        id="s20-fact", subject="local", predicate="is", object="offline", confidence=0.9, source="seed",
    ))
    token = runtime.admit()
    try:
        custody = runtime.begin_maintenance(profile_id="a", canonical_root=root, deadline=time.monotonic() + 30)
        await runtime.drain_maintenance(custody)
        yield runtime, custody
    finally:
        runtime.release_admitted(token)
        assert (await runtime.close()).code is None


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["snapshot", "rebuild"])
async def test_s20a_public_storage_error_no_absolute_path(tmp_path, caplog, operation):
    root = tmp_path / "S20_CANARY_ABSOLUTE_PATH"
    async with projection_owner(root) as (runtime, custody):
        snapshot = None
        if operation == "rebuild":
            snapshot = await projections.snapshot_owned_projection_records(runtime, custody=custody)
        logger = logging.getLogger(projections.__name__)
        logger.addHandler(caplog.handler)  # Alembic fileConfig replaces pytest's root capture handler.
        moved = tmp_path / "moved"
        root.rename(moved)
        root.symlink_to(moved, target_is_directory=True)
        try:
            with pytest.raises(Exception) as rejected:
                if operation == "snapshot":
                    await projections.snapshot_owned_projection_records(runtime, custody=custody)
                else:
                    await projections.rebuild_owned_projections(snapshot, custody=custody, targets=frozenset({"graph"}))
            error = rejected.value
            public = repr(vars(error)) + repr(error.args) + str(error)
            assert not any(s in public for s in (str(tmp_path), "S20_CANARY", "/")), "S20A_CANARY_NO_ABSOLUTE_PATH"
            assert isinstance(error, runtime_module.MaintenanceError), "S20A_CANARY_DOMAIN_ERROR"
            assert error.code == "E_STORAGE", "S20A_CANARY_STORAGE_CODE"
            assert error.__cause__ is None and error.__context__ is None, "S20A_CANARY_NO_EXCEPTION_CHAIN"
            warnings = [record for record in caplog.records if record.name == projections.__name__]
            assert warnings and all(record.exc_info is None for record in warnings), "S20A_CANARY_REDACTED_LOG"
            for record in caplog.records:
                assert "S20_CANARY" not in repr(vars(record)), "S20A_CANARY_LOG_RECORD"
        finally:
            logger.removeHandler(caplog.handler)
            root.unlink()
            moved.rename(root)


@pytest.mark.asyncio
@pytest.mark.parametrize("remote", [False, True], ids=["local", "remote"])
async def test_s20a_network_false_classifies_captured_embedder(tmp_path, remote):
    embedder = (OpenAIEmbeddingProvider(base_url="https://s20-canary.invalid/v1", api_key="canary")
                if remote else MockEmbeddingProvider())
    async with projection_owner(tmp_path / "a", embedder) as (runtime, custody):
        snapshot = await projections.snapshot_owned_projection_records(runtime, custody=custody)
        if remote:
            with pytest.raises(runtime_module.MaintenanceError) as rejected:
                await projections.rebuild_owned_projections(
                    snapshot, custody=custody, targets=frozenset({"vector"}), allow_network_embedding=False)
            assert rejected.value.code == "E_CAPABILITY_UNAVAILABLE", "S20A_NETWORK_FALSE_REMOTE"
            assert embedder._client is None, "S20A_NETWORK_FALSE_NO_CLIENT"
        else:
            result = await projections.rebuild_owned_projections(
                snapshot, custody=custody, targets=frozenset({"vector"}), allow_network_embedding=False)
            assert result.eligible_counts["fact"] == 1 and embedder._cache, "S20A_NETWORK_FALSE_LOCAL"
            assert (runtime.root / "vector").is_dir(), "S20A_NETWORK_FALSE_LOCAL_PUBLISHED"


@pytest.mark.asyncio
@pytest.mark.parametrize("hours,phase,deleted", [
    (25, "reconciled", True), (27, "reconciled", True), (24, "reconciled", False),
    (23, "reconciled", False), (25, "captured", False),
])
async def test_s20a_retention_age_latest_and_lock(tmp_path, hours, phase, deleted):
    async with projection_owner(tmp_path / "a") as (runtime, custody):
        snapshot = await projections.snapshot_owned_projection_records(runtime, custody=custody)
        parent = runtime.root / ".projection-rebuild"
        older = parent / snapshot.run_id
        latest = parent / ("b" * 32)
        shutil.copytree(older, latest)
        now = float(int(time.time()))
        for path, age, status in ((older, hours, phase), (latest, hours - 1, "reconciled")):
            state = json.loads((path / "journal.json").read_text())
            state.update(run=path.name, phase=status)
            (path / "journal.json").write_text(json.dumps(state))
            timestamp = now - age * 3600
            os.utime(path / "snapshot.db", (timestamp, timestamp))
        cleanup = getattr(projections, "_owned_cleanup", None)
        assert callable(cleanup), "S20A_RETENTION: missing age cleanup"
        with pytest.raises(runtime_module.MaintenanceError):
            cleanup(runtime, None)
        assert older.exists() and latest.exists(), "S20A_NO_CLEANUP_WITHOUT_LOCK"
        # Exercise actual snapshot-triggered cleanup; use fixed time only for the exact-age boundary.
        if deleted:
            await projections.snapshot_owned_projection_records(runtime, custody=custody)
            assert len(list(parent.iterdir())) == 2, "S20A_CLEANUP_UNDER_LOCK_GENERATION_CAP"
        else:
            cleanup(runtime, custody, now=now)
        assert older.exists() is not deleted, "S20A_RETENTION_24H_COMPLETED"
        assert latest.exists(), "S20A_RETENTION_LATEST"


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
