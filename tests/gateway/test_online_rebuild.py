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


def test_s20a_surface_required():
    import __future__

    import ast
    import subprocess
    from pathlib import Path

    from storage.outbox_worker import OutboxWorker

    from memory_server import projection_rebuild as projections
    from memory_server.runtime import runtime as runtime_module

    for owner, names in (
        (CMMSRuntime, ("begin_maintenance", "drain_maintenance", "check_maintenance", "finish_maintenance")),
        (OutboxWorker, ("_pause_for_maintenance",)),
        (projections, ("snapshot_owned_projection_records", "rebuild_owned_projections")),
    ):
        for name in names:
            assert callable(getattr(owner, name, None)), f"S20A_SURFACE: {owner}.{name}"

    # Signature-only BASE declarations: do not import/execute another runtime,
    # register duplicate metrics, or treat the surface as behavioural evidence.
    root = Path(__file__).resolve().parents[2]
    base = "9b07be1262cc7b1185bd97596a934f998ad592d2"
    for module, path, class_name, names in (
        (projections, "src/memory_server/projection_rebuild.py", None, (
            "rebuild_projections", "iter_canonical_projection_records",
            "verify_staged_projections", "verify_published_projections",
        )),
        (runtime_module, "src/memory_server/runtime/runtime.py", "CMMSRuntime", (
            "configure_startup_recovery", "admit", "release_admitted", "register_admitted",
            "spawn_owned_task", "submit_native",
        )),
    ):
        source = subprocess.check_output(["git", "show", f"{base}:{path}"], cwd=root)
        tree = ast.parse(source)
        parent = tree if class_name is None else next(
            node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name
        )
        owner = module if class_name is None else getattr(module, class_name)
        for name in names:
            declarations = [node for node in parent.body if isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name]
            assert len(declarations) == 1, f"S20A_SURFACE: BASE declaration {name}"
            declaration = declarations[0]
            declaration.body = [ast.Pass()]
            declaration.decorator_list = []
            stub = ast.fix_missing_locations(ast.Module(body=[declaration], type_ignores=[]))
            namespace = dict(vars(module))
            exec(compile(stub, "<BASE-signature>", "exec",
                         flags=__future__.annotations.compiler_flag, dont_inherit=True), namespace)
            assert inspect.signature(getattr(owner, name)) == inspect.signature(namespace[name]), (
                f"S20A_SURFACE: strict legacy signature {name}"
            )
    assert "job_token" not in inspect.signature(CMMSRuntime.begin_maintenance).parameters, "S20A_SURFACE"
    assert not hasattr(OutboxWorker, "pause") and not hasattr(OutboxWorker, "resume"), "S20A_SURFACE"


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


@asynccontextmanager
async def started_worker_owner(root):
    from sqlalchemy import text
    from storage.outbox_worker import OutboxWorker

    from memory_server.providers.graph_provider import SimpleGraph
    from memory_server.providers.lancedb_provider import LanceDBProvider
    from memory_server.providers.sqlite_provider import SQLiteProvider
    from memory_server.runtime.dependencies import RuntimeDependencies

    async def sqlite(owner):
        provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{root / 'memory.db'}")
        await provider.initialize()
        async with provider.engine.begin() as connection:
            await connection.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)"))
            await connection.execute(text("INSERT INTO alembic_version VALUES ('0007')"))
        return provider

    async def worker(owner):
        vector = LanceDBProvider(db_path=str(root / "vector"), native_submitter=owner.submit_native)
        result = OutboxWorker(engine=owner.dependencies.sqlite.engine, qdrant=vector,
                              task_spawner=owner.spawn_owned_task, native_submitter=owner.submit_native,
                              poll_interval_seconds=0.01, compact_interval_seconds=0)
        await result.initialize()
        return result

    layout = SimpleNamespace(data_root=root, vector=SimpleNamespace(backend="lancedb", local_path=root / "vector"),
                             graph_snapshot_path=root / "graph.json")
    runtime = CMMSRuntime(layout, profile_id="a", dependencies=RuntimeDependencies(
        recipes=(("sqlite", sqlite), ("outbox_worker", worker)),
    ))
    await runtime.start()
    root.chmod(0o700)
    (root / "vector").mkdir(mode=0o700)
    SimpleGraph().save_snapshot(root / "graph.json")
    (root / "graph.json").chmod(0o600)
    try:
        yield runtime
    finally:
        runtime.dependencies.outbox_worker.stop()
        assert (await runtime.close()).code is None, "S20A_DRAIN: terminal cleanup"


async def wait_observed(predicate, marker):
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.005)
    assert predicate(), marker


@pytest.mark.asyncio
async def test_s20a_drain_exact_job_token(tmp_path, monkeypatch):
    import threading

    from memory_server import projection_rebuild as p

    async with started_worker_owner(tmp_path / "a") as runtime:
        worker = runtime.dependencies.outbox_worker
        provider = worker._qdrant
        # The actual provider constructs a real Lance table before the hold.
        table = await provider._get_table()
        compact_entered, compact_release = asyncio.Event(), asyncio.Event()
        native_entered, native_release = threading.Event(), threading.Event()
        original_run = provider._run

        async def held_optimize(function, *args, **kwargs):
            if kwargs.get("kind") == "lancedb-optimize":
                compact_entered.set()
                await compact_release.wait()
            return await original_run(function, *args, **kwargs)

        def held_native():
            native_entered.set()
            assert native_release.wait(10), "S20A_DRAIN: native release timeout"
            return table.count_rows()

        # A bootstrap host and another admitted operation must both drain;
        # neither may be confused with the admitted maintenance child itself.
        bootstrap = runtime.admit()
        other_ready, other_release = asyncio.Event(), asyncio.Event()
        child_ready, snapshot_done = asyncio.Event(), asyncio.Event()
        observed = {}

        async def other_operation():
            token = runtime.admit()
            other_ready.set()
            try:
                await other_release.wait()
            finally:
                runtime.release_admitted(token)

        async def job():
            token = runtime.admit()
            try:
                custody = runtime.begin_maintenance(profile_id="a", canonical_root=runtime.root,
                                                    deadline=time.monotonic() + 20)
                observed["custody"] = custody
                child_ready.set()
                await runtime.drain_maintenance(custody, worker_task=parent)
                observed["snapshot"] = await p.snapshot_owned_projection_records(runtime, custody=custody)
                snapshot_done.set()
            finally:
                runtime.release_admitted(token)

        parent = other = child = None
        handle = None
        try:
            with monkeypatch.context() as patch:
                patch.setattr(provider, "_run", held_optimize)
                await worker._maybe_compact()
                await asyncio.wait_for(compact_entered.wait(), 5)
                compact = worker._compact_task
                assert worker._active_claim is not None and not compact.done(), "S20A_DRAIN: real compact claim"
                handle = await runtime.submit_native(held_native, kind="drain-test-held-io")
                await wait_observed(native_entered.is_set, "S20A_DRAIN: native not entered")
                parent = runtime.spawn_owned_task(worker.run, kind="daemon-worker-parent")
                other = asyncio.create_task(other_operation())
                await other_ready.wait()
                child = asyncio.create_task(job())
                await child_ready.wait()
                await wait_observed(lambda: worker._maintenance_paused, "S20A_DRAIN: not paused")
                assert observed["custody"].self_task is child and child in runtime._admitted, "S20A_DRAIN: exact token"
                assert bootstrap in runtime._admitted and other in runtime._admitted, "S20A_DRAIN: real admissions"
                assert not snapshot_done.is_set(), "S20A_DRAIN: snapshot before compact/native/bootstrap drain"
                compact_release.set()
                await compact
                for item in (runtime.root / "vector").rglob("*"):
                    item.chmod(0o700 if item.is_dir() else 0o600)
                assert not snapshot_done.is_set(), "S20A_DRAIN: held native excluded"
                native_release.set()
                await handle.result()
                assert await handle.drain(time.monotonic() + 5), "S20A_DRAIN: native not settled"
                await asyncio.sleep(0.03)
                assert not snapshot_done.is_set(), "S20A_DRAIN: bootstrap/other excluded"
                runtime.release_admitted(bootstrap)
                await asyncio.sleep(0.03)
                assert not snapshot_done.is_set(), "S20A_DRAIN: other admitted task excluded"
                other_release.set()
                await asyncio.wait_for(asyncio.gather(other, child), 5)
                assert snapshot_done.is_set() and not parent.done(), "S20A_DRAIN: self excluded, persistent parent kept"
                assert worker._maintenance_pause_ack and worker._active_claim is None, "S20A_DRAIN: pause/claim"
                assert not runtime._live_natives(runtime.owned_snapshot()[2]), "S20A_DRAIN: native registry"
                assert observed["snapshot"].records == (), "S20A_DRAIN: real H03 snapshot"
        finally:
            native_release.set()
            compact_release.set()
            other_release.set()
            runtime.release_admitted(bootstrap)
            worker.stop()
            await asyncio.gather(*(t for t in (other, child, parent, worker._compact_task) if t is not None),
                                 return_exceptions=True)
            if handle is not None:
                await handle.drain(time.monotonic() + 5)


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["none", "foreign", "sibling", "done", "exact"])
async def test_s20a_drain_worker_identity(tmp_path, identity):
    from storage.outbox_worker import OutboxWorker

    async with started_worker_owner(tmp_path / "a") as runtime:
        worker = runtime.dependencies.outbox_worker
        token = runtime.admit()
        parent = candidate = holder = releaser = None
        release, entered = asyncio.Event(), asyncio.Event()
        sibling = None

        async def foreign():
            await release.wait()

        async def hold_claim():
            async with worker.claim_scope(deadline=time.monotonic() + 10):
                entered.set()
                await release.wait()

        try:
            assert "job_token" not in inspect.signature(runtime.begin_maintenance).parameters, "S20A_WORKER_IDENTITY"
            if identity == "exact":
                holder = runtime.spawn_owned_task(hold_claim, kind="identity-held-claim")
                await entered.wait()
            if identity == "done":
                worker.stop()
            parent = runtime.spawn_owned_task(worker.run, kind="daemon-worker-parent")
            if identity == "done":
                await parent
            candidate = parent
            if identity == "none":
                candidate = None
            elif identity == "foreign":
                candidate = runtime.spawn_owned_task(foreign, kind="daemon-worker-parent")
            elif identity == "sibling":
                sibling = OutboxWorker(engine=worker._engine, task_spawner=runtime.spawn_owned_task,
                                       native_submitter=runtime.submit_native, poll_interval_seconds=0.01)
                await sibling.initialize()
                candidate = runtime.spawn_owned_task(sibling.run, kind="daemon-worker-parent")
            custody = runtime.begin_maintenance(profile_id="a", canonical_root=runtime.root,
                                                deadline=time.monotonic() + (0.2 if identity == "none" else 10))
            assert custody.self_task is asyncio.current_task() is token, "S20A_WORKER_IDENTITY: admitted current task"

            async def release_after_ack():
                await wait_observed(lambda: worker._maintenance_pause_ack, "S20A_WORKER_IDENTITY: missing ack")
                assert worker._active_claim is not None and not holder.done(), "S20A_WORKER_IDENTITY: hold not real"
                assert runtime._maintenance_state == "draining", "S20A_WORKER_IDENTITY: early active"
                release.set()

            if identity == "exact":
                releaser = asyncio.create_task(release_after_ack())
                await runtime.drain_maintenance(custody, worker_task=candidate)
                await asyncio.gather(holder, releaser)
                assert worker._maintenance_pause_ack and not parent.done(), "S20A_WORKER_IDENTITY: persistent parent"
                assert worker._active_claim is None and not worker._retained_claims, "S20A_WORKER_IDENTITY: claims"
                assert not worker._claim_lock.locked(), "S20A_WORKER_IDENTITY: claim lock"
                assert worker._compact_task is None or worker._compact_task.done(), "S20A_WORKER_IDENTITY: compact"
                assert not runtime._live_natives(runtime.owned_snapshot()[2]), "S20A_WORKER_IDENTITY: native"
                assert runtime._maintenance_state == "active", "S20A_WORKER_IDENTITY: exact parent did not drain"
            else:
                with pytest.raises(Exception) as rejected:
                    await runtime.drain_maintenance(custody, worker_task=candidate)
                assert rejected.value.code == ("E_TIMEOUT" if identity == "none" else "E_MAINTENANCE_CONFLICT"), (
                    "S20A_WORKER_IDENTITY: wrong rejection"
                )
                assert runtime._maintenance_state != "active", "S20A_WORKER_IDENTITY: invalid parent excluded"
                if identity == "none":
                    assert worker._maintenance_pause_ack and not parent.done(), (
                        "S20A_WORKER_IDENTITY: None excludes parent"
                    )
            assert not (runtime.root / ".projection-rebuild").exists(), "S20A_WORKER_IDENTITY: unexpected snapshot"
        finally:
            release.set()
            worker.stop()
            if sibling is not None:
                sibling.stop()
            await asyncio.gather(*(t for t in (parent, candidate, holder, releaser) if t is not None),
                                 return_exceptions=True)
            runtime.release_admitted(token)


def artifact_identity(path):
    import hashlib

    return {
        str(item.relative_to(path)): (
            item.stat().st_dev, item.stat().st_ino,
            hashlib.sha256(item.read_bytes()).hexdigest() if item.is_file() else None,
        ) for item in [path, *sorted(path.rglob("*"))]
    }


@pytest.mark.asyncio
async def test_s20a_finish_x10_no_unlock_or_republish(tmp_path, monkeypatch):
    import json
    import os

    from memory_server import projection_rebuild as p

    async with started_worker_owner(tmp_path / "a") as runtime:
        async with projection_owner(tmp_path / "b") as (foreign, foreign_custody):
            worker = runtime.dependencies.outbox_worker
            token = runtime.admit()
            parent = runtime.spawn_owned_task(worker.run, kind="daemon-worker-parent")
            custody = runtime.begin_maintenance(profile_id="a", canonical_root=runtime.root,
                                                deadline=time.monotonic() + 20)
            lock, fh = runtime.lock, runtime.lock._fh
            lock_identity = os.fstat(fh)
            renames = []
            original_rename = os.rename

            def observed_rename(*args, **kwargs):
                renames.append((args, kwargs))
                return original_rename(*args, **kwargs)

            try:
                await runtime.drain_maintenance(custody, worker_task=parent)
                snapshot = await p.snapshot_owned_projection_records(runtime, custody=custody)
                run = runtime.root / p._OWNED_DIRECTORY / snapshot.run_id
                with monkeypatch.context() as patch:
                    patch.setattr(os, "rename", observed_rename)
                    await p.rebuild_owned_projections(snapshot, custody=custody, targets=frozenset({"graph"}))
                    assert len(renames) == 2, "S20A_FINISH_NO_UNLOCK: real publication renames"
                    journal = (run / "journal.json").read_bytes()
                    assert json.loads(journal)["phase"] == "published", "S20A_FINISH_NO_UNLOCK: publication"
                    before = artifact_identity(runtime.root / "graph.json")
                    for _ in range(10):
                        with pytest.raises(p.MaintenanceError) as rejected:
                            runtime.finish_maintenance(custody)
                        assert rejected.value.code == "E_MAINTENANCE_CONFLICT", "S20A_FINISH_NO_UNLOCK: finish"
                        with pytest.raises(p.MaintenanceError) as admission:
                            runtime.admit()
                        assert admission.value.code == "E_MAINTENANCE_CONFLICT", "S20A_FINISH_NO_UNLOCK: admission"
                        assert worker._maintenance_paused and not parent.done(), "S20A_FINISH_NO_UNLOCK: worker"
                        assert runtime.lock is lock and lock._fh is fh, "S20A_FINISH_NO_UNLOCK: EX replaced"
                        assert os.fstat(fh) == lock_identity, "S20A_FINISH_NO_UNLOCK: EX identity"
                        assert (run / "journal.json").read_bytes() == journal, "S20A_FINISH_NO_UNLOCK: journal mutation"
                        assert artifact_identity(runtime.root / "graph.json") == before, "S20A_FINISH_NO_UNLOCK: live"
                        assert len(renames) == 2, "S20A_FINISH_NO_UNLOCK: republished"
                    with pytest.raises(p.MaintenanceError):
                        runtime.finish_maintenance(foreign_custody)

                    async def wrong_task():
                        with pytest.raises(p.MaintenanceError) as rejected:
                            runtime.finish_maintenance(custody)
                        assert rejected.value.code == "E_RUNTIME_NOT_READY", "S20A_FINISH_NO_UNLOCK: foreign task"

                    await asyncio.create_task(wrong_task())
                    with monkeypatch.context() as clock_patch:
                        clock_patch.setattr(runtime, "_clock", lambda: custody._identity[-1] + 1)
                        with pytest.raises(p.MaintenanceError) as expired:
                            runtime.finish_maintenance(custody)
                        assert expired.value.code == "E_TIMEOUT", "S20A_FINISH_NO_UNLOCK: expired"
                    runtime.release_admitted(token)
                    with pytest.raises(p.MaintenanceError) as stale:
                        runtime.finish_maintenance(custody)
                    assert stale.value.code == "E_RUNTIME_NOT_READY", "S20A_FINISH_NO_UNLOCK: released custody"
                    runtime._begin_close()
                    assert runtime.state == "CLOSING", "S20A_FINISH_NO_UNLOCK: real closing"
                    with pytest.raises(p.MaintenanceError) as closing:
                        runtime.finish_maintenance(custody)
                    assert closing.value.code == "E_RUNTIME_NOT_READY", "S20A_FINISH_NO_UNLOCK: CLOSING"
                    assert len(renames) == 2 and (run / "journal.json").read_bytes() == journal, (
                        "S20A_FINISH_NO_UNLOCK: invalid custody caused publication"
                    )
                    assert worker._maintenance_paused and runtime.lock is lock, "S20A_FINISH_NO_UNLOCK: fence lost"
            finally:
                runtime.release_admitted(token)
                worker.stop()
                await asyncio.gather(parent, return_exceptions=True)
            assert foreign.state == "READY", "S20A_FINISH_NO_UNLOCK: foreign owner changed"
