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
async def projection_owner(root, embedder=None, *, live_target=None, unavailable=frozenset()):
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
    async def live(owner):
        if live_target == "vector":
            from memory_server.providers.lancedb_provider import LanceDBProvider

            return LanceDBProvider(db_path=str(root / "vector"), native_submitter=owner.submit_native)
        graph = SimpleGraph()
        graph.save_snapshot(root / "graph.json")
        graph.load_snapshot(root / "graph.json")
        return graph

    layout = SimpleNamespace(data_root=root, vector=SimpleNamespace(backend="lancedb", local_path=root / "vector"),
                             graph_snapshot_path=root / "graph.json", unavailable_projections=unavailable)
    recipes = (("sqlite", sqlite),) + (((live_target, live),) if live_target is not None else ())
    runtime = CMMSRuntime(layout, profile_id="a", dependencies=RuntimeDependencies(
        embedder=embedder, recipes=recipes,
    ))
    await runtime.start()
    if live_target == "vector":
        await runtime.dependencies.vector._get_table()
    root.chmod(0o700)
    (root / "vector").mkdir(mode=0o700, exist_ok=True)
    (root / "vector").chmod(0o700)
    for item in (root / "vector").rglob("*"):
        item.chmod(0o700 if item.is_dir() else 0o600)
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


@pytest.mark.asyncio
async def test_s20a_snapshot_owner_wal(tmp_path, monkeypatch):
    import hashlib
    import json
    import shutil
    import sqlite3
    from pathlib import Path

    import aiosqlite
    from sqlalchemy import text

    from memory_server import projection_rebuild as p

    async with projection_owner(tmp_path / "a") as (runtime, custody):
        engine = runtime.dependencies.sqlite.engine
        async with engine.connect() as connection:
            raw = await connection.get_raw_connection()
            borrowed = raw.driver_connection
            await connection.execute(text("PRAGMA wal_autocheckpoint=0"))
            await connection.execute(text("UPDATE facts SET object='committed WAL payload' WHERE id='f'"))
            await connection.commit()
            rows = (await connection.execute(text("SELECT * FROM facts ORDER BY id"))).mappings().all()
            expected = [dict(row) for row in rows]
        live = runtime.root / "memory.db"
        wal = Path(str(live) + "-wal")
        assert wal.is_file() and wal.stat().st_size > 0, "S20A_SNAPSHOT: committed WAL missing"
        # An immutable reader deliberately ignores WAL: the committed value must
        # differ from the main-file-only view, or this would not prove backup.
        with sqlite3.connect(f"file:{live}?immutable=1", uri=True) as main_only:
            try:
                old = main_only.execute("SELECT object FROM facts WHERE id='f'").fetchone()
            except sqlite3.OperationalError:
                old = None
        assert old != ("committed WAL payload",), "S20A_SNAPSHOT: row already checkpointed"
        calls, constructors, observations = [], [], []
        original_backup = aiosqlite.Connection.backup
        original_connect = sqlite3.connect
        original_engine = p.create_async_engine
        original_observe = p._observe_snapshot

        async def backup(self, target, **kwargs):
            assert self is borrowed, "S20A_SNAPSHOT: backup did not borrow owner driver"
            calls.append("borrowed-backup")
            return await original_backup(self, target, **kwargs)

        def connect(database, *args, **kwargs):
            constructors.append(str(database))
            assert str(live) not in str(database), "S20A_SNAPSHOT: new live sqlite constructor"
            return original_connect(database, *args, **kwargs)

        def snapshot_engine(url, *args, **kwargs):
            constructors.append(str(url))
            assert str(live) not in str(url), "S20A_SNAPSHOT: new live SQLAlchemy engine"
            return original_engine(url, *args, **kwargs)

        def observe(url, observed, errors):
            original_observe(url, observed, errors)
            observations.append((observed.snapshot_integrity, observed.snapshot_revision, list(errors)))

        def no_copy(*args, **kwargs):
            pytest.fail("S20A_SNAPSHOT: source database/WAL file-copy attempted")

        with monkeypatch.context() as patch:
            patch.setattr(aiosqlite.Connection, "backup", backup)
            patch.setattr(sqlite3, "connect", connect)
            patch.setattr(p, "create_async_engine", snapshot_engine)
            patch.setattr(p, "_observe_snapshot", observe)
            for name in ("copyfile", "copy", "copy2"):
                patch.setattr(shutil, name, no_copy)
            snapshot = await p.snapshot_owned_projection_records(runtime, custody=custody)
            eligible = [record async for record in p.iter_canonical_projection_records(snapshot.snapshot_url,
                                                                                     batch_size=1)]
        assert calls == ["borrowed-backup"], "S20A_SNAPSHOT: real backup not executed"
        assert runtime.dependencies.sqlite.engine is engine, "S20A_SNAPSHOT: owner engine replaced"
        assert observations == [("ok", "0007", [])], "S20A_SNAPSHOT: integrity/revision"
        assert len(eligible) == 1 and eligible[0].record_id == "f", "S20A_SNAPSHOT: exact eligible ID"
        payload = dict(expected[0])
        payload.pop("id")
        assert dict(eligible[0].payload) == payload, "S20A_SNAPSHOT: exact eligible WAL content"
        assert tuple(eligible) == snapshot.records, "S20A_SNAPSHOT: owner/iterator content mismatch"
        destination = Path(snapshot.snapshot_url.removeprefix("sqlite+aiosqlite:///"))
        assert constructors and all(str(destination) in value for value in constructors), (
            "S20A_SNAPSHOT: constructors not confined to snapshot-only path"
        )
        assert any(value.startswith("sqlite+aiosqlite:") for value in constructors), "S20A_SNAPSHOT: iterator engine"
        assert hashlib.sha256(destination.read_bytes()).hexdigest() == snapshot.snapshot_digest, (
            "S20A_SNAPSHOT: independent snapshot file digest"
        )
        canonical = json.dumps(("fact", "f", payload), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        assert hashlib.sha256(canonical).hexdigest() == snapshot.content_digest, (
            "S20A_SNAPSHOT: independent content digest"
        )


async def seed_projection_corpus(runtime):
    from sqlalchemy import insert
    from storage.models.belief import BeliefORM
    from storage.models.decision import DecisionORM
    from storage.models.skill import SkillORM

    async with runtime.dependencies.sqlite.engine.begin() as connection:
        await connection.execute(insert(BeliefORM), [
            dict(id="b", proposition="belief content", lifecycle_state="active"),
            dict(id="bad-b", proposition="excluded belief", lifecycle_state="archived"),
        ])
        await connection.execute(insert(DecisionORM), [
            dict(id="d", choice="choose", reason="because", context="subject", dedup_key="d"),
            dict(id="bad-d", choice="", reason="because", context="subject", dedup_key="bad-d"),
        ])
        await connection.execute(insert(SkillORM), [
            dict(id="s", purpose="purpose", steps='["step"]'),
            dict(id="bad-s", purpose="excluded skill", steps='[]'),
        ])


@pytest.mark.asyncio
@pytest.mark.parametrize("selection", ["vector", "graph", "both", "empty"])
async def test_s20a_target_selective_native(tmp_path, monkeypatch, selection):
    import json

    import lancedb
    from sqlalchemy import text

    from memory_server import projection_rebuild as p
    from memory_server.providers.embedding_provider import MockEmbeddingProvider
    from memory_server.providers.lancedb_provider import LanceDBProvider

    embedder = MockEmbeddingProvider()
    targets = frozenset({selection}) if selection in {"vector", "graph"} else frozenset({"vector", "graph"})
    async with projection_owner(tmp_path / "a", embedder) as (runtime, custody):
        await seed_projection_corpus(runtime)
        if selection == "empty":
            async with runtime.dependencies.sqlite.engine.begin() as connection:
                for table in ("beliefs", "decisions", "facts", "skills"):
                    await connection.execute(text(f"DELETE FROM {table}"))
        snapshot = await p.snapshot_owned_projection_records(runtime, custody=custody)
        assert len(snapshot.records) == (0 if selection == "empty" else 7), "S20A_TARGETS: real SQL corpus"
        expected = p.expected_projection(snapshot.records)
        assert expected.eligible_counts == ({} if selection == "empty" else
                                           {"belief": 1, "decision": 1, "fact": 1, "skill": 1}), (
            "S20A_TARGETS: ineligible rows included"
        )
        if selection != "empty":
            async with runtime.dependencies.sqlite.engine.begin() as connection:
                await connection.execute(text("UPDATE facts SET object='changed only payload' WHERE id='f'"))
            changed = await p.snapshot_owned_projection_records(runtime, custody=custody)
            assert [(r.record_type, r.record_id) for r in changed.records] == [
                (r.record_type, r.record_id) for r in snapshot.records
            ], "S20A_TARGETS: payload-only control IDs changed"
            assert changed.content_digest != snapshot.content_digest, "S20A_TARGETS: content digest ignores payload"
        paths = {"vector": runtime.root / "vector", "graph": runtime.root / "graph.json"}
        before = {key: artifact_identity(path) for key, path in paths.items()}
        calls, vector_constructors, mappings = [], [], []
        original_embed, original_connect = embedder.embed_batch, lancedb.connect
        original_init, original_map = LanceDBProvider.__init__, p.map_projection_record

        def embed(texts):
            calls.append(list(texts))
            return original_embed(texts)

        def connect(*args, **kwargs):
            vector_constructors.append("lancedb.connect")
            return original_connect(*args, **kwargs)

        def provider_init(self, *args, **kwargs):
            vector_constructors.append("LanceDBProvider")
            original_init(self, *args, **kwargs)

        def mapping(record):
            mappings.append((record.record_type, record.record_id))
            return original_map(record)

        def forbidden(*args, **kwargs):
            pytest.fail("S20A_TARGETS: legacy rebuild/embedding-plan digest used")

        with monkeypatch.context() as patch:
            patch.setattr(embedder, "embed_batch", embed)
            patch.setattr(lancedb, "connect", connect)
            patch.setattr(LanceDBProvider, "__init__", provider_init)
            patch.setattr(p, "map_projection_record", mapping)
            patch.setattr(p, "rebuild_projections", forbidden)
            patch.setattr(p, "embedding_plan_digest", forbidden)
            result = await p.rebuild_owned_projections(snapshot, custody=custody, targets=targets)
        for key in paths.keys() - targets:
            assert artifact_identity(paths[key]) == before[key], "S20A_TARGETS: nonselected bytes/identity changed"
        if selection == "graph":
            assert calls == [] and vector_constructors == [], "S20A_TARGETS: graph-only embed/vector constructor"
        if "vector" in targets:
            rows = original_connect(str(paths["vector"])).open_table("memories").to_arrow().to_pylist()
            wanted = {original_map(r).point_id: dict(original_map(r).payload) for r in snapshot.records
                      if p.is_eligible(r) and r.record_type in {"fact", "belief"}}
            assert {row["id"]: json.loads(row["_metadata"]) for row in rows} == wanted, (
                "S20A_TARGETS: actual additive stage does not match frozen mapping"
            )
            assert len(rows) == len(wanted), "S20A_TARGETS: duplicate vector rows"
            assert set(mappings) == ({("belief", "b"), ("fact", "f")} if wanted else set()), (
                "S20A_TARGETS: frozen vector mapping boundary not crossed"
            )
        if "graph" in targets:
            graph = json.loads(paths["graph"].read_bytes())
            assert {(n["id"], n["type"]) for n in graph["nodes"].values()} == set(expected.graph_nodes), (
                "S20A_TARGETS: disk graph node/type mapping"
            )
            assert {(e["source_id"], e["target_id"], e["relation"]) for e in graph["edges"]} == set(
                expected.graph_edges
            ), "S20A_TARGETS: disk graph edge mapping"
        assert result.eligible_counts == expected.eligible_counts, "S20A_TARGETS: result counts"
        assert runtime._maintenance_state == "blocked", "S20A_TARGETS: publication unlocked owner"


@pytest.mark.asyncio
@pytest.mark.parametrize("case", [
    "vector-shape", "vector-id", "vector-payload", "vector-duplicate",
    "graph-type", "graph-edge", "valid", "open-vector", "open-graph",
])
async def test_s20a_selected_verification_real_artifacts(tmp_path, monkeypatch, case):
    import hashlib
    import json
    import os

    import lancedb
    import pyarrow as pa

    from memory_server import projection_rebuild as p
    from memory_server.providers.embedding_provider import MockEmbeddingProvider

    # Required-field schema identity is deliberately left to the existing T02
    # regression. Here schema corruption changes the actual vector dimension.
    live_target = case.removeprefix("open-") if case.startswith("open-") else None
    async with projection_owner(tmp_path / "a", MockEmbeddingProvider(), live_target=live_target) as (runtime, custody):
        snapshot = await p.snapshot_owned_projection_records(runtime, custody=custody)
        paths = {"vector": runtime.root / "vector", "graph": runtime.root / "graph.json"}
        before = {target: artifact_identity(path) for target, path in paths.items()}
        stage_state, events, renames = {}, [], []
        original_stage, original_reader = p._owned_stage, p._owned_vector_read
        original_graph, original_rename = p._observe_graph_artifact, os.rename

        def stage(path, records, targets, embedder, dimension, end):
            original_stage(path, records, targets, embedder, dimension, end)
            if case.startswith("vector-"):
                database = lancedb.connect(str(path / "vector"))
                table = database.open_table("memories")
                rows, schema = table.to_arrow().to_pylist(), table.schema
                if case == "vector-shape":
                    schema = pa.schema([("id", pa.string()), ("vector", pa.list_(pa.float32(), dimension - 1)),
                                        ("_metadata", pa.string())])
                    rows[0]["vector"] = rows[0]["vector"][:-1]
                elif case == "vector-id":
                    rows[0]["id"] = "unexpected-vector-id"
                elif case == "vector-payload":
                    payload = json.loads(rows[0]["_metadata"])
                    payload["object"] = "corrupt payload"
                    rows[0]["_metadata"] = json.dumps(payload)
                else:
                    rows.append(dict(rows[0]))
                database.drop_table("memories")
                database.create_table("memories", schema=schema).add(rows)
            elif case.startswith("graph-"):
                data = json.loads((path / "graph").read_bytes())
                if case == "graph-type":
                    next(iter(data["nodes"].values()))["type"] = "skill"
                else:
                    data["edges"][0]["relation"] = "corrupt-relation"
                (path / "graph").write_text(json.dumps(data))
            for item in path.rglob("*"):
                item.chmod(0o700 if item.is_dir() else 0o600)
            stage_state.update(path=path, identity={target: artifact_identity(path / target) for target in targets})

        def reader(*args):
            events.append("bounded-owner-vector")
            return original_reader(*args)

        def graph(*args):
            events.append("shared-graph-observer")
            return original_graph(*args)

        def rename(*args, **kwargs):
            renames.append((args, kwargs))
            return original_rename(*args, **kwargs)

        def forbidden(*args, **kwargs):
            pytest.fail("S20A_VERIFY: vector MUST use bounded owner verifier; legacy observer forbidden (RULINGS_010)")

        with monkeypatch.context() as patch:
            patch.setattr(p, "_owned_stage", stage)
            patch.setattr(p, "_owned_vector_read", reader)
            patch.setattr(p, "_observe_graph_artifact", graph)
            patch.setattr(p, "_observe_vector_artifact", forbidden)
            patch.setattr(os, "rename", rename)
            caught, result = None, None
            try:
                result = await p.rebuild_owned_projections(snapshot, custody=custody,
                                                         targets=frozenset({"vector", "graph"}))
            except p.MaintenanceError as exc:
                caught = exc
        run = runtime.root / p._OWNED_DIRECTORY / snapshot.run_id
        journal = json.loads((run / "journal.json").read_bytes())
        if live_target is not None:
            assert caught is not None and caught.code == "E_MAINTENANCE_CONFLICT", "S20A_VERIFY: open destination"
            assert not stage_state and events == [] and journal["phase"] == "captured", (
                "S20A_VERIFY: open/live provider reached stage/verifiers"
            )
        else:
            assert events.count("bounded-owner-vector") == 1, (
                "S20A_VERIFY: mandatory vector half MUST cross bounded owner verifier, never legacy vector observer"
            )
            if not case.startswith("vector-"):
                assert events == ["bounded-owner-vector", "shared-graph-observer"], (
                    "S20A_VERIFY: mandatory graph half MUST cross shared _observe_graph_artifact (RULINGS_010)"
                )
            if case == "valid":
                assert caught is None and journal["phase"] == "published", "S20A_VERIFY: positive publication"
                assert len(renames) == 4, "S20A_VERIFY: actual whole selected rename"
                for target, path in paths.items():
                    assert artifact_identity(path) == stage_state["identity"][target], (
                        "S20A_VERIFY: independently read published disk differs from staged bytes/identity"
                    )
                rows = lancedb.connect(str(paths["vector"])).open_table("memories").to_arrow().to_pylist()
                data = json.loads(paths["graph"].read_bytes())
                def digest(values):
                    return hashlib.sha256("\n".join(sorted(values)).encode()).hexdigest()
                assert result.vector_ids_digest == digest({row["id"] for row in rows}), (
                    "S20A_VERIFY: published vector digest must equal independent disk read, not result.valid"
                )
                assert result.graph_nodes_digest == digest({node["id"] for node in data["nodes"].values()}), (
                    "S20A_VERIFY: published graph node digest independent read"
                )
                assert result.graph_edges_digest == digest({
                    f"{edge['source_id']}|{edge['target_id']}|{edge['relation']}" for edge in data["edges"]
                }), "S20A_VERIFY: published graph edge digest independent read"
            else:
                assert caught is not None and caught.code == (
                    "E_VECTOR_BACKEND" if case.startswith("vector-") else "E_GRAPH_BACKEND"
                ), "S20A_VERIFY: bounded vector/shared graph corruption not rejected"
                assert journal["phase"] == "stage", "S20A_VERIFY: corrupt artifact reached publication"
                for target in paths:
                    assert artifact_identity(stage_state["path"] / target) == stage_state["identity"][target], (
                        "S20A_VERIFY: verifier mutated staged bytes/identity"
                    )
        if case != "valid":
            assert renames == [] and not list(run.glob("old-*")), "S20A_VERIFY: rejected artifact renamed"
            for target, path in paths.items():
                assert artifact_identity(path) == before[target], "S20A_VERIFY: rejected artifact changed live bytes"
        if live_target is None:
            assert runtime._maintenance_state == "blocked", "S20A_VERIFY: verifier/publication lost fence"
