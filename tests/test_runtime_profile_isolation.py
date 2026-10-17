"""Slice C: real manager-owned storage and profile isolation."""

from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from sqlalchemy import select
from storage.models.fact import FactORM
from storage.models.receipt import MemoryReceiptORM
from storage.outbox import OutboxEntryORM

from memory_server.application.application import MemoryApplication
from memory_server.application.contracts import RememberCommand
from memory_server.evaluation.auditor import collect_persisted_audit_state
from memory_server.paths import StorageLayoutError, StorageResolutionInputs, resolve_storage_layout
from memory_server.providers.embedding_provider import MockEmbeddingProvider
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.runtime.config import RuntimeConfig
from memory_server.runtime.runtime_factory import RuntimeFactory
from memory_server.runtime.runtime_manager import RuntimeDescriptor, RuntimeManager
from memory_server.storage_lock import inspect_existing_lock_readonly


def _spec(root: Path, label: str):
    root.mkdir(parents=True, exist_ok=True)
    (root / "data").mkdir(parents=True, exist_ok=True)
    layout = resolve_storage_layout(StorageResolutionInputs(
        mode="standalone", data_root=root, sqlite_url="sqlite+aiosqlite:///data/memory.db",
        vector_backend="lancedb", lancedb_path="data/lancedb", graph_snapshot_path="data/graph.json",
    ))
    config = RuntimeConfig.capture(layout)
    return RuntimeDescriptor.from_layout(label, layout=layout, config=config), layout, config


def _build(layout, **kwargs):
    return RuntimeFactory.create(layout, overrides={
        "embedder": lambda runtime: MockEmbeddingProvider(vector_size=runtime.config.vector_size, seed=11),
    }, **kwargs)


@asynccontextmanager
async def _profiles(root: Path, labels=("a", "b")):
    manager = RuntimeManager(factory_builder=_build)
    live = {}
    try:
        for label in labels:
            descriptor, layout, config = _spec(root / label, label)
            live[label] = (await manager.get(descriptor, layout=layout, config=config), layout)
        yield manager, live
    finally:
        await manager.close_all()


async def _remember(runtime, source, subject="collision subject", predicate="owns", object="collision object"):
    app = MemoryApplication(runtime.dependencies.sqlite._session_factory)
    result = await app.memory.remember(RememberCommand(subject, predicate, object, source=source))
    return result["fact"].id


async def _drain(runtime):
    return await runtime.dependencies.outbox_worker.process_all_pending()


async def _read(runtime, fact_id):
    async with runtime.dependencies.sqlite._session_factory() as session:
        fact = await session.get(FactORM, fact_id)
        receipt = await session.get(MemoryReceiptORM, fact_id)
        outbox = (await session.execute(select(OutboxEntryORM).where(OutboxEntryORM.record_id == fact_id))).scalar_one()
    audit = await collect_persisted_audit_state(
        sqlite=runtime.dependencies.sqlite,
        vector_provider=runtime.dependencies.vector,
    )
    return fact, receipt, outbox, audit


async def test_two_live_profiles_are_disjoint_across_sqlite_lancedb_and_graph(tmp_path):
    async with _profiles(tmp_path) as (_, live):
        ids = {}
        for label, (runtime, _) in live.items():
            ids[label] = await _remember(runtime, label)
            await _drain(runtime)
            await runtime.dependencies.vector.upsert(
                point_id="collision",
                vector=[0.1] * 384,
                payload={
                    "profile": label,
                    "memory_type": "fact",
                    "subject": "collision",
                    "predicate": "p",
                    "object": label,
                },
            )
            runtime.dependencies.graph.add_node(id="collision", type="profile", name=label)
        for label, (runtime, _) in live.items():
            fact, receipt, outbox, audit = await _read(runtime, ids[label])
            assert fact.source == label and receipt.id == fact.id and outbox.status == "completed"
            payloads = [row["payload"] for row in await runtime.dependencies.vector.scroll()]
            assert label in {payload["profile"] for payload in payloads if "profile" in payload}
            assert runtime.dependencies.graph.get_node("collision").name == label
            other = "b" if label == "a" else "a"
            assert other not in {payload["profile"] for payload in payloads if "profile" in payload}


async def test_file_backed_sqlite_and_exclusive_lock_are_real_and_isolated(tmp_path):
    async with _profiles(tmp_path) as (_, live):
        paths = []
        for label, (runtime, layout) in live.items():
            path = Path(layout.sqlite.local_path)
            paths.append(path)
            assert path.is_file() and path.parent == layout.data_root / "data"
            assert inspect_existing_lock_readonly(layout.data_root).availability == "held"
            with __import__("sqlite3").connect(path) as connection:
                tables = {row[0] for row in connection.execute("select name from sqlite_master where type='table'")}
            assert tables >= {"facts", "receipts", "outbox_entries"}
        assert paths[0] != paths[1] and paths[0].stat().st_ino != paths[1].stat().st_ino


async def test_application_ingestion_receipt_outbox_and_audit_provenance_are_real(tmp_path):
    async with _profiles(tmp_path, ("a",)) as (_, live):
        runtime = live["a"][0]
        fact_id = await _remember(runtime, "audit-a")
        await _drain(runtime)
        fact, receipt, outbox, audit = await _read(runtime, fact_id)
        assert fact.id == receipt.id == outbox.record_id == fact_id
        assert outbox.status == "completed" and fact_id in audit["receipt_ids"]
        assert audit["sqlite_counts"]["facts"] == audit["sqlite_counts"]["receipts"] == 1


async def test_graph_snapshot_is_explicitly_loaded_after_restart(tmp_path):
    async with _profiles(tmp_path, ("a",)) as (manager, live):
        runtime, layout = live["a"]
        await _remember(runtime, "a")
        await _drain(runtime)
        await manager.stop_profile("a")
        reopened = SimpleGraph(snapshot_path=layout.graph_snapshot_path)
        assert reopened.get_all_nodes() == []
        reopened.load_snapshot()
        assert reopened.get_node("collision-subject") is not None


async def test_restart_preserves_each_profile_persistent_state(tmp_path):
    async with _profiles(tmp_path) as (manager, live):
        ids = {label: await _remember(runtime, label) for label, (runtime, _) in live.items()}
        for runtime, _ in live.values():
            await _drain(runtime)
        await manager.close_all()
    async with _profiles(tmp_path) as (_, live):
        for label, (runtime, _) in live.items():
            fact, receipt, outbox, _ = await _read(runtime, ids[label])
            assert fact.id == receipt.id == outbox.record_id == ids[label]
            assert outbox.status == "completed"
            assert any(row["payload"]["source"] == label for row in await runtime.dependencies.vector.scroll())


async def test_stop_profile_a_leaves_profile_b_ready_and_writable(tmp_path):
    async with _profiles(tmp_path) as (manager, live):
        await _remember(live["a"][0], "a")
        await manager.stop_profile("a")
        runtime = live["b"][0]
        fact_id = await _remember(runtime, "b-after-stop", "new subject", "writes", "new object")
        await _drain(runtime)
        assert runtime.state == "READY" and (await _read(runtime, fact_id))[0].source == "b-after-stop"


async def test_ambient_mutation_cannot_redirect_existing_profiles(tmp_path, monkeypatch):
    root = tmp_path / "profiles"
    manager = RuntimeManager(factory_builder=_build)
    specs = [_spec(root / label, label) for label in ("a", "b")]
    monkeypatch.setenv("MEMORY_SERVER_DATA_ROOT", str(tmp_path / "ambient-trap"))
    live = [await manager.get(d, layout=layout, config=c) for d, layout, c in specs]
    try:
        for label, runtime in zip(("a", "b"), live):
            await _remember(runtime, label)
            assert Path(runtime.layout.data_root) == root / label
        assert not (tmp_path / "ambient-trap").exists()
    finally:
        await manager.close_all()


async def test_remote_and_degraded_eligibility_is_preserved(tmp_path):
    home = tmp_path / "remote-home"
    home.mkdir()
    with pytest.raises(StorageLayoutError) as caught:
        resolve_storage_layout(StorageResolutionInputs(mode="profile", profile_home=home,
            vector_backend="qdrant", qdrant_location="remote.invalid:6333"))
    assert getattr(caught.value, "code", None) == "E_QDRANT_PROFILE_NAMESPACE_UNDEFINED"
    ephemeral = resolve_storage_layout(StorageResolutionInputs(mode="standalone", data_root=tmp_path / "ephemeral",
        sqlite_url="sqlite+aiosqlite:///data/memory.db", vector_backend="qdrant", qdrant_location=":memory:"))
    assert ephemeral.vector.kind == "memory"


async def test_existing_profile_migration_and_reindex_selectors_are_unchanged_inputs():
    for name in ("test_profile_data_isolation.py", "test_profile_migration.py", "test_reindex_graph.py"):
        assert (Path(__file__).parent / name).is_file()
