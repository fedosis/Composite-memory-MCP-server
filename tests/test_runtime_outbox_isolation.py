"""Slice C: real worker claim, status, and compaction isolation."""

import asyncio
import logging

from storage.outbox import OutboxEntryORM, OutboxRepository

from tests.test_runtime_profile_isolation import _drain, _profiles, _read, _remember


async def test_worker_a_never_claims_or_consumes_profile_b_rows(tmp_path):
    async with _profiles(tmp_path) as (_, live):
        ids = {label: await _remember(runtime, label) for label, (runtime, _) in live.items()}
        result = await _drain(live["a"][0])
        assert result["processed"] == 1
        assert (await _read(live["a"][0], ids["a"]))[2].status == "completed"
        assert (await _read(live["b"][0], ids["b"]))[2].status == "pending"


async def test_claim_process_and_per_row_status_readback_cross_real_worker(tmp_path):
    async with _profiles(tmp_path, ("a",)) as (_, live):
        runtime = live["a"][0]
        ids = [await _remember(runtime, "a", f"subject-{i}", "owns", f"object-{i}") for i in range(2)]
        attempted = await runtime.dependencies.outbox_worker._poll_once()
        assert attempted == 2
        statuses = [(await _read(runtime, item))[2].status for item in ids]
        assert statuses == ["completed", "completed"]


async def test_compaction_descendant_is_test_owned_and_awaited_without_run_loop(tmp_path, caplog):
    async with _profiles(tmp_path, ("a",)) as (_, live):
        runtime = live["a"][0]
        await _remember(runtime, "a")
        await _drain(runtime)
        worker = runtime.dependencies.outbox_worker
        worker._compact_interval_seconds = 0
        caplog.set_level(logging.INFO, logger="storage.outbox_worker")
        await worker._maybe_compact()
        assert worker._compact_task is not None
        task = worker._compact_task
        _, records, _ = runtime.owned_snapshot()
        owned = [record for record in records if record.task is task]
        assert len(owned) == 1
        record = owned[0]
        assert record.kind == "outbox-compact" and record.state == "ATTACHED"
        await asyncio.wait_for(task, timeout=10)
        assert task.done() and not task.cancelled() and task.exception() is None
        assert "compaction done" in caplog.text
        # Completion schedules the runtime reaper through two loop callbacks.
        for _ in range(10):
            if record not in runtime.owned_snapshot()[1]:
                break
            await asyncio.sleep(0)
        assert record.state == "TERMINATED" and record.terminal_type is None
        assert record not in runtime.owned_snapshot()[1]
    assert runtime.state == "CLOSED"
    assert runtime.owned_snapshot()[1:] == ((), ())


async def test_receipt_audit_and_vector_graph_provenance_remain_profile_local(tmp_path):
    async with _profiles(tmp_path) as (_, live):
        ids = {label: await _remember(runtime, label) for label, (runtime, _) in live.items()}
        for runtime, _ in live.values():
            await _drain(runtime)
        for label, (runtime, _) in live.items():
            fact, receipt, outbox, audit = await _read(runtime, ids[label])
            assert fact.source == receipt.source == label and outbox.status == "completed"
            assert ids[label] in audit["receipt_ids"]
            payloads = [row["payload"] for row in await runtime.dependencies.vector.scroll()]
            assert any(p.get("source") == label for p in payloads)
            assert all(p.get("source") != ("b" if label == "a" else "a") for p in payloads)
            assert runtime.dependencies.graph.get_node("collision-subject") is not None


async def test_outbox_attempt_count_is_not_completion_count(tmp_path):
    async with _profiles(tmp_path, ("a",)) as (_, live):
        runtime = live["a"][0]
        good = await _remember(runtime, "a")
        async with runtime.dependencies.sqlite._session_factory() as session:
            entry = await OutboxRepository(session).add_entry("fact", "bad-id", "unknown_operation", {})
            await session.commit()
        result = await runtime.dependencies.outbox_worker.process_all_pending()
        assert result == {"processed": 1, "failed": 0}
        assert (await _read(runtime, good))[2].status == "completed"
        async with runtime.dependencies.sqlite._session_factory() as session:
            bad = await session.get(OutboxEntryORM, entry.id)
        assert bad.status == "pending" and bad.retry_count == 1
