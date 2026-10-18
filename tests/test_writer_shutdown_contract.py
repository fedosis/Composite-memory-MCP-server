"""C05: real WriterQueue custody/serialization at writer.py:131-263 (base).

Only callbacks and the clock are controlled failure edges; no queue/lock mock.
Storage settlement is separately exercised through learn in test_writer_queue.
"""
import asyncio
import time
from dataclasses import asdict
from types import SimpleNamespace

import pytest

from memory_server.plugins.hermes import writer
from memory_server.plugins.hermes.writer import WriterQueue

pytestmark = pytest.mark.asyncio


async def cancelled(task):
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def finish(queue):
    return await queue.flush_final(time.monotonic() + 2)


async def test_cancelled_flush_does_not_lose_in_flight_items():
    entered, release = asyncio.Event(), asyncio.Event()

    async def callback(batch):
        entered.set()
        await release.wait()

    queue = WriterQueue(callback)
    for i in range(3):
        await queue.add_turn(f'private-{i}', str(i))
    caller = asyncio.create_task(queue.flush())
    await asyncio.wait_for(entered.wait(), 1)
    await cancelled(caller)
    retained = sum(len(b.items) for b in getattr(queue, '_in_flight', {}).values())
    try:
        # Behavioral baseline contrast: queue is already empty, yet no owner
        # retains the three accepted items on the old production implementation.
        assert queue._queue.empty()
        assert queue.total_flushed + queue.total_failed + retained == queue.total_queued
    finally:
        release.set()
        if hasattr(queue, 'settle'):
            await queue.settle()


async def test_concurrent_flush_calls_serialize():
    entered, release, overlapping = asyncio.Event(), asyncio.Event(), asyncio.Event()
    active = 0

    async def callback(batch):
        nonlocal active
        active += 1
        if active > 1:
            overlapping.set()
        entered.set()
        try:
            await release.wait()
        finally:
            active -= 1

    queue = WriterQueue(callback, flush_interval=0, max_batch=1)
    await queue.add_turn('one', '1')
    await queue.add_turn('two', '2')
    await queue.start()
    await asyncio.wait_for(entered.wait(), 1)
    explicit = asyncio.create_task(queue.flush())
    try:
        done, _ = await asyncio.wait([explicit], timeout=0.03)
        assert not done
        assert not overlapping.is_set(), 'background and explicit callbacks overlap'
    finally:
        release.set()
        await explicit
        await queue.shutdown()


async def test_unknown_callback_outcome_is_not_replayed_or_promoted():
    calls = []

    async def callback(batch):
        calls.extend(batch)
        # Fault edge: commit may already have happened; cancellation is not
        # a negative outcome authorizing a replay.
        raise asyncio.CancelledError

    queue = WriterQueue(callback)
    await queue.add_turn('unknown', 'u')
    report = await finish(queue)
    original = next(iter(queue._in_flight.values())).callback_task
    for _ in range(2):
        assert await queue.flush() == 0
        report = await finish(queue)
    assert (report.total, report.flushed, report.failed, report.unsettled) == (1, 0, 0, 1)
    assert len(calls) == 1
    assert queue.total_requeued == 0
    assert next(iter(queue._in_flight.values())).callback_task is original


@pytest.mark.parametrize('mode', ['short', 'long', 'malformed', 'invalid_status', 'wrong_item', 'unsupported'])
async def test_callback_result_count_mismatch_marks_batch_failed_without_success_claims(mode):
    def callback(batch):
        good = [(item, True, None) for item in batch]
        return {
            'short': good[:1], 'long': good + good[:1],
            'malformed': [good[0], None],
            'invalid_status': [good[0], (batch[1], 'yes', None)],
            'wrong_item': [good[0], (('wrong', 'id'), True, None)],
            'unsupported': 42,
        }[mode]

    queue = WriterQueue(callback)
    await queue.add_turn('x', 'x')
    await queue.add_turn('y', 'y')
    assert await queue.flush() == 0
    assert queue.total_flushed == 0 and queue.total_requeued == 2
    report = await finish(queue)
    assert (report.flushed, report.failed, report.unsettled) == (0, 2, 0)
    assert all(item['attempts'] == 3 for item in queue.failed_items)


async def test_callback_task_is_retained_and_settled_after_caller_cancellation():
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def callback(batch):
        calls.extend(batch)
        entered.set()
        await release.wait()

    queue = WriterQueue(callback)
    await queue.add_turn('x', 'x')
    caller = asyncio.create_task(queue.flush())
    await asyncio.wait_for(entered.wait(), 1)
    await cancelled(caller)
    ledger = next(iter(queue._in_flight.values()))
    assert ledger.callback_task is not None and not ledger.callback_task.done()
    report = await queue.flush_final(time.monotonic() + 0.01)
    assert report.unsettled == 1 and report.flushed == 0
    assert ledger.callback_task is next(iter(queue._in_flight.values())).callback_task
    release.set()
    report = await queue.settle()
    assert report.flushed == 1 and report.unsettled == 0
    assert not queue._in_flight
    assert len(calls) == 1


async def test_pause_prevents_new_batches_and_retains_ledger():
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def callback(batch):
        calls.extend(batch)
        entered.set()
        await release.wait()

    queue = WriterQueue(callback, flush_interval=0, max_batch=1)
    await queue.add_turn('one', '1')
    await queue.add_turn('two', '2')
    await queue.start()
    await asyncio.wait_for(entered.wait(), 1)
    await queue.pause()
    assert len(queue._in_flight) == 1
    assert queue._queue.qsize() == 1
    assert await queue.flush() == 0
    queue.stop_admission()
    with pytest.raises(RuntimeError):
        await queue.add_turn('rejected', '3')
    assert queue.total_queued == 2
    release.set()
    await queue.settle()
    assert len(calls) == 1
    assert (await finish(queue)).flushed == 2


async def test_flush_final_covers_queue_retry_heap_and_in_flight():
    entered, release = asyncio.Event(), asyncio.Event()
    attempts = {}

    async def callback(batch):
        item = batch[0]
        tid = item[1]
        attempts[tid] = attempts.get(tid, 0) + 1
        if tid == 'retry' and attempts[tid] == 1:
            return [(item, False, 'retry')]
        if tid == 'inflight':
            entered.set()
            await release.wait()
        return [(item, True, None)]

    queue = WriterQueue(callback, max_batch=1)
    await queue.add_turn('r', 'retry')
    await queue.flush()
    assert len(queue._retry_heap) == 1
    await queue.add_turn('i', 'inflight')
    caller = asyncio.create_task(queue.flush())
    await asyncio.wait_for(entered.wait(), 1)
    await cancelled(caller)
    await queue.add_turn('q', 'queued')
    report = await queue.flush_final(time.monotonic() + 0.01)
    assert (report.total, report.flushed, report.failed, report.unsettled) == (3, 0, 0, 3)
    release.set()
    report = await finish(queue)
    assert (report.total, report.flushed, report.failed, report.unsettled) == (3, 3, 0, 0)
    assert attempts == {'retry': 2, 'inflight': 1, 'queued': 1}
    assert not queue._retry_heap and not queue._in_flight and queue._queue.empty()


async def test_terminal_failure_report_has_counts_and_no_message_contents():
    secret = 'private-message-token'
    queue = WriterQueue(lambda batch: [(batch[0], False, secret)])
    await queue.add_turn(secret, 'private-turn')
    report = await finish(queue)
    assert report.failed == report.failed_records == 1
    assert report.requeued == 2
    assert all(type(value) is int for value in asdict(report).values())
    assert secret not in repr(report) and 'private-turn' not in repr(report)
    assert queue.failed_items == [dict(messages=secret, turn_id='private-turn', attempts=3, error=secret)]
    assert await finish(queue) == report


async def test_retry_cap_backoff_and_heap_semantics_unchanged(monkeypatch):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(writer, 'time', SimpleNamespace(monotonic=lambda: clock.now))
    seen = []

    def callback(batch):
        seen.append([item[1] for item in batch])
        return [(item, False, 'known') for item in batch]

    queue = WriterQueue(callback)
    for tid in ('a', 'b'):
        await queue.add_turn(tid, tid)
    assert [queue._retry_delay(i) for i in (0, 1, 2, 10, 20)] == [0.05, 0.05, 0.1, 0.5, 0.5]
    await queue.flush()
    assert [(due, seq) for due, seq, _ in queue._retry_heap] == [(100.05, 1), (100.05, 2)]
    assert await queue.flush() == 0 and len(seen) == 1
    clock.now = 100.05
    await queue.flush()
    assert all(item.attempts == 2 for _, _, item in queue._retry_heap)
    clock.now = 101.0
    await queue.flush()
    assert seen == [['a', 'b']] * 3
    assert (queue.total_failed, queue.total_requeued, queue.total_flushed) == (2, 4, 0)
    assert not queue._retry_heap


async def test_shutdown_wrapper_is_a_thin_pause_plus_flush_final(monkeypatch):
    queue = WriterQueue(lambda batch: None)
    calls = []
    before = time.monotonic()

    async def pause():
        calls.append('pause')

    async def final(deadline):
        calls.append('final')
        assert before < deadline <= time.monotonic() + 30
        return SimpleNamespace(flushed=2)

    monkeypatch.setattr(queue, 'pause', pause)
    monkeypatch.setattr(queue, 'flush_final', final)
    assert await queue.shutdown() == 2
    assert calls == ['pause', 'final']


async def test_no_duplicate_item_is_flushed_by_two_passes():
    seen = []
    active = 0

    async def callback(batch):
        nonlocal active
        active += 1
        assert active == 1
        await asyncio.sleep(0)
        seen.extend(tid for _, tid in batch)
        active -= 1

    queue = WriterQueue(callback, max_batch=1)
    for i in range(12):
        await queue.add_turn(str(i), str(i))
    await asyncio.gather(*(queue.flush() for _ in range(12)))
    report = await finish(queue)
    assert report.flushed == 12 and report.unsettled == 0
    assert sorted(seen) == sorted(str(i) for i in range(12))


async def test_settle_custody_counts_multi_item_in_flight():
    from memory_server.plugins.hermes.writer import QueueCustodySnapshot
    seen = []
    async def callback(batch):
        seen.extend(batch); raise asyncio.CancelledError
    queue = WriterQueue(callback, max_batch=3)
    for i in range(3):
        await queue.add_turn(f'item-{i}', str(i))
    await queue.flush(); original = next(iter(queue._in_flight.values())).callback_task
    queue.stop_admission(); await queue.pause(); snap = await queue.settle_custody()
    assert isinstance(snap, QueueCustodySnapshot)
    assert snap.unknown_in_flight_items == 3 and snap.safe_queued_items == 0
    assert snap.raw_unsettled == 3 and snap.invariant_ok
    assert next(iter(queue._in_flight.values())).callback_task is original and len(seen) == 3
async def test_settle_custody_counts_retry_heap():
    queue = WriterQueue(lambda batch: [(item, False, 'retry') for item in batch])
    await queue.add_turn('r', 'retry'); await queue.flush(); assert len(queue._retry_heap) == 1
    queue.stop_admission(); await queue.pause(); snap = await queue.settle_custody()
    assert snap.safe_retry_items == 1 and snap.unknown_in_flight_items == 0
    assert len(queue._retry_heap) == 1 and queue._queue.empty() and snap.invariant_ok
async def test_settle_custody_counts_safe_backlog():
    queue = WriterQueue(lambda batch: None)
    await queue.add_turn('a', '1'); await queue.add_turn('b', '2')
    queue.stop_admission(); await queue.pause(); snap = await queue.settle_custody()
    assert snap.safe_queued_items == 2 and snap.unknown_in_flight_items == 0
    assert queue._queue.qsize() == 2 and snap.raw_unsettled == 2 and snap.invariant_ok
async def test_settle_custody_terminal_failure_keeps_provenance_and_counts_only():
    secret = 'private-message-token'
    queue = WriterQueue(lambda batch: [(batch[0], False, secret)])
    await queue.add_turn(secret, 'private-turn'); await finish(queue)
    queue.stop_admission(); await queue.pause(); snap = await queue.settle_custody()
    assert snap.failed == 1 and snap.unknown_in_flight_items == 0
    assert all(type(value) in (int, bool) for value in asdict(snap).values())
    assert secret not in repr(snap) and 'private-turn' not in repr(snap)
    assert queue.failed_items == [dict(messages=secret, turn_id='private-turn', attempts=3, error=secret)]
async def test_settle_custody_conservation_mismatch_sets_invariant_ok_false():
    queue = WriterQueue(lambda batch: None)
    await queue.add_turn('x', 'x'); queue.stop_admission(); await queue.pause()
    queued_before = queue._total_queued; queue._total_flushed += 1
    snap = await queue.settle_custody()
    assert snap.invariant_ok is False
    assert queue._queue.qsize() == 1 or queue._in_flight or queue._retry_heap
    assert queue._total_queued == queued_before != 0 and queue._total_flushed == 1
async def test_flush_final_raw_unsettled_includes_residual():
    entered, release, attempts = asyncio.Event(), asyncio.Event(), {}
    async def callback(batch):
        item = batch[0]; tid = item[1]; attempts[tid] = attempts.get(tid, 0) + 1
        if tid == 'retry' and attempts[tid] == 1:
            return [(item, False, 'retry')]
        if tid == 'inflight':
            entered.set(); await release.wait()
        return [(item, True, None)]
    queue = WriterQueue(callback, max_batch=1)
    await queue.add_turn('r', 'retry'); await queue.flush()
    await queue.add_turn('i', 'inflight')
    caller = asyncio.create_task(queue.flush())
    await asyncio.wait_for(entered.wait(), 1); await cancelled(caller)
    await queue.add_turn('q', 'queued')
    report = await queue.flush_final(time.monotonic() + 0.01)
    assert report.unsettled == report.total - report.flushed - report.failed == 3
    queue.stop_admission(); await queue.pause(); release.set()
    snap = await queue.settle_custody()
    assert snap.safe_queued_items == 1 and snap.safe_retry_items == 1
    assert (await finish(queue)).unsettled == 0
async def test_settle_custody_under_flush_lock_after_pause():
    entered, release = asyncio.Event(), asyncio.Event()
    async def callback(batch):
        entered.set(); await release.wait()
    queue = WriterQueue(callback)
    await queue.add_turn('x', 'x')
    starter = asyncio.create_task(queue.flush())
    await asyncio.wait_for(entered.wait(), 1); await cancelled(starter)
    await queue.pause(); assert await queue.flush() == 0
    queue.stop_admission()
    with pytest.raises(RuntimeError):
        await queue.add_turn('rejected', '3')
    settler = asyncio.create_task(queue.settle_custody())
    for _ in range(40):
        if queue._flush_lock.locked():
            break
        await asyncio.sleep(0)
    assert queue._flush_lock.locked()
    waiting = asyncio.create_task(queue.flush())
    done, _ = await asyncio.wait([waiting], timeout=0.05)
    assert not done, 'flush() must wait while settle_custody holds _flush_lock'
    release.set(); await settler; assert await waiting == 0
async def test_settle_and_flush_final_and_flushreport_unchanged():
    import inspect as inspect_module
    from memory_server.plugins.hermes.writer import FlushReport
    assert list(FlushReport.__dataclass_fields__) == [
        'total', 'flushed', 'failed', 'unsettled', 'failed_records', 'requeued']
    params = inspect_module.signature(WriterQueue.flush_final).parameters
    assert list(params) == ['self', 'deadline']
    report = await WriterQueue(lambda batch: None).settle()
    assert type(report) is FlushReport
