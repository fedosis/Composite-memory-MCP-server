"""Unit tests for the writer pending-snapshot and admission cap (slice T3)."""

from __future__ import annotations

import heapq
from typing import Any

import pytest

from memory_server.plugins.hermes.provider import HermesProvider
from memory_server.plugins.hermes.writer import (
    PendingSnapshot,
    WriterBacklogError,
    WriterQueue,
    _InFlightBatch,
    _QueuedTurn,
)


class _StatefulWriter:
    """Controllable writer double whose pending state is driven by the test."""

    def __init__(self, live: int = 0, terminal: int = 0):
        self.live = live
        self.terminal = terminal
        self.add_turn_calls = 0

    def pending_snapshot(self) -> PendingSnapshot:
        return PendingSnapshot(
            live_pending=self.live,
            terminal_failed=self.terminal,
            retry_waiting=0,
            in_flight=0,
        )

    async def add_turn(self, messages: Any, turn_id: str | None = None) -> None:
        self.add_turn_calls += 1
        self.live += 1


def test_snapshot_counts_structures():
    queue = WriterQueue(write_callback=lambda batch: None, flush_interval=10.0, max_batch=50)

    for i in range(3):
        queue._queue.put_nowait(_QueuedTurn(messages=f"q{i}", turn_id=f"q{i}"))
    for i in range(2):
        heapq.heappush(queue._retry_heap, (float(i), i, _QueuedTurn(messages=f"r{i}", turn_id=f"r{i}")))
    queue._in_flight[1] = _InFlightBatch(
        1, [_QueuedTurn(messages="a", turn_id="a"), _QueuedTurn(messages="b", turn_id="b")], 0.0
    )
    queue._in_flight[2] = _InFlightBatch(
        2, [_QueuedTurn(messages="c", turn_id="c") for _ in range(3)], 0.0
    )
    queue._failed_items = [{"turn_id": f"f{i}"} for i in range(4)]

    snap = queue.pending_snapshot()

    assert snap.live_pending == 10  # 3 queued + 2 retry + 5 in-flight
    assert snap.terminal_failed == 4
    assert snap.retry_waiting == 2
    assert snap.in_flight == 5


async def test_gate_rejects_at_live_64():
    provider = HermesProvider()
    writer = _StatefulWriter(live=64, terminal=0)
    provider._writer = writer

    accepted = True
    try:
        await provider.enqueue_observation_async([], session_id="s", live_limit=64, terminal_limit=64)
    except WriterBacklogError as exc:
        accepted = False
        assert exc.code == "E_BACKLOG"

    assert accepted is False
    assert writer.add_turn_calls == 0


async def test_gate_rejects_at_terminal_64():
    provider = HermesProvider()
    writer = _StatefulWriter(live=0, terminal=64)
    provider._writer = writer

    accepted = True
    try:
        await provider.enqueue_observation_async([], session_id="s", live_limit=64, terminal_limit=64)
    except WriterBacklogError as exc:
        accepted = False
        assert exc.code == "E_BACKLOG"

    assert accepted is False
    assert writer.add_turn_calls == 0


async def test_upper_bound_127():
    provider = HermesProvider()
    writer = _StatefulWriter()
    provider._writer = writer

    max_total = 0
    for _ in range(200):
        total = writer.live + writer.terminal
        max_total = max(max_total, total)
        assert total <= 127
        assert writer.terminal <= 127
        try:
            await provider.enqueue_observation_async([], session_id="s", live_limit=64, terminal_limit=64)
        except WriterBacklogError:
            # Simulate a prior live item settling into a terminal failure so
            # the terminal side of the bound is exercised.
            if writer.terminal < 64 and writer.live > 0:
                writer.live -= 1
                writer.terminal += 1

    assert max_total <= 127


async def test_accepted_items_not_deleted_after_trip():
    provider = HermesProvider()
    writer = _StatefulWriter(live=64, terminal=0)
    provider._writer = writer

    with pytest.raises(WriterBacklogError):
        await provider.enqueue_observation_async([], session_id="s", live_limit=64, terminal_limit=64)

    # Accepted custody is never deleted by the gate.
    assert writer.live == 64
    assert writer.terminal == 0
    assert writer.add_turn_calls == 0


def test_existing_retry_policy_unchanged():
    queue = WriterQueue(write_callback=lambda batch: None, flush_interval=10.0, max_batch=50)

    item1 = _QueuedTurn(messages="a", turn_id="a", attempts=0)
    queue._apply_outcomes([item1], [((item1.messages, item1.turn_id), False, "boom")])
    assert queue.total_requeued == 1
    assert queue.total_failed == 0
    assert len(queue._retry_heap) == 1

    item2 = _QueuedTurn(messages="b", turn_id="b", attempts=2)
    queue._apply_outcomes([item2], [((item2.messages, item2.turn_id), False, "boom")])
    assert queue.total_failed == 1
    assert len(queue._failed_items) == 1
    assert queue.total_requeued == 1
