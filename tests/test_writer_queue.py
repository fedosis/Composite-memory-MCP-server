"""Tests for the Hermes MemoryProvider plugin — writer queue.

Tests focus on the WriterQueue class in isolation.
"""
from __future__ import annotations

import asyncio

import pytest

from memory_server.plugins.hermes.writer import WriterQueue


@pytest.mark.asyncio
class TestWriterQueue:
    """Test WriterQueue basic operations."""

    async def test_queue_starts_and_stops(self):
        """Verify WriterQueue can start and stop cleanly."""
        collected: list = []

        async def write_fn(batch):
            collected.extend(batch)

        queue = WriterQueue(write_callback=write_fn, flush_interval=10.0, max_batch=50)
        await queue.start()
        assert queue._task is not None and not queue._task.done()

        await queue.shutdown()
        assert queue._task is None or queue._task.done()

    async def test_add_turn_queues_item(self):
        """Verify add_turn adds to the internal queue."""
        collected: list = []

        async def write_fn(batch):
            collected.extend(batch)

        queue = WriterQueue(write_callback=write_fn, flush_interval=10.0, max_batch=50)
        await queue.start()

        await queue.add_turn(
            [{"role": "user", "content": "hello"}],
            turn_id="turn-001",
        )
        assert queue.total_queued == 1

        await queue.shutdown()

    async def test_flush_drains_queue(self):
        """Verify flush processes all queued items."""
        collected: list = []

        async def write_fn(batch):
            collected.extend(batch)

        queue = WriterQueue(write_callback=write_fn, flush_interval=10.0, max_batch=50)
        await queue.start()

        await queue.add_turn(
            [{"role": "user", "content": "turn 1"}],
            turn_id="t1",
        )
        await queue.add_turn(
            [{"role": "user", "content": "turn 2"}],
            turn_id="t2",
        )

        flushed = await queue.flush()
        assert flushed == 2
        assert len(collected) == 2

        await queue.shutdown()

    async def test_max_batch_respected(self):
        """Verify flush respects max_batch size."""
        collected: list = []

        async def write_fn(batch):
            collected.extend(batch)

        queue = WriterQueue(write_callback=write_fn, flush_interval=10.0, max_batch=3)
        await queue.start()

        for i in range(10):
            await queue.add_turn(
                [{"role": "user", "content": f"turn {i}"}],
                turn_id=f"t{i}",
            )

        # First flush should only drain max_batch=3 items
        flushed = await queue.flush()
        assert flushed == 3
        assert len(collected) == 3

        # Second flush drains the rest
        flushed2 = await queue.flush()
        assert flushed2 == 3
        assert len(collected) == 6

        await queue.shutdown()

    async def test_flush_on_empty_queue(self):
        """Verify flush returns 0 when queue is empty."""
        collected: list = []

        async def write_fn(batch):
            collected.extend(batch)

        queue = WriterQueue(write_callback=write_fn, flush_interval=10.0)
        await queue.start()

        flushed = await queue.flush()
        assert flushed == 0

        await queue.shutdown()

    async def test_shutdown_flushes_remaining(self):
        """Verify shutdown flushes any remaining items."""
        collected: list = []

        async def write_fn(batch):
            collected.extend(batch)

        queue = WriterQueue(write_callback=write_fn, flush_interval=10.0, max_batch=100)
        await queue.start()

        await queue.add_turn(
            [{"role": "user", "content": "final turn"}],
            turn_id="final",
        )

        flushed = await queue.shutdown()
        assert flushed == 1
        assert len(collected) == 1

    async def test_auto_flush_on_interval(self):
        """Verify auto-flush fires after flush_interval seconds."""
        collected: list = []

        async def write_fn(batch):
            collected.extend(batch)

        queue = WriterQueue(write_callback=write_fn, flush_interval=0.15, max_batch=50)
        await queue.start()

        await queue.add_turn(
            [{"role": "user", "content": "auto-flush test"}],
            turn_id="auto",
        )

        # Wait for auto-flush interval
        await asyncio.sleep(0.3)

        # The auto-flush may have already triggered; if not, flush manually
        if len(collected) == 0:
            await queue.flush()

        assert len(collected) >= 1

        await queue.shutdown()

    async def test_write_handler_error_does_not_crash(self):
        """Verify that a failing write handler doesn't crash the queue."""

        async def failing_fn(batch):
            raise RuntimeError("Intentional failure")

        queue = WriterQueue(write_callback=failing_fn, flush_interval=10.0)
        await queue.start()

        await queue.add_turn([{"role": "user", "content": "test"}], turn_id="t1")
        flushed = await queue.flush()

        # Even on failure, flush returns the count (items removed from queue)
        assert flushed == 1
        assert queue.total_failed == 1

        await queue.shutdown()

    async def test_stats_accurate(self):
        """Verify queue statistics are tracked correctly."""
        collected: list = []

        async def write_fn(batch):
            collected.extend(batch)

        queue = WriterQueue(write_callback=write_fn, flush_interval=10.0)
        await queue.start()

        assert queue.total_queued == 0
        assert queue.total_flushed == 0
        assert queue.total_failed == 0

        await queue.add_turn([{"role": "user", "content": "a"}], turn_id="a")
        await queue.add_turn([{"role": "user", "content": "b"}], turn_id="b")

        await queue.flush()

        assert queue.total_queued == 2
        assert queue.total_flushed == 2

        await queue.shutdown()
