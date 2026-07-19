"""Async batch writer queue for the Hermes MemoryProvider plugin.

Pattern: Collect observations from sync_turn() calls, batch them,
and flush periodically or on explicit flush (session switch/end).
Ref: Hindsight writer pattern in plugins/memory/hindsight/__init__.py
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Sentinel sent to the queue to trigger a clean shutdown.
_SENTINEL = object()


class WriterQueue:
    """Non-blocking batch writer with flush-on-switch semantics.

    Collects turn observations from sync_turn() calls and writes them
    in batches to the CMMS backend via the provided write callback.
    Automatically flushes every `flush_interval` seconds and on
    explicit `flush()` calls (e.g. on session switch).

    Usage:
        queue = WriterQueue(write_fn, flush_interval=5.0, max_batch=50)
        await queue.start()
        await queue.add_turn(messages, turn_id)
        await queue.flush()  # explicit flush
        await queue.shutdown()
    """

    def __init__(
        self,
        write_callback: Callable[[list[tuple[list, str | None]]], Any],
        flush_interval: float = 5.0,
        max_batch: int = 50,
    ):
        self._write_callback = write_callback
        self._flush_interval = flush_interval
        self._max_batch = max_batch
        self._queue: asyncio.Queue = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._total_queued = 0
        self._total_flushed = 0
        self._total_failed = 0

    @property
    def total_queued(self) -> int:
        return self._total_queued

    @property
    def total_flushed(self) -> int:
        return self._total_flushed

    @property
    def total_failed(self) -> int:
        return self._total_failed

    async def start(self) -> None:
        """Start the background flush loop."""
        if self._task is not None and not self._task.done():
            logger.warning("WriterQueue already started")
            return
        self._task = asyncio.create_task(self._run())
        logger.info(
            "WriterQueue started (flush_interval=%s, max_batch=%s)",
            self._flush_interval,
            self._max_batch,
        )

    async def add_turn(self, messages: list, turn_id: str | None = None) -> None:
        """Add a turn observation to the queue.

        Never blocks — the write happens asynchronously.
        """
        await self._queue.put((messages, turn_id))
        self._total_queued += 1
        logger.debug("WriterQueue: queued turn %s (total=%s)", turn_id, self._total_queued)

    async def flush(self) -> int:
        """Drain the queue synchronously.

        Returns the number of items flushed.
        """
        batch: list[tuple[list, str | None]] = []
        # Drain all currently available items (non-blocking)
        while not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                if item is _SENTINEL:
                    continue
                batch.append(item)
            except asyncio.QueueEmpty:
                break
            if len(batch) >= self._max_batch:
                break

        if not batch:
            return 0

        try:
            result = self._write_callback(batch)
            if asyncio.iscoroutine(result):
                await result
            self._total_flushed += len(batch)
            logger.debug(
                "WriterQueue: flushed %s items (total_flushed=%s)",
                len(batch), self._total_flushed,
            )
        except Exception:
            self._total_failed += len(batch)
            logger.exception("WriterQueue: flush failed for %s items", len(batch))

        return len(batch)

    async def shutdown(self) -> int:
        """Flush remaining items and stop the background loop.

        Returns the number of items flushed during shutdown.
        """
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        flushed = await self.flush()
        logger.info(
            "WriterQueue: shutdown complete (queued=%s, flushed=%s, failed=%s)",
            self._total_queued,
            self._total_flushed,
            self._total_failed,
        )
        return flushed

    async def _run(self) -> None:
        """Background loop: periodically flush pending items."""
        try:
            while True:
                await asyncio.sleep(self._flush_interval)
                await self.flush()
        except asyncio.CancelledError:
            logger.debug("WriterQueue: background task cancelled")
            raise


def default_write_handler(batch: list[tuple[list, str | None]]) -> None:
    """Default write handler — logs the batch.

    Override in production via WriterQueue's write_callback parameter.
    """
    logger.debug(
        "default_write_handler: %s turns in batch",
        len(batch),
    )
