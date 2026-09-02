"""Async batch writer queue for the Hermes MemoryProvider plugin.

Pattern: Collect observations from sync_turn() calls, batch them,
and flush periodically or on explicit flush (session switch/end).
Ref: Hindsight writer pattern in plugins/memory/hindsight/__init__.py
"""

from __future__ import annotations

import asyncio
import heapq
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Sentinel sent to the queue to trigger a clean shutdown.
_SENTINEL = object()


@dataclass(slots=True)
class _QueuedTurn:
    messages: Any
    turn_id: str | None
    attempts: int = 0
    available_at: float = 0.0


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
        write_callback: Callable[[list[tuple[Any, str | None]]], Any],
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
        self._total_requeued = 0
        self._failed_items: list[dict[str, Any]] = []
        self._retry_heap: list[tuple[float, int, _QueuedTurn]] = []
        self._retry_sequence = 0

    @property
    def total_queued(self) -> int:
        return self._total_queued

    @property
    def total_flushed(self) -> int:
        return self._total_flushed

    @property
    def total_failed(self) -> int:
        return self._total_failed

    @property
    def total_requeued(self) -> int:
        return self._total_requeued

    @property
    def queued(self) -> int:
        return self._total_queued

    @property
    def flushed(self) -> int:
        return self._total_flushed

    @property
    def failed(self) -> int:
        return self._total_failed

    @property
    def requeued(self) -> int:
        return self._total_requeued

    @property
    def failed_items(self) -> list[dict[str, Any]]:
        return list(self._failed_items)

    def _retry_delay(self, attempts: int) -> float:
        return min(0.05 * max(1, attempts), 0.5)

    def _drain_due_retries(self, now: float) -> None:
        while self._retry_heap and self._retry_heap[0][0] <= now:
            _, _, item = heapq.heappop(self._retry_heap)
            self._queue.put_nowait(item)

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

    async def add_turn(self, messages: Any, turn_id: str | None = None) -> None:
        """Add a turn observation to the queue.

        Never blocks — the write happens asynchronously.
        """
        await self._queue.put(_QueuedTurn(messages=messages, turn_id=turn_id))
        self._total_queued += 1
        logger.debug("WriterQueue: queued turn %s (total=%s)", turn_id, self._total_queued)

    async def flush(self) -> int:
        """Drain the queue synchronously.

        Returns the number of items successfully flushed in this pass.
        """
        now = time.monotonic()
        self._drain_due_retries(now)
        batch: list[_QueuedTurn] = []
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

        outcomes: list[tuple[tuple[Any, str | None], bool, str | None]]
        try:
            callback_batch = [(item.messages, item.turn_id) for item in batch]
            result = self._write_callback(callback_batch)
            if asyncio.iscoroutine(result):
                result = await result
            if result is None:
                outcomes = [((item.messages, item.turn_id), True, None) for item in batch]
            elif isinstance(result, list):
                if len(result) != len(batch):
                    raise RuntimeError(
                        f"write callback returned {len(result)} outcomes for {len(batch)} items"
                    )
                outcomes = result
            else:
                raise RuntimeError(f"write callback returned unsupported result: {type(result)!r}")
        except Exception as exc:
            logger.exception("WriterQueue: flush failed for %s items", len(batch))
            outcomes = [((item.messages, item.turn_id), False, str(exc)) for item in batch]

        flushed_now = 0
        for item, outcome in zip(batch, outcomes, strict=True):
            _, ok, error = outcome
            if ok:
                self._total_flushed += 1
                flushed_now += 1
                continue
            attempts = item.attempts + 1
            error_text = error or "unknown write failure"
            if attempts < 3:
                item.attempts = attempts
                item.available_at = time.monotonic() + self._retry_delay(attempts)
                self._total_requeued += 1
                self._retry_sequence += 1
                heapq.heappush(self._retry_heap, (item.available_at, self._retry_sequence, item))
                logger.warning(
                    "WriterQueue: requeued turn %s after attempt %s/3: %s",
                    item.turn_id,
                    attempts,
                    error_text,
                )
            else:
                self._total_failed += 1
                self._failed_items.append(
                    {
                        "messages": item.messages,
                        "turn_id": item.turn_id,
                        "attempts": attempts,
                        "error": error_text,
                    }
                )
                logger.error(
                    "WriterQueue: failed turn %s after %s attempts: %s",
                    item.turn_id,
                    attempts,
                    error_text,
                )

        logger.debug(
            "WriterQueue: flushed=%s queued=%s requeued=%s failed=%s",
            flushed_now,
            self._total_queued,
            self._total_requeued,
            self._total_failed,
        )
        return flushed_now

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

        flushed = 0
        while True:
            passed = await self.flush()
            flushed += passed
            if self._queue.empty() and not self._retry_heap:
                break
            if self._retry_heap:
                delay = max(0.0, self._retry_heap[0][0] - time.monotonic())
                await asyncio.sleep(min(delay, 0.05))
            else:
                await asyncio.sleep(0)
        logger.info(
            "WriterQueue: shutdown complete (queued=%s, flushed=%s, failed=%s, requeued=%s)",
            self._total_queued,
            self._total_flushed,
            self._total_failed,
            self._total_requeued,
        )
        return flushed

    async def _run(self) -> None:
        """Background loop: periodically flush pending items."""
        try:
            while True:
                self._drain_due_retries(time.monotonic())
                await asyncio.sleep(self._flush_interval)
                await self.flush()
        except asyncio.CancelledError:
            logger.debug("WriterQueue: background task cancelled")
            raise


def default_write_handler(batch: list[tuple[Any, str | None]]) -> None:
    """Default write handler — logs the batch.

    Override in production via WriterQueue's write_callback parameter.
    """
    logger.debug(
        "default_write_handler: %s turns in batch",
        len(batch),
    )
