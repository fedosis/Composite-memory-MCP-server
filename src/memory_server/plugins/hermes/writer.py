"""Async batch writer queue for the Hermes MemoryProvider plugin.

Pattern: Collect observations from sync_turn() calls, batch them,
and flush periodically or on explicit flush (session switch/end).
Ref: Hindsight writer pattern in plugins/memory/hindsight/__init__.py
"""

from __future__ import annotations

import asyncio
import heapq
import inspect
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Literal

logger = logging.getLogger(__name__)

# Sentinel sent to the queue to trigger a clean shutdown.
_SENTINEL = object()


@dataclass(slots=True)
class _QueuedTurn:
    messages: Any
    turn_id: str | None
    attempts: int = 0
    available_at: float = 0.0


@dataclass(slots=True)
class _InFlightBatch:
    seq: int
    items: list[_QueuedTurn]
    started_at: float
    callback_task: asyncio.Task | None = None
    state: Literal["draining", "callback_running", "settled", "unsettled"] = "draining"
    outcomes: list[tuple[tuple[Any, str | None], bool, str | None]] | None = None


@dataclass(frozen=True, slots=True)
class FlushReport:
    """Cumulative accounting only; provenance stays private on the queue."""

    total: int
    flushed: int
    failed: int
    unsettled: int
    failed_records: int
    requeued: int


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
        self._in_flight: dict[int, _InFlightBatch] = {}
        self._batch_seq = 0
        self._flush_lock = asyncio.Lock()
        self._paused = False
        self._admission_closed = False

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
        if self._paused or self._admission_closed:
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
        if self._admission_closed:
            raise RuntimeError("WriterQueue admission is closed")
        self._queue.put_nowait(_QueuedTurn(messages=messages, turn_id=turn_id))
        self._total_queued += 1
        logger.debug("WriterQueue: queued turn %s (total=%s)", turn_id, self._total_queued)

    async def flush(self) -> int:
        """Serialize explicit/background passes, settling prior custody first."""
        async with self._flush_lock:
            if self._paused:
                return 0
            before = self._total_flushed
            await self._settle_locked()
            if not self._in_flight and not self._paused:
                await self._flush_batch_locked()
            return self._total_flushed - before

    async def _flush_batch_locked(self) -> None:
        self._drain_due_retries(time.monotonic())
        if self._queue.empty():
            return
        self._batch_seq += 1
        ledger = _InFlightBatch(self._batch_seq, [], time.monotonic())
        self._in_flight[ledger.seq] = ledger
        # No await between dequeue and ledger custody (nor before task retention).
        while len(ledger.items) < self._max_batch:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item is not _SENTINEL:
                ledger.items.append(item)
        if not ledger.items:
            del self._in_flight[ledger.seq]
            return
        callback_batch = [(item.messages, item.turn_id) for item in ledger.items]
        ledger.callback_task = asyncio.ensure_future(self._invoke_callback(callback_batch))
        ledger.state = "callback_running"
        await self._settle_locked()

    async def _invoke_callback(self, batch):
        try:
            result = self._write_callback(batch)
            if inspect.isawaitable(result):
                result = await result
            if result is None:
                return [(item, True, None) for item in batch]
            if not isinstance(result, list):
                raise RuntimeError(f"write callback returned unsupported result: {type(result)!r}")
            if len(result) != len(batch):
                raise RuntimeError(
                    f"write callback returned {len(result)} outcomes for {len(batch)} items"
                )
            # Validate the WHOLE batch before any counter/retry mutation.
            for item, outcome in zip(batch, result, strict=True):
                if (not isinstance(outcome, (tuple, list)) or len(outcome) != 3
                        or not isinstance(outcome[0], (tuple, list))
                        or len(outcome[0]) != 2 or tuple(outcome[0]) != item
                        or type(outcome[1]) is not bool
                        or (outcome[2] is not None and not isinstance(outcome[2], str))):
                    raise RuntimeError("write callback returned invalid per-item outcome")
            return result
        except Exception as exc:
            logger.exception("WriterQueue: flush failed for %s items", len(batch))
            return [(item, False, str(exc)) for item in batch]

    async def _settle_locked(self) -> None:
        for ledger in list(self._in_flight.values()):
            task = ledger.callback_task
            if task is None:
                ledger.state = "unsettled"
                continue
            try:
                ledger.outcomes = await asyncio.shield(task)
            except asyncio.CancelledError:
                ledger.state = "unsettled"
                # Distinguish caller cancellation from the callback's unknown
                # outcome. Neither is evidence permitting a replay.
                caller = asyncio.current_task()
                if caller is not None and caller.cancelling():
                    raise
                continue
            self._apply_outcomes(ledger.items, ledger.outcomes)
            ledger.state = "settled"
            del self._in_flight[ledger.seq]

    def _apply_outcomes(self, batch, outcomes) -> int:
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

    def stop_admission(self) -> None:
        """Freeze accepted work; no new turn can enter after this call."""
        self._admission_closed = True

    async def pause(self) -> None:
        """Stop normal batches, without cancelling the owned callback task."""
        self._paused = True
        task = self._task
        if task is not None:
            if not task.done():
                task.cancel()
            # wait() does not propagate cancellation into the background task;
            # the batch callback is separately shielded and strongly retained.
            await asyncio.wait([task])
            if not task.cancelled():
                task.result()
            self._task = None

    def _report(self) -> FlushReport:
        return FlushReport(
            total=self._total_queued,
            flushed=self._total_flushed,
            failed=self._total_failed,
            unsettled=self._total_queued - self._total_flushed - self._total_failed,
            failed_records=len(self._failed_items),
            requeued=self._total_requeued,
        )

    async def settle(self) -> FlushReport:
        """Join original callback tasks; never start a fresh callback here."""
        async with self._flush_lock:
            await self._settle_locked()
            return self._report()

    async def flush_final(self, deadline: float) -> FlushReport:
        """Drain under one lock until an absolute monotonic deadline.

        A timeout cancels only this waiter. Callback custody survives in the
        ledger, and a later invocation resumes the same operation first.
        """
        if deadline <= time.monotonic():
            return self._report()
        try:
            async with asyncio.timeout(deadline - time.monotonic()):
                async with self._flush_lock:
                    await self._settle_locked()
                    while not self._in_flight:
                        if time.monotonic() >= deadline:
                            break
                        await self._flush_batch_locked()
                        if self._in_flight or (self._queue.empty() and not self._retry_heap):
                            break
                        if self._queue.empty() and self._retry_heap:
                            delay = max(0.0, self._retry_heap[0][0] - time.monotonic())
                            await asyncio.sleep(min(delay, 0.05))
        except TimeoutError:
            pass
        return self._report()

    async def shutdown(self) -> int:
        """Compatibility wrapper: pause, final flush, return this call's count."""
        before = self._total_flushed
        await self.pause()
        report = await self.flush_final(time.monotonic() + 30.0)
        return report.flushed - before

    async def _run(self) -> None:
        """Background loop: never drain retry custody outside the flush lock."""
        try:
            while not self._paused:
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
