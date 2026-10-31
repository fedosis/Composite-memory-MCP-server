"""Fail-quiet retrieval trace observation and persistence."""

from __future__ import annotations

import asyncio
import math
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Any
from uuid import uuid4

from memory_server.application.memory_read_service import canonical
from memory_server.control.policy import TokenError, require_permission
from memory_server.control.store import ControlError
from memory_server.gateway_contracts import dto as d
from memory_server.transports.http.errors import HttpError


def _safe_id(value: Any) -> str | None:
    return value if type(value) is str and 1 <= len(value) <= 128 and value.isprintable() else None


def _safe_score(value: Any) -> float | None:
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _contribution(ident, source, raw=None, normalized=None, before=None, after=None, winner=None):
    try:
        return d.TraceContribution(
            memory_id=_safe_id(ident),
            source=source,
            raw_score=_safe_score(raw),
            normalized_score=_safe_score(normalized),
            rank_before_merge=before if type(before) is int and before >= 1 else None,
            rank_after_merge=after if type(after) is int and after >= 1 else None,
            matched_terms=[],
            relation_path=[],
            dedup_winner_id=_safe_id(winner),
        )
    except Exception:
        return None


def _copy_items(items):
    try:
        return [item.model_copy(deep=True) if hasattr(item, "model_copy") else item for item in list(items)]
    except Exception:
        return []


class _TraceObserver:
    """Per-request observer; it deliberately has no persistence behavior."""

    def __init__(self, ctx=None, operation="search", max_contributions=100):
        self.trace_id = None
        self.overflow = False
        self.stages: dict[str, list[d.TraceContribution]] = {}
        self.stage_status: dict[str, str] = {}
        self.merge: list[d.TraceContribution] = []
        self.deduplicated: list[d.TraceContribution] = []
        self.final_items: list[d.TraceContribution] = []
        self._contributions = 0
        self._max_contributions = max_contributions
        self.ctx, self.operation = ctx, operation

    def _accept(self, count):
        if self.overflow:
            return False
        self._contributions += count
        if self._contributions > self._max_contributions:
            self.overflow = True
            return False
        return True

    def stage(self, name, items, status):
        try:
            if self.overflow:
                return
            values = _copy_items(items)
            if not self._accept(len(values)):
                return
            self.stages[name] = values
            self.stage_status[name] = status
        except Exception:
            return

    def merged(self, items, deduplicated):
        try:
            if self.overflow:
                return
            values, unique = _copy_items(items), _copy_items(deduplicated)
            if not self._accept(len(values) + len(unique)):
                return
            self.merge, self.deduplicated = values, unique
        except Exception:
            return

    def final(self, items):
        try:
            if self.overflow:
                return
            values = _copy_items(items)
            if not self._accept(len(values)):
                return
            self.final_items = values
        except Exception:
            return


class _ObserverTap:
    def __init__(self, observer):
        self.observer = observer
        self.raw_items = {}
        self.ordered = []
        self.kept = []
        self.dropped = []

    def raw(self, source, items):
        try:
            self.raw_items[source] = list(items)
        except Exception:
            self.raw_items[source] = []

    def order(self, items):
        try:
            self.ordered = list(items)
        except Exception:
            self.ordered = []

    def dedup(self, kept, dropped):
        try:
            self.kept, self.dropped = list(kept), list(dropped)
        except Exception:
            self.kept, self.dropped = [], []

    def emit(self):
        try:
            raw = {
                (ident, source): (score, before)
                for source, rows in self.raw_items.items()
                for ident, score, before in rows
            }
            losers = {
                (loser_id, loser_source): winner_id
                for winner_id, _winner_source, loser_id, loser_source in self.dropped
            }
            merged = []
            by_key = {}
            for ident, source, normalized, after in self.ordered:
                raw_score, before = raw.get((ident, source), (None, None))
                item = _contribution(ident, source, raw_score, normalized, before, after, losers.get((ident, source)))
                if item is not None:
                    merged.append(item)
                    by_key[(ident, source)] = item
            dedup = [by_key[key] for key in self.kept if key in by_key]
            return merged, dedup
        except Exception:
            return [], []


class RetrievalTraceService:
    def __init__(
        self,
        leases,
        store,
        *,
        cursor_codec=None,
        metrics=None,
        clock=None,
        max_contributions=100,
        settings_digest=None,
        code_revision=None,
    ):
        self.leases = leases
        self._store = store
        self.cursor_codec = cursor_codec
        self.metrics = metrics
        self.clock = clock or monotonic
        self._max_contributions = max_contributions
        self.settings_digest = settings_digest or ("0" * 64)
        self.code_revision = code_revision or os.getenv("MEMORY_SERVER_CODE_REVISION") or "unknown"
        self.dropped = 0

    @asynccontextmanager
    async def observe(self, ctx, operation):
        observer = _TraceObserver(ctx, operation, self._max_contributions)
        try:
            yield observer
        except BaseException:
            raise
        else:
            await self._persist(ctx, observer)

    async def _persist(self, ctx, observer):
        try:
            if observer.overflow or ctx.deadline - self.clock() < 0.05:
                return
            created = datetime.now(UTC)
            expiry = created + timedelta(seconds=self._store.ttl_seconds)
            trace = d.Trace(
                id=str(uuid4()),
                request_id=ctx.request_id,
                profile_id=ctx.profile_id,
                operation=observer.operation,
                query=None,
                stages=observer.stages,
                stage_status=observer.stage_status,
                merge=observer.merge,
                deduplicated=observer.deduplicated,
                final=observer.final_items,
                created_at=created,
                expires_at=expiry,
                settings_digest=self.settings_digest,
                code_revision=self.code_revision,
            )
            body = trace.model_dump(mode="json")
            canonical(body)
            remaining = ctx.deadline - self.clock()
            if remaining < 0.05:
                return
            ident = await asyncio.wait_for(
                asyncio.to_thread(self._store.insert, body, now=created), min(0.25, remaining)
            )
            observer.trace_id = ident
        except Exception:
            self.dropped += 1

    async def execute(self, ctx, command):
        if command.operation not in {"traces.get", "traces.list"}:
            raise HttpError("E_VALIDATION")
        if command.profile_id != ctx.profile_id:
            raise HttpError("E_PERMISSION_DENIED")
        try:
            require_permission(ctx.principal, "memory.read", profile_id=command.profile_id)
        except TokenError:
            raise HttpError("E_PERMISSION_DENIED") from None
        if ctx.deadline - self.clock() <= 0:
            raise HttpError("E_TIMEOUT")
        try:
            async with self.leases.acquire(ctx):
                if ctx.deadline - self.clock() <= 0:
                    raise HttpError("E_TIMEOUT")
                now = datetime.now(UTC)
                if command.operation == "traces.get":
                    from memory_server.transports.http.routes.traces import validate_trace_id

                    validate_trace_id(command.path_ids["id"])
                    value = await asyncio.to_thread(self._store.get, ctx.profile_id, command.path_ids["id"], now=now)
                    if value is None:
                        raise HttpError("E_NOT_FOUND")
                    from memory_server.transports.http.routes.traces import _validate_trace

                    return _validate_trace(d.Trace.model_validate_json(canonical(value), strict=True))
                state = self.cursor_codec.validate(ctx.principal, command, now=now) if command.payload.cursor else None
                rows = await asyncio.to_thread(
                    self._store.page,
                    ctx.profile_id,
                    limit=command.payload.limit,
                    cursor=None if state is None else state["last"],
                    now=now,
                )
                more = len(rows) > command.payload.limit
                rows = rows[: command.payload.limit]
                items = [
                    d.TraceSummary.model_validate_json(
                        canonical(
                            {
                                k: v[k]
                                for k in (
                                    "id",
                                    "request_id",
                                    "operation",
                                    "created_at",
                                    "expires_at",
                                    "settings_digest",
                                    "code_revision",
                                )
                            }
                        ),
                        strict=True,
                    )
                    for v in rows
                ]
                token = None
                if more:
                    token = self.cursor_codec.issue(
                        ctx,
                        command,
                        last=(datetime.fromisoformat(rows[-1]["created_at"]).isoformat(), rows[-1]["id"]),
                        limit=command.payload.limit,
                        now=now,
                    )
                return d.Page[d.TraceSummary](items=items, next_cursor=token, has_more=more, request_id=ctx.request_id)
        except HttpError:
            raise
        except (ControlError, sqlite3.Error, OSError):
            raise HttpError("E_STORAGE") from None
        except (ValueError, TypeError, KeyError):
            raise HttpError("E_INTERNAL") from None


class InMemoryTracePort:
    """Minimal TracePort used by S11A and by embedding applications."""

    def __init__(self):
        self.observers = []

    @asynccontextmanager
    async def observe(self, ctx, operation):
        observer = _TraceObserver(ctx, operation)
        self.observers.append(observer)
        try:
            yield observer
        except BaseException:
            raise

    async def execute(self, ctx, command):
        raise RuntimeError("trace persistence is not available in S11A")
