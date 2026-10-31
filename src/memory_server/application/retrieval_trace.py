"""Fail-quiet, in-memory retrieval trace observation for S11A."""

from __future__ import annotations

import math
from contextlib import asynccontextmanager
from typing import Any

from memory_server.gateway_contracts import dto as d


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

    def __init__(self, ctx=None, operation="search"):
        self.trace_id = None
        self.overflow = False
        self.stages: dict[str, list[d.TraceContribution]] = {}
        self.stage_status: dict[str, str] = {}
        self.merge: list[d.TraceContribution] = []
        self.deduplicated: list[d.TraceContribution] = []
        self.final_items: list[d.TraceContribution] = []
        self._contributions = 0
        self._max_contributions = 100
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
