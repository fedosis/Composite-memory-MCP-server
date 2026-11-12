from __future__ import annotations

import asyncio
import hashlib
import importlib
import inspect
import sqlite3
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from time import monotonic

import pytest
import pytest_asyncio
from sqlalchemy import event

from memory_server.application.gateway_retrieval import GatewayRetrievalPort
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import RequestContext
from memory_server.router.ranking import RankMerger, RankResult
from memory_server.transports.http.errors import HttpError
from tests.gateway.test_http_retrieval import live_server, prepare_indexed_registry, similarity_embedder  # noqa: F401


def _module():
    assert importlib.util.find_spec("memory_server.application.retrieval_trace"), "observer stages missing"
    return importlib.import_module("memory_server.application.retrieval_trace")


def _merger(tap):
    assert "tap" in inspect.signature(RankMerger).parameters, "rank tap observations missing"
    return RankMerger(tap=tap)


class Tap:
    def __init__(self, hostile=False):
        self.calls, self.hostile = [], hostile

    def record(self, name, *args):
        self.calls.append((name, *args))
        if self.hostile:
            for arg in args:
                if isinstance(arg, list):
                    arg[:] = [(None, "hostile", -999.0, -1)]
            raise RuntimeError("hostile tap")

    def raw(self, source, items):
        self.record("raw", source, items)

    def order(self, items):
        self.record("order", items)

    def dedup(self, kept, dropped):
        self.record("dedup", kept, dropped)


def test_rank_tap_raw_before_normalization():
    tap = Tap()
    result = _merger(tap).merge([RankResult("a", 7.5), RankResult("b", 2.5)], [], [])
    assert tap.calls[:3] == [
        ("raw", "fts", [(None, 7.5, 1), (None, 2.5, 2)]),
        ("raw", "semantic", []),
        ("raw", "graph", []),
    ]
    assert [row[2] for row in tap.calls[3][1]] == [1.0, 0.0]
    assert [r.score for r in result] == [1.0, 0.0]


def test_rank_tap_exception_does_not_change_result():
    def batch():
        return [RankResult("A", 7.5), RankResult(" a ", 2.5), RankResult("", 1.0)]

    baseline = RankMerger().merge(batch(), [], [])
    assert _merger(Tap(hostile=True)).merge(batch(), [], []) == baseline


@pytest_asyncio.fixture
async def gateway(tmp_path, request):
    request.getfixturevalue("similarity_embedder")
    registry = await prepare_indexed_registry(
        tmp_path,
        [
            {"id": "trace-alpha", "subject": "alpha", "predicate": "owns", "object": "beta"},
            {"id": "x" * 129, "subject": "alpha", "predicate": "owns", "object": "beta"},
        ],
    )
    async with live_server(tmp_path, registry=registry) as (holder, _, auth):
        deps = holder.app.dependencies
        principal = await deps.tokens.authenticate(auth["Authorization"][7:], now=datetime.now(UTC))
        ctx = RequestContext("trace-request", principal, "alpha", None, monotonic() + 120)
        yield deps.ports["search"], ctx


def _port(base, traces):
    assert "traces" in inspect.signature(GatewayRetrievalPort).parameters, "opt-in trace observer missing"
    return GatewayRetrievalPort(base.leases, base.bind_read, base.metrics, native=base.native, traces=traces)


def _command(mode="hybrid", trace=True):
    operation = "context" if mode == "context" else "search"
    payload = (
        d.ContextRequest(task="alpha", subject=None, trace=trace)
        if mode == "context"
        else d.SearchRequest(
            query="alpha owns beta",
            mode=mode,
            trace=trace,
            filters=d.SearchFilters(source=None, min_confidence=None),
        )
    )
    return d.COMMAND_TYPES[operation](operation=operation, profile_id="alpha", path_ids={}, payload=payload)


def _identity(result):
    if isinstance(result, Exception):
        return ("error", type(result).__name__)
    return [(h.memory.id, h.source, h.score, h.rank) for h in result.results], result.degraded


@pytest.mark.asyncio
async def test_observer_on_off_identity_keyword_and_hybrid(gateway):
    base, ctx = gateway
    module = _module()

    class Hostile(module._TraceObserver):
        def stage(self, name, items, status):
            super().stage(name, items, status)
            for item in items:
                item.matched_terms.append("HOSTILE-CANARY")
            items.clear()
            raise RuntimeError("hostile observer")

        def merged(self, items, deduplicated):
            super().merged(items, deduplicated)
            items.clear()
            deduplicated.clear()
            raise RuntimeError("hostile observer")

        def final(self, items):
            super().final(items)
            items.clear()
            raise RuntimeError("hostile observer")

    class HostilePort(module.InMemoryTracePort):
        @asynccontextmanager
        async def observe(self, ctx, operation):
            observer = Hostile(ctx, operation)
            self.observers.append(observer)
            yield observer

    for mode in ("keyword", "hybrid"):
        plain = await base.execute(ctx, _command(mode, False))
        assert plain.results
        for store in (HostilePort(), module.InMemoryTracePort()):
            try:
                traced = await _port(base, store).execute(ctx, _command(mode))
            except Exception as exc:
                # Success vs observer-induced failure is an identity violation,
                # not an unrelated failure before the comparison is reached.
                traced = exc
            assert _identity(traced) == _identity(plain)
            observer = store.observers[-1]
            assert observer.final_items and observer.merge
            assert all(not c.matched_terms for c in observer.merge + observer.final_items)
            assert [(c.memory_id, c.source) for c in observer.final_items] == [
                (h.memory.id if len(h.memory.id) <= 128 else None, h.source) for h in traced.results
            ]
            if mode == "keyword":
                assert all(c.raw_score is None and c.normalized_score is None for c in observer.merge)


@pytest.mark.asyncio
async def test_dedup_winner_and_source_pairs(gateway):
    base, ctx = gateway
    store = _module().InMemoryTracePort()
    result = await _port(base, store).execute(ctx, _command())
    observer = store.observers[-1]
    rows = [c for c in observer.merge if c.memory_id == "trace-alpha"]
    assert {c.source for c in rows} >= {"fts", "semantic"}
    assert any(c.source == "semantic" and c.dedup_winner_id == "trace-alpha" for c in rows)
    assert sum(c.memory_id == "trace-alpha" for c in observer.deduplicated) == 1
    assert sum(h.memory.id == "trace-alpha" for h in result.results) == 1
    assert [c.normalized_score for c in observer.merge] == sorted(
        (c.normalized_score for c in observer.merge), reverse=True
    )
    assert all(c.raw_score is not None and c.rank_before_merge >= 1 for c in rows)


@pytest.mark.asyncio
async def test_stage_status_matrix(gateway, monkeypatch):
    base, ctx = gateway
    store = _module().InMemoryTracePort()
    port = _port(base, store)
    async with base.leases.acquire(ctx) as scope:
        retrieval = scope.application.retrieval
        router = await retrieval._get_embedding_router()

        async def fail(**kwargs):
            raise RuntimeError("external vector backend failure")

        with monkeypatch.context() as patch:
            patch.setattr(router._vector_provider, "search", fail)
            result = await port.execute(ctx, _command())
            assert any(f.source == "semantic" and f.code == "E_VECTOR_BACKEND" for f in result.degraded)
            assert store.observers[-1].stage_status["semantic"] == "failed"
            count = len(store.observers)
            with pytest.raises(HttpError) as error:
                await port.execute(ctx, _command("semantic"))
            assert error.value.code == "E_VECTOR_BACKEND" and len(store.observers) == count
        with monkeypatch.context() as patch:
            patch.setattr(retrieval, "_get_embedding_router", None)
            result = await port.execute(ctx, _command())
            assert store.observers[-1].stage_status["semantic"] == "skipped"
            assert any(f.code == "E_CAPABILITY_UNAVAILABLE" for f in result.degraded)
            count = len(store.observers)
            with pytest.raises(HttpError) as error:
                await port.execute(ctx, _command("semantic"))
            assert error.value.code == "E_CAPABILITY_UNAVAILABLE" and len(store.observers) == count
    for mode in ("keyword", "context", "semantic", "graph"):
        await port.execute(ctx, _command(mode))
        observer = store.observers[-1]
        assert observer.stage_status == {"fts" if mode == "keyword" else mode: "ran"}
        if mode == "context":
            assert observer.stages == {"context": []} and observer.merge == observer.final_items == []


@pytest.mark.asyncio
async def test_concurrent_observers_do_not_share_state(gateway):
    base, ctx = gateway
    module = _module()
    store = module.InMemoryTracePort()
    port = _port(base, store)
    await asyncio.gather(port.execute(ctx, _command("keyword")), port.execute(ctx, _command("context")))
    first, second = store.observers
    assert first is not second and first.stages == {"fts": first.stages["fts"]}
    assert second.stages == {"context": []}
    first.stage("fts", [module._contribution("a", "fts")] * 101, "ran")
    first.final([])
    assert first.overflow and not second.overflow and first.final_items
    item = module._contribution("bad\n", "fts", float("inf"), float("nan"), -1, 0)
    assert item.memory_id is None and item.raw_score is None and item.rank_before_merge is None
    first.stage([], None, "ran")


@pytest.mark.asyncio
async def test_merge_emits_no_sql(gateway):
    base, ctx = gateway
    async with base.leases.acquire(ctx) as scope:
        engine = base.bind_read(scope).provider.engine.sync_engine
        statements = []

        def listener(*args):
            statements.append(args[2])

        event.listen(engine, "before_cursor_execute", listener)
        try:
            tap = _module()._ObserverTap(_module()._TraceObserver())
            _merger(tap).merge([RankResult("a", 7.5, metadata={"id": "a"})], [], [])
            tap.emit()
            assert statements == []
        finally:
            event.remove(engine, "before_cursor_execute", listener)


@pytest.mark.asyncio
async def test_trace_id_only_when_requested(gateway, tmp_path):
    base, ctx = gateway
    store = _module().InMemoryTracePort()
    port = _port(base, store)

    def snapshot():
        result = {}
        for path in tmp_path.rglob("*"):
            if path.is_file() and path.read_bytes()[:16] == b"SQLite format 3\x00":
                with sqlite3.connect(path) as conn:
                    result[str(path)] = tuple(conn.iterdump())
        return result

    before = snapshot()
    assert before
    for path, dump in before.items():
        digest = hashlib.sha256("\n".join(dump).encode()).hexdigest()
        print("NO_WRITE_BEFORE", path, digest)
    for mode in ("keyword", "hybrid", "context"):
        count = len(store.observers)
        assert (await port.execute(ctx, _command(mode, False))).trace_id is None
        assert len(store.observers) == count
        assert (await port.execute(ctx, _command(mode))).trace_id is None
        assert len(store.observers) == count + 1
    after = snapshot()
    assert after == before
    for path, dump in after.items():
        print("NO_WRITE_AFTER", path, hashlib.sha256("\n".join(dump).encode()).hexdigest())
    result = await base.execute(ctx, _command())
    assert result.trace_id is not None
