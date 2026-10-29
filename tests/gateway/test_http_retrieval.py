"""S08a TCP regression tests added before implementation."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import sqlite3
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from time import monotonic
from types import SimpleNamespace

import pytest
from sqlalchemy import event, text
from storage.models.fact import FactORM
from storage.repositories.fact_repo import FactRepository

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import HttpConfig, RequestContext
from memory_server.providers.embedding_provider import MockEmbeddingProvider
from memory_server.runtime import runtime_factory
from memory_server.runtime.daemon_cli import run_daemon
from memory_server.transports.http.app import create_http_app
from tests.gateway.test_slice_runtime import (
    SocketServer,
    build_registry,
    free_port,
    http_request,
    seed_fact,
    wait_started,
)


@pytest.fixture(autouse=True)
def offline_embedder(monkeypatch):
    monkeypatch.setitem(runtime_factory._DEFAULT_RECIPES, "embedder", lambda runtime: MockEmbeddingProvider())


class SimilarityEmbeddingProvider:
    """Deterministic unit vectors for the real LanceDB/outbox path."""

    def __init__(self, vector_size=384, mapping=None, **_kwargs):
        self.vector_size = vector_size
        self.mapping = dict(mapping or {})
        self._fallback = self._basis(vector_size - 1)

    @staticmethod
    def _basis(index, size=384):
        vector = [0.0] * size
        vector[index] = 1.0
        return vector

    def embed(self, text):
        return list(self.mapping.get(text, self._fallback))

    def embed_batch(self, texts):
        return [self.embed(text) for text in texts]


@pytest.fixture
def similarity_embedder(monkeypatch):
    mapping = {
        "alpha owns beta": SimilarityEmbeddingProvider._basis(0),
        "alpha distractor one": SimilarityEmbeddingProvider._basis(1),
        "alpha distractor two": SimilarityEmbeddingProvider._basis(2),
        "graph source relates target": SimilarityEmbeddingProvider._basis(3),
    }
    monkeypatch.setitem(
        runtime_factory._DEFAULT_RECIPES,
        "embedder",
        lambda runtime: SimilarityEmbeddingProvider(mapping=mapping),
    )
    return mapping


async def _start(tmp_path, *, request_timeout_seconds: float = 30, retrieval_concurrency=4, registry=None):
    if registry is None:
        registry, _ = await build_registry(tmp_path)
    config = HttpConfig(
        control_path=tmp_path / "control" / "control.sqlite",
        host="127.0.0.1",
        port=free_port(),
        rate_per_minute=10000,
        rate_burst=1000,
        request_timeout_seconds=request_timeout_seconds,
        header_timeout_seconds=min(5, request_timeout_seconds),
        retrieval_concurrency=retrieval_concurrency,
    )
    holder = SimpleNamespace(server=None, app=None)

    def factory(server_config):
        holder.server = SocketServer(server_config)
        holder.app = server_config.app
        return holder.server

    task = asyncio.create_task(run_daemon(config, registry, factory=create_http_app, server_factory=factory))
    async with asyncio.timeout(5):
        while holder.server is None:
            if task.done():
                await task
            await asyncio.sleep(0.01)
    port = await wait_started(holder.server)
    now = datetime.now(UTC)
    created = holder.app.dependencies.tokens._bootstrap_create(
        d.TokenCreate(
            subject="s08a", profiles=["alpha", "beta"], permissions=["memory.read"], expires_at=now + timedelta(hours=1)
        ),
        now=now,
    )
    bearer = f"cmms_{created.token.id}.{created.secret}"
    principal = await holder.app.dependencies.tokens.authenticate(bearer, now=now)
    ctx = RequestContext("s08a-seed", principal, "alpha", None, now.timestamp() + 30)
    async with holder.app.dependencies.leases.acquire(ctx) as scope:
        await seed_fact(scope.application.read_resources(), "s08a-marker", "alpha")
    return holder, task, port, {"Authorization": "Bearer " + bearer}


async def prepare_indexed_registry(
    tmp_path, rows, *, registry=None, profile="alpha", graph_nodes=(), graph_edges=(), vector_points=()
):
    """Prepare real projections, close runtime, then let HTTP serve stable stores."""
    from memory_server.models import Fact, MemoryReceipt
    from memory_server.runtime import daemon_cli
    from memory_server.runtime.runtime_factory import RuntimeFactory
    from memory_server.runtime.runtime_manager import RuntimeManager

    if registry is None:
        registry, _ = await build_registry(tmp_path)
    binding = next(b for b in daemon_cli.load_bindings(registry) if b.profile.id == profile)
    manager = RuntimeManager(factory_builder=RuntimeFactory.create)
    runtime = await manager.start_profile(binding.descriptor, layout=binding.layout, config=binding.config)
    try:
        provider = runtime.dependencies.sqlite
        for row in rows:
            created = row.get("created_at", datetime(2026, 1, 1, tzinfo=UTC))
            ident = row["id"]
            subject, predicate, object_ = row["subject"], row["predicate"], row["object"]
            lifecycle = row.get("lifecycle_state", "active")
            confidence = row.get("confidence", 0.8)
            fact = Fact(
                id=ident,
                subject=subject,
                predicate=predicate,
                object=object_,
                dedup_key=ident,
                confidence=confidence,
                source="s08b-test-source",
                creator="s08b-test-creator",
                created_at=created,
                updated_at=created,
                lifecycle_state=lifecycle,
            )
            receipt = MemoryReceipt(
                id=ident,
                memory_type="fact",
                source="s08b-test-receipt",
                created_by="s08b-test-creator",
                timestamp=created,
                confidence=confidence,
                lifecycle_state=lifecycle,
            )
            outbox = (
                None
                if not row.get("index", True)
                else [
                    {
                        "record_type": "fact",
                        "record_id": ident,
                        "operation": "index_fact",
                        "payload": {
                            "subject": subject,
                            "predicate": predicate,
                            "object": object_,
                            "source": "s08b-test-source",
                        },
                    }
                ]
            )
            await provider.create_in_transaction(fact=fact, receipt=receipt, outbox_entries=outbox)
        worker = runtime.dependencies.outbox_worker
        if worker is not None:
            await worker.process_all_pending()
        # Bounded, real readback precedes closing the preparation runtime.
        for row in rows:
            if not row.get("index", True):
                continue
            deadline = monotonic() + 10
            while True:
                hits = await runtime.dependencies.vector.search(
                    collection=runtime.config.vector_collection,
                    vector=runtime.dependencies.embedder.embed(f"{row['subject']} {row['predicate']} {row['object']}"),
                    limit=100,
                )
                graph = runtime.dependencies.graph
                names = {node.name for node in graph.get_all_nodes()}
                if (
                    any(hit["id"] == str(uuid.uuid5(uuid.NAMESPACE_DNS, f"fact:{row['id']}")) for hit in hits)
                    and {row["subject"], row["object"]} <= names
                ):
                    break
                assert monotonic() < deadline, "real outbox projection readback failed"
                await asyncio.sleep(0.001)
        async with provider.engine.connect() as connection:
            statuses = (await connection.execute(text("SELECT status FROM outbox_entries"))).scalars().all()
            assert all(status == "completed" for status in statuses), statuses
        for node_id, node_type, name in graph_nodes:
            runtime.dependencies.graph.add_node(node_id, node_type, name)
        for source_id, target_id, relation in graph_edges:
            runtime.dependencies.graph.add_edge(source_id, target_id, relation)
        if vector_points:
            vector = runtime.dependencies.vector
            collection = runtime.config.vector_collection
            embedder = runtime.dependencies.embedder
            for point in vector_points:
                await vector.upsert(
                    collection=collection,
                    point_id=point["id"],
                    vector=embedder.embed(point["text"]),
                    payload=point["payload"],
                )
        async with provider.engine.begin() as connection:
            for row in rows:
                if row.get("delete_after_index"):
                    await connection.execute(text("DELETE FROM facts WHERE id=:id"), {"id": row["id"]})
    finally:
        await manager.close_all()
    return registry


@pytest.mark.asyncio
async def test_s08b_similarity_fixture_outbox_control(tmp_path, similarity_embedder):
    registry = await prepare_indexed_registry(
        tmp_path, [{"id": "s08b-control", "subject": "alpha", "predicate": "owns", "object": "beta"}]
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            router = await scope.application.retrieval._get_embedding_router()
            hits = await router._vector_provider.search(
                collection=router._collection,
                vector=similarity_embedder["alpha owns beta"],
                limit=3,
            )
            assert hits and hits[0]["id"] == str(uuid.uuid5(uuid.NAMESPACE_DNS, "fact:s08b-control"))
            assert hits[0]["score"] == pytest.approx(1.0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,query,expected_source",
    [
        ("semantic", "alpha owns beta", "semantic"),
        ("graph", "alpha beta", "graph"),
        ("hybrid", "alpha owns beta", "fts"),
    ],
)
@pytest.mark.parametrize("revealer", [False, True])
async def test_s08b_modes_seeded_fact(tmp_path, similarity_embedder, mode, query, expected_source, revealer):
    registry = await prepare_indexed_registry(
        tmp_path, [{"id": "s08b-seeded", "subject": "alpha", "predicate": "owns", "object": "beta"}]
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        if revealer:
            now = datetime.now(UTC)
            token = holder.app.dependencies.tokens._bootstrap_create(
                d.TokenCreate(
                    subject="revealer",
                    profiles=["alpha"],
                    permissions=["memory.read", "memory.reveal"],
                    expires_at=now + timedelta(hours=1),
                ),
                now=now,
            )
            auth = {"Authorization": f"Bearer cmms_{token.token.id}.{token.secret}"}
        status, body = await post(
            port,
            auth,
            query=query,
            mode=mode,
            limit=10,
            filters={"source": None, "min_confidence": None},
        )
        assert status == 200, body
        assert body["total"] == 1
        assert body["results"][0]["memory"]["id"] == "s08b-seeded"
        assert body["results"][0]["rank"] == 1
        assert body["trace_id"] is None
        assert body["results"][0]["memory"]["content"] is None
        assert body["results"][0]["memory"]["payload"] is None
        assert body["results"][0]["memory"]["masked"] is True
        assert body["degraded"] == []
        if expected_source is not None:
            assert body["results"][0]["source"] == expected_source
        else:
            assert body["results"][0]["source"] in {"fts", "semantic", "graph"}
        assert "s08b-test-source" not in json.dumps(body)
        assert body["results"][0]["score"] == pytest.approx(0.8 if mode == "graph" else 1.0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_hit",
    [
        {
            "id": "not-a-canonical-point",
            "score": 0.99,
            "payload": {"subject": "forged", "predicate": "owns", "object": "gamma", "memory_type": "fact"},
        },
        {
            "id": "override",
            "score": 0.99,
            "payload": {
                "id": "s08b-good",
                "subject": "alpha",
                "predicate": "owns",
                "object": "beta",
                "memory_type": "fact",
            },
        },
        {"id": "missing", "score": 0.99, "payload": {"memory_type": "fact", "subject": "alpha"}},
        {
            "id": "nonfinite",
            "score": math.nan,
            "payload": {"memory_type": "fact", "subject": "alpha", "predicate": "owns", "object": "beta"},
        },
        {
            "id": "belief",
            "score": 0.99,
            "payload": {"memory_type": "belief", "subject": "alpha", "predicate": "owns", "object": "beta"},
        },
        None,
        {
            "id": "bool-score",
            "score": True,
            "payload": {"memory_type": "fact", "subject": "alpha", "predicate": "owns", "object": "beta"},
        },
    ],
)
async def test_s08b_untrusted_projection_selective_drop(tmp_path, similarity_embedder, monkeypatch, bad_hit):
    registry = await prepare_indexed_registry(
        tmp_path,
        [
            {"id": "s08b-good", "subject": "alpha", "predicate": "owns", "object": "beta"},
            {"id": "s08b-forged-target", "subject": "forged", "predicate": "owns", "object": "gamma", "index": False},
        ],
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            router = await scope.application.retrieval._get_embedding_router()
            original = router._vector_provider.search

            async def polluted(*args, **kwargs):
                hits = await original(*args, **kwargs)
                return [*hits, bad_hit]

            monkeypatch.setattr(router._vector_provider, "search", polluted)
        status, body = await post(
            port,
            auth,
            query="alpha owns beta",
            mode="semantic",
            filters={"source": None, "min_confidence": None},
        )
        assert status == 200, body
        assert [hit["memory"]["id"] for hit in body["results"]] == ["s08b-good"]
        assert body["degraded"] == []


@pytest.mark.asyncio
async def test_s08b_foreign_same_uuid_two_roots_and_missing_identity(tmp_path, similarity_embedder):
    shared_id = "s08b-shared-uuid"
    registry = await prepare_indexed_registry(
        tmp_path,
        [
            {"id": shared_id, "subject": "beta", "predicate": "owns", "object": "gamma"},
        ],
        profile="beta",
    )
    registry = await prepare_indexed_registry(
        tmp_path,
        [
            {"id": "s08b-alpha-control", "subject": "alpha", "predicate": "owns", "object": "beta"},
            {"id": shared_id, "subject": "alpha", "predicate": "controls", "object": "beta"},
            {
                "id": "s08b-stale",
                "subject": "alpha",
                "predicate": "stale",
                "object": "beta",
                "delete_after_index": True,
            },
        ],
        registry=registry,
        vector_points=[
            {
                "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"fact:{shared_id}")),
                "text": "beta owns gamma",
                "payload": {"subject": "beta", "predicate": "owns", "object": "gamma", "memory_type": "fact"},
            },
            {
                "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "fact:s08b-missing")),
                "text": "alpha owns beta",
                "payload": {"subject": "alpha", "predicate": "owns", "object": "beta", "memory_type": "fact"},
            },
        ],
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        status, body = await post(
            port,
            auth,
            profile="alpha",
            query="alpha owns beta",
            mode="semantic",
            filters={"source": None, "min_confidence": None},
        )
        assert status == 200, body
        assert [hit["memory"]["id"] for hit in body["results"]] == ["s08b-alpha-control"], body
        assert body["degraded"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["graph", "hybrid"])
async def test_s08b_graph_ambiguous_triple_expands(tmp_path, similarity_embedder, mode):
    registry = await prepare_indexed_registry(
        tmp_path,
        [
            {"id": "s08b-active", "subject": "alpha", "predicate": "owns", "object": "beta"},
            {
                "id": "s08b-inactive",
                "subject": "alpha",
                "predicate": "owns",
                "object": "beta",
                "lifecycle_state": "rejected",
                "index": False,
                "created_at": datetime(2025, 1, 1, tzinfo=UTC),
            },
        ],
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        for include_inactive, expected in ((False, ["s08b-active"]), (True, ["s08b-active", "s08b-inactive"])):
            status, body = await post(
                port,
                auth,
                query="alpha beta",
                mode=mode,
                include_inactive=include_inactive,
                limit=2,
                filters={"source": None, "min_confidence": None},
            )
            assert status == 200, body
            assert [hit["memory"]["id"] for hit in body["results"]] == expected
            assert body["degraded"] == []


@pytest.mark.asyncio
async def test_s08b_graph_expansion_timestamp_tie(tmp_path, similarity_embedder):
    rows = [
        dict(id=ident, subject="alpha", predicate="owns", object="beta", lifecycle_state=state, index=state == "active")
        for ident, state in (("a-active", "active"), ("b-inactive", "rejected"), ("c-inactive", "rejected"))
    ]
    registry = await prepare_indexed_registry(tmp_path, rows)
    async with live_server(tmp_path, registry=registry) as (_, port, auth):
        status, body = await post(port, auth, query="alpha beta", mode="graph", include_inactive=True)
        assert status == 200, body
        assert [hit["memory"]["id"] for hit in body["results"]] == ["a-active", "b-inactive", "c-inactive"]
        assert all(hit["score"] == 0.8 for hit in body["results"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,mode,code",
    [
        ("vector", "semantic", "E_VECTOR_BACKEND"),
        ("embed", "semantic", "E_VECTOR_BACKEND"),
        ("graph", "graph", "E_GRAPH_BACKEND"),
    ],
)
async def test_s08b_typed_backend_failure(tmp_path, similarity_embedder, monkeypatch, failure, mode, code):
    registry = await prepare_indexed_registry(
        tmp_path, [{"id": "s08b-fault", "subject": "alpha", "predicate": "owns", "object": "beta"}]
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            if failure == "embed":
                router = await scope.application.retrieval._get_embedding_router()

                def embed_failed(*args, **kwargs):
                    raise RuntimeError("embedding failure")

                monkeypatch.setattr(router._embedder, "embed", embed_failed)
            elif mode == "semantic":
                router = await scope.application.retrieval._get_embedding_router()

                async def failed(*args, **kwargs):
                    from memory_server.providers.exceptions import ProviderSearchError

                    raise ProviderSearchError("backend failure")

                monkeypatch.setattr(router._vector_provider, "search", failed)
            else:
                graph_service = scope.application.graph
                graph_router = await graph_service._get_graph_router()
                monkeypatch.setattr(
                    graph_router.graph,
                    "get_all_nodes",
                    lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("graph failure")),
                )
        status, body = await post(
            port,
            auth,
            query="alpha owns beta",
            mode=mode,
            filters={"source": None, "min_confidence": None},
        )
        assert status == 503, body
        assert body["error"]["code"] == code
        status, body = await post(port, auth, query="alpha owns beta", mode="hybrid")
        assert status == 200, body
        assert body["results"][0]["memory"]["id"] == "s08b-fault"
        assert body["degraded"] == [{"source": mode, "code": code}]


@pytest.mark.asyncio
async def test_s08b_healthy_empty_vs_failed(tmp_path, similarity_embedder):
    async with live_server(tmp_path) as (holder, port, auth):
        for mode in ("semantic", "graph", "hybrid"):
            status, body = await post(
                port,
                auth,
                query="no such indexed fact",
                mode=mode,
                filters={"source": None, "min_confidence": None},
            )
            assert status == 200, body
            assert body["results"] == [] and body["total"] == 0
            assert body["degraded"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["semantic", "graph"])
async def test_s08b_post_resolve_filter_matrix(tmp_path, similarity_embedder, monkeypatch, mode):
    registry = await prepare_indexed_registry(
        tmp_path, [{"id": "s08b-high", "subject": "alpha", "predicate": "owns", "object": "beta", "confidence": 0.9}]
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            router = await scope.application.retrieval._get_embedding_router()
            calls = 0
            graph_router = await scope.application.graph._get_graph_router()
            original = router._vector_provider.search

            async def counted(*args, **kwargs):
                nonlocal calls
                calls += 1
                return await original(*args, **kwargs)

            if mode == "semantic":
                monkeypatch.setattr(router._vector_provider, "search", counted)
            else:
                original_graph = graph_router.graph.get_all_nodes

                def graph_counted(*args, **kwargs):
                    nonlocal calls
                    calls += 1
                    return original_graph(*args, **kwargs)

                monkeypatch.setattr(graph_router.graph, "get_all_nodes", graph_counted)
        for filters, expected in (
            ({"min_confidence": 0.95}, 0),
            ({"min_confidence": 0.8}, 1),
            ({"memory_types": ["belief"]}, 0),
            ({"lifecycle_states": ["rejected"]}, 0),
        ):
            status, body = await post(
                port,
                auth,
                query="alpha owns beta",
                mode=mode,
                filters={"source": None, "min_confidence": None, **filters},
            )
            assert status == 200 and body["total"] == expected, body
            assert body["degraded"] == []
        assert calls >= 4
        status, body = await post(
            port,
            auth,
            query="alpha owns beta",
            mode="semantic",
            filters={"source": "forbidden", "min_confidence": None},
        )
        assert status == 422 and body["error"]["code"] == "E_VALIDATION"


@pytest.mark.asyncio
async def test_s08b_limits_scores_rank(tmp_path, similarity_embedder):
    registry = await prepare_indexed_registry(
        tmp_path,
        [
            {"id": ident, "subject": "alpha", "predicate": "owns", "object": obj}
            for ident, obj in (("s08b-one", "beta"), ("s08b-two", "gamma"), ("s08b-three", "delta"))
        ],
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        status, body = await post(
            port,
            auth,
            query="alpha owns beta",
            mode="semantic",
            limit=1,
            filters={"source": None, "min_confidence": None},
        )
        assert status == 200 and body["total"] == 1 and body["results"][0]["rank"] == 1, body
        assert body["results"][0]["memory"]["id"] == "s08b-one"
        for limit in (0, 101):
            status, body = await post(
                port,
                auth,
                query="alpha owns beta",
                mode="semantic",
                limit=limit,
                filters={"source": None, "min_confidence": None},
            )
            assert status == 422 and body["error"]["code"] == "E_VALIDATION"


@pytest.mark.asyncio
async def test_s08b_all_failed_is_storage_not_empty(tmp_path, similarity_embedder, monkeypatch):
    from sqlalchemy.exc import SQLAlchemyError

    registry = await prepare_indexed_registry(
        tmp_path, [{"id": "s08b-all-failed", "subject": "alpha", "predicate": "owns", "object": "beta"}]
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            router = await scope.application.retrieval._get_embedding_router()

            async def vector_failed(*args, **kwargs):
                raise RuntimeError("vector failed")

            monkeypatch.setattr(router._vector_provider, "search", vector_failed)
            graph_router = await scope.application.graph._get_graph_router()
            monkeypatch.setattr(
                graph_router.graph,
                "get_all_nodes",
                lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("graph failed")),
            )
            provider = scope.application.read_resources().provider
            original = provider.search_facts_in_session

            async def storage_failed(*args, **kwargs):
                raise SQLAlchemyError("storage failed")

            monkeypatch.setattr(provider, "search_facts_in_session", storage_failed)
        status, body = await post(
            port,
            auth,
            query="alpha owns beta",
            mode="hybrid",
            filters={"source": None, "min_confidence": None},
        )
        assert status == 503 and body["error"]["code"] == "E_STORAGE", body
        assert "results" not in body and body.get("degraded") is None
        monkeypatch.setattr(provider, "search_facts_in_session", original)
        for query in ("alpha owns beta", "healthy-empty-fts"):
            status, body = await post(port, auth, query=query, mode="hybrid")
            assert status == 200, body
            assert body["degraded"] == [
                {"source": "semantic", "code": "E_VECTOR_BACKEND"},
                {"source": "graph", "code": "E_GRAPH_BACKEND"},
            ]


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["vector", "graph"])
async def test_s08b_projection_absence_layout_two_phase(tmp_path, similarity_embedder, monkeypatch, missing):
    # Phase 1 must finish real outbox indexing before either projection is hidden.
    registry = await prepare_indexed_registry(
        tmp_path, [{"id": "s08b-absence", "subject": "alpha", "predicate": "owns", "object": "beta"}]
    )

    # Phase 2 reuses those stores and changes only the captured layout flags.
    from memory_server.runtime import daemon
    from memory_server.runtime.runtime_factory import RuntimeFactory
    from memory_server.runtime.runtime_manager import RuntimeManager

    original_init = daemon.Daemon.__init__

    def patched_init(self, registry_obj, *, policies, manager=None, clock=None):
        def builder(layout, **kwargs):
            return RuntimeFactory.create(layout, overrides={missing: lambda runtime: None}, **kwargs)

        manager = RuntimeManager(factory_builder=builder)
        original_init(
            self, registry_obj, policies=policies, manager=manager, **({} if clock is None else {"clock": clock})
        )

    monkeypatch.setattr(daemon.Daemon, "__init__", patched_init)
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        status, body = await post(
            port,
            auth,
            query="alpha owns beta",
            mode="semantic",
            filters={"source": None, "min_confidence": None},
        )
        if missing == "vector":
            assert status == 503 and body["error"]["code"] == "E_CAPABILITY_UNAVAILABLE", body
            status, body = await post(
                port,
                auth,
                query="alpha beta",
                mode="graph",
                filters={"source": None, "min_confidence": None},
            )
            assert status == 200 and body["results"][0]["memory"]["id"] == "s08b-absence", body
            status, body = await post(
                port,
                auth,
                query="alpha owns beta",
                mode="hybrid",
                filters={"source": None, "min_confidence": None},
            )
            assert status == 200 and body["degraded"] == [{"source": "semantic", "code": "E_CAPABILITY_UNAVAILABLE"}], (
                body
            )
        else:
            assert status == 200 and body["results"][0]["memory"]["id"] == "s08b-absence", body
            status, body = await post(
                port,
                auth,
                query="alpha beta",
                mode="graph",
                filters={"source": None, "min_confidence": None},
            )
            assert status == 503 and body["error"]["code"] == "E_CAPABILITY_UNAVAILABLE", body
            status, body = await post(
                port,
                auth,
                query="alpha owns beta",
                mode="hybrid",
                filters={"source": None, "min_confidence": None},
            )
            assert status == 200 and body["degraded"] == [{"source": "graph", "code": "E_CAPABILITY_UNAVAILABLE"}], body


@pytest.mark.asyncio
async def test_s08b_sources_stage_audit_contract(tmp_path, similarity_embedder, monkeypatch):
    registry = await prepare_indexed_registry(
        tmp_path, [{"id": "s08b-stage", "subject": "alpha", "predicate": "owns", "object": "beta"}]
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        deps = holder.app.dependencies
        active, stages, acquired = set(), [], []
        acquire, measured = deps.leases.acquire, deps.metrics.retrieval_completed

        @asynccontextmanager
        async def watched(ctx):
            with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
                assert db.execute(
                    "SELECT state FROM access_audit WHERE request_id=?", (ctx.request_id,)
                ).fetchone() == ("intent",)
            async with acquire(ctx) as scope:
                acquired.append(ctx.request_id)
                active.add(ctx.request_id)
                try:
                    yield scope
                finally:
                    active.remove(ctx.request_id)

        def record(ctx, stage, duration, code):
            assert ctx.request_id in active and duration >= 0
            stages.append((ctx.request_id, stage, code))
            measured(ctx, stage, duration, code)

        monkeypatch.setattr(deps.leases, "acquire", watched)
        monkeypatch.setattr(deps.metrics, "retrieval_completed", record)
        for mode, expected in (
            ("semantic", {"semantic", "merge"}),
            ("graph", {"graph", "merge"}),
            ("hybrid", {"fts", "semantic", "graph", "merge"}),
        ):
            status, body = await post(port, auth, query="alpha owns beta", mode=mode)
            assert status == 200, body
            ident = body["request_id"]
            observations = [(stage, code) for request, stage, code in stages if request == ident]
            assert len(observations) == len(expected) and {stage for stage, _ in observations} == expected
            assert all(code is None for _, code in observations)
            assert acquired.count(ident) == 1
            with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
                assert db.execute("SELECT state FROM access_audit WHERE request_id=?", (ident,)).fetchall() == [
                    ("terminal",)
                ]


@pytest.mark.asyncio
async def test_s08b_graph_untrusted_relations_are_not_memories(tmp_path, similarity_embedder):
    registry = await prepare_indexed_registry(
        tmp_path,
        [
            {"id": "s08b-graph-control", "subject": "alpha", "predicate": "owns", "object": "beta"},
            {
                "id": "s08b-decision-trap",
                "subject": "trapdecision",
                "predicate": "owns",
                "object": "beta",
                "index": False,
            },
            {"id": "s08b-skill-trap", "subject": "trapskill", "predicate": "owns", "object": "beta", "index": False},
            {"id": "s08b-case-collision", "subject": "ALPHA", "predicate": "owns", "object": "beta", "index": False},
        ],
        graph_nodes=[
            ("trapdecision", "decision", "trapdecision"),
            ("trapskill", "skill", "trapskill"),
            ("uuid-trap", "entity", "s08b-graph-control"),
        ],
        graph_edges=[
            ("trapdecision", "beta", "owns"),
            ("trapskill", "beta", "owns"),
            ("uuid-trap", "beta", "owns"),
            ("beta", "alpha", "owns"),
        ],
    )
    async with live_server(tmp_path, registry=registry) as (_, port, auth):
        status, body = await post(
            port, auth, query="alpha beta trapdecision trapskill s08b-graph-control", mode="graph"
        )
        assert status == 200, body
        assert [hit["memory"]["id"] for hit in body["results"]] == ["s08b-graph-control"]
        assert body["degraded"] == []


@pytest.mark.asyncio
async def test_s08b_repeated_cancel_retains_native_lease(tmp_path, similarity_embedder, monkeypatch):
    import threading

    registry = await prepare_indexed_registry(
        tmp_path, [{"id": "s08b-cancel", "subject": "alpha", "predicate": "owns", "object": "beta"}]
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        status, body = await post(port, auth, query="alpha owns beta", mode="semantic")
        assert status == 200, body
        entered, release = threading.Event(), threading.Event()
        async with await read_scope(holder, auth) as scope:
            runtime = scope.runtime
            router = await scope.application.retrieval._get_embedding_router()
            embed = router._embedder.embed

            def held(text):
                entered.set()
                release.wait()
                return embed(text)

            monkeypatch.setattr(router._embedder, "embed", held)
        principal = await holder.app.dependencies.tokens.authenticate(
            auth["Authorization"].removeprefix("Bearer "), now=datetime.now(UTC)
        )
        ctx = RequestContext("s08b-repeat-cancel", principal, "alpha", None, monotonic() + 30)
        command = d.COMMAND_TYPES["search"](
            operation="search",
            profile_id="alpha",
            path_ids={},
            payload=d.SearchRequest(
                query="alpha owns beta", mode="semantic", filters=d.SearchFilters(source=None, min_confidence=None)
            ),
        )
        task = asyncio.create_task(holder.app.dependencies.ports["search"].execute(ctx, command))
        try:
            async with asyncio.timeout(5):
                while not entered.is_set():
                    await asyncio.sleep(0.001)
            captured = runtime.owned_snapshot()[2]
            for _ in range(2):
                task.cancel()
                for _ in range(5):
                    await asyncio.sleep(0)
                assert not task.done()
                assert holder.app.dependencies.leases.snapshot()["alpha"].admissions == 1
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert all(record.state == "TERMINATED" for record in captured)
        assert holder.app.dependencies.leases.snapshot()["alpha"].admissions == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["getter", "vector-submit"])
async def test_s08b_runtime_authority_is_not_backend_failure(tmp_path, similarity_embedder, monkeypatch, boundary):
    registry = await prepare_indexed_registry(
        tmp_path, [{"id": "s08b-ready", "subject": "alpha", "predicate": "owns", "object": "beta"}]
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        status, body = await post(port, auth, query="alpha owns beta", mode="semantic")
        assert status == 200, body
        from memory_server.runtime.errors import RuntimeNotReadyError

        async with await read_scope(holder, auth) as scope:
            retrieval = scope.application.retrieval

            async def not_ready():
                raise RuntimeNotReadyError()

            if boundary == "getter":
                monkeypatch.setattr(retrieval, "_get_embedding_router", not_ready)
            else:
                router = await retrieval._get_embedding_router()

                async def submit_not_ready(*args, **kwargs):
                    raise RuntimeNotReadyError()

                monkeypatch.setattr(router._vector_provider, "_native_submitter", submit_not_ready)
        for mode in ("semantic", "hybrid"):
            status, body = await post(port, auth, query="alpha owns beta", mode=mode)
            assert status == 503 and body["error"]["code"] == "E_RUNTIME_NOT_READY", body


@pytest.mark.asyncio
async def test_s08a_metadata_filter_accepted_over_tcp(tmp_path):
    holder, task, port, auth = await _start(tmp_path)
    try:
        status, body = await http_request(
            port,
            "POST",
            "/api/v1/profiles/alpha/search",
            body=b'{"query":"alpha","filters":{"source":null,"min_confidence":0.0}}',
            headers={**auth, "Content-Type": "application/json"},
        )
        assert status == 200, body
        assert body["total"] == 1
    finally:
        holder.server.should_exit = True
        await asyncio.wait_for(task, 10)


@pytest.mark.asyncio
async def test_s08b_case3b_inactive_window_keeps_active_and_newest_eight(tmp_path, similarity_embedder):
    rows = [{"id": "s08b-window-active", "subject": "alpha", "predicate": "owns", "object": "beta"}]
    rows += [
        {
            "id": f"s08b-window-inactive-{index}",
            "subject": "alpha",
            "predicate": "owns",
            "object": "beta",
            "lifecycle_state": "rejected",
            "index": False,
            "created_at": datetime(2025, 1, 1, tzinfo=UTC) + timedelta(days=index),
        }
        for index in range(9)
    ]
    registry = await prepare_indexed_registry(
        tmp_path,
        rows,
        vector_points=[
            {
                "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"fact:s08b-window-inactive-{index}")),
                "text": "alpha owns beta",
                "payload": {"subject": "alpha", "predicate": "owns", "object": "beta", "memory_type": "fact"},
            }
            for index in range(9)
        ],
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        status, body = await post(
            port,
            auth,
            query="alpha beta",
            mode="graph",
            include_inactive=True,
            limit=100,
            filters={"source": None, "min_confidence": None},
        )
        assert status == 200, body
        ids = [hit["memory"]["id"] for hit in body["results"]]
        assert "s08b-window-active" in ids
        assert "s08b-window-inactive-0" not in ids
        assert {ident for ident in ids if ident.startswith("s08b-window-inactive-")} == {
            f"s08b-window-inactive-{index}" for index in range(1, 9)
        }
        for inactive, expected_count in ((False, 1), (True, 9)):
            status, body = await post(
                port, auth, query="alpha owns beta", mode="semantic", include_inactive=inactive, limit=100
            )
            assert status == 200 and body["total"] == expected_count, body
            ids = {hit["memory"]["id"] for hit in body["results"]}
            assert "s08b-window-active" in ids and "s08b-window-inactive-0" not in ids


@pytest.mark.asyncio
async def test_s08b_graph_budget_refuses_501_nodes(tmp_path, similarity_embedder):
    registry = await prepare_indexed_registry(
        tmp_path, [], graph_nodes=[(f"s08b-budget-{index}", "entity", f"budget-{index}") for index in range(501)]
    )
    async with live_server(tmp_path, registry=registry) as (holder, port, auth):
        status, body = await post(
            port,
            auth,
            query="budget-1",
            mode="graph",
            filters={"source": None, "min_confidence": None},
        )
        assert status == 503 and body["error"]["code"] == "E_GRAPH_BACKEND", body
        status, body = await post(
            port,
            auth,
            query="budget-1",
            mode="hybrid",
            filters={"source": None, "min_confidence": None},
        )
        assert status == 200, body
        assert body["degraded"] == [{"source": "graph", "code": "E_GRAPH_BACKEND"}]
        # Only reached after the real HTTP bounded path succeeds; baseline RED
        # remains assertion-level rather than importing an absent production API.
        from memory_server.providers.graph_provider import GraphBudgetExceeded, SimpleGraph
        from memory_server.router.graph_router import GraphRouter

        caps = dict(max_nodes=500, max_edges=1000, max_pairs=100, max_depth=4)

        class NoValues(dict):
            def values(self):
                raise AssertionError("materialized oversized graph")

        oversized = SimpleGraph()
        for index in range(501):
            oversized.add_node(f"n{index}", "entity", f"name{index}")
        oversized._nodes = NoValues(oversized._nodes)
        with pytest.raises(GraphBudgetExceeded) as caught:
            GraphRouter(graph=oversized).query_bounded("none", **caps)
        assert (caught.value.dimension, caught.value.limit, caught.value.attempted) == ("nodes", 500, 501)
        for size in (499, 500):
            graph = SimpleGraph()
            for index in range(size):
                graph.add_node(f"n{index}", "entity", "focus" if index == 0 else f"name{index}")
            assert len(GraphRouter(graph=graph).query_bounded("focus", **caps)["entities"]) == 1
        graph = SimpleGraph()
        graph.add_node("a", "entity", "focus")
        graph.add_node("b", "entity", "other")
        for index in range(1001):
            graph.add_edge("a", "b", f"r{index}")
        with pytest.raises(GraphBudgetExceeded) as caught:
            GraphRouter(graph=graph).query_bounded("focus", **caps)
        assert (caught.value.dimension, caught.value.limit, caught.value.attempted) == ("edges", 1000, 1001)
        with pytest.raises(GraphBudgetExceeded) as caught:
            graph.get_neighbors("a", edge_limit=1000)
        assert (caught.value.dimension, caught.value.limit, caught.value.attempted) == ("edges", 1000, 1001)
        graph = SimpleGraph()
        for index in range(15):
            graph.add_node(f"n{index}", "entity", f"match{index}")
        with pytest.raises(GraphBudgetExceeded) as caught:
            GraphRouter(graph=graph).query_bounded(" ".join(f"match{i}" for i in range(15)), **caps)
        assert (caught.value.dimension, caught.value.limit, caught.value.attempted) == ("pairs", 100, 101)
        graph = SimpleGraph()
        for index in range(6):
            graph.add_node(f"n{index}", "entity", f"chain{index}")
        for index in range(5):
            graph.add_edge(f"n{index}", f"n{index + 1}", "r")
        with pytest.raises(GraphBudgetExceeded) as caught:
            GraphRouter(graph=graph).query_bounded("chain0 chain5", **caps)
        assert (caught.value.dimension, caught.value.limit, caught.value.attempted) == ("depth", 4, 5)
        graph = SimpleGraph()
        for ident in ("s", "a", "b", "t"):
            graph.add_node(ident, "entity", ident)
        for source, target in (("s", "a"), ("s", "b"), ("a", "t"), ("b", "t")):
            graph.add_edge(source, target, "r")
        assert [[n.id for n in path] for path in graph.find_path("s", "t", node_limit=10)] == [
            ["s", "a", "t"],
            ["s", "b", "t"],
        ]
        dense = SimpleGraph()
        for ident in ["source", "target", *[f"left{i}" for i in range(16)], *[f"right{i}" for i in range(16)]]:
            dense.add_node(ident, "entity", ident)
        for i in range(16):
            dense.add_edge("source", f"left{i}", "r")
            dense.add_edge(f"right{i}", "target", "r")
            for j in range(16):
                dense.add_edge(f"left{i}", f"right{j}", "r")
        with pytest.raises(GraphBudgetExceeded) as caught:
            GraphRouter(graph=dense).query_bounded("source target", **caps)
        assert (caught.value.dimension, caught.value.limit, caught.value.attempted) == ("nodes", 500, 501)


@pytest.mark.asyncio
@pytest.mark.parametrize("native_path", ["embed", "graph", "vector"])
async def test_s08b_deadline_late_native_custody(tmp_path, similarity_embedder, monkeypatch, native_path):
    import threading

    registry = await prepare_indexed_registry(
        tmp_path, [{"id": "s08b-late", "subject": "alpha", "predicate": "owns", "object": "beta"}]
    )
    holder, task, port, auth = await _start(
        tmp_path, request_timeout_seconds=0.2, retrieval_concurrency=1, registry=registry
    )
    entered = threading.Event()
    release = threading.Event()
    request = None
    try:
        status, body = await post(port, auth, query="alpha owns beta", mode="semantic")
        assert status == 200, body
        async with await read_scope(holder, auth) as scope:
            runtime = scope.runtime
            original_submit = runtime.submit_native
            gate_used = False

            async def wrapped_submit(function, /, *args, kind, **kwargs):
                nonlocal gate_used
                should_gate = (
                    (native_path == "embed" and kind == "retrieval-embed")
                    or (native_path == "vector" and "lancedb" in kind)
                    or (native_path == "graph" and not gate_used)
                )
                if not should_gate:
                    return await original_submit(function, *args, kind=kind, **kwargs)

                gate_used = True

                def held(*call_args, **call_kwargs):
                    entered.set()
                    while not release.wait(0.01):
                        pass
                    return function(*call_args, **call_kwargs)

                return await original_submit(held, *args, kind=kind, **kwargs)

            monkeypatch.setattr(runtime, "submit_native", wrapped_submit)
            if native_path == "vector":
                # The provider captured its bound submitter at construction.
                # Wrap that real boundary too, not a fake async search result.
                router = await scope.application.retrieval._get_embedding_router()
                monkeypatch.setattr(router._vector_provider, "_native_submitter", wrapped_submit)

        request = asyncio.create_task(
            post(
                port,
                auth,
                query="alpha owns beta" if native_path != "graph" else "alpha beta",
                mode="semantic" if native_path != "graph" else "graph",
                filters={"source": None, "min_confidence": None},
            )
        )
        async with asyncio.timeout(5):
            while not entered.is_set():
                await asyncio.sleep(0.01)
        deadline = monotonic() + 0.3
        while monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert not request.done()
        observation = holder.app.dependencies.leases.snapshot()["alpha"]
        assert observation.admissions == 1
        captured = runtime.owned_snapshot()[2]
        assert any(record.state != "TERMINATED" for record in captured)
        with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
            assert db.execute("SELECT count(*) FROM access_audit WHERE state='intent'").fetchone()[0] == 1

        concurrent_status, _ = await post(
            port,
            auth,
            query="alpha owns beta",
            mode="keyword",
            filters={"source": None, "min_confidence": None},
        )
        assert concurrent_status == 429
    finally:
        # This must run even when an assertion above fails, or the native thread
        # would retain the lease and prevent daemon shutdown.
        release.set()
        if request is not None:
            status, body = await request
            assert status == 504, body
            assert all(record.state == "TERMINATED" for record in captured)
            assert holder.app.dependencies.leases.snapshot()["alpha"].admissions == 0
            with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
                state = db.execute(
                    "SELECT state FROM access_audit WHERE request_id=?", (body["error"]["request_id"],)
                ).fetchone()
            assert state == ("terminal",)
            assert body["error"]["code"] == "E_TIMEOUT"
            status, body = await post(
                port,
                auth,
                query="alpha",
                mode="keyword",
                filters={"source": None, "min_confidence": None},
            )
            assert status == 200, body
        holder.server.should_exit = True
        await asyncio.wait_for(task, 10)


@pytest.mark.asyncio
async def test_s08b_filter_and_trace_rejections_remain_prelease(tmp_path, similarity_embedder, monkeypatch):
    async with live_server(tmp_path) as (holder, port, auth):
        calls = []
        original = holder.app.dependencies.leases.acquire

        def counted(ctx):
            calls.append(ctx.request_id)
            return original(ctx)

        monkeypatch.setattr(holder.app.dependencies.leases, "acquire", counted)
        for payload in (
            {
                "query": "alpha owns beta",
                "mode": "semantic",
                "trace": True,
                "filters": {"source": None, "min_confidence": None},
            },
            {
                "query": "alpha owns beta",
                "mode": "semantic",
                "filters": {"source": "forbidden", "min_confidence": None},
            },
            {
                "query": "alpha owns beta",
                "mode": "semantic",
                "filters": {"tags": ["forbidden"], "source": None, "min_confidence": None},
            },
        ):
            status, body = await post(port, auth, **payload)
            assert status in {422, 503}
            assert body["error"]["code"] in {"E_VALIDATION", "E_CAPABILITY_UNAVAILABLE"}
        assert calls == []


@pytest.mark.asyncio
async def test_s08a_context_mounted_over_tcp(tmp_path):
    from memory_server.transports.http.routes import retrieval

    holder, task, port, auth = await _start(tmp_path)
    try:
        parsers = {"search": retrieval._parse_search, "context": retrieval._parse_context}
        mounted = [spec for spec in holder.app.read_routes if spec.operation in parsers]
        assert {spec.operation for spec in mounted} == set(parsers)
        for spec in mounted:
            # Anchor the expected parser independently: the shared map itself may be swapped.
            canonical = replace(retrieval.RETRIEVAL_ROUTE_SPECS[spec.operation], parse=parsers[spec.operation])
            assert spec.parse is canonical.parse
        status, body = await http_request(
            port,
            "POST",
            "/api/v1/profiles/alpha/context",
            body=b'{"task":"alpha","subject":null,"max_results":20,"include_inactive":false,"trace":false}',
            headers={**auth, "Content-Type": "application/json"},
        )
        assert status == 200, body
        assert body["package"]["total"] == 1
        assert len(body["memories"]) == 1
    finally:
        holder.server.should_exit = True
        await asyncio.wait_for(task, 10)


@asynccontextmanager
async def live_server(tmp_path, *, registry=None):
    holder, task, port, auth = await _start(tmp_path, registry=registry)
    try:
        yield holder, port, auth
    finally:
        holder.server.should_exit = True
        await asyncio.wait_for(task, 10)


async def read_scope(holder, auth, profile="alpha"):
    principal = await holder.app.dependencies.tokens.authenticate(
        auth["Authorization"].removeprefix("Bearer "), now=datetime.now(UTC)
    )
    return holder.app.dependencies.leases.acquire(
        RequestContext("s08a-fixture", principal, profile, None, monotonic() + 60)
    )


async def post(port, auth, operation="search", profile="alpha", **changes):
    payload = (
        {"query": "needle", "mode": "keyword", "filters": {"source": None, "min_confidence": None}}
        if operation == "search"
        else {"task": "needle", "subject": None}
    )
    payload.update(changes)
    response_headers = {}
    status, body = await http_request(
        port,
        "POST",
        f"/api/v1/profiles/{profile}/{operation}",
        body=json.dumps(payload).encode(),
        headers={**auth, "Content-Type": "application/json"},
        response_headers=response_headers,
    )
    assert response_headers["cache-control"] == "no-store"
    assert response_headers["x-request-id"] == body.get("request_id", body.get("error", {}).get("request_id"))
    assert "CANARY" not in json.dumps(response_headers)
    return status, body


async def insert_facts(resources, rows):
    async with resources.session_factory() as session, session.begin():
        for index, row in enumerate(rows):
            values = dict(
                id=f"f{index}",
                subject="needle",
                predicate="CANARY_predicate",
                object="needle",
                dedup_key=f"CANARY_dedup{index}",
                confidence=0.8,
                source="CANARY_source",
                creator="CANARY_creator",
                lifecycle_state="active",
                verification_status="candidate",
                version="1",
                created_at=datetime(2026, 1, 1) + timedelta(seconds=index),
                updated_at=datetime(2026, 1, 1) + timedelta(seconds=index),
            )
            values.update(row)
            session.add(FactORM(**values))


async def seed_indexed_fact(scope, ident, subject, predicate, object_, *, confidence=0.8, lifecycle_state="active"):
    """Write canonical SQLite plus the real transactional indexing outbox."""
    from memory_server.models import Fact, MemoryReceipt

    created = datetime(2026, 1, 1, tzinfo=UTC)
    fact = Fact(
        id=ident,
        subject=subject,
        predicate=predicate,
        object=object_,
        dedup_key=ident,
        confidence=confidence,
        source="s08b-test-source",
        creator="s08b-test-creator",
        created_at=created,
        updated_at=created,
        lifecycle_state=lifecycle_state,
    )
    receipt = MemoryReceipt(
        id=ident,
        memory_type="fact",
        source="s08b-test-receipt",
        created_by="s08b-test-creator",
        timestamp=created,
        confidence=confidence,
        lifecycle_state=lifecycle_state,
    )
    resources = scope.application.read_resources()
    await resources.provider.create_in_transaction(
        fact=fact,
        receipt=receipt,
        outbox_entries=[
            {
                "record_type": "fact",
                "record_id": ident,
                "operation": "index_fact",
                "payload": {
                    "subject": subject,
                    "predicate": predicate,
                    "object": object_,
                    "source": "s08b-test-source",
                },
            }
        ],
    )
    return fact


async def wait_for_index(scope, ident, query_text, *, graph_names=()):
    """Poll real vector/graph projections and outbox state without fixed sleeps."""
    vector_service = getattr(scope.application, "retrieval", None)
    deadline = monotonic() + 10
    point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"fact:{ident}"))
    while monotonic() < deadline:
        vector_ready = False
        if vector_service is not None:
            try:
                router = await vector_service._get_embedding_router()
                hits = await router._vector_provider.search(
                    collection=router._collection,
                    vector=router._embedder.embed(query_text),
                    limit=10,
                )
                vector_ready = any(hit.get("id") == point_id for hit in hits)
            except (AttributeError, ValueError):
                vector_ready = False
        graph_ready = True
        if graph_names:
            try:
                graph_service = scope.application.graph
                graph_router = await graph_service._get_graph_router()
                names = {node.name for node in graph_router.graph.get_all_nodes()}
                graph_ready = set(graph_names) <= names
            except (AttributeError, TypeError, ValueError):
                graph_ready = False
        if vector_ready and graph_ready:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"index readback timed out for {ident}")


@pytest.mark.asyncio
@pytest.mark.parametrize("no_fts", [False, True])
async def test_metadata_matrix_before_limit_two_profiles(tmp_path, no_fts):
    async with live_server(tmp_path) as (holder, port, auth):
        for profile in ("alpha", "beta"):
            async with await read_scope(holder, auth, profile) as scope:
                resources = scope.application.read_resources()
                await insert_facts(
                    resources,
                    [
                        {"id": "eligible", "confidence": 0.9, "lifecycle_state": "validated"},
                        {"id": "active", "confidence": 1.0},
                        {"id": "candidate", "confidence": 0.0, "lifecycle_state": "candidate"},
                        {"id": "archived", "confidence": 0.95, "lifecycle_state": "archived"},
                        {"id": "rejected", "lifecycle_state": "rejected"},
                        {"id": "top-ineligible", "confidence": 0.1, "object": "needle " * 12},
                    ],
                )
                if no_fts:
                    async with resources.session_factory() as session, session.begin():
                        await session.execute(text("DROP TABLE facts_fts"))
                async with resources.session_factory() as session:
                    unfiltered = await FactRepository(session).search(text="needle", limit=1, lifecycle_state="active")
                    assert [f.id for f in unfiltered] == ["top-ineligible"]
            cases = [
                ({"memory_types": ["belief"]}, False, set()),
                ({"memory_types": ["fact"]}, False, {"eligible", "active", "candidate", "top-ineligible"}),
                ({"memory_types": ["fact", "belief"]}, False, {"eligible", "active", "candidate", "top-ineligible"}),
                ({"min_confidence": 0.0}, False, {"eligible", "active", "candidate", "top-ineligible"}),
                ({"min_confidence": 1.0}, False, {"active"}),
                ({"min_confidence": 0.9}, False, {"active", "eligible"}),
                ({"lifecycle_states": ["active"]}, False, {"active", "top-ineligible"}),
                ({"lifecycle_states": ["validated"]}, False, {"eligible"}),
                ({"lifecycle_states": ["candidate"]}, False, {"candidate"}),
                ({"lifecycle_states": ["archived"]}, False, set()),
                ({"lifecycle_states": ["archived"]}, True, {"archived"}),
                ({"lifecycle_states": ["rejected"]}, True, {"rejected"}),
                ({"lifecycle_states": ["active", "archived"]}, False, {"active", "top-ineligible"}),
                ({"lifecycle_states": ["active", "archived"]}, True, {"active", "top-ineligible", "archived"}),
                ({"lifecycle_states": ["unknown"]}, True, set()),
                (
                    {
                        "memory_types": ["fact", "belief"],
                        "lifecycle_states": ["validated", "archived"],
                        "min_confidence": 0.9,
                    },
                    True,
                    {"eligible", "archived"},
                ),
            ]
            for filters, inactive, expected in cases:
                status, result = await post(
                    port,
                    auth,
                    profile=profile,
                    include_inactive=inactive,
                    filters={"source": None, "min_confidence": None, **filters},
                )
                assert status == 200, (filters, result)
                assert {hit["memory"]["id"] for hit in result["results"]} == expected, (filters, result)
            # More ineligible rows than LIMIT; a late eligible MUST survive SQL filtering.
            status, result = await post(
                port,
                auth,
                profile=profile,
                limit=1,
                filters={"source": None, "min_confidence": 0.9, "lifecycle_states": ["validated"]},
            )
            assert status == 200 and [h["memory"]["id"] for h in result["results"]] == ["eligible"], result


@pytest.mark.asyncio
@pytest.mark.parametrize("no_fts", [False, True])
async def test_neutral_filter_parity_real_sqlite(tmp_path, no_fts):
    async with live_server(tmp_path) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            resources = scope.application.read_resources()
            await insert_facts(
                resources,
                [
                    {"id": "p1", "object": 'needle quote" star* AND percent% under_score back\\slash'},
                    {"id": "p2", "object": 'needle quote" other AND percent% under_score back\\slash'},
                    {"id": "p3", "object": "unrelated"},
                ],
            )
            if no_fts:
                async with resources.session_factory() as session, session.begin():
                    await session.execute(text("DROP TABLE facts_fts"))
            for query in ('quote"', "star*", "needle AND", "percent%", "under_score", "back\\slash", "!!!", "needle"):
                for neutral in ({"min_confidence": 0.0}, {"memory_types": ["fact"]}):
                    async with resources.session_factory() as session:
                        repo = FactRepository(session)
                        expected = await repo.search(text=query, limit=2, lifecycle_state="active")
                        over = await repo.search(text=query, limit=3, lifecycle_state="active")
                        actual = await resources.provider.search_facts_in_session(
                            session, text=query, limit=2, include_inactive=False, **neutral
                        )
                    assert [f.id for f in actual] == [f.id for f in expected], (query, neutral)
                    status, result = await post(
                        port, auth, query=query, limit=2, filters={"source": None, "min_confidence": None, **neutral}
                    )
                    assert status == 200, (query, result)
                    assert [h["memory"]["id"] for h in result["results"]] == [f.id for f in expected]
                    async with resources.session_factory() as session:
                        actual_over = await resources.provider.search_facts_in_session(
                            session, text=query, limit=3, include_inactive=False, **neutral
                        )
                    assert (len(actual_over) > 2) == (len(over) > 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("no_fts", [False, True])
async def test_debug_logs_exclude_retrieval_bind_parameters(tmp_path, caplog, monkeypatch, no_fts):
    # Start with inherited DEBUG, including when another daemon test set WARNING.
    monkeypatch.setattr(logging.getLogger("aiosqlite"), "level", logging.NOTSET)
    with caplog.at_level(logging.DEBUG):
        async with live_server(tmp_path) as (holder, port, auth):
            if no_fts:
                async with await read_scope(holder, auth) as scope:
                    resources = scope.application.read_resources()
                    async with resources.session_factory() as session, session.begin():
                        await session.execute(text("DROP TABLE facts_fts"))
            canaries = ("FIX1_CANARY_TASK", "FIX1_CANARY_SUBJECT", "FIX1_CANARY_QUERY")
            status, body = await post(port, auth, "context", task=canaries[0], subject=canaries[1])
            assert status == 200, body
            status, body = await post(port, auth, query=canaries[2], filters={"source": None, "min_confidence": 0.0})
            assert status == 200, body
            logging.getLogger("fix1.capture").debug("debug capture is active")
        assert "debug capture is active" in caplog.text
        leaked = [record for record in caplog.records if any(value in record.getMessage() for value in canaries)]
        assert not leaked, [(record.name, record.getMessage()) for record in leaked]


@pytest.mark.asyncio
async def test_s08b_audit_stages_privacy_real_lancedb_failure(tmp_path, similarity_embedder, caplog):
    from pathlib import Path

    with caplog.at_level(logging.DEBUG):
        registry = await prepare_indexed_registry(
            tmp_path,
            [
                {
                    "id": "s08b-real-failure",
                    "subject": "PRIVACY_SUBJECT_CANARY",
                    "predicate": "PRIVACY_PREDICATE_CANARY",
                    "object": "PRIVACY_OBJECT_CANARY",
                }
            ],
        )
        async with live_server(tmp_path, registry=registry) as (holder, port, auth):
            async with await read_scope(holder, auth) as scope:
                router = await scope.application.retrieval._get_embedding_router()
                provider = router._vector_provider
                store = Path(provider._db_path)
                manifests = tuple((store / "memories.lance" / "_versions").glob("*.manifest"))
                assert manifests
                backups = {path: path.read_bytes() for path in manifests}
                # Corrupt real backend manifests without replacing the admitted
                # projection root (which would test runtime identity instead).
                for path in manifests:
                    path.write_bytes(b"not a LanceDB manifest")
            try:
                status, body = await post(
                    port,
                    auth,
                    query="REAL_BACKEND_QUERY_CANARY",
                    mode="semantic",
                    filters={"source": None, "min_confidence": None},
                )
                assert status == 503 and body["error"]["code"] == "E_VECTOR_BACKEND", body
                assert "REAL_BACKEND_QUERY_CANARY" not in json.dumps(body)
                with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
                    state = db.execute(
                        "SELECT state FROM access_audit WHERE request_id=?", (body["error"]["request_id"],)
                    ).fetchone()
                assert state == ("terminal",)
            finally:
                for path, original in backups.items():
                    path.write_bytes(original)

        provider_records = [record for record in caplog.records if "lancedb" in record.name.lower()]
        assert provider_records, [(record.name, record.getMessage()) for record in caplog.records]
        assert any("Search failed:" in record.getMessage() for record in provider_records)
        leaked = [
            record
            for record in caplog.records
            if any(
                canary in record.getMessage()
                for canary in (
                    "REAL_BACKEND_QUERY_CANARY",
                    "PRIVACY_SUBJECT_CANARY",
                    "PRIVACY_PREDICATE_CANARY",
                    "PRIVACY_OBJECT_CANARY",
                    "TASK_CANARY",
                )
            )
        ]
        assert not leaked, [(record.name, record.getMessage()) for record in leaked]


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["task", "subject", "query"])
@pytest.mark.parametrize("control", ["\n", "\t", "\x00"])
async def test_nonprintable_retrieval_rejected_before_audit_and_lease(tmp_path, monkeypatch, field, control):
    async with live_server(tmp_path) as (holder, port, auth):
        calls = []
        acquire = holder.app.dependencies.leases.acquire

        def counted(ctx):
            calls.append(ctx.request_id)
            return acquire(ctx)

        monkeypatch.setattr(holder.app.dependencies.leases, "acquire", counted)
        operation = "search" if field == "query" else "context"
        status, body = await post(port, auth, operation, **{field: "FIX1_FORGED" + control + "entry"})
        assert status == 422 and body["error"]["code"] == "E_VALIDATION", body
        assert calls == []
        with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
            assert db.execute("SELECT count(*) FROM access_audit").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_preflight_audit_modes_and_canaries(tmp_path, caplog, monkeypatch):
    async with live_server(tmp_path) as (holder, port, auth):
        deps = holder.app.dependencies
        original = deps.leases.acquire
        calls = []

        def acquire(ctx):
            calls.append(ctx.profile_id)
            return original(ctx)

        monkeypatch.setattr(deps.leases, "acquire", acquire)
        now = datetime.now(UTC)
        token = deps.tokens._bootstrap_create(
            d.TokenCreate(
                subject="revealer",
                profiles=["alpha", "beta"],
                permissions=["memory.read", "memory.reveal"],
                expires_at=now + timedelta(hours=1),
            ),
            now=now,
        )
        revealer = {"Authorization": f"Bearer cmms_{token.token.id}.{token.secret}"}
        for profile in ("alpha", "beta"):
            for filters in ({"source": ""}, {"source": "CANARY_filter"}, {"tags": [""]}, {"tags": ["CANARY_tag"]}):
                for headers in (auth, revealer):
                    status, body = await post(
                        port, headers, profile=profile, filters={"source": None, "min_confidence": None, **filters}
                    )
                    assert status == 422 and body["error"]["code"] == "E_VALIDATION"
            for operation, change in (
                ("search", {"mode": "semantic"}),
                ("search", {"mode": "graph"}),
                ("search", {"trace": True}),
                ("context", {"trace": True}),
            ):
                status, body = await post(port, auth, operation=operation, profile=profile, **change)
                if change.get("trace"):
                    assert status == 503 and body["error"]["code"] == "E_CAPABILITY_UNAVAILABLE"
                else:
                    assert status == 200, body
        assert len(calls) == 4
        # Validation and trace rejection still precede audit and lease.
        with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
            rows = db.execute("SELECT operation,state FROM access_audit ORDER BY rowid").fetchall()
        assert len(rows) == 4 and all(state == "terminal" for _, state in rows)
        status, keyword = await post(port, auth, query="alpha")
        status2, hybrid = await post(port, auth, query="alpha", mode="hybrid")
        assert status == status2 == 200
        assert keyword["total"] == hybrid["total"] == 1
        assert keyword["degraded"] == [] and hybrid["degraded"] == []
        for hit in keyword["results"]:
            assert hit["source"] == "fts" and hit["score"] == 0.0 and hit["memory"]["masked"]
        assert "CANARY_filter" not in caplog.text and "CANARY_tag" not in caplog.text


@pytest.mark.asyncio
async def test_retrieval_audit_intent_one_lease_stage_custody(tmp_path, monkeypatch):
    async with live_server(tmp_path) as (holder, port, auth):
        deps = holder.app.dependencies
        acquire = deps.leases.acquire
        completed = deps.metrics.retrieval_completed
        active, observed, stages = set(), [], []

        @asynccontextmanager
        async def watched(ctx):
            with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
                row = db.execute(
                    "SELECT state,profile_id,token_id,operation FROM access_audit WHERE request_id=?", (ctx.request_id,)
                ).fetchall()
            assert len(row) == 1 and row[0][0] == "intent" and row[0][1] == ctx.profile_id
            assert row[0][2] == ctx.principal.token_id
            observed.append((ctx.request_id, ctx.profile_id, row[0][3]))
            async with acquire(ctx) as scope:
                active.add(ctx.request_id)
                try:
                    yield scope
                finally:
                    active.remove(ctx.request_id)

        def measured(ctx, stage, duration, error):
            assert ctx.request_id in active
            assert duration >= 0 and stage in ("fts", "context")
            stages.append((ctx.request_id, ctx.profile_id, stage, error))
            completed(ctx, stage, duration, error)

        monkeypatch.setattr(deps.leases, "acquire", watched)
        monkeypatch.setattr(deps.metrics, "retrieval_completed", measured)
        results = await asyncio.gather(
            *(
                post(port, auth, operation, profile, **changes)
                for operation, profile, changes in (
                    ("search", "alpha", {"query": "alpha"}),
                    ("context", "alpha", {"task": "alpha"}),
                    ("search", "beta", {"query": "alpha"}),
                    ("context", "beta", {"task": "alpha"}),
                )
            )
        )
        assert all(status == 200 for status, _ in results), results
        assert len(observed) == len(stages) == len({r for r, _, _ in observed}) == 4
        assert {(r, p) for r, p, _ in observed} == {(r, p) for r, p, _, _ in stages}
        assert all(error is None for _, _, _, error in stages)
        for status, body in results:
            with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
                rows = db.execute("SELECT * FROM access_audit WHERE request_id=?", (body["request_id"],)).fetchall()
            assert len(rows) == 1 and "CANARY" not in str(rows)
        assert results[0][1]["total"] == results[1][1]["package"]["total"] == 1
        assert results[2][1]["total"] == results[3][1]["package"]["total"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("no_fts", [False, True])
async def test_search_filter_sql_and_snapshot(tmp_path, no_fts):
    async with live_server(tmp_path) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            resources = scope.application.read_resources()
            await insert_facts(resources, [{"id": "match", "confidence": 0.9}])
            if no_fts:
                async with resources.session_factory() as session, session.begin():
                    await session.execute(text("DROP TABLE facts_fts"))
            statements = []

            def capture(conn, cursor, statement, parameters, context, executemany):
                statements.append((id(conn), statement))

            event.listen(resources.provider.engine.sync_engine, "before_cursor_execute", capture)
            try:
                status, body = await post(
                    port, auth, limit=1, filters={"source": None, "min_confidence": 0.8, "lifecycle_states": ["active"]}
                )
            finally:
                event.remove(resources.provider.engine.sync_engine, "before_cursor_execute", capture)
            assert status == 200 and body["total"] == 1
            branch = "LIKE" if no_fts else "MATCH"
            matching = [(conn, sql) for conn, sql in statements if branch in sql]
            assert len(matching) == 1, statements
            sql = matching[0][1]
            where_start = sql.index("WHERE")
            limit_start = sql.index("LIMIT")
            assert where_start < sql.index("f.confidence >=") < limit_start
            assert where_start < sql.index("f.lifecycle_state IN") < limit_start
            # Both the default active restriction and explicit lifecycle filter belong in WHERE.
            assert sql[where_start:limit_start].count("f.lifecycle_state IN") == 2
            assert any(sql.strip().upper() == "BEGIN" for _, sql in statements)
            assert len({conn for conn, _ in statements}) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,expected",
    [
        ("no such table: facts_fts", 200),
        ("malformed MATCH expression", 200),
        ("unable to use function MATCH in the requested context", 200),
        ("no such module: fts5", 200),
        ("database is locked", 503),
        ("disk I/O error", 503),
    ],
)
async def test_filter_fts_failure_classification_over_tcp(tmp_path, failure, expected):
    from sqlalchemy.exc import OperationalError

    async with live_server(tmp_path) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            resources = scope.application.read_resources()
            await insert_facts(resources, [{"id": "good", "confidence": 0.9}, {"id": "bad", "confidence": 0.1}])

            def fault(conn, cursor, statement, parameters, context, executemany):
                if "MATCH :" in statement or "MATCH ?" in statement:
                    raise OperationalError("CANARY_SQL", {"canary": "CANARY_PARAM"}, sqlite3.OperationalError(failure))

            event.listen(resources.provider.engine.sync_engine, "before_cursor_execute", fault)
            try:
                status, body = await post(port, auth, filters={"source": None, "min_confidence": 0.8})
            finally:
                event.remove(resources.provider.engine.sync_engine, "before_cursor_execute", fault)
            assert status == expected, body
            if status == 200:
                assert [hit["memory"]["id"] for hit in body["results"]] == ["good"]
            else:
                assert body["error"]["code"] == "E_STORAGE"
            assert "CANARY" not in json.dumps(body)


@pytest.mark.asyncio
async def test_retrieval_strict_validation_and_durable_audit(tmp_path, caplog, monkeypatch):
    async with live_server(tmp_path) as (holder, port, auth):
        for operation, fields in (
            ("search", {"limit": 0}),
            ("search", {"limit": 101}),
            ("search", {"limit": True}),
            ("search", {"filters": {"source": None, "min_confidence": float("nan")}}),
            ("search", {"filters": {"source": None, "min_confidence": -0.1}}),
            ("context", {"max_results": 0}),
            ("context", {"max_results": 101}),
            ("context", {"task": "x" * 4097}),
            ("context", {"extra": "CANARY_extra"}),
        ):
            status, body = await post(port, auth, operation, **fields)
            assert status == 422 and body["error"]["code"] == "E_VALIDATION"
        for operation in ("search", "context"):
            path = f"/api/v1/profiles/alpha/{operation}"
            status, body = await http_request(port, "POST", path, body=b"{}", headers=auth)
            assert status == 415
            status, body = await http_request(
                port, "POST", path, body=b"{", headers={**auth, "Content-Type": "application/json"}
            )
            assert status == 422
            status, body = await http_request(
                port, "POST", path + "?CANARY_query=1", body=b"{}", headers={**auth, "Content-Type": "application/json"}
            )
            assert status == 422
            status, body = await post(
                port, auth, operation, **({"query": "alpha"} if operation == "search" else {"task": "alpha"})
            )
            assert status == 200, body
            with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
                rows = db.execute(
                    "SELECT state,profile_id,operation FROM access_audit WHERE request_id=?", (body["request_id"],)
                ).fetchall()
            # Durable audit updates the intent row in place to terminal.
            assert rows == [("terminal", "alpha", operation)], rows
        deps = holder.app.dependencies
        acquired = []
        original_acquire = deps.leases.acquire

        def counted(ctx):
            acquired.append(ctx.request_id)
            return original_acquire(ctx)

        monkeypatch.setattr(deps.leases, "acquire", counted)

        async def failed(*args, **kwargs):
            raise OSError("CANARY_AUDIT_FAILURE")

        for operation in ("search", "context"):
            for stage in ("begin", "finish"):
                acquired.clear()
                with monkeypatch.context() as patch:
                    patch.setattr(deps.access, stage, failed)
                    status, body = await post(port, auth, operation)
                assert status == 503 and body["error"]["code"] == "E_AUDIT_UNAVAILABLE"
                assert len(acquired) == (0 if stage == "begin" else 1)
                assert "CANARY" not in json.dumps(body)
                with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
                    rows = db.execute(
                        "SELECT state FROM access_audit WHERE request_id=?", (body["error"]["request_id"],)
                    ).fetchall()
                assert rows == ([] if stage == "begin" else [("intent",)])
        assert "CANARY" not in caplog.text
