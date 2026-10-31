"""SLICE01B acceptance: production daemon composition over a real socket."""

from __future__ import annotations

import asyncio
import http.client
import json
import socket
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from time import monotonic
from types import SimpleNamespace
from urllib.parse import quote

import pytest
import uvicorn
from storage.models.fact import FactORM
from storage.models.receipt import MemoryReceiptORM

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import HttpConfig, RequestContext
from memory_server.providers.embedding_provider import MockEmbeddingProvider
from memory_server.providers.lancedb_provider import LanceDBProvider
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.runtime import runtime_factory
from memory_server.runtime.daemon import Daemon
from memory_server.runtime.daemon_cli import run_daemon
from tests.gateway.test_http_shell import shell as shell


@pytest.fixture(autouse=True)
def offline_embedder(monkeypatch):
    monkeypatch.setitem(runtime_factory._DEFAULT_RECIPES, "embedder", lambda runtime: MockEmbeddingProvider())


class SocketServer(uvicorn.Server):
    """The production uvicorn server, supplied a test-owned real TCP socket."""

    def __init__(self, config):
        super().__init__(config)
        self.socket = None

    async def serve(self, sockets=None):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.config.host, self.config.port))
        self.socket.listen()
        self.socket.setblocking(False)
        try:
            return await super().serve(sockets=[self.socket])
        finally:
            self.socket.close()


async def wait_started(server: SocketServer) -> int:
    async with asyncio.timeout(5):
        while not server.started:
            await asyncio.sleep(0.01)
        assert server.socket is not None
        return server.socket.getsockname()[1]


async def http_request(port: int, method: str, path: str, body: bytes = b"", headers=None, response_headers=None):
    def request():
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        try:
            connection.request(method, path, body=body, headers=headers or {})
            response = connection.getresponse()
            if response_headers is not None:
                response_headers.update({k.lower(): v for k, v in response.getheaders()})
            return response.status, json.loads(response.read() or b"{}")
        finally:
            connection.close()

    return await asyncio.to_thread(request)


def free_port() -> int:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]
    finally:
        probe.close()


def seed_fact(provider, ident: str, marker: str, *, lifecycle_state="active"):
    async def insert():
        async with provider.session_factory() as session:
            async with session.begin():
                session.add(
                    FactORM(
                        id=ident,
                        subject="slice01b",
                        predicate="contains",
                        object=marker,
                        dedup_key=ident,
                        confidence=0.8,
                        source="slice01b-test",
                        creator="slice01b-test",
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
                        verification_status="candidate",
                        lifecycle_state=lifecycle_state,
                        version="1",
                    )
                )
                session.add(
                    MemoryReceiptORM(
                        id=ident,
                        memory_type="fact",
                        source="SLICE_CANARY_receipt",
                        created_by="SLICE_CANARY_actor",
                        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                        version="1",
                        confidence=0.8,
                        verification_status="unverified",
                        lifecycle_state="active",
                        history=json.dumps(
                            [{"timestamp": "2026-01-01T00:00:00+00:00", "reason": "SLICE_CANARY_reason"}]
                        ),
                    )
                )

    return insert()


async def build_registry(tmp_path):
    roots = {name: tmp_path / name for name in ("alpha", "beta")}
    for root in roots.values():
        root.mkdir()
        # Prepare a real empty vector collection before manager identity capture.
        # Fresh lazy vector creation in ProfileBundle changes directory identity
        # after RuntimeManager._start (a pre-existing runtime boundary).
        vector = LanceDBProvider(db_path=str(root / "lancedb"))
        await vector.create_collection("memories")
        await vector.close()
    registry = tmp_path / "registry.json"
    registry.write_text(
        json.dumps({"profiles": [{"id": name, "data_root": str(root)} for name, root in roots.items()]})
    )
    return registry, roots


@pytest.mark.asyncio
async def test_slice_production_mount_and_search(tmp_path):
    """RED control: mounted GET and POST must not remain baseline 404s."""
    registry, _ = await build_registry(tmp_path)
    config = HttpConfig(
        control_path=tmp_path / "control" / "control.sqlite",
        host="127.0.0.1",
        port=free_port(),
    )
    holder = SimpleNamespace(server=None, app=None)

    def factory(server_config):
        holder.server = SocketServer(server_config)
        holder.app = server_config.app
        return holder.server

    task = asyncio.create_task(
        run_daemon(
            config,
            registry,
            factory=__import__("memory_server.transports.http.app", fromlist=["create_http_app"]).create_http_app,
            server_factory=factory,
        )
    )
    try:
        async with asyncio.timeout(5):
            while holder.server is None:
                await asyncio.sleep(0.01)
        port = await wait_started(holder.server)
        now = datetime.now(UTC)
        created = holder.app.dependencies.tokens._bootstrap_create(
            d.TokenCreate(
                subject="slice01b",
                profiles=["alpha", "beta"],
                permissions=["memory.read"],
                expires_at=now + timedelta(hours=1),
            ),
            now=now,
        )
        bearer = f"cmms_{created.token.id}.{created.secret}"
        principal = await holder.app.dependencies.tokens.authenticate(bearer, now=now)
        ctx = RequestContext("seed-mount", principal, "alpha", None, monotonic() + 30)
        async with holder.app.dependencies.leases.acquire(ctx) as scope:
            await seed_fact(scope.application.read_resources(), "alpha-mount-marker", "alpha")
        auth = {"Authorization": "Bearer " + bearer}
        get_status, get_body = await http_request(port, "GET", "/api/v1/profiles/alpha/memories", headers=auth)
        search_status, search_body = await http_request(
            port,
            "POST",
            "/api/v1/profiles/alpha/search",
            body=b'{"query":"alpha","filters":{"source":null,"min_confidence":null}}',
            headers={**auth, "Content-Type": "application/json"},
        )
        assert (get_status, search_status) == (200, 200), (
            {"get_status": get_status, "get_body": get_body},
            {"search_status": search_status, "search_body": search_body},
        )
        assert search_body["total"] == 1
        assert [hit["memory"]["id"] for hit in search_body["results"]] == ["alpha-mount-marker"]
    finally:
        if holder.server is not None:
            holder.server.should_exit = True
        await asyncio.wait_for(task, 10)


@pytest.mark.asyncio
async def test_slice_search_mode_filter_trace_and_profile_isolation(tmp_path, monkeypatch, caplog):
    """Real two-profile search matrix; only the embedder is replaced by a mock."""
    monkeypatch.setitem(runtime_factory._DEFAULT_RECIPES, "embedder", lambda runtime: MockEmbeddingProvider())
    registry, roots = await build_registry(tmp_path)
    providers = {}

    config = HttpConfig(
        control_path=tmp_path / "control" / "control.sqlite", host="127.0.0.1", port=free_port(), rate_burst=200
    )
    holder = SimpleNamespace(server=None, app=None)

    def factory(server_config):
        holder.server = SocketServer(server_config)
        holder.app = server_config.app
        return holder.server

    task = asyncio.create_task(
        run_daemon(
            config,
            registry,
            factory=__import__("memory_server.transports.http.app", fromlist=["create_http_app"]).create_http_app,
            server_factory=factory,
        )
    )
    try:
        async with asyncio.timeout(5):
            while holder.server is None:
                await asyncio.sleep(0.01)
        port = await wait_started(holder.server)
        now = datetime.now(UTC)
        created = holder.app.dependencies.tokens._bootstrap_create(
            d.TokenCreate(
                subject="slice01b",
                profiles=["alpha", "beta"],
                permissions=["memory.read"],
                expires_at=now + timedelta(hours=1),
            ),
            now=now,
        )
        bearer = f"cmms_{created.token.id}.{created.secret}"
        assert isinstance(holder.app.dependencies.leases, Daemon)
        assert holder.app.dependencies.ports == holder.app.guard.dependencies.ports
        principal = await holder.app.dependencies.tokens.authenticate(bearer, now=now)
        for profile in roots:
            ctx = RequestContext("seed-" + profile, principal, profile, None, monotonic() + 30)
            async with holder.app.dependencies.leases.acquire(ctx) as scope:
                resources = scope.application.read_resources()
                providers[profile] = resources
                await seed_fact(resources, f"{profile}-marker", f"{profile}-unique-marker")
                await seed_fact(resources, f"{profile}-common", "common-marker")
                if profile == "alpha":
                    await seed_fact(resources, "alpha-inactive", "alpha-inactive-marker", lifecycle_state="inactive")
                    await seed_fact(resources, "alpha-sensitive", "common-marker sk-" + "x" * 48)
                    await seed_fact(resources, "alpha-oversized", "common-marker " + "x" * 40000)
        auth = {"Authorization": "Bearer " + bearer, "Content-Type": "application/json"}
        acquisitions = []
        real_acquire = holder.app.dependencies.leases.acquire

        @asynccontextmanager
        async def counted_acquire(ctx):
            acquisitions.append(ctx.request_id)
            async with real_acquire(ctx) as scope:
                yield scope

        monkeypatch.setattr(holder.app.dependencies.leases, "acquire", counted_acquire)
        search_sessions = []
        for resources in providers.values():
            provider = resources.provider
            original_search = provider.search_facts_in_session
            original_read = provider.read_fact_records

            async def search_in_snapshot(session, *, _search=original_search, **kwargs):
                assert session.in_transaction()
                search_sessions.append(session)
                return await _search(session, **kwargs)

            async def hydrate_in_snapshot(session, ids, *, _read=original_read):
                assert search_sessions[-1] is session and session.in_transaction()
                return await _read(session, ids)

            monkeypatch.setattr(provider, "search_facts_in_session", search_in_snapshot)
            monkeypatch.setattr(provider, "read_fact_records", hydrate_in_snapshot)

        async def checked(
            method, path, operation, *, payload=None, profile="alpha", headers=None, eligible=True, token_id=None
        ):
            wire_headers = {}
            status, result = await http_request(
                port,
                method,
                path,
                body=b"" if payload is None else json.dumps(payload).encode(),
                headers=auth if headers is None else headers,
                response_headers=wire_headers,
            )
            request_id = wire_headers["x-request-id"]
            with sqlite3.connect(config.control_path.parent / "http-access" / "access.sqlite") as db:
                rows = db.execute(
                    "SELECT state,profile_id,token_id,operation,result FROM access_audit WHERE request_id=?",
                    (request_id,),
                ).fetchall()
            if eligible:
                assert acquisitions.count(request_id) == 1
                assert rows == [
                    (
                        "terminal",
                        profile,
                        token_id or created.token.id,
                        operation,
                        "success" if status < 400 else result["error"]["code"],
                    )
                ], rows
            else:
                assert rows == []
                assert request_id not in acquisitions
            assert "SLICE_CANARY" not in json.dumps(result)
            assert "sk-" + "x" * 48 not in json.dumps(result)
            return status, result

        async def search(profile, payload):
            eligible = not (
                payload.get("trace")
                or any(payload.get("filters", {}).get(f) for f in ("tags",))
                or payload.get("filters", {}).get("source") is not None
            )
            return await checked(
                "POST",
                f"/api/v1/profiles/{profile}/search",
                "search",
                payload=payload,
                profile=profile,
                eligible=eligible,
            )

        base = {"query": "alpha-unique-marker", "mode": "keyword", "filters": {"source": None, "min_confidence": None}}
        status, result = await search("alpha", base)
        assert status == 200 and result["total"] == 1, result
        assert result["results"][0]["memory"]["id"] == "alpha-marker"
        assert result["results"][0]["score"] == 0.0
        assert result["results"][0]["rank"] == 1
        assert result["results"][0]["source"] == "fts"
        assert result["results"][0]["memory"]["content"] is None
        assert result["trace_id"] is None
        assert result["degraded"] == []
        for limit in (1, 100):
            status, limited = await search("alpha", {**base, "query": "common-marker", "limit": limit})
            assert status == 200 and limited["total"] == min(limit, 3)

        status, result = await search("alpha", {**base, "mode": "hybrid"})
        assert status == 200 and result["degraded"] == []
        assert result["results"][0]["score"] == 1.0
        status, result = await search("alpha", {**base, "mode": "keyword"})
        assert status == 200 and result["degraded"] == []
        for mode in ("semantic", "graph"):
            status, result = await search("alpha", {**base, "mode": mode})
            assert status == 200 and result["degraded"] == [], result
        status, result = await search("alpha", {**base, "trace": True})
        assert status == 503 and result["error"]["code"] == "E_CAPABILITY_UNAVAILABLE"
        status, result = await search("alpha", {**base, "filters": {**base["filters"], "memory_types": ["fact"]}})
        assert status == 200 and result["total"] == 1
        status, result = await search("alpha", {**base, "filters": {**base["filters"], "lifecycle_states": ["active"]}})
        assert status == 200 and result["total"] == 1
        status, result = await search("alpha", {**base, "filters": {**base["filters"], "tags": ["fact"]}})
        assert status == 422 and result["error"]["code"] == "E_VALIDATION"
        for profile, query, expected in (("alpha", "beta-unique-marker", 0), ("beta", "alpha-unique-marker", 0)):
            status, result = await search(profile, {**base, "query": query, "mode": "keyword"})
            assert status == 200 and result["total"] == expected
        status, result = await search("alpha", {**base, "query": "no-such-marker", "mode": "keyword"})
        assert status == 200 and result["results"] == [] and result["total"] == 0
        status, result = await search("alpha", {**base, "include_inactive": True, "query": "alpha-inactive-marker"})
        assert status == 200 and result["total"] == 1, result
        status, result = await search("alpha", {**base, "query": "alpha-inactive-marker", "mode": "keyword"})
        assert status == 200 and result["total"] == 0
        status, result = await search("alpha", {**base, "filters": {"source": "", "min_confidence": None}})
        assert status == 422
        status, result = await search("alpha", {**base, "filters": {"source": None, "min_confidence": 0.0}})
        assert status == 200 and result["total"] == 1
        for profile in ("alpha", "beta"):
            status, result = await search(profile, {**base, "query": "common-marker", "mode": "keyword"})
            assert status == 200 and result["total"] == len(result["results"]) > 0
            with sqlite3.connect(roots[profile] / "memory.db") as db:
                database_ids = {r[0] for r in db.execute("SELECT id FROM facts WHERE object LIKE '%common-marker%'")}
            assert {h["memory"]["id"] for h in result["results"]} == database_ids
            for rank, hit in enumerate(result["results"], 1):
                ident = hit["memory"]["id"]
                assert hit["score"] == 0.0 and hit["rank"] == rank and hit["source"] == "fts"
                assert ident.startswith(profile + "-")
                status, memory = await checked(
                    "GET", f"/api/v1/profiles/{profile}/memories/{ident}", "memories.get", profile=profile
                )
                assert status == 200 and memory == hit["memory"]
        status, page = await checked("GET", "/api/v1/profiles/alpha/memories?limit=1", "memories.list")
        assert status == 200 and page["has_more"]
        cursor = quote(page["next_cursor"], safe="")
        status, result = await checked(
            "GET",
            "/api/v1/profiles/beta/memories?limit=1&cursor=" + cursor,
            "memories.list",
            profile="beta",
            eligible=False,
        )
        assert status == 400 and result["error"]["code"] == "E_CURSOR_INVALID"
        not_found = []
        for ident in ("beta-marker", "unknown-id"):
            status, result = await checked("GET", f"/api/v1/profiles/alpha/memories/{ident}", "memories.get")
            assert status == 404 and result["error"]["code"] == "E_NOT_FOUND"
            error = dict(result["error"])
            error.pop("request_id")
            not_found.append(error)
        assert not_found[0] == not_found[1]
        status, result = await checked(
            "POST",
            "/api/v1/profiles/alpha/memories/alpha-marker/reveal",
            "memories.reveal",
            payload={"reason": "review", "memory_type": "fact", "expected_revision": 1},
            eligible=False,
        )
        assert status == 403
        for suffix, operation in (("/receipt", "receipts.get"), ("/history", "history.list")):
            status, result = await checked("GET", "/api/v1/profiles/alpha/memories/alpha-marker" + suffix, operation)
            assert status == 200
        status, result = await checked("GET", "/api/v1/profiles/alpha/lookup?kind=memory&id=alpha-marker", "lookup")
        assert status == 200 and result["found"]
        for profile in ("alpha", "beta"):
            scoped = holder.app.dependencies.tokens._bootstrap_create(
                d.TokenCreate(
                    subject="scoped",
                    profiles=[profile],
                    permissions=["memory.read"],
                    expires_at=now + timedelta(hours=1),
                ),
                now=now,
            )
            other = "beta" if profile == "alpha" else "alpha"
            scoped_headers = {"Authorization": f"Bearer cmms_{scoped.token.id}.{scoped.secret}"}
            status, result = await checked(
                "GET",
                f"/api/v1/profiles/{other}/memories",
                "memories.list",
                profile=other,
                headers=scoped_headers,
                eligible=False,
            )
            assert status == 403
        revealer = holder.app.dependencies.tokens._bootstrap_create(
            d.TokenCreate(
                subject="revealer",
                profiles=["alpha"],
                permissions=["memory.read", "memory.reveal"],
                expires_at=now + timedelta(hours=1),
            ),
            now=now,
        )
        reveal_headers = {
            "Authorization": f"Bearer cmms_{revealer.token.id}.{revealer.secret}",
            "Content-Type": "application/json",
        }
        status, result = await checked(
            "POST",
            "/api/v1/profiles/alpha/memories/alpha-marker/reveal",
            "memories.reveal",
            headers=reveal_headers,
            token_id=revealer.token.id,
            payload={"reason": "review", "memory_type": "fact", "expected_revision": 1},
        )
        assert status == 200
        status, result = await checked(
            "POST",
            "/api/v1/profiles/alpha/search",
            "search",
            headers=reveal_headers,
            token_id=revealer.token.id,
            payload={**base, "query": "common-marker", "mode": "keyword"},
        )
        assert status == 200 and all(h["memory"]["masked"] for h in result["results"])
        status, result = await checked(
            "GET", "/api/v1/profiles/alpha/lookup?kind=trace&id=absent-trace", "lookup"
        )
        assert status == 503 and result["error"]["code"] == "E_CAPABILITY_UNAVAILABLE"
        status, result = await search("alpha", {**base, "query": "SLICE_CANARY_QUERY", "mode": "keyword"})
        assert status == 200 and result["total"] == 0
        ctx = RequestContext("unavailable-seam", principal, "alpha", None, monotonic() + 30)
        async with real_acquire(ctx) as scope:
            application = scope.application
        for attribute in ("_read_provider", "_retrieval"):
            saved = getattr(application, attribute)
            setattr(application, attribute, None)
            try:
                status, result = await search("alpha", base)
                assert status == 503 and result["error"]["code"] == "E_RUNTIME_NOT_READY"
            finally:
                setattr(application, attribute, saved)
        from sqlalchemy.exc import SQLAlchemyError

        provider = providers["alpha"].provider
        saved_search = provider.search_facts_in_session

        async def broken_storage(*args, **kwargs):
            raise SQLAlchemyError("SLICE_CANARY_SQL")

        monkeypatch.setattr(provider, "search_facts_in_session", broken_storage)
        try:
            status, result = await search("alpha", base)
            assert status == 503 and result["error"]["code"] == "E_STORAGE"
        finally:
            monkeypatch.setattr(provider, "search_facts_in_session", saved_search)

        from prometheus_client import generate_latest

        channels = [caplog.text.encode(), generate_latest(holder.app.dependencies.metrics._registry)]
        channels.extend(p.read_bytes() for p in config.control_path.parent.rglob("*") if p.is_file())
        for channel in channels:
            assert b"SLICE_CANARY" not in channel and bearer.encode() not in channel

        # A terminal audit failure must suppress an otherwise successful body.
        async def fail_finish(ticket, outcome):
            raise OSError("audit unavailable")

        monkeypatch.setattr(holder.app.dependencies.access, "finish", fail_finish)
        status, result = await http_request(
            port, "POST", "/api/v1/profiles/alpha/search", body=json.dumps(base).encode(), headers=auth
        )
        assert status == 503 and result["error"]["code"] == "E_AUDIT_UNAVAILABLE"
        assert "results" not in result and "SLICE_CANARY" not in json.dumps(result)
    finally:
        if holder.server is not None:
            holder.server.should_exit = True
        await asyncio.wait_for(task, 10)


@pytest.mark.parametrize(
    "payload",
    [
        {"query": "alpha", "filters": {}},
        {"query": "alpha"},
        {"query": "", "filters": {"source": None, "min_confidence": None}},
        {"query": "alpha", "filters": {"source": None, "min_confidence": None}, "limit": 0},
        {"query": "alpha", "filters": {"source": None, "min_confidence": None}, "extra": True},
    ],
)
@pytest.mark.asyncio
async def test_slice_search_invalid_payloads_are_422_over_socket(tmp_path, payload):
    """Malformed search never reaches a provider and never becomes an empty success."""
    registry, _ = await build_registry(tmp_path)
    config = HttpConfig(
        control_path=tmp_path / "control" / "control.sqlite", host="127.0.0.1", port=free_port(), rate_burst=200
    )
    holder = SimpleNamespace(server=None, app=None)

    def factory(server_config):
        holder.server = SocketServer(server_config)
        holder.app = server_config.app
        return holder.server

    task = asyncio.create_task(
        run_daemon(
            config,
            registry,
            factory=__import__("memory_server.transports.http.app", fromlist=["create_http_app"]).create_http_app,
            server_factory=factory,
        )
    )
    try:
        async with asyncio.timeout(5):
            while holder.server is None:
                await asyncio.sleep(0.01)
        port = await wait_started(holder.server)
        now = datetime.now(UTC)
        created = holder.app.dependencies.tokens._bootstrap_create(
            d.TokenCreate(
                subject="slice01b", profiles=["alpha"], permissions=["memory.read"], expires_at=now + timedelta(hours=1)
            ),
            now=now,
        )
        bearer = f"cmms_{created.token.id}.{created.secret}"
        status, result = await http_request(
            port,
            "POST",
            "/api/v1/profiles/alpha/search",
            body=json.dumps(payload).encode(),
            headers={"Authorization": "Bearer " + bearer, "Content-Type": "application/json"},
        )
        assert status == 422
        assert result["error"]["code"] == "E_VALIDATION"
    finally:
        if holder.server is not None:
            holder.server.should_exit = True
        await asyncio.wait_for(task, 10)


@pytest.mark.parametrize(
    "operation,path,method,body",
    [
        ("memories.list", "/api/v1/profiles/alpha/memories", "GET", b""),
        ("memories.get", "/api/v1/profiles/alpha/memories/00000000-0000-0000-0000-000000000001", "GET", b""),
        ("receipts.get", "/api/v1/profiles/alpha/memories/00000000-0000-0000-0000-000000000001/receipt", "GET", b""),
        ("history.list", "/api/v1/profiles/alpha/memories/00000000-0000-0000-0000-000000000001/history", "GET", b""),
        ("lookup", "/api/v1/profiles/alpha/lookup?kind=memory&id=00000000-0000-0000-0000-000000000001", "GET", b""),
        (
            "memories.reveal",
            "/api/v1/profiles/alpha/memories/00000000-0000-0000-0000-000000000001/reveal",
            "POST",
            b'{"reason":"review","memory_type":"fact","expected_revision":1}',
        ),
        (
            "search",
            "/api/v1/profiles/alpha/search",
            "POST",
            b'{"query":"marker","filters":{"source":null,"min_confidence":null}}',
        ),
    ],
)
@pytest.mark.asyncio
async def test_slice_missing_each_read_port_is_503_before_lease_or_audit(
    tmp_path, operation, path, method, body, monkeypatch
):
    """A mounted route must fail closed when its production port is absent."""
    registry, _ = await build_registry(tmp_path)
    config = HttpConfig(
        control_path=tmp_path / "control" / "control.sqlite", host="127.0.0.1", port=free_port(), rate_burst=200
    )
    holder = SimpleNamespace(server=None, app=None)

    def app_factory(config, deps, *, read_routes):
        from memory_server.transports.http.app import create_http_app

        expected = {
            "memories.list", "memories.get", "receipts.get", "history.list",
            "lookup", "memories.reveal", "search", "context",
            "relations.for_memory", "relations.list", "relations.get",
            "graph.neighborhood", "graph.path", "graph.entity",
            "audit", "indexes.status", "outbox.list", "overview", "reflect",
        }
        assert set(deps.ports) == expected
        assert {spec.operation for spec in read_routes} == expected
        missing = dict(deps.ports)
        del missing[operation]
        return create_http_app(config, replace(deps, ports=missing), read_routes=read_routes)

    def factory(server_config):
        holder.server = SocketServer(server_config)
        holder.app = server_config.app
        assert operation not in holder.app.dependencies.ports
        assert holder.app.dependencies.ports == holder.app.guard.dependencies.ports
        return holder.server

    task = asyncio.create_task(
        run_daemon(
            config,
            registry,
            factory=app_factory,
            server_factory=factory,
        )
    )
    try:
        async with asyncio.timeout(5):
            while holder.server is None:
                await asyncio.sleep(0.01)
        port = await wait_started(holder.server)
        now = datetime.now(UTC)
        created = holder.app.dependencies.tokens._bootstrap_create(
            d.TokenCreate(
                subject="slice01b",
                profiles=["alpha"],
                permissions=["memory.read", "memory.reveal"],
                expires_at=now + timedelta(hours=1),
            ),
            now=now,
        )
        bearer = f"cmms_{created.token.id}.{created.secret}"
        headers = {"Authorization": "Bearer " + bearer}
        if method == "POST":
            headers["Content-Type"] = "application/json"
        calls = []
        acquire = holder.app.dependencies.leases.acquire

        @asynccontextmanager
        async def counted(ctx):
            calls.append(ctx.request_id)
            async with acquire(ctx) as scope:
                yield scope

        monkeypatch.setattr(holder.app.dependencies.leases, "acquire", counted)
        response_headers = {}
        status, result = await http_request(
            port, method, path, body=body, headers=headers, response_headers=response_headers
        )
        assert status == 503, result
        assert result["error"]["code"] == "E_CAPABILITY_UNAVAILABLE"
        assert calls == []
        with sqlite3.connect(config.control_path.parent / "http-access" / "access.sqlite") as db:
            assert (
                db.execute(
                    "SELECT COUNT(*) FROM access_audit WHERE request_id=?", (response_headers["x-request-id"],)
                ).fetchone()[0]
                == 0
            )
        surviving_operation = "search" if operation == "memories.list" else "memories.list"
        if surviving_operation == "search":
            status, result = await http_request(
                port,
                "POST",
                "/api/v1/profiles/alpha/search",
                body=b'{"query":"none","filters":{"source":null,"min_confidence":null}}',
                headers={**headers, "Content-Type": "application/json"},
            )
        else:
            status, result = await http_request(port, "GET", "/api/v1/profiles/alpha/memories", headers=headers)
        assert status == 200, result
        assert len(calls) == 1
    finally:
        if holder.server is not None:
            holder.server.should_exit = True
        await asyncio.wait_for(task, 10)


@pytest.mark.parametrize(
    "kind",
    ["plain-search", "search-as-read", "context", "method", "path", "permission", "duplicate", "foreign", "object",
     "context-parser", "search-parser", "context-result", "search-result", "plain-context", "context-as-search"],
)
def test_slice_mount_catalogue_rejects_substitution(shell, kind):
    from memory_server.transports.http.app import create_http_app
    from memory_server.transports.http.guards import ReadRouteSpec
    from memory_server.transports.http.routes.retrieval import create_context_routes, create_search_routes

    spec = create_search_routes()[0]
    context = create_context_routes()[0]
    cases = {
        "plain-search": (ReadRouteSpec(**vars(spec)),),
        "search-as-read": (
            replace(spec, operation="memories.list", method="GET", path="/api/v1/profiles/{profile}/memories"),
        ),
        "context": (replace(spec, operation="context", path="/api/v1/profiles/{profile}/context"),),
        "method": (replace(spec, method="GET"),),
        "path": (replace(spec, path="/wrong"),),
        "permission": (replace(spec, permission="memory.reveal"),),
        "duplicate": (spec, spec),
        "foreign": (
            replace(spec, operation="profiles.list", method="GET", path="/api/v1/profiles", permission="profile.read"),
        ),
        "object": (object(),),
        "context-parser": (replace(context, parse=spec.parse),),
        "search-parser": (replace(spec, parse=context.parse),),
        "context-result": (replace(context, validate_result=spec.validate_result),),
        "search-result": (replace(spec, validate_result=context.validate_result),),
        "plain-context": (ReadRouteSpec(**vars(context)),),
        "context-as-search": (replace(context, operation="search", path=spec.path),),
    }
    with pytest.raises(ValueError):
        create_http_app(shell.config, shell.deps, read_routes=cases[kind])
    # Equivalent copies with canonical callable identities remain valid.
    create_http_app(shell.config, shell.deps, read_routes=(replace(spec), replace(context)))


@pytest.mark.asyncio
async def test_slice_read_resources_both_constructors_and_snapshot(tmp_path):
    from sqlalchemy import event, text

    from memory_server.application.application import MemoryApplication
    from memory_server.application.errors import UnknownDependencyError

    provider = SQLiteProvider("sqlite+aiosqlite:///" + str(tmp_path / "memory.db"))
    await provider.initialize()
    try:
        for app in (
            MemoryApplication.from_provider(provider),
            MemoryApplication(provider._session_factory, retrieval_provider=provider),
        ):
            resources = app.read_resources()
            assert resources.provider is provider and resources.session_factory is provider._session_factory
        with pytest.raises(UnknownDependencyError):
            MemoryApplication(None).read_resources()
        await seed_fact(resources, "snapshot", "snapshotmarker")
        sql = []
        event.listen(
            provider.engine.sync_engine,
            "before_cursor_execute",
            lambda conn, cursor, statement, parameters, context, many: sql.append(statement),
        )
        async with resources.session_factory() as session:
            async with session.begin():
                await session.execute(text("BEGIN"))
                facts = await provider.search_facts_in_session(
                    session, text="snapshotmarker", limit=20, include_inactive=False
                )
                assert [f.id for f in facts] == ["snapshot"]
                async with resources.session_factory() as writer:
                    await writer.execute(text("DELETE FROM facts WHERE id='snapshot'"))
                    await writer.commit()
                start = len(sql)
                records = await provider.read_fact_records(session, ("snapshot",))
                assert [r.memory["id"] for r in records] == ["snapshot"]
                assert len(sql[start:]) == 2
                start = len(sql)
                assert await provider.read_fact_records(session, ()) == ()
                assert len(sql) == start
    finally:
        await provider.close()
