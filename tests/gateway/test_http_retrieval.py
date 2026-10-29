"""S08a TCP regression tests added before implementation."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

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


async def _start(tmp_path):
    registry, _ = await build_registry(tmp_path)
    config = HttpConfig(control_path=tmp_path / "control" / "control.sqlite", host="127.0.0.1", port=free_port())
    holder = SimpleNamespace(server=None, app=None)

    def factory(server_config):
        holder.server = SocketServer(server_config)
        holder.app = server_config.app
        return holder.server

    task = asyncio.create_task(run_daemon(config, registry, factory=create_http_app, server_factory=factory))
    async with asyncio.timeout(5):
        while holder.server is None:
            await asyncio.sleep(0.01)
    port = await wait_started(holder.server)
    now = datetime.now(UTC)
    created = holder.app.dependencies.tokens._bootstrap_create(
        d.TokenCreate(
            subject="s08a", profiles=["alpha"], permissions=["memory.read"], expires_at=now + timedelta(hours=1)
        ),
        now=now,
    )
    bearer = f"cmms_{created.token.id}.{created.secret}"
    principal = await holder.app.dependencies.tokens.authenticate(bearer, now=now)
    ctx = RequestContext("s08a-seed", principal, "alpha", None, now.timestamp() + 30)
    async with holder.app.dependencies.leases.acquire(ctx) as scope:
        await seed_fact(scope.application.read_resources(), "s08a-marker", "alpha")
    return holder, task, port, {"Authorization": "Bearer " + bearer}


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
async def test_s08a_context_mounted_over_tcp(tmp_path):
    holder, task, port, auth = await _start(tmp_path)
    try:
        status, body = await http_request(
            port,
            "POST",
            "/api/v1/profiles/alpha/context",
            body=b'{"task":"alpha","subject":null,"max_results":20,"include_inactive":false,"trace":false}',
            headers={**auth, "Content-Type": "application/json"},
        )
        assert status == 200, body
        assert body["package"]["total"] >= 0
    finally:
        holder.server.should_exit = True
        await asyncio.wait_for(task, 10)
