"""Pre-auth isolation plus the independent process admission ceiling."""
from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from memory_server.transports.http.app import create_http_app
from tests.gateway.test_http_shell import code, request, rows
from tests.gateway.test_http_shell import shell as shell_fixture


@pytest.fixture(name="shell")
async def inherited_shell(tmp_path):
    async for state in shell_fixture.__wrapped__(tmp_path):
        yield state


async def test_pre_auth_source_isolation_and_process_ceiling(shell):
    app = create_http_app(replace(shell.config, rate_burst=20, rate_per_minute=1), shell.app.dependencies)

    async def from_peer(peer, **kwargs):
        async def source(scope, receive, send):
            scope["client"] = (peer, 12345)
            await app(scope, receive, send)
        return await request(source, "/api/v1/profiles", **kwargs)

    for _ in range(20):
        code(await from_peer("192.0.2.1"), "E_AUTH_REQUIRED", 401)
    assert rows(shell) == []
    response = await from_peer("192.0.2.2", bearer=shell.bearer)
    assert response.status == 200, response.body
    code(await from_peer("192.0.2.1", headers=[("X-Forwarded-For", "192.0.2.99")]), "E_RATE_LIMIT", 429)
    assert (await from_peer("192.0.2.3", bearer=shell.bearer)).status == 200
    assert set(app.guard.pre_auth.buckets) == {"192.0.2.1", "192.0.2.2", "192.0.2.3"}

    # Block real request-body reads, not an artificial active-counter assignment.
    entered, release = asyncio.Event(), asyncio.Event()
    count = 0

    async def blocked_body():
        nonlocal count
        count += 1
        if count == 16:
            entered.set()
        await release.wait()
        return {"type": "http.request", "body": b"", "more_body": False}

    tasks = [asyncio.create_task(from_peer(f"198.51.100.{n}", bearer=shell.bearer, receive=blocked_body))
             for n in range(1, 17)]
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert app.guard.active == 16
        code(await from_peer("203.0.113.1", bearer=shell.bearer), "E_RATE_LIMIT", 429)
        assert "203.0.113.1" not in app.guard.pre_auth.buckets
        assert (await request(app, "/healthz")).status == 200
    finally:
        release.set()
        responses = await asyncio.gather(*tasks)
        await app.close()
    assert all(response.status == 200 for response in responses)
    assert app.guard.active == 0
