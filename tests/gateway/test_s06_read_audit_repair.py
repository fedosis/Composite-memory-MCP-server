"""Reads require durable audit admission before touching the registry."""
from __future__ import annotations

import pytest

from tests.gateway.test_http_shell import code, request, rows
from tests.gateway.test_http_shell import shell as shell_fixture


@pytest.fixture(name="shell")
async def inherited_shell(tmp_path):
    async for state in shell_fixture.__wrapped__(tmp_path):
        yield state


@pytest.mark.parametrize("path,boundary", [
    ("/api/v1/profiles", "page"), ("/api/v1/profiles/alpha", "resolve"),
])
async def test_read_audit_ticket_before_registry(shell, monkeypatch, path, boundary):
    calls = []
    original = getattr(shell.registry, boundary)

    async def inspect(*args, **kwargs):
        calls.append(boundary)
        assert rows(shell)[-1]["state"] == "intent"
        return await original(*args, **kwargs)

    monkeypatch.setattr(shell.registry, boundary, inspect)
    response = await request(shell.app, path, bearer=shell.bearer)
    assert response.status == 200, response.body
    assert calls and rows(shell)[-1]["state"] == "terminal"
    calls.clear()

    async def missing_ticket(*args, **kwargs):
        return None

    monkeypatch.setattr(shell.access, "begin", missing_ticket)
    code(await request(shell.app, path, bearer=shell.bearer), "E_AUDIT_UNAVAILABLE", 503)
    assert calls == []
    assert len(rows(shell)) == 1
