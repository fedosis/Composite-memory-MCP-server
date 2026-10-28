"""Read-only principals cannot cross the HTTP control-port boundary."""
from __future__ import annotations

from uuid import uuid4

import pytest

from memory_server.gateway_contracts.catalog import CATALOG
from tests.gateway.test_http_shell import code, request, rows
from tests.gateway.test_http_shell import shell as shell_fixture


@pytest.fixture(name="shell")
async def inherited_shell(tmp_path):
    async for state in shell_fixture.__wrapped__(tmp_path):
        yield state


@pytest.mark.parametrize("operation", ["enable", "disable"])
async def test_read_only_token_cannot_reach_control_port(shell, monkeypatch, operation):
    bearer, _ = shell.mint(permissions=("profile.read",))
    calls = []
    original = shell.app.profile_port.execute

    async def inspect(ctx, command):
        if command.operation in {"profiles.enable", "profiles.disable"}:
            calls.append(command.operation)
        return await original(ctx, command)

    monkeypatch.setattr(shell.app.profile_port, "execute", inspect)
    # Prove least privilege with the SAME principal, not the fixture's manager.
    for path in ("/api/v1/profiles", "/api/v1/profiles/alpha"):
        response = await request(shell.app, path, bearer=bearer)
        assert response.status == 200, response.body
    response = await request(
        shell.app, f"/api/v1/profiles/alpha/{operation}", method="POST", bearer=bearer,
        body=dict(reason="read-only", confirmation=True, expected_generation=1),
        headers=[("Idempotency-Key", str(uuid4()))],
    )
    code(response, "E_PERMISSION_DENIED", 403)
    assert calls == []
    assert not shell.store.journal(profile_ids=["alpha"])
    assert not shell.store.control_requests(limit=10)
    assert len(rows(shell)) == 2  # Only the successful reads were admitted.
    route = next(route for route in shell.app.routes if route.name == f"profiles.{operation}")
    assert route.spec is CATALOG.by_operation[f"profiles.{operation}"]
