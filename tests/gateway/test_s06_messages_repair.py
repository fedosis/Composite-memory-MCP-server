"""C5 messages stay public and do not mislabel client errors as 500s."""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from memory_server.transports.http.errors import _DEFAULT_STATUS, error_response
from tests.gateway.test_http_shell import code, request
from tests.gateway.test_http_shell import shell as shell_fixture


@pytest.fixture(name="shell")
async def inherited_shell(tmp_path):
    async for state in shell_fixture.__wrapped__(tmp_path):
        yield state


async def test_conflict_permission_validation_messages_are_consistent(shell):
    bearer, _ = shell.mint(permissions=("profile.read",))
    payload = dict(reason="test", confirmation=True, expected_generation=2)
    cases = [
        (shell.bearer, payload, "E_VERSION_CONFLICT", 409, "Version conflict"),
        (bearer, payload, "E_PERMISSION_DENIED", 403, "Permission denied"),
        (shell.bearer, {**payload, "confirmation": False}, "E_VALIDATION", 422, "Invalid request"),
    ]
    for token, body, expected, status, message in cases:
        response = await request(
            shell.app, "/api/v1/profiles/alpha/disable", method="POST", bearer=token, body=body,
            headers=[("Idempotency-Key", str(uuid4()))],
        )
        code(response, expected, status)
        assert response.json["error"]["message"] == message
    for name, status in _DEFAULT_STATUS.items():
        error = json.loads(error_response(name, "req_test").body)["error"]
        assert error["message"]
        if status != 500:
            assert "internal server error" not in error["message"].lower()
    assert json.loads(error_response("unknown", "req_test").body)["error"]["message"] == "Internal server error"
