"""S13A admission and preflight negatives at the remote application boundary."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest

from memory_server.admission import MemoryAdmissionGate
from memory_server.application.remote_ingestion import RemoteIngestionPort
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import RequestContext
from memory_server.transports.http.errors import HttpError
from tests.gateway.test_http_ingestion import counts, key, post, remember_body, request, stored_receipt

pytest_plugins = ("tests.gateway.test_http_ingestion",)


@pytest.mark.parametrize("field,value", [("force", True), ("admission", {}), ("source_identity", "forged")])
async def test_closed_dto_rejects_client_admission_controls(harness, field, value):
    response = await post(harness, {**remember_body(), field: value})
    assert response.status == 422 and response.json["error"]["code"] == "E_VALIDATION"
    assert await counts(harness.providers["alpha"]) == (0, 0, 0, 0)
    assert harness.leases.acquisitions == []


async def test_client_cannot_select_source_scope(harness):
    calls = []

    class Gate(MemoryAdmissionGate):
        def classify(self, text, **kwargs):
            calls.append((text, kwargs))
            return super().classify(text, **kwargs)

    port = harness.deps.ports["memories.create"]
    port._admission = Gate()
    response = await post(harness, remember_body())
    assert response.status == 201
    assert calls[0][1]["source_scope"] == "remote"
    assert calls[0][1]["force"] is False


async def test_rejected_candidate_is_before_lease_and_has_no_rows(harness):
    response = await post(harness, remember_body("temporary", "note", "today"))
    assert response.status == 422 and response.json["error"]["code"] == "E_ADMISSION_REJECTED"
    assert harness.leases.acquisitions == []
    assert await counts(harness.providers["alpha"]) == (0, 0, 0, 0)


async def test_ticket_required_before_lease(harness):
    original = harness.deps.ports["memories.create"]
    port = RemoteIngestionPort(
        harness.deps,
        ticket_provider=lambda: None,
        publish_outcome=original._publish_outcome,
    )
    principal = harness.tokens._principals[harness.bearer]
    ctx = RequestContext(
        "req-ticket-none",
        principal,
        "alpha",
        uuid4(),
        asyncio.get_running_loop().time() + 30,
    )
    command = d.COMMAND_TYPES["memories.create"](
        operation="memories.create",
        profile_id="alpha",
        path_ids={},
        payload=d.RememberRequest.model_validate(remember_body()),
    )
    with pytest.raises(HttpError, match="E_AUDIT_UNAVAILABLE"):
        await port.execute(ctx, command)
    assert harness.leases.acquisitions == []
    assert await counts(harness.providers["alpha"]) == (0, 0, 0, 0)


@pytest.mark.parametrize(
    "case,code",
    [
        ("permission", "E_PERMISSION_DENIED"),
        ("profile", "E_VALIDATION"),
        ("scope", "E_PERMISSION_DENIED"),
        ("key", "E_IDEMPOTENCY_REQUIRED"),
        ("deadline", "E_TIMEOUT"),
        ("operation", "E_VALIDATION"),
        ("forged", "E_VALIDATION"),
    ],
)
async def test_direct_preflight_negatives_do_not_lease(harness, case, code):
    port = harness.deps.ports["memories.create"]
    principal = harness.tokens._principals[harness.bearer]
    command = d.COMMAND_TYPES["memories.create"](
        operation="memories.create",
        profile_id="alpha",
        path_ids={},
        payload=d.RememberRequest.model_validate(remember_body()),
    )
    ctx = RequestContext("req-preflight", principal, "alpha", uuid4(), asyncio.get_running_loop().time() + 30)
    if case == "permission":
        ctx = replace(ctx, principal=replace(principal, permissions=frozenset()))
    elif case == "profile":
        command = command.model_copy(update={"profile_id": "beta"})
    elif case == "scope":
        ctx = replace(ctx, principal=replace(principal, profiles=frozenset({"beta"})))
    elif case == "key":
        ctx = replace(ctx, idempotency_key=None)
    elif case == "deadline":
        ctx = replace(ctx, deadline=asyncio.get_running_loop().time() - 1)
    elif case == "operation":
        command = command.model_copy(update={"operation": "unknown.operation"})
    elif case == "forged":
        command = command.model_copy(update={"payload": {"subject": "not-a-dto"}})
    with pytest.raises(HttpError) as caught:
        await port.execute(ctx, command)
    assert caught.value.code == code
    assert harness.leases.acquisitions == []
    assert await counts(harness.providers["alpha"]) == (0, 0, 0, 0)


async def test_stale_current_principal_rejected_before_handler_effect(harness):
    harness.tokens.generation += 1
    response = await post(harness, remember_body())
    assert response.status == 401 and response.json["error"]["code"] == "E_AUTH_REQUIRED"
    assert harness.tokens.verify_calls >= 1
    assert await counts(harness.providers["alpha"]) == (0, 0, 0, 0)


async def test_admission_metadata_once_after_replay(harness):
    idem = key()
    first = await post(harness, remember_body(), idem=idem)
    replay = await post(harness, remember_body(), idem=idem)
    assert first.status == replay.status == 201 and first.body == replay.body
    assert replay.json["receipts"][0]["history"] == []
    receipt = await stored_receipt(harness.providers["alpha"], replay.json["receipts"][0]["id"])
    assert receipt is not None
    assert sum("admission" in event.get("metadata", {}) for event in receipt.history) == 1


async def test_query_and_non_json_rejected_before_audit(harness):
    queried = await request(
        harness.app,
        "/api/v1/profiles/alpha/memories",
        bearer=harness.bearer,
        body=remember_body(),
        headers=(("Idempotency-Key", key()),),
        query=b"force=true",
    )
    raw = await request(
        harness.app,
        "/api/v1/profiles/alpha/memories",
        bearer=harness.bearer,
        body=b"{}",
        headers=(("Idempotency-Key", key()), ("Content-Type", "text/plain")),
    )
    assert queried.status == 422 and queried.json["error"]["code"] == "E_VALIDATION"
    assert raw.status == 415 and raw.json["error"]["code"] == "E_UNSUPPORTED_MEDIA_TYPE"
    assert harness.access.intents == []


def test_remember_handler_has_no_unused_clock_dependency():
    import inspect

    from memory_server.application.remote_ingestion import RememberHandler

    assert "clock" not in inspect.signature(RememberHandler).parameters


def test_no_s04_implementation_imports():
    import ast
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    for name in (
        "src/memory_server/application/remote_ingestion.py",
        "src/memory_server/transports/http/routes/ingestion.py",
    ):
        tree = ast.parse((root / name).read_text())
        modules = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }
        modules.update(
            alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
        )
        assert not any(module.startswith("memory_server.runtime.daemon") for module in modules), name

@pytest.mark.parametrize(
    "case,code",
    [
        ("rejected", "E_ADMISSION_REJECTED"),
        ("permission", "E_PERMISSION_DENIED"),
        ("profile", "E_VALIDATION"),
        ("key", "E_IDEMPOTENCY_REQUIRED"),
        ("deadline", "E_TIMEOUT"),
    ],
)
async def test_learn_15_preflight_before_classify_and_lease(harness, case, code):
    principal = harness.tokens._principals[harness.bearer]
    ctx = RequestContext("req-learn-preflight", principal, "alpha", uuid4(), asyncio.get_running_loop().time() + 30)
    if case == "permission":
        ctx = replace(ctx, principal=replace(principal, permissions=frozenset()))
    elif case == "key":
        ctx = replace(ctx, idempotency_key=None)
    elif case == "deadline":
        ctx = replace(ctx, deadline=0)
    command = d.COMMAND_TYPES["learn"](
        operation="learn", profile_id="alpha", path_ids={}, payload=d.LearnRequest(text="temporary is today.")
    )
    if case == "profile":
        # The same text reaches admission with matching profiles: learn is supported.
        with pytest.raises(HttpError) as supported:
            await harness.deps.ports["memories.create"].execute(ctx, command)
        assert supported.value.code == "E_ADMISSION_REJECTED"
        command = command.model_copy(update={"profile_id": "beta"})
        assert command.profile_id != ctx.profile_id
        assert command.profile_id in principal.profiles
    with pytest.raises(HttpError) as caught:
        await harness.deps.ports["memories.create"].execute(ctx, command)
    assert caught.value.code == code
    assert harness.leases.acquisitions == []


def test_ingestion_service_has_no_transport_dependency():
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    source = (root / "src/memory_server/services/ingestion_service.py").read_text()
    assert "transports" not in source
