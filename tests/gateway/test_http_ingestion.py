"""S13A remote remember through the real HTTP, application, UoW and SQLite boundaries."""

from __future__ import annotations

import asyncio
import base64
import json
import secrets
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from storage.models.http_request import HttpRequestJournalORM
from storage.repositories.fact_repo import FactRepository
from storage.repositories.http_request_repo import HttpRequestRepository
from storage.repositories.receipt_repo import ReceiptRepository

from memory_server.admission import MemoryAdmissionGate
from memory_server.application import MemoryApplication, RememberCommand
from memory_server.application.memory_read_service import MemoryReadService
from memory_server.application.mutation_uow import MutationUoW
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import (
    AuditTicket,
    GatewayDependencies,
    HttpConfig,
    Principal,
    ProfileSession,
)
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.runtime.read_composition import compose_ingestion_ports
from memory_server.transports.http.app import create_http_app
from memory_server.transports.http.routes.memories_read import CursorCodec, create_memory_read_routes


class _AuthError(Exception):  # noqa: N818
    code = "E_AUTH_REQUIRED"


class Tokens:
    def __init__(self):
        self._principals = {}
        self.generation = 1
        self.verify_calls = 0

    def mint(self, *, profiles=("alpha", "beta"), permissions=("memory.read", "memory.write")):
        token_id = uuid4().hex
        secret = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
        bearer = f"cmms_{token_id}.{secret}"
        self._principals[bearer] = Principal(
            token_id,
            "remote-actor",
            None,
            frozenset(profiles),
            frozenset(permissions),
            self.generation,
            datetime.now(UTC) + timedelta(hours=1),
        )
        return bearer

    async def authenticate(self, bearer, *, now):
        principal = self._principals.get(bearer)
        if principal is None or principal.expires_at <= now:
            raise _AuthError
        return principal

    def verify_principal(self, principal, *, now):
        self.verify_calls += 1
        if principal.credential_generation != self.generation or principal.expires_at <= now:
            raise _AuthError


class AccessSpy:
    def __init__(self):
        self.intents = []
        self.finished = []
        self.published = []
        self.publish_result = True

    async def begin(self, ctx, operation):
        ticket = AuditTicket(f"ticket-{len(self.intents) + 1}", ctx.request_id)
        self.intents.append((ticket, operation))
        return ticket

    async def finish(self, ticket, outcome):
        self.finished.append((ticket, outcome))

    async def publish_committed(self, ticket, outcome):
        self.published.append((ticket, outcome))
        return self.publish_result

    async def record_unauthenticated(self, request_id, code):
        return None

    def saturation(self):
        return {"authenticated": 0, "denials": 0, "stale": 0}


class Metrics:
    def __init__(self):
        self.completed = []

    def http_completed(self, ctx, operation, status, duration_seconds):
        self.completed.append((ctx.request_id, operation, status))


class Registry:
    async def reconcile_control(self, **kwargs):
        return True


class Leases:
    def __init__(self, applications):
        self.applications = applications
        self.acquisitions = []

    @asynccontextmanager
    async def acquire(self, ctx):
        self.acquisitions.append((ctx.request_id, ctx.profile_id))
        application = self.applications[ctx.profile_id]
        yield ProfileSession(binding=object(), application=application, runtime=object())


def remember_body(subject="User", predicate="prefers", object="concise durable answers", **updates):
    return {
        "type": "fact",
        "subject": subject,
        "predicate": predicate,
        "object": object,
        "confidence": 0.95,
        "source": "agent",
        "tags": [],
        "evidence": None,
        "expected_version": None,
        **updates,
    }


async def request(app, path, *, method="POST", bearer=None, body=b"", headers=(), query=b""):
    sent = []
    values = [(b"host", b"localhost")]
    values.extend((key.lower().encode(), value.encode()) for key, value in headers)
    if bearer is not None:
        values.append((b"authorization", ("Bearer " + bearer).encode()))
    if isinstance(body, dict):
        body = json.dumps(body).encode()
        if not any(key == b"content-type" for key, _ in values):
            values.append((b"content-type", b"application/json"))
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "scheme": "http",
        "method": method,
        "path": path,
        "raw_path": path.encode(),
        "query_string": query,
        "root_path": "",
        "headers": values,
        "client": ("127.0.0.1", 12345),
        "server": ("127.0.0.1", 8080),
    }

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        sent.append(message)

    async with asyncio.timeout(10):
        await app(scope, receive, send)
    start = next(message for message in sent if message["type"] == "http.response.start")
    raw = b"".join(message.get("body", b"") for message in sent if message["type"] == "http.response.body")
    return SimpleNamespace(
        status=start["status"],
        body=raw,
        json=json.loads(raw) if raw else None,
        headers={key.decode().lower(): value.decode() for key, value in start["headers"]},
    )


async def counts(provider):
    async with provider._session_factory() as session:
        return tuple([
            (await session.execute(text(f"SELECT count(*) FROM {table}"))).scalar_one()
            for table in ("facts", "receipts", "outbox_entries", "http_request_journal")
        ])


async def stored_receipt(provider, receipt_id):
    async with provider._session_factory() as session:
        return await ReceiptRepository(session).get(receipt_id)


async def journal_rows(provider):
    async with provider._session_factory() as session:
        return (await session.execute(select(HttpRequestJournalORM).order_by(HttpRequestJournalORM.id))).scalars().all()


@pytest.fixture
async def harness(tmp_path):
    for name in ("alpha", "beta"):
        (tmp_path / name).mkdir()
    providers = {
        name: SQLiteProvider("sqlite+aiosqlite:///" + str(tmp_path / name / "memory.db"))
        for name in ("alpha", "beta")
    }
    for provider in providers.values():
        await provider.initialize()
    applications = {name: MemoryApplication.from_provider(provider) for name, provider in providers.items()}
    leases = Leases(applications)
    access, tokens, metrics = AccessSpy(), Tokens(), Metrics()
    deps = GatewayDependencies(Registry(), tokens, access, metrics, {}, leases=leases)
    config = HttpConfig(
        control_path=tmp_path / "control" / "control.sqlite",
        rate_burst=500,
        rate_per_minute=500,
        retrieval_concurrency=16,
        allowed_origins=("https://allowed.example",),
    )
    deps, mutation_specs = compose_ingestion_ports(deps, config)
    reader = MemoryReadService(
        leases,
        lambda scope: scope.application.read_resources(),
        classifier=MemoryAdmissionGate(),
        cursor_codec=CursorCodec(secrets.token_bytes(32)),
    )
    read_specs = create_memory_read_routes(reader, None, cursor_codec=reader.cursor_codec)
    ports = dict(deps.ports)
    ports.update({spec.operation: reader for spec in read_specs})
    deps = replace(deps, ports=ports)
    app = create_http_app(config, deps, read_routes=read_specs, mutation_routes=mutation_specs)
    state = SimpleNamespace(
        app=app,
        deps=deps,
        providers=providers,
        applications=applications,
        leases=leases,
        access=access,
        tokens=tokens,
        metrics=metrics,
        bearer=tokens.mint(),
        alpha_only=tokens.mint(profiles=("alpha",)),
        mutation_specs=mutation_specs,
        read_specs=read_specs,
        config=config,
    )
    try:
        yield state
    finally:
        await app.close()
        for provider in providers.values():
            await provider.close()


def key():
    return str(uuid4())


async def post(harness, body, *, profile="alpha", idem=None, bearer=None, headers=()):
    idem_headers = (("Idempotency-Key", idem or key()),)
    return await request(
        harness.app,
        f"/api/v1/profiles/{profile}/memories",
        bearer=bearer or harness.bearer,
        body=body,
        headers=(*idem_headers, *headers),
    )


async def test_create_201_masked_real_effects_and_publication(harness):
    response = await post(harness, remember_body())
    assert response.status == 201
    result = d.WriteResult.model_validate_json(response.body)
    assert result.request_id.startswith("req_")
    assert len(result.memories) == len(result.receipts) == len(result.outbox_ids) == 1
    assert len(result.admission) == 1 and result.admission[0].admitted is True
    assert result.memories[0].masked is True
    assert result.memories[0].content is result.memories[0].payload is None
    assert result.memories[0].projection.outbox == "pending"
    assert result.receipts[0].history == []
    raw_receipt = await stored_receipt(harness.providers["alpha"], result.receipts[0].id)
    assert raw_receipt is not None
    assert sum("admission" in event.get("metadata", {}) for event in raw_receipt.history) == 1
    assert await counts(harness.providers["alpha"]) == (1, 1, 1, 1)
    row = (await journal_rows(harness.providers["alpha"]))[0]
    assert row.audit_ticket_id is not None and row.pending_publication == 0
    assert len(harness.access.published) == 1 and harness.access.finished == []
    ticket, outcome = harness.access.published[0]
    assert ticket.id == row.audit_ticket_id and outcome.status == 201
    assert outcome.receipt_ids == (result.receipts[0].id,)


async def test_replay_twice_byte_equal_headers_and_single_effect(harness):
    idem = key()
    responses = [await post(harness, remember_body(), idem=idem) for _ in range(3)]
    assert [response.status for response in responses] == [201, 201, 201]
    assert len({response.body for response in responses}) == 1
    original = responses[0].json["request_id"]
    for response in responses[1:]:
        assert response.headers["idempotency-replayed"] == "true"
        assert response.headers["x-original-request-id"] == original
    assert await counts(harness.providers["alpha"]) == (1, 1, 1, 1)
    assert len(harness.access.published) == 1
    assert len(harness.access.finished) == 2


async def test_replay_2xx_finishes_current_intent(harness):
    idem = key()
    assert (await post(harness, remember_body(), idem=idem)).status == 201
    harness.access.publish_result = False
    replay = await post(harness, remember_body(), idem=idem)
    assert replay.status == 201 and replay.headers["idempotency-replayed"] == "true"
    current = harness.access.intents[-1][0]
    assert [(ticket, outcome.status) for ticket, outcome in harness.access.finished] == [(current, 201)]


@pytest.mark.parametrize("pending", [False, True])
async def test_replay_never_publishes_current_ticket(harness, monkeypatch, pending):
    idem = key()
    harness.access.publish_result = not pending
    assert (await post(harness, remember_body(), idem=idem)).status == 201
    original = harness.access.intents[0][0]
    harness.access.published.clear()
    marked = []
    mark = MutationUoW.mark_published

    async def track_mark(self, request_id):
        marked.append(request_id)
        await mark(self, request_id)

    monkeypatch.setattr(MutationUoW, "mark_published", track_mark)
    harness.access.publish_result = True
    replay = await post(harness, remember_body(), idem=idem)
    assert replay.status == 201 and replay.headers["idempotency-replayed"] == "true"
    current = harness.access.intents[-1][0]
    assert current != original
    assert all(ticket.request_id != current.request_id for ticket, _ in harness.access.published)
    assert [ticket for ticket, _ in harness.access.published] == ([original] if pending else [])
    assert current.request_id not in marked
    assert (await journal_rows(harness.providers["alpha"]))[0].pending_publication == 0


@pytest.mark.parametrize("failure", ["false", "exception"])
async def test_replay_2xx_survives_publish_failure(harness, failure):
    harness.access.publish_result = False
    idem = key()
    first = await post(harness, remember_body(), idem=idem)
    assert first.status == 201
    attempts = []

    async def fail_publish(ticket, outcome):
        attempts.append(ticket)
        if failure == "exception":
            raise RuntimeError("synthetic publisher failure")
        return False

    harness.deps.ports["memories.create"]._publish_outcome = fail_publish
    replay = await post(harness, remember_body(), idem=idem)
    assert replay.status == 201 and replay.body == first.body
    assert replay.headers["idempotency-replayed"] == "true"
    assert attempts
    assert (await journal_rows(harness.providers["alpha"]))[0].pending_publication == 1


async def test_key_conflict_missing_malformed_duplicate_and_media_validation(harness):
    idem = key()
    assert (await post(harness, remember_body(), idem=idem)).status == 201
    conflict = await post(harness, remember_body(object="changed durable value"), idem=idem)
    assert conflict.status == 409 and conflict.json["error"]["code"] == "E_IDEMPOTENCY_CONFLICT"
    missing = await request(
        harness.app,
        "/api/v1/profiles/alpha/memories",
        bearer=harness.bearer,
        body=remember_body(subject="Missing"),
    )
    malformed = await post(harness, remember_body(subject="Malformed"), idem="not-a-uuid")
    duplicate = await request(
        harness.app,
        "/api/v1/profiles/alpha/memories",
        bearer=harness.bearer,
        body=remember_body(subject="Duplicate"),
        headers=(("Idempotency-Key", key()), ("Idempotency-Key", key())),
    )
    for response, code in (
        (missing, "E_IDEMPOTENCY_REQUIRED"),
        (malformed, "E_VALIDATION"),
        (duplicate, "E_VALIDATION"),
    ):
        assert response.status == 400 if code == "E_IDEMPOTENCY_REQUIRED" else response.status == 422
        assert response.json["error"]["code"] == code


async def test_concurrent_ten_one_effect(harness):
    idem = key()
    responses = await asyncio.gather(*(post(harness, remember_body(), idem=idem) for _ in range(10)))
    assert sum(response.status == 201 for response in responses) >= 1
    assert all(response.status in {201, 409} for response in responses), [
        (response.status, response.json) for response in responses
    ]
    assert all(
        response.status == 201 or response.json["error"]["code"] == "E_REQUEST_IN_PROGRESS"
        for response in responses
    )
    assert await counts(harness.providers["alpha"]) == (1, 1, 1, 1)


async def test_admission_reject_zero_rows_and_metadata_once(harness):
    rejected = await post(harness, remember_body("temporary", "note", "today"))
    assert rejected.status == 422 and rejected.json["error"]["code"] == "E_ADMISSION_REJECTED"
    assert await counts(harness.providers["alpha"]) == (0, 0, 0, 0)
    accepted = await post(harness, remember_body())
    assert accepted.status == 201
    assert accepted.json["receipts"][0]["history"] == []
    raw_receipt = await stored_receipt(
        harness.providers["alpha"], accepted.json["receipts"][0]["id"]
    )
    assert raw_receipt is not None
    assert sum("admission" in event.get("metadata", {}) for event in raw_receipt.history) == 1


async def test_dedup_requires_version_then_reinforces_once(harness):
    created = await post(harness, remember_body())
    memory_id = created.json["memories"][0]["id"]
    missing = await post(harness, remember_body())
    assert missing.status == 428 and missing.json["error"]["details"] == {"id": memory_id}
    wrong = await post(harness, remember_body(expected_version=99))
    assert wrong.status == 409 and wrong.json["error"]["code"] == "E_VERSION_CONFLICT"
    idem = key()
    reinforced = await post(harness, remember_body(expected_version=1), idem=idem)
    assert reinforced.status == 200
    assert reinforced.json["reinforced_ids"] == [memory_id]
    assert reinforced.json["memories"][0]["revision"] == 2
    replay = await post(harness, remember_body(expected_version=1), idem=idem)
    assert replay.status == 200 and replay.body == reinforced.body
    async with harness.providers["alpha"]._session_factory() as session:
        assert (await FactRepository(session).get(memory_id)).version == 2
    assert await counts(harness.providers["alpha"]) == (1, 1, 1, 2)


async def test_silent_reinforce_race_rolls_back_request(harness, monkeypatch):
    created = await post(harness, remember_body())
    assert created.status == 201
    memory_id = created.json["memories"][0]["id"]
    before_counts = await counts(harness.providers["alpha"])
    before_receipt = await stored_receipt(harness.providers["alpha"], memory_id)
    assert before_receipt is not None
    before_history = list(before_receipt.history)
    find = FactRepository.find_existing
    calls = []

    async def hide_first(self, *args, **kwargs):
        calls.append(None)
        if len(calls) == 1:
            return None
        return await find(self, *args, **kwargs)

    memory = harness.applications["alpha"].memory
    remember = memory.remember_in_session
    observed = []

    async def observe_reinforce(session, command):
        result = await remember(session, command)
        observed.append(result["receipt"].history[-1]["kind"])
        return result

    monkeypatch.setattr(FactRepository, "find_existing", hide_first)
    monkeypatch.setattr(memory, "remember_in_session", observe_reinforce)
    response = await post(harness, remember_body())
    assert observed == ["reinforce"] and len(calls) >= 2
    assert response.status == 409 and response.json["error"]["code"] == "E_VERSION_CONFLICT"
    assert await counts(harness.providers["alpha"]) == before_counts
    async with harness.providers["alpha"]._session_factory() as session:
        assert (await FactRepository(session).get(memory_id)).version == 1
    after_receipt = await stored_receipt(harness.providers["alpha"], memory_id)
    assert after_receipt is not None
    assert after_receipt.version == before_receipt.version
    assert after_receipt.history == before_history
    assert len(harness.access.published) == 1
    assert len(harness.access.finished) == 1 and harness.access.finished[0][1].status == 409


async def test_publish_pending_blocks_fresh_and_replay_self_heals(harness):
    harness.access.publish_result = False
    idem = key()
    first = await post(harness, remember_body(), idem=idem)
    assert first.status == 201
    assert (await journal_rows(harness.providers["alpha"]))[0].pending_publication == 1
    blocked = await post(harness, remember_body("Other", "stores", "durable preference"))
    assert blocked.status == 503 and blocked.json["error"]["code"] == "E_AUDIT_UNAVAILABLE"
    harness.access.publish_result = True
    replay = await post(harness, remember_body(), idem=idem)
    assert replay.status == 201 and replay.headers["idempotency-replayed"] == "true"
    assert (await journal_rows(harness.providers["alpha"]))[0].pending_publication == 0
    fresh = await post(harness, remember_body("Other", "stores", "durable preference"))
    assert fresh.status == 201
    original_ticket = (await journal_rows(harness.providers["alpha"]))[0].audit_ticket_id
    assert sum(ticket.id == original_ticket for ticket, _ in harness.access.published) >= 2


async def test_mark_published_failure_preserves_2xx_and_pending(harness, monkeypatch):
    async def fail_mark_published(self, request_id):
        raise RuntimeError("synthetic mark-published failure")

    monkeypatch.setattr(MutationUoW, "mark_published", fail_mark_published)
    response = await post(harness, remember_body())
    assert response.status == 201
    assert await counts(harness.providers["alpha"]) == (1, 1, 1, 1)
    assert (await journal_rows(harness.providers["alpha"]))[0].pending_publication == 1


async def test_non_2xx_guard_finishes_once_and_port_does_not_publish(harness):
    response = await post(harness, remember_body("temporary", "note", "today"))
    assert response.status == 422
    assert len(harness.access.intents) == 1
    assert len(harness.access.finished) == 1
    assert harness.access.finished[0][1].status == 422
    assert harness.access.published == []


async def test_journal_failure_rolls_back_all_rows(harness, monkeypatch):
    async def fail(*args, **kwargs):
        import sqlite3

        raise sqlite3.OperationalError("synthetic journal failure")

    monkeypatch.setattr(HttpRequestRepository, "add", fail)
    response = await post(harness, remember_body())
    assert response.status == 503 and response.json["error"]["code"] == "E_STORAGE"
    assert await counts(harness.providers["alpha"]) == (0, 0, 0, 0)
    assert len(harness.access.finished) == 1 and harness.access.published == []


async def test_profile_isolation_same_key_and_cross_profile_denied(harness):
    idem = key()
    alpha = await post(harness, remember_body(), profile="alpha", idem=idem)
    beta = await post(harness, remember_body(), profile="beta", idem=idem)
    assert alpha.status == beta.status == 201
    assert alpha.json["memories"][0]["id"] != beta.json["memories"][0]["id"]
    assert await counts(harness.providers["alpha"]) == (1, 1, 1, 1)
    assert await counts(harness.providers["beta"]) == (1, 1, 1, 1)
    denied = await post(
        harness,
        remember_body("Denied", "stores", "durable value"),
        profile="beta",
        bearer=harness.alpha_only,
    )
    assert denied.status == 403 and denied.json["error"]["code"] == "E_PERMISSION_DENIED"


async def test_mount_missing_port_and_shared_path_method_dispatch(harness):
    assert len(harness.mutation_specs) == 2
    assert len(harness.read_specs) == 6
    get = await request(
        harness.app,
        "/api/v1/profiles/alpha/memories",
        method="GET",
        bearer=harness.bearer,
    )
    assert get.status == 200
    head = await request(
        harness.app,
        "/api/v1/profiles/alpha/memories",
        method="HEAD",
        bearer=harness.bearer,
    )
    assert head.status == 200 and head.body == b""
    options = await request(
        harness.app,
        "/api/v1/profiles/alpha/memories",
        method="OPTIONS",
        headers=(("Origin", "https://allowed.example"),),
    )
    assert options.status == 204
    assert options.headers["access-control-allow-methods"] == "GET, POST, HEAD, OPTIONS"
    put = await request(harness.app, "/api/v1/profiles/alpha/memories", method="PUT")
    assert put.status == 405 and put.json["error"]["code"] == "E_METHOD_NOT_ALLOWED"

    missing_ports = dict(harness.deps.ports)
    del missing_ports["memories.create"]
    missing = create_http_app(
        harness.config,
        replace(harness.deps, ports=missing_ports),
        read_routes=harness.read_specs,
        mutation_routes=harness.mutation_specs,
    )
    before = len(harness.leases.acquisitions)
    try:
        response = await request(
            missing,
            "/api/v1/profiles/alpha/memories",
            bearer=harness.bearer,
            body=remember_body(),
            headers=(("Idempotency-Key", key()),),
        )
        assert response.status == 503 and response.json["error"]["code"] == "E_CAPABILITY_UNAVAILABLE"
        assert len(harness.leases.acquisitions) == before
    finally:
        await missing.close()


async def test_legacy_remember_characterization_unchanged(harness):
    result = await harness.applications["alpha"].memory.remember(
        RememberCommand(subject="Legacy", predicate="keeps", object="existing behavior", source="user")
    )
    assert set(result) == {"fact", "receipt"}
    assert result["fact"].id == result["receipt"].id
    assert result["fact"].version == 1
    assert result["receipt"].history == []
    assert await counts(harness.providers["alpha"]) == (1, 1, 1, 0)
    reinforced = await harness.applications["alpha"].memory.remember(
        RememberCommand(subject="Legacy", predicate="keeps", object="existing behavior", source="user")
    )
    assert reinforced["fact"].id == result["fact"].id
    assert reinforced["fact"].version == 2
    assert reinforced["receipt"].history[-1]["kind"] == "reinforce"
    before = await counts(harness.providers["alpha"])
    remote = await post(harness, remember_body("Legacy", "keeps", "existing behavior"))
    assert remote.status == 428
    assert remote.json["error"]["code"] == "E_PRECONDITION_REQUIRED"
    assert remote.json["error"]["details"] == {"id": result["fact"].id}
    assert await counts(harness.providers["alpha"]) == before
    async with harness.providers["alpha"]._session_factory() as session:
        assert (await FactRepository(session).get(result["fact"].id)).version == 2

LEARN_TEXT = "Postgres is a reliable relational database."
MIXED_TEXT = (
    LEARN_TEXT + "\nWe decided to prefer durable database storage because reliability matters."
    "\nTo preserve durable database backups, do: 1) snapshot, 2) verify."
)


async def learn_post(h, *, value=LEARN_TEXT, profile="alpha", idem=None, **updates):
    return await request(
        h.app,
        f"/api/v1/profiles/{profile}/learn",
        bearer=h.bearer,
        body={"text": value, "extract_beliefs": False, **updates},
        headers=(("Idempotency-Key", idem or key()),),
    )


async def learn_counts(h):
    async with h.providers["alpha"]._session_factory() as session:
        return tuple(
            [
                (await session.execute(text(f"SELECT count(*) FROM {table}"))).scalar_one()
                for table in (
                    "facts",
                    "decisions",
                    "skills",
                    "beliefs",
                    "receipts",
                    "outbox_entries",
                    "http_request_journal",
                )
            ]
        )


def belief_stub(monkeypatch):
    from memory_server.extractors.belief_extractor import BeliefExtractor, ExtractedBelief

    async def extract(self, text):
        return [
            ExtractedBelief(
                proposition="Durable database storage improves reliability",
                confidence=0.9,
                source_refs=["Postgres is a reliable relational database"],
                tags=["database"],
            )
        ]

    monkeypatch.setattr(BeliefExtractor, "extract", extract)


async def test_learn_01_fact(harness):
    r = await learn_post(harness)
    assert r.status == 201
    result = d.WriteResult.model_validate_json(r.body)
    assert len(result.memories) == len(result.receipts) == len(result.outbox_ids) == 1
    m = result.memories[0]
    assert m.memory_type == "fact" and m.revision == m.source_version == 1
    assert m.masked and m.content is m.payload is m.source is m.creator is None
    assert m.projection.outbox == "pending" and result.admission[0].admitted
    assert await learn_counts(harness) == (1, 0, 0, 0, 1, 1, 1)
    assert (await journal_rows(harness.providers["alpha"]))[0].pending_publication == 0
    assert len(harness.access.published) == 1 and harness.access.finished == []


async def test_learn_02_mixed(harness):
    r = await learn_post(harness, value=MIXED_TEXT)
    assert r.status == 201
    items = {m["memory_type"]: m for m in r.json["memories"]}
    assert set(items) == {"fact", "decision", "skill"}
    for kind, version in (("decision", "0.1.0"), ("skill", "1.0.0")):
        assert items[kind]["revision"] is None and items[kind]["source_version"] == version
    assert await learn_counts(harness) == (1, 1, 1, 0, 3, 3, 1)
    assert len(r.json["admission"]) == 3


async def test_learn_03_rejected(harness):
    r = await learn_post(harness, value="temporary is today.", extract_beliefs=True)
    assert r.status == 422 and r.json["error"]["code"] == "E_ADMISSION_REJECTED"
    assert await learn_counts(harness) == (0,) * 7 and harness.leases.acquisitions == []


async def test_learn_04_partial(harness):
    r = await learn_post(harness, value=LEARN_TEXT + "\ntemporary is today.")
    assert r.status == 201
    assert len(r.json["memories"]) == len(r.json["admission"]) == 1
    assert all(a["admitted"] for a in r.json["admission"])
    assert await learn_counts(harness) == (1, 0, 0, 0, 1, 1, 1)


@pytest.mark.parametrize("expected,status", [(None, 428), (99, 409), (1, 200)])
async def test_learn_05_cas(harness, expected, status):
    created = await post(harness, remember_body("Postgres", "is", "a reliable relational database"))
    assert created.status == 201
    mid = created.json["memories"][0]["id"]
    r = await learn_post(harness, expected_versions={} if expected is None else {mid: expected})
    assert r.status == status
    if status == 428:
        assert r.json["error"]["details"] == {"id": mid}
    elif status == 409:
        assert r.json["error"]["code"] == "E_VERSION_CONFLICT"
    else:
        assert r.json["reinforced_ids"] == [mid] and r.json["memories"][0]["revision"] == 2
    assert await counts(harness.providers["alpha"]) == (1, 1, 1, 2 if status == 200 else 1)


async def test_learn_06_cas_before_writes(harness, monkeypatch):
    created = await post(harness, remember_body("Postgres", "is", "a reliable relational database"))
    mid = created.json["memories"][0]["id"]
    before = await learn_counts(harness)
    writes = []
    create = FactRepository.create

    async def observe(self, fact):
        writes.append(fact.id)
        return await create(self, fact)

    monkeypatch.setattr(FactRepository, "create", observe)
    r = await learn_post(
        harness, value="SQLite is a durable embedded database.\n" + MIXED_TEXT, expected_versions={mid: 99}
    )
    assert r.status == 409
    assert writes == [] and await learn_counts(harness) == before


async def test_learn_07_belief_rollback(harness, monkeypatch):
    from storage.repositories import BeliefRepository

    belief_stub(monkeypatch)
    calls = []

    async def fail(self, belief):
        calls.append(belief.id)
        raise RuntimeError("synthetic belief failure")

    monkeypatch.setattr(BeliefRepository, "create", fail)
    r = await learn_post(harness, value=MIXED_TEXT, extract_beliefs=True)
    assert r.status == 500
    assert len(calls) == 1 and await learn_counts(harness) == (0,) * 7


@pytest.mark.parametrize("enabled", [False, True])
async def test_learn_08_belief_flag(harness, monkeypatch, enabled):
    belief_stub(monkeypatch)
    r = await learn_post(harness, extract_beliefs=enabled)
    assert r.status == 201
    assert {m["memory_type"] for m in r.json["memories"]} == ({"fact", "belief"} if enabled else {"fact"})
    assert await learn_counts(harness) == (1, 0, 0, int(enabled), 1 + int(enabled), 1 + int(enabled), 1)
    if enabled:
        belief = next(m for m in r.json["memories"] if m["memory_type"] == "belief")
        assert belief["revision"] == belief["source_version"] == 1
        async with harness.providers["alpha"]._session_factory() as session:
            assert (await session.execute(text("SELECT count(*) FROM evidence"))).scalar_one() == 1


async def test_learn_09_replay(harness):
    idem = key()
    first = await learn_post(harness, value=MIXED_TEXT, idem=idem)
    replay = await learn_post(harness, value=MIXED_TEXT, idem=idem)
    assert first.status == replay.status == 201
    assert replay.body == first.body and replay.headers["idempotency-replayed"] == "true"
    assert await learn_counts(harness) == (1, 1, 1, 0, 3, 3, 1)
    assert len(harness.access.published) == len(harness.access.finished) == 1


async def test_learn_10_profile(harness):
    idem = key()
    a = await learn_post(harness, idem=idem)
    b = await learn_post(harness, profile="beta", idem=idem)
    assert a.status == b.status == 201
    assert a.json["memories"][0]["id"] != b.json["memories"][0]["id"]
    assert await counts(harness.providers["beta"]) == (1, 1, 1, 1)
    denied = await request(
        harness.app,
        "/api/v1/profiles/beta/learn",
        bearer=harness.alpha_only,
        body={"text": LEARN_TEXT},
        headers=(("Idempotency-Key", key()),),
    )
    assert denied.status == 403


async def test_learn_11_http_guards(harness):
    path = "/api/v1/profiles/alpha/learn"
    missing = await request(harness.app, path, bearer=harness.bearer, body={"text": LEARN_TEXT})
    assert missing.status == 400
    media = await request(
        harness.app,
        path,
        bearer=harness.bearer,
        body=b"{}",
        headers=(("Idempotency-Key", key()), ("Content-Type", "text/plain")),
    )
    assert media.status == 415
    assert (await request(harness.app, path, method="GET", bearer=harness.bearer)).status == 405
    ports = {k: v for k, v in harness.deps.ports.items() if k != "learn"}
    app = create_http_app(harness.config, replace(harness.deps, ports=ports), mutation_routes=harness.mutation_specs)
    try:
        r = await request(
            app, path, bearer=harness.bearer, body={"text": LEARN_TEXT}, headers=(("Idempotency-Key", key()),)
        )
        assert r.status == 503 and r.json["error"]["code"] == "E_CAPABILITY_UNAVAILABLE"
        assert harness.leases.acquisitions == []
    finally:
        await app.close()


async def test_learn_12_mount(harness):
    from memory_server.transports.http.routes.ingestion import INGESTION_ROUTE_SPECS, create_ingestion_routes

    specs = create_ingestion_routes()
    assert len(specs) == 2 and {s.operation for s in specs} == {"memories.create", "learn"}
    assert all(s is INGESTION_ROUTE_SPECS[s.operation] for s in specs)
    assert harness.deps.ports["memories.create"] is harness.deps.ports["learn"]


async def test_learn_13_legacy_parity(harness):
    from memory_server.application.contracts import LearnCommand

    memory = harness.applications["alpha"].memory
    first = await memory.learn(LearnCommand(text=LEARN_TEXT, source="user", extract_beliefs=False))
    second = await memory.learn(LearnCommand(text=LEARN_TEXT, source="user", extract_beliefs=False))
    assert first["facts"][0]["item"]["id"] == second["facts"][0]["item"]["id"]
    assert second["facts"][0]["item"]["version"] == 2
    assert second["facts"][0]["receipt"]["history"][-1]["kind"] == "reinforce"
    r = await learn_post(harness)
    assert r.status == 428
    assert await counts(harness.providers["alpha"]) == (1, 1, 1, 0)


async def test_learn_14_soft_limit(harness, monkeypatch):
    from storage.repositories import BeliefRepository

    from memory_server.models import Belief

    belief_stub(monkeypatch)
    async with harness.providers["alpha"]._session_factory() as session, session.begin():
        await BeliefRepository(session).create(Belief(proposition="Existing active durable belief", confidence=0.9))
    monkeypatch.setattr("memory_server.settings.get_settings", lambda: SimpleNamespace(max_active_beliefs=1))
    r = await learn_post(harness, extract_beliefs=True)
    assert r.status == 201 and [m["memory_type"] for m in r.json["memories"]] == ["fact"]
    assert await learn_counts(harness) == (1, 0, 0, 1, 1, 1, 1)


@pytest.mark.parametrize("expected,status", [(None, 428), (99, 409), (1, 200)])
async def test_learn_race_cas_after_integrity_error(harness, monkeypatch, expected, status):
    created = await post(harness, remember_body("Postgres", "is", "a reliable relational database"))
    mid = created.json["memories"][0]["id"]
    find = FactRepository.find_existing
    calls = []

    async def hide_prechecks(self, *args, **kwargs):
        calls.append(None)
        return None if len(calls) <= 2 else await find(self, *args, **kwargs)

    monkeypatch.setattr(FactRepository, "find_existing", hide_prechecks)
    r = await learn_post(harness, expected_versions={} if expected is None else {mid: expected})
    assert len(calls) == 3 and r.status == status
    if status == 428:
        assert r.json["error"]["details"] == {"id": mid}
    async with harness.providers["alpha"]._session_factory() as session:
        assert (await FactRepository(session).get(mid)).version == (2 if status == 200 else 1)
    assert await counts(harness.providers["alpha"]) == (1, 1, 1, 2 if status == 200 else 1)


async def test_learn_belief_reinforce_and_outbox_scope(harness, monkeypatch):
    belief_stub(monkeypatch)
    first = await learn_post(harness, extract_beliefs=True)
    assert first.status == 201
    mid = next(m["id"] for m in first.json["memories"] if m["memory_type"] == "fact")
    second = await learn_post(harness, extract_beliefs=True, expected_versions={mid: 1})
    assert second.status == 201
    belief = next(m for m in second.json["memories"] if m["memory_type"] == "belief")
    assert belief["revision"] == belief["source_version"] == 2
    assert len(second.json["outbox_ids"]) == 1
    assert not set(second.json["outbox_ids"]) & set(first.json["outbox_ids"])
    assert await learn_counts(harness) == (1, 0, 0, 1, 2, 3, 2)
