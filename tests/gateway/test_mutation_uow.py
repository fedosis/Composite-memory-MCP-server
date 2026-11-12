"""S12 seam tests: real ingestion/storage, test-only DTO mapping (S13 owns mapping)."""

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from storage.models.http_request import HttpRequestJournalORM
from storage.models.receipt import MemoryReceiptORM
from storage.outbox import OutboxEntryORM
from storage.repositories.fact_repo import FactRepository
from storage.repositories.http_request_repo import HttpRequestRepository

from memory_server.application import MemoryApplication, RememberCommand
from memory_server.application.memory_service import MemoryService
from memory_server.application.mutation_uow import _HTTP_SHAPE, MUTATION_REPLY, MutationUoW, _digest
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import Principal, RequestContext
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.services.ingestion_service import MemoryIngestionService
from memory_server.transports.http.errors import HttpError


@pytest.fixture
async def provider(tmp_path):
    value = SQLiteProvider("sqlite+aiosqlite:///" + str(tmp_path / "memory.db"))
    await value.initialize()
    try:
        yield value
    finally:
        await value.close()


@pytest.fixture
def app(provider):
    return MemoryApplication(provider._session_factory)


def principal(profile="p1", perms=("memory.write",)):
    return Principal(
        "token", "subject", None, frozenset([profile]), frozenset(perms), 1, datetime.now(UTC) + timedelta(days=1)
    )


def ctx(request_id="r-1", key=None, profile="p1"):
    return RequestContext(
        request_id, principal(profile), profile, key or uuid4(), asyncio.get_running_loop().time() + 30
    )


def command(key_subject="s", object="o", tags=None, profile="p1"):
    return d.MemoriesCreateCommand(
        operation="memories.create",
        profile_id=profile,
        path_ids={},
        payload=d.RememberRequest(
            type="fact",
            subject=key_subject,
            predicate="p",
            object=object,
            tags=tags or [],
            evidence=None,
            expected_version=None,
        ),
    )


def _aware(dt):
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


class _RememberHandler:
    """Only the supplied session; no commit/rollback or external side effects."""

    def __init__(self, provider, context, *, source=None):
        self.provider, self.ctx, self.source = provider, context, source
        self.calls = 0

    async def apply(self, session, command):
        self.calls += 1
        payload = command.payload
        result = await MemoryService(self.provider._session_factory).remember_in_session(
            session,
            RememberCommand(
                subject=payload.subject,
                predicate=payload.predicate,
                object=payload.object,
                confidence=payload.confidence,
                source=self.source or payload.source,
            ),
        )
        fact = result["fact"]
        row = await FactRepository(session).get(fact.id)
        memory = d.Memory(
            id=row.id,
            memory_type="fact",
            content=None,
            payload=None,
            confidence=row.confidence,
            lifecycle_state=row.lifecycle_state,
            revision=row.version,
            source_version=row.version,
            source=None,
            creator=None,
            created_at=_aware(row.created_at),
            updated_at=_aware(row.updated_at),
            risk_tags=[],
            receipt_id=None,
            masked=True,
            projection=d.ItemProjection(vector="unknown", graph="unknown", outbox="unknown"),
        )
        receipt_row = (
            await session.execute(
                select(MemoryReceiptORM).where(
                    MemoryReceiptORM.id == fact.id,
                    MemoryReceiptORM.memory_type == "fact",
                )
            )
        ).scalar_one()
        receipt = d.Receipt(
            id=receipt_row.id,
            memory_type="fact",
            source=receipt_row.source,
            created_by=receipt_row.created_by,
            timestamp=_aware(receipt_row.timestamp),
            confidence=receipt_row.confidence,
            verification_status=receipt_row.verification_status,
            lifecycle_state=receipt_row.lifecycle_state,
            source_version=receipt_row.version,
            history=[],
            history_complete=False,
        )
        outbox_ids = (
            (
                await session.execute(
                    select(OutboxEntryORM.id).where(
                        OutboxEntryORM.record_id == fact.id,
                    )
                )
            )
            .scalars()
            .all()
        )
        return d.WriteResult(
            request_id=self.ctx.request_id,
            memories=[memory],
            receipts=[receipt],
            admission=[],
            outbox_ids=list(outbox_ids),
            reinforced_ids=[],
        )


def executor(provider, **kwargs):
    return MutationUoW(provider._session_factory, verify_principal=lambda p: None, **kwargs)


async def counts(provider):
    async with provider._session_factory() as session:
        return tuple(
            [
                (await session.execute(text(f"SELECT count(*) FROM {table}"))).scalar_one()
                for table in ("facts", "receipts", "outbox_entries", "http_request_journal")
            ]
        )


async def rows(provider):
    async with provider._session_factory() as session:
        return (await session.execute(select(HttpRequestJournalORM))).scalars().all()


async def expect_error(code, awaitable):
    with pytest.raises(HttpError) as caught:
        await awaitable
    assert caught.value.code == code
    assert str(caught.value) == code
    assert MUTATION_REPLY.get() is None


async def test_replay_ten_times_single_effect(provider, app):
    context, cmd = ctx(), command()
    handler, uow = _RememberHandler(provider, context), executor(provider)
    bodies = []
    for index in range(10):
        result = await uow.run(replace(context, request_id=f"r-{index + 1}"), cmd, handler)
        bodies.append(result.model_dump_json())
        assert MUTATION_REPLY.get().replayed is (index > 0)
        assert MUTATION_REPLY.get().original_request_id == "r-1"
    assert len(set(bodies)) == 1
    assert handler.calls == 1
    assert await counts(provider) == (1, 1, 1, 1)
    row = (await rows(provider))[0]
    assert row.response_snapshot == bodies[0]
    result = d.WriteResult.model_validate_json(bodies[0])
    assert json.loads(row.result_refs) == [result.memories[0].id, result.receipts[0].id, *result.outbox_ids]
    assert row.audit_ticket_id is None and row.pending_publication == 0


async def test_concurrent_ten_single_effect(provider):
    context, cmd, uow = ctx(), command(), executor(provider)

    async def run(index):
        local = replace(context, request_id=f"r-{index}")
        try:
            result = await uow.run(local, cmd, _RememberHandler(provider, local))
            return (MUTATION_REPLY.get().replayed, result.model_dump_json())
        except HttpError as exc:
            assert exc.code == "E_REQUEST_IN_PROGRESS"
            return (None, None)

    results = await asyncio.gather(*(run(index) for index in range(10)))
    assert sum(replayed is False for replayed, _ in results) == 1
    assert len({body for _, body in results if body is not None}) == 1
    assert await counts(provider) == (1, 1, 1, 1)
    await uow.run(context, cmd, _RememberHandler(provider, context))
    assert MUTATION_REPLY.get().replayed is True


async def test_journal_unique_scope_backstop(provider):
    # Backstop control: writer serialization + re-read are the primary protection
    # (test_concurrent_ten_single_effect above), so exercise the repo/constraint, not HTTP.
    first = provider._session_factory()
    second = provider._session_factory()
    try:
        first_repo = HttpRequestRepository(first)
        second_repo = HttpRequestRepository(second)
        assert await first_repo.find("subject", "p1", "memories.create", "k") is None
        assert await second_repo.find("subject", "p1", "memories.create", "k") is None
        now = datetime(2026, 1, 1)
        fields = {
            "subject": "subject",
            "profile_id": "p1",
            "operation": "memories.create",
            "idem_key": "k",
            "digest": "a" * 64,
            "response_snapshot": "{}",
            "result_refs": "[]",
            "audit_ticket_id": None,
            "pending_publication": 0,
            "created_at": now,
            "expires_at": now + timedelta(days=1),
        }
        await first_repo.add(**fields, request_id="r-1")
        await first.commit()
        await second_repo.add(**fields, request_id="r-2")
        with pytest.raises(IntegrityError) as caught:
            await second.flush()
        assert "http_request_journal" in str(caught.value.orig)
        assert "UNIQUE constraint failed" in str(caught.value.orig)
    finally:
        try:
            await first.rollback()
        finally:
            try:
                await first.close()
            finally:
                try:
                    await second.rollback()
                finally:
                    await second.close()


async def test_same_key_different_payload_conflict(provider):
    context, uow = ctx(), executor(provider)
    handler = _RememberHandler(provider, context)
    await uow.run(context, command(), handler)
    digest = (await rows(provider))[0].digest
    await expect_error("E_IDEMPOTENCY_CONFLICT", uow.run(context, command(object="o2"), handler))
    assert await counts(provider) == (1, 1, 1, 1)
    assert (await rows(provider))[0].digest == digest
    assert handler.calls == 1


async def test_ttl_published_row_released(provider):
    now = [datetime(2026, 1, 1, tzinfo=UTC)]
    context, cmd = ctx(), command()
    uow = executor(provider, clock=lambda: now[0], ttl_seconds=10)
    handler = _RememberHandler(provider, context)
    first = await uow.run(context, cmd, handler)
    old = (await rows(provider))[0]
    now[0] += timedelta(seconds=9)
    assert (await uow.run(context, cmd, handler)).model_dump_json() == first.model_dump_json()
    assert handler.calls == 1
    now[0] += timedelta(seconds=1)
    await uow.run(context, command(object="after-ttl"), handler)
    assert MUTATION_REPLY.get().replayed is False
    assert handler.calls == 2
    current = (await rows(provider))[0]
    assert current.created_at > old.created_at
    assert current.expires_at.tzinfo is None
    assert await counts(provider) == (2, 2, 2, 1)


async def test_purge_bounded_and_scoped(provider):
    now = datetime(2026, 1, 1)
    async with provider._session_factory() as session, session.begin():
        repo = HttpRequestRepository(session)
        for index in range(61):
            await repo.add(
                subject="s",
                profile_id="p1" if index < 60 else "other",
                operation="learn",
                idem_key=str(uuid4()),
                digest="a" * 64,
                response_snapshot="{}",
                result_refs="[]",
                request_id=str(index),
                audit_ticket_id=None,
                pending_publication=0,
                created_at=now,
                expires_at=now - timedelta(seconds=1) if index < 60 else now + timedelta(days=1),
            )
        await session.flush()
        assert await repo.purge_expired(now, limit=50) == 50
        assert await repo.purge_expired(now, limit=50) == 10
        for limit in (0, 201, True, 1.5):
            with pytest.raises(ValueError):
                await repo.purge_expired(now, limit=limit)
    remaining = await rows(provider)
    assert len(remaining) == 1 and remaining[0].profile_id == "other"


async def test_revoked_principal_blocks_replay(provider):
    class _Revoked(Exception):  # noqa: N818
        code = "E_AUTH_REQUIRED"

    revoked = [False]

    def verify(principal):
        if revoked[0]:
            raise _Revoked()

    context, cmd = ctx(), command()
    uow = MutationUoW(provider._session_factory, verify_principal=verify)
    handler = _RememberHandler(provider, context)
    first = await uow.run(context, cmd, handler)
    revoked[0] = True
    await expect_error("E_AUTH_REQUIRED", uow.run(context, cmd, handler))
    assert handler.calls == 1 and await counts(provider) == (1, 1, 1, 1)
    revoked[0] = False
    assert (await uow.run(context, cmd, handler)).model_dump_json() == first.model_dump_json()
    assert handler.calls == 1


class _AfterWrite(_RememberHandler):
    def __init__(self, provider, context, action):
        super().__init__(provider, context)
        self.action = action

    async def apply(self, session, command):
        result = await super().apply(session, command)
        await self.action(session)
        return result


async def test_handler_root_commit_denied(provider):
    async def commit(session):
        await session.commit()

    context = ctx()
    await expect_error("E_INTERNAL", executor(provider).run(context, command(), _AfterWrite(provider, context, commit)))
    assert await counts(provider) == (0, 0, 0, 0)


async def test_handler_exception_rolls_back(provider):
    async def fail(session):
        raise RuntimeError("private handler failure")

    context, uow = ctx(), executor(provider)
    await expect_error("E_INTERNAL", uow.run(context, command(), _AfterWrite(provider, context, fail)))
    assert await counts(provider) == (0, 0, 0, 0)
    await uow.run(context, command(), _RememberHandler(provider, context))
    assert await counts(provider) == (1, 1, 1, 1)


async def test_cancel_releases_and_retry(provider):
    ready = asyncio.Event()

    async def wait(session):
        ready.set()
        await asyncio.Event().wait()

    context, uow = ctx(), executor(provider)
    task = asyncio.create_task(uow.run(context, command(), _AfterWrite(provider, context, wait)))
    await asyncio.wait_for(ready.wait(), timeout=10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await counts(provider) == (0, 0, 0, 0)
    await uow.run(context, command(), _RememberHandler(provider, context))
    assert await counts(provider) == (1, 1, 1, 1)


async def test_snapshot_integrity_and_canary(provider, caplog):
    context, uow = ctx(), executor(provider)
    result = await uow.run(context, command(tags=["CANARY-PAYLOAD"]), _RememberHandler(provider, context))
    row = (await rows(provider))[0]
    assert row.response_snapshot == result.model_dump_json()
    assert "CANARY-PAYLOAD" not in repr(vars(row))
    other = ctx("r-2")
    await uow.run(other, command(object="canary-result"), _RememberHandler(provider, other, source="CANARY-RESULT"))
    assert "CANARY-RESULT" in (await rows(provider))[1].response_snapshot

    async def fail(session):
        raise RuntimeError("CANARY-RESULT")

    await expect_error("E_INTERNAL", uow.run(ctx("r-3"), command(object="failure"), _AfterWrite(provider, other, fail)))
    async with provider._session_factory() as session, session.begin():
        await session.execute(
            text("UPDATE http_request_journal SET response_snapshot=:snapshot WHERE request_id=:id"),
            {"snapshot": "{", "id": context.request_id},
        )
    await expect_error(
        "E_INTERNAL", uow.run(context, command(tags=["CANARY-PAYLOAD"]), _RememberHandler(provider, context))
    )
    assert "CANARY-PAYLOAD" not in caplog.text
    assert "CANARY-RESULT" not in caplog.text
    assert await counts(provider) == (2, 2, 2, 2)


async def test_cross_profile_isolation(provider, tmp_path):
    other = SQLiteProvider("sqlite+aiosqlite:///" + str(tmp_path / "other.db"))
    await other.initialize()
    try:
        key = uuid4()
        for db, profile in ((provider, "p1"), (other, "p2")):
            context, cmd = ctx(profile=profile, key=key), command(profile=profile)
            uow, handler = executor(db), _RememberHandler(db, context)
            first = await uow.run(context, cmd, handler)
            replay = await uow.run(context, cmd, handler)
            assert replay.model_dump_json() == first.model_dump_json()
            assert MUTATION_REPLY.get().replayed is True
            assert await counts(db) == (1, 1, 1, 1)
            assert (await rows(db))[0].profile_id == profile
    finally:
        await other.close()


async def test_result_shape_guard(provider):
    class Wrong(_RememberHandler):
        async def apply(self, session, command):
            await super().apply(session, command)
            return d.Health(status="ok")

    context = ctx()
    await expect_error("E_INTERNAL", executor(provider).run(context, command(), Wrong(provider, context)))
    assert await counts(provider) == (0, 0, 0, 0)


async def test_schema_rehearsal_on_historical_db(provider, app):
    await app.memory.remember(RememberCommand(subject="legacy", predicate="p", object="o"))
    assert await counts(provider) == (1, 1, 1, 0)
    async with provider._engine.begin() as connection:
        await connection.execute(text("DROP TABLE http_request_journal"))
    await provider.close()
    for index in range(2):
        await provider.initialize()
        assert await counts(provider) == ((1, 1, 1, 0) if index == 0 else (2, 2, 2, 1))
        if index == 0:
            context = ctx()
            await executor(provider).run(context, command(), _RememberHandler(provider, context))
        await provider.close()


async def test_legacy_remember_identity(provider, app, monkeypatch):
    result = await app.memory.remember(RememberCommand(subject="s", predicate="p", object="o", source="user"))
    assert set(result) == {"fact", "receipt"}
    fact, receipt = result["fact"], result["receipt"]
    assert (fact.subject, fact.predicate, fact.object, fact.confidence, fact.source) == ("s", "p", "o", 1.0, "user")
    assert fact.id == receipt.id and receipt.memory_type == "fact"
    assert receipt.source == "user" and receipt.created_by == "user" and receipt.history == []
    assert await counts(provider) == (1, 1, 1, 0)
    reinforced = await app.memory.remember(RememberCommand(subject="s", predicate="p", object="o"))
    assert reinforced["fact"].id == fact.id
    assert await counts(provider) == (1, 1, 1, 0)
    calls = []

    def forbidden():
        calls.append(True)
        raise AssertionError("must validate before opening a session")

    ingestion = MemoryIngestionService(forbidden)
    memory = MemoryService(forbidden)
    for invalid in ({"subject": ""}, {"predicate": ""}, {"object": ""}, {"confidence": 1.1}):
        values = {"subject": "s", "predicate": "p", "object": "o", **invalid}
        with pytest.raises(ValueError):
            await ingestion.remember(**values)
        with pytest.raises(ValueError):
            await memory.remember(RememberCommand(**values))
    assert calls == []


async def test_ticket_pending_blocks_fresh_until_published(provider):
    context, uow = ctx(), executor(provider, ticket_provider=lambda: "t-1")
    handler = _RememberHandler(provider, context)
    await uow.run(context, command(), handler)
    row = (await rows(provider))[0]
    assert row.audit_ticket_id == "t-1" and row.pending_publication == 1
    await expect_error("E_AUDIT_UNAVAILABLE", uow.run(ctx("r-2"), command(object="other"), handler))
    assert await counts(provider) == (1, 1, 1, 1)
    await uow.run(context, command(), handler)
    assert MUTATION_REPLY.get().replayed is True
    assert await uow.mark_published("r-1") is True
    assert await uow.mark_published("r-1") is False
    assert (await rows(provider))[0].pending_publication == 0
    await uow.run(ctx("r-2"), command(object="other"), handler)
    assert await counts(provider) == (2, 2, 2, 2)


async def test_savepoint_allowed_root_commit_denied(provider):
    context, uow = ctx(), executor(provider)
    await uow.run(context, command(), _RememberHandler(provider, context))
    assert await counts(provider) == (1, 1, 1, 1)

    async def commit(session):
        async with session.begin_nested():
            await session.flush()
        await session.commit()

    await expect_error(
        "E_INTERNAL", uow.run(ctx("r-2"), command(object="denied"), _AfterWrite(provider, context, commit))
    )
    assert await counts(provider) == (1, 1, 1, 1)


def test_digest_shape_matches_catalog():
    expected = {
        "memories.create": ("POST", "/api/v1/profiles/{profile}/memories"),
        "learn": ("POST", "/api/v1/profiles/{profile}/learn"),
        "memories.transition": ("POST", "/api/v1/profiles/{profile}/memories/{id}/transition"),
        "memories.invalidate": ("POST", "/api/v1/profiles/{profile}/memories/{id}/invalidate"),
        "beliefs.set": ("POST", "/api/v1/profiles/{profile}/beliefs"),
        "conflicts.resolve": ("POST", "/api/v1/profiles/{profile}/conflicts/{id}/resolve"),
        "relations.create": ("POST", "/api/v1/profiles/{profile}/relations"),
        "relations.delete": ("DELETE", "/api/v1/profiles/{profile}/relations/{id}"),
    }
    assert set(_HTTP_SHAPE) == set(d.PROFILE_MUTATIONS)
    assert dict(_HTTP_SHAPE) == expected
    with pytest.raises(TypeError):
        _HTTP_SHAPE["learn"] = ("DELETE", "bad")
    first = command()
    other = first.model_copy(update={"operation": "learn"})
    assert _digest(first) != _digest(other)
    assert _digest(first) == _digest(command())
    assert _digest(first) != _digest(command(object="changed"))


async def test_ttl_pending_not_deleted(provider):
    now = [datetime(2026, 1, 1, tzinfo=UTC)]
    context = ctx()
    uow = executor(provider, ticket_provider=lambda: "t-1", ttl_seconds=1, clock=lambda: now[0])
    handler = _RememberHandler(provider, context)
    first = await uow.run(context, command(), handler)
    now[0] += timedelta(seconds=1)
    async with provider._session_factory() as session, session.begin():
        repo = HttpRequestRepository(session)
        assert await repo.purge_expired(now[0].replace(tzinfo=None), limit=50) == 0
        row = await repo.find(context.principal.subject, "p1", "memories.create", str(context.idempotency_key))
        with pytest.raises(ValueError, match="pending journal row"):
            await repo.delete(row)
    await expect_error("E_AUDIT_UNAVAILABLE", uow.run(ctx("r-2"), command(object="fresh"), handler))
    assert (await uow.run(context, command(), handler)).model_dump_json() == first.model_dump_json()
    assert MUTATION_REPLY.get().replayed is True
    assert await counts(provider) == (1, 1, 1, 1)
    assert await uow.mark_published("r-1")
    await uow.run(ctx("r-2"), command(object="fresh"), handler)
    assert await counts(provider) == (2, 2, 2, 1)
    assert (await rows(provider))[0].request_id == "r-2"


@pytest.mark.parametrize(
    "case,code",
    [
        ("missing", "E_IDEMPOTENCY_REQUIRED"),
        ("malformed", "E_IDEMPOTENCY_REQUIRED"),
        ("profile", "E_VALIDATION"),
        ("scope", "E_PERMISSION_DENIED"),
        ("timeout", "E_TIMEOUT"),
        ("operation", "E_VALIDATION"),
        ("reveal", "E_VALIDATION"),
        ("shape", "E_VALIDATION"),
    ],
)
async def test_preflight_matrix(provider, case, code):
    context, cmd = ctx(), command()
    if case == "missing":
        context = replace(context, idempotency_key=None)
    elif case == "malformed":
        context = replace(context, idempotency_key="not-uuid")
    elif case == "profile":
        cmd = command(profile="other")
    elif case == "scope":
        context = replace(context, principal=principal("other"))
    elif case == "timeout":
        context = replace(context, deadline=asyncio.get_running_loop().time() - 1)
    elif case in {"operation", "reveal"}:
        cmd = cmd.model_copy(update={"operation": "unknown" if case == "operation" else "memories.reveal"})
    elif case == "shape":
        cmd.payload.tags.append(123)
    handler = _RememberHandler(provider, context)
    await expect_error(code, executor(provider).run(context, cmd, handler))
    assert handler.calls == 0
    assert await counts(provider) == (0, 0, 0, 0)
