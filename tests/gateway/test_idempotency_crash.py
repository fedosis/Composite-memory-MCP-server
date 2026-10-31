"""SIGKILL at real transaction boundaries; no production hooks or live databases."""

import asyncio
import os
import sqlite3
import subprocess
import sys
import time
from uuid import UUID

import pytest

from memory_server.application.mutation_uow import MUTATION_REPLY
from memory_server.providers.sqlite_provider import SQLiteProvider
from tests.gateway.test_mutation_uow import _RememberHandler, command, counts, ctx, executor

CHILD = r"""
import asyncio
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

from sqlalchemy import select

from memory_server.application import MemoryApplication, RememberCommand
from memory_server.application.memory_service import MemoryService
from memory_server.application.mutation_uow import MutationUoW
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import Principal, RequestContext
from memory_server.providers.sqlite_provider import SQLiteProvider
from storage.models.receipt import MemoryReceiptORM
from storage.outbox import OutboxEntryORM
from storage.repositories.fact_repo import FactRepository

KEY = UUID("12345678-1234-4234-8234-123456789abc")

def aware(dt):
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)

class Handler:
    async def apply(self, session, command):
        payload = command.payload
        result = await MemoryService(provider._session_factory).remember_in_session(
            session, RememberCommand(subject=payload.subject, predicate=payload.predicate,
                                     object=payload.object, confidence=payload.confidence,
                                     source=payload.source))
        fact = result["fact"]
        row = await FactRepository(session).get(fact.id)
        memory = d.Memory(
            id=row.id, memory_type="fact", content=None, payload=None, confidence=row.confidence,
            lifecycle_state=row.lifecycle_state, revision=row.version, source_version=row.version,
            source=None, creator=None, created_at=aware(row.created_at), updated_at=aware(row.updated_at),
            risk_tags=[], receipt_id=None, masked=True,
            projection=d.ItemProjection(vector="unknown", graph="unknown", outbox="unknown"))
        receipt_row = (await session.execute(select(MemoryReceiptORM).where(
            MemoryReceiptORM.id == fact.id, MemoryReceiptORM.memory_type == "fact"))).scalar_one()
        receipt = d.Receipt(
            id=receipt_row.id, memory_type="fact", source=receipt_row.source, created_by=receipt_row.created_by,
            timestamp=aware(receipt_row.timestamp), confidence=receipt_row.confidence,
            verification_status=receipt_row.verification_status, lifecycle_state=receipt_row.lifecycle_state,
            source_version=receipt_row.version, history=[], history_complete=False)
        ids = (await session.execute(select(OutboxEntryORM.id).where(
            OutboxEntryORM.record_id == fact.id))).scalars().all()
        result = d.WriteResult(request_id="child-original", memories=[memory], receipts=[receipt],
                               admission=[], outbox_ids=list(ids), reinforced_ids=[])
        Path(os.environ["S12_EXPECTED"]).write_text(result.model_dump_json())
        if os.environ["S12_MODE"] == "pre":
            Path(os.environ["S12_MARKER"]).write_text("ready")
            await asyncio.Event().wait()
        return result

async def main():
    global provider
    provider = SQLiteProvider("sqlite+aiosqlite:///" + os.environ["S12_DB"])
    await provider.initialize()
    app = MemoryApplication(provider._session_factory)
    principal = Principal("token", "subject", None, frozenset(["p1"]), frozenset(["memory.write"]),
                          1, datetime.now(UTC) + timedelta(days=1))
    context = RequestContext("child-original", principal, "p1", KEY, asyncio.get_running_loop().time() + 30)
    command = d.MemoriesCreateCommand(
        operation="memories.create", profile_id="p1", path_ids={},
        payload=d.RememberRequest(type="fact", subject="s", predicate="p", object="o",
                                  evidence=None, expected_version=None))
    await MutationUoW(provider._session_factory, verify_principal=lambda p: None).run(context, command, Handler())
    await asyncio.sleep(10)
    print("response", flush=True)
    await provider.close()

asyncio.run(main())
"""

KEY = UUID("12345678-1234-4234-8234-123456789abc")


def _committed(db):
    if not db.exists():
        return False
    try:
        with sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=0.1) as connection:
            return (
                connection.execute("SELECT count(*) FROM http_request_journal WHERE state='final'").fetchone()[0] == 1
            )
    except sqlite3.OperationalError:
        return False


async def _crash(tmp_path, mode):
    db, marker, expected = (tmp_path / name for name in ("memory.db", "marker", "expected.json"))
    env = dict(
        os.environ,
        S12_DB=str(db),
        S12_MODE=mode,
        S12_MARKER=str(marker),
        S12_EXPECTED=str(expected),
        PYTHONDONTWRITEBYTECODE="1",
    )
    with (tmp_path / "child.log").open("wb") as log:
        child = subprocess.Popen([sys.executable, "-c", CHILD, mode], env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 15
            while not (marker.exists() if mode == "pre" else _committed(db)):
                if child.poll() is not None:
                    pytest.fail("child exited before transaction boundary: " + (tmp_path / "child.log").read_text())
                if time.monotonic() >= deadline:
                    pytest.fail("child transaction boundary timeout")
                await asyncio.sleep(0.02)
            child.kill()
            assert await asyncio.to_thread(child.wait, timeout=5) == -9
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
    return db, expected


class _NeverHandler:
    async def apply(self, session, command):
        raise AssertionError("replay must not invoke handler")


async def _replay(provider, expected):
    context = ctx("parent", key=KEY)
    uow = executor(provider)
    for _ in range(2):
        result = await uow.run(context, command(), _NeverHandler())
        assert result.model_dump_json() == expected.read_text()
        assert MUTATION_REPLY.get().replayed is True
        assert MUTATION_REPLY.get().original_request_id == "child-original"
        assert await counts(provider) == (1, 1, 1, 1)


async def test_crash_before_commit_leaves_nothing(tmp_path):
    db, _ = await _crash(tmp_path, "pre")
    provider = SQLiteProvider("sqlite+aiosqlite:///" + str(db))
    await provider.initialize()
    try:
        assert await counts(provider) == (0, 0, 0, 0)
        context = ctx("parent", key=KEY)
        await executor(provider).run(context, command(), _RememberHandler(provider, context))
        assert MUTATION_REPLY.get().replayed is False
        assert await counts(provider) == (1, 1, 1, 1)
    finally:
        await provider.close()


async def test_crash_after_commit_replays_original(tmp_path):
    db, expected = await _crash(tmp_path, "post")
    provider = SQLiteProvider("sqlite+aiosqlite:///" + str(db))
    await provider.initialize()
    try:
        assert await counts(provider) == (1, 1, 1, 1)
        await _replay(provider, expected)
    finally:
        await provider.close()


async def test_restart_is_idempotent(tmp_path):
    db, expected = await _crash(tmp_path, "post")
    for _ in range(2):
        provider = SQLiteProvider("sqlite+aiosqlite:///" + str(db))
        await provider.initialize()
        try:
            assert await counts(provider) == (1, 1, 1, 1)
            await _replay(provider, expected)
        finally:
            await provider.close()
