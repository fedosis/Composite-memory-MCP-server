"""PR13 acceptance tests: atomic lifecycle, provenance, and outbox writes."""
from __future__ import annotations

import json
from typing import Any

import pytest
from sqlalchemy import event, text
from storage.models import LifecycleEventORM

from memory_server.models import Belief
from memory_server.plugins.hermes.provider import HermesProvider
from memory_server.providers.sqlite_provider import SQLiteProvider


async def _provider(tmp_path, name: str = "memory.db") -> SQLiteProvider:
    provider = SQLiteProvider(
        url=f"sqlite+aiosqlite:///{tmp_path / name}", busy_timeout_ms=100
    )
    await provider.initialize()
    return provider


async def _snapshot(provider: SQLiteProvider) -> dict[str, list[tuple[Any, ...]]]:
    """Return deterministic contents of every SQL table in the database."""
    async with await provider._get_session() as session:
        table_rows = (
            await session.execute(
                text("SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name")
            )
        ).all()
        result: dict[str, list[tuple[Any, ...]]] = {}
        for name, ddl in table_rows:
            if (
                name.startswith("sqlite_")
                or (ddl and "VIRTUAL TABLE" in ddl.upper())
                or name.endswith(("_config", "_content", "_data", "_docsize", "_idx"))
            ):
                continue
            columns = (
                await session.execute(text(f'PRAGMA table_info("{name}")'))
            ).all()
            order = " ORDER BY rowid" if any(row[5] for row in columns) else ""
            rows = (
                await session.execute(text(f'SELECT * FROM "{name}"{order}'))
            ).all()
            result[name] = [tuple(row) for row in rows]
    return result


async def _seed(provider: SQLiteProvider, *items: tuple[str, float]) -> list[Belief]:
    beliefs = []
    for proposition, confidence in items:
        belief = Belief(proposition=proposition, confidence=confidence, source="seed")
        await provider.create_belief(belief)
        beliefs.append(belief)
    return beliefs


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resolution,auto_resolve,new_proposition",
    [
        ("discard_both", False, ""),
        ("merge", False, "merged PR13 proposition"),
        ("keep_a", True, ""),  # confidence gap auto-closes the lower belief
    ],
)
async def test_hermes_conflict_success_is_one_durable_uow(
    tmp_path, resolution, auto_resolve, new_proposition
):
    provider = await _provider(tmp_path)
    hermes = HermesProvider()
    hermes._provider = provider
    hermes._initialized = True
    try:
        a, b = await _seed(provider, ("alpha fact", 0.2), ("beta fact", 0.9))
        result = json.loads(
            await hermes._handle_resolve_conflict(
                a.id, b.id, resolution, new_proposition, auto_resolve
            )
        )
        assert result["events"]
        after = await _snapshot(provider)
        assert after["lifecycle_events"]
        assert after["lifecycle_states"]
        assert any("receipt" in key for key in after)
        if resolution == "merge":
            assert after["outbox_entries"]
            assert result["created"]["proposition"] == new_proposition
            assert sum(1 for row in after["beliefs"] if row) == 3
        else:
            assert not after["outbox_entries"]
        if auto_resolve:
            assert len(result["events"]) == 1
            assert result["events"][0]["to_state"] == "superseded"
        elif resolution == "discard_both":
            assert len(result["events"]) == 2
            assert {e["to_state"] for e in result["events"]} == {"discarded"}
        # A separate provider/session must observe the committed complete state.
        fresh = await _provider(tmp_path)
        try:
            assert await _snapshot(fresh) == after
        finally:
            await fresh.close()
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_hermes_replace_belief_is_atomic_and_emits_durable_records(tmp_path):
    provider = await _provider(tmp_path)
    hermes = HermesProvider()
    hermes._provider = provider
    hermes._initialized = True
    try:
        old = (await _seed(provider, ("old proposition", 0.4)))[0]
        result = json.loads(
            await hermes._handle_set_belief(
                "replacement proposition", 0.7, '[{"source_type":"paper","source_id":"p1","weight":0.6}]',
                '["pr13"]', "test", old.id,
            )
        )
        assert result["belief"]["id"] != old.id
        assert result["superseded"]["id"] == old.id
        state = await _snapshot(provider)
        assert len(state["beliefs"]) == 2
        assert len(state["evidence"]) == 1
        receipt_table = next(key for key in state if "receipt" in key)
        assert len(state[receipt_table]) == 1
        assert len(state["lifecycle_events"]) == 1
        assert len(state["outbox_entries"]) == 1
        assert state["outbox_entries"][0][2] == result["belief"]["id"]
    finally:
        await provider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["lifecycle_flush", "before_commit", "second_supersede"])
@pytest.mark.parametrize("operation", ["merge", "discard_both", "auto_close", "replace", "reinforce"])
async def test_hermes_conflict_fault_rolls_back_every_sql_table(tmp_path, fault, operation):
    provider = await _provider(tmp_path)
    hermes = HermesProvider()
    hermes._provider = provider
    hermes._initialized = True
    reached = False
    event_count = 0
    if fault == "second_supersede" and operation not in {"merge", "discard_both", "auto_close"}:
        await provider.close()
        pytest.skip("operation has one lifecycle transition")
    try:
        a, b = await _seed(provider, ("fault alpha", 0.3), ("fault beta", 0.8))
        before = await _snapshot(provider)
        sync_cls = provider._session_factory.class_.sync_session_class

        def fail_flush(session, flush_context, instances):
            nonlocal reached, event_count
            if any(isinstance(obj, LifecycleEventORM) for obj in session.new):
                event_count += 1
                if fault == "second_supersede" and event_count < 2:
                    return
                reached = True
                raise RuntimeError("PR13 injected lifecycle flush failure")

        def fail_commit(session):
            nonlocal reached
            reached = True
            raise RuntimeError("PR13 injected before_commit failure")

        listener = fail_flush if fault != "before_commit" else fail_commit
        identifier = "before_flush" if fault != "before_commit" else "before_commit"
        event.listen(sync_cls, identifier, listener)
        try:
            with pytest.raises(RuntimeError, match="PR13 injected"):
                if operation in {"replace", "reinforce"}:
                    await hermes._handle_set_belief(
                        "fault alpha" if operation == "reinforce" else "new replacement",
                        0.8, '[{"source_id":"new-evidence","weight":0.8}]',
                        '["new-tag"]', "test", a.id if operation == "replace" else "",
                    )
                else:
                    await hermes._handle_resolve_conflict(
                        a.id, b.id, "keep_a" if operation == "auto_close" else operation,
                        "fault merged", operation == "auto_close",
                    )
        finally:
            event.remove(sync_cls, identifier, listener)
        assert reached, f"failure injection point {fault} was not reached"
        assert await _snapshot(provider) == before
        fresh = await _provider(tmp_path)
        try:
            assert await _snapshot(fresh) == before
        finally:
            await fresh.close()
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_reinforcement_persists_provenance_history_and_version_for_hermes_and_server(
    tmp_path, monkeypatch
):
    provider = await _provider(tmp_path)
    try:
        original = (await _seed(provider, ("reinforce once", 0.4)))[0]
        hermes = HermesProvider()
        hermes._provider = provider
        hermes._initialized = True
        result = json.loads(
            await hermes._handle_set_belief(
                "reinforce once", 0.8,
                '[{"source_type":"paper","source_id":"s1","weight":0.8}]',
                '["tag-pr13"]', "hermes", "",
            )
        )
        assert result["reinforced"] is True
        assert result["belief"]["confidence"] == pytest.approx(0.6)
        assert result["belief"]["version"] == original.version + 1
        assert result["receipt"]["history"][-1]["confidence"] == pytest.approx(0.6)
        assert result["receipt"]["history"][-1]["parsed_tags"] == ["tag-pr13"]
        state = await _snapshot(provider)
        from storage.repositories import EvidenceRepository
        async with await provider._get_session() as session:
            evidence = await EvidenceRepository(session).get_by_belief_id(original.id)
            assert len(evidence) == 1
            assert evidence[0].weight == pytest.approx(0.8)
            assert evidence[0].source_id == "s1"
        assert state["outbox_entries"]
        receipt = await provider.get_receipt(original.id)
        assert receipt is not None
        assert receipt.confidence == pytest.approx(0.6)
        assert receipt.history[-1]["confidence"] == pytest.approx(0.6)

        import memory_server.server as server
        monkeypatch.setattr(server, "_provider", provider)
        monkeypatch.setattr(server, "_graph", None)
        server_result = json.loads(
            await server.set_belief_tool(
                proposition="reinforce once", confidence=0.8,
                sources='[{"source_type":"url","source_id":"s2","weight":0.6}]',
                tags='["server-tag"]', source="server", replace_belief_id="",
            )
        )
        assert server_result["reinforced"] is True
        assert server_result["belief"]["confidence"] == pytest.approx(0.7)
        assert server_result["belief"]["version"] == original.version + 2
        state = await _snapshot(provider)
        assert len(state["evidence"]) == 2
        assert len(state["outbox_entries"]) >= 2
    finally:
        await provider.close()
