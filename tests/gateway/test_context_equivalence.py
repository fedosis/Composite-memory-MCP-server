"""S08a context mask and typed-key invariants."""

import copy
import json
import re
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import event, text
from storage.models.decision import DecisionORM

from memory_server.api.get_context import get_context
from memory_server.application.contracts import ContextCommand
from memory_server.application.gateway_retrieval import mask_context_package
from memory_server.application.retrieval_service import RetrievalService
from memory_server.providers.embedding_provider import MockEmbeddingProvider
from memory_server.runtime import runtime_factory
from tests.gateway.test_http_retrieval import insert_facts, live_server, post, read_scope


def _package(version="1.2.3"):
    return {
        "facts": [
            {
                "id": "f",
                "subject": "secret",
                "predicate": "p",
                "object": "o",
                "dedup_key": "real",
                "source": "s",
                "creator": "c",
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "verification_status": "candidate",
                "lifecycle_state": "active",
                "confidence": 0.8,
                "version": version,
            }
        ],
        "decisions": [
            {
                "id": "d",
                "context": "secret",
                "choice": "choice",
                "reason": "reason",
                "source": "s",
                "creator": "c",
                "rejected_alternatives": ["alt", ""],
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "verification_status": "candidate",
                "lifecycle_state": "active",
                "confidence": 0.8,
                "version": version,
            }
        ],
        "total": 2,
    }


def test_context_mask_is_fresh_and_nested_and_preserves_safe_versions():
    raw = _package()
    masked = mask_context_package(raw)
    assert raw["facts"][0]["dedup_key"] == "real"
    assert masked["facts"][0]["dedup_key"] == "[MASKED]"
    assert masked["decisions"][0]["rejected_alternatives"] == ["[MASKED]", "[MASKED]"]
    assert masked["facts"][0]["version"] == "1.2.3"
    assert list(masked) == list(raw)


@pytest.mark.parametrize(
    "version,expected",
    [
        ("0.1.0", "0.1.0"),
        ("1", "1"),
        ("007", "007"),
        ("1.2.3", "1.2.3"),
        ("v1", "[MASKED]"),
        ("1.2.3-rc1", "[MASKED]"),
        ("", "[MASKED]"),
    ],
)
def test_context_version_rule(version, expected):
    assert mask_context_package(_package(version))["facts"][0]["version"] == expected


def test_context_mask_fails_closed_on_unexpected_shape():
    with pytest.raises(ValueError):
        mask_context_package({"facts": [], "decisions": [], "total": 0, "extra": True})


@pytest.fixture(autouse=True)
def offline_embedder(monkeypatch):
    monkeypatch.setitem(runtime_factory._DEFAULT_RECIPES, "embedder", lambda runtime: MockEmbeddingProvider())


async def seed_decisions(resources, profile="alpha"):
    async with resources.session_factory() as session, session.begin():
        for index, (ident, state, confidence, choice) in enumerate(
            [
                ("shared", "active", 0.8, "CANARY_choice"),
                ("duplicate", "archived", 1.0, "CANARY_choice"),
                ("other", "active", 0.9, "CANARY_other_choice"),
            ]
        ):
            session.add(
                DecisionORM(
                    id=ident,
                    context=f"needle CANARY_context_{profile}",
                    choice=choice,
                    dedup_key=ident,
                    rejected_alternatives=json.dumps(["CANARY_alt0", "", "CANARY_alt2"]),
                    reason="CANARY_reason",
                    source="CANARY_decision_source",
                    creator="CANARY_decision_creator",
                    confidence=confidence if profile == "alpha" else confidence - 0.05,
                    lifecycle_state=state,
                    verification_status="candidate",
                    version="CANARY_version",
                    created_at=datetime(2026, 1, 1) + timedelta(seconds=index),
                    updated_at=datetime(2026, 1, 1) + timedelta(seconds=index),
                )
            )


def independent_oracle(raw):
    value = copy.deepcopy(raw)
    # This oracle is deliberately independent of production mask and gateway DTOs.
    for kind, fields in (
        ("facts", ("subject", "predicate", "object", "dedup_key", "source", "creator")),
        ("decisions", ("context", "choice", "reason", "source", "creator")),
    ):
        for item in value[kind]:
            for key in fields:
                if item[key] is not None:
                    item[key] = "[MASKED]"
            version = item["version"]
            if isinstance(version, str) and (
                len(version) > 128 or re.fullmatch(r"[0-9]+(?:\.[0-9]+)*", version) is None
            ):
                item["version"] = "[MASKED]"
            if kind == "decisions":
                item["rejected_alternatives"] = ["[MASKED]" for _ in item["rejected_alternatives"]]
            for key in ("created_at", "updated_at"):
                stamp = datetime.fromisoformat(item[key])
                item[key] = stamp.replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")
    return value


@pytest.mark.asyncio
@pytest.mark.parametrize("no_fts", [False, True])
async def test_context_direct_service_http_equivalence_typed_mask(tmp_path, no_fts, caplog, monkeypatch):
    async with live_server(tmp_path) as (holder, port, auth):
        for profile in ("alpha", "beta"):
            async with await read_scope(holder, auth, profile) as scope:
                resources = scope.application.read_resources()
                provider = resources.provider
                await insert_facts(
                    resources,
                    [
                        {
                            "id": "shared",
                            "confidence": 0.7 if profile == "alpha" else 0.6,
                            "subject": f"needle CANARY_subject_{profile}",
                            "object": f"needle CANARY_object_{profile}",
                        }
                    ],
                )
                await seed_decisions(resources, profile)
                if no_fts:
                    async with resources.session_factory() as session, session.begin():
                        await session.execute(text("DROP TABLE facts_fts"))
                service = scope.application.retrieval
                batch_calls = []
                sql = []
                original_batch = provider.read_memory_records

                async def batch(session, keys):
                    batch_calls.append(keys)
                    before = len(sql)
                    result = await original_batch(session, keys)
                    data = [s for s in sql[before:] if s.lstrip().upper().startswith("SELECT")]
                    assert len(data) <= 3, data
                    return result

                def capture(conn, cursor, statement, parameters, context, executemany):
                    sql.append(statement)

                monkeypatch.setattr(provider, "read_memory_records", batch)
                event.listen(provider.engine.sync_engine, "before_cursor_execute", capture)
                try:
                    for subject in (None, "", f"needle CANARY_subject_{profile}"):
                        for inactive in (False, True):
                            for limit in (1, 100):
                                raw = await get_context(
                                    provider,
                                    "needle",
                                    subject=subject or None,
                                    max_results=limit,
                                    include_inactive=inactive,
                                    overfetch_factor=service.policy.context_dedup_overfetch_factor,
                                )
                                frozen = copy.deepcopy(raw)
                                direct = await service.get_context(
                                    ContextCommand(
                                        task="needle", subject=subject, max_results=limit, include_inactive=inactive
                                    )
                                )
                                assert direct == raw
                                status, body = await post(
                                    port,
                                    auth,
                                    "context",
                                    profile,
                                    task="needle",
                                    subject=subject,
                                    max_results=limit,
                                    include_inactive=inactive,
                                )
                                assert status == 200, body
                                expected = independent_oracle(raw)
                                assert body["package"] == expected
                                assert list(body["package"]) == list(raw)
                                for group in ("facts", "decisions"):
                                    assert [list(item) for item in body["package"][group]] == [
                                        list(item) for item in raw[group]
                                    ]
                                keys = tuple(
                                    (kind, item["id"])
                                    for group, kind in (("facts", "fact"), ("decisions", "decision"))
                                    for item in raw[group]
                                )
                                assert keys and batch_calls[-1] == keys
                                assert tuple((m["memory_type"], m["id"]) for m in body["memories"]) == keys
                                assert all(
                                    m["masked"]
                                    and m["payload"] is None
                                    and m["content"] is None
                                    and m["source"] is None
                                    and m["creator"] is None
                                    for m in body["memories"]
                                )
                                assert body["estimated_tokens"] is body["tokenizer"] is None
                                assert body["token_count_is_estimate"] is True
                                assert raw == frozen
                                assert "CANARY" not in json.dumps(body)
                    if no_fts:
                        assert raw["facts"][0]["dedup_key"] is not None
                    assert [d["id"] for d in raw["decisions"]] == ["other", "shared"]
                finally:
                    event.remove(provider.engine.sync_engine, "before_cursor_execute", capture)
                    monkeypatch.setattr(provider, "read_memory_records", original_batch)
        assert "CANARY" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["delete", "inactive", "confidence", "timestamp"])
async def test_context_real_membership_metadata_race_fails_closed(tmp_path, monkeypatch, mutation):
    async with live_server(tmp_path) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            resources = scope.application.read_resources()
            await insert_facts(resources, [{"id": "race"}])
            original = RetrievalService.get_context

            async def racing(service, command):
                raw = await original(service, command)
                assert raw["facts"]
                async with resources.session_factory() as session, session.begin():
                    sql = {
                        "delete": "DELETE FROM facts WHERE id='race'",
                        "inactive": "UPDATE facts SET lifecycle_state='archived' WHERE id='race'",
                        "confidence": "UPDATE facts SET confidence=0.1 WHERE id='race'",
                        "timestamp": "UPDATE facts SET updated_at='2026-02-02 00:00:00.000000' WHERE id='race'",
                    }[mutation]
                    await session.execute(text(sql))
                return raw

            monkeypatch.setattr(RetrievalService, "get_context", racing)
            status, body = await post(port, auth, "context")
            assert status == 503 and body["error"]["code"] == "E_STORAGE", body
            assert "package" not in body


@pytest.mark.asyncio
async def test_bulk_typed_keys_bounds_order_and_empty(tmp_path):
    async with live_server(tmp_path) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            resources = scope.application.read_resources()
            await insert_facts(resources, [{"id": "shared"}])
            await seed_decisions(resources)
            queries = []

            def capture(conn, cursor, statement, parameters, context, executemany):
                queries.append(statement)

            event.listen(resources.provider.engine.sync_engine, "before_cursor_execute", capture)
            try:
                async with resources.session_factory() as session:
                    assert await resources.provider.read_memory_records(session, ()) == ()
                    assert not queries
                    keys = (("fact", "shared"), ("decision", "shared"), ("decision", "other"))
                    records = await resources.provider.read_memory_records(session, keys)
                    assert tuple((r.memory_type, r.memory["id"]) for r in records) == keys
                    assert len([q for q in queries if q.lstrip().startswith("SELECT")]) == 3
                    for bad in (
                        (("fact", "shared"),) * 2,
                        (("belief", "id"),),
                        (("fact", ""),),
                        tuple(("fact", str(i)) for i in range(201)),
                    ):
                        with pytest.raises(ValueError):
                            await resources.provider.read_memory_records(session, bad)
            finally:
                event.remove(resources.provider.engine.sync_engine, "before_cursor_execute", capture)


@pytest.mark.parametrize("change", ["missing", "extra", "type", "total"])
def test_context_mask_rejects_malformed_row_or_total(change):
    package = _package()
    if change == "missing":
        del package["facts"][0]["subject"]
    elif change == "extra":
        package["facts"][0]["secret_extra"] = "CANARY"
    elif change == "type":
        package["facts"][0]["subject"] = 5
    else:
        package["total"] = 99
    with pytest.raises(ValueError):
        mask_context_package(package)


@pytest.mark.asyncio
async def test_context_max200_empty_and_revealer_stay_masked(tmp_path, monkeypatch):
    from memory_server.gateway_contracts import dto as d

    async with live_server(tmp_path) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            resources = scope.application.read_resources()
            await insert_facts(
                resources, [{"id": f"bulk-{i}", "subject": "bulkmarker", "object": "bulkmarker"} for i in range(100)]
            )
            async with resources.session_factory() as session, session.begin():
                for i in range(100):
                    session.add(
                        DecisionORM(
                            id=f"bulk-{i}",
                            context="bulkmarker",
                            choice=f"CANARY_bulk{i}",
                            dedup_key=str(i),
                            reason="CANARY_reason",
                            rejected_alternatives='["CANARY_alt"]',
                            confidence=0.8,
                            created_at=datetime(2026, 1, 1),
                            updated_at=datetime(2026, 1, 1),
                        )
                    )
            calls = []
            original = resources.provider.read_memory_records

            async def batch(session, keys):
                calls.append(keys)
                return await original(session, keys)

            monkeypatch.setattr(resources.provider, "read_memory_records", batch)
            now = datetime.now(UTC)
            token = holder.app.dependencies.tokens._bootstrap_create(
                d.TokenCreate(
                    subject="revealer",
                    profiles=["alpha"],
                    permissions=["memory.read", "memory.reveal"],
                    expires_at=now + timedelta(hours=1),
                ),
                now=now,
            )
            revealer = {"Authorization": f"Bearer cmms_{token.token.id}.{token.secret}"}
            for headers in (auth, revealer):
                status, body = await post(port, headers, "context", task="bulkmarker", max_results=100)
                assert status == 200, body
                assert body["package"]["total"] == len(body["memories"]) == len(calls[-1]) == 200
                assert len(body["package"]["facts"]) == len(body["package"]["decisions"]) == 100
                assert [kind for kind, _ in calls[-1]] == ["fact"] * 100 + ["decision"] * 100
                assert "CANARY" not in json.dumps(body)
                assert all(m["masked"] for m in body["memories"])
            calls.clear()
            status, body = await post(port, auth, "context", task="absentmarker")
            assert status == 200 and body["package"] == {"facts": [], "decisions": [], "total": 0}
            assert body["memories"] == [] and calls == []
