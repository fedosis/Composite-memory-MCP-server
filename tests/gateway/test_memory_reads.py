"""S07 real HTTP -> Guard -> read service -> borrowed SQLAlchemy -> SQLite.

Only lease/trace neighbours are synthetic Protocol implementations; profile
resolution, tokens, audit, metrics, storage and classifier are production.
"""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlencode
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest
from prometheus_client import generate_latest
from sqlalchemy import event, insert, text
from sqlalchemy.ext.asyncio import AsyncSession
from storage.models.belief import BeliefORM
from storage.models.decision import DecisionORM
from storage.models.fact import FactORM
from storage.models.lifecycle import LifecycleEventORM
from storage.models.receipt import MemoryReceiptORM
from storage.models.skill import SkillORM

from memory_server.admission import MemoryAdmissionGate
from memory_server.application.memory_read_service import (
    HistorySlice,
    MemoryReadService,
    ReadRecord,
    ReadResources,
    classify_read_content,
)
from memory_server.gateway_contracts import dto as d
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.transports.http.app import create_http_app
from memory_server.transports.http.routes.memories_read import CursorCodec, create_memory_read_routes
from tests.gateway.test_http_shell import code, request, rows
from tests.gateway.test_http_shell import shell as shell

STAMP = datetime(2026, 1, 1, tzinfo=UTC)
KINDS = ("fact", "belief", "decision", "skill")
IDS = {kind: str(uuid5(NAMESPACE_URL, "s07-" + kind)) for kind in KINDS}
C = "S07_CANARY_"


def canary_evidence(channel, value, *, live=False):
    encoded = value if isinstance(value, bytes) else value.encode()
    present = C.encode() in encoded
    assert present is live
    if os.environ.get("S07_EVIDENCE_DIR"):
        path = Path(os.environ["S07_EVIDENCE_DIR"]) / "canary-scans.jsonl"
        with path.open("a") as out:
            out.write(
                json.dumps(
                    dict(
                        test=os.environ.get("PYTEST_CURRENT_TEST"),
                        channel=channel,
                        bytes_scanned=len(encoded),
                        canary_present=present,
                        expected_live=live,
                    )
                )
                + "\n"
            )


async def seed(provider, kind="fact", ident=None, marker="alpha", at=STAMP, history=None, receipt=True):
    ident = ident or IDS[kind]
    common = dict(
        id=ident,
        confidence=0.8,
        source=C + marker + "_source",
        creator=C + marker + "_creator",
        created_at=at,
        updated_at=at,
        lifecycle_state="active",
        verification_status="candidate",
        version=1 if kind == "belief" else "1",
    )
    model, fields = {
        "fact": (FactORM, dict(subject="project", predicate="uses", object=C + marker + "_object", dedup_key=ident)),
        "belief": (BeliefORM, dict(proposition=C + marker + "_proposition", tags=[], source_ids=[])),
        "decision": (
            DecisionORM,
            dict(
                context="project",
                choice=C + marker + "_choice",
                reason=C + marker + "_reason",
                rejected_alternatives="[]",
                dedup_key=ident,
            ),
        ),
        "skill": (
            SkillORM,
            dict(
                name="build",
                purpose="project",
                steps=json.dumps([C + marker + "_steps"]),
                constraints="[]",
                validation="[]",
                success_rate=0.9,
            ),
        ),
    }[kind]
    if history is None:
        history = [
            dict(
                timestamp=STAMP.isoformat(),
                reason=C + marker + "_history",
                evidence=dict(
                    method=C + marker + "_method",
                    sources=[C + marker + "_sources"],
                    derived_from=[C + marker + "_derived"],
                    session_id=C + marker + "_session",
                    source_date=C + marker + "_source_date",
                ),
            )
        ]
    async with provider._session_factory() as session:
        async with session.begin():
            session.add(model(**common, **fields))
            if receipt:
                session.add(
                    MemoryReceiptORM(
                        id=ident,
                        memory_type=kind,
                        source=C + marker + "_receipt_source",
                        created_by=C + marker + "_receipt_creator",
                        timestamp=at,
                        version=C + marker + "_receipt_version",
                        confidence=0.8,
                        verification_status="unverified",
                        lifecycle_state="active",
                        history=json.dumps(history),
                    )
                )
    return ident


@pytest.fixture
async def reads(shell, tmp_path):
    providers, sessions, closed, sql = {}, [], [], []

    class Session(AsyncSession):
        async def __aenter__(self):
            sessions.append(self)
            return await super().__aenter__()

        async def __aexit__(self, *args):
            try:
                return await super().__aexit__(*args)
            finally:
                closed.append(self)

    for profile in ("alpha", "beta"):
        root = tmp_path / ("real-profile-" + profile)
        root.mkdir()
        provider = SQLiteProvider("sqlite+aiosqlite:///" + str(root / "memory.db"))
        await provider.initialize()
        providers[profile] = provider
        for kind in KINDS:
            await seed(provider, kind, marker=profile + "_" + kind)

        def trace(conn, cursor, statement, params, context, many):
            sql.append((statement, params))

        event.listen(provider.engine.sync_engine, "before_cursor_execute", trace)

    class Leases:
        @asynccontextmanager
        async def acquire(self, ctx):
            binding = await shell.registry.resolve(ctx.principal, ctx.profile_id)
            yield SimpleNamespace(binding=binding)

    def bind(scope):
        provider = providers[scope.binding.profile.id]
        return ReadResources(provider, lambda: Session(provider.engine, expire_on_commit=False))

    codec = CursorCodec.from_path(tmp_path / "control" / "http-memory-cursor.key")
    port = MemoryReadService(Leases(), bind, classifier=MemoryAdmissionGate(), cursor_codec=codec)
    specs = create_memory_read_routes(port, shell.app.guard, cursor_codec=codec)
    deps = replace(shell.app.dependencies, ports={**shell.app.dependencies.ports, **{s.operation: port for s in specs}})
    app = create_http_app(shell.config, deps, read_routes=specs)
    reader, _ = shell.mint(permissions=("memory.read",))
    revealer, _ = shell.mint(permissions=("memory.read", "memory.reveal", "audit.read", "relation.read"))
    state = SimpleNamespace(**vars(shell))
    state.app, state.port, state.codec = app, port, codec
    state.providers, state.sessions, state.closed, state.sql = providers, sessions, closed, sql
    state.reader, state.revealer, state.specs = reader, revealer, specs

    async def call(path="memories", *, profile="alpha", bearer=None, query=None, **kwargs):
        return await request(
            app,
            "/api/v1/profiles/" + profile + "/" + path,
            bearer=bearer or reader,
            query=urlencode(query or {}, doseq=True).encode(),
            **kwargs,
        )

    state.call = call
    try:
        yield state
    finally:
        await app.close()
        for provider in providers.values():
            await provider.close()
        assert sessions == closed
        if os.environ.get("S07_EVIDENCE_DIR"):
            target = Path(os.environ["S07_EVIDENCE_DIR"]) / "sql-traces.jsonl"
            with target.open("a") as out:
                out.write(
                    json.dumps({"test": os.environ.get("PYTEST_CURRENT_TEST"), "statements": sql}, default=str) + "\n"
                )


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("reveal_enabled", [False, True])
async def test_t1_t14_all_gets_whole_null_and_live_reveal(reads, kind, reveal_enabled, caplog):
    r = reads
    ident = IDS[kind]
    bearer = r.revealer if reveal_enabled else r.reader
    gets = [
        ("memories", {}),
        (f"memories/{ident}", {}),
        (f"memories/{ident}/receipt", {}),
        (f"memories/{ident}/history", {}),
        ("lookup", dict(kind="memory", id=ident)),
        ("lookup", dict(kind="receipt", id=ident)),
    ]
    for path, query in gets:
        # Reset budgets only in this exhaustive projection matrix, not rate tests.
        r.app.guard.pre_auth.buckets.clear()
        r.app.guard.per_token.buckets.clear()
        response = await r.call(path, bearer=bearer, query=query)
        assert response.status == 200, response.body
        canary_evidence("GET", response.body)
        obj = response.json
        if path == "memories":
            assert len(obj["items"]) == 4
            for item in obj["items"]:
                assert set(item) == set(d.Memory.model_fields)
                assert item["payload"] is None and item["content"] is None and item["masked"]
                assert item["source_version"] == (1 if item["memory_type"] == "belief" else "1")
                assert item["revision"] == (1 if item["memory_type"] in ("fact", "belief") else None)
                d.Memory.model_validate_json(json.dumps(item))
        if path == f"memories/{ident}":
            assert set(obj) == set(d.Memory.model_fields) and obj["payload"] is None and obj["masked"]
        if path.endswith("/receipt") or query.get("kind") == "receipt":
            receipt = obj.get("target", obj)
            assert set(receipt) == set(d.Receipt.model_fields)
            assert all(receipt[k] is None for k in ("source", "created_by", "source_version"))
            assert receipt["history_complete"] is False and receipt["history"]
            for event_ in receipt["history"]:
                assert event_["evidence"] is None and event_["reason"] is None and event_["actor"] is None
            d.Receipt.model_validate_json(json.dumps(receipt))
    response = await r.call(
        f"memories/{ident}/reveal",
        method="POST",
        bearer=r.revealer,
        body=dict(reason="inspection", memory_type=kind, expected_revision=1 if kind in ("fact", "belief") else None),
    )
    assert response.status == 200, response.body
    assert not response.json["masked"] and response.json["payload"] is not None
    canary_evidence("reveal-live", response.body, live=True)
    fields = {"fact": ["object"], "belief": ["proposition"], "decision": ["choice", "reason"], "skill": ["steps"]}[kind]
    for field in fields:
        assert C + "alpha_" + kind + "_" + field in json.dumps(response.json["payload"][field])
    # Receipt/evidence-only canaries are negative controls, not promised in Memory reveal.
    async with r.providers["alpha"]._session_factory() as session:
        stored = (
            await session.execute(
                text("SELECT source,created_by,version,history FROM receipts WHERE id=:id"), {"id": ident}
            )
        ).first()
        for suffix in (
            "receipt_source",
            "receipt_creator",
            "receipt_version",
            "history",
            "method",
            "sources",
            "derived",
            "session",
            "source_date",
        ):
            assert C + "alpha_" + kind + "_" + suffix in json.dumps(list(stored))
    assert C not in repr(rows(r)) + caplog.text


@pytest.mark.parametrize("field", ["source", "tags", "risk_tags", "authority_level", "epistemic_status"])
@pytest.mark.parametrize("privileged", [False, True])
async def test_t2_filter_oracle_denied_before_backend(reads, field, privileged):
    r = reads
    # Same metadata/created key; only masked contents differ.
    twin = str(uuid4())
    await seed(r.providers["alpha"], ident=twin, marker="twin")
    before = (len(r.sessions), len(rows(r)))
    for value in (C + "alpha_fact_object", C + "twin_object", "absent"):
        response = await r.call(query={field: value}, bearer=r.revealer if privileged else r.reader)
        code(response, "E_PERMISSION_DENIED", 403)
    assert (len(r.sessions), len(rows(r))) == before
    ok = await r.call(
        query={"memory_types": "fact", "min_confidence": "0.7", "max_confidence": "0.9", "lifecycle_states": "active"}
    )
    assert ok.status == 200, ok.body
    assert {v["id"] for v in ok.json["items"]} == {IDS["fact"], twin}
    if field == "source":
        async with r.providers["alpha"]._session_factory() as s:
            async with s.begin():
                await s.execute(text("UPDATE facts SET source='' WHERE id=:id"), {"id": twin})
        empty = await r.call(query={"source": "", "memory_types": "fact"})
        assert empty.status == 200 and {v["id"] for v in empty.json["items"]} == {IDS["fact"], twin}


@pytest.mark.parametrize(
    "query",
    [
        {"limit": "0"},
        {"limit": "201"},
        {"limit": '"50"'},
        {"limit": "1.2"},
        {"unknown": "x"},
        {"limit": ["1", "2"]},
        {"created_from": "invalid"},
        {"min_confidence": "nan"},
        {"min_confidence": "0.9", "max_confidence": "0.1"},
    ],
)
async def test_t5_validation_zero_sessions_audit(reads, query):
    before = (len(reads.sessions), len(rows(reads)))
    code(await reads.call(query=query), "E_VALIDATION", 422)
    assert (len(reads.sessions), len(rows(reads))) == before


async def test_t5_typed_limit_rejects_string():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        d.PageQuery(limit="50")


async def test_t5_bounded_collection_history_and_receipt(reads):
    r = reads
    provider = r.providers["alpha"]
    async with provider._session_factory() as session:
        async with session.begin():
            await session.execute(
                insert(FactORM),
                [
                    dict(
                        id=f"bulk-{i:05}",
                        subject="project",
                        predicate="uses",
                        object="sqlite",
                        dedup_key=f"bulk-{i:05}",
                        version="1",
                        created_at=STAMP,
                        updated_at=STAMP,
                    )
                    for i in range(5000)
                ],
            )
            chain = [
                {"timestamp": (STAMP + timedelta(seconds=i)).isoformat(), "reason": C + "chain"} for i in range(1000)
            ]
            await session.execute(
                text("UPDATE receipts SET history=:h WHERE id=:id"), {"h": json.dumps(chain), "id": IDS["fact"]}
            )
    r.sql.clear()
    page = await r.call(query={"limit": 200, "memory_types": "fact"})
    assert page.status == 200, page.body
    assert len(page.json["items"]) == 200 and page.json["has_more"] and page.json["next_cursor"]
    assert len(r.sql) <= 8, r.sql
    history = await r.call(f"memories/{IDS['fact']}/history", query={"limit": 200})
    receipt = await r.call(f"memories/{IDS['fact']}/receipt")
    assert history.status == receipt.status == 200, (history.body, receipt.body)
    assert len(history.json["items"]) == 200 and history.json["has_more"]
    assert len(receipt.json["history"]) == 200 and receipt.json["history_complete"] is False
    assert [x["id"] for x in history.json["items"]] == [IDS["fact"] + ":" + str(i) for i in range(200)]
    selects = [(q, p) for q, p in r.sql if q.lstrip().upper().startswith(("SELECT", "WITH"))]
    assert selects and all("LIMIT" in q.upper() for q, p in selects)
    assert any("UNION ALL" in q and 201 in tuple(p) for q, p in selects)
    assert not any("SELECT history FROM" in q for q, p in selects)
    assert C.encode() not in history.body + receipt.body


async def test_t5_batch_receipts_match_single_reads(reads):
    r = reads
    provider = r.providers["alpha"]
    for i in range(51):
        for kind in KINDS:
            await seed(
                provider,
                kind,
                ident=f"batch-{i:03}",
                marker=f"{kind}-{i}",
                receipt=i % 2 == 0 and kind == KINDS[(i // 2) % 4],
            )
    async with provider._session_factory() as session:
        r.sql.clear()
        page = await provider.read_memory_page(
            session,
            d.MemoryListQuery(
                limit=200,
                cursor=None,
                min_confidence=None,
                max_confidence=None,
                source=None,
                authority_level=None,
                epistemic_status=None,
                created_from=None,
                created_to=None,
                updated_from=None,
                updated_to=None,
                valid_at=None,
            ),
        )
        count = len(r.sql)
        assert len(page.records) == 200 and page.has_more
        # Reconstruct the old per-row receipt result, including absent receipts
        # and same-id/different-type collisions. Ordering/LIMIT must not change.
        old = [await provider.read_receipt_metadata(session, v.memory["id"], v.memory_type) for v in page.records]
        assert [v.receipt for v in page.records] == old
        assert any(v is None for v in old) and any(v is not None for v in old)
        expected = sorted(
            [(STAMP.isoformat(timespec="microseconds"), kind, f"batch-{i:03}") for i in range(51) for kind in KINDS]
            + [(STAMP.isoformat(timespec="microseconds"), kind, IDS[kind]) for kind in KINDS]
        )[:200]
        assert [(v.memory["created_at"], v.memory_type, v.memory["id"]) for v in page.records] == expected
        assert count <= 8, count


async def test_t12_max_and_page_share_real_sqlite_snapshot(reads, monkeypatch):
    r = reads
    provider = r.providers["alpha"]
    async with provider.engine.connect() as connection:
        assert (await connection.execute(text("PRAGMA journal_mode=WAL"))).scalar() == "wal"
    execute = AsyncSession.execute
    writes = []
    begins = []

    def observe(conn, cursor, statement, params, context, many):
        if statement == "BEGIN":
            begins.append(conn.connection.driver_connection.in_transaction)

    event.listen(provider.engine.sync_engine, "before_cursor_execute", observe)

    async def concurrent_write(session, statement, *args, **kwargs):
        result = await execute(session, statement, *args, **kwargs)
        if "AS memories" in str(statement) and "ORDER BY created_at DESC" in str(statement):
            await seed(provider, ident="backdated-concurrent", at=STAMP - timedelta(days=1))
            async with provider._session_factory() as writer:
                async with writer.begin():
                    await execute(writer, text("DELETE FROM facts WHERE id=:id"), {"id": IDS["fact"]})
            writes.append(True)
        return result

    monkeypatch.setattr(AsyncSession, "execute", concurrent_write)
    try:
        result = await r.call(query={"memory_types": "fact"})
        assert result.status == 200, result.body
        assert writes == [True]
        assert [v["id"] for v in result.json["items"]] == [IDS["fact"]]
        # session.begin() starts SQLAlchemy bookkeeping, not a sqlite read txn.
        # Exactly one actual BEGIN is sent, while the driver is not in a txn.
        assert begins == [False]
    finally:
        event.remove(provider.engine.sync_engine, "before_cursor_execute", observe)
    async with provider._session_factory() as session:
        assert (await execute(session, text("SELECT id FROM facts"))).scalars().all() == ["backdated-concurrent"]


@pytest.mark.parametrize("case", ["missing", "null", "unsupported", "malformed", "truncated", "ties"])
async def test_t6_history_honest_exact_prefix(reads, case):
    r = reads
    ident = IDS["fact"]
    provider = r.providers["alpha"]
    history = [] if case == "missing" else [{"timestamp": STAMP.isoformat(), "reason": C + "event"}]
    if case == "null":
        history = [{"reason": C + "missing_timestamp"}]
    if case == "unsupported":
        history += [{"kind": "future-source", "timestamp": STAMP.isoformat()}]
    if case == "truncated":
        history *= 205
    if case == "ties":
        history = [{"timestamp": "2026-01-01T03:00:00+03:00"}, {"timestamp": None}, {"timestamp": STAMP.isoformat()}]
    async with provider._session_factory() as session:
        async with session.begin():
            await session.execute(
                text("UPDATE receipts SET history=:h WHERE id=:id"),
                {"h": "{invalid" if case == "malformed" else json.dumps(history), "id": ident},
            )
            if case == "ties":
                for ident_ in ("z", "a"):
                    session.add(
                        LifecycleEventORM(
                            id=ident_,
                            memory_id=ident,
                            memory_type="fact",
                            from_state="candidate",
                            to_state="active",
                            reason=C + "lifecycle",
                            timestamp=STAMP,
                        )
                    )
    receipt = await r.call(f"memories/{ident}/receipt")
    assert receipt.status == 200, receipt.body
    assert receipt.json["history_complete"] is False
    available = receipt.json["history"]
    if case in ("missing", "malformed"):
        assert available == []
    if case in ("null", "unsupported"):
        assert [v["id"] for v in available] == [ident + ":0"]
    if case == "null":
        assert available[0]["timestamp"] is None
    if case == "truncated":
        assert [v["id"] for v in available] == [ident + ":" + str(i) for i in range(200)]
    if case == "ties":
        assert [v["id"] for v in available] == [ident + ":1", ident + ":0", ident + ":2", "a", "z"]
    if case == "unsupported":
        async with provider._session_factory() as session:
            page = await provider.read_history_page(session, ident, "fact", limit=1, after=None, upper=None)
            assert page.incomplete


@pytest.mark.parametrize("incomplete", [True, False])
@pytest.mark.parametrize("lookup", [True, False])
async def test_t6_receipt_consults_history_slice(reads, monkeypatch, incomplete, lookup):
    provider = reads.providers["alpha"]
    original = provider.read_history_page
    consulted = []

    class Incomplete:
        def __bool__(self):
            consulted.append(incomplete)
            return incomplete

    async def history(*args, **kwargs):
        page = await original(*args, **kwargs)
        assert isinstance(page, HistorySlice) and page.events and not page.has_more
        return replace(page, incomplete=Incomplete())

    monkeypatch.setattr(provider, "read_history_page", history)
    result = await reads.call(
        "lookup" if lookup else f"memories/{IDS['fact']}/receipt",
        query=dict(kind="receipt", id=IDS["fact"]) if lookup else {},
    )
    assert result.status == 200, result.body
    receipt = result.json["target"] if lookup else result.json
    assert receipt["history"] and receipt["history_complete"] is False
    assert consulted == [incomplete]


@pytest.mark.parametrize("kind,permission", [("request", "audit.read"), ("relation", "relation.read")])
async def test_t7_lookup_extra_permissions_and_no_fake_port(reads, kind, permission):
    r = reads
    before = (len(r.sessions), len(rows(r)))
    for ident in (IDS["fact"], "foreign", "absent"):
        code(await r.call("lookup", query=dict(kind=kind, id=ident)), "E_PERMISSION_DENIED", 403)
    assert (len(r.sessions), len(rows(r))) == before
    for ident in (IDS["fact"], "foreign", "absent"):
        code(
            await r.call("lookup", query=dict(kind=kind, id=ident), bearer=r.revealer), "E_CAPABILITY_UNAVAILABLE", 503
        )
    assert len(r.sessions) == before[0]


@pytest.mark.parametrize("safe_provenance", [False, True])
@pytest.mark.parametrize("caller", ["reader", "revealer"])
async def test_t7_trace_scoped_masked_live_and_absent(reads, safe_provenance, caller):
    r = reads
    item = d.TraceContribution(source="fts", matched_terms=[C + "matched"], relation_path=[C + "relation_path"])
    known_stages = ("fts", "semantic", "graph", "merge", "context")
    stages = {
        stage: [item.model_copy(update=dict(memory_id=stage, rank_before_merge=rank))]
        for rank, stage in enumerate(known_stages, start=1)
    }
    statuses = dict(zip(known_stages, ("ran", "skipped", "failed", "ran", "skipped"), strict=True))
    trace = d.Trace(
        id="trace-one",
        request_id="req_trace",
        profile_id="alpha",
        operation="search",
        query=C + "query",
        stages={**stages, C + "unknown_stage": [item]},
        stage_status={**statuses, C + "unknown_status": "ran"},
        merge=[item],
        deduplicated=[item],
        final=[item],
        created_at=STAMP,
        expires_at=STAMP + timedelta(hours=1),
        settings_digest="a" * 64 if safe_provenance else C + "digest",
        code_revision="b" * 40 if safe_provenance else C + "revision",
    )

    class Traces:
        async def execute(self, ctx, command):
            if command.path_ids["id"] != "trace-one":
                raise KeyError("absent")
            return trace

    canary_evidence("trace-fixture-live", trace.model_dump_json(), live=True)
    original = trace.model_dump()
    r.port.traces = Traces()
    result = await r.call("lookup", query=dict(kind="trace", id="trace-one"), bearer=getattr(r, caller))
    assert result.status == 200, result.body
    assert C.encode() not in result.body and result.json["target"]["id"] == "trace-one"
    target = result.json["target"]
    assert target["settings_digest"] == ("a" * 64 if safe_provenance else "")
    assert target["code_revision"] == ("b" * 40 if safe_provenance else "")
    assert target["stages"] == {
        stage: [v.model_copy(update=dict(matched_terms=[], relation_path=[])).model_dump(mode="json") for v in items]
        for stage, items in stages.items()
    }
    assert target["stage_status"] == statuses
    for field in ("merge", "deduplicated", "final"):
        assert target[field] == [
            item.model_copy(update=dict(matched_terms=[], relation_path=[])).model_dump(mode="json")
        ]
    assert trace.model_dump() == original
    trace.profile_id = "beta"
    code(await r.call("lookup", query=dict(kind="trace", id="trace-one")), "E_NOT_FOUND", 404)
    code(await r.call("lookup", query=dict(kind="trace", id="absent")), "E_NOT_FOUND", 404)
    r.port.traces = None
    code(await r.call("lookup", query=dict(kind="trace", id="trace-one")), "E_CAPABILITY_UNAVAILABLE", 503)


@pytest.mark.parametrize("kind", KINDS)
async def test_t8_reveal_permission_revision_audit_privacy(reads, kind, caplog):
    r = reads
    path = f"memories/{IDS[kind]}/reveal"
    body = dict(reason=C + "REASON", memory_type=kind, expected_revision=1 if kind in ("fact", "belief") else None)
    before = (len(rows(r)), len(r.sessions))
    code(await r.call(path, method="POST", body=body), "E_PERMISSION_DENIED", 403)
    assert (len(rows(r)), len(r.sessions)) == before
    for bad in (
        {k: v for k, v in body.items() if k != "expected_revision"},
        {**body, "reason": ""},
        {**body, "reason": "x" * 2049},
    ):
        code(await r.call(path, method="POST", body=bad, bearer=r.revealer), "E_VALIDATION", 422)
    assert (len(rows(r)), len(r.sessions)) == before
    if kind in ("fact", "belief"):
        code(
            await r.call(path, method="POST", body={**body, "expected_revision": None}, bearer=r.revealer),
            "E_PRECONDITION_REQUIRED",
            428,
        )
        code(
            await r.call(path, method="POST", body={**body, "expected_revision": 2}, bearer=r.revealer),
            "E_VERSION_CONFLICT",
            409,
        )
    else:
        code(
            await r.call(path, method="POST", body={**body, "expected_revision": 1}, bearer=r.revealer),
            "E_VALIDATION",
            422,
        )
    count = len(rows(r))
    result = await r.call(path, method="POST", body=body, bearer=r.revealer)
    assert result.status == 200 and not result.json["masked"], result.body
    audit = rows(r)
    assert len(audit) == count + 1 and audit[-1]["state"] == "terminal"
    canary_evidence("audit+logs", repr(audit) + repr([vars(record) for record in caplog.records]))
    canary_evidence("metric-labels", generate_latest(r.metrics._registry))


@pytest.mark.parametrize("unsafe_version", [C + "unsafe_version", (C + "unsafe_blob").encode()])
async def test_t8_legacy_version_and_t14_unsafe_list_version(reads, unsafe_version):
    r = reads
    async with r.providers["alpha"]._session_factory() as s:
        async with s.begin():
            await s.execute(text("UPDATE facts SET version=:v WHERE id=:id"), {"v": "0.1.0", "id": IDS["fact"]})
    get = await r.call("memories/" + IDS["fact"])
    assert get.status == 200 and get.json["revision"] is None
    code(
        await r.call(
            "memories/" + IDS["fact"] + "/reveal",
            method="POST",
            bearer=r.revealer,
            body=dict(reason="inspection", memory_type="fact", expected_revision=1),
        ),
        "E_CAPABILITY_UNAVAILABLE",
        503,
    )
    async with r.providers["alpha"]._session_factory() as s:
        async with s.begin():
            await s.execute(text("UPDATE facts SET version=:v WHERE id=:id"), {"v": unsafe_version, "id": IDS["fact"]})
    page = await r.call(query={"memory_types": "fact"})
    assert page.status == 200 and len(page.json["items"]) == 1
    assert page.json["items"][0]["source_version"] is None and C.encode() not in page.body


@pytest.mark.parametrize("mode", ["untagged", "benign", "stored-risk", "exception", "oversize", "unsupported"])
async def test_t15_real_heuristic_and_unknown_fail_closed(reads, mode):
    r = reads
    provider = r.providers["alpha"]
    ident = IDS["fact"]
    if mode in ("untagged", "oversize"):
        content = "password is sensitive" if mode != "oversize" else "x" * 33000
        async with provider._session_factory() as s:
            async with s.begin():
                await s.execute(text("UPDATE facts SET object=:v WHERE id=:id"), {"v": content, "id": ident})
    if mode == "benign":
        ident = IDS["belief"]
        async with provider._session_factory() as s:
            async with s.begin():
                await s.execute(
                    text("UPDATE beliefs SET proposition=:v,tags=:tags WHERE id=:id"),
                    {"v": "password is sensitive", "tags": '["benign"]', "id": ident},
                )
    if mode == "exception":

        class Broken:
            def classify(self, *a, **kw):
                raise RuntimeError(C + "classifier")

        r.port.classifier = Broken()
    if mode == "stored-risk":
        history = [dict(timestamp=STAMP.isoformat(), metadata=dict(admission=dict(risk_tags=["security_sensitive"])))]
        async with provider._session_factory() as s:
            async with s.begin():
                await s.execute(
                    text("UPDATE receipts SET history=:h WHERE id=:id"), {"h": json.dumps(history), "id": ident}
                )
    if mode == "unsupported":
        ident = IDS["skill"]
        async with provider._session_factory() as s:
            async with s.begin():
                await s.execute(text("UPDATE skills SET steps='{}' WHERE id=:id"), {"id": ident})
    result = await r.call(
        "memories/" + ident + "/reveal",
        method="POST",
        bearer=r.revealer,
        body=dict(
            reason="inspection",
            memory_type="skill" if mode == "unsupported" else "belief" if mode == "benign" else "fact",
            expected_revision=None if mode == "unsupported" else 1,
        ),
    )
    assert result.status == 200 and result.json["masked"] and result.json["payload"] is None, result.body
    if mode == "benign":
        raw = ReadRecord({"object": "password is sensitive", "risk_tags": []}, "fact")
        assert classify_read_content(raw, MemoryAdmissionGate()) == "sensitive"


@pytest.mark.parametrize("field", ["source", "tags", "risk_tags", "authority_level", "epistemic_status"])
async def test_t11_telemetry_filter_content_and_reason_sweep(reads, caplog, field):
    r = reads
    code(await r.call(query={field: C + "FILTER_" + field}), "E_PERMISSION_DENIED", 403)
    result = await r.call(
        "memories/" + IDS["fact"] + "/reveal",
        method="POST",
        bearer=r.revealer,
        body=dict(reason=C + "REASON", memory_type="fact", expected_revision=1),
    )
    assert result.status == 200 and C.encode() in result.body
    canary_evidence("filter-audit-logs", repr(rows(r)) + repr([vars(v) for v in caplog.records]))
    canary_evidence("filter-metrics", generate_latest(r.metrics._registry))


async def test_t12_fresh_sessions_errors_and_cancel(reads, monkeypatch, caplog):
    r = reads
    path = "memories/" + IDS["fact"]
    for _ in range(2):
        assert (await r.call(path)).status == 200
    assert len(r.sessions) == 2 and r.sessions[0] is not r.sessions[1] and r.sessions == r.closed

    async def broken(*a, **k):
        raise RuntimeError(C + "backend")

    monkeypatch.setattr(r.providers["alpha"], "read_memory", broken)
    code(await r.call(path), "E_INTERNAL", 500)
    assert r.sessions == r.closed and C not in caplog.text + repr(rows(r))
    entered = asyncio.Event()

    async def blocking(*a, **kw):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(r.providers["alpha"], "read_memory", blocking)
    task = asyncio.create_task(r.call(path))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert r.sessions == r.closed
    assert rows(r)[-1]["state"] == "terminal" and rows(r)[-1]["result"] == "E_TIMEOUT"


async def test_t13_reveal_budget_before_storage(reads):
    r = reads
    r.app.guard.per_token.config = replace(r.config, rate_burst=2, rate_per_minute=1)
    misses = []
    for _ in range(2):
        result = await r.call(
            "memories/" + str(uuid4()) + "/reveal",
            method="POST",
            bearer=r.revealer,
            body=dict(reason="inspection", memory_type="fact", expected_revision=1),
        )
        code(result, "E_NOT_FOUND", 404)
        misses.append(result)
    assert misses[0].status == misses[1].status
    assert set(misses[0].json) == set(misses[1].json)
    assert set(misses[0].json["error"]) == set(misses[1].json["error"])
    assert len(misses[0].body) == len(misses[1].body)
    # Authorized misses must read storage and receive terminal audit records.
    assert len(r.sessions) == len(rows(r)) == 2
    before = (len(r.sessions), len(rows(r)))
    limited = []
    for ident in (str(uuid4()), IDS["fact"], str(uuid4())):
        result = await r.call(
            "memories/" + ident + "/reveal",
            method="POST",
            bearer=r.revealer,
            body=dict(reason=C + "RANDOM_REASON", memory_type="fact", expected_revision=1),
        )
        code(result, "E_RATE_LIMIT", 429)
        assert ident.encode() not in result.body and C.encode() not in result.body
        limited.append(result)
    assert all(set(v.json) == set(limited[0].json) for v in limited)
    assert all(set(v.json["error"]) == set(limited[0].json["error"]) for v in limited)
    assert all(len(v.body) == len(limited[0].body) for v in limited)
    errors = [{k: value for k, value in v.json["error"].items() if k != "request_id"} for v in limited]
    assert errors[0] == errors[1] == errors[2]
    assert before == (len(r.sessions), len(rows(r)))


async def test_t12_real_sqlite_failure_safe_storage_envelope(reads, caplog):
    r = reads
    async with r.providers["alpha"].engine.begin() as conn:
        await conn.execute(text("DROP TABLE facts"))
    code(await r.call("memories/" + IDS["fact"]), "E_STORAGE", 503)
    assert r.sessions == r.closed and len(r.sessions) == 1
    assert rows(r)[-1]["state"] == "terminal" and rows(r)[-1]["result"] == "E_STORAGE"
    canary_evidence("storage-error-telemetry", repr(rows(r)) + caplog.text)


async def test_t9_t10_unknown_disabled_profile_no_sessions(reads):
    r = reads
    unknown, _ = r.mint(profiles=("missing",), permissions=("memory.read",))
    code(await r.call(profile="missing", bearer=unknown), "E_PROFILE_NOT_FOUND", 404)
    disabled = await request(
        r.app,
        "/api/v1/profiles/alpha/disable",
        method="POST",
        bearer=r.bearer,
        body=dict(reason="disabled", confirmation=True, expected_generation=1),
        headers=[("Idempotency-Key", str(uuid4()))],
    )
    assert disabled.status == 200, disabled.body
    code(await r.call(), "E_PROFILE_DISABLED", 409)
    assert r.sessions == []


@pytest.mark.parametrize("field", ["created_from", "created_to", "updated_from", "updated_to", "valid_at"])
async def test_metadata_time_filters_before_limit(reads, field):
    r = reads
    if field == "valid_at":
        entry = dict(
            timestamp=STAMP.isoformat(),
            metadata=dict(
                admission=dict(valid_from=STAMP.isoformat(), valid_to=(STAMP + timedelta(days=1)).isoformat())
            ),
        )
        async with r.providers["alpha"]._session_factory() as session:
            async with session.begin():
                await session.execute(
                    text("UPDATE receipts SET history=:h WHERE id=:id"), {"h": json.dumps([entry]), "id": IDS["fact"]}
                )
    clock = STAMP + timedelta(days=2) if field.endswith("_from") or field == "valid_at" else STAMP - timedelta(days=2)
    response = await r.call(query={"memory_types": "fact", field: clock.isoformat(), "limit": 1})
    assert response.status == 200 and response.json["items"] == [], response.body
    response = await r.call(query={"memory_types": "fact", field: STAMP.isoformat(), "limit": 1})
    assert response.status == 200 and len(response.json["items"]) == 1, response.body


@pytest.mark.parametrize(
    "suffix", ["", "/" + IDS["fact"], "/" + IDS["fact"] + "/receipt", "/" + IDS["fact"] + "/history"]
)
async def test_read_auth_revalidation_profile_scope(reads, suffix):
    r = reads
    a_only, _ = r.mint(profiles=("alpha",), permissions=("memory.read",))
    before = (len(r.sessions), len(rows(r)))
    code(await r.call("memories" + suffix, profile="beta", bearer=a_only), "E_PERMISSION_DENIED", 403)
    expired, _ = r.mint(permissions=("memory.read",), expired=True)
    code(await r.call("memories" + suffix, bearer=expired), "E_AUTH_REQUIRED", 401)
    assert before == (len(r.sessions), len(rows(r)))
