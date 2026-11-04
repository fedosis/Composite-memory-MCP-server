"""S17 preview discriminators crossing actual daemon, lease and canonical SQLite."""

import asyncio
from dataclasses import replace
from datetime import timedelta
from time import monotonic
from uuid import UUID, uuid4

import pytest

from memory_server.gateway_contracts import dto as d
from tests.gateway.test_admission_dry_run import (
    T0,
    A,
    B,
    F,
    context,
    domain_snapshot,
    error,
    mint,
    post,
    rows,
)
from tests.gateway.test_admission_dry_run import (
    inspection_http as inspection_http,
)
from tests.gateway.test_http_shell import request
from tests.gateway.test_slice_runtime import offline_embedder as offline_embedder


def ident(i):
    return str(UUID(int=0x10000000000040008000000000000000 + i))


def payload(operation="transition", **updates):
    return (
        dict(
            operation=operation,
            memory_type="fact",
            to_state="stale" if operation == "transition" else None,
            relation_id=None,
            conflict_id=None,
        )
        | updates
    )


def path(ident=F, profile="alpha"):
    return f"/api/v1/profiles/{profile}/memories/{ident}/preview"


async def preview(state, body=None, ident=F, profile="alpha", **kwargs):
    return await post(state, payload() if body is None else body, path=path(ident, profile), **kwargs)


async def seed(state):
    from storage.models.belief import BeliefORM
    from storage.models.fact import FactORM

    for profile, versions in (("alpha", ("7", 3, 5)), ("beta", ("19", 11, 13))):
        async with state.deps.leases.acquire(context(state, profile)) as scope:
            async with scope.application.read_resources().session_factory() as session, session.begin():
                session.add(
                    FactORM(
                        id=F,
                        subject="S17_CONTENT_CANARY",
                        predicate="is",
                        object="fixture",
                        dedup_key=F,
                        version=versions[0],
                        confidence=0.8,
                        lifecycle_state="active",
                        created_at=T0,
                        updated_at=T0,
                    )
                )
                for item, version in zip((A, B), versions[1:]):
                    session.add(
                        BeliefORM(
                            id=item,
                            proposition="S17_CONTENT_CANARY " + item,
                            version=version,
                            confidence=0.8,
                            lifecycle_state="active",
                            created_at=T0,
                            updated_at=T0,
                        )
                    )
        assert await rows(state, "SELECT version FROM facts WHERE id=:id", {"id": F}, profile) == [(versions[0],)]


async def change(state, sql, params=None, profile="alpha"):
    from sqlalchemy import text

    async with state.deps.leases.acquire(context(state, profile)) as scope:
        async with scope.application.read_resources().session_factory() as session, session.begin():
            await session.execute(text(sql), params or {})


async def relation_write(state, *, deleted=False, revision=1):
    from memory_server.application.graph_read_service import relation_id

    endpoint = "/api/v1/profiles/alpha/relations"
    body = dict(expected_versions={A: 3, F: 7}, reason="S17_REASON_CANARY")
    if deleted:
        endpoint += "/" + relation_id(A, F, "supports")
        body.update(expected_version=revision, confirmation=True)
    else:
        body.update(source_id=A, target_id=F, relation_type="supports")
    return await request(
        state.app,
        endpoint,
        method="DELETE" if deleted else "POST",
        body=body,
        bearer=state.bearer,
        headers=(("Idempotency-Key", str(uuid4())),),
    )


async def test_s17_preview_public_mount_required(inspection_http):
    await seed(inspection_http)
    response = await preview(inspection_http)
    assert response.status == 200, f"S17_RED_MOUNT_REQUIRED expected200 actual{response.status} {response.body!r}"
    assert response.json["preview_only"] is True, "S17_RED_MOUNT_REQUIRED"


async def test_s17_preview_field_matrix(inspection_http):
    from memory_server.application.graph_read_service import relation_id
    from memory_server.application.remote_beliefs import encode_conflict_id as conflict_id

    state = inspection_http
    await seed(state)
    for operation in ("transition", "invalidate", "relation_delete", "resolve"):
        body = payload(operation)
        if operation == "relation_delete":
            body["relation_id"] = relation_id(A, F, "supports")
        if operation == "resolve":
            body.update(memory_type="belief", conflict_id=conflict_id(A, B))
        for key in body:
            bad = dict(body)
            del bad[key]
            error(
                await preview(state, bad, ident=A if operation == "resolve" else F),
                422,
                "E_VALIDATION",
                "S17_FIELD_MATRIX",
            )
        for key in body:
            bad = body | {key: 123}
            error(await preview(state, bad), 422, "E_VALIDATION", "S17_FIELD_MATRIX")
        error(await preview(state, body | {"extra": True}), 422, "E_VALIDATION", "S17_FIELD_MATRIX")
    for ident in (F.upper().replace("000001", "00000A"), "{" + F + "}", F.replace("-", ""), " " + F):
        error(await preview(state, ident=ident), 422, "E_VALIDATION", "S17_FIELD_MATRIX")
    for kind in ("decision", "skill", None):
        error(await preview(state, payload(memory_type=kind)), 422, "E_VALIDATION", "S17_FIELD_MATRIX")
    for target in ("trusted", "deprecated", "active", "unknown"):
        error(await preview(state, payload(to_state=target)), 422, "E_VALIDATION", "S17_FIELD_MATRIX")


async def test_s17_lifecycle_actions_match_actual(inspection_http):
    from memory_server.evaluation.validator import is_valid_transition
    from memory_server.models.receipt import LifecycleState

    state = inspection_http
    await seed(state)
    for kind, item in (("fact", F), ("belief", A)):
        for current in LifecycleState:
            await change(
                state,
                f"UPDATE {'facts' if kind == 'fact' else 'beliefs'} SET lifecycle_state=:s WHERE id=:id",
                {"s": current.value, "id": item},
            )
            expected = [target.value for target in LifecycleState if is_valid_transition(current, target)]
            for target in LifecycleState:
                response = await preview(state, payload(memory_type=kind, to_state=target.value), ident=item)
                if target.value not in expected:
                    error(response, 422, "E_VALIDATION", "S17_LIFECYCLE_PARITY")
                else:
                    assert response.status == 200, ("S17_LIFECYCLE_PARITY", response.body)
                    assert response.json["allowed_actions"] == expected, "S17_LIFECYCLE_PARITY"
    await change(state, "UPDATE facts SET lifecycle_state='active' WHERE id=:id", {"id": F})
    # The current schema forbids a persisted NULL. Exercise its legacy
    # normalization at the real application helper without weakening that schema.
    from types import SimpleNamespace

    from storage.repositories.lifecycle_repo import normalize_version

    from memory_server.application.inspection_service import _version

    assert _version(SimpleNamespace(kind="fact", version=None)) == normalize_version(None) == 1, "S17_LIFECYCLE_PARITY"
    assert await rows(state, "SELECT version FROM facts WHERE id=:id", {"id": F}) == [("7",)], "S17_LIFECYCLE_PARITY"
    for value, revision in (("0.1.0", 1), (" 007 ", 7), ("0", 1)):
        await change(state, "UPDATE facts SET version=:v WHERE id=:id", {"v": value, "id": F})
        response = await preview(state)
        assert response.status == 200 and response.json["observed_versions"][F] == revision, "S17_LIFECYCLE_PARITY"


async def test_s17_direct_impact_bounded(inspection_http, monkeypatch):
    from storage.models.belief import BeliefORM
    from storage.models.relation import ClaimRelationORM

    from memory_server.application.inspection_service import _PreviewSnapshot

    queries = []
    original_query = _PreviewSnapshot._query

    async def traced_query(self, sql, params):
        result = await original_query(self, sql, params)
        queries.append((sql, len(result)))
        return result

    monkeypatch.setattr(_PreviewSnapshot, "_query", traced_query)
    state = inspection_http
    await seed(state)
    for count in (0, 1, 99, 100, 101, 512, 513):
        await change(state, "DELETE FROM claim_relations")
        async with state.deps.leases.acquire(context(state)) as scope:
            async with scope.application.read_resources().session_factory() as session, session.begin():
                for i in range(1, count + 1):
                    if i <= 101:
                        await session.merge(
                            BeliefORM(
                                id=ident(i),
                                proposition="dependent",
                                version=i,
                                confidence=0.8,
                                lifecycle_state="active",
                            )
                        )
                    session.add(ClaimRelationORM(source_id=ident(i), target_id=F, relation_type="derived_from"))
        response = await preview(state, payload("invalidate"))
        assert response.status == 200, ("S17_BOUNDED", response.body)
        result = response.json
        if count > 512:
            assert result["allowed_actions"] == [], "S17_BOUNDED"
        else:
            expected = [F] + [ident(i) for i in range(1, min(count, 99) + 1)]
            assert result["affected_ids"] == expected, "S17_BOUNDED"
            assert result["affected_count"] == len(expected), "S17_BOUNDED"
            assert result["count_exact"] is (count < 100), "S17_BOUNDED"
            assert result["truncated"] is (count >= 100), "S17_BOUNDED"
            assert set(result["observed_versions"]) == set(expected), "S17_BOUNDED"
    windows = [(sql, fetched) for sql, fetched in queries if "SELECT DISTINCT source_id" in sql]
    assert windows, "S17_BOUNDED impact must cross borrowed SQLite"
    assert all("LIMIT 101" in sql and fetched <= 101 for sql, fetched in windows), "S17_BOUNDED raw window"
    assert all(fetched <= 513 for sql, fetched in queries if "LIMIT 513" in sql), "S17_BOUNDED direct refs"


async def test_s17_relation_delete_canonical_preview(inspection_http):
    from memory_server.application.graph_read_service import relation_id

    state = inspection_http
    await seed(state)
    handle = relation_id(A, F, "supports")
    body = payload("relation_delete", relation_id=handle)
    for deleted, revision in ((False, 1), (True, 2), (False, 3)):
        response = await relation_write(state, deleted=deleted)
        assert response.status == (200 if deleted else 201), ("S17_RELATION_CANONICAL", response.body)
        assert response.json["relation"]["revision"] == revision, "S17_STALE_PREVIEW"
        result = await preview(state, body)
        if deleted:
            error(result, 404, "E_NOT_FOUND", "S17_RELATION_CANONICAL")
        else:
            assert result.status == 200, ("S17_RELATION_CANONICAL", result.body)
            assert result.json["observed_versions"] == {A: 3, F: 7, handle: revision}, "S17_STALE_PREVIEW"
            assert result.json["allowed_actions"] == ["relation_delete"], "S17_RELATION_CANONICAL"
    stale = await relation_write(state, deleted=True, revision=1)
    assert stale.status == 409, "S17_STALE_PREVIEW"


async def test_s17_resolve_pair_actions_q10(inspection_http, monkeypatch):
    from memory_server.application.inspection_service import _PreviewSnapshot
    from memory_server.application.remote_beliefs import encode_conflict_id as conflict_id

    original = _PreviewSnapshot._query
    windows = []

    async def traced(self, sql, params):
        result = await original(self, sql, params)
        if "SELECT DISTINCT source_id" in sql:
            windows.append((sql, len(result)))
        return result

    monkeypatch.setattr(_PreviewSnapshot, "_query", traced)
    state = inspection_http
    await seed(state)
    body = payload("resolve", memory_type="belief", conflict_id=conflict_id(A, B))
    response = await preview(state, body, ident=A)
    assert response.status == 200, ("S17_RESOLVE_Q10", response.body)
    assert response.json["allowed_actions"] == [
        "keep_a",
        "keep_b",
        "merge",
        "mark_contradicted",
        "discard_both",
        "defer",
    ], "S17_RESOLVE_Q10"
    assert response.json["affected_ids"] == [A, B], "S17_RESOLVE_Q10"
    assert windows and all("LIMIT 101" in sql and fetched <= 101 for sql, fetched in windows), "S17_BOUNDED pair window"
    await change(
        state,
        "INSERT INTO claim_relations(source_id,target_id,relation_type,created_at) VALUES(:a,:b,'derived_from',:t)",
        {"a": A, "b": B, "t": T0},
    )
    response = await preview(state, body, ident=A)
    assert response.status == 200, "S17_RESOLVE_Q10"
    assert response.json["allowed_actions"] == ["keep_b", "defer"], "S17_RESOLVE_Q10"
    assert response.json["observed_versions"] == {A: 3, B: 5}, "S17_RESOLVE_Q10"


async def test_s17_preview_no_write_all_operations(inspection_http, monkeypatch):
    import copy

    from memory_server.application.graph_read_service import relation_id
    from memory_server.application.inspection_service import _PreviewSnapshot
    from memory_server.application.remote_beliefs import encode_conflict_id as conflict_id

    state = inspection_http
    await seed(state)
    # Retain a real admitted read scope while the actual write's worker settles:
    # a new admission during atomic graph publication can observe its old inode.
    from sqlalchemy import text

    async with state.deps.leases.acquire(context(state)) as scope:
        resources = scope.application.read_resources()
        assert (await relation_write(state)).status == 201, "S17_NO_WRITE"
        async with asyncio.timeout(15):
            while True:
                async with resources.session_factory() as session:
                    pending = (
                        await session.execute(text("SELECT id FROM outbox_entries WHERE status != 'completed'"))
                    ).all()
                if not pending:
                    break
                await asyncio.sleep(0.01)
    operations = [
        (F, payload()),
        (F, payload("invalidate")),
        (F, payload("relation_delete", relation_id=relation_id(A, F, "supports"))),
        (A, payload("resolve", memory_type="belief", conflict_id=conflict_id(A, B))),
    ]
    # The actual relation write queued projection work. Observe its completion
    # before attributing any subsequent domain changes to an Inspector request.
    async with asyncio.timeout(15):
        while await rows(state, "SELECT id FROM outbox_entries WHERE status != 'completed'"):
            await asyncio.sleep(0.01)
    before = await domain_snapshot(state)
    async with state.deps.leases.acquire(context(state)) as scope:
        graph = scope.runtime.dependencies.graph
        graph_before = copy.deepcopy(graph.to_dict())
        reopened = type(graph)(graph._snapshot_path)
        reopened.load_snapshot()
        persisted_before = reopened.to_dict()
    statements = []
    original_statement = _PreviewSnapshot._statement

    def traced_statement(self, connection, cursor, statement, parameters, ctx, executemany):
        statements.append(statement)
        return original_statement(self, connection, cursor, statement, parameters, ctx, executemany)

    monkeypatch.setattr(_PreviewSnapshot, "_statement", traced_statement)
    for item, body in operations:
        audit_before = len(list(state.deps.access._db.execute("SELECT * FROM access_audit")))
        for _ in range(10):
            result = await preview(state, body, ident=item)
            assert result.status == 200, ("S17_NO_WRITE", result.body)
        ready, go = asyncio.Queue(), asyncio.Event()

        async def concurrent_preview():
            ready.put_nowait(True)
            await go.wait()
            return await preview(state, body, ident=item)

        tasks = [asyncio.create_task(concurrent_preview()) for _ in range(10)]
        for _ in tasks:
            await ready.get()
        go.set()
        results = await asyncio.gather(*tasks)
        assert all(r.status == 200 for r in results), ("S17_NO_WRITE", [(r.status, r.body) for r in results])
        assert await domain_snapshot(state) == before, "S17_NO_WRITE"
        audit_rows = list(state.deps.access._db.execute("SELECT * FROM access_audit ORDER BY seq"))[audit_before:]
        assert len(audit_rows) == 20, "S17_NO_WRITE audit delta"
        assert all(row["state"] == "terminal" and row["result"] == "success" for row in audit_rows), "S17_NO_WRITE"
        assert len({row["request_id"] for row in audit_rows}) == 20, "S17_NO_WRITE request IDs"
        assert graph.to_dict() == graph_before, "S17_NO_WRITE in-memory graph"
        reopened = type(graph)(graph._snapshot_path)
        reopened.load_snapshot()
        assert reopened.to_dict() == persisted_before, "S17_NO_WRITE persisted graph"
    assert statements, "S17_NO_WRITE borrowed SQLite trace required"
    assert all(sql.lstrip().split(None, 1)[0].upper() in {"SELECT", "BEGIN", "ROLLBACK"} for sql in statements), (
        "S17_NO_WRITE SQL"
    )
    print("S17_NO_WRITE operations=4 sequential=40 barrier=40 terminal_audit_rows=80")


async def test_s17_preview_stale_actual_cas(inspection_http):
    state = inspection_http
    await seed(state)
    result = await preview(state)
    assert result.status == 200 and result.json["observed_versions"][F] == 7, "S17_STALE_PREVIEW"
    body = dict(memory_type="fact", to_state="stale", expected_version=7, confirmation=True, reason="S17_REASON_CANARY")
    endpoint = f"/api/v1/profiles/alpha/memories/{F}/transition"
    first = await request(
        state.app, endpoint, method="POST", body=body, bearer=state.bearer, headers=(("Idempotency-Key", str(uuid4())),)
    )
    assert first.status == 200, ("S17_STALE_PREVIEW", first.body)
    before = await domain_snapshot(state)
    stale = await request(
        state.app, endpoint, method="POST", body=body, bearer=state.bearer, headers=(("Idempotency-Key", str(uuid4())),)
    )
    assert stale.status == 409 and stale.json["error"]["code"] == "E_VERSION_CONFLICT", "S17_STALE_PREVIEW"
    assert await domain_snapshot(state) == before, "S17_STALE_PREVIEW"


async def test_s17_preview_ttl_advisory(inspection_http):
    state = inspection_http
    await seed(state)
    service = state.deps.ports["mutations.preview"]
    object.__setattr__(service, "clock", lambda: T0)
    first = await preview(state)
    assert first.status == 200, "S17_TTL"
    assert first.json["expires_at"] == (T0 + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"), "S17_TTL"
    object.__setattr__(service, "clock", lambda: T0 + timedelta(seconds=31))
    second = await preview(state)
    assert second.json["expires_at"] == (T0 + timedelta(seconds=61)).isoformat().replace("+00:00", "Z"), "S17_TTL"
    assert second.json["observed_versions"] == first.json["observed_versions"], "S17_TTL"


async def test_s17_preview_auth_and_no_oracle(inspection_http):
    from memory_server.transports.http.errors import HttpError

    state = inspection_http
    await seed(state)
    reader = mint(state, ["memory.read"])
    for profile, version in (("alpha", 7), ("beta", 19)):
        response = await preview(state, bearer=reader, profile=profile)
        assert response.status == 200 and response.json["observed_versions"][F] == version, "S17_AUTH_SCOPE"
    error(
        await preview(state, bearer=mint(state, ["memory.read"], ("alpha",)), profile="beta"),
        403,
        "E_PERMISSION_DENIED",
        "S17_AUTH_SCOPE",
    )
    error(await preview(state, ident="00000000-0000-4000-8000-000000000099"), 404, "E_NOT_FOUND", "S17_AUTH_SCOPE")
    command = d.MutationsPreviewCommand(
        operation="mutations.preview", profile_id="beta", path_ids={"id": F}, payload=d.PreviewRequest(**payload())
    )
    rejected = False
    try:
        await state.deps.ports["mutations.preview"].execute(context(state), command)
    except HttpError as exc:
        rejected = exc.code == "E_VALIDATION"
    assert rejected, "S17_AUTH_SCOPE direct command profile binding"


async def test_s17_preview_limits_and_backend_failure(inspection_http, monkeypatch):
    import json

    from memory_server.application.inspection_service import _PreviewSnapshot
    from memory_server.transports.http.app import create_http_app
    from memory_server.transports.http.errors import HttpError

    state = inspection_http
    await seed(state)
    command = d.MutationsPreviewCommand(
        operation="mutations.preview", profile_id="alpha", path_ids={"id": F}, payload=d.PreviewRequest(**payload())
    )
    with pytest.raises(HttpError) as caught:
        await state.deps.ports["mutations.preview"].execute(replace(context(state), deadline=monotonic() - 1), command)
    assert caught.value.code == "E_TIMEOUT", "S17_BOUNDED"
    error(
        await preview(state, b" " * 1048577, headers=(("Content-Type", "application/json"),)),
        413,
        "E_REQUEST_TOO_LARGE",
        "S17_BOUNDED",
    )
    limited = create_http_app(
        replace(state.config, rate_burst=1, rate_per_minute=1),
        state.deps,
        read_routes=state.specs,
        mutation_routes=state.mutation_specs,
    )
    first = await request(limited, path(), method="POST", body=payload(), bearer=state.bearer)
    assert first.status == 200, "S17_BOUNDED rate control"
    rejected = await request(limited, path(), method="POST", body=payload(), bearer=state.bearer)
    assert rejected.status == 429 and rejected.json["error"]["code"] == "E_RATE_LIMIT", "S17_BOUNDED rate"
    assert "retry-after" in rejected.headers, "S17_BOUNDED rate"
    before = await domain_snapshot(state)
    original = _PreviewSnapshot._inspect
    completed = []

    async def exact_budget(self, cmd):
        result = await original(self, cmd)
        while self.statements < 63:
            await self._query("SELECT 1", {})
        completed.append(self)
        return result

    with monkeypatch.context() as patch:
        patch.setattr(_PreviewSnapshot, "_inspect", exact_budget)
        assert (await preview(state)).status == 200, "S17_BOUNDED statement boundary"
    assert completed[0].statements == 64, "S17_BOUNDED rollback reserved"

    async def over_budget(self, cmd):
        await exact_budget(self, cmd)
        await self._query("SELECT 1", {})

    with monkeypatch.context() as patch:
        patch.setattr(_PreviewSnapshot, "_inspect", over_budget)
        response = await preview(state)
        assert response.status == 503 and response.json["error"]["details"] == {"limit": 64}, "S17_BOUNDED statements"

    async def vm_budget(self, cmd):
        await self._query(
            "SELECT sum(a.value*b.value) FROM json_each(:items) a CROSS JOIN json_each(:items) b",
            {"items": json.dumps(list(range(2000)))},
        )
        raise AssertionError("S17_BOUNDED VM failed to interrupt real SQLite")

    with monkeypatch.context() as patch:
        patch.setattr(_PreviewSnapshot, "_inspect", vm_budget)
        response = await preview(state)
        assert response.status == 503 and response.json["error"]["details"] == {"limit": 2_000_000}, "S17_BOUNDED VM"
    assert (await preview(state)).status == 200, "S17_BOUNDED callback reset"
    assert await domain_snapshot(state) == before, "S17_BOUNDED no partial effect"


async def test_s17_preview_cancel_late_response_isolation(inspection_http, monkeypatch):
    from memory_server.application.inspection_service import _PreviewSnapshot

    state = inspection_http
    await seed(state)
    entered, release = asyncio.Event(), asyncio.Event()
    original = _PreviewSnapshot._cleanup

    async def blocked(self, connection):
        entered.set()
        await release.wait()
        return await original(self, connection)

    monkeypatch.setattr(_PreviewSnapshot, "_cleanup", blocked)
    command = d.MutationsPreviewCommand(
        operation="mutations.preview", profile_id="alpha", path_ids={"id": F}, payload=d.PreviewRequest(**payload())
    )
    task = asyncio.create_task(state.deps.ports["mutations.preview"].execute(context(state), command))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done(), "S17_LATE_ISOLATION"
        assert state.deps.leases.snapshot()["alpha"].admissions >= 1, "S17_LATE_ISOLATION"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
    response = await preview(state, profile="beta")
    assert response.status == 200 and response.json["observed_versions"][F] == 19, "S17_LATE_ISOLATION"
    # A failed progress reset must invalidate while the connection is still
    # borrowed, not after session.rollback() has returned it to the pool.
    from sqlalchemy import event

    monkeypatch.setattr(_PreviewSnapshot, "_cleanup", original)
    original_handler = _PreviewSnapshot._handler
    events = []
    pools = []

    def invalidated(*args):
        events.append("invalidate")

    def checked_in(*args):
        events.append("checkin")

    async def reset_failure(self, connection, callback, step):
        pool = connection.sync_connection.engine.pool
        if pool not in pools:
            pools.append(pool)
            event.listen(pool, "invalidate", invalidated)
            event.listen(pool, "checkin", checked_in)
        if callback is None:
            events.append("reset-failed")
            raise RuntimeError("synthetic reset failure")
        await original_handler(self, connection, callback, step)

    monkeypatch.setattr(_PreviewSnapshot, "_handler", reset_failure)
    try:
        error(await preview(state), 503, "E_STORAGE", "S17_LATE_ISOLATION")
        assert "invalidate" in events, ("S17_LATE_ISOLATION reset failure must invalidate", events)
        assert events.index("invalidate") < events.index("checkin"), (
            "S17_LATE_ISOLATION invalidation before pool release",
            events,
        )
    finally:
        for pool in pools:
            event.remove(pool, "invalidate", invalidated)
            event.remove(pool, "checkin", checked_in)


async def test_s17_preview_projection_privacy(inspection_http, caplog, monkeypatch):
    from memory_server.application.inspection_service import _PreviewSnapshot

    state = inspection_http
    await seed(state)
    response = await preview(state)
    assert response.status == 200, "S17_PRIVACY"
    assert response.json["projections"] == dict(vector="unknown", graph="unknown", outbox="unknown"), "S17_PRIVACY"
    original = _PreviewSnapshot._query

    async def sql_failure(self, sql, params):
        return await original(self, "SELECT S17_SQL_CANARY FROM s17_missing_table", {})

    monkeypatch.setattr(_PreviewSnapshot, "_query", sql_failure)
    failed = await preview(state)
    error(failed, 503, "E_STORAGE", "S17_PRIVACY")
    for canary in (
        "S17_CONTENT_CANARY",
        "S17_REASON_CANARY",
        "S17_SQL_CANARY",
        "S17_SECRET_CANARY",
        "/S17_PATH_CANARY",
    ):
        assert canary not in response.body.decode() + failed.body.decode() + caplog.text, "S17_PRIVACY"


async def test_s17_preview_does_not_authorize_mutation(inspection_http):
    state = inspection_http
    await seed(state)
    bearer = mint(state, ["memory.read"])
    assert (await preview(state, bearer=bearer)).status == 200, "S17_STALE_PREVIEW"
    body = dict(memory_type="fact", to_state="stale", expected_version=7, confirmation=True, reason="test")
    endpoint = f"/api/v1/profiles/alpha/memories/{F}/transition"
    result = await request(
        state.app, endpoint, method="POST", body=body, bearer=bearer, headers=(("Idempotency-Key", str(uuid4())),)
    )
    error(result, 403, "E_PERMISSION_DENIED", "S17_STALE_PREVIEW")
    result = await request(state.app, endpoint, method="POST", body=body, bearer=state.bearer)
    error(result, 400, "E_IDEMPOTENCY_REQUIRED", "S17_STALE_PREVIEW")
