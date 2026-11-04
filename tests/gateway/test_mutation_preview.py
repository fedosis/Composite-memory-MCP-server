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
    await change(
        state,
        "INSERT INTO claim_relations(source_id,target_id,relation_type,created_at) VALUES(:a,:f,'supports',:t)",
        {"a": A, "f": F, "t": T0},
    )
    for operation in ("transition", "invalidate", "relation_delete", "resolve"):
        body = payload(operation)
        if operation == "relation_delete":
            body["relation_id"] = relation_id(A, F, "supports")
        if operation == "resolve":
            body.update(memory_type="belief", conflict_id=conflict_id(A, B))
        item = A if operation == "resolve" else F
        baseline = await preview(state, body, ident=item)
        assert baseline.status == 200, ("S17_FIELD_MATRIX valid baseline", operation, baseline.body)
        nonnull = {"to_state": "stale", "relation_id": relation_id(A, F, "supports"), "conflict_id": conflict_id(A, B)}
        for key in body:
            invalid = None if body[key] is not None else nonnull[key]
            error(
                await preview(state, body | {key: invalid}, ident=item),
                422,
                "E_VALIDATION",
                "S17_FIELD_MATRIX nullability",
            )
            for invalid in (True, [], {}):
                error(
                    await preview(state, body | {key: invalid}, ident=item),
                    422,
                    "E_VALIDATION",
                    "S17_FIELD_MATRIX types",
                )
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
    from sqlalchemy import text
    from storage.models.belief import BeliefORM
    from storage.models.fact import FactORM

    from memory_server.transports.http.errors import HttpError

    for kind in ("fact", "belief"):
        for corrupt in (True, False, "invalid", -1, 0, None):
            if kind == "fact" and corrupt in (0, -1, None) and type(corrupt) is not bool:
                continue  # Valid legacy normalization, covered above.
            with pytest.raises(HttpError) as caught:
                _version(SimpleNamespace(kind=kind, version=corrupt))
            assert caught.value.code == "E_INTERNAL", "S17_LIFECYCLE_PARITY corrupt/bool version"
    for table, item, bad in (("facts", F, "invalid"), ("beliefs", A, "invalid"), ("beliefs", A, 0)):
        await change(
            state, f"UPDATE {table} SET version=:v,lifecycle_state='active' WHERE id=:id", {"v": bad, "id": item}
        )
        error(
            await preview(state, payload(memory_type="fact" if table == "facts" else "belief"), ident=item),
            500,
            "E_INTERNAL",
            "S17_LIFECYCLE_PARITY stored corrupt version",
        )
    serial = 2000
    async with state.deps.leases.acquire(context(state)) as scope:
        resources = scope.application.read_resources()
        for kind, version in (("fact", 7), ("belief", 3)):
            for current in LifecycleState:
                for operation, target in [("transition", s.value) for s in LifecycleState] + [
                    ("invalidate", "discarded")
                ]:
                    serial += 1
                    item = ident(serial)
                    common = dict(
                        id=item,
                        version=str(version) if kind == "fact" else version,
                        lifecycle_state=current.value,
                        confidence=0.8,
                        created_at=T0,
                        updated_at=T0,
                    )
                    row = (
                        FactORM(**common, subject="fresh", predicate="is", object="fixture", dedup_key=item)
                        if kind == "fact"
                        else BeliefORM(**common, proposition="Fresh independently seeded lifecycle comparison.")
                    )
                    async with resources.session_factory() as session, session.begin():
                        session.add(row)
                    command = payload(
                        operation, memory_type=kind, to_state=target if operation == "transition" else None
                    )
                    advised = await preview(state, command, ident=item)
                    valid = is_valid_transition(current, LifecycleState(target))
                    assert advised.status == (200 if valid else 422), (
                        "S17_LIFECYCLE_PARITY fresh preview",
                        kind,
                        current,
                        operation,
                        target,
                        advised.body,
                    )
                    actual = await request(
                        state.app,
                        f"/api/v1/profiles/alpha/memories/{item}/{operation}",
                        method="POST",
                        bearer=state.bearer,
                        body=dict(memory_type=kind, expected_version=version, confirmation=True, reason="test")
                        | ({"to_state": target} if operation == "transition" else {}),
                        headers=(("Idempotency-Key", str(uuid4())),),
                    )
                    assert actual.status == (200 if valid else 422), (
                        "S17_LIFECYCLE_PARITY fresh actual",
                        kind,
                        current,
                        operation,
                        target,
                        actual.body,
                    )
                    async with resources.session_factory() as session:
                        durable = (
                            await session.execute(
                                text(
                                    "SELECT version,lifecycle_state FROM "
                                    f"{'facts' if kind == 'fact' else 'beliefs'} WHERE id=:id"
                                ),
                                {"id": item},
                            )
                        ).one()
                    assert int(durable[0]) == version + int(valid) and durable[1] == (
                        target if valid else current.value
                    ), "S17_LIFECYCLE_PARITY fresh persisted effect"
                    async with asyncio.timeout(15):
                        while True:
                            async with resources.session_factory() as session:
                                pending = (
                                    await session.execute(
                                        text("SELECT id FROM outbox_entries WHERE status != 'completed'")
                                    )
                                ).all()
                            if not pending:
                                break
                            await asyncio.sleep(0.01)
        async with asyncio.timeout(30):
            while True:
                async with resources.session_factory() as session:
                    pending = (
                        await session.execute(text("SELECT id FROM outbox_entries WHERE status != 'completed'"))
                    ).all()
                if not pending:
                    break
                await asyncio.sleep(0.01)


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
            assert result["affected_ids"] == [F] and result["observed_versions"] == {F: 7}, (
                "S17_BOUNDED raw513 no hidden sentinel/version"
            )
            assert result["count_exact"] is True and result["truncated"] is False, (
                "S17_BOUNDED raw513 complete feasible impact"
            )
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
    import random

    async with state.deps.leases.acquire(context(state)) as scope:
        graph = scope.runtime.dependencies.graph
        for item in (F, ident(6)):
            if graph.get_node(item) is None:
                graph.add_node(item, "belief", "graph-only decoy")
        graph.add_edge(ident(6), F, "derived_from")
    edges = [
        (ident(1), F, "derived_from"),
        (ident(2), F, "derived_from"),
        (F, F, "derived_from"),
        (ident(1), A, "derived_from"),
        (ident(1), B, "derived_from"),
        (ident(2), B, "derived_from"),
        (ident(900), F, "derived_from"),
        (F, ident(3), "derived_from"),
        (ident(4), F, "derives"),
        (ident(5), ident(1), "derived_from"),
    ]
    rng = random.Random(170017)
    for _ in range(8):
        rng.shuffle(edges)
        await change(state, "DELETE FROM claim_relations")
        async with state.deps.leases.acquire(context(state)) as scope:
            async with scope.application.read_resources().session_factory() as session, session.begin():
                session.add_all([ClaimRelationORM(source_id=a, target_id=b, relation_type=t) for a, b, t in edges])
        response = await preview(state, payload("invalidate"))
        assert response.status == 200, ("S17_BOUNDED permutation", response.body)
        assert response.json["affected_ids"] == [F, ident(1), ident(2)], (
            "S17_BOUNDED self/shared/dangling/inverse/transitive/graph-decoys"
        )
        assert response.json["observed_versions"] == {F: 7, ident(1): 1, ident(2): 2}, (
            "S17_BOUNDED permutation versions"
        )
        assert response.json["count_exact"] is True and response.json["truncated"] is False, (
            "S17_BOUNDED complete permutation"
        )
        from memory_server.application.remote_beliefs import encode_conflict_id

        shared = await preview(
            state, payload("resolve", memory_type="belief", conflict_id=encode_conflict_id(A, B)), ident=A
        )
        assert shared.status == 200, ("S17_BOUNDED shared pair", shared.body)
        assert shared.json["affected_ids"] == [A, B, ident(1), ident(2)], "S17_BOUNDED shared pair distinct union"
        assert shared.json["observed_versions"] == {A: 3, B: 5, ident(1): 1, ident(2): 2}, "S17_BOUNDED shared versions"
    for raw in (100, 101, 512, 513):
        await change(state, "DELETE FROM claim_relations")
        async with state.deps.leases.acquire(context(state)) as scope:
            async with scope.application.read_resources().session_factory() as session, session.begin():
                session.add_all(
                    [
                        ClaimRelationORM(source_id=ident(10000 + i), target_id=F, relation_type="derived_from")
                        for i in range(raw)
                    ]
                )
        response = await preview(state, payload("invalidate"))
        assert response.status == 200, ("S17_BOUNDED all-dangling", response.body)
        complete = raw == 100 or raw == 513  # 513 disallows the invalidating action before impact.
        assert response.json["affected_ids"] == [F] and response.json["observed_versions"] == {F: 7}, (
            "S17_BOUNDED no hidden sentinel/version"
        )
        assert response.json["count_exact"] is complete and response.json["truncated"] is not complete, (
            "S17_BOUNDED count_exact iff complete raw101/513"
        )


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
    import base64
    import json

    from storage.models.fact import FactORM

    virtual = relation_id(A, B, "supports")
    await change(
        state,
        "INSERT INTO claim_relations(source_id,target_id,relation_type,created_at) VALUES(:a,:b,'supports',:t)",
        {"a": A, "b": B, "t": T0},
    )
    ledger_before = await rows(state, "SELECT * FROM relation_revisions ORDER BY rowid")
    result = await preview(state, payload("relation_delete", memory_type="belief", relation_id=virtual), ident=A)
    assert result.status == 200 and result.json["observed_versions"] == {A: 3, B: 5, virtual: 1}, (
        "S17_RELATION_CANONICAL virtual ledger"
    )
    assert await rows(state, "SELECT * FROM relation_revisions ORDER BY rowid") == ledger_before, (
        "S17_RELATION_CANONICAL virtual no ledger write"
    )
    malformed = [virtual + "=", "proj_" + virtual[4:], "rel_!", relation_id(A, B, "derived_from")]
    for parts in ([A, B], [B, A, "unknown"], [A.upper(), B, "supports"], [A, B, "supports", "extra"]):
        malformed.append("rel_" + base64.urlsafe_b64encode(json.dumps(parts).encode()).decode().rstrip("="))
    for bad in malformed:
        error(
            await preview(state, payload("relation_delete", memory_type="belief", relation_id=bad), ident=A),
            422,
            "E_VALIDATION",
            "S17_RELATION_CANONICAL codec",
        )
    for bad in (relation_id(A, B, "contradicts"), relation_id(ident(900), B, "supports")):
        error(
            await preview(state, payload("relation_delete", relation_id=bad)),
            404,
            "E_NOT_FOUND",
            "S17_RELATION_CANONICAL unrelated/absent",
        )
    await change(
        state,
        "INSERT INTO claim_relations(source_id,target_id,relation_type,created_at) VALUES(:a,:a,'supports',:t)",
        {"a": A, "t": T0},
    )
    self_handle = relation_id(A, A, "supports")
    result = await preview(state, payload("relation_delete", memory_type="belief", relation_id=self_handle), ident=A)
    assert result.status == 200 and result.json["affected_ids"] == [A], "S17_RELATION_CANONICAL self once"
    for ceiling, count in (("incidence", 512), ("incidence", 513), ("pair", 16), ("pair", 17)):
        await change(
            state,
            "DELETE FROM claim_relations WHERE NOT (source_id=:a AND target_id=:f AND relation_type='supports')",
            {"a": A, "f": F},
        )
        for i in range(count - 1):
            await change(
                state,
                "INSERT INTO claim_relations(source_id,target_id,relation_type,created_at) VALUES(:a,:t,:kind,:time)",
                {"a": A, "t": F if ceiling == "pair" else ident(20000 + i), "kind": "synthetic_" + str(i), "time": T0},
            )
        result = await preview(state, body)
        assert result.status == 200, ("S17_RELATION_CANONICAL ceiling", result.body)
        assert result.json["allowed_actions"] == (["relation_delete"] if count in (512, 16) else []), (
            "S17_RELATION_CANONICAL incidence512/513 pair16/17"
        )
        assert result.json["observed_versions"] == {A: 3, F: 7, handle: 3}, "S17_RELATION_CANONICAL ceiling versions"
    async with state.deps.leases.acquire(context(state)) as scope:
        async with scope.application.read_resources().session_factory() as session, session.begin():
            session.add(
                FactORM(
                    id=A,
                    subject="collision",
                    predicate="is",
                    object="test",
                    dedup_key=A,
                    version="1",
                    lifecycle_state="active",
                    confidence=0.8,
                )
            )
    error(await preview(state, body), 404, "E_NOT_FOUND", "S17_RELATION_CANONICAL collision")


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
    from sqlalchemy import text
    from storage.models.belief import BeliefORM, EvidenceORM

    await change(state, "DELETE FROM claim_relations")
    for count in (1024, 1025):
        await change(state, "DELETE FROM evidence")
        async with state.deps.leases.acquire(context(state)) as scope:
            async with scope.application.read_resources().session_factory() as session, session.begin():
                session.add_all(
                    [
                        EvidenceORM(
                            id=ident(30000 + i),
                            belief_id=A,
                            source_type="test",
                            source_id="test",
                            weight=0.7,
                            contributor="test",
                            note="S17_REASON_CANARY",
                        )
                        for i in range(count)
                    ]
                )
        result = await preview(state, body, ident=A)
        assert result.status == 200, ("S17_RESOLVE_Q10 evidence", result.body)
        assert ("merge" in result.json["allowed_actions"]) is (count == 1024), "S17_RESOLVE_Q10 evidence1024/1025"
        assert "defer" in result.json["allowed_actions"] and result.json["observed_versions"] == {A: 3, B: 5}, (
            "S17_RESOLVE_Q10 evidence no conf version"
        )
    async with state.deps.leases.acquire(context(state)) as scope:
        resources = scope.application.read_resources()
        for index, action in enumerate(("keep_a", "keep_b", "merge", "mark_contradicted", "discard_both", "defer")):
            a, b = ident(40000 + 2 * index), ident(40001 + 2 * index)
            async with resources.session_factory() as session, session.begin():
                session.add_all(
                    [
                        BeliefORM(
                            id=item,
                            proposition="SQLite provides canonical durable memory storage.",
                            version=version,
                            confidence=0.8,
                            lifecycle_state="active",
                            created_at=T0,
                            updated_at=T0,
                        )
                        for item, version in ((a, 3), (b, 5))
                    ]
                )
            handle = conflict_id(a, b)
            advice = await preview(state, payload("resolve", memory_type="belief", conflict_id=handle), ident=a)
            assert advice.status == 200 and action in advice.json["allowed_actions"], (
                "S17_RESOLVE_Q10 fresh per-action advice"
            )
            result = await request(
                state.app,
                f"/api/v1/profiles/alpha/conflicts/{handle}/resolve",
                method="POST",
                bearer=state.bearer,
                body=dict(
                    belief_a_id=a,
                    belief_b_id=b,
                    resolution=action,
                    new_proposition="The project uses SQLite for canonical memory storage."
                    if action == "merge"
                    else None,
                    expected_versions=advice.json["observed_versions"],
                    reason="test",
                    confirmation=True,
                ),
                headers=(("Idempotency-Key", str(uuid4())),),
            )
            assert result.status == 200, ("S17_RESOLVE_Q10 fresh per-action actual", action, result.body)
            targets = {
                "keep_a": {b: "discarded"},
                "keep_b": {a: "discarded"},
                "merge": {a: "superseded", b: "superseded"},
                "mark_contradicted": {a: "contradicted", b: "contradicted"},
                "discard_both": {a: "discarded", b: "discarded"},
                "defer": {},
            }[action]
            async with resources.session_factory() as session:
                actual_rows = (
                    await session.execute(
                        text("SELECT id,version,lifecycle_state FROM beliefs WHERE id IN (:a,:b) ORDER BY id"),
                        {"a": a, "b": b},
                    )
                ).all()
                for receipt in result.json["receipt_ids"]:
                    assert (
                        await session.execute(text("SELECT id FROM receipts WHERE id=:id"), {"id": receipt})
                    ).scalar_one() == receipt, "S17_RESOLVE_Q10 actual action receipt"
            assert actual_rows == [
                (item, version + int(item in targets), targets.get(item, "active"))
                for item, version in ((a, 3), (b, 5))
            ], "S17_RESOLVE_Q10 fresh per-action durable parity"
        async with asyncio.timeout(30):
            while True:
                async with resources.session_factory() as session:
                    pending = (
                        await session.execute(text("SELECT id FROM outbox_entries WHERE status != 'completed'"))
                    ).all()
                if not pending:
                    break
                await asyncio.sleep(0.01)


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
    dbapi_statements = []
    original_handler = _PreviewSnapshot._handler

    async def traced_driver(self, connection, callback, step):
        await original_handler(self, connection, callback, step)

        async def install(driver):
            await driver.set_trace_callback(dbapi_statements.append if callback is not None else None)

        await connection.run_sync(lambda conn: conn.connection.dbapi_connection.run_async(install))

    monkeypatch.setattr(_PreviewSnapshot, "_handler", traced_driver)
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
    assert dbapi_statements, "S17_NO_WRITE DBAPI statement trace required"
    assert all(sql.lstrip().split(None, 1)[0].upper() in {"SELECT", "BEGIN", "ROLLBACK"} for sql in dbapi_statements), (
        "S17_NO_WRITE DBAPI zero DML/DDL"
    )

    async def write_literal(self, command):
        await self.session.execute(text("UPDATE facts SET version='999' WHERE id=:id"), {"id": F})
        pytest.fail("S17_NO_WRITE allowlist accepted write literal")

    with monkeypatch.context() as patch:
        patch.setattr(_PreviewSnapshot, "_inspect", write_literal)
        error(await preview(state), 503, "E_STORAGE", "S17_NO_WRITE allowlist rejects write literal")
    assert any(sql.startswith("UPDATE facts") for sql in statements), "S17_NO_WRITE write reached production allowlist"
    assert not any(sql.startswith("UPDATE facts") for sql in dbapi_statements), "S17_NO_WRITE write never reached DBAPI"
    assert await domain_snapshot(state) == before, "S17_NO_WRITE masked write has no effect"
    print(
        "S17_NO_WRITE operations=4 sequential=40 barrier=40 terminal_audit_rows=80 "
        "DBAPI_DML_DDL=0 write_literal_rejected=true"
    )


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

    from memory_server.application.remote_beliefs import encode_conflict_id

    handle = encode_conflict_id(A, B)
    pair_preview = await preview(state, payload("resolve", memory_type="belief", conflict_id=handle), ident=A)
    assert pair_preview.status == 200, ("S17_STALE_PREVIEW", pair_preview.body)
    assert pair_preview.json["observed_versions"] == {A: 3, B: 5}, "S17_STALE_PREVIEW"
    from sqlalchemy import text

    # Keep the admitted runtime and its borrowed read resources while the real
    # mutation publishes its graph snapshot; do not re-admit across inode swap.
    async with state.deps.leases.acquire(context(state)) as scope:
        resources = scope.application.read_resources()
        updated = await request(
            state.app,
            f"/api/v1/profiles/alpha/memories/{A}/transition",
            method="POST",
            body=dict(memory_type="belief", to_state="stale", expected_version=3, confirmation=True, reason="test"),
            bearer=state.bearer,
            headers=(("Idempotency-Key", str(uuid4())),),
        )
        assert updated.status == 200, ("S17_STALE_PREVIEW side update", updated.body)
        async with asyncio.timeout(15):
            while True:
                async with resources.session_factory() as session:
                    version = (
                        await session.execute(text("SELECT version FROM beliefs WHERE id=:id"), {"id": A})
                    ).scalar_one()
                    pending = (
                        await session.execute(text("SELECT id FROM outbox_entries WHERE status != 'completed'"))
                    ).all()
                assert version == 4, "S17_STALE_PREVIEW"
                if not pending:
                    break
                await asyncio.sleep(0.01)
    before_defer = await domain_snapshot(state)
    stale_defer = await request(
        state.app,
        f"/api/v1/profiles/alpha/conflicts/{handle}/resolve",
        method="POST",
        body=dict(
            belief_a_id=A,
            belief_b_id=B,
            resolution="defer",
            new_proposition=None,
            expected_versions=pair_preview.json["observed_versions"],
            reason="test",
            confirmation=True,
        ),
        bearer=state.bearer,
        headers=(("Idempotency-Key", str(uuid4())),),
    )
    after_defer = await domain_snapshot(state)
    receipt_delta = [row for row in after_defer["receipts"] if row not in before_defer["receipts"]]
    assert stale_defer.status == 409, (
        "S17_STALE_PREVIEW stale resolve-defer expected409",
        stale_defer.status,
        stale_defer.body,
        "persisted_receipt_delta",
        receipt_delta,
    )
    assert stale_defer.json["error"]["code"] == "E_VERSION_CONFLICT", "S17_STALE_PREVIEW"
    assert after_defer == before_defer, "S17_STALE_PREVIEW stale defer no action receipt or journal"


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
    from sqlalchemy import text

    endpoint = f"/api/v1/profiles/alpha/memories/{F}/transition"
    async with state.deps.leases.acquire(context(state)) as scope:
        resources = scope.application.read_resources()

        async def actual(target, revision):
            return await request(
                state.app,
                endpoint,
                method="POST",
                bearer=state.bearer,
                body=dict(
                    memory_type="fact", to_state=target, expected_version=revision, confirmation=True, reason="test"
                ),
                headers=(("Idempotency-Key", str(uuid4())),),
            )

        async def settled_version():
            async with asyncio.timeout(15):
                while True:
                    async with resources.session_factory() as session:
                        pending = (
                            await session.execute(text("SELECT id FROM outbox_entries WHERE status != 'completed'"))
                        ).all()
                        revision = (
                            await session.execute(text("SELECT version FROM facts WHERE id=:id"), {"id": F})
                        ).scalar_one()
                    if not pending:
                        return revision
                    await asyncio.sleep(0.01)

        object.__setattr__(service, "clock", lambda: T0 + timedelta(seconds=1))
        assert (await actual("stale", 7)).status == 200, "S17_TTL first actual mutation"
        assert await settled_version() == "8", "S17_TTL actual revision"
        stale = await actual("archived", first.json["observed_versions"][F])
        assert stale.status == 409 and stale.json["error"]["code"] == "E_VERSION_CONFLICT", (
            "S17_TTL stale before expiry"
        )
        object.__setattr__(service, "clock", lambda: T0 + timedelta(seconds=31))
        fresh = await preview(state, payload(to_state="archived"))
        assert fresh.status == 200 and fresh.json["observed_versions"][F] == 8, "S17_TTL fresh after expiry"
        assert (await actual("archived", fresh.json["observed_versions"][F])).status == 200, "S17_TTL CAS after expiry"
        assert await settled_version() == "9", "S17_TTL durable after expiry"


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
    from datetime import UTC, datetime

    from memory_server.application.graph_read_service import relation_id
    from memory_server.application.remote_beliefs import encode_conflict_id

    await change(
        state,
        "INSERT INTO claim_relations(source_id,target_id,relation_type,created_at) VALUES(:a,:f,'supports',:t)",
        {"a": A, "f": F, "t": T0},
    )
    operations = [
        (F, payload()),
        (F, payload("invalidate")),
        (F, payload("relation_delete", relation_id=relation_id(A, F, "supports"))),
        (A, payload("resolve", memory_type="belief", conflict_id=encode_conflict_id(A, B))),
    ]
    for item, body in operations:
        assert (await preview(state, body, ident=item, bearer=reader)).status == 200, (
            "S17_AUTH_SCOPE all-four read-only"
        )
        for bearer in ("", mint(state, ["memory.read"], expired=True)):
            error(
                await preview(state, body, ident=item, bearer=bearer),
                401,
                "E_AUTH_REQUIRED",
                "S17_AUTH_SCOPE lifecycle",
            )
    state.deps.tokens._unchecked_revoke(
        state.deps.tokens._local_context(datetime.now(UTC)),
        reader[5:].split(".", 1)[0],
        d.ControlMutation(reason="revoke", confirmation=True, expected_generation=1),
        datetime.now(UTC),
        local=True,
    )
    for item, body in operations:
        error(
            await preview(state, body, ident=item, bearer=reader),
            401,
            "E_AUTH_REQUIRED",
            "S17_AUTH_SCOPE cached revoke",
        )
    disabled = await request(
        state.app,
        "/api/v1/profiles/alpha/disable",
        method="POST",
        bearer=state.bearer,
        body=dict(reason="test", confirmation=True, expected_generation=1),
        headers=(("Idempotency-Key", str(uuid4())),),
    )
    assert disabled.status == 200, ("S17_AUTH_SCOPE disable", disabled.body)
    for item, body in operations:
        error(await preview(state, body, ident=item), 409, "E_PROFILE_DISABLED", "S17_AUTH_SCOPE disabled")


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
    original_dump = d.PreviewResult.model_dump
    # Inject only at the otherwise unreachable serializer boundary: request,
    # snapshot, strict result validation and the production byte guard stay real.
    for size in (65536, 65537):

        def boundary_dump(self, *args, **kwargs):
            value = original_dump(self, *args, **kwargs)
            value["s17_boundary_padding"] = ""
            overhead = len(json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode())
            value["s17_boundary_padding"] = "x" * (size - overhead)
            return value

        with monkeypatch.context() as patch:
            patch.setattr(d.PreviewResult, "model_dump", boundary_dump)
            boundary = await preview(state)
        if size == 65536:
            assert boundary.status == 200 and len(boundary.body) == 65536, (
                "S17_BOUNDED output65536",
                boundary.status,
                len(boundary.body),
            )
        else:
            assert boundary.status == 503 and boundary.json["error"]["details"] == {"limit": 65536}, (
                "S17_BOUNDED output65537"
            )
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
    import json
    import threading

    from memory_server.transports.http.errors import HttpError

    loop = asyncio.get_running_loop()
    progress = _PreviewSnapshot._progress
    for mode in ("cancel", "deadline"):
        sql_entered, cleanup_entered = asyncio.Event(), asyncio.Event()
        cleanup_release = asyncio.Event()
        driver_release = threading.Event()
        signalled, cleanup_tasks = [], []

        def blocked_progress(self):
            if not signalled:
                signalled.append(True)
                loop.call_soon_threadsafe(sql_entered.set)
                driver_release.wait(10)
            return progress(self)

        async def long_select(self, cmd):
            await self._query(
                "SELECT sum(a.value*b.value) FROM json_each(:items) a CROSS JOIN json_each(:items) b",
                {"items": json.dumps(list(range(2000)))},
            )
            pytest.fail("S17_LATE_ISOLATION real long SELECT unexpectedly completed")

        async def cleanup_trace(self, connection):
            cleanup_tasks.append(asyncio.current_task())
            cleanup_entered.set()
            await cleanup_release.wait()
            return await original(self, connection)

        with monkeypatch.context() as patch:
            patch.setattr(_PreviewSnapshot, "_progress", blocked_progress)
            patch.setattr(_PreviewSnapshot, "_inspect", long_select)
            patch.setattr(_PreviewSnapshot, "_cleanup", cleanup_trace)
            ctx = replace(context(state), deadline=monotonic() + (0.5 if mode == "deadline" else 20))
            query_task = asyncio.create_task(state.deps.ports["mutations.preview"].execute(ctx, command))
            try:
                await asyncio.wait_for(sql_entered.wait(), 5)
                if mode == "cancel":
                    query_task.cancel()
                    await asyncio.sleep(0)
                else:
                    await asyncio.sleep(max(0, ctx.deadline - monotonic()) + 0.02)
                assert not query_task.done(), "S17_LATE_ISOLATION driver running after cancel/deadline"
                assert state.deps.leases.snapshot()["alpha"].admissions >= 1, "S17_LATE_ISOLATION driver lease held"
                driver_release.set()
                await asyncio.wait_for(cleanup_entered.wait(), 10)
                if mode == "cancel":
                    query_task.cancel()
                    await asyncio.sleep(0)
                assert not query_task.done(), "S17_LATE_ISOLATION long SELECT cleanup not joined"
                assert state.deps.leases.snapshot()["alpha"].admissions >= 1, (
                    "S17_LATE_ISOLATION long SELECT lease held"
                )
            finally:
                driver_release.set()
                cleanup_release.set()
                outcome = (await asyncio.gather(query_task, return_exceptions=True))[0]
            assert cleanup_tasks and all(task.done() for task in cleanup_tasks), (
                "S17_LATE_ISOLATION long SELECT owned tasks joined"
            )
            if mode == "cancel":
                assert isinstance(outcome, asyncio.CancelledError), "S17_LATE_ISOLATION cancellation preserved"
            else:
                assert isinstance(outcome, HttpError) and outcome.code == "E_TIMEOUT", (
                    "S17_LATE_ISOLATION deadline barrier"
                )
        for profile, revision in (("beta", 19), ("alpha", 7)):
            result = await preview(state, profile=profile)
            assert result.status == 200 and result.json["observed_versions"][F] == revision, (
                "S17_LATE_ISOLATION long SELECT next read"
            )
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
    monkeypatch.setattr(_PreviewSnapshot, "_query", original)
    from prometheus_client import generate_latest

    original_inspect = _PreviewSnapshot._inspect
    corruptions = (
        ("observed_versions", {F: True}),
        ("affected_count", True),
        ("count_exact", 1),
        ("truncated", 0),
        ("preview_only", 1),
        ("nested-extra", "S17_SECRET_CANARY"),
        ("nested-type", "S17_CONTENT_CANARY"),
        ("extra", "/S17_PATH_CANARY"),
    )
    corrupt_responses = []
    for key, value in corruptions:

        async def corrupt(self, command, key=key, value=value):
            result = await original_inspect(self, command)
            if key == "nested-extra":
                result.projections.__dict__["hidden"] = value
            elif key == "nested-type":
                result.projections.__dict__["vector"] = value
            else:
                result.__dict__[key] = value
            return result

        with monkeypatch.context() as patch:
            patch.setattr(_PreviewSnapshot, "_inspect", corrupt)
            invalid = await preview(state)
        error(invalid, 500, "E_INTERNAL", "S17_PRIVACY nested result corruption")
        corrupt_responses.append(invalid)
    access = [dict(row) for row in state.deps.access._db.execute("SELECT * FROM access_audit")]
    surfaces = repr(access) + generate_latest(state.deps.metrics._registry).decode()
    surfaces += "".join(r.body.decode() for r in corrupt_responses)
    for canary in (
        "S17_CONTENT_CANARY",
        "S17_REASON_CANARY",
        "S17_SQL_CANARY",
        "S17_SECRET_CANARY",
        "/S17_PATH_CANARY",
    ):
        assert canary not in response.body.decode() + failed.body.decode() + caplog.text + surfaces, "S17_PRIVACY"


async def test_s17_preview_does_not_authorize_mutation(inspection_http, monkeypatch):
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
    from sqlalchemy import text
    from storage.repositories.http_request_repo import HttpRequestRepository

    from memory_server.application.mutation_uow import MutationUoW

    run_original, crossed = MutationUoW.run, []

    async def traced_run(self, ctx, command, handler):
        crossed.append(command.operation)
        return await run_original(self, ctx, command, handler)

    monkeypatch.setattr(MutationUoW, "run", traced_run)
    missing = dict(body)
    del missing["expected_version"]
    absent = await request(
        state.app,
        endpoint,
        method="POST",
        body=missing,
        bearer=state.bearer,
        headers=(("Idempotency-Key", str(uuid4())),),
    )
    assert absent.status == 428 and absent.json["error"]["code"] == "E_PRECONDITION_REQUIRED", (
        "S17_STALE_PREVIEW missing CAS",
        absent.body,
    )
    async with state.deps.leases.acquire(context(state)) as scope:
        resources = scope.application.read_resources()
        ready, go = asyncio.Queue(), asyncio.Event()
        keys = [str(uuid4()), str(uuid4())]

        async def contender(key):
            ready.put_nowait(True)
            await go.wait()
            deadline = monotonic() + 2
            for attempt in range(40):
                result = await request(
                    state.app,
                    endpoint,
                    method="POST",
                    body=body,
                    bearer=state.bearer,
                    headers=(("Idempotency-Key", key),),
                )
                transient = result.status == 503 and result.json["error"]["code"] == "E_AUDIT_UNAVAILABLE"
                if not transient or monotonic() >= deadline or attempt == 39:
                    return result
                await asyncio.sleep(0.01)

        tasks = [asyncio.create_task(contender(key)) for key in keys]
        for _ in tasks:
            await ready.get()
        go.set()
        results = await asyncio.gather(*tasks)
        assert sorted(r.status for r in results) == [200, 409], (
            "S17_STALE_PREVIEW concurrent CAS",
            [(r.status, r.body) for r in results],
        )
        winner = next(i for i, r in enumerate(results) if r.status == 200)
        loser = results[1 - winner]
        assert loser.json["error"]["code"] == "E_VERSION_CONFLICT", "S17_STALE_PREVIEW concurrent stale loser"
        async with asyncio.timeout(15):
            while True:
                async with resources.session_factory() as session:
                    pending = (
                        await session.execute(text("SELECT id FROM outbox_entries WHERE status != 'completed'"))
                    ).all()
                if not pending:
                    break
                await asyncio.sleep(0.01)
        before_replay = await domain_snapshot(state)
        for _ in range(10):
            replay = await request(
                state.app,
                endpoint,
                method="POST",
                body=body,
                bearer=state.bearer,
                headers=(("Idempotency-Key", keys[winner]),),
            )
            assert replay.status == 200 and replay.body == results[winner].body, "S17_STALE_PREVIEW same-key replay"
        assert await domain_snapshot(state) == before_replay, "S17_STALE_PREVIEW replay one durable effect"
        assert await rows(state, "SELECT version,lifecycle_state FROM facts WHERE id=:id", {"id": F}) == [
            ("8", "stale")
        ], "S17_STALE_PREVIEW concurrent durable CAS"
        add_original, precommit = HttpRequestRepository.add, []

        async def fail_precommit(self, *args, **kwargs):
            await add_original(self, *args, **kwargs)
            precommit.append(True)
            raise RuntimeError("S17_SECRET_CANARY")

        with monkeypatch.context() as patch:
            patch.setattr(HttpRequestRepository, "add", fail_precommit)
            failed = await request(
                state.app,
                f"/api/v1/profiles/alpha/memories/{A}/transition",
                method="POST",
                bearer=state.bearer,
                body=dict(memory_type="belief", to_state="stale", expected_version=3, confirmation=True, reason="test"),
                headers=(("Idempotency-Key", str(uuid4())),),
            )
        error(failed, 500, "E_INTERNAL", "S17_STALE_PREVIEW precommit failure")
        assert precommit and await domain_snapshot(state) == before_replay, (
            "S17_STALE_PREVIEW precommit rollback no receipt/outbox/journal"
        )
        assert crossed and all(op == "memories.transition" for op in crossed), (
            "S17_STALE_PREVIEW real MutationExecutor.run boundary"
        )
