"""Profile isolation, cursor replay, high water and HTTP preflight proofs."""

import json
import os
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text

from memory_server.application.memory_read_service import b64, canonical, unb64
from memory_server.transports.http.app import create_http_app
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.routes.memories_read import CursorCodec
from tests.gateway.test_http_shell import code, rows
from tests.gateway.test_http_shell import shell as shell
from tests.gateway.test_memory_reads import IDS, STAMP, C, seed
from tests.gateway.test_memory_reads import reads as reads


async def test_t3_two_real_roots_same_uuid_all_routes(reads):
    r = reads
    ident = IDS["fact"]
    foreign = str(uuid4())
    await seed(r.providers["beta"], ident=foreign, marker="FOREIGN_ONLY")
    assert r.providers["alpha"]._url != r.providers["beta"]._url
    for path, query in [
        ("memories", {}),
        ("memories/" + ident, {}),
        ("memories/" + ident + "/receipt", {}),
        ("memories/" + ident + "/history", {}),
        ("lookup", dict(kind="memory", id=ident)),
    ]:
        response = await r.call(path, query=query)
        assert response.status == 200, response.body
        assert b"FOREIGN_ONLY" not in response.body and foreign.encode() not in response.body
        assert C.encode() not in response.body
    for profile in ("alpha", "beta"):
        response = await r.call(
            "memories/" + ident + "/reveal",
            profile=profile,
            method="POST",
            bearer=r.revealer,
            body=dict(reason="inspection", memory_type="fact", expected_revision=1),
        )
        assert response.status == 200, response.body
        assert C + profile + "_fact_object" in response.body.decode()
        assert C + ("beta" if profile == "alpha" else "alpha") + "_fact_object" not in response.body.decode()
    for path, query in [
        ("memories/" + foreign, {}),
        ("memories/" + foreign + "/receipt", {}),
        ("memories/" + foreign + "/history", {}),
        ("lookup", dict(kind="memory", id=foreign)),
        ("lookup", dict(kind="receipt", id=foreign)),
    ]:
        code(await r.call(path, query=query), "E_NOT_FOUND", 404)
    code(
        await r.call(
            "memories/" + foreign + "/reveal",
            method="POST",
            bearer=r.revealer,
            body=dict(reason="inspection", memory_type="fact", expected_revision=1),
        ),
        "E_NOT_FOUND",
        404,
    )


async def test_t3_t14_cross_type_collision_no_arbitrary_winner(reads):
    r = reads
    ident = IDS["fact"]
    await seed(r.providers["alpha"], "decision", ident=ident, marker="collision", receipt=False)
    for path in ("memories/" + ident, "memories/" + ident + "/receipt", "memories/" + ident + "/history"):
        code(await r.call(path), "E_VALIDATION", 422)
    code(await r.call("lookup", query=dict(kind="memory", id=ident)), "E_VALIDATION", 422)
    page = await r.call()
    assert page.status == 200
    assert {(m["memory_type"], m["id"]) for m in page.json["items"] if m["id"] == ident} == {
        ("fact", ident),
        ("decision", ident),
    }
    for kind in ("fact", "decision"):
        revealed = await r.call(
            "memories/" + ident + "/reveal",
            method="POST",
            bearer=r.revealer,
            body=dict(reason="inspection", memory_type=kind, expected_revision=1 if kind == "fact" else None),
        )
        assert revealed.status == 200 and revealed.json["memory_type"] == kind, revealed.body


@pytest.mark.parametrize(
    "drift", ["principal", "profile", "operation", "filters", "tamper", "limit", "expired", "expired_wrong_binding"]
)
async def test_t4_cursor_rejection_before_port_session_audit(reads, drift):
    r = reads
    issued = await r.call(query={"limit": 1})
    assert issued.status == 200 and issued.json["next_cursor"], issued.body
    token = issued.json["next_cursor"]
    query = {"limit": 1, "cursor": token}
    path = "memories"
    profile = "alpha"
    bearer = r.reader
    expected = "E_CURSOR_INVALID"
    if drift in ("principal", "expired_wrong_binding"):
        bearer, _ = r.mint(subject="another", permissions=("memory.read",))
    if drift == "profile":
        profile = "beta"
    if drift == "operation":
        path = "memories/" + IDS["fact"] + "/history"
    if drift == "filters":
        query["memory_types"] = "fact"
    if drift == "tamper":
        query["cursor"] = token[:-2] + ("AA" if token[-2:] != "AA" else "BB")
    if drift == "limit":
        query["limit"] = 2
    if drift in ("expired", "expired_wrong_binding"):
        now = r.codec.clock()
        r.codec.clock = lambda: now + timedelta(seconds=901)
        expected = "E_CURSOR_EXPIRED" if drift == "expired" else "E_CURSOR_INVALID"
    before = (len(r.sessions), len(rows(r)))
    original = r.port.execute
    calls = []

    async def spy(*a, **kw):
        calls.append(1)
        return await original(*a, **kw)

    r.port.execute = spy
    code(
        await r.call(path, profile=profile, bearer=bearer, query=query),
        expected,
        410 if expected == "E_CURSOR_EXPIRED" else 400,
    )
    assert (len(r.sessions), len(rows(r))) == before and calls == []


async def test_t4_fixed_upper_backdated_allowed_and_continuation(reads):
    r = reads
    first = await r.call(query={"limit": 1})
    assert first.status == 200, first.body
    token = first.json["next_cursor"]
    backdated = str(uuid4())
    future = str(uuid4())
    # First key is belief; an equal-time fact sorts between it and skill upper.
    await seed(r.providers["alpha"], ident=backdated, marker="backdated")
    await seed(r.providers["alpha"], ident=future, marker="future", at=STAMP + timedelta(days=1))
    found = [(m["memory_type"], m["id"]) for m in first.json["items"]]
    for _ in range(10):
        if token is None:
            break
        page = await r.call(query={"limit": 1, "cursor": token})
        assert page.status == 200, page.body
        found.extend((m["memory_type"], m["id"]) for m in page.json["items"])
        token = page.json["next_cursor"]
    assert token is None and len(found) == 5 and len(set(found)) == 5
    assert ("fact", backdated) in found and ("fact", future) not in found


async def test_t4_history_cross_item_and_sql_continuation(reads):
    r = reads
    ident = IDS["fact"]
    async with r.providers["alpha"]._session_factory() as s:
        async with s.begin():
            await s.execute(
                text("UPDATE receipts SET history=:h WHERE id=:id"),
                {
                    "id": ident,
                    "h": json.dumps([dict(timestamp=(STAMP + timedelta(seconds=i)).isoformat()) for i in range(5)]),
                },
            )
    first = await r.call("memories/" + ident + "/history", query={"limit": 2})
    assert first.status == 200 and first.json["has_more"], first.body
    before = (len(r.sessions), len(rows(r)))
    code(
        await r.call("memories/" + IDS["belief"] + "/history", query={"limit": 2, "cursor": first.json["next_cursor"]}),
        "E_CURSOR_INVALID",
        400,
    )
    assert before == (len(r.sessions), len(rows(r)))
    ids = [x["id"] for x in first.json["items"]]
    token = first.json["next_cursor"]
    for _ in range(6):
        if token is None:
            break
        page = await r.call("memories/" + ident + "/history", query={"limit": 1, "cursor": token})
        assert page.status == 200, page.body
        ids += [x["id"] for x in page.json["items"]]
        token = page.json["next_cursor"]
    assert token is None and ids == [ident + ":" + str(i) for i in range(5)]


def test_t4_independent_key_custody(tmp_path):
    path = tmp_path / "memory.key"
    codec = CursorCodec.from_path(path)
    assert len(codec._key) == 32 and path.stat().st_mode & 0o777 == 0o600
    os.chmod(path, 0o644)
    with pytest.raises(HttpError, match="E_STORAGE"):
        CursorCodec.from_path(path)
    path.unlink()
    path.symlink_to(tmp_path / "absent")
    with pytest.raises(HttpError, match="E_STORAGE"):
        CursorCodec.from_path(path)


@pytest.mark.parametrize("mutation", ["extra", "upper", "bool_limit", "noncanonical", "time", "bad_key"])
async def test_t4_authenticated_malformed_cursor_shapes(reads, mutation):
    import hmac

    r = reads
    first = await r.call(query={"limit": 1})
    body, sig = first.json["next_cursor"].split(".")
    data = json.loads(unb64(body))
    if mutation == "extra":
        data["extra"] = 1
    if mutation == "upper":
        data["upper"] = None
    if mutation == "bool_limit":
        data["limit"] = True
    if mutation == "time":
        data["issued_at"] = "bad"
    if mutation == "bad_key":
        data["last"] = ["invalid", "fact", "x"]
    raw = canonical(data) if mutation != "noncanonical" else json.dumps(data, indent=1).encode()
    body = b64(raw)
    token = body + "." + b64(hmac.digest(r.codec._key, body.encode(), "sha256"))
    before = (len(r.sessions), len(rows(r)))
    code(await r.call(query={"limit": 1, "cursor": token}), "E_CURSOR_INVALID", 400)
    assert before == (len(r.sessions), len(rows(r)))


async def test_t9_read_mount_catalog_duplicates_and_absent_port(reads):
    r = reads
    for specs in (
        r.specs + (r.specs[0],),
        (replace(r.specs[0], permission="profile.read"),),
        (replace(r.specs[0], operation="profiles.list"),),
    ):
        with pytest.raises(ValueError):
            create_http_app(r.config, r.app.dependencies, read_routes=specs)
    deps = replace(r.app.dependencies, ports={})
    app = create_http_app(r.config, deps, read_routes=r.specs)
    from tests.gateway.test_http_shell import request

    try:
        before = len(rows(r))
        code(await request(app, "/api/v1/profiles/alpha/memories", bearer=r.reader), "E_CAPABILITY_UNAVAILABLE", 503)
        assert len(rows(r)) == before and r.sessions == []
    finally:
        await app.close()


@pytest.mark.parametrize(
    "path,kwargs",
    [
        ("lookup", {"body": b"x", "query": {"kind": "memory", "id": "id"}}),
        ("memories", {"body": b"x"}),
        ("memories/" + IDS["fact"], {"query": {"x": "y"}}),
        ("memories/" + IDS["fact"] + "/history", {"query": {"limit": ["1", "2"]}}),
    ],
)
async def test_t9_query_body_strict_preflight(reads, path, kwargs):
    before = (len(reads.sessions), len(rows(reads)))
    code(await reads.call(path, **kwargs), "E_VALIDATION", 422)
    assert before == (len(reads.sessions), len(rows(reads)))


async def test_history_ordinal_ties_null_cursor_and_fixed_upper(reads):
    r = reads
    ident = IDS["fact"]
    history = [dict(timestamp=None)] + [dict(timestamp=STAMP.isoformat()) for _ in range(12)]
    async with r.providers["alpha"]._session_factory() as s:
        async with s.begin():
            await s.execute(
                text("UPDATE receipts SET history=:h WHERE id=:id"), {"h": json.dumps(history), "id": ident}
            )
    first = await r.call("memories/" + ident + "/history", query={"limit": 1})
    assert first.status == 200 and first.json["items"][0]["timestamp"] is None, first.body
    # Appended after issue: higher timestamp excluded, older equal-time event
    # after the fixed upper ordinal also excluded (upper is not recomputed).
    history += [dict(timestamp=(STAMP + timedelta(days=1)).isoformat()), dict(timestamp=STAMP.isoformat())]
    async with r.providers["alpha"]._session_factory() as s:
        async with s.begin():
            await s.execute(
                text("UPDATE receipts SET history=:h WHERE id=:id"), {"h": json.dumps(history), "id": ident}
            )
    ids = [v["id"] for v in first.json["items"]]
    token = first.json["next_cursor"]
    for _ in range(15):
        if token is None:
            break
        page = await r.call("memories/" + ident + "/history", query={"limit": 1, "cursor": token})
        assert page.status == 200, page.body
        ids.extend(v["id"] for v in page.json["items"])
        token = page.json["next_cursor"]
    assert token is None and ids == [ident + ":" + str(i) for i in range(13)]
