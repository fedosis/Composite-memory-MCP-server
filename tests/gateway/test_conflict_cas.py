"""S15B2 resolve through the production daemon, Guard, UoW and SQLite."""
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from memory_server.gateway_contracts import dto as d
from tests.gateway.test_http_beliefs import belief_http as belief_http
from tests.gateway.test_http_beliefs import context, error_code, http, ident, orm_seed, set_snapshot, sql
from tests.gateway.test_belief_write_foundation import pause_index_consumer as pause_index_consumer
from tests.gateway.test_slice_runtime import offline_embedder as offline_embedder


@pytest.fixture
async def resolve_http(belief_http):
    state = belief_http
    now = datetime.now(UTC)
    token = state.deps.tokens._bootstrap_create(
        d.TokenCreate(subject="s15b2", profiles=["alpha", "beta"],
                      permissions=["belief.read", "belief.write", "belief.resolve"],
                      expires_at=now + timedelta(hours=1)), now=now)
    state.resolve_bearer = f"cmms_{token.token.id}.{token.secret}"
    await orm_seed(state, [dict(id=ident(1), proposition="PostgreSQL provides atomic database transactions.",
                                confidence=0.8, tags=["a"], source_ids=["declared:a"]),
                           dict(id=ident(2), proposition="SQLite serializes concurrent database writers.",
                                confidence=0.6, tags=["a", "b"], source_ids=["declared:b"])])
    return state


def resolve_payload(action="keep_a", **updates):
    return dict(belief_a_id=ident(1), belief_b_id=ident(2), resolution=action,
                new_proposition="Database transactions provide atomic durable updates." if action == "merge" else None,
                expected_versions={ident(1): 1, ident(2): 1}, reason="  RESOLVE_REASON_CANARY  ",
                confirmation=True) | updates


def conflict_path(a=ident(1), b=ident(2)):
    from memory_server.application.remote_beliefs import encode_conflict_id
    return encode_conflict_id(a, b)


async def resolve(state, payload=None, *, key=None, conflict=None, headers=(), **kwargs):
    return await http(state, f"conflicts/{conflict or conflict_path()}/resolve", method="POST",
                      bearer=state.resolve_bearer, body=resolve_payload() if payload is None else payload,
                      headers=(("Idempotency-Key", str(key or uuid4())), *headers), **kwargs)


@pytest.mark.parametrize("action", ["keep_a", "keep_b", "merge", "mark_contradicted", "discard_both", "defer"])
async def test_conflicts_resolve_keep_merge_discard_semantics(resolve_http, action):
    state = resolve_http
    response = await resolve(state, resolve_payload(action))
    assert response.status == 200, response.body
    assert response.json["resolution"] == action
    assert len(response.json["memories"]) == (3 if action == "merge" else 2)
    for receipt_id in response.json["receipt_ids"]:
        assert await sql(state, "SELECT id FROM receipts WHERE id=:id", {"id": receipt_id}) == [(receipt_id,)]
    assert b"CANARY" not in response.body
