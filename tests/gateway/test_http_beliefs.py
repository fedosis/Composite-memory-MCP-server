"""S15A public-daemon read regression discriminators."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from time import monotonic
from types import SimpleNamespace

import pytest

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import HttpConfig, RequestContext
from memory_server.runtime.daemon_cli import run_daemon
from memory_server.transports.http.app import create_http_app
from tests.gateway.test_http_shell import request
from tests.gateway.test_slice_runtime import build_registry, offline_embedder as offline_embedder

READS = {"beliefs.list", "beliefs.get", "conflicts.list"}
WRITES = {"memories.create", "learn", "memories.transition", "memories.invalidate"}
IDENT = "00000000-0000-4000-8000-000000000001"


@pytest.fixture
async def belief_http(tmp_path):
    registry, roots = await build_registry(tmp_path)
    config = HttpConfig(control_path=tmp_path / "control" / "control.sqlite", rate_burst=200)
    state = SimpleNamespace(config=config, roots=roots, specs=(), mutation_specs=())
    ready, stop = asyncio.Event(), asyncio.Event()

    def factory(config, deps, *, read_routes, mutation_routes):
        state.specs, state.mutation_specs = read_routes, mutation_routes
        state.deps = deps
        state.app = create_http_app(config, deps, read_routes=read_routes, mutation_routes=mutation_routes)
        return state.app

    class Server:
        def __init__(self, config):
            self.started = False

        async def serve(self):
            self.started = True
            ready.set()
            await stop.wait()

    task = asyncio.create_task(run_daemon(config, registry, factory=factory, server_factory=Server))
    waiter = asyncio.create_task(ready.wait())
    try:
        done, _ = await asyncio.wait((task, waiter), timeout=15, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            await task
        assert waiter in done, "daemon did not compose"
        now = datetime.now(UTC)
        created = state.deps.tokens._bootstrap_create(
            d.TokenCreate(subject="s15a", profiles=["alpha", "beta"], permissions=["belief.read", "memory.read"],
                          expires_at=now + timedelta(hours=1)), now=now,
        )
        state.bearer = f"cmms_{created.token.id}.{created.secret}"
        state.principal = await state.deps.tokens.authenticate(state.bearer, now=now)
        yield state
    finally:
        stop.set()
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        assert await asyncio.wait_for(task, 20) == 0


def context(state, profile="alpha"):
    return RequestContext("seed-s15a", state.principal, profile, None, monotonic() + 30)


async def native_seed(state, profile, proposition):
    from memory_server.application.belief_service import BeliefService
    from memory_server.application.contracts import SetBeliefCommand

    async with state.deps.leases.acquire(context(state, profile)) as scope:
        provider = scope.application.read_resources().provider
        return await BeliefService(provider).set_belief(SetBeliefCommand(proposition=proposition))


async def test_beliefs_get_by_id_is_not_text_search(belief_http):
    state = belief_http
    seeded = await native_seed(state, "alpha", "S15A_ALPHA_PROPOSITION_CANARY")
    ident = seeded["belief"]["id"]
    # Explicit ORM fixture: equal UUID in separate stores, divergent versions/content.
    from sqlalchemy import text
    from storage.models.belief import BeliefORM

    async with state.deps.leases.acquire(context(state, "beta")) as scope:
        async with scope.application.read_resources().session_factory() as session, session.begin():
            session.add(BeliefORM(id=ident, proposition="S15A_BETA_CANARY", confidence=0.7, source="test",
                                 creator="test", version=9))
    async with state.deps.leases.acquire(context(state)) as scope:
        async with scope.application.read_resources().session_factory() as session, session.begin():
            await session.execute(text("UPDATE beliefs SET version=3 WHERE id=:id"), {"id": ident})
    response = await request(state.app, f"/api/v1/profiles/alpha/beliefs/{ident}", bearer=state.bearer)
    assert response.status == 200, response.body
    assert response.json["id"] == ident
    assert response.json["revision"] == response.json["source_version"] == 3
    assert response.headers["etag"] == f'"m:belief:{ident}:3"'
    assert response.json["masked"] and response.json["payload"] is None
    foreign = await request(state.app, f"/api/v1/profiles/beta/beliefs/{ident}", bearer=state.bearer)
    assert foreign.status == 200 and foreign.json["revision"] == 9


async def test_belief_composition_regression_counts(belief_http):
    state = belief_http
    assert (len(state.specs), len(state.mutation_specs), len(state.specs) + len(state.mutation_specs)) == (24, 4, 28)
    reads = {spec.operation for spec in state.specs}
    assert READS <= reads
    assert {spec.operation for spec in state.mutation_specs} == WRITES
    assert set(state.deps.ports) == reads | WRITES
    assert "beliefs.set" not in state.deps.ports and "conflicts.resolve" not in state.deps.ports
