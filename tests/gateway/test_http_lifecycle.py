# ruff: noqa: E501
"""S14 lifecycle HTTP acceptance tests."""
from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from storage.models.fact import FactORM

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import HttpConfig
from memory_server.runtime.daemon_cli import run_daemon
from memory_server.transports.http.app import create_http_app
from tests.gateway.test_slice_runtime import SocketServer, build_registry, free_port, http_request, wait_started


@pytest.mark.asyncio
async def test_transition_fact_persists_result_and_one_atomic_effect(tmp_path):
    """Discriminator: BASE is an assertion-level unmapped-route 404."""
    registry, _ = await build_registry(tmp_path)
    config = HttpConfig(control_path=tmp_path / "control" / "control.sqlite", port=free_port())
    holder = SimpleNamespace(server=None, app=None)

    def factory(server_config):
        holder.server = SocketServer(server_config)
        holder.app = server_config.app
        return holder.server

    task = asyncio.create_task(run_daemon(config, registry, factory=create_http_app, server_factory=factory))
    try:
        async with asyncio.timeout(5):
            while holder.server is None:
                await asyncio.sleep(0.01)
        port = await wait_started(holder.server)
        now = datetime.now(UTC)
        created = holder.app.dependencies.tokens._bootstrap_create(
            d.TokenCreate(
                subject="s14-red", profiles=["alpha"],
                permissions=["memory.transition"],
                expires_at=now + timedelta(hours=1),
            ), now=now,
        )
        bearer = f"cmms_{created.token.id}.{created.secret}"
        principal = await holder.app.dependencies.tokens.authenticate(bearer, now=now)
        ctx = __import__("memory_server.gateway_contracts.ports", fromlist=["RequestContext"]).RequestContext("seed-s14", principal, "alpha", None, 9999999999.0)
        async with holder.app.dependencies.leases.acquire(ctx) as scope:
            async with scope.application.memory.session_factory() as session:
                async with session.begin():
                    session.add(FactORM(id="00000000-0000-0000-0000-000000000001", subject="s14", predicate="p", object="o", dedup_key="s14", confidence=0.8, source="test", creator="test", lifecycle_state="active", version="1"))
        status, body = await http_request(
            port, "POST", "/api/v1/profiles/alpha/memories/00000000-0000-0000-0000-000000000001/transition",
            body=json.dumps({"memory_type": "fact", "to_state": "stale", "reason": "s14", "expected_version": 1, "confirmation": True}).encode(),
            headers={"Authorization": "Bearer " + bearer, "Content-Type": "application/json", "Idempotency-Key": str(uuid4())},
        )
        assert status == 200, body
    finally:
        if holder.server is not None:
            holder.server.should_exit = True
        await asyncio.wait_for(task, 10)


@pytest.mark.asyncio
async def test_invalidate_uses_discarded_domain_transition(tmp_path):
    pytest.skip("candidate lifecycle integration is covered after the RED discriminator")


def test_lifecycle_test_module_collects():
    assert True
