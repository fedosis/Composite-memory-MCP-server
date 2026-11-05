"""Descriptor/direct adapter evidence, deliberately not HTTP mount evidence."""

import ast
import importlib
import importlib.util
import subprocess
from pathlib import Path

import pytest


def test_s18c2_surface_required():
    names = ("memory_server.application.job_runner", "memory_server.transports.http.routes.jobs")
    assert all(importlib.util.find_spec(name) is not None for name in names), "S18C2_SURFACE"
    runner, routes = (importlib.import_module(name) for name in names)
    composition = importlib.import_module("memory_server.runtime.read_composition")
    assert all(
        callable(getattr(runner.JobCoordinator, name, None)) for name in ("attach", "reconcile_startup", "close")
    ), "S18C2_SURFACE"
    assert callable(routes.create_job_routes) and callable(routes.JobReadAdapter.execute), "S18C2_SURFACE"
    assert callable(getattr(composition, "compose_job_ports", None)), "S18C2_SURFACE"


def request(ident, query=b""):
    from starlette.requests import Request

    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/v1/jobs/" + ident,
            "headers": [],
            "query_string": query,
            "path_params": {"id": ident},
        }
    )


async def test_s18_job_read_route_spec(tmp_path):
    from memory_server.transports.http.guards import ReadRouteSpec
    from memory_server.transports.http.routes.jobs import JobReadAdapter, create_job_routes
    from tests.gateway.test_job_shutdown import harness as fixture
    from tests.gateway.test_job_shutdown import reserve

    gen = fixture.__wrapped__(tmp_path)
    h = await anext(gen)
    try:
        reservation = reserve(h)
        (spec,) = create_job_routes()
        assert type(spec) is ReadRouteSpec, "S18C2_DESCRIPTOR"
        assert (spec.operation, spec.method, spec.path, spec.permission) == (
            "jobs.get",
            "GET",
            "/api/v1/jobs/{id}",
            "index.manage",
        ), "S18C2_DESCRIPTOR"
        command = spec.parse(request(reservation.job_id), b"", h.ctx.principal)
        assert command.profile_id is None and command.path_ids == {"id": reservation.job_id}, "S18C2_DESCRIPTOR"
        job = await JobReadAdapter(h.service).execute(h.ctx, command)
        assert spec.validate_result(job) == await h.service.get(h.ctx, reservation.job_id), "S18C2_DESCRIPTOR"
        from memory_server.control.jobs import JobError
        from memory_server.gateway_contracts.ports import GatewayDependencies, HttpConfig
        from memory_server.runtime.read_composition import compose_job_ports
        from memory_server.transports.http.telemetry import BoundedMetricsSink

        deps = GatewayDependencies(
            h.registry, h.tokens, h.access, BoundedMetricsSink(h.principal.profiles), {}, leases=h.daemon
        )
        wired, descriptors = compose_job_ports(deps, HttpConfig(control_path=h.store.root / "control.sqlite"))
        assert descriptors == (spec,) and wired.ports["jobs.get"].jobs is wired.jobs, "S18C2_WIRING"
        assert await wired.ports["jobs.get"].execute(h.ctx, command) == job, "S18C2_DESCRIPTOR"
        from tests.gateway.test_job_shutdown import payload

        with pytest.raises(JobError, match="E_CAPABILITY_UNAVAILABLE"):
            await wired.jobs.enqueue(h.ctx, "outbox_replay", payload())
    finally:
        await gen.aclose()


async def test_s18c2_descriptor_privacy_and_rejects():
    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.errors import HttpError
    from memory_server.transports.http.routes.jobs import JobReadAdapter, create_job_routes

    (spec,) = create_job_routes()
    for ident, query, body in (
        ("A" * 48, b"", b""),
        ("a" * 47, b"", b""),
        ("a" * 48, b"cursor=PRIVATE", b""),
        ("a" * 48, b"profile=beta", b""),
        ("a" * 48, b"", b"{}"),
    ):
        with pytest.raises(HttpError, match="E_VALIDATION"):
            spec.parse(request(ident, query), body, None)
    calls = []

    class Port:
        async def get(self, ctx, ident):
            calls.append(ident)
            return {"secret": "PRIVATE_C2_CANARY"}

    adapter = JobReadAdapter(Port())
    command = spec.parse(request("a" * 48), b"", None)
    for invalid in (
        object(),
        command.model_copy(update={"profile_id": "alpha"}),
        command.model_copy(update={"path_ids": {"id": "a" * 48, "profile": "beta"}}),
        command.model_copy(update={"payload": {"cursor": "PRIVATE"}}),
    ):
        with pytest.raises(HttpError, match="E_VALIDATION"):
            await adapter.execute(None, invalid)
    assert not calls, "S18C2_PRIVACY"
    with pytest.raises(HttpError, match="E_INTERNAL") as caught:
        await adapter.execute(None, command)
    assert calls == ["a" * 48] and "PRIVATE" not in str(caught.value), "S18C2_PRIVACY"
    with pytest.raises(HttpError, match="E_INTERNAL"):
        spec.validate_result(d.Job.model_construct(id="PRIVATE"))


def test_s18_job_wiring_legacy_identity():
    from memory_server.runtime.read_composition import compose_job_ports

    root = Path(__file__).resolve().parents[2]
    read = "src/memory_server/runtime/read_composition.py"
    cli = "src/memory_server/runtime/daemon_cli.py"

    def base(path):
        return subprocess.check_output(["git", "show", "56896fb:" + path], cwd=root)

    original = base(read)
    candidate = (root / read).read_bytes()
    assert candidate.startswith(original), "S18C2_WIRING"
    added = ast.parse(candidate[len(original) :].decode())
    assert len(added.body) == 1 and added.body[0].name == "compose_job_ports", "S18C2_WIRING"
    assert compose_job_ports.__name__ == "compose_job_ports", "S18C2_WIRING"
    source = (root / cli).read_bytes()
    lines = (b"                compose_job_ports,\n", b"            deps, _ = compose_job_ports(deps, config)\n")
    for line in lines:
        assert source.count(line) == 1, "S18C2_WIRING"
        source = source.replace(line, b"", 1)
    assert source == base(cli), "S18C2_WIRING"
    assert "reconcile_startup" not in source.decode(), "S18C2_WIRING"
