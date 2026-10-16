"""Slice A reflection application-boundary tests."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import memory_server
from memory_server.api import reflect as reflect_api
from memory_server.api.reflect import ReflectEngine
from memory_server.models import Belief, Evidence
from memory_server.providers.sqlite_provider import SQLiteProvider

SUBJECT = os.environ.get("C09_SUBJECT")
if SUBJECT not in {"base", "candidate"}:
    raise RuntimeError("C09_SUBJECT must be exactly base or candidate")
SUBJECT_ROOT = Path(__file__).resolve().parents[1]
ORACLE = Path(os.environ["C09_ORACLE"])
SCRATCH = Path(os.environ["C09_SCRATCH"]).resolve()


def _assert_import_isolation(*modules: object) -> None:
    roots = (SUBJECT_ROOT.resolve(),)
    for module in (memory_server, reflect_api, *modules):
        origin = getattr(module, "__file__", None)
        assert origin, f"missing module origin for {module!r}"
        resolved = Path(origin).resolve()
        assert any(resolved == root or root in resolved.parents for root in roots), (
            f"source isolation violation: {module!r} resolved to {resolved}, "
            f"subject root is {SUBJECT_ROOT}"
        )


_assert_import_isolation()

_FIXED_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
_BASE_RESULTS: dict[str, object] = {}


def _settings(cap: int = 10000) -> SimpleNamespace:
    return SimpleNamespace(
        reflect_min_confidence=0.0,
        reflect_limit=50,
        reflect_belief_cap=cap,
        ttl_days={
            "fact": 90.0,
            "decision": 180.0,
            "skill": 365.0,
            "entity": 365.0,
            "belief": 180.0,
        },
        decay_stale_ratio=0.7,
        decay_forgotten_ratio=2.0,
        decay_default_ttl_days=90.0,
        decay_forecast_window_days=7.0,
    )


def _normalize(value: object, key: str = "") -> object:
    if isinstance(value, dict):
        return {k: _normalize(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize(v, key) for v in value]
    if isinstance(value, str) and (key.endswith("_at") or key in {"created", "updated"}):
        return "<TIMESTAMP>"
    return value


def _record(name: str, value: object) -> None:
    normalized = _normalize(value)
    if SUBJECT == "base":
        _BASE_RESULTS[name] = normalized
        return
    assert ORACLE.is_file(), f"base oracle missing: {ORACLE}"
    oracle = json.loads(ORACLE.read_text())
    assert oracle[name] == normalized, f"oracle mismatch for {name}"


def _freeze_base_oracle() -> None:
    if SUBJECT == "base":
        expected = {"six_modes", "relations", "invalid_mode", "composition"}
        assert set(_BASE_RESULTS) == expected
        ORACLE.parent.mkdir(parents=True, exist_ok=True)
        ORACLE.write_text(json.dumps(_BASE_RESULTS, indent=2, sort_keys=True) + "\n")
    else:
        assert ORACLE.is_file(), f"base oracle missing: {ORACLE}"


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return _FIXED_NOW if tz is not None else _FIXED_NOW.replace(tzinfo=None)


@pytest.fixture
def fixed_reflection_clock(monkeypatch):
    monkeypatch.setattr(reflect_api, "datetime", _FixedDateTime)
    return _FIXED_NOW


@pytest.fixture
async def provider(request):
    db = SCRATCH / "db" / SUBJECT / f"{request.node.name}.sqlite"
    db.parent.mkdir(parents=True, exist_ok=True)
    result = SQLiteProvider(url=f"sqlite+aiosqlite:///{db}")
    await result.initialize()
    try:
        yield result
    finally:
        await result.close()


async def _seed(provider: SQLiteProvider) -> None:
    created = datetime(2025, 12, 1, tzinfo=timezone.utc)
    beliefs = [
        Belief(
            id="belief-1",
            proposition="Docker is better than Podman",
            confidence=0.8,
            tags=["docker", "container"],
            created_at=created,
            updated_at=created,
            last_reinforced_at=created,
        ),
        Belief(
            id="belief-2",
            proposition="Docker is worse than Podman",
            confidence=0.6,
            tags=["podman", "container"],
            created_at=created,
            updated_at=created,
            last_reinforced_at=created,
        ),
        Belief(
            id="belief-3",
            proposition="Caddy is a fast web server",
            confidence=0.85,
            tags=["caddy", "web"],
            created_at=created,
            updated_at=created,
            last_reinforced_at=created,
        ),
        Belief(
            id="belief-4",
            proposition="User prefers dark mode",
            confidence=0.7,
            tags=["user-preference"],
            created_at=created,
            updated_at=created,
            last_reinforced_at=created,
        ),
        Belief(
            id="belief-5",
            proposition="Deprecated belief",
            confidence=0.3,
            tags=["old"],
            lifecycle_state="superseded",
            created_at=created,
            updated_at=created,
            last_reinforced_at=created,
        ),
    ]
    for belief in beliefs:
        await provider.create_belief(
            belief,
            [
                Evidence(
                    belief_id=belief.id,
                    source_type="fact",
                    source_id=f"source-{belief.id}",
                    weight=0.9,
                )
            ],
        )


def _candidate_service(provider, settings=None):
    from memory_server.application.contracts import ReflectionPolicy
    from memory_server.application.reflection_service import ReflectionService

    settings = settings or _settings()
    policy = ReflectionPolicy.capture(settings)
    return ReflectionService(provider, policy=policy), policy


@pytest.mark.asyncio
async def test_reflection_red_captured_policy_vs_legacy_ambient_reread(provider, monkeypatch):
    await _seed(provider)
    settings = _settings(cap=2)
    if SUBJECT == "base":
        def monkeypatch_target():
            return settings

        monkeypatch.setattr(reflect_api, "get_settings", monkeypatch_target)
        engine = ReflectEngine(provider)
        settings.reflect_belief_cap = 1
        result = await engine.overview(limit=0)
        assert result["total_beliefs"] == 2, (
            "captured-policy boundary expected 2; legacy api/reflect.py:192-198 "
            f"re-read the ambient cap and observed {result['total_beliefs']}"
        )
    else:
        service, _policy = _candidate_service(provider, settings)
        settings.reflect_belief_cap = 1

        def fail_if_called():
            raise AssertionError("explicit reflection policy called api get_settings")

        monkeypatch.setattr(reflect_api, "get_settings", fail_if_called)
        result = await service.reflect(
            __import__("memory_server.application.contracts", fromlist=["ReflectionCommand"])
            .ReflectionCommand(mode="overview", limit=0)
        )
        assert result["total_beliefs"] == 2


@pytest.mark.asyncio
async def test_reflection_characterization_six_modes(provider, fixed_reflection_clock):
    await _seed(provider)
    settings = _settings()
    if SUBJECT == "base":
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(reflect_api, "get_settings", lambda: settings)
        try:
            engine = ReflectEngine(provider)
            result = {
                mode: await getattr(engine, method)()
                for mode, method in (
                    ("overview", "overview"),
                    ("contradictions", "contradictions"),
                    ("decay", "decay_analysis"),
                    ("topics", "topics"),
                    ("evidence_audit", "evidence_audit"),
                    ("confidence", "confidence_histogram"),
                )
            }
        finally:
            monkeypatch.undo()
    else:
        service, _policy = _candidate_service(provider, settings)
        from memory_server.application.contracts import ReflectionCommand

        result = {}
        for mode in ("overview", "contradictions", "decay", "topics", "evidence_audit", "confidence"):
            result[mode] = await service.reflect(ReflectionCommand(mode=mode))
    assert set(result) == {"overview", "contradictions", "decay", "topics", "evidence_audit", "confidence"}
    assert "relations" not in result
    _record("six_modes", result)


@pytest.mark.asyncio
async def test_reflection_characterization_relations_direct_method(provider, fixed_reflection_clock):
    await _seed(provider)
    settings = _settings()
    if SUBJECT == "base":
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(reflect_api, "get_settings", lambda: settings)
        try:
            result = await ReflectEngine(provider).relations(
                context="container", strict_same_context=True, limit=0
            )
        finally:
            monkeypatch.undo()
    else:
        service, _policy = _candidate_service(provider, settings)
        from memory_server.application.contracts import ReflectionCommand

        result = await service.relations(
            ReflectionCommand(mode="relations", context="container", limit=0)
        )
    assert result["mode"] == "relations"
    _record("relations", result)


@pytest.mark.asyncio
async def test_reflection_characterization_invalid_mode_payload(provider, monkeypatch):
    if SUBJECT == "base":
        from prometheus_client import CollectorRegistry

        from memory_server import server
        from memory_server.evaluation.metrics import MetricsCollector

        _assert_import_isolation(server)
        isolated_collector = MetricsCollector(registry=CollectorRegistry())

        async def scratch_provider():
            return provider

        monkeypatch.setattr(server, "_get_provider", scratch_provider)
        monkeypatch.setattr(server, "get_collector", lambda: isolated_collector)
        result = json.loads(await server.reflect_tool(mode="invalid"))
    else:
        from memory_server.application.contracts import ReflectionCommand

        service, _policy = _candidate_service(provider)
        result = await service.reflect(ReflectionCommand(mode="invalid"))
    assert result == {
        "error": "Invalid mode 'invalid'. Must be one of: confidence, contradictions, "
        "decay, evidence_audit, overview, topics"
    }
    _record("invalid_mode", result)


@pytest.mark.asyncio
async def test_reflection_composition_and_describe(provider):
    from memory_server.application.contracts import MemoryPolicy

    if SUBJECT == "base":
        from memory_server.application.application import MemoryApplication
        from memory_server.application.errors import UnknownDependencyError

        app = MemoryApplication(object(), policy=MemoryPolicy(max_active_beliefs=500))
        assert app.describe() == {"policy": {"max_active_beliefs": 500}}
        with pytest.raises(UnknownDependencyError):
            _ = app.retrieval
        result = app.describe()
    else:
        from memory_server.application.application import MemoryApplication
        from memory_server.application.contracts import ReflectionPolicy

        policy = ReflectionPolicy.capture(_settings())
        app = MemoryApplication(
            object(),
            policy=MemoryPolicy(max_active_beliefs=500),
            reflection_provider=provider,
            reflection_policy=policy,
        )
        reflection_service = __import__(
            "memory_server.application.reflection_service",
            fromlist=["ReflectionService"],
        )
        _assert_import_isolation(reflection_service)
        assert app.describe() == {"policy": {"max_active_beliefs": 500}}
        assert app.reflection.policy == policy
        result = app.describe()
    _record("composition", result)
    if SUBJECT == "base":
        _freeze_base_oracle()
    else:
        _freeze_base_oracle()


B_ORACLE = ORACLE.with_name("application-operations-b.json")


def _stack_modules() -> set[str]:
    import inspect

    return {
        frame.frame.f_globals.get("__name__", "")
        for frame in inspect.stack()
        if frame.frame.f_globals.get("__name__")
    }


def _normalize_b(value: object, key: str = "") -> object:
    import uuid

    if isinstance(value, dict):
        return {k: _normalize_b(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_b(v, key) for v in value]
    if key == "id" and isinstance(value, str):
        try:
            uuid.UUID(value)
        except ValueError:
            return value
        return "<UUID>"
    if key == "timestamp" and isinstance(value, str):
        return "<TIMESTAMP>"
    return _normalize(value, key)


def _record_b(name: str, value: object) -> None:
    normalized = _normalize_b(value)
    if SUBJECT == "base":
        existing = json.loads(B_ORACLE.read_text()) if B_ORACLE.is_file() else {}
        existing[name] = normalized
        B_ORACLE.parent.mkdir(parents=True, exist_ok=True)
        B_ORACLE.write_text(json.dumps(existing, indent=2, sort_keys=True) + "\n")
        return
    assert B_ORACLE.is_file(), f"base B oracle missing: {B_ORACLE}"
    assert json.loads(B_ORACLE.read_text())[name] == normalized, (
        f"B oracle mismatch for {name}"
    )


def _relation_graph():
    from memory_server.providers.graph_provider import SimpleGraph

    graph = SimpleGraph()
    graph.add_node(id="relation-source", type="fact", name="Source")
    graph.add_node(id="relation-target", type="fact", name="Target")
    return graph


def _application_for_b(provider, graph):
    from memory_server.application.application import MemoryApplication

    async def get_graph():
        return graph

    return MemoryApplication(
        object(),
        get_graph=get_graph,
        relation_provider=provider,
        lifecycle_provider=provider,
    )


def _legacy_payload(result):
    return {
        "memory": result.memory.model_dump(mode="json"),
        "event": result.event,
        "propagated": result.propagated,
    }


@pytest.mark.asyncio
async def test_relation_lifecycle_red_direct_application_boundary_after_real_base_operations(
    provider, monkeypatch
):
    from storage.repositories import LifecycleRepository

    from memory_server.models import Fact
    from memory_server.providers.graph_provider import SimpleGraph
    from memory_server.services.lifecycle_service import LifecycleService as DomainLifecycleService

    graph = _relation_graph()
    relation_modules: list[set[str]] = []
    lifecycle_modules: list[set[str]] = []
    original_add_edge = SimpleGraph.add_edge
    original_transition = DomainLifecycleService.transition

    def probed_add_edge(self, *args, **kwargs):
        relation_modules.append(_stack_modules())
        return original_add_edge(self, *args, **kwargs)

    async def probed_transition(self, *args, **kwargs):
        lifecycle_modules.append(_stack_modules())
        return await original_transition(self, *args, **kwargs)

    monkeypatch.setattr(SimpleGraph, "add_edge", probed_add_edge)
    monkeypatch.setattr(DomainLifecycleService, "transition", probed_transition)

    fact = Fact(
        id="red-lifecycle-fact",
        subject="Red",
        predicate="proves",
        object="Boundary",
    )
    await provider.create_fact(fact)

    if SUBJECT == "base":
        from memory_server.api.add_relation import add_relation

        relation_result = await add_relation(
            graph, "relation-source", "relation-target", " SUPPORTS ", provider
        )
        lifecycle_result = await DomainLifecycleService(provider).transition(
            fact.id, "fact", "stale", expected_version=1
        )
    else:
        from memory_server.application.contracts import LifecycleCommand, RelationCommand

        app = _application_for_b(provider, graph)
        relation_result = await app.relations.add_relation(
            RelationCommand("relation-source", "relation-target", " SUPPORTS ")
        )
        lifecycle_result = await app.lifecycle.transition(
            LifecycleCommand(fact.id, "fact", "stale", expected_version=1)
        )

    assert relation_result["edge"]["relation"] == "supports"
    assert graph.get_edge("relation-source", "relation-target") is not None
    assert "relation-source" in await provider.get_claim_dependents(
        "relation-target", relation_type="supports"
    )
    lifecycle_memory = (
        lifecycle_result["memory"]
        if SUBJECT == "candidate"
        else lifecycle_result.memory.model_dump(mode="json")
    )
    assert lifecycle_memory["lifecycle_state"] == "stale"
    current = await provider.get_fact(fact.id)
    assert current is not None and current.lifecycle_state == "stale" and current.version == 2
    async with await provider._get_session() as session:
        events = await LifecycleRepository(session).get_events(fact.id)
    assert len(events) == 1

    assert relation_modules and lifecycle_modules
    observed_relation = set().union(*relation_modules)
    observed_lifecycle = set().union(*lifecycle_modules)
    assert "memory_server.application.relation_service" in observed_relation, (
        "missing production-derived relation-service caller provenance after real "
        f"SQL/graph operation; observed={sorted(observed_relation)}"
    )
    assert "memory_server.application.lifecycle_service" in observed_lifecycle, (
        "missing production-derived lifecycle-service caller provenance after real "
        f"SQL/event operation; observed={sorted(observed_lifecycle)}"
    )


@pytest.mark.asyncio
async def test_relation_characterization_result_sql_and_graph_parity(provider, monkeypatch):
    graph = _relation_graph()
    ordering: list[str] = []
    original_create = provider.create_in_transaction
    original_search = graph.search_by_relation
    original_add = graph.add_edge

    async def probed_create(*args, **kwargs):
        ordering.append("sql")
        return await original_create(*args, **kwargs)

    def probed_search(*args, **kwargs):
        ordering.append("graph_search")
        return original_search(*args, **kwargs)

    def probed_add(*args, **kwargs):
        ordering.append("graph_add")
        return original_add(*args, **kwargs)

    monkeypatch.setattr(provider, "create_in_transaction", probed_create)
    monkeypatch.setattr(graph, "search_by_relation", probed_search)
    monkeypatch.setattr(graph, "add_edge", probed_add)
    if SUBJECT == "base":
        from memory_server.api.add_relation import add_relation

        first = await add_relation(
            graph, "relation-source", "relation-target", " SUPPORTS ", provider
        )
        second = await add_relation(
            graph, "relation-source", "relation-target", "supports", provider
        )
    else:
        from memory_server.application.contracts import RelationCommand

        service = _application_for_b(provider, graph).relations
        command = RelationCommand("relation-source", "relation-target", " SUPPORTS ")
        first = await service.add_relation(command)
        second = await service.add_relation(
            RelationCommand("relation-source", "relation-target", "supports")
        )
    _record_b(
        "relation_result_sql_graph",
        {
            "first": first,
            "second": second,
            "ordering": ordering[:3],
            "edge_count": len(graph.search_by_relation("supports")),
            "dependents": await provider.get_claim_dependents(
                "relation-target", relation_type="supports"
            ),
        },
    )
    assert ordering[:3] == ["sql", "graph_search", "graph_add"]


@pytest.mark.asyncio
async def test_relation_characterization_validation_order_and_idempotence(provider):
    graph = _relation_graph()
    if SUBJECT == "base":
        from memory_server.api.add_relation import add_relation as call
    else:
        from memory_server.application.contracts import RelationCommand

        service = _application_for_b(provider, graph).relations

        async def call(graph, source_id, target_id, relation_type, provider):
            return await service.add_relation(RelationCommand(source_id, target_id, relation_type))

    errors = []
    for args in (
        ("relation-source", "relation-target", "invalid"),
        ("missing-source", "relation-target", "supports"),
        ("relation-source", "missing-target", "supports"),
    ):
        with pytest.raises((ValueError, KeyError)) as raised:
            await call(graph, *args, provider)
        errors.append({"type": type(raised.value).__name__, "message": raised.value.args[0]})
    assert errors == [
        {
            "type": "ValueError",
            "message": "relation_type must be one of: supports, contradicts, derives",
        },
        {"type": "KeyError", "message": "Source node 'missing-source' not found"},
        {"type": "KeyError", "message": "Target node 'missing-target' not found"},
    ]
    _record_b("relation_validation", errors)


@pytest.mark.asyncio
async def test_lifecycle_characterization_cas_event_and_propagation(provider):
    from storage.repositories import LifecycleRepository

    from memory_server.models import Fact

    parent = Fact(id="lifecycle-parent", subject="Parent", predicate="is", object="valid")
    child = Fact(id="lifecycle-child", subject="Child", predicate="depends", object="Parent", confidence=0.5)
    await provider.create_fact(parent)
    await provider.create_fact(child)
    await provider.create_in_transaction(
        relation_entries=[
            {"source_id": child.id, "target_id": parent.id, "relation_type": "derived_from"}
        ]
    )
    graph = _relation_graph()
    graph.add_node(id=parent.id, type="fact", name="parent")
    graph.add_node(id=child.id, type="fact", name="child")
    graph.add_edge(source_id=child.id, target_id=parent.id, relation="derived_from")

    if SUBJECT == "base":
        from memory_server.services.lifecycle_service import LifecycleService as DomainLifecycleService

        result = _legacy_payload(
            await DomainLifecycleService(provider).transition(
                parent.id, "fact", "discarded", expected_version=1, graph=graph
            )
        )
    else:
        from memory_server.application.contracts import LifecycleCommand

        result = await _application_for_b(provider, graph).lifecycle.transition(
            LifecycleCommand(parent.id, "fact", "discarded", expected_version=1)
        )
    updated_child = await provider.get_fact(child.id)
    assert updated_child is not None
    async with await provider._get_session() as session:
        events = await LifecycleRepository(session).get_events(parent.id)
        child_events = await LifecycleRepository(session).get_events(child.id)
    with pytest.raises(Exception) as stale:
        if SUBJECT == "base":
            await DomainLifecycleService(provider).transition(
                parent.id, "fact", "stale", expected_version=1, graph=graph
            )
        else:
            await _application_for_b(provider, graph).lifecycle.transition(
                LifecycleCommand(parent.id, "fact", "stale", expected_version=1)
            )
    assert "expected_version mismatch" in str(stale.value)
    _record_b(
        "lifecycle_cas_event_propagation",
        {
            "result": result,
            "child": updated_child.model_dump(mode="json"),
            "parent_events": events,
            "child_events": child_events,
            "stale": {"type": type(stale.value).__name__, "message": str(stale.value)},
        },
    )


@pytest.mark.asyncio
async def test_lifecycle_characterization_graph_fallback_and_rollback(provider, monkeypatch):
    from sqlalchemy import text
    from storage.repositories import LifecycleRepository
    from storage.repositories.relation_repo import RelationRepository

    from memory_server.models import Fact

    parent = Fact(id="fallback-parent", subject="Parent", predicate="is", object="valid")
    child = Fact(id="fallback-child", subject="Child", predicate="depends", object="Parent", confidence=0.5)
    await provider.create_fact(parent)
    await provider.create_fact(child)
    graph = _relation_graph()
    graph.add_node(id=parent.id, type="fact", name="parent")
    graph.add_node(id=child.id, type="fact", name="child")
    graph.add_edge(source_id=child.id, target_id=parent.id, relation="derived_from")

    async def unavailable(self, *args, **kwargs):
        raise RuntimeError("relation SQL unavailable")

    monkeypatch.setattr(RelationRepository, "get_dependents", unavailable)
    if SUBJECT == "base":
        from memory_server.services.lifecycle_service import LifecycleService as DomainLifecycleService

        fallback = _legacy_payload(
            await DomainLifecycleService(provider).transition(
                parent.id, "fact", "discarded", expected_version=1, graph=graph
            )
        )
    else:
        from memory_server.application.contracts import LifecycleCommand

        fallback = await _application_for_b(provider, graph).lifecycle.transition(
            LifecycleCommand(parent.id, "fact", "discarded", expected_version=1)
        )
    assert fallback["propagated"] and fallback["propagated"][0]["memory_id"] == child.id

    rollback_parent = Fact(id="rollback-parent", subject="Parent", predicate="is", object="valid")
    await provider.create_fact(rollback_parent)
    async with await provider._get_session() as session:
        await session.execute(
            text(
                "CREATE TRIGGER reject_c09_lifecycle_event BEFORE INSERT ON lifecycle_events "
                "BEGIN SELECT RAISE(ABORT, 'c09 event rejected'); END"
            )
        )
        await session.commit()
    with pytest.raises(Exception, match="c09 event rejected"):
        if SUBJECT == "base":
            await DomainLifecycleService(provider).transition(
                rollback_parent.id, "fact", "stale", expected_version=1, graph=graph
            )
        else:
            await _application_for_b(provider, graph).lifecycle.transition(
                LifecycleCommand(rollback_parent.id, "fact", "stale", expected_version=1)
            )
    rolled_back = await provider.get_fact(rollback_parent.id)
    assert rolled_back is not None
    async with await provider._get_session() as session:
        rollback_events = await LifecycleRepository(session).get_events(rollback_parent.id)
    assert (rolled_back.lifecycle_state, rolled_back.version, rollback_events) == ("active", 1, [])
    _record_b(
        "lifecycle_fallback_rollback",
        {
            "fallback": fallback,
            "rollback": {
                "state": rolled_back.lifecycle_state,
                "version": rolled_back.version,
                "events": rollback_events,
            },
        },
    )


_C_ORACLE = ORACLE.with_name("c09_c_characterization.json")


def _record_c(name: str, value: object) -> None:
    normalized = _normalize(value)
    if SUBJECT == "base":
        records = json.loads(_C_ORACLE.read_text()) if _C_ORACLE.is_file() else {}
        records[name] = normalized
        _C_ORACLE.parent.mkdir(parents=True, exist_ok=True)
        _C_ORACLE.write_text(json.dumps(records, indent=2, sort_keys=True) + "\n")
    else:
        assert _C_ORACLE.is_file(), f"C base oracle missing: {_C_ORACLE}"
        assert json.loads(_C_ORACLE.read_text())[name] == normalized


async def _audit_fixture(provider):
    from memory_server.evaluation.auditor import collect_persisted_audit_state
    from memory_server.evaluation.confidence import ConfidenceEngine
    from memory_server.evaluation.validator import Validator
    from memory_server.models import Fact
    from memory_server.providers.graph_provider import SimpleGraph

    fact = Fact(id="c-audit-fact", subject="C", predicate="is", object="real")
    await provider.create_fact(fact)
    validator = Validator()
    validator.register(fact.id, confidence=0.8)
    engine = ConfidenceEngine()
    graph = SimpleGraph()
    graph.add_node(id=fact.id, type="fact", name="C")
    persisted = await collect_persisted_audit_state(sqlite=provider)
    return fact, validator, engine, graph, persisted


async def _c_audit(provider, validator, engine, graph, persisted, audit_type="full"):
    if SUBJECT == "base":
        from memory_server.evaluation.auditor import MemoryAuditor

        return MemoryAuditor(
            validator=validator,
            confidence_engine=engine,
            graph=graph,
            sqlite=provider,
            receipt_ids=set(persisted["receipt_ids"]),
            sqlite_counts=dict(persisted["sqlite_counts"]),
            vector_point_count=persisted["vector_point_count"],
            persisted_records=persisted["records"],
        ).audit_report(audit_type)

    from memory_server.application.application import MemoryApplication
    from memory_server.application.contracts import AuditCommand

    async def get_graph():
        return graph

    app = MemoryApplication(
        object(),
        audit_validator=validator,
        audit_confidence_engine=engine,
        audit_sqlite=provider,
        audit_state=persisted,
        get_graph=get_graph,
    )
    return await app.audit.audit(AuditCommand(audit_type=audit_type))


@pytest.mark.asyncio
async def test_audit_red_real_persisted_report_before_borrowed_application_boundary(
    provider, monkeypatch
):
    from memory_server.evaluation.auditor import MemoryAuditor

    fact, validator, engine, graph, persisted = await _audit_fixture(provider)
    calls = []
    original = MemoryAuditor.audit_report

    def probe(self, *args, **kwargs):
        calls.append(__import__("inspect").stack())
        return original(self, *args, **kwargs)

    monkeypatch.setattr(MemoryAuditor, "audit_report", probe)
    report = await _c_audit(provider, validator, engine, graph, persisted)
    assert report["audit_type"] == "full"
    assert calls and any(
        "memory_server.application.audit_service" == frame.frame.f_globals.get("__name__")
        for frame in calls[0]
    )
    assert await provider.get_fact(fact.id) is not None
    assert persisted["sqlite_counts"]["facts"] == 1
    assert report == await _c_audit(provider, validator, engine, graph, persisted)


@pytest.mark.asyncio
async def test_audit_characterization_raw_persisted_full_report(provider):
    fact, validator, engine, graph, persisted = await _audit_fixture(provider)
    report = await _c_audit(provider, validator, engine, graph, persisted)
    assert report["audit_type"] == "full"
    assert report["stats"]
    assert persisted["sqlite_counts"]["facts"] == 1
    assert await provider.get_fact(fact.id) is not None
    _record_c("raw_persisted_full_report", report)


@pytest.mark.asyncio
async def test_audit_characterization_optional_resources_preserve_behavior(provider):
    from memory_server.evaluation.auditor import collect_persisted_audit_state
    from memory_server.evaluation.confidence import ConfidenceEngine
    from memory_server.evaluation.validator import Validator

    persisted = await collect_persisted_audit_state(sqlite=provider)
    report = await _c_audit(provider, Validator(), ConfidenceEngine(), None, persisted)
    assert set(report) == {"audit_type", "warnings", "errors", "stats"}
    assert report["audit_type"] == "full"
    _record_c("optional_resources_preserve_behavior", report)


@pytest.mark.asyncio
async def test_audit_characterization_invalid_type_is_forwarded(provider):
    fact, validator, engine, graph, persisted = await _audit_fixture(provider)
    report = await _c_audit(
        provider, validator, engine, graph, persisted, audit_type="invalid"
    )
    assert report["audit_type"] == "invalid"
    assert report["warnings"] == []
    assert report["errors"] == []
    assert report["stats"] == {}
    assert await provider.get_fact(fact.id) is not None
    _record_c("invalid_type_is_forwarded", report)


@pytest.mark.asyncio
async def test_audit_application_missing_optional_resources_and_composition(provider):
    from memory_server.application.application import MemoryApplication
    from memory_server.application.contracts import MemoryPolicy
    from memory_server.application.errors import UnknownDependencyError

    app = MemoryApplication(object(), policy=MemoryPolicy(max_active_beliefs=500))
    assert app.describe() == {"policy": {"max_active_beliefs": 500}}
    missing = MemoryApplication(object())
    if SUBJECT == "candidate":
        from memory_server.application.contracts import AuditCommand
        from memory_server.evaluation.auditor import collect_persisted_audit_state
        from memory_server.evaluation.confidence import ConfidenceEngine
        from memory_server.evaluation.validator import Validator

        persisted = await collect_persisted_audit_state(sqlite=provider)
        app = MemoryApplication(
            object(),
            policy=MemoryPolicy(max_active_beliefs=500),
            audit_validator=Validator(),
            audit_confidence_engine=ConfidenceEngine(),
            audit_state=persisted,
        )
        result = await app.audit.audit(AuditCommand())
        assert result["audit_type"] == "full"
        with pytest.raises(UnknownDependencyError, match="audit service"):
            _ = missing.audit
    else:
        with pytest.raises(UnknownDependencyError):
            _ = missing.retrieval
