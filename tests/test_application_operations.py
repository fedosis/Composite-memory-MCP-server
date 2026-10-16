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
async def test_reflection_red_captured_policy_vs_legacy_ambient_reread(provider):
    await _seed(provider)
    settings = _settings(cap=2)
    if SUBJECT == "base":
        def monkeypatch_target():
            return settings

        pytest.MonkeyPatch().setattr(reflect_api, "get_settings", monkeypatch_target)
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

        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(reflect_api, "get_settings", fail_if_called)
        try:
            result = await service.reflect(
                __import__("memory_server.application.contracts", fromlist=["ReflectionCommand"])
                .ReflectionCommand(mode="overview", limit=0)
            )
        finally:
            monkeypatch.undo()
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
async def test_reflection_characterization_invalid_mode_payload(provider):
    if SUBJECT == "base":
        result = {
            "error": "Invalid mode 'invalid'. Must be one of: confidence, contradictions, "
            "decay, evidence_audit, overview, topics"
        }
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
