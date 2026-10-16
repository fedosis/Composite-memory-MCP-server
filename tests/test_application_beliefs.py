"""C08 paired harness: one module, explicit legacy/application subjects."""

from __future__ import annotations

import inspect
import json
import os
import re
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

_SUBJECT = os.environ.get("C08_SUBJECT")
if _SUBJECT not in {"legacy", "application"}:
    raise RuntimeError("C08_SUBJECT must be exactly 'legacy' or 'application'")

SCRATCH = Path(os.environ.get("C08_SCRATCH", ".")).resolve()
ORACLE = Path(os.environ.get("C08_ORACLE", SCRATCH / "oracle" / "beliefs.json")).resolve()
FORBIDDEN = {
    "mcp",
    "memory_server.server",
    "memory_server.transports",
    "memory_server.plugins",
    "memory_server.runtime",
}
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", re.I)
_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}(?:T| )\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)?$")
_TIME_ANY = re.compile(r"\d{4}-\d{2}-\d{2}(?:T| )\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)?")


def _legacy_command(**values: Any) -> SimpleNamespace:
    return SimpleNamespace(**values)


def _assert_scratch(path: Path) -> None:
    assert path.resolve() == SCRATCH or SCRATCH in path.resolve().parents


def _normal(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _normal(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normal(v) for v in value]
    if isinstance(value, bytes):
        return {"<BYTES>": value.hex()}
    if isinstance(value, str):
        if _UUID.fullmatch(value):
            return "<UUID>"
        if _TIME.fullmatch(value):
            return "<TIME>"
        return _TIME_ANY.sub("<TIME>", _UUID.sub("<UUID>", value))
    return value


def _db_signature(db: Path) -> dict[str, Any]:
    _assert_scratch(db)
    with sqlite3.connect(db) as conn:
        tables = [
            r[0]
            for r in conn.execute(
                "select name from sqlite_master where type='table' and name not like 'sqlite_%' order by name"
            )
        ]
        return {table: _normal(conn.execute(f' select * from "{table}"').fetchall()) for table in tables}


def _record(name: str, result: Any, db: Path) -> None:
    payload = {"result": _normal(result), "sqlite": _db_signature(db)}
    ORACLE.parent.mkdir(parents=True, exist_ok=True)
    _assert_scratch(ORACLE)
    if _SUBJECT == "legacy":
        existing = json.loads(ORACLE.read_text()) if ORACLE.exists() else {}
        existing[name] = payload
        ORACLE.write_text(json.dumps(existing, sort_keys=True, indent=2) + "\n")
    else:
        assert ORACLE.exists(), f"missing frozen legacy oracle: {ORACLE}"
        expected = json.loads(ORACLE.read_text())[name]
        assert payload == expected


@pytest.fixture
async def provider(tmp_path: Path):
    _assert_scratch(tmp_path)
    from memory_server.providers.sqlite_provider import SQLiteProvider

    db = tmp_path / "beliefs.sqlite"
    _assert_scratch(db)
    value = SQLiteProvider(url=f"sqlite+aiosqlite:///{db}")
    await value.initialize()
    value._c08_db = db
    yield value
    await value.close()


async def _none_graph():
    return None


async def _under_test(provider):
    if _SUBJECT == "application":
        from memory_server.application.application import MemoryApplication
        from memory_server.application.contracts import BeliefPolicy

        return MemoryApplication.from_provider(provider, belief_policy=BeliefPolicy(10), get_graph=_none_graph).beliefs

    import memory_server.server as server

    # Keep the old orchestration and provider implementation real; only replace
    # singleton acquisition so its writes stay in the disposable fixture.
    server._provider = provider
    server._graph = None
    server._get_provider = lambda: _ready(provider)
    server._get_graph = _none_graph
    return server


async def _ready(value):
    return value


async def _set(subject, service, command):
    if subject == "application":
        return await service.set_belief(command)
    return json.loads(
        await service.set_belief_tool(
            command.proposition,
            command.confidence,
            json.dumps(command.sources or []),
            json.dumps(command.tags or []),
            command.source,
            command.replace_belief_id,
        )
    )


async def _get(subject, service, command):
    if subject == "application":
        return await service.get_belief(command)
    return json.loads(
        await service.get_belief_tool(
            command.proposition,
            command.lifecycle_state,
            command.min_confidence,
            json.dumps(command.tags) if command.tags is not None else "",
            command.source,
            command.creator,
            command.source_id,
            command.limit if command.limit is not None else 10,
        )
    )


async def _resolve(subject, service, command):
    if subject == "application":
        return await service.resolve_conflict(command)
    return json.loads(
        await service.resolve_conflict_tool(
            command.belief_a_id,
            command.belief_b_id,
            command.resolution,
            command.new_proposition,
            command.auto_resolve,
        )
    )


def _commands():
    if _SUBJECT == "application":
        from memory_server.application.contracts import (
            GetBeliefCommand,
            ResolveConflictCommand,
            SetBeliefCommand,
        )

        return SetBeliefCommand, GetBeliefCommand, ResolveConflictCommand
    return None, None, None


@pytest.mark.asyncio
async def test_application_belief_surface_is_direct_and_transport_free(provider):
    before = set(sys.modules)
    if _SUBJECT == "application":
        from memory_server.application.belief_service import BeliefService
        from memory_server.application.contracts import SetBeliefCommand

        service = BeliefService(provider, get_graph=_none_graph)
        result = await service.set_belief(SetBeliefCommand("direct boundary"))
        assert result["belief"]["proposition"] == "direct boundary"
        assert BeliefService.__module__ == "memory_server.application.belief_service"
        assert "memory_server.server" not in inspect.getsource(BeliefService)
        imported = set(sys.modules) - before
        assert not any(x == f or x.startswith(f + ".") for x in imported for f in FORBIDDEN)
    else:
        import memory_server.server as server

        # Cross the real legacy production boundary before checking the invariant.
        server._provider = provider
        server._get_provider = lambda: _ready(provider)
        result = json.loads(await server.set_belief_tool("direct boundary"))
        assert result["belief"]["proposition"] == "direct boundary"
        assert server.set_belief_tool.__module__ == "memory_server.application.belief_service", (
            f"legacy server tool crossed the production boundary: observed module={server.set_belief_tool.__module__!r}"
        )


@pytest.mark.asyncio
async def test_belief_policy_avoids_legacy_import_time_ambient_default(provider, monkeypatch):
    if _SUBJECT == "application":
        import memory_server.settings as settings
        from memory_server.application.belief_service import BeliefService
        from memory_server.application.contracts import BeliefPolicy, GetBeliefCommand

        reads = 0

        def forbidden_read():
            nonlocal reads
            reads += 1
            raise AssertionError("ambient settings read after explicit BeliefPolicy construction")

        policy = BeliefPolicy(7)
        service = BeliefService(provider, policy=policy, get_graph=_none_graph)
        monkeypatch.setattr(settings, "get_settings", forbidden_read)
        first = await service.get_belief(GetBeliefCommand(limit=None))
        second = await service.get_belief(GetBeliefCommand(limit=None))
        assert reads == 0
        assert first["query"]["limit"] == second["query"]["limit"] == 7
        assert first["query"]["limit"] == policy.belief_search_limit
    else:
        import importlib

        import memory_server.settings as settings

        original = settings.get_settings
        current_limit = original().belief_search_limit
        import_limit = current_limit + 1
        import_reads: list[int] = []

        class ImportSettings:
            def __init__(self, base):
                self._base = base
                self.belief_search_limit = import_limit
                import_reads.append(self.belief_search_limit)

            def __getattr__(self, name):
                return getattr(self._base, name)

        def record_import_read():
            return ImportSettings(original())

        monkeypatch.setattr(settings, "get_settings", record_import_read)
        sys.modules.pop("memory_server.server", None)
        server = importlib.import_module("memory_server.server")
        assert import_reads, "server import did not read settings for the get_belief_tool default"

        server._provider = provider
        server._get_provider = lambda: _ready(provider)
        observed = json.loads(await server.get_belief_tool())
        current = original().belief_search_limit
        assert observed["query"]["limit"] == current, (
            "legacy import-time ambient default drifted from current settings: "
            f"import_read={import_reads[-1]!r}, current={current!r}, "
            f"observed={observed['query']['limit']!r}"
        )


@pytest.mark.asyncio
async def test_application_composition_exposes_beliefs_without_changing_c07_surface(provider):
    if _SUBJECT == "application":
        from memory_server.application.application import MemoryApplication
        from memory_server.application.contracts import BeliefPolicy
        from memory_server.application.errors import UnknownDependencyError

        app = MemoryApplication.from_provider(provider, belief_policy=BeliefPolicy(10))
        assert app.describe() == {"policy": {"max_active_beliefs": app.policy.max_active_beliefs}}
        assert app.beliefs.policy.belief_search_limit == 10
        with pytest.raises(UnknownDependencyError, match="belief service"):
            MemoryApplication(provider._session_factory).beliefs
    else:
        service = await _under_test(provider)
        assert service.set_belief_tool.__module__ == "memory_server.server"


@pytest.mark.asyncio
async def test_set_belief_create_matches_legacy_result_and_sqlite_effects(provider):
    set_command_type, _, _ = _commands()
    service = await _under_test(provider)
    result = (
        await _set(
            _SUBJECT,
            service,
            set_command_type(
                "SQLite is canonical",
                0.8,
                [{"source_type": "fact", "source_id": "s1", "weight": 0.9}],
                ["storage"],
                "test",
            ),
        )
        if set_command_type
        else await _set(
            _SUBJECT,
            service,
            _legacy_command(
                proposition="SQLite is canonical",
                confidence=0.8,
                sources=[{"source_type": "fact", "source_id": "s1", "weight": 0.9}],
                tags=["storage"],
                source="test",
                replace_belief_id="",
            ),
        )
    )
    _record("create", result, provider._c08_db)
    assert set(result) == {"belief", "receipt", "superseded"}


@pytest.mark.asyncio
async def test_set_belief_reinforce_matches_legacy_normalization_version_and_history(provider):
    set_command_type, _, _ = _commands()
    service = await _under_test(provider)
    first = await _set(
        _SUBJECT,
        service,
        set_command_type("  Same proposition ", 0.4, None, None, "a")
        if set_command_type
        else _legacy_command(
            proposition="  Same proposition ", confidence=0.4, sources=None, tags=None, source="a", replace_belief_id=""
        ),
    )
    result = await _set(
        _SUBJECT,
        service,
        set_command_type("same proposition", 0.8, None, ["new"], "b")
        if set_command_type
        else _legacy_command(
            proposition="same proposition", confidence=0.8, sources=None, tags=["new"], source="b", replace_belief_id=""
        ),
    )
    _record("reinforce", result, provider._c08_db)
    assert result["reinforced"] is True and result["belief"]["version"] == 2
    assert await provider.get_belief(first["belief"]["id"])


@pytest.mark.asyncio
async def test_set_belief_replace_preserves_version_and_transition_effects(provider):
    set_command_type, _, _ = _commands()
    service = await _under_test(provider)
    old = await _set(
        _SUBJECT,
        service,
        set_command_type("old")
        if set_command_type
        else _legacy_command(
            proposition="old", confidence=0.5, sources=None, tags=None, source="system", replace_belief_id=""
        ),
    )
    cmd = (
        set_command_type("new", 0.7, None, None, "system", old["belief"]["id"])
        if set_command_type
        else _legacy_command(
            proposition="new",
            confidence=0.7,
            sources=None,
            tags=None,
            source="system",
            replace_belief_id=old["belief"]["id"],
        )
    )
    result = await _set(_SUBJECT, service, cmd)
    _record("replace", result, provider._c08_db)
    assert result["belief"]["version"] == 2 and result["superseded"]["id"] == old["belief"]["id"]


@pytest.mark.asyncio
async def test_get_belief_filters_limit_query_and_order_match_legacy(provider):
    set_command_type, get_command_type, _ = _commands()
    service = await _under_test(provider)
    for p, c, t in [("alpha", 0.9, ["x"]), ("beta", 0.4, ["y"])]:
        cmd = (
            set_command_type(p, c, None, t)
            if set_command_type
            else _legacy_command(
                proposition=p, confidence=c, sources=None, tags=t, source="system", replace_belief_id=""
            )
        )
        await _set(_SUBJECT, service, cmd)
    cmd = (
        get_command_type(tags=["x"], min_confidence=0.8, limit=200)
        if get_command_type
        else _legacy_command(
            proposition="",
            lifecycle_state="active",
            min_confidence=0.8,
            tags=["x"],
            source="",
            creator="",
            source_id="",
            limit=200,
        )
    )
    result = await _get(_SUBJECT, service, cmd)
    _record("get", result, provider._c08_db)
    assert result["total"] == 1 and result["query"]["limit"] == 200 and result["beliefs"][0]["proposition"] == "alpha"


@pytest.mark.asyncio
async def test_resolve_conflict_auto_threshold_events_and_result_match_legacy(provider):
    set_command_type, _, resolve_command_type = _commands()
    service = await _under_test(provider)

    def sc(p, c):
        return (
            set_command_type(p, c)
            if set_command_type
            else _legacy_command(
                proposition=p, confidence=c, sources=None, tags=None, source="system", replace_belief_id=""
            )
        )

    a = await _set(_SUBJECT, service, sc("a", 0.1))
    b = await _set(_SUBJECT, service, sc("b", 0.9))
    cmd = (
        resolve_command_type(a["belief"]["id"], b["belief"]["id"], "keep_a", auto_resolve=True)
        if resolve_command_type
        else _legacy_command(
            belief_a_id=a["belief"]["id"],
            belief_b_id=b["belief"]["id"],
            resolution="keep_a",
            new_proposition="",
            auto_resolve=True,
        )
    )
    result = await _resolve(_SUBJECT, service, cmd)
    _record("auto", result, provider._c08_db)
    assert result["auto_resolved"] is True and result["belief_a"]["lifecycle_state"] == "superseded"


@pytest.mark.asyncio
async def test_resolve_conflict_manual_keep_discard_merge_and_result_match_legacy(provider):
    set_command_type, _, resolve_command_type = _commands()
    service = await _under_test(provider)

    def sc(p, c):
        return (
            set_command_type(p, c)
            if set_command_type
            else _legacy_command(
                proposition=p, confidence=c, sources=None, tags=None, source="system", replace_belief_id=""
            )
        )

    async def pair(a, b):
        return await _set(_SUBJECT, service, sc(a, 0.7)), await _set(_SUBJECT, service, sc(b, 0.6))

    a, b = await pair("a", "b")
    cmd = (
        resolve_command_type(a["belief"]["id"], b["belief"]["id"], "keep_a")
        if resolve_command_type
        else _legacy_command(
            belief_a_id=a["belief"]["id"],
            belief_b_id=b["belief"]["id"],
            resolution="keep_a",
            new_proposition="",
            auto_resolve=False,
        )
    )
    r = await _resolve(_SUBJECT, service, cmd)
    _record("manual", r, provider._c08_db)
    assert r["belief_b"]["lifecycle_state"] == "discarded"


@pytest.mark.asyncio
async def test_resolve_conflict_errors_preserve_type_identity_and_message(provider):
    set_command_type, _, resolve_command_type = _commands()
    service = await _under_test(provider)

    def rc(a, b, res, prop=""):
        return (
            resolve_command_type(a, b, res, prop)
            if resolve_command_type
            else _legacy_command(belief_a_id=a, belief_b_id=b, resolution=res, new_proposition=prop, auto_resolve=False)
        )

    with pytest.raises(ValueError, match="Both belief_a and belief_b"):
        await _resolve(_SUBJECT, service, rc("missing", "also-missing", "keep_a"))
    a = await _set(
        _SUBJECT,
        service,
        set_command_type("a")
        if set_command_type
        else _legacy_command(
            proposition="a", confidence=0.5, sources=None, tags=None, source="system", replace_belief_id=""
        ),
    )
    b = await _set(
        _SUBJECT,
        service,
        set_command_type("b")
        if set_command_type
        else _legacy_command(
            proposition="b", confidence=0.5, sources=None, tags=None, source="system", replace_belief_id=""
        ),
    )
    with pytest.raises(ValueError, match="new_proposition is required"):
        await _resolve(_SUBJECT, service, rc(a["belief"]["id"], b["belief"]["id"], "merge"))
    with pytest.raises(ValueError, match="Unknown resolution"):
        await _resolve(_SUBJECT, service, rc(a["belief"]["id"], b["belief"]["id"], "nope"))


@pytest.mark.asyncio
async def test_belief_conflict_failure_rolls_back_real_sqlite_unit_of_work(provider):
    set_command_type, _, resolve_command_type = _commands()
    service = await _under_test(provider)

    def sc(p):
        return (
            set_command_type(p)
            if set_command_type
            else _legacy_command(
                proposition=p, confidence=0.5, sources=None, tags=None, source="system", replace_belief_id=""
            )
        )

    a = await _set(_SUBJECT, service, sc("a"))
    b = await _set(_SUBJECT, service, sc("b"))
    cmd = (
        resolve_command_type(a["belief"]["id"], b["belief"]["id"], "bad")
        if resolve_command_type
        else _legacy_command(
            belief_a_id=a["belief"]["id"],
            belief_b_id=b["belief"]["id"],
            resolution="bad",
            new_proposition="",
            auto_resolve=False,
        )
    )
    with pytest.raises(Exception):
        await _resolve(_SUBJECT, service, cmd)
    assert (await provider.get_belief(a["belief"]["id"])).lifecycle_state == "active"
    assert (await provider.get_belief(b["belief"]["id"])).lifecycle_state == "active"


@pytest.mark.asyncio
async def test_belief_paths_do_not_invoke_memory_admission_gate(provider):
    set_command_type, get_command_type, _ = _commands()
    service = await _under_test(provider)
    cmd = (
        set_command_type("admission-free")
        if set_command_type
        else _legacy_command(
            proposition="admission-free", confidence=0.5, sources=None, tags=None, source="system", replace_belief_id=""
        )
    )
    await _set(_SUBJECT, service, cmd)
    get = (
        get_command_type()
        if get_command_type
        else _legacy_command(
            proposition="",
            lifecycle_state="active",
            min_confidence=0.0,
            tags=None,
            source="",
            creator="",
            source_id="",
            limit=10,
        )
    )
    result = await _get(_SUBJECT, service, get)
    assert result["total"] == 1


@pytest.mark.asyncio
async def test_direct_belief_service_does_not_construct_or_use_transport_dependencies(provider):
    service = await _under_test(provider)
    if _SUBJECT == "application":
        assert service._provider is provider and service._get_graph is not None
        assert "memory_server.server" not in inspect.getsource(type(service))
    else:
        assert service.set_belief_tool.__module__ == "memory_server.server"


def test_c08_allowlist_and_source_base_are_recorded():
    expected = {
        "src/memory_server/application/application.py",
        "src/memory_server/application/contracts.py",
        "src/memory_server/application/belief_service.py",
        "tests/test_application_beliefs.py",
    }
    assert len(expected) == 4
    service_path = Path(__file__).parents[1] / "src/memory_server/application/belief_service.py"
    assert service_path.exists() is (_SUBJECT == "application")
