"""C06 — transport-free application layer over the shared memory use cases.

Module-level imports are base-existing ONLY and ``memory_server.application`` is imported lazily inside
the tests, so a pre-change run reports a genuine ASSERTION failure rather than a collection-time import
error. Exactly one test here is RED; every other new-package assertion is labelled RED-by-absence.
"""

from __future__ import annotations

import ast
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import textwrap
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import event, text

from memory_server.api.bulk_import import import_memory_md, parse_memory_md
from memory_server.api.learn import learn as api_learn
from memory_server.api.remember import remember as api_remember
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.services.ingestion_service import MemoryIngestionService

REPO_ROOT = Path(__file__).resolve().parents[1]
APPLICATION_DIR = REPO_ROOT / "src" / "memory_server" / "application"
WIRE_SNAPSHOT = REPO_ROOT / "tests" / "fixtures" / "runtime_contracts.json"
LIVE_DATA_ROOTS = (Path("/home/shtorm/.hermes/data"), Path("/home/shtorm/memory-server/data"))
TOOL_NAMES_C00 = frozenset(
    ("add_relation", "audit", "get_belief", "get_context", "graph_search", "invalidate", "learn",
     "metrics", "ping", "reflect", "remember", "resolve_conflict", "route", "search",
     "semantic_search", "set_belief"))
BLOCKED_IMPORT_ROOTS = (
    "mcp", "typer", "memory_server.server", "memory_server.transports", "memory_server.runtime",
    "memory_server.plugins.hermes.provider", "memory_server.plugins.hermes.writer", "lancedb",
    "numpy", "pyarrow", "qdrant_client", "sentence_transformers")
FACT_COLS = ("subject", "predicate", "object", "dedup_key", "confidence", "source", "lifecycle_state")
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2})?$")
_SCRATCH_PATHS: list[Path] = []  # db paths, temporary files and subprocess cwds


def _db_url(directory: Path | str, name: str = "memory.db") -> str:
    path = (Path(directory) / name).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    _SCRATCH_PATHS.append(path)
    return "sqlite+aiosqlite:///" + str(path)


async def _provider(directory: Path | str, name: str = "memory.db") -> SQLiteProvider:
    provider = SQLiteProvider(url=_db_url(directory, name), busy_timeout_ms=100)
    await provider.initialize()
    return provider


@asynccontextmanager
async def _two_db(tmp_path: Path, label: str):
    """Two independent openable file DBs: (app path, api path, app directory)."""
    app_dir, api_dir = tmp_path / f"{label}-app", tmp_path / f"{label}-api"
    left, right = await _provider(app_dir, "memory.db"), await _provider(api_dir, "memory.db")
    try:
        yield left, right, app_dir
    finally:
        await left.close()
        await right.close()


async def _snapshot(provider: SQLiteProvider) -> dict[str, list[dict[str, Any]]]:
    """Every SQL table as {column: value}; FTS shadow tables excluded (pr13 technique)."""
    async with provider._session_factory() as session:
        query = text("SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = (await session.execute(query)).all()
        out: dict[str, list[dict[str, Any]]] = {}
        for name, ddl in tables:
            if name.startswith("sqlite_") or (ddl and "VIRTUAL TABLE" in ddl.upper()) or name.endswith(
                    ("_config", "_content", "_data", "_docsize", "_idx")):
                continue
            cols = (await session.execute(text(f'PRAGMA table_info("{name}")'))).all()
            order = " ORDER BY rowid" if any(col[5] for col in cols) else ""
            rows = (await session.execute(text(f'SELECT * FROM "{name}"{order}'))).all()
            names = [col[1] for col in cols]
            out[name] = [dict(zip(names, row)) for row in rows]
    return out


def _normalize(value: Any) -> Any:
    """Normalize ONLY ids and timestamps (DETAIL §2.3 whitelist)."""
    if isinstance(value, datetime):
        return "<ts>"
    if isinstance(value, (bytes, bytearray)):
        return "<bytes>"
    if isinstance(value, str):
        if _UUID_RE.match(value):
            return "<id>"
        if _TS_RE.match(value):
            return "<ts>"
        if value.strip()[:1] in "[{":
            try:
                return _normalize(json.loads(value))
            except (ValueError, TypeError):
                return value
        return value
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    return value


def _canonical(value: Any) -> str:
    return json.dumps(_normalize(value), sort_keys=True, default=str)


def _project(rows: list[dict[str, Any]], columns: Any) -> list[tuple[Any, ...]]:
    return sorted(tuple(row[column] for column in columns) for row in rows)


def _receipt_key(row: dict[str, Any]) -> tuple[Any, ...]:
    history = json.loads(row["history"]) if row["history"] else []
    return (row["memory_type"], row["source"], row["created_by"], row["confidence"],
            row["verification_status"], _canonical(history[-1].get("metadata") if history else None))


def _outbox_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (row["record_type"], row["operation"], _canonical(row["payload_json"]))


def _run_python(source: str, workspace: Path, *, timeout: int = 240) -> subprocess.CompletedProcess:
    """Run a probe in a FRESH interpreter with the current PYTHONPATH."""
    resolved = Path(workspace).resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    _SCRATCH_PATHS.append(resolved)
    script = resolved / "c06_probe.py"
    script.write_text(textwrap.dedent(source), encoding="utf-8")
    env = dict(os.environ, PYTHONHASHSEED="0", PYTHONDONTWRITEBYTECODE="1")
    return subprocess.run([sys.executable, str(script)], cwd=str(resolved), env=env,
                          capture_output=True, text=True, timeout=timeout)


def _registered_tool_names() -> list[str]:
    """Static AST scan of every ``@mcp.tool`` / ``@tool`` decorator under src/."""
    names: list[str] = []
    for path in sorted((REPO_ROOT / "src" / "memory_server").rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), filename=str(path))):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            for decorator in node.decorator_list:
                target = decorator.func if isinstance(decorator, ast.Call) else decorator
                named = (isinstance(target, ast.Attribute) and target.attr == "tool") or (
                    isinstance(target, ast.Name) and target.id == "tool")
                if not named:
                    continue
                explicit = None
                for keyword in decorator.keywords if isinstance(decorator, ast.Call) else ():
                    if keyword.arg == "name" and isinstance(keyword.value, ast.Constant):
                        explicit = keyword.value.value
                names.append(explicit or node.name)
    return names


def _graph_shape(graph: SimpleGraph) -> tuple[list[Any], list[Any]]:
    nodes = {(graph.get_node(i).name, graph.get_node(i).type) for i in list(graph._nodes)}
    edges = {(graph.get_node(e.source_id).name, graph.get_node(e.target_id).name, e.relation)
             for e in graph.search_by_relation("derived_from")}
    return sorted(nodes), sorted(edges)


@pytest.fixture
async def live_env(tmp_path):
    """A real SQLiteProvider plus a MemoryApplication over it, closed after the test."""
    from memory_server.application import MemoryApplication
    provider = await _provider(tmp_path, "live.db")
    try:
        yield MemoryApplication.from_provider(provider), provider
    finally:
        await provider.close()


async def test_belief_soft_limit_is_captured_at_construction_and_not_read_from_ambient_settings(
    tmp_path, monkeypatch, caplog
) -> None:
    """RED — the ambient ``max_active_beliefs`` must not be read at call time.
    Before: ``services/ingestion_service.py:917-919`` reads it DURING the call (one
    ambient read + the skip branch at ``:925-930``). After: it is captured at
    construction, so the call reads nothing and skips nothing.
    """
    import memory_server.settings as settings_module
    monkeypatch.setenv("MEMORY_SERVER_MAX_ACTIVE_BELIEFS", "500")
    settings_module.get_settings.cache_clear()
    real_get_settings = settings_module.get_settings
    provider = SQLiteProvider(url=_db_url(tmp_path, "red.db"))
    await provider.initialize()
    try:
        svc = MemoryIngestionService(provider._session_factory)
        monkeypatch.setenv("MEMORY_SERVER_MAX_ACTIVE_BELIEFS", "0")
        real_get_settings.cache_clear()
        reads: list[int] = []
        monkeypatch.setattr(settings_module, "get_settings", lambda: (reads.append(1), real_get_settings())[1])
        with caplog.at_level(logging.WARNING, logger="memory_server.services.ingestion_service"):
            result = await svc.learn(text="Docker is container. I prefer Docker over Podman",
                                     extract_beliefs=True, min_belief_confidence=0.3)
        assert len(result["facts"]) >= 1  # core write succeeds: failure is behavioural
        # Both discriminators are asserted together: Python aborts on the first failing
        # assert, and the RED output must contain BOTH.
        failures = [f"ambient reader called during the call: reads={reads!r}"] if reads else []
        if "skipping belief extraction" in caplog.text:
            failures.append("soft-limit skip branch taken from ambient settings")
        assert failures == [], failures
    finally:
        await provider.close()
        real_get_settings.cache_clear()


async def test_legacy_ambient_default_is_still_honoured_when_no_explicit_limit_is_injected(
    tmp_path, monkeypatch, caplog
) -> None:
    """Characterization — the ambient default is preserved when nothing is injected."""
    import memory_server.settings as settings_module
    monkeypatch.setenv("MEMORY_SERVER_MAX_ACTIVE_BELIEFS", "0")
    settings_module.get_settings.cache_clear()
    provider = SQLiteProvider(url=_db_url(tmp_path, "legacy.db"))
    await provider.initialize()
    try:
        svc = MemoryIngestionService(provider._session_factory)
        with caplog.at_level(logging.WARNING, logger="memory_server.services.ingestion_service"):
            result = await svc.learn(text="Docker is container. I prefer Docker over Podman",
                                     extract_beliefs=True, min_belief_confidence=0.3)
        assert result["beliefs"] == [] and len(result["facts"]) >= 1
        assert "skipping belief extraction" in caplog.text
    finally:
        await provider.close()
        settings_module.get_settings.cache_clear()


def test_application_package_imports_without_transports_or_optional_backends(tmp_path) -> None:
    """RED-by-absence — Boundary crossed: the real import of application/__init__.py."""
    source = ("import builtins, sys\nBLOCKED = " + repr(BLOCKED_IMPORT_ROOTS) + "\n"
              "_import = __import__\n"
              "def _guarded(name, *args, **kwargs):\n"
              "    assert not any(name == b or name.startswith(b + '.') for b in BLOCKED), name\n"
              "    return _import(name, *args, **kwargs)\n"
              "builtins.__import__ = _guarded\n"
              "import memory_server.application\n"
              "leaked = [n for n in sys.modules if any(n == b or n.startswith(b + '.') for b in BLOCKED)]\n"
              "assert not leaked, leaked\nprint('IMPORT_GUARD_OK')\n")
    completed = _run_python(source, tmp_path / "import-guard")
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "IMPORT_GUARD_OK" in completed.stdout


def test_application_use_cases_run_without_any_transport_module_imported(tmp_path) -> None:
    """RED-by-absence — Boundary crossed: memory_service -> ingestion_service.py:285 -> SQLite."""
    db_path = (tmp_path / "usecase" / "memory.db").resolve()
    _SCRATCH_PATHS.append(db_path)
    source = f"""
        import asyncio, sys
        from sqlalchemy import text
        from memory_server.providers.sqlite_provider import SQLiteProvider
        from memory_server.application import MemoryApplication, RememberCommand
        async def main():
            provider = SQLiteProvider(url="sqlite+aiosqlite:///{db_path}")
            await provider.initialize()
            app = MemoryApplication.from_provider(provider)
            result = await app.memory.remember(RememberCommand("Docker", "runs_on", "OMV8"))
            assert set(result) == {{"receipt", "fact"}}, sorted(result)
            async with provider._session_factory() as session:
                counts = {{t: (await session.execute(text("SELECT count(*) FROM " + t))).scalar()
                          for t in ("facts", "receipts", "outbox_entries")}}
            assert counts == {{"facts": 1, "receipts": 1, "outbox_entries": 1}}, counts
            assert "memory_server.server" not in sys.modules and "mcp" not in sys.modules
            await provider.close()
            print("USECASE_OK", counts)
        asyncio.run(main())
    """
    completed = _run_python(source, tmp_path / "usecase")
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "USECASE_OK" in completed.stdout


async def test_application_never_reads_ambient_settings_after_construction(live_env, monkeypatch) -> None:
    """RED-by-absence — the injected-policy path reads no ambient settings."""
    import memory_server.settings as settings_module
    from memory_server.application import LearnCommand, MemoryPolicy, MemoryService, RememberCommand
    _, provider = live_env
    real_get_settings = settings_module.get_settings
    svc = MemoryService(provider._session_factory, policy=MemoryPolicy(max_active_beliefs=500))
    monkeypatch.setenv("MEMORY_SERVER_MAX_ACTIVE_BELIEFS", "0")
    monkeypatch.setenv("MEMORY_SERVER_MIN_BELIEF_CONFIDENCE", "0.99")
    real_get_settings.cache_clear()
    reads: list[int] = []
    monkeypatch.setattr(settings_module, "get_settings", lambda: (reads.append(1), real_get_settings())[1])
    remembered = await svc.remember(RememberCommand("Docker", "runs_on", "OMV8"))
    learned = await svc.learn(LearnCommand(text="Docker is container. I prefer Docker over Podman",
                                           extract_beliefs=True, min_belief_confidence=0.3))
    assert remembered["fact"].subject == "Docker" and len(learned["facts"]) >= 1
    assert reads == []
    real_get_settings.cache_clear()


def test_command_dto_defaults_are_taken_from_code_not_from_architecture_source() -> None:
    """RED-by-absence — ARCHITECTURE_SOURCE §21 is not a source of defaults."""
    from memory_server.application import BulkImportCommand, LearnCommand, RememberCommand
    remember = RememberCommand(subject="s", predicate="p", object="o")
    assert (remember.confidence, remember.source) == (1.0, "user")
    assert remember.metadata is None and remember.admission is None
    learn = LearnCommand(text="t")
    assert (learn.source, learn.extract_beliefs, learn.min_belief_confidence) == ("user", False, 0.6)
    assert learn.llm_extractor is None and learn.llm_timeout_seconds is None
    assert learn.llm_max_input_chars is None and learn.llm_confidence_gate is None
    bulk = BulkImportCommand(path="/tmp/MEMORY.md")
    assert (bulk.source, bulk.gate) == ("MEMORY.md", None)
    contracts = (APPLICATION_DIR / "contracts.py").read_text(encoding="utf-8")
    assert '= 0.5' not in contracts, "ARCHITECTURE_SOURCE §21 confidence default leaked in"
    assert '"manual"' not in contracts, "ARCHITECTURE_SOURCE §21 source default leaked in"


def test_command_dto_defaults_match_the_c00_wire_snapshot() -> None:
    """RED-by-absence — DTO defaults equal the C00 stdio wire defaults."""
    from memory_server.application import LearnCommand, RememberCommand
    raw = WIRE_SNAPSHOT.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == "0f369d2079973c6d4798144f78730f134c07a3e56416a87f8c6b9188d7236338"
    tools = {tool["name"]: tool for tool in json.loads(raw)["mcp"]["tools"]}
    remember_props = tools["remember"]["input_schema"]["properties"]
    learn_props = tools["learn"]["input_schema"]["properties"]
    remember = RememberCommand(subject="s", predicate="p", object="o")
    assert remember.confidence == remember_props["confidence"]["default"] == 1.0
    assert remember.source == remember_props["source"]["default"] == "user"
    learn = LearnCommand(text="t")
    assert learn.min_belief_confidence == learn_props["min_belief_confidence"]["default"] == 0.6
    assert learn.source == learn_props["source"]["default"] == "user"
    assert learn.extract_beliefs is learn_props["extract_beliefs"]["default"] is False


async def test_command_validation_raises_valueerror_with_the_existing_messages() -> None:
    """RED-by-absence — messages match ingestion_service.py:312-319."""
    from memory_server.application import InvalidCommandError, MemoryApplication, RememberCommand
    from memory_server.application.contracts import validate_remember_command
    assert issubclass(InvalidCommandError, ValueError)
    assert InvalidCommandError.code == "E_APPLICATION_INVALID_COMMAND"
    cases = [("", "is", "x", 1.0, "subject"), ("X", "", "x", 1.0, "predicate"), ("X", "is", "", 1.0, "object"),
             ("X", "is", "y", 2.0, "confidence"), ("X", "is", "y", -0.1, "confidence")]
    for subject, predicate, obj, confidence, pattern in cases:
        with pytest.raises(ValueError, match=pattern):
            validate_remember_command(RememberCommand(subject, predicate, obj, confidence))
    def _explode():  # pragma: no cover - must never be reached
        raise AssertionError("session factory was used before validation")
    with pytest.raises(InvalidCommandError, match="subject"):
        await MemoryApplication(_explode).memory.remember(RememberCommand("", "is", "x"))


async def test_learn_command_omits_none_extraction_kwargs_so_service_defaults_apply(live_env) -> None:
    """RED-by-absence — behavioural form of the api/learn.py:64-76 forwarding rule."""
    from memory_server.application import LearnCommand
    app, _ = live_env
    calls: list[str] = []
    def extractor(call_text: str) -> dict[str, Any]:
        calls.append(call_text)
        return {"facts": [{"subject": "Alice", "predicate": "is", "object": "a tester",
                           "confidence": 0.9}], "decisions": []}
    result = await app.memory.learn(LearnCommand(text="Alice is a tester", llm_extractor=extractor,
                                                 llm_timeout_seconds=None, llm_max_input_chars=None))
    # Had None been forwarded, the service's TypeError checks (ingestion_service.py:517-532)
    # would have degraded to regex and the callable would never have run.
    assert calls == ["Alice is a tester"]
    assert result["facts"][0]["item"]["subject"] == "Alice"


async def test_remember_effect_parity_with_the_existing_api_path(tmp_path) -> None:
    """RED-by-absence — parity guard on two independent openable file DBs."""
    from memory_server.application import MemoryApplication, RememberCommand
    async with _two_db(tmp_path, "parity") as (provider_app, provider_api, app_dir):
        metadata = {"evidence": {"claim_type": "fact", "derived_from": []}}
        await MemoryApplication.from_provider(provider_app).memory.remember(
            RememberCommand("Docker", "runs_on", "OMV8", confidence=0.9, source="parity",
                            metadata=dict(metadata)))
        await api_remember(provider_api, "Docker", "runs_on", "OMV8", confidence=0.9,
                           source="parity", metadata=dict(metadata))
        got, expected = await _snapshot(provider_app), await _snapshot(provider_api)
        assert len(got["facts"]) == 1
        assert _project(got["facts"], FACT_COLS) == _project(expected["facts"], FACT_COLS)
        assert sorted(map(_receipt_key, got["receipts"])) == sorted(map(_receipt_key, expected["receipts"]))
        assert sorted(map(_outbox_key, got["outbox_entries"])) == sorted(
            map(_outbox_key, expected["outbox_entries"]))
        # A fresh provider on the same file must observe the same committed state.
        fresh = SQLiteProvider(url=_db_url(app_dir, "memory.db"), busy_timeout_ms=100)
        await fresh.initialize()
        try:
            assert await _snapshot(fresh) == got
        finally:
            await fresh.close()


async def test_learn_effect_parity_with_the_existing_api_path(tmp_path) -> None:
    """RED-by-absence — parity guard, regex extraction only."""
    from memory_server.application import LearnCommand, MemoryApplication
    text_input = "Docker is container. I prefer Docker over Podman for development"
    async with _two_db(tmp_path, "learn") as (provider_app, provider_api, _):
        response_app = await MemoryApplication.from_provider(provider_app).memory.learn(
            LearnCommand(text=text_input, extract_beliefs=False, llm_extractor=None))
        response_api = await api_learn(provider_api, text=text_input, extract_beliefs=False)
        assert set(response_app) == set(response_api)
        assert {k: len(v) for k, v in response_app.items()} == {k: len(v) for k, v in response_api.items()}
        got, expected = await _snapshot(provider_app), await _snapshot(provider_api)
        for table in ("facts", "decisions", "skills"):
            assert len(got[table]) == len(expected[table]), table
            if got[table]:
                # canonical columns only: ids and timestamps are normalized away
                columns = tuple(sorted(set(got[table][0]) - {"id", "created_at", "updated_at"}))
                assert _project(got[table], columns) == _project(expected[table], columns), table
        assert got["facts"], "regex extraction stored no fact — nothing to compare"
        assert sorted(r["memory_type"] for r in got["receipts"]) == sorted(
            r["memory_type"] for r in expected["receipts"])
        assert sorted((r["record_type"], r["operation"]) for r in got["outbox_entries"]) == sorted(
            (r["record_type"], r["operation"]) for r in expected["outbox_entries"])


async def test_dedup_second_identical_command_reinforces_without_a_new_fact(live_env) -> None:
    """RED-by-absence — parity guard for the dedup/reinforce path."""
    from memory_server.application import RememberCommand
    app, provider = live_env
    first = await app.memory.remember(RememberCommand("Docker", "runs_on", "OMV8", confidence=0.4))
    second = await app.memory.remember(RememberCommand("Docker", "runs_on", "OMV8", confidence=0.9))
    assert second["fact"].id == first["fact"].id
    assert second["fact"].version == first["fact"].version + 1
    got = await _snapshot(provider)
    assert len(got["facts"]) == 1 and len(got["outbox_entries"]) == 1 and len(got["receipts"]) == 1
    reinforces = [e for e in json.loads(got["receipts"][0]["history"]) if e.get("kind") == "reinforce"]
    assert len(reinforces) == 1 and reinforces[0]["previous_confidence"] == 0.4


@pytest.mark.parametrize("identifier", ["before_flush", "before_commit"])
async def test_rollback_on_injected_database_failure_matches_the_existing_path(tmp_path, identifier) -> None:
    """RED-by-absence — parity guard at the real SQLAlchemy failure edge."""
    from memory_server.application import MemoryApplication, RememberCommand
    outcomes: list[tuple[list[dict[str, Any]], bool]] = []
    for label, use_application in (("app", True), ("api", False)):
        directory = tmp_path / f"rollback-{label}-{identifier}"
        provider = await _provider(directory, "memory.db")
        try:
            before, reached = await _snapshot(provider), False
            sync_cls = provider._session_factory.class_.sync_session_class
            def fail(session, *args, **kwargs):
                nonlocal reached
                reached = True
                raise RuntimeError(f"C06 injected {identifier} failure")
            event.listen(sync_cls, identifier, fail)
            try:
                with pytest.raises(RuntimeError, match="C06 injected"):
                    if use_application:
                        await MemoryApplication.from_provider(provider).memory.remember(
                            RememberCommand("Docker", "runs_on", "OMV8", source="rollback"))
                    else:
                        await api_remember(provider, "Docker", "runs_on", "OMV8", source="rollback")
            finally:
                event.remove(sync_cls, identifier, fail)
            assert reached, f"failure injection point {identifier} was not reached"
            after = await _snapshot(provider)
            assert after == before
            fresh = SQLiteProvider(url=_db_url(directory, "memory.db"), busy_timeout_ms=100)
            await fresh.initialize()
            try:
                assert await _snapshot(fresh) == before
            finally:
                await fresh.close()
            outcomes.append((after, reached))
        finally:
            await provider.close()
    assert outcomes[0][0] == outcomes[1][0]
    assert outcomes[0][1] and outcomes[1][1]


async def test_application_returns_the_unchanged_response_shapes(live_env, tmp_path) -> None:
    """RED-by-absence — parity guard for the response envelopes."""
    from memory_server.application import BulkImportCommand, LearnCommand, RememberCommand
    from memory_server.models import MemoryReceipt, VerificationStatus
    app, _ = live_env
    memory_md = tmp_path / "MEMORY.md"
    memory_md.write_text("# MEMORY\n\n- User prefers concise terminal-friendly responses.\n",
                         encoding="utf-8")
    remembered = await app.memory.remember(RememberCommand("Docker", "runs_on", "OMV8"))
    assert set(remembered) == {"receipt", "fact"}
    assert isinstance(remembered["receipt"], MemoryReceipt)
    assert remembered["receipt"].verification_status is VerificationStatus.CANDIDATE
    learned = await app.memory.learn(LearnCommand(text="Docker is container"))
    assert set(learned) == {"facts", "decisions", "skills", "beliefs", "receipts"}
    imported = await app.memory.bulk_import(BulkImportCommand(path=memory_md))
    assert set(imported) == {"imported", "skipped", "items", "skipped_items"}
    for payload in (remembered, learned, imported):
        assert not isinstance(payload, str)
        assert set(payload) & {"content", "isError", "is_error"} == set()
        assert not any(isinstance(v, str) and v.strip()[:1] == "{" for v in payload.values())


async def test_graph_derived_from_best_effort_is_preserved(tmp_path) -> None:
    """RED-by-absence — parity guard for the verbatim derived_from graph block."""
    from memory_server.application import MemoryApplication, RememberCommand
    async with _two_db(tmp_path, "graph") as (provider_app, provider_api, _):
        app, graph_app = MemoryApplication.from_provider(provider_app), SimpleGraph()
        parent_id = (await app.memory.remember(RememberCommand("Docker", "runs_on", "OMV8")))["fact"].id
        child_id = (await app.memory.remember(RememberCommand(
            "Container", "derived_from", "Docker",
            metadata={"evidence": {"claim_type": "fact", "derived_from": [parent_id]}}),
            graph=graph_app))["fact"].id
        assert graph_app.get_node(child_id) is not None and graph_app.get_node(parent_id) is not None
        assert any(e.source_id == child_id and e.target_id == parent_id and e.relation == "derived_from"
                   for e in graph_app.search_by_relation("derived_from"))
        graph_api = SimpleGraph()
        api_parent_id = (await api_remember(provider_api, "Docker", "runs_on", "OMV8"))["fact"].id
        await api_remember(provider_api, "Container", "derived_from", "Docker", graph=graph_api,
                           metadata={"evidence": {"claim_type": "fact", "derived_from": [api_parent_id]}})
        assert _graph_shape(graph_app) == _graph_shape(graph_api)
        class BoomGraph:
            def get_node(self, node_id):  # noqa: ANN001
                raise RuntimeError("graph is unavailable")
        survived = await app.memory.remember(
            RememberCommand("Boom", "is", "safe",
                            metadata={"evidence": {"claim_type": "fact",
                                                   "derived_from": [parent_id]}}),
            graph=BoomGraph())
        assert survived["fact"].id


async def test_application_builds_from_a_provider_without_importing_the_runtime(live_env) -> None:
    """RED-by-absence — parity guard for the C10 handoff shape."""
    from memory_server.application import MemoryApplication, MemoryPolicy
    app, provider = live_env
    ruled = MemoryApplication.from_provider(provider, policy=MemoryPolicy(max_active_beliefs=7))
    assert ruled.describe() == {"policy": {"max_active_beliefs": 7}} and app.memory is not None
    class SqliteHandle:
        _session_factory = object()  # the RuntimeDependencies.sqlite shape
    duck = MemoryApplication.from_provider(SqliteHandle())
    assert duck.memory is not None and "memory_server" not in json.dumps(duck.describe())
    forbidden = ("memory_server.runtime", "memory_server.server", "memory_server.providers",
                 "memory_server.plugins", "memory_server.transports")
    for path in sorted(APPLICATION_DIR.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        modules = [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names]
        modules += [n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module]
        assert not any(m.startswith(forbidden) for m in modules), (path, modules)


def test_bulk_import_is_not_published_as_a_new_tool() -> None:
    """RED-by-absence — the registered tool inventory stays the C00 baseline."""
    names = _registered_tool_names()
    assert len(names) == 16 and "bulk_import" not in names, sorted(names)
    tools = json.loads(WIRE_SNAPSHOT.read_text(encoding="utf-8"))["mcp"]["tools"]
    assert set(names) == set(TOOL_NAMES_C00) == {tool["name"] for tool in tools}
    import memory_server.application as application
    assert [a for a in dir(application) if any(t in a.lower() for t in ("tool", "mcp", "register"))] == []


async def test_bulk_import_use_case_keeps_its_api_entry_point_and_result_shape(tmp_path) -> None:
    """RED-by-absence — the strangler import path and the API result shape."""
    import inspect as inspect_module

    from memory_server.application import MemoryApplication, MemoryService
    memory_md = tmp_path / "MEMORY.md"
    memory_md.write_text("# MEMORY\n\n- thanks, ok\n- User prefers concise terminal-friendly responses.\n",
                         encoding="utf-8")
    assert parse_memory_md(memory_md) == ["thanks, ok",
                                          "User prefers concise terminal-friendly responses."]
    params = inspect_module.signature(import_memory_md).parameters
    assert [(n, p.kind.name, p.default) for n, p in params.items()] == [
        ("provider", "POSITIONAL_OR_KEYWORD", inspect_module.Parameter.empty),
        ("path", "POSITIONAL_OR_KEYWORD", inspect_module.Parameter.empty),
        ("source", "KEYWORD_ONLY", "MEMORY.md"), ("gate", "KEYWORD_ONLY", None)]
    assert inspect_module.iscoroutinefunction(MemoryService.bulk_import)
    assert not hasattr(MemoryService, "tool") and not hasattr(MemoryApplication, "tool")
    provider = await _provider(tmp_path, "bulk.db")
    try:
        result = await import_memory_md(provider, memory_md, source="MEMORY.md")
        assert set(result) == {"imported", "skipped", "items", "skipped_items"}
        assert (result["imported"], result["skipped"]) == (1, 1)
    finally:
        await provider.close()


def test_every_scratch_path_used_by_this_module_is_inside_basetemp(tmp_path, tmp_path_factory) -> None:
    """RED-by-absence — nothing this module uses resolves outside basetemp."""
    basetemp = Path(str(tmp_path_factory.getbasetemp())).resolve()
    probe = tmp_path / "basetemp-probe.txt"
    probe.write_text("probe", encoding="utf-8")
    _SCRATCH_PATHS.extend([probe.resolve(), tmp_path.resolve()])
    module_root = Path(__file__).resolve().parents[1]
    assert not module_root.is_relative_to("/home/shtorm/cmms-runtime-refactor")
    candidates = [*_SCRATCH_PATHS, tmp_path, basetemp]
    assert len(candidates) >= 4
    for candidate in candidates:
        resolved = Path(candidate).resolve()
        assert resolved.is_relative_to(basetemp), f"scratch path outside basetemp: {resolved}"
        assert not any(resolved == r or resolved.is_relative_to(r) for r in LIVE_DATA_ROOTS), resolved


async def test_learn_forwards_native_submitter_to_ingestion(live_env, monkeypatch) -> None:
    from memory_server.application import LearnCommand, MemoryService
    from memory_server.application import memory_service as ms
    from tests.test_llm_extraction import REGEX_TEXT
    _, provider = live_env
    captured, seen = [], []
    real = ms.MemoryIngestionService
    class Capturing(real):
        def __init__(self, *a, **k):
            super().__init__(*a, **k); captured.append(self)
        async def learn(self, **kwargs):
            seen.append(kwargs.get("llm_extractor")); return await super().learn(**kwargs)
    monkeypatch.setattr(ms, "MemoryIngestionService", Capturing)
    marker, spy = object(), (lambda text: {"facts": [], "decisions": []})
    await MemoryService(provider._session_factory, native_submitter=marker).learn(
        LearnCommand(text=REGEX_TEXT, llm_extractor=spy))
    assert captured[0]._native_submitter is marker and seen[0] is spy

async def test_api_learn_forwards_keyword_only_submitter(live_env, monkeypatch) -> None:
    from memory_server.application import LearnCommand
    from memory_server.application import memory_service as ms
    _, provider = live_env
    captured = []
    real = ms.MemoryIngestionService
    class Capturing(real):
        def __init__(self, *a, **k):
            super().__init__(*a, **k); captured.append(self)
        async def learn(self, **kwargs):
            return {"facts": [], "decisions": [], "skills": [], "beliefs": [], "receipts": []}
    monkeypatch.setattr(ms, "MemoryIngestionService", Capturing)
    marker, spy = object(), (lambda text: {"facts": [], "decisions": []})
    await api_learn(provider, text="Docker is container", llm_extractor=spy, native_submitter=marker)
    assert captured[0]._native_submitter is marker
    assert "native_submitter" not in LearnCommand.__dataclass_fields__
    assert not hasattr(LearnCommand(text="t"), "native_submitter")

def test_api_learn_public_parameters_unchanged() -> None:
    import inspect as inspect_module
    from memory_server.api.learn import learn
    params = list(inspect_module.signature(learn).parameters.values())
    assert [p.name for p in params[:9]] == [
        "provider", "text", "source", "extract_beliefs", "min_belief_confidence",
        "llm_extractor", "llm_timeout_seconds", "llm_max_input_chars", "llm_confidence_gate"]
    empty = inspect_module.Parameter.empty
    assert [p.default for p in params[:9]] == [empty, empty, "user", False, 0.6, None, None, None, None]
    assert params[9].name == "native_submitter" and params[9].kind is inspect_module.Parameter.KEYWORD_ONLY
    assert params[9].default is None
    assert all(p.kind is inspect_module.Parameter.POSITIONAL_OR_KEYWORD for p in params[:9])

async def test_memory_application_legacy_path_has_no_submitter(live_env) -> None:
    app, _ = live_env
    assert app.memory._native_submitter is None
