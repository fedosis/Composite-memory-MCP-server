"""C00 runtime-contract baseline — pure characterization, no production edits.

Captures the real MCP stdio wire behaviour and the real Hermes native provider
surface of base ``3c5f11a7cbe76f11a85116699e9b0b5d97c3c59e`` into
``tests/fixtures/runtime_contracts.json`` (DETAIL §4.1 steps 5-7, §2.3 B1-B6)
and re-proves it on every run by driving the same transports again.  It is
characterization, never RED (§1.3).  Every entry carries the raw transcript of a
live transport plus its SHA-256, and
``test_baseline_snapshots_were_produced_by_real_transports`` refuses a fixture
the transports do not reproduce.

Boundaries crossed (§1.4): real ``memory-server serve`` (``cli.py`` ->
``server.py:1466-1467``) over real stdio (``tests/test_ping.py:8-17`` pattern);
``server.py:86-108`` projection envelope; real ``HermesProvider``
(``provider.py:1165``, ``:1542-1572``) on a real SQLite engine; canonical rows
through ``api/remember.py`` / ``api/learn.py`` -> ``services/ingestion``.

Isolation (§1.2): every child runs with ``cwd`` inside the C00 scratch root and
synthetic ``HOME``/``HERMES_HOME``/``TMPDIR``; only scratch SQLite URLs are
bound.  ``HF_HUB_OFFLINE``/``TRANSFORMERS_OFFLINE`` are pinned so no embedding
model can be downloaded; embedder-dependent surfaces are therefore captured as
the real offline refusal, never as a fabricated success.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import os
import re
import shutil
import sqlite3
import uuid
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "runtime_contracts.json"
CONTRACTS_DIR = REPO_ROOT / "contracts"
CODER_SCRATCH_ROOT = Path(
    "/home/shtorm/.hermes/profiles/coder/cache/scratch/cmms-runtime-refactor-exclusive"
)
SCRATCH = Path(os.environ.get("CMMS_C00_SCRATCH", str(CODER_SCRATCH_ROOT / "C00")))
BASE_REPO = Path(os.environ.get("CMMS_C00_REPO", str(SCRATCH / "base-3c5f11a7")))
PYTHON = Path(os.environ.get("CMMS_C00_PY", "/home/shtorm/memory-server/.venv/bin/python"))
SERVER_BIN = Path(
    os.environ.get("CMMS_C00_SERVER_BIN", "/home/shtorm/memory-server/.venv/bin/memory-server")
)
BASE_COMMIT = "3c5f11a7cbe76f11a85116699e9b0b5d97c3c59e"
CAPTURE_MODE = os.environ.get("CMMS_C00_CAPTURE") == "1"
LIVE_DATA_ROOTS = (Path("/home/shtorm/.hermes/data"), Path("/home/shtorm/memory-server/data"))

#: The 16 tools registered on the FastMCP app (B1) and the 14 native schemas (B5).
EXPECTED_MCP_TOOLS = (
    "ping", "search", "get_context", "remember", "learn", "semantic_search",
    "graph_search", "add_relation", "invalidate", "route", "audit", "metrics",
    "set_belief", "get_belief", "resolve_conflict", "reflect",
)
EXPECTED_NATIVE_TOOLS = (
    "ping", "search", "remember", "get_context", "learn", "semantic_search",
    "graph_search", "route", "audit", "metrics", "set_belief", "get_belief",
    "resolve_conflict", "reflect",
)
NORMALIZATION_WHITELIST = (
    "rfc4122-uuid -> <uuid>",
    "iso8601-timestamp -> <timestamp>",
    "unix-epoch-gauge (>=1e8) -> <epoch>",
    "prometheus-*_sum-latency-sample -> <number>",
    "prometheus-sample-values -> family/label projection (metrics entry only)",
    "scratch-absolute-path -> <scratch>",
)

_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_TS = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?")
_EPOCH = re.compile(r"(?<![\w.])(?:1\d{8,}(?:\.\d+)?|\d(?:\.\d+)?e\+0?\d{1,2})(?![\w.])")
_SUM = re.compile(r"(?m)^([A-Za-z_:][A-Za-z0-9_:]*_sum)\s+[^\s]+(\s*)$")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _inside(child: Path, parent: Path) -> bool:
    child, parent = Path(child).resolve(), Path(parent).resolve()
    return child == parent or parent in child.parents


def _normalize(text: str) -> str:
    """Apply the closed normalization whitelist (ids/timestamps/wall-clock only)."""
    for pattern, replacement in (
        (_UUID, "<uuid>"), (_TS, "<timestamp>"), (_EPOCH, "<epoch>")
    ):
        text = pattern.sub(replacement, text)
    return _SUM.sub(r"\1 <number>", text).replace(str(SCRATCH), "<scratch>")


def _metrics_projection(text: str) -> str:
    """Family/label projection of a prometheus snapshot (values are volatile)."""
    families, samples = [], []
    for line in text.splitlines():
        if line.startswith("# TYPE "):
            name, _, kind = line[len("# TYPE "):].partition(" ")
            families.append([name, kind.strip()])
        elif line and not line.startswith("#"):
            samples.append(line.split(" ", 1)[0])
    return json.dumps({"families": sorted(families), "samples": sorted(set(samples))})


def _project(key: str, raw: str) -> str:
    return _metrics_projection(raw) if key.split(".")[-1] == "metrics" else _normalize(raw)


def _sandbox_env(wire: Path) -> dict[str, str]:
    """``env -i``-style child environment, entirely inside scratch (§1.2 allowlist)."""
    home, tmp = SCRATCH / "home", SCRATCH / "tmp"
    (home / ".hermes").mkdir(parents=True, exist_ok=True)
    tmp.mkdir(parents=True, exist_ok=True)
    wire.mkdir(parents=True, exist_ok=True)
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONPATH": f"{REPO_ROOT}:{SRC_ROOT}",
        "HOME": str(home), "HERMES_HOME": str(home / ".hermes"), "TMPDIR": str(tmp),
        "PYTHONHASHSEED": "0", "LC_ALL": "C.UTF-8", "TZ": "UTC",
        "MEMORY_SERVER_DB_URL": f"sqlite+aiosqlite:///{wire / 'memory.db'}",
        "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
    }


#: Fixed happy-path sequence; the scratch DB is rebuilt empty on every capture so
#: derived counts stay stable.  ``invalidate``/``resolve_conflict`` run after it,
#: using the identifiers the earlier calls produced.
MCP_HAPPY_ARGS: tuple[tuple[str, dict[str, Any]], ...] = (
    ("ping", {}),
    ("search", {"query": "nonexistent-xyz"}),
    ("get_context", {"task": "how do I deploy the service"}),
    ("semantic_search", {"query": "which port does the service listen on"}),
    ("graph_search", {"entity_id": "seed-node-alpha"}),
    ("route", {"query": "which port does the service listen on"}),
    ("get_belief", {"proposition": "Baseline"}),
    ("reflect", {"mode": "overview"}),
    ("remember", {"subject": "PostgreSQL", "predicate": "stores", "object": "user data", "source": "baseline"}),
    ("remember", {"subject": "Caddy", "predicate": "serves", "object": "static files", "source": "baseline"}),
    ("add_relation", {"source_id": "seed-node-alpha", "target_id": "seed-node-beta", "relation_type": "supports"}),
    ("graph_search", {"relation_type": "supports"}),
    ("learn", {"text": "Caddy is a web server", "source": "baseline"}),
    ("set_belief", {"proposition": "Baseline proposition one", "confidence": 0.7, "source": "baseline"}),
    ("set_belief", {"proposition": "Baseline proposition two", "confidence": 0.4, "source": "baseline"}),
    ("get_belief", {"proposition": "Baseline"}),
    ("reflect", {"mode": "overview"}),
    ("audit", {"audit_type": "full"}),
    ("metrics", {}),
)
#: One invalid-input call per tool (wrong type or missing required argument).
MCP_INVALID_ARGS = {
    "ping": {"__unexpected__": 1}, "search": {"limit": "seven"}, "get_context": {},
    "remember": {"subject": 3, "predicate": "p", "object": "o"},
    "learn": {"text": ["not", "a", "string"]}, "semantic_search": {"top_k": "many"},
    "graph_search": {"query": 12}, "add_relation": {"source_id": "a"},
    "invalidate": {"memory_id": 5, "memory_type": None, "reason": "r"},
    "route": {"score_threshold": "high"}, "audit": {"audit_type": 7},
    "metrics": {"__unexpected__": 1}, "set_belief": {"confidence": "high"},
    "get_belief": {"limit": "many"}, "resolve_conflict": {"belief_a_id": "a"},
    "reflect": {"mode": 5},
}
#: Tools whose signature accepts no arguments: an extra key is ignored, so the
#: baseline records a success rather than inventing a failure.
ARGUMENTLESS_TOOLS = ("ping", "metrics")

PROJECTION_DRIVER = '''\
"""Generated by tests/test_runtime_baseline.py (C00).

Drives the REAL FastMCP app and the REAL server.py tools over real stdio; only
the projection-unavailable *condition* (provider.py:170-171) is injected at the
dependency boundary, because a standalone-mode server cannot enter that state
through configuration.  The injection stays inert while the lifespan builds the
stores, so server startup itself is real.
"""
from memory_server import server

_real_graph_router = server._get_graph_router


async def _unavailable_graph_router():
    if server._outbox_worker is None:      # lifespan startup path: build for real
        return await _real_graph_router()
    raise RuntimeError("E_PROJECTION_UNAVAILABLE: graph requires migration")


server._get_graph_router = _unavailable_graph_router
server.mcp.run(transport="stdio")
'''


def _seed_graph(wire: Path) -> dict[str, str]:
    """Real ``SimpleGraph`` snapshot used as reproducible input state.

    ``add_relation`` refuses unknown endpoints (``api/add_relation.py:27-33``)
    and the MCP ``remember`` tool only writes graph nodes when
    ``evidence.derived_from`` is supplied (``api/remember.py:108-131``), which
    the tool cannot express; the baseline therefore provides real input state
    through the real serialiser.
    """
    from memory_server.providers.graph_provider import SimpleGraph

    snapshot = wire / "data" / "graph.json"
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    graph = SimpleGraph(snapshot_path=snapshot)
    graph.add_node(id="seed-node-alpha", type="fact", name="PostgreSQL stores user data")
    graph.add_node(id="seed-node-beta", type="fact", name="Caddy serves static files")
    graph.save_snapshot(snapshot)
    assert snapshot.exists()
    return {"snapshot_path": str(snapshot), "source_node": "seed-node-alpha", "target_node": "seed-node-beta"}


def _record(store: dict[str, Any], key: str, result: Any) -> str:
    raw = result.content[0].text
    store[key] = {
        "raw": raw, "transcript_sha256": _sha(raw), "observed": _project(key, raw),
        "isError": bool(result.isError),
    }
    return raw


async def _stdin_session(wire: Path, driver: Path | None, script) -> dict[str, Any]:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    params = StdioServerParameters(
        command=str(PYTHON) if driver else str(SERVER_BIN),
        args=[str(driver)] if driver else ["serve"],
        env=_sandbox_env(wire), cwd=str(wire),
    )
    out: dict[str, Any] = {"entries": {}}

    async def call(session, key, name, arguments):
        return _record(
            out["entries"], key,
            await asyncio.wait_for(session.call_tool(name, arguments=arguments), timeout=90),
        )

    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await script(session, call, out)
    return out


async def _main_script(session, call, out) -> None:
    init = await session.initialize()
    out["initialize"] = {
        "protocolVersion": init.protocolVersion,
        "serverInfo": {"name": init.serverInfo.name, "version": init.serverInfo.version},
        "capabilities": json.loads(json.dumps(
            init.capabilities.model_dump(by_alias=True, exclude_none=True), default=str)),
    }
    out["tools"] = [
        {"name": t.name, "description": t.description or "", "input_schema": t.inputSchema}
        for t in (await session.list_tools()).tools
    ]
    facts, beliefs = [], []
    for index, (name, arguments) in enumerate(MCP_HAPPY_ARGS):
        text = await call(session, f"happy.{index}.{name}", name, arguments)
        if name == "remember":
            facts.append(json.loads(text)["fact"]["id"])
        elif name == "set_belief":
            beliefs.append(json.loads(text)["belief"]["id"])
    await call(session, "happy.invalidate", "invalidate", {
        "memory_id": facts[0], "memory_type": "fact", "reason": "baseline invalidation"})
    await call(session, "happy.resolve_conflict", "resolve_conflict", {
        "belief_a_id": beliefs[0], "belief_b_id": beliefs[1], "resolution": "keep_a"})
    for name in EXPECTED_MCP_TOOLS:
        await call(session, f"invalid.{name}", name, MCP_INVALID_ARGS[name])


async def _projection_script(session, call, out) -> None:
    await session.initialize()
    await call(session, "graph_search", "graph_search", {"query": "anything"})
    await call(session, "add_relation", "add_relation",
               {"source_id": "a", "target_id": "b", "relation_type": "supports"})


def _read_effects(db: Path) -> dict[str, Any]:
    """Real SQLite effects of a captured session (B6).

    Outbox *status* is retry-timing dependent (the offline embedder fails every
    attempt), so only canonical (record_type, operation) pairs are asserted.
    """
    connection = sqlite3.connect(db)
    try:
        facts = sorted(connection.execute(
            "SELECT subject, predicate, object, confidence, lifecycle_state FROM facts"))
        receipts = connection.execute(
            "SELECT memory_type, COUNT(*) FROM receipts GROUP BY 1 ORDER BY 1").fetchall()
        outbox = connection.execute(
            "SELECT record_type, operation, COUNT(*) FROM outbox_entries "
            "GROUP BY 1, 2 ORDER BY 1, 2").fetchall()
        beliefs = sorted(connection.execute(
            "SELECT proposition, confidence, lifecycle_state, version FROM beliefs"))
        events = connection.execute("SELECT COUNT(*) FROM lifecycle_events").fetchone()[0]
    finally:
        connection.close()
    return {
        "facts": [list(r) for r in facts],
        "receipts_by_memory_type": dict(receipts),
        "outbox_by_record_and_operation": {f"{r[0]}:{r[1]}": r[2] for r in outbox},
        "beliefs": [list(r) for r in beliefs],
        "lifecycle_events": events,
    }


def _capture_mcp() -> dict[str, Any]:
    wire = SCRATCH / "wire"
    shutil.rmtree(wire, ignore_errors=True)
    seed = _seed_graph(wire)
    main = asyncio.run(_stdin_session(wire, None, _main_script))
    main["canonical_effects"] = _read_effects(wire / "memory.db")

    driver_dir = SCRATCH / "wire-projection"
    shutil.rmtree(driver_dir, ignore_errors=True)
    driver_dir.mkdir(parents=True, exist_ok=True)
    driver = driver_dir / "projection_driver.py"
    driver.write_text(PROJECTION_DRIVER, encoding="utf-8")
    projection = asyncio.run(_stdin_session(driver_dir, driver, _projection_script))
    return {"main": main, "seed_graph": seed, "projection": projection["entries"]}


def _capture_contracts() -> dict[str, Any]:
    """Committed ``contracts/*.schema.json`` as found, drift included (B1)."""
    tools, drift = {}, {}
    files = sorted(p.name for p in CONTRACTS_DIR.glob("*.schema.json"))
    for path in sorted(CONTRACTS_DIR.glob("*.schema.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        name = data.get("tool", {}).get("name")
        if name is None:
            continue
        props = sorted(data.get("input", {}).get("properties", {}))
        tools[name] = props
    return {
        "files": files,
        "tool_contracts": tools,
        "tools_without_contract": sorted(set(EXPECTED_MCP_TOOLS) - set(tools)),
        "property_drift": drift,
    }


def _contract_drift(mcp_tools: list[dict[str, Any]]) -> dict[str, Any]:
    """Property-level drift between each contract file and the live tool schema."""
    live = {t["name"]: sorted(t["input_schema"].get("properties", {})) for t in mcp_tools}
    contracts = _capture_contracts()["tool_contracts"]
    drift = {}
    for name, props in sorted(contracts.items()):
        recorded = live.get(name, [])
        if props != recorded:
            drift[name] = {
                "only_in_contract": sorted(set(props) - set(recorded)),
                "only_in_live_tool": sorted(set(recorded) - set(props)),
            }
    return drift


def _provenance() -> dict[str, Any]:
    import platform

    def version(module: str) -> str | None:
        try:
            return getattr(__import__(module), "__version__", None)
        except Exception:  # pragma: no cover - provenance only
            return None

    return {
        "interpreter": {
            "path": str(PYTHON), "version": platform.python_version(),
            "implementation": platform.python_implementation(),
        },
        "server_command": [str(SERVER_BIN), "serve"],
        "transport": "mcp.client.stdio.stdio_client",
        "dependency_versions": {
            name: version(name) for name in ("mcp", "pydantic", "sqlalchemy", "aiosqlite", "sqlite3")
        },
        "repo_root": str(REPO_ROOT), "base_repo": str(BASE_REPO), "scratch_root": str(SCRATCH),
    }


def _capture_native(monkeypatch: pytest.MonkeyPatch, synthetic: Any) -> dict[str, Any]:
    """Drive the real ``HermesProvider`` inside the synthetic harness (B5/B6)."""
    import memory_server.providers.embedding_provider as embedding_module
    from memory_server.plugins.hermes.provider import HermesProvider
    from memory_server.providers.embedding_provider import MockEmbeddingProvider

    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider)
    root, home, db = synthetic.root, synthetic.root / "native-home", synthetic.root / "native.db"
    home.mkdir(parents=True, exist_ok=True)

    def build():
        provider = HermesProvider()
        provider.initialize(
            "c00-baseline",
            config={"db_url": f"sqlite+aiosqlite:///{db}", "path": str(root)},
            hermes_home=str(home),
        )
        return provider

    observed: dict[str, Any] = {"envelopes": {}, "calls": {}, "unknown_tool_error": None}

    # 1. not-initialized envelope (provider.py:1550-1551), before initialize().
    observed["envelopes"]["not_initialized"] = HermesProvider().handle_tool_call("ping", {})

    provider = build()
    try:
        observed["schemas"] = [
            {"name": s["name"], "description": s.get("description", ""),
             "input_schema": s.get("inputSchema", {})}
            for s in provider.get_tool_schemas()
        ]
        observed["calls"]["remember"] = provider.handle_tool_call("remember", {
            "subject": "baseline-native", "predicate": "records",
            "object": "canonical rows", "source": "baseline"})
        observed["calls"]["learn"] = provider.handle_tool_call(
            "learn", {"text": "Baseline native fact", "source": "baseline"})
        # 2. generic error envelope (provider.py:1572) from a real failing call;
        #    ``invalidate``/``add_relation`` are MCP-only tools (provider.py:1576-1591).
        observed["envelopes"]["generic_error"] = provider.handle_tool_call("resolve_conflict", {
            "belief_a_id": "11111111-1111-1111-1111-111111111111",
            "belief_b_id": "22222222-2222-2222-2222-222222222222",
            "resolution": "keep_a"})
        observed["canonical_effects"] = _read_effects(db)
        # 3. unknown tool raises ValueError (provider.py:1560-1562) — an
        #    initialized provider, otherwise the not-initialized guard answers first.
        try:
            provider.handle_tool_call("not_a_cmms_tool", {})
        except ValueError as exc:
            observed["unknown_tool_error"] = {"type": "ValueError", "message": _normalize(str(exc))}
        else:  # pragma: no cover - contract breach
            raise AssertionError("unknown tool must raise ValueError")
    finally:
        provider.shutdown()

    # 4. projection-unavailable envelope (provider.py:1568-1570) from a real
    #    degraded projection produced by the real resolver over a symlinked
    #    store (shape established at tests/test_profile_data_isolation.py:560-590).
    degraded_home = root / "degraded-home"
    (degraded_home / "data").mkdir(parents=True, exist_ok=True)
    external = root / "degraded-external"
    external.mkdir(exist_ok=True)
    link = degraded_home / "data" / "graph.json"
    link.symlink_to(external)
    degraded = HermesProvider()
    try:
        degraded.initialize("c00-degraded", hermes_home=str(degraded_home))
        assert "graph" in degraded._storage_layout.unavailable_projections
        observed["envelopes"]["projection_unavailable"] = degraded.handle_tool_call(
            "graph_search", {"query": "anything"})
    finally:
        degraded.shutdown()

    # 5. close-failed envelope (provider.py:1552-1558): a real close fault at the
    #    external provider edge.  The fault is injected on the SQLite provider's
    #    close so the handle is RETAINED (``provider.py:649-656``) — with any other
    #    resource failing while the provider closes, ``_provider`` becomes None and
    #    the not-initialized guard (``:1550``) answers first instead.
    failing = build()

    async def _boom() -> None:
        raise RuntimeError("injected close failure (baseline capture)")

    fault = pytest.MonkeyPatch()
    fault.setattr(failing._provider, "close", _boom)
    try:
        with pytest.raises(RuntimeError):
            failing.shutdown()
    finally:
        fault.undo()
    assert failing._cleanup_failed is True and failing._provider is not None
    observed["envelopes"]["close_failed"] = failing.handle_tool_call("ping", {})
    return observed


def _digest(entries: dict[str, Any]) -> str:
    return _sha(json.dumps({k: v["observed"] for k, v in sorted(entries.items())},
                           sort_keys=True, ensure_ascii=False))


def _build_fixture(live: dict[str, Any]) -> dict[str, Any]:
    mcp, native = live["mcp"], live["native"]
    entries = mcp["main"]["entries"]
    return {
        "schema": "cmms.runtime_contracts.v1",
        "provenance": {
            "card": "C00", "base_commit": BASE_COMMIT,
            "capture_run_id": str(uuid.uuid4()), "capture_mode": CAPTURE_MODE,
            "normalization_whitelist": list(NORMALIZATION_WHITELIST),
            "provenance": _provenance(),
            "tool_names": list(EXPECTED_MCP_TOOLS),
            "native_tool_names": list(EXPECTED_NATIVE_TOOLS),
            "entry_keys": sorted(entries),
            "entry_transcript_hashes": {k: v["transcript_sha256"] for k, v in sorted(entries.items())},
            "entry_digest": _digest(entries),
            "projection_entry_hashes": {k: v["transcript_sha256"] for k, v in sorted(mcp["projection"].items())},
            "seed_graph": mcp["seed_graph"],
            "scratch_root": str(SCRATCH),
            "live_data_roots": [str(r) for r in LIVE_DATA_ROOTS],
        },
        "mcp": {
            "initialize": mcp["main"]["initialize"],
            "tools": mcp["main"]["tools"],
            "entries": entries,
            "canonical_effects": mcp["main"]["canonical_effects"],
            "projection_unavailable": mcp["projection"],
            "contracts": _capture_contracts(),
            "contract_drift": _contract_drift(mcp["main"]["tools"]),
        },
        "native": native,
    }


@functools.lru_cache(maxsize=1)
def _capture_once(tmp_root: str) -> tuple[dict[str, Any], tuple[Path, ...]]:
    mcp = _capture_mcp()
    monkeypatch, paths = pytest.MonkeyPatch(), [SCRATCH, BASE_REPO, Path(tmp_root)]
    try:
        from tests.synthetic_storage_env import activate_synthetic_storage_env

        synthetic = activate_synthetic_storage_env(Path(tmp_root), monkeypatch)
        native = _capture_native(monkeypatch, synthetic)
        paths.extend(p for p in (synthetic.root, synthetic.home_dir, synthetic.tmp_dir) if p)
    finally:
        monkeypatch.undo()
    return {"mcp": mcp, "native": native}, tuple(paths)


@pytest.fixture(scope="session")
def baseline(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    """Capture the live contracts once per session, then load the fixture."""
    live, paths = _capture_once(str(tmp_path_factory.mktemp("c00-native")))
    if CAPTURE_MODE:
        FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
        FIXTURE_PATH.write_text(
            json.dumps(_build_fixture(live), indent=1, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8")
    assert FIXTURE_PATH.exists(), (
        f"runtime contract fixture missing: {FIXTURE_PATH}; capture with CMMS_C00_CAPTURE=1")
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return {"live": live, "fixture": fixture, "paths": paths}


def _entries(baseline: dict[str, Any], prefix: str) -> dict[str, Any]:
    """MCP entries of the live capture (``mcp.main.entries``) or of the fixture."""
    mcp = (baseline["live"] if prefix == "live" else baseline["fixture"])["mcp"]
    return mcp["main"]["entries"] if "main" in mcp else mcp["entries"]


# --------------------------------------------------------------------------- #
# Tests (DETAIL §4.1)
# --------------------------------------------------------------------------- #


def test_baseline_provenance_and_normalization_rules_are_complete(baseline) -> None:
    """The fixture is auditable: commit, interpreter, tools, hashes, whitelist."""
    provenance = baseline["fixture"]["provenance"]
    recorded = provenance["provenance"]
    assert provenance["base_commit"] == BASE_COMMIT
    assert recorded["interpreter"]["path"] == str(PYTHON)
    assert recorded["interpreter"]["version"]
    assert recorded["transport"] == "mcp.client.stdio.stdio_client"
    assert recorded["server_command"] == [str(SERVER_BIN), "serve"]
    assert provenance["tool_names"] == list(EXPECTED_MCP_TOOLS)
    assert provenance["native_tool_names"] == list(EXPECTED_NATIVE_TOOLS)
    assert set(provenance["entry_transcript_hashes"]) == set(provenance["entry_keys"])
    assert len(provenance["entry_transcript_hashes"]) >= 16
    assert tuple(provenance["normalization_whitelist"]) == NORMALIZATION_WHITELIST
    assert provenance["entry_digest"] == _digest(baseline["fixture"]["mcp"]["entries"])


def test_mcp_stdio_initialize_capabilities_match_snapshot(baseline) -> None:
    """Protocol version, server identity and capabilities over real stdio."""
    live = baseline["live"]["mcp"]["main"]["initialize"]
    assert live == baseline["fixture"]["mcp"]["initialize"]
    assert live["serverInfo"]["name"] == "CompositeMemoryServer"
    assert live["protocolVersion"]
    assert "tools" in live["capabilities"]


def test_mcp_stdio_tool_inventory_and_schemas_match_snapshot(baseline) -> None:
    """The 16 registered tools, their live schemas and the committed contracts."""
    live_tools = baseline["live"]["mcp"]["main"]["tools"]
    assert live_tools == baseline["fixture"]["mcp"]["tools"]
    assert [t["name"] for t in live_tools] == list(EXPECTED_MCP_TOOLS)
    contracts = baseline["fixture"]["mcp"]["contracts"]
    assert contracts == _capture_contracts()
    # Recorded, not repaired: three registered tools have no contract file and two
    # contract files drifted from the live tool schema (C00 finding).
    assert contracts["tools_without_contract"] == ["add_relation", "invalidate", "metrics"]
    assert set(contracts["tools_without_contract"]) | set(contracts["tool_contracts"]) == set(
        EXPECTED_MCP_TOOLS)
    assert sorted(baseline["fixture"]["mcp"]["contract_drift"]) == [
        "graph_search", "reflect"]
    assert baseline["fixture"]["mcp"]["contract_drift"] == _contract_drift(live_tools)


@pytest.mark.parametrize("tool", EXPECTED_MCP_TOOLS)
def test_mcp_stdio_call_tool_happy_paths_match_snapshot(baseline, tool) -> None:
    """Every happy-path call re-run over real stdio equals the captured result."""
    keys = [k for k in _entries(baseline, "live")
            if k.startswith("happy.") and k.split(".")[-1] == tool]
    assert keys, f"no happy-path capture for {tool}"
    for key in keys:
        live, fixture = _entries(baseline, "live")[key], _entries(baseline, "fixture")[key]
        assert live["isError"] is False, live["raw"]
        assert live["observed"] == fixture["observed"]
        # The raw transcript itself carries fresh ids/timestamps, so only its
        # self-consistency (and the normalized observation) can be reproduced.
        assert live["transcript_sha256"] == _sha(live["raw"])


@pytest.mark.parametrize("tool", EXPECTED_MCP_TOOLS)
def test_mcp_stdio_invalid_input_error_envelope_matches_snapshot(baseline, tool) -> None:
    """Raw framework validation envelopes, captured per tool (never normalized)."""
    key = f"invalid.{tool}"
    live, fixture = _entries(baseline, "live")[key], _entries(baseline, "fixture")[key]
    assert live["observed"] == fixture["observed"]
    assert live["isError"] == fixture["isError"]
    if tool in ARGUMENTLESS_TOOLS:
        assert live["isError"] is False
    else:
        assert live["isError"] is True
        assert fixture["raw"].startswith(f"Error executing tool {tool}: ")


def test_mcp_stdio_projection_unavailable_envelope_is_a_string_result(baseline) -> None:
    """``server.py:86-108``: the projection failure is a JSON string, not a raise."""
    fixture = baseline["fixture"]["mcp"]["projection_unavailable"]
    live = baseline["live"]["mcp"]["projection"]
    for tool in ("graph_search", "add_relation"):
        assert live[tool]["observed"] == fixture[tool]["observed"]
        assert live[tool]["isError"] is False, "the decorator must not raise"
        payload = json.loads(fixture[tool]["raw"])
        assert payload["error"] == "E_PROJECTION_UNAVAILABLE"
        assert payload["message"].startswith("E_PROJECTION_UNAVAILABLE:")
        assert payload["hint"] == (
            "Run `memory-server migrate-profile-storage` to rebuild unavailable projections.")


def test_native_provider_tool_schemas_match_snapshot(baseline) -> None:
    """The 14 native schemas, from a real provider, with no ``mcp_`` prefix."""
    live = baseline["live"]["native"]["schemas"]
    assert live == baseline["fixture"]["native"]["schemas"]
    assert [s["name"] for s in live] == list(EXPECTED_NATIVE_TOOLS)
    assert all(not s["name"].startswith("mcp_") for s in live)


def test_native_provider_tool_call_envelopes_match_snapshot(baseline) -> None:
    """The five ``provider.py:1542-1572`` envelope shapes."""
    live, fixture = baseline["live"]["native"], baseline["fixture"]["native"]
    assert live["envelopes"] == fixture["envelopes"]
    assert live["unknown_tool_error"] == fixture["unknown_tool_error"]
    assert live["envelopes"]["not_initialized"] == json.dumps(
        {"error": "HermesProvider not initialized"})
    assert live["envelopes"]["close_failed"] == json.dumps(
        {"error": "HermesProvider cleanup failed (close-failed); resources retained"})
    assert json.loads(live["envelopes"]["projection_unavailable"])["error"] == "E_PROJECTION_UNAVAILABLE"
    generic = json.loads(live["envelopes"]["generic_error"])
    assert generic["tool"] == "resolve_conflict"
    assert "must exist in the store" in generic["error"]


def test_native_unknown_tool_raises_value_error(baseline) -> None:
    """An unknown tool name is a ``ValueError``, never a success envelope."""
    error = baseline["fixture"]["native"]["unknown_tool_error"]
    assert error["type"] == "ValueError"
    assert error["message"] == "Unknown CMMS tool: not_a_cmms_tool"


def test_canonical_storage_effects_match_snapshot(baseline) -> None:
    """Real SQLite rows/receipts/outbox for remember/learn/set_belief/invalidate."""
    live_mcp = baseline["live"]["mcp"]["main"]["canonical_effects"]
    assert live_mcp == baseline["fixture"]["mcp"]["canonical_effects"]
    assert live_mcp["facts"] == [
        ["Caddy", "is", "a web server", 0.5, "active"],
        ["Caddy", "serves", "static files", 1.0, "active"],
        ["PostgreSQL", "stores", "user data", 1.0, "discarded"],
    ]
    assert live_mcp["receipts_by_memory_type"] == {"fact": 3, "belief": 2}
    assert live_mcp["outbox_by_record_and_operation"] == {
        "belief:index_belief": 2, "fact:index_fact": 3}
    assert live_mcp["lifecycle_events"] >= 2

    live_native = baseline["live"]["native"]["canonical_effects"]
    assert live_native == baseline["fixture"]["native"]["canonical_effects"]
    assert live_native["facts"] == [
        ["baseline-native", "records", "canonical rows", 1.0, "active"]]
    assert live_native["receipts_by_memory_type"] == {"fact": 1}
    assert live_native["outbox_by_record_and_operation"] == {"fact:index_fact": 1}
    # ``learn`` on this text extracts nothing (regex mode, no LLM configured):
    # the real empty result is captured, not a fabricated row.
    assert json.loads(baseline["live"]["native"]["calls"]["learn"])["facts"] == []


def test_baseline_snapshots_were_produced_by_real_transports(baseline) -> None:
    """Refuse a hand-assembled fixture: the live transports reproduce every entry."""
    provenance = baseline["fixture"]["provenance"]
    entries = baseline["fixture"]["mcp"]["entries"]
    assert entries, "fixture carries no MCP entries"
    for key, entry in sorted(entries.items()):
        assert set(entry) >= {"raw", "transcript_sha256", "observed", "isError"}
        assert entry["transcript_sha256"] == _sha(entry["raw"]), (
            f"{key}: stored raw transcript does not match its recorded hash")
        assert provenance["entry_transcript_hashes"][key] == entry["transcript_sha256"]
        assert entry["observed"] == _project(key, entry["raw"]), (
            f"{key}: fixture observation is not the projection of its own raw text")
        assert _entries(baseline, "live")[key]["observed"] == entry["observed"], (
            f"{key}: the live transport does not reproduce the fixture entry")
    assert provenance["entry_digest"] == _digest(_entries(baseline, "live"))
    for key, entry in baseline["fixture"]["mcp"]["projection_unavailable"].items():
        assert entry["transcript_sha256"] == _sha(entry["raw"])
        assert baseline["live"]["mcp"]["projection"][key]["observed"] == entry["observed"]
    assert baseline["live"]["native"]["schemas"] == baseline["fixture"]["native"]["schemas"]


def test_baseline_harness_paths_are_inside_scratch(baseline) -> None:
    """Isolation guard: every harness path is inside scratch, never a live root."""
    assert SCRATCH.is_dir() and _inside(SCRATCH, CODER_SCRATCH_ROOT)
    assert _inside(REPO_ROOT, SCRATCH), (
        f"the source under test must be the disposable copy, got {REPO_ROOT}")
    for path in baseline["paths"]:
        assert _inside(path, SCRATCH), f"harness path outside scratch: {path}"
        for live_root in LIVE_DATA_ROOTS:
            assert not _inside(path, live_root), f"harness touched a live root: {path}"
    assert str(SRC_ROOT) in _sandbox_env(SCRATCH / "wire").get("PYTHONPATH", "")
