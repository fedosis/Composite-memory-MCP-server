"""MCP wire contracts over disposable storage, plus optional C00 comparisons.

The immutable C00 input sequence runs through the real CLI/stdio boundary in
ordinary pytest. Only equality with the external oracle needs harness env.
"""
from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import os
import re
from functools import partial
from pathlib import Path
from typing import Any

import pytest

EXPECTED_TOOLS = (
    "ping", "search", "get_context", "remember", "learn", "semantic_search",
    "graph_search", "add_relation", "invalidate", "route", "audit", "metrics",
    "set_belief", "get_belief", "resolve_conflict", "reflect",
)
HAPPY_CASES = (
    ("happy.0.ping", "ping", {}),
    ("happy.1.search", "search", {"query": "nonexistent-xyz"}),
    ("happy.2.get_context", "get_context", {"task": "how do I deploy the service"}),
    ("happy.3.semantic_search", "semantic_search", {"query": "which port does the service listen on"}),
    ("happy.4.graph_search", "graph_search", {"entity_id": "seed-node-alpha"}),
    ("happy.5.route", "route", {"query": "which port does the service listen on"}),
    ("happy.6.get_belief", "get_belief", {"proposition": "Baseline"}),
    ("happy.7.reflect", "reflect", {"mode": "overview"}),
    ("happy.8.remember", "remember", {"subject": "PostgreSQL", "predicate": "stores", "object": "user data", "source": "baseline"}),
    ("happy.9.remember", "remember", {"subject": "Caddy", "predicate": "serves", "object": "static files", "source": "baseline"}),
    ("happy.10.add_relation", "add_relation", {"source_id": "seed-node-alpha", "target_id": "seed-node-beta", "relation_type": "supports"}),
    ("happy.11.graph_search", "graph_search", {"relation_type": "supports"}),
    ("happy.12.learn", "learn", {"text": "Caddy is a web server", "source": "baseline"}),
    ("happy.13.set_belief", "set_belief", {"proposition": "Baseline proposition one", "confidence": 0.7, "source": "baseline"}),
    ("happy.14.set_belief", "set_belief", {"proposition": "Baseline proposition two", "confidence": 0.4, "source": "baseline"}),
    ("happy.15.get_belief", "get_belief", {"proposition": "Baseline"}),
    ("happy.16.reflect", "reflect", {"mode": "overview"}),
    ("happy.17.audit", "audit", {"audit_type": "full"}),
    ("happy.18.metrics", "metrics", {}),
    ("happy.invalidate", "invalidate", {"memory_id": "__fact_id__", "memory_type": "fact", "reason": "baseline invalidation"}),
    ("happy.resolve_conflict", "resolve_conflict", {"belief_a_id": "__belief_a__", "belief_b_id": "__belief_b__", "resolution": "keep_a"}),
)
INVALID_CASES = (
    ("invalid.add_relation", "add_relation", {"source_id": "a"}),
    ("invalid.audit", "audit", {"audit_type": 7}),
    ("invalid.get_belief", "get_belief", {"limit": "many"}),
    ("invalid.get_context", "get_context", {}),
    ("invalid.graph_search", "graph_search", {"query": 12}),
    ("invalid.invalidate", "invalidate", {"memory_id": 5, "memory_type": None, "reason": "r"}),
    ("invalid.learn", "learn", {"text": ["not", "a", "string"]}),
    ("invalid.metrics", "metrics", {"__unexpected__": 1}),
    ("invalid.ping", "ping", {"__unexpected__": 1}),
    ("invalid.reflect", "reflect", {"mode": 5}),
    ("invalid.remember", "remember", {"subject": 3, "predicate": "p", "object": "o"}),
    ("invalid.resolve_conflict", "resolve_conflict", {"belief_a_id": "a"}),
    ("invalid.route", "route", {"score_threshold": "high"}),
    ("invalid.search", "search", {"limit": "seven"}),
    ("invalid.semantic_search", "semantic_search", {"top_k": "many"}),
    ("invalid.set_belief", "set_belief", {"confidence": "high"}),
)


REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def oracle() -> dict[str, Any]:
    value = os.environ.get("CMMS_C00_ORACLE")
    if not value:
        pytest.skip("C00 oracle comparison requires harness environment variable: CMMS_C00_ORACLE")
    path = Path(value)
    assert path.is_file(), f"CMMS_C00_ORACLE is not a file: {path}"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == "0f369d2079973c6d4798144f78730f134c07a3e56416a87f8c6b9188d7236338"
    return json.loads(path.read_text(encoding="utf-8"))


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _normalize(value: str, root: Path) -> str:
    value = re.sub(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "<uuid>", value)
    value = re.sub(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?", "<timestamp>", value)
    value = re.sub(r"(?<![\w.])(?:1\d{8,}(?:\.\d+)?|\d(?:\.\d+)?e\+0?\d{1,2})(?![\w.])", "<epoch>", value)
    value = re.sub(r"(?m)^([A-Za-z_:][A-Za-z0-9_:]*_sum)\s+[^\s]+(\s*)$", r"\1 <number>", value)
    return value.replace(str(root), "<scratch>")


def _observed(key: str, raw: str, root: Path) -> str:
    if key.rsplit(".", 1)[-1] == "metrics":
        families, samples = [], []
        for line in raw.splitlines():
            if line.startswith("# TYPE "):
                name, _, kind = line[7:].partition(" ")
                families.append([name, kind.strip()])
            elif line and not line.startswith("#"):
                samples.append(line.split(" ", 1)[0])
        return json.dumps({"families": sorted(families), "samples": sorted(set(samples))})
    return _normalize(raw, root)


def _env(root: Path) -> dict[str, str]:
    values = {
        "PATH": os.environ["PATH"], "PYTHONHASHSEED": "0", "PYTHONDONTWRITEBYTECODE": "1", "LC_ALL": "C", "TZ": "UTC",
        "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
        "HOME": str(root / "home"), "HERMES_HOME": str(root / "hermes"), "TMPDIR": str(root / "tmp"),
        "PYTHONPATH": f"{REPO_ROOT / 'src'}:{REPO_ROOT}",
        "MEMORY_SERVER_DATA_ROOT": str(root / "data"),
        "MEMORY_SERVER_DB_URL": f"sqlite+aiosqlite:///{root / 'data' / 'memory.db'}",
        "MEMORY_SERVER_LANCEDB_PATH": str(root / "data" / "lancedb"),
        "MEMORY_SERVER_GRAPH_SNAPSHOT_PATH": str(root / "data" / "graph.json"),
        "MEMORY_SERVER_VECTOR_BACKEND": "lancedb",
    }
    assert root == root.resolve() and Path(values["TMPDIR"]) == root / "tmp", (root, values["TMPDIR"])
    assert set(values) <= {"PATH", "PYTHONHASHSEED", "PYTHONDONTWRITEBYTECODE", "LC_ALL", "TZ", "HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "HOME", "HERMES_HOME", "TMPDIR", "PYTHONPATH", "MEMORY_SERVER_DATA_ROOT", "MEMORY_SERVER_DB_URL", "MEMORY_SERVER_LANCEDB_PATH", "MEMORY_SERVER_GRAPH_SNAPSHOT_PATH", "MEMORY_SERVER_VECTOR_BACKEND", "CMMS_C00_ORACLE"}
    for key in ("HOME", "HERMES_HOME", "TMPDIR", "MEMORY_SERVER_DATA_ROOT", "MEMORY_SERVER_LANCEDB_PATH", "MEMORY_SERVER_GRAPH_SNAPSHOT_PATH"):
        Path(values[key]).parent.mkdir(parents=True, exist_ok=True)
    return values


def _seed_graph(root: Path) -> None:
    from memory_server.providers.graph_provider import SimpleGraph

    snapshot = root / "data" / "graph.json"
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    graph = SimpleGraph(snapshot_path=snapshot)
    graph.add_node(id="seed-node-alpha", type="fact", name="PostgreSQL stores user data")
    graph.add_node(id="seed-node-beta", type="fact", name="Caddy serves static files")
    graph.save_snapshot(snapshot)


def _record(out: dict[str, Any], key: str, result: Any, *, root: Path) -> str:
    raw = result.content[0].text
    out[key] = {"raw": raw, "observed": _observed(key, raw, root), "isError": bool(result.isError), "transcript_sha256": _sha(raw)}
    assert out[key]["transcript_sha256"] == _sha(raw)
    return raw


async def _session(args: list[str], script, *, root: Path) -> dict[str, Any]:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    root.mkdir(parents=True, exist_ok=True)
    (root / "stdio").mkdir(parents=True, exist_ok=True)
    params = StdioServerParameters(command=args[0], args=args[1:], env=_env(root), cwd=str(root / "stdio"))
    out: dict[str, Any] = {}
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await script(session, out)
    return out


async def _main_script(session, out, *, root: Path) -> None:
    init = await session.initialize()
    out["initialize"] = {
        "protocolVersion": init.protocolVersion,
        "serverInfo": {"name": init.serverInfo.name, "version": init.serverInfo.version},
        "capabilities": json.loads(json.dumps(init.capabilities.model_dump(by_alias=True, exclude_none=True), default=str)),
    }
    out["tools"] = [
        {"name": tool.name, "description": tool.description or "", "input_schema": tool.inputSchema}
        for tool in (await session.list_tools()).tools
    ]
    facts, beliefs = [], []
    for key, name, arguments in HAPPY_CASES[:-2]:
        raw = await session.call_tool(name, arguments=arguments)
        text = _record(out, key, raw, root=root)
        if name == "remember":
            facts.append(json.loads(text)["fact"]["id"])
        if name == "set_belief":
            beliefs.append(json.loads(text)["belief"]["id"])
    for key, name, arguments in HAPPY_CASES[-2:]:
        arguments = dict(arguments)
        arguments["memory_id"] = facts[0] if "memory_id" in arguments else arguments.get("memory_id")
        if name == "invalidate":
            arguments = {"memory_id": facts[0], "memory_type": "fact", "reason": "baseline invalidation"}
        else:
            arguments = {"belief_a_id": beliefs[0], "belief_b_id": beliefs[1], "resolution": "keep_a"}
        _record(out, key, await session.call_tool(name, arguments=arguments), root=root)
    for key, name, arguments in INVALID_CASES:
        _record(out, key, await session.call_tool(name, arguments=arguments), root=root)


@pytest.fixture(scope="module")
def parity_capture(tmp_path_factory) -> dict[str, Any]:
    from tests.test_mcp_runtime_shutdown import _python

    root = tmp_path_factory.mktemp("mcp-parity")
    _seed_graph(root)
    return asyncio.run(_session(
        [_python(), "-m", "memory_server.cli", "serve"],
        partial(_main_script, root=root), root=root,
    ))


def test_mcp_stdio_initialize_capabilities_match_c00(oracle, parity_capture):
    assert parity_capture["initialize"] == oracle["mcp"]["initialize"]


def test_mcp_stdio_tool_inventory_and_schemas_match_c00(oracle, parity_capture):
    assert parity_capture["tools"] == oracle["mcp"]["tools"]
    files = sorted(p.name for p in Path("contracts").glob("*.schema.json"))
    contracts = {}
    for path in sorted(Path("contracts").glob("*.schema.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        name = data.get("tool", {}).get("name")
        if name is not None:
            contracts[name] = sorted(data.get("input", {}).get("properties", {}))
    assert files == oracle["mcp"]["contracts"]["files"]
    assert contracts == oracle["mcp"]["contracts"]["tool_contracts"]
    assert oracle["mcp"]["contracts"]["tools_without_contract"] == ["add_relation", "invalidate", "metrics"]


@pytest.mark.parametrize("case", HAPPY_CASES, ids=[case[0] for case in HAPPY_CASES])
def test_mcp_stdio_happy_paths_match_c00(oracle, parity_capture, case):
    key, _, _ = case
    expected = oracle["mcp"]["entries"][key]
    actual = parity_capture[key]
    assert actual["observed"] == expected["observed"]


@pytest.mark.parametrize("case", INVALID_CASES, ids=[case[0] for case in INVALID_CASES])
def test_mcp_stdio_invalid_inputs_match_c00(oracle, parity_capture, case):
    key, name, _ = case
    expected = oracle["mcp"]["entries"][key]
    actual = parity_capture[key]
    assert actual["observed"] == expected["observed"]
    assert actual["isError"] == expected["isError"]



@pytest.fixture(scope="module")
def projection_capture(tmp_path_factory):
    from tests.test_mcp_runtime_shutdown import _python

    root = tmp_path_factory.mktemp("mcp-projection")
    _seed_graph(root)
    driver = """from memory_server import server
real = server._get_graph_router
async def unavailable():
    if server._outbox_worker is None:
        return await real()
    raise RuntimeError('E_PROJECTION_UNAVAILABLE: graph requires migration')
server._get_graph_router = unavailable
server.mcp.run(transport='stdio')
"""

    async def script(session, out):
        await session.initialize()
        for name, arguments in (("graph_search", {"query": "anything"}), ("add_relation", {"source_id": "a", "target_id": "b", "relation_type": "supports"})):
            _record(out, name, await session.call_tool(name, arguments=arguments), root=root)

    return asyncio.run(_session([_python(), "-c", driver], script, root=root))


def test_mcp_stdio_projection_error_matches_c00(oracle, projection_capture):
    expected = oracle["mcp"]["projection_unavailable"]
    for name in ("graph_search", "add_relation"):
        assert projection_capture[name]["observed"] == expected[name]["observed"]


def test_mcp_stdio_projection_error_is_json_string(projection_capture):
    for name in ("graph_search", "add_relation"):
        assert projection_capture[name]["isError"] is False
        assert json.loads(projection_capture[name]["raw"])["error"] == "E_PROJECTION_UNAVAILABLE"


@pytest.mark.asyncio
async def test_mcp_other_runtime_error_reraises():
    from memory_server import server

    async def other_runtime_error():
        raise RuntimeError("not projection")

    with pytest.raises(RuntimeError, match="not projection"):
        await server._stable_projection_errors(other_runtime_error)()



def test_mcp_graph_cases_preserve_nodes_edges_paths_exactly(oracle, parity_capture):
    oracle = oracle["mcp"]["entries"]
    assert json.loads(parity_capture["happy.4.graph_search"]["raw"]) == json.loads(oracle["happy.4.graph_search"]["raw"])
    relation = json.loads(parity_capture["happy.11.graph_search"]["raw"])
    assert relation == json.loads(oracle["happy.11.graph_search"]["raw"])


def test_server_mcp_run_and_api_import_compatibility():
    from memory_server import server
    from memory_server.api.add_relation import add_relation
    from memory_server.api.learn import learn
    from memory_server.api.remember import remember
    from memory_server.api.search import search

    assert server.mcp is not None
    assert callable(server.run)
    assert callable(add_relation) and callable(learn) and callable(remember) and callable(search)
    assert "transport=\"stdio\"" in Path(server.__file__).read_text(encoding="utf-8")


def test_graph_duplicate_is_removed_and_only_graph_service_dispatch_remains():
    from memory_server import server

    source = Path(server.__file__).read_text(encoding="utf-8")
    # The compatibility alias is public; the duplicate implementation is not.
    assert callable(server.graph_search_fn)
    assert "search_by_relation" not in source
    assert "find_path" not in source
    assert "router.query" not in source
    assert "GraphSearchCommand" in source
    assert "application.graph.search" in source
    tree = ast.parse(source)
    has_graph_fn = any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "graph_search_fn"
        for node in tree.body
    )
    assert has_graph_fn is False


def test_mcp_stdio_tool_inventory(parity_capture):
    assert [item["name"] for item in parity_capture["tools"]] == list(EXPECTED_TOOLS)


@pytest.mark.parametrize("case", HAPPY_CASES, ids=[case[0] for case in HAPPY_CASES])
def test_mcp_stdio_happy_paths(parity_capture, case):
    key, _, _ = case
    actual = parity_capture[key]
    assert actual["isError"] is False, actual["raw"]
    assert actual["transcript_sha256"] == _sha(actual["raw"])


@pytest.mark.parametrize("case", INVALID_CASES, ids=[case[0] for case in INVALID_CASES])
def test_mcp_stdio_invalid_inputs(parity_capture, case):
    key, name, _ = case
    actual = parity_capture[key]
    if name not in {"ping", "metrics"}:
        assert actual["isError"] is True
        assert actual["raw"].startswith(f"Error executing tool {name}: ")


def test_mcp_graph_response_schema(parity_capture):
    relation = json.loads(parity_capture["happy.11.graph_search"]["raw"])
    assert list(relation) == ["nodes", "edges", "paths"]
    assert set(relation) == {"nodes", "edges", "paths"}
