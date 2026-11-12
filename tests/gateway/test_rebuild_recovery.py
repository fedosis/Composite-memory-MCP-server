"""S20A1 recovery and frozen-source assertions against concrete owner APIs."""

import ast
import hashlib
import subprocess
from pathlib import Path

import pytest

from memory_server import projection_rebuild as projections
from memory_server.runtime.dependencies import RuntimeDependencies
from memory_server.runtime.runtime import CMMSRuntime

BASE = "9b07be1262cc7b1185bd97596a934f998ad592d2"
ROOT = Path(__file__).resolve().parents[2]
P = "src/memory_server/projection_rebuild.py"
R = "src/memory_server/runtime/runtime.py"
PINS = (
    (P, 178, 408, "3fb6caf05e08019e0c38189976700a176c2787410cf4fb81cb5f83a946863fb0"),
    (P, 691, 759, "4d031ea6b9ccb2b90199ff7fdcf5e135b71000999d87f2d42eacb2cc809d20ef"),
    (P, 987, 1049, "47a46b4cc309c44e5ebdcfb89bfa1a701623fb58f92b48636e4bd1b970732386"),
    (P, 869, 980, "ad333cfc4c33df34baf6fd1b1f3a9883a032e23ffabbcc2856c2cc6b7004d5dc"),
    (P, 900, 980, "0a28958e6b9863d4640c4f7ee917d4bc5bd25514e47fc5fbe74c008bee8dab5c"),
    (R, 487, 535, "6ba05337500f30676f3338225af517cd67df9edf96eed46e990247e01781eb6d"),
    (R, 608, 693, "ffefa1461f87ca417f451da7d4e1b8176508fe15dcd65f4c8c2ae7406c271968"),
    (R, 746, 753, "74f433f4fb270b4c2672a5b05a73b7282a3d83b9510a8580396bb5e294be8900"),
    (R, 801, 912, "7d3016157937b744364e2fd924a583fa44aeae886e7df186550c792ebd5c4651"),
)


def owned_surface():
    for name in ("snapshot_owned_projection_records", "rebuild_owned_projections"):
        assert callable(getattr(projections, name, None)), f"S20A_SURFACE: {name}"


def nodes(raw):
    tree = ast.parse(raw)
    result = {}

    def walk(node, prefix=""):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                key = prefix + child.name
                assert key not in result, f"S20A_FROZEN: duplicate {key}"
                result[key] = child
                walk(child, key + ".")
            else:
                walk(child, prefix)

    walk(tree)
    return result


@pytest.mark.parametrize("path,start,end,digest", PINS)
def test_s20a_frozen_boundaries(path, start, end, digest):
    before = subprocess.check_output(["git", "show", f"{BASE}:{path}"], cwd=ROOT)
    after = (ROOT / path).read_bytes()
    base_lines, new_lines = before.splitlines(keepends=True), after.splitlines(keepends=True)
    expected = b"".join(base_lines[start - 1:end])
    assert hashlib.sha256(expected).hexdigest() == digest, "S20A_FROZEN: pinned oracle"
    old_nodes, new_nodes = nodes(before), nodes(after)
    if start == 900:
        base_node = old_nodes["rebuild_projections"]
        new_node = new_nodes["rebuild_projections"]
        actual = b"".join(new_lines[new_node.lineno + start - base_node.lineno - 1:new_node.end_lineno])
    else:
        selected = {key: node for key, node in old_nodes.items() if start <= node.lineno <= end}
        assert selected, "S20A_FROZEN: missing span"
        first = min(selected, key=lambda k: selected[k].lineno)
        last = max(selected, key=lambda k: selected[k].end_lineno)
        actual = b"".join(new_lines[new_nodes[first].lineno - 1:new_nodes[last].end_lineno])
    assert actual == expected, "S20A_FROZEN: byte identity"


def owned_references(owned):
    return {
        item.attr if isinstance(item, ast.Attribute) else item.id
        for node in owned for item in ast.walk(node)
        if isinstance(item, (ast.Name, ast.Attribute))
    }


@pytest.mark.parametrize("expression,name", [
    ("f(asyncio.to_thread, work)", "to_thread"),
    ("f(to_thread, work)", "to_thread"),
    ("Thread(target=_observe_vector_artifact)", "_observe_vector_artifact"),
    ("f(_staged_ids)", "_staged_ids"),
])
def test_s20a_ast_function_arguments(expression, name):
    assert name in owned_references([ast.parse(expression)]), "S20A_AST_ARGUMENT"


@pytest.mark.parametrize("name", ["snapshot_owned_projection_records", "rebuild_owned_projections"])
def test_s20a_new_api_bytes(name):
    before = subprocess.check_output(["git", "show", f"{BASE}:{P}"], cwd=ROOT)
    after = (ROOT / P).read_bytes()
    declaration = f"async def {name}(".encode()
    assert declaration not in before, "S20A_NEW_API_BYTES: invalid BASE oracle"
    assert after != before and declaration in after, "S20A_NEW_API_BYTES: no new API bytes"


def test_s20a_owned_surface():
    owned_surface()


def test_s20a_owned_reader_no_legacy_observer():
    owned_surface()
    source = (ROOT / P).read_bytes()
    tree = ast.parse(source)
    base = subprocess.check_output(["git", "show", f"{BASE}:{P}"], cwd=ROOT)
    old_nodes, new_nodes = nodes(base), nodes(source)
    definitions = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
    legacy_names = {node.name for node in ast.parse(base).body if isinstance(node, definitions)}
    owned = [node for node in tree.body if isinstance(node, definitions) and node.name not in legacy_names]
    forbidden = {"to_thread", "_staged_ids", "_observe_vector_artifact", "rebuild_projections", "to_arrow"}
    references = owned_references(owned)
    assert not (references & forbidden), "S20A_OWNED_NO_LEGACY_OBSERVER"
    for name, start, end in (("_observe_vector_artifact", 496, 561), ("_staged_ids", 840, 866)):
        old_node, new_node = old_nodes[name], new_nodes[name]
        assert (old_node.lineno, old_node.end_lineno) == (start, end), "S20A_FROZEN: observer span"
        expected = base.splitlines(keepends=True)[start - 1:end]
        actual = source.splitlines(keepends=True)[new_node.lineno - 1:new_node.end_lineno]
        assert actual == expected, "S20A_FROZEN: observer byte identity"


@pytest.mark.asyncio
@pytest.mark.parametrize("manifest", ["{", '{"version": 999}', None])
async def test_s20a_startup_preprovider_rejects_unsafe_journal(tmp_path, manifest):
    from memory_server.providers.sqlite_provider import SQLiteProvider

    root = tmp_path / "profile"
    run = root / ".projection-rebuild" / ("a" * 32)
    run.mkdir(mode=0o700, parents=True)
    root.chmod(0o700)
    run.parent.chmod(0o700)
    if manifest is not None:
        (run / "journal.json").write_text(manifest)
        (run / "journal.json").chmod(0o600)
    trace = []

    async def build(owner):
        trace.append("provider")
        provider = SQLiteProvider(url=f"sqlite+aiosqlite:///{root / 'memory.db'}")
        await provider.initialize()
        return provider

    runtime = CMMSRuntime(root, profile_id="a", dependencies=RuntimeDependencies(recipes=(("sqlite", build),)))
    try:
        rejected = None
        try:
            await runtime.start()
        except Exception as exc:
            rejected = getattr(exc, "code", None)
        assert rejected == "E_STORAGE", "S20A_STARTUP: unsafe journal admitted"
        assert trace == [], "S20A_STARTUP: provider constructed before reconciliation"
    finally:
        await runtime.close()
