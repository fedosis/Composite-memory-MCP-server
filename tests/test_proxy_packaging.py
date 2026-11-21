"""Packaging contract tests for the standalone proxy plugin (slice T5).

Checks the plugin tree shape, the stdlib-only import discipline (AST scan), the
artifact hash, and that ``register`` hands back a fresh provider per call.
"""

from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_DIR = REPO_ROOT / "plugin-dist" / "memory_server_proxy"

for _p in (str(REPO_ROOT / "plugin-dist"), "/home/shtorm/.hermes/hermes-agent"):
    if _p not in sys.path:
        sys.path.insert(0, _p)

PLUGIN_FILES = {
    "plugin.yaml",
    "__init__.py",
    "wire.py",
    "settings.py",
    "provider.py",
    "schema_v1.json",
    "system_prompt.txt",
}
FORBIDDEN_MODULES = {"memory_server", "mcp", "sqlalchemy", "prometheus_client", "yaml", "hermes_agent"}


def _py_files() -> list[Path]:
    return sorted(PLUGIN_DIR.glob("*.py"))


def test_plugin_tree_complete() -> None:
    names = {p.name for p in PLUGIN_DIR.iterdir() if p.is_file()}
    assert names == PLUGIN_FILES


def test_no_backend_imports() -> None:
    for path in _py_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=path.name)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots = {alias.name.split(".")[0] for alias in node.names}
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                roots = {node.module.split(".")[0]} if node.module else set()
            else:
                continue
            assert not (roots & FORBIDDEN_MODULES), \
                f"{path.name} imports a forbidden backend module: {roots & FORBIDDEN_MODULES}"


def test_python_dependencies_empty() -> None:
    text = (PLUGIN_DIR / "plugin.yaml").read_text(encoding="utf-8")
    assert "python_dependencies: []" in text



def test_artifacts_hash_matches_schema_v1() -> None:
    from memory_server_proxy import wire

    document = json.loads((PLUGIN_DIR / "schema_v1.json").read_text(encoding="utf-8"))
    tools = document["tools"]
    prompt = document["prompt"]
    stored = document["schema_hash"]
    assert wire.schema_hash_from_artifacts(tools, prompt) == stored

    from memory_server.native.schema_v1 import schema_hash as native_schema_hash

    assert stored == native_schema_hash()


def test_register_returns_new_instance() -> None:
    from agent.memory_provider import MemoryProvider
    from memory_server_proxy import register

    class Collector:
        def __init__(self) -> None:
            self.provider = None

        def register_memory_provider(self, provider) -> None:
            self.provider = provider

    first, second = Collector(), Collector()
    register(first)
    register(second)
    assert first.provider is not None and second.provider is not None
    assert first.provider is not second.provider
    assert isinstance(first.provider, MemoryProvider)
    assert first.provider.name == "memory_server_proxy"


def test_no_absolute_paths_baked() -> None:
    abs_path = re.compile(r"""["']/(?:home|srv|run|tmp|var|etc|usr|opt|root)/""")
    for path in _py_files():
        source = path.read_text(encoding="utf-8")
        assert "/home/" not in source, f"{path.name} bakes /home/"
        assert not abs_path.search(source), f"{path.name} bakes an absolute path"
