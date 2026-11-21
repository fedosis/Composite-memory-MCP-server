"""Hermes loader compatibility for the standalone proxy plugin (slice T5).

These tests exercise the REAL Hermes loader against a temp ``HERMES_HOME``. If
``plugins.memory`` cannot be imported in this environment, the discovery test
skips with an explicit reason; the ABC and collector tests still run against the
imported host ``agent.memory_provider``.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
HERMES_AGENT = Path("/home/shtorm/.hermes/hermes-agent")
PLUGIN_SRC = REPO_ROOT / "plugin-dist" / "memory_server_proxy"

for _p in (str(REPO_ROOT / "plugin-dist"), str(HERMES_AGENT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from memory_server_proxy.provider import MemoryServerProxyProvider  # noqa: E402

try:
    import plugins.memory as memory_plugins  # noqa: E402
except Exception as exc:  # noqa: BLE001
    memory_plugins = None
    _LOADER_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"
else:
    _LOADER_IMPORT_ERROR = None


def test_find_provider_dir_finds_plugin(tmp_path: Path, monkeypatch) -> None:
    if memory_plugins is None:
        pytest.skip(f"plugins.memory not importable ({_LOADER_IMPORT_ERROR})")

    plugin_dst = tmp_path / "plugins" / "memory_server_proxy"
    plugin_dst.parent.mkdir(parents=True)
    shutil.copytree(PLUGIN_SRC, plugin_dst)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    found = memory_plugins.find_provider_dir("memory_server_proxy")
    assert found is not None
    assert (found / "__init__.py").exists()


def test_host_abc_methods_present() -> None:
    from agent.memory_provider import MemoryProvider

    provider = MemoryServerProxyProvider()
    provider_methods = set(dir(provider))
    host_methods = {m for m in dir(MemoryProvider) if not m.startswith("_")}
    assert host_methods <= provider_methods
    # Instantiable implies every abstract method is concretely implemented.
    assert not MemoryServerProxyProvider.__abstractmethods__


def test_register_via_collector() -> None:
    from memory_server_proxy import register

    class Collector:
        def __init__(self) -> None:
            self.provider = None

        def register_memory_provider(self, provider) -> None:
            self.provider = provider

    collector = Collector()
    register(collector)
    assert collector.provider is not None
    assert collector.provider.name == "memory_server_proxy"
