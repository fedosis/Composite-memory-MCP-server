"""Test stubs for Hermes plugin discovery functions (_load_provider_from_dir, _is_memory_provider_dir).

These stubs replicate the Hermes v0.19 internal API that CMMS tests depend on,
without requiring the actual ``plugins.memory`` package from Hermes.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any


class _FakeCollector:
    """Minimal replica of Hermes's _ProviderCollector."""

    def __init__(self) -> None:
        self.provider: Any = None

    def register_memory_provider(self, provider: Any) -> None:
        self.provider = provider

    def register_tool(self, *args: Any, **kwargs: Any) -> None:
        pass


def _load_provider_from_dir(plugin_dir: Path) -> Any:
    """Load a Hermes memory provider from a plugin directory.

    Imports ``__init__`` from the directory, calls ``register()`` with a
    ``_FakeCollector``, and returns the registered provider instance.
    """
    init_file = plugin_dir / "__init__.py"
    if not init_file.exists():
        raise ImportError(f"No __init__.py in {plugin_dir}")

    spec = importlib.util.spec_from_file_location(
        f"_{plugin_dir.name}_shim",
        str(init_file),
        submodule_search_locations=[str(plugin_dir)],
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load spec from {init_file}")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    if not hasattr(mod, "register"):
        raise AttributeError(f"{init_file} has no register() function")

    collector = _FakeCollector()
    mod.register(collector)
    return collector.provider


def _is_memory_provider_dir(plugin_dir: Path) -> bool:
    """Check whether a directory contains a Hermes memory provider plugin.

    A valid plugin directory has an ``__init__.py`` whose ``register()``
    function calls ``ctx.register_memory_provider()``.
    """
    init_file = plugin_dir / "__init__.py"
    if not init_file.exists():
        return False

    source = init_file.read_text(encoding="utf-8")
    return "register_memory_provider" in source
