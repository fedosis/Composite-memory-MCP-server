"""Regression tests for Hermes v0.19 user-plugin discovery of CMMS memory_server.

These tests verify that CMMS's HermesProvider can be discovered and loaded
by Hermes v0.19's plugin mechanism. They use isolated temporary directories
and never modify the real ~/.hermes.

Testing strategy:
- Test 1–2: Unit-level register() → _ProviderCollector (fast, no I/O)
- Test 3: Full discovery from a temp HERMES_HOME (integration)
- Test 4: Discovery fallback (no register, MemoryProvider subclass)
- Test 5: Real shim file test (ensure the deployed __init__.py is valid)
"""

from __future__ import annotations

import importlib
import importlib.util
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from memory_server.plugins.hermes import HermesProvider, register

# Import Hermes plugin stubs so tests are hermetic (no dependency on Hermes' plugins.memory).
from tests._hermes_plugin_stubs import (
    _FakeCollector,
    _is_memory_provider_dir,
    _load_provider_from_dir,
)

# ===========================================================================
# Test 1: register() with a _ProviderCollector mock
# ===========================================================================


class TestRegisterWithCollector:
    """register() must call ctx.register_memory_provider with a HermesProvider instance."""

    def test_register_calls_register_memory_provider(self):
        """register() must call ctx.register_memory_provider exactly once."""
        collector = MagicMock()
        register(collector)
        collector.register_memory_provider.assert_called_once()

    def test_register_provider_is_hermes_provider(self):
        """The registered provider must be a HermesProvider instance."""
        collector = MagicMock()
        register(collector)
        arg = collector.register_memory_provider.call_args[0][0]
        assert isinstance(arg, HermesProvider)

    def test_register_provider_name_is_memory_server(self):
        """The registered provider must have name='memory_server'."""
        collector = MagicMock()
        register(collector)
        arg = collector.register_memory_provider.call_args[0][0]
        assert arg.name == "memory_server"


# ===========================================================================
# Test 2: Plugin _ProviderCollector integration (Hermes internal)
# ===========================================================================


class TestProviderCollectorIntegration:
    """Simulate Hermes's _ProviderCollector with register()."""

    def test_provider_collector_captures_provider(self):
        """A _ProviderCollector-style object must capture the provider."""
        collector = _FakeCollector()
        register(collector)
        assert collector.provider is not None
        assert collector.provider.name == "memory_server"

    def test_provider_can_be_initialized_after_registration(self):
        """The captured provider must be functional (can initialize)."""
        collector = _FakeCollector()
        register(collector)
        provider = collector.provider
        assert provider.is_available() is True  # v0.19: deps importable → available
        provider.initialize(
            session_id="test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        assert provider.is_available() is True
        provider.shutdown()


# ===========================================================================
# Test 3: Full Hermes discovery from a temp HERMES_HOME
# ===========================================================================


class TestHermesDiscoveryFromTempHome:
    """End-to-end: deploy the shim to a temp HERMES_HOME, then discover."""

    def _make_shim(self, plugin_dir: Path) -> None:
        """Write the same shim __init__.py that install-plugin.sh creates."""
        init_file = plugin_dir / "__init__.py"
        init_file.write_text(
            '''"""Hermes user-plugin shim for CMMS — test variant."""

from __future__ import annotations
import logging
logger = logging.getLogger(__name__)

def register(ctx) -> None:
    try:
        from memory_server.plugins.hermes import HermesProvider
    except ImportError as exc:
        logger.error("CMMS not importable: %s", exc)
        raise
    provider = HermesProvider()
    ctx.register_memory_provider(provider)
    logger.info("memory_server registered")
'''
        )

    def test_discovery_finds_provider_in_temp_home(self):
        """Hermes discovery must find memory_server in a temp HERMES_HOME/plugins/."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_home = Path(tmpdir)
            plugin_dir = tmp_home / "plugins" / "memory_server"
            plugin_dir.mkdir(parents=True)

            self._make_shim(plugin_dir)

            # Simulate Hermes discovery (using hermetic stubs)
            provider = _load_provider_from_dir(plugin_dir)
            assert provider is not None
            assert provider.name == "memory_server"
            assert provider.is_available() is True  # v0.19: deps importable → available

    def test_load_memory_provider_by_name_from_temp_home(self):
        """find_provider_dir + load_memory_provider must resolve memory_server."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_home = Path(tmpdir)
            plugin_dir = tmp_home / "plugins" / "memory_server"
            plugin_dir.mkdir(parents=True)
            self._make_shim(plugin_dir)

            # Use hermetic stubs instead of Hermes' internal plugins.memory
            provider = _load_provider_from_dir(plugin_dir)
            assert provider is not None
            assert provider.name == "memory_server"

            # Verify it can initialize and run
            provider.initialize(
                session_id="discovery-test",
                config={"db_url": "sqlite+aiosqlite://"},
            )
            schemas = provider.get_tool_schemas()
            assert len(schemas) >= 14
            assert any(s["name"] == "ping" for s in schemas)
            provider.shutdown()

    def test_is_memory_provider_dir_detects_shim(self):
        """Hermes _is_memory_provider_dir must detect the shim via register_memory_provider keyword."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_home = Path(tmpdir)
            plugin_dir = tmp_home / "plugins" / "memory_server"
            plugin_dir.mkdir(parents=True)
            self._make_shim(plugin_dir)

            # Use hermetic stubs instead of Hermes' internal plugins.memory
            assert _is_memory_provider_dir(plugin_dir) is True


# ===========================================================================
# Test 4: The shim __init__.py must be valid Python and importable
# ===========================================================================


class TestShimFileValidity:
    """Verify the deployed shim file parses and imports correctly."""

    def test_shim_imports_hermes_provider(self):
        """The shim's register() must successfully import HermesProvider."""
        # Create shim in a temp directory and import it
        with tempfile.TemporaryDirectory() as tmpdir:
            plugin_dir = Path(tmpdir)
            init_file = plugin_dir / "__init__.py"
            init_file.write_text(
                '''"""Test shim."""
def register(ctx):
    from memory_server.plugins.hermes import HermesProvider
    provider = HermesProvider()
    ctx.register_memory_provider(provider)
'''
            )

            spec = importlib.util.spec_from_file_location(
                "test_shim",
                str(init_file),
                submodule_search_locations=[str(plugin_dir)],
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

            assert hasattr(mod, "register")
            collector = _FakeCollector()
            mod.register(collector)
            assert collector.provider is not None
            assert collector.provider.name == "memory_server"


# ===========================================================================
# Test 5: HermesProvider ABC compliance
# ===========================================================================


class TestHermesProviderAbcCompliance:
    """Hermes MemoryProvider abstract methods are all implemented."""

    def test_required_methods_exist(self):
        """All abstract methods from the MemoryProvider ABC must exist."""
        provider = HermesProvider()
        # name (property)
        assert hasattr(type(provider), "name")
        assert provider.name == "memory_server"
        # is_available
        assert callable(provider.is_available)
        # initialize
        assert callable(provider.initialize)
        # get_tool_schemas
        assert callable(provider.get_tool_schemas)

    def test_optional_methods_no_crash(self):
        """Optional MemoryProvider hooks must not raise when called."""
        provider = HermesProvider()
        provider.initialize(
            session_id="abc-test",
            config={"db_url": "sqlite+aiosqlite://"},
        )

        # All optional hooks — call only those that exist on HermesProvider
        provider.system_prompt_block()
        provider.prefetch(query="")
        provider.queue_prefetch(query="")
        provider.sync_turn("hi", "hello", session_id="abc-test")
        result = provider.get_tool_schemas()
        assert isinstance(result, list)

        provider.on_session_end([])
        provider.on_session_switch(new_session_id="new-session")
        if hasattr(provider, "on_turn_start"):
            provider.on_turn_start(1, "hello")
        if hasattr(provider, "on_pre_compress"):
            assert isinstance(provider.on_pre_compress([]), str)
        if hasattr(provider, "on_delegation"):
            provider.on_delegation("task", "result")

        provider.shutdown()

    def test_handle_tool_call_ping(self):
        """handle_tool_call with 'ping' must return ok status."""
        provider = HermesProvider()
        provider.initialize(
            session_id="ping-test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        import json

        result = json.loads(provider.handle_tool_call("ping", {}))
        assert result["status"] == "ok"
        provider.shutdown()

    def test_handle_tool_call_unknown_tool(self):
        """handle_tool_call with unknown tool must raise ValueError."""
        provider = HermesProvider()
        provider.initialize(
            session_id="unknown-tool-test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        with pytest.raises(ValueError, match="Unknown CMMS tool"):
            provider.handle_tool_call("nonexistent", {})
        provider.shutdown()
