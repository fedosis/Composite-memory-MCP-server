"""Tests for the Hermes MemoryProvider plugin — HermesProvider.

Tests the HermesProvider class with:
- Lifecycle hooks (initialize, prefetch, sync_turn, on_session_end, on_session_switch, shutdown)
- Tool schemas (all 14 CMMS tools present and correctly named)
- Tool call routing (mocked CMMS services)
- Writer queue integration

These tests mock the CMMS backend (SQLiteProvider) and Hermes ABC contract.
"""
from __future__ import annotations

import json

import pytest

from memory_server.plugins.hermes.config import (
    HermesPluginConfig,
)
from memory_server.plugins.hermes.provider import HermesProvider


class TestHermesProviderLifecycle:
    """Test the MemoryProvider lifecycle contract."""

    def test_name(self):
        """Verify provider name matches expected value."""
        provider = HermesProvider()
        assert provider.name == "memory_server"

    def test_is_available_returns_false_before_init(self):
        """Verify is_available returns False before initialize()."""
        provider = HermesProvider()
        assert provider.is_available() is False

    def test_is_available_returns_true_after_init(self):
        """Verify is_available returns True after successful initialize()."""
        provider = HermesProvider()
        # Initialize with :memory: database
        provider.initialize(
            session_id="test-session",
            config={"db_url": "sqlite+aiosqlite://"},
            hermes_home="/tmp/test-hermes",
        )
        assert provider.is_available() is True
        provider.shutdown()

    def test_initialize_sets_session_id(self):
        """Verify initialize stores the session_id."""
        provider = HermesProvider()
        provider.initialize(
            session_id="my-session",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        assert provider._session_id == "my-session"
        provider.shutdown()

    def test_double_initialize_is_idempotent(self):
        """Verify calling initialize twice doesn't crash."""
        provider = HermesProvider()
        provider.initialize(
            session_id="s1",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        provider.initialize(
            session_id="s1",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        assert provider.is_available() is True
        provider.shutdown()

    def test_shutdown_cleans_up(self):
        """Verify shutdown sets is_available to False."""
        provider = HermesProvider()
        provider.initialize(
            session_id="s1",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        provider.shutdown()
        assert provider.is_available() is False

    def test_system_prompt_block_returns_text(self):
        """Verify system_prompt_block returns non-empty instructions."""
        provider = HermesProvider()
        block = provider.system_prompt_block()
        assert isinstance(block, str)
        assert len(block) > 0
        assert "CMMS" in block or "memory" in block

    def test_shutdown_is_safe_when_not_initialized(self):
        """Verify shutdown is safe to call without initialize."""
        provider = HermesProvider()
        provider.shutdown()  # Should not raise


class TestHermesProviderToolSchemas:
    """Test that all 14 CMMS tools are exposed as native schemas."""

    EXPECTED_TOOLS = {
        "ping", "search", "remember", "get_context", "learn",
        "semantic_search", "graph_search", "route", "audit", "metrics",
        "set_belief", "get_belief", "resolve_conflict", "reflect",
    }

    def test_get_tool_schemas_returns_list(self):
        """Verify get_tool_schemas returns a list."""
        provider = HermesProvider()
        schemas = provider.get_tool_schemas()
        assert isinstance(schemas, list)

    def test_all_14_tools_present(self):
        """Verify all 14 expected tools are in the schema list."""
        provider = HermesProvider()
        schemas = provider.get_tool_schemas()
        names = {s["name"] for s in schemas}
        assert names == self.EXPECTED_TOOLS, (
            f"Missing: {self.EXPECTED_TOOLS - names}, "
            f"Extra: {names - self.EXPECTED_TOOLS}"
        )

    def test_tool_schemas_have_description(self):
        """Verify every tool schema has a non-empty description."""
        provider = HermesProvider()
        schemas = provider.get_tool_schemas()
        for schema in schemas:
            assert schema.get("description"), (
                f"Tool '{schema['name']}' missing description"
            )

    def test_tool_schemas_have_parameters(self):
        """Verify every tool schema has a parameters dict."""
        provider = HermesProvider()
        schemas = provider.get_tool_schemas()
        for schema in schemas:
            assert "parameters" in schema, (
                f"Tool '{schema['name']}' missing parameters"
            )
            assert isinstance(schema["parameters"], dict)

    def test_no_mcp_prefix_on_tool_names(self):
        """Verify tool names don't have the mcp_ prefix."""
        provider = HermesProvider()
        schemas = provider.get_tool_schemas()
        for schema in schemas:
            assert not schema["name"].startswith("mcp_"), (
                f"Tool '{schema['name']}' has mcp_ prefix"
            )

    def test_required_tools_have_required_params(self):
        """Verify tools with required parameters declare them."""
        provider = HermesProvider()
        schemas = {s["name"]: s for s in provider.get_tool_schemas()}

        # remember requires subject, predicate, object
        remember = schemas["remember"]
        assert "required" in remember["parameters"]
        assert "subject" in remember["parameters"]["required"]
        assert "predicate" in remember["parameters"]["required"]
        assert "object" in remember["parameters"]["required"]

        # get_context requires task
        get_context = schemas["get_context"]
        assert "required" in get_context["parameters"]
        assert "task" in get_context["parameters"]["required"]

        # learn requires text
        learn = schemas["learn"]
        assert "required" in learn["parameters"]
        assert "text" in learn["parameters"]["required"]

        # set_belief requires proposition
        set_belief = schemas["set_belief"]
        assert "required" in set_belief["parameters"]
        assert "proposition" in set_belief["parameters"]["required"]

        # resolve_conflict requires belief_a_id, belief_b_id, resolution
        resolve = schemas["resolve_conflict"]
        assert "belief_a_id" in resolve["parameters"]["required"]
        assert "belief_b_id" in resolve["parameters"]["required"]
        assert "resolution" in resolve["parameters"]["required"]

    def test_schema_names_no_special_chars(self):
        """Verify tool names are valid (alphanumeric + underscores)."""
        import re
        provider = HermesProvider()
        schemas = provider.get_tool_schemas()
        valid_name = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
        for schema in schemas:
            assert valid_name.match(schema["name"]), (
                f"Invalid tool name: '{schema['name']}'"
            )


class TestHermesProviderToolRouting:
    """Test that handle_tool_call routes to correct handlers."""

    def test_unknown_tool_raises(self):
        """Verify unknown tool raises ValueError."""
        provider = HermesProvider()
        provider.initialize(
            session_id="test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        with pytest.raises(ValueError):
            provider.handle_tool_call("nonexistent_tool", {})
        provider.shutdown()

    def test_ping_returns_ok(self):
        """Verify ping tool returns ok status."""
        provider = HermesProvider()
        provider.initialize(
            session_id="test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        result = json.loads(provider.handle_tool_call("ping", {}))
        assert result["status"] == "ok"
        provider.shutdown()

    def test_remember_with_fact(self):
        """Verify remember stores a fact and returns receipt."""
        provider = HermesProvider()
        provider.initialize(
            session_id="test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        result = json.loads(provider.handle_tool_call(
            "remember",
            {
                "subject": "Python",
                "predicate": "is",
                "object": "a programming language",
                "confidence": 1.0,
                "source": "test",
            },
        ))
        assert "receipt" in result
        assert "fact" in result
        assert result["fact"]["subject"] == "Python"
        provider.shutdown()

    def test_get_context_returns_facts(self):
        """Verify get_context returns context with facts."""
        provider = HermesProvider()
        provider.initialize(
            session_id="test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        # First store a fact
        provider.handle_tool_call(
            "remember",
            {
                "subject": "Docker",
                "predicate": "runs_on",
                "object": "Linux",
                "confidence": 1.0,
                "source": "test",
            },
        )
        # Then retrieve context
        result = json.loads(provider.handle_tool_call(
            "get_context",
            {"task": "Docker"},
        ))
        assert "facts" in result
        assert len(result["facts"]) >= 1
        provider.shutdown()

    def test_search_facts(self):
        """Verify search returns stored facts."""
        provider = HermesProvider()
        provider.initialize(
            session_id="test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        # Store a fact
        provider.handle_tool_call(
            "remember",
            {
                "subject": "Caddy",
                "predicate": "uses",
                "object": "Port 443",
                "confidence": 1.0,
                "source": "test",
            },
        )
        # Search for it
        result = json.loads(provider.handle_tool_call(
            "search",
            {"query": "Caddy"},
        ))
        assert result["total"] >= 1
        assert any(
            f["subject"] == "Caddy"
            for f in result["results"]
        )
        provider.shutdown()


class TestHermesProviderPrefetch:
    """Test the prefetch lifecycle hook."""

    def test_prefetch_returns_string(self):
        """Verify prefetch returns a string (empty or content)."""
        provider = HermesProvider()
        provider.initialize(
            session_id="test-session",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        result = provider.prefetch(query="test")
        assert isinstance(result, str)
        provider.shutdown()

    def test_prefetch_empty_query_returns_empty(self):
        """Verify prefetch with empty query returns empty string."""
        provider = HermesProvider()
        provider.initialize(
            session_id="test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        result = provider.prefetch(query="")
        assert result == ""
        provider.shutdown()

    def test_prefetch_returns_context_when_data_exists(self):
        """Verify prefetch returns non-empty context when facts exist."""
        provider = HermesProvider()
        provider.initialize(
            session_id="test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        # Store a fact first
        provider.handle_tool_call(
            "remember",
            {
                "subject": "Hermes",
                "predicate": "is",
                "object": "an AI agent",
                "confidence": 0.95,
                "source": "test",
            },
        )
        result = provider.prefetch(query="Hermes")
        assert len(result) > 0
        assert "Hermes" in result
        provider.shutdown()


class TestHermesProviderSessionHooks:
    """Test session lifecycle hooks (sync_turn, on_session_end, on_session_switch)."""

    def test_sync_turn_does_not_crash(self):
        """Verify sync_turn is safe to call."""
        provider = HermesProvider()
        provider.initialize(
            session_id="test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        # sync_turn should not raise
        provider.sync_turn(
            user_content="Hello",
            assistant_content="Hi there!",
            session_id="test",
        )
        provider.shutdown()

    def test_on_session_end_does_not_crash(self):
        """Verify on_session_end is safe to call."""
        provider = HermesProvider()
        provider.initialize(
            session_id="test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        provider.on_session_end([])
        provider.shutdown()

    def test_on_session_switch_updates_session_id(self):
        """Verify on_session_switch updates the internal session_id."""
        provider = HermesProvider()
        provider.initialize(
            session_id="old-session",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        provider.on_session_switch(new_session_id="new-session")
        assert provider._session_id == "new-session"
        provider.shutdown()

    def test_on_session_switch_reset_clears_cache(self):
        """Verify on_session_switch with reset=True clears the context cache."""
        provider = HermesProvider()
        provider.initialize(
            session_id="s1",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        provider._context_cache["test"] = "value"
        provider.on_session_switch(
            new_session_id="s2",
            reset=True,
        )
        assert provider._context_cache == {}
        provider.shutdown()


class TestHermesPluginConfig:
    """Test HermesPluginConfig loading."""

    def test_from_dict_minimal(self):
        """Verify from_dict works with empty dict."""
        config = HermesPluginConfig.from_dict({})
        assert config.db_url == "sqlite+aiosqlite:///data/memory.db"
        assert config.writer.flush_interval == 5.0
        assert config.writer.max_batch == 50

    def test_from_dict_with_values(self):
        """Verify from_dict accepts custom values."""
        config = HermesPluginConfig.from_dict({
            "db_url": "sqlite+aiosqlite:///custom.db",
            "writer": {
                "flush_interval": 2.0,
                "max_batch": 100,
            },
        })
        assert config.db_url == "sqlite+aiosqlite:///custom.db"
        assert config.writer.flush_interval == 2.0
        assert config.writer.max_batch == 100

    def test_from_dict_none(self):
        """Verify from_dict handles None gracefully."""
        config = HermesPluginConfig.from_dict(None)
        assert config.db_url == "sqlite+aiosqlite:///data/memory.db"

    def test_resolve_db_url(self):
        """Verify resolve_db_url expands relative paths."""
        config = HermesPluginConfig()
        resolved = config.resolve_db_url("/tmp/hermes_home")
        assert "tmp/hermes_home" in resolved

    def test_resolve_db_url_absolute(self):
        """Verify resolve_db_url doesn't expand absolute paths."""
        config = HermesPluginConfig(
            db_url="sqlite+aiosqlite:////absolute/path/memory.db",
        )
        resolved = config.resolve_db_url("/tmp/hermes_home")
        assert resolved == "sqlite+aiosqlite:////absolute/path/memory.db"
