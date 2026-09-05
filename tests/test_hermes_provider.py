"""Tests for the Hermes MemoryProvider plugin — HermesProvider.

Tests the HermesProvider class with:
- Lifecycle hooks (initialize, prefetch, sync_turn, on_session_end, on_session_switch, shutdown)
- Tool schemas (all 14 CMMS tools present and correctly named)
- Tool call routing (mocked CMMS services)
- Writer queue integration

These tests mock the CMMS backend (SQLiteProvider) and Hermes ABC contract.
"""
from __future__ import annotations

import asyncio
import json
import time

import pytest
from storage.outbox import OutboxRepository
from storage.repositories import LifecycleRepository
from storage.repositories.belief_repo import BeliefRepository

from memory_server.models import Belief, Decision
from memory_server.plugins.hermes.config import (
    HermesPluginConfig,
)
from memory_server.plugins.hermes.provider import HermesProvider, _run_async
from memory_server.providers.embedding_provider import MockEmbeddingProvider


class TestHermesProviderLifecycle:
    """Test the MemoryProvider lifecycle contract."""

    def test_name(self):
        """Verify provider name matches expected value."""
        provider = HermesProvider()
        assert provider.name == "memory_server"

    def test_is_available_returns_true_before_init(self):
        """Verify is_available returns True before initialize() when CMMS deps are present.

        After Hermes v0.19, is_available() is called during discovery
        *before* initialize(). It must report availability based on
        dependency presence, not initialization state.
        """
        provider = HermesProvider()
        assert provider.is_available() is True

    def test_is_available_does_not_init_db(self):
        """Verify is_available() does NOT initialize the DB or set _initialized."""
        provider = HermesProvider()
        # Call is_available() to simulate discovery
        result = provider.is_available()
        assert result is True
        # Verify no initialization side-effects
        assert provider._initialized is False
        assert provider._provider is None

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

    def test_is_available_returns_false_when_cmms_missing(self):
        """Verify is_available returns False when CMMS package cannot be imported.

        This is the regression test for the ``ImportError`` path — when
        ``memory_server`` is genuinely absent, is_available() must not
        lie about it.
        """
        import builtins
        from unittest.mock import patch

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "memory_server":
                raise ImportError("No module named memory_server")
            return original_import(name, *args, **kwargs)

        provider = HermesProvider()
        with patch.object(builtins, "__import__", mock_import):
            assert provider.is_available() is False

        # After the patch is removed, it should be available again
        assert provider.is_available() is True


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

    def test_semantic_search_uses_persistent_backend_and_outbox_worker(
        self, tmp_path, monkeypatch
    ):
        """remember() should become visible to native semantic_search via persisted vectors."""
        monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")

        import memory_server.providers.embedding_provider as embedding_module

        monkeypatch.setattr(
            embedding_module,
            "SentenceTransformerEmbeddingProvider",
            MockEmbeddingProvider,
        )

        provider = HermesProvider()
        provider.initialize(
            session_id="semantic-native",
            config={
                "db_url": f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}",
                "path": str(tmp_path),
            },
            hermes_home=str(tmp_path),
        )
        try:
            provider.handle_tool_call(
                "remember",
                {
                    "subject": "HermesSemanticUnique",
                    "predicate": "stores",
                    "object": "persistent vectors",
                    "confidence": 1.0,
                    "source": "test",
                },
            )

            found = False
            for _ in range(6):
                result = json.loads(
                    provider.handle_tool_call(
                        "semantic_search",
                        {"query": "HermesSemanticUnique persistent", "top_k": 5},
                    )
                )
                hits = result.get("semantic_results", [])
                if any(hit.get("payload", {}).get("subject") == "HermesSemanticUnique" for hit in hits):
                    found = True
                    break
                time.sleep(1)

            assert found, "native semantic_search should see facts after outbox indexing"
            assert (tmp_path / "data" / "lancedb").exists()
        finally:
            provider.shutdown()

    def test_shutdown_stops_outbox_worker(self, tmp_path, monkeypatch):
        """shutdown() must actually terminate the outbox run loop.

        Regression: the task is scheduled via run_coroutine_threadsafe, whose
        concurrent.futures.Future.cancel() does not cancel the coroutine —
        without the stop flag the worker keeps polling a disposed engine.
        """
        monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")

        import memory_server.providers.embedding_provider as embedding_module

        monkeypatch.setattr(
            embedding_module,
            "SentenceTransformerEmbeddingProvider",
            MockEmbeddingProvider,
        )

        provider = HermesProvider()
        provider.initialize(
            session_id="shutdown-outbox",
            config={
                "db_url": f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}",
                "path": str(tmp_path),
            },
            hermes_home=str(tmp_path),
        )
        try:
            assert provider._outbox_worker is not None
            assert provider._outbox_task is not None
            assert not provider._outbox_task.done()
        finally:
            provider.shutdown()

        assert provider._outbox_worker is None
        assert provider._outbox_task is None

    def test_route_serializes_rank_results_after_native_indexing(self, tmp_path, monkeypatch):
        """route() should return JSON-serializable ranked results once vectors exist."""
        monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")

        import memory_server.providers.embedding_provider as embedding_module

        monkeypatch.setattr(
            embedding_module,
            "SentenceTransformerEmbeddingProvider",
            MockEmbeddingProvider,
        )

        provider = HermesProvider()
        provider.initialize(
            session_id="route-native",
            config={
                "db_url": f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}",
                "path": str(tmp_path),
            },
            hermes_home=str(tmp_path),
        )
        try:
            provider.handle_tool_call(
                "remember",
                {
                    "subject": "HermesRouteUnique",
                    "predicate": "uses",
                    "object": "rank serialization",
                    "confidence": 1.0,
                    "source": "test",
                },
            )

            route_result = None
            for _ in range(6):
                route_result = json.loads(
                    provider.handle_tool_call(
                        "route",
                        {"query": "HermesRouteUnique serialization", "top_k": 5},
                    )
                )
                if route_result.get("total", 0) > 0:
                    break
                time.sleep(1)

            assert route_result is not None
            assert route_result["total"] > 0
            assert isinstance(route_result.get("all_results"), list)
            assert isinstance(route_result["all_results"][0], dict)
        finally:
            provider.shutdown()


class TestHermesProviderBeliefReinforcement:
    """Regression coverage for exact-match reinforcement."""

    def _make_provider(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
        import memory_server.providers.embedding_provider as embedding_module

        monkeypatch.setattr(
            embedding_module,
            "SentenceTransformerEmbeddingProvider",
            MockEmbeddingProvider,
        )

        provider = HermesProvider()
        provider.initialize(
            session_id="reinforce-native",
            config={
                "db_url": f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}",
                "path": str(tmp_path),
            },
            hermes_home=str(tmp_path),
        )
        return provider

    def test_reinforcement_returns_fresh_belief_and_persists_history(
        self, tmp_path, monkeypatch
    ):
        provider = self._make_provider(tmp_path, monkeypatch)
        try:
            original = provider._provider.create_belief(
                Belief(proposition="User prefers Docker", confidence=0.4)
            )
            # ``create_belief`` is async; initialize the belief in the backing store.
            original = _run_async(original)

            payload = json.loads(
                provider.handle_tool_call(
                    "set_belief",
                    {
                        "proposition": "User prefers Docker",
                        "confidence": 0.8,
                        "sources": json.dumps(
                            [
                                {
                                    "source_type": "fact",
                                    "source_id": "fact-1",
                                    "weight": 0.8,
                                }
                            ]
                        ),
                        "tags": json.dumps(["docker", "infra"]),
                        "source": "test",
                    },
                )
            )

            assert payload["reinforced"] is True
            assert payload["belief"]["id"] == original.id
            assert payload["belief"]["confidence"] == pytest.approx(0.6)
            assert payload["receipt"]["confidence"] == pytest.approx(0.6)
            assert payload["receipt"]["history"][-1]["kind"] == "reinforce"
            assert payload["receipt"]["history"][-1]["previous_confidence"] == pytest.approx(0.4)
            assert payload["receipt"]["history"][-1]["confidence"] == pytest.approx(0.6)
            assert set(payload["belief"]["tags"]) == {"docker", "infra"}
            assert set(payload["belief"]["source_ids"]) == {"fact-1"}

            stored = _run_async(provider._provider.get_belief(original.id))
            fresh_receipt = _run_async(provider._provider.get_receipt(original.id))
            assert stored is not None
            assert stored.confidence == pytest.approx(0.6)
            assert set(stored.tags) == {"docker", "infra"}
            assert set(stored.source_ids) == {"fact-1"}
            assert fresh_receipt is not None
            assert fresh_receipt.history[-1]["kind"] == "reinforce"
        finally:
            provider.shutdown()

    def test_reinforcement_rolls_back_when_timestamp_update_fails(
        self, tmp_path, monkeypatch
    ):
        provider = self._make_provider(tmp_path, monkeypatch)

        async def boom(self, belief_id: str):
            raise RuntimeError("reinforced_at failed")

        monkeypatch.setattr(BeliefRepository, "update_reinforced_at", boom)
        try:
            original = _run_async(
                provider._provider.create_belief(
                    Belief(proposition="Rollback Docker", confidence=0.4)
                )
            )

            payload = json.loads(
                provider.handle_tool_call(
                    "set_belief",
                    {
                        "proposition": "Rollback Docker",
                        "confidence": 0.8,
                        "sources": "[]",
                        "tags": "[]",
                        "source": "test",
                    },
                )
            )
            assert payload["error"] == "reinforced_at failed"

            stored = _run_async(provider._provider.get_belief(original.id))
            receipt = _run_async(provider._provider.get_receipt(original.id))
            async def _inspect():
                async with await provider._provider._get_session() as session:
                    lifecycle_repo = LifecycleRepository(session)
                    events = await lifecycle_repo.get_events(original.id)
                    outbox_repo = OutboxRepository(session)
                    pending = await outbox_repo.get_pending_count()
                    return events, pending

            events, pending = _run_async(_inspect())

            assert stored is not None
            assert stored.confidence == pytest.approx(0.4)
            assert receipt is None
            assert events == []
            assert pending == 0
        finally:
            provider.shutdown()


class TestHermesProviderConflictAtomicity:
    """Regression coverage for conflict resolution staying in one UoW."""

    def _make_provider(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
        import memory_server.providers.embedding_provider as embedding_module

        monkeypatch.setattr(
            embedding_module,
            "SentenceTransformerEmbeddingProvider",
            MockEmbeddingProvider,
        )

        provider = HermesProvider()
        provider.initialize(
            session_id="conflict-native",
            config={
                "db_url": f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}",
                "path": str(tmp_path),
            },
            hermes_home=str(tmp_path),
        )
        return provider

    def test_discard_both_commits_without_nested_session_lock(self, tmp_path, monkeypatch):
        provider = self._make_provider(tmp_path, monkeypatch)
        try:
            belief_a = _run_async(provider._provider.create_belief(
                Belief(proposition="discard A", confidence=0.8)
            ))
            belief_b = _run_async(provider._provider.create_belief(
                Belief(proposition="discard B", confidence=0.7)
            ))
            payload = json.loads(_run_async(provider._handle_resolve_conflict(
                belief_a_id=belief_a.id,
                belief_b_id=belief_b.id,
                resolution="discard_both",
            )))
            assert payload["resolution"] == "discard_both"
            assert len(payload["events"]) == 2
            assert _run_async(provider._provider.get_receipt(payload["receipt"]["id"])) is not None
            fresh_a = _run_async(provider._provider.get_belief(belief_a.id))
            fresh_b = _run_async(provider._provider.get_belief(belief_b.id))
            assert fresh_a.lifecycle_state == "discarded"
            assert fresh_b.lifecycle_state == "discarded"
            assert fresh_a.version == 2
            assert fresh_b.version == 2
        finally:
            provider.shutdown()

    @pytest.mark.parametrize(
        "resolution,auto_resolve,new_proposition",
        [
            ("merge", False, "Merged belief"),
            ("discard_both", False, ""),
            ("keep_a", True, ""),
        ],
    )
    def test_second_event_failure_rolls_back_conflict_resolution(
        self,
        tmp_path,
        monkeypatch,
        resolution,
        auto_resolve,
        new_proposition,
    ):
        provider = self._make_provider(tmp_path, monkeypatch)
        calls = {"count": 0}

        original_record_event = LifecycleRepository.record_event

        async def boom(self, *args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 2:
                raise RuntimeError("event flush failed")
            return await original_record_event(self, *args, **kwargs)

        monkeypatch.setattr(LifecycleRepository, "record_event", boom)
        try:
            belief_a = _run_async(
                provider._provider.create_belief(
                    Belief(
                        proposition="Docker is better than Podman",
                        confidence=0.8,
                    )
                )
            )
            belief_b = _run_async(
                provider._provider.create_belief(
                    Belief(
                        proposition="Docker is worse than Podman",
                        confidence=0.6,
                    )
                )
            )

            kwargs = {
                "belief_a_id": belief_a.id,
                "belief_b_id": belief_b.id,
                "resolution": resolution,
                "auto_resolve": auto_resolve,
            }
            if new_proposition:
                kwargs["new_proposition"] = new_proposition

            with pytest.raises(RuntimeError, match="event flush failed"):
                _run_async(provider._handle_resolve_conflict(**kwargs))

            fresh_a = _run_async(provider._provider.get_belief(belief_a.id))
            fresh_b = _run_async(provider._provider.get_belief(belief_b.id))
            async def _inspect():
                async with await provider._provider._get_session() as session:
                    lifecycle_repo = LifecycleRepository(session)
                    events_a = await lifecycle_repo.get_events(belief_a.id)
                    events_b = await lifecycle_repo.get_events(belief_b.id)
                    outbox_repo = OutboxRepository(session)
                    pending = await outbox_repo.get_pending_count()
                    return events_a, events_b, pending

            events_a, events_b, pending = _run_async(_inspect())

            assert fresh_a is not None and fresh_a.lifecycle_state == "active"
            assert fresh_b is not None and fresh_b.lifecycle_state == "active"
            assert fresh_a.version == 1
            assert fresh_b.version == 1
            assert events_a == []
            assert events_b == []
            assert pending == 0
            if resolution == "merge":
                merged = _run_async(
                    provider._provider.search_beliefs(
                        proposition="Merged belief",
                        lifecycle_state=None,
                        include_inactive=True,
                        limit=10,
                    )
                )
                assert merged == []
        finally:
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

    def test_prefetch_includes_decisions(self):
        """Matching decisions appear in the Memory Context block."""
        provider = HermesProvider()
        provider.initialize(
            session_id="test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        try:
            _run_async(provider._provider.create_decision(
                Decision(
                    id="pd1",
                    context="Hermes",
                    choice="Use native CMMS provider",
                    reason="Faster than MCP transport",
                )
            ))
            result = provider.prefetch(query="Hermes")
            assert "--- Memory Context ---" in result
            assert "Decisions:" in result
            assert "Use native CMMS provider" in result
        finally:
            provider.shutdown()

    def test_prefetch_filters_low_confidence_decisions(self):
        """High-confidence decisions render; low-confidence (<0.8) are excluded.

        Seeds BOTH a high-confidence and a low-confidence matching decision so
        the test is red on the pre-change stub (no decisions wired at all) and
        genuinely exercises the confidence filter on current code.
        """
        provider = HermesProvider()
        provider.initialize(
            session_id="test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        try:
            _run_async(provider._provider.create_decision(
                Decision(
                    id="pd_high",
                    context="Hermes",
                    choice="High confidence choice",
                    reason="Confirmed",
                    confidence=0.95,
                )
            ))
            _run_async(provider._provider.create_decision(
                Decision(
                    id="pd_low",
                    context="Hermes",
                    choice="Low confidence choice",
                    reason="Uncertain",
                    confidence=0.5,
                )
            ))
            result = provider.prefetch(query="Hermes")
            assert "--- Memory Context ---" in result
            assert "Decisions:" in result
            assert "High confidence choice" in result
            assert "Low confidence choice" not in result
        finally:
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


class TestHermesProviderGenerationGuard:
    """HERM-3/4: generation-guarded async prefetch pipeline.

    Every queued context load carries a token (session, generation, seq):
      - a stale-generation / stale-session load never publishes;
      - a slow older load never overwrites a fast newer load (barrier A/B);
      - a cancelled load never pollutes the cache;
      - cache entries are served only to the query+session they were
        computed for;
      - a session switch invalidates the previous session's cache entry.

    The DB-adjacent loads are monkeypatched away: these are unit tests of the
    guard, not of get_context.
    """

    @staticmethod
    def _make_provider() -> HermesProvider:
        provider = HermesProvider()  # uninitialized — guard tests need no DB
        provider._session_id = "s1"
        return provider

    def test_cache_served_only_for_same_query_and_session(self):
        """A cache entry computed for query A is never returned for query B
        or for another session — prefetch falls back instead of leaking."""
        provider = self._make_provider()
        provider._provider = object()  # bypass the not-initialized guard only

        async def fake(query, max_results=5):
            return {"qA": "CONTEXT-A", "qB": "CONTEXT-B"}.get(query, "")

        provider._prefetch_async = fake
        provider._session_id = "s1"
        # Publish qA entry for session s1.
        _run_async(provider._queue_prefetch_async(
            "qA", session="s1", gen=0, seq=1,
        ))

        # Same query + same session -> served from cache.
        assert provider.prefetch("qA", session_id="s1") == "CONTEXT-A"
        # A DIFFERENT query must not receive qA's cached context. Pre-fix this
        # returned qA's cached text; now the sync fallback computes qB fresh.
        assert provider.prefetch("qB", session_id="s1") == "CONTEXT-B"
        # The cache entry itself still belongs to qA (fallback never
        # overwrites a queued entry with a synchronous load).
        entry = provider._context_cache["prefetch"]
        assert entry["query"] == "qA" and entry["session"] == "s1"
        # A different session must not be served the s1 entry: the request
        # misses the cache (session mismatch) and falls back.
        assert provider.prefetch("qA", session_id="other") == "CONTEXT-A"
        assert provider._context_cache["prefetch"]["session"] == "s1"

    def test_stale_generation_task_never_publishes(self):
        """A load whose generation predates the current one is dropped."""
        provider = self._make_provider()
        provider._prefetch_generation = 7

        _run_async(provider._queue_prefetch_async(
            "qOld", session="s1", gen=6, seq=1,
        ))

        assert "prefetch" not in provider._context_cache

    def test_stale_session_task_never_publishes(self):
        """A load started for an old session is dropped after a switch."""
        provider = self._make_provider()
        provider._session_id = "s2"  # switched away from s1

        _run_async(provider._queue_prefetch_async(
            "qA", session="s1", gen=provider._prefetch_generation, seq=1,
        ))

        assert "prefetch" not in provider._context_cache

    def test_barrier_slow_a_fast_b_leaves_b(self):
        """A slow in-flight load (A, older seq) that finishes after a fast
        load (B, newer seq) must not overwrite B's cache entry."""

        async def scenario():
            provider = self._make_provider()
            provider._session_id = "s1"
            release = asyncio.Event()

            async def slow(query, max_results=5):
                await release.wait()
                return "CONTEXT-A"

            provider._prefetch_async = slow
            task_a = asyncio.create_task(provider._queue_prefetch_async(
                "qA", session="s1", gen=0, seq=1,
            ))
            await asyncio.sleep(0.01)  # A is now parked on the barrier

            async def fast(query, max_results=5):
                return "CONTEXT-B"

            provider._prefetch_async = fast
            await provider._queue_prefetch_async(
                "qB", session="s1", gen=0, seq=2,
            )
            entry = provider._context_cache["prefetch"]
            assert entry["query"] == "qB" and entry["text"] == "CONTEXT-B"

            release.set()
            await task_a  # A finishes late and must be dropped

            entry = provider._context_cache["prefetch"]
            assert entry["query"] == "qB", "stale A overwrote B"
            assert entry["text"] == "CONTEXT-B"

        asyncio.run(scenario())

    def test_cancelled_task_never_publishes(self):
        """A cancelled load leaves no trace in the cache."""

        async def scenario():
            provider = self._make_provider()
            provider._session_id = "s1"
            started = asyncio.Event()
            release = asyncio.Event()

            async def cancellable(query, max_results=5):
                started.set()
                await release.wait()
                return "CONTEXT-X"

            provider._prefetch_async = cancellable
            task = asyncio.create_task(provider._queue_prefetch_async(
                "qX", session="s1", gen=0, seq=1,
            ))
            await started.wait()
            task.cancel()
            await task  # CancelledError is swallowed inside the guard
            assert "prefetch" not in provider._context_cache
            release.set()  # allow the helper task to exit cleanly

        asyncio.run(scenario())

    def test_session_switch_invalidates_cache_entry(self):
        """on_session_switch drops the previous session's prefetch entry and
        bumps the generation (even without reset=True)."""
        provider = HermesProvider()
        provider.initialize(
            session_id="s1",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        try:
            gen_before = provider._prefetch_generation
            provider._context_cache["prefetch"] = {
                "query": "qOld", "session": "s1", "seq": 1, "text": "ctx-s1",
            }
            provider.on_session_switch(new_session_id="s2")

            assert provider._session_id == "s2"
            assert provider._prefetch_generation == gen_before + 1
            assert "prefetch" not in provider._context_cache
        finally:
            provider.shutdown()

    def test_session_switch_prevents_stale_publish_into_new_session(self):
        """An in-flight load for the old session that completes AFTER a switch
        never publishes into the new session's cache."""

        async def scenario():
            provider = self._make_provider()
            provider._session_id = "s1"
            release = asyncio.Event()

            async def slow(query, max_results=5):
                await release.wait()
                return "CONTEXT-OLD"

            provider._prefetch_async = slow
            task = asyncio.create_task(provider._queue_prefetch_async(
                "qOld", session="s1", gen=provider._prefetch_generation, seq=1,
            ))
            await asyncio.sleep(0.01)
            # Session switches while the old-session load is still running.
            provider._prefetch_generation += 1
            provider._session_id = "s2"
            provider._context_cache.pop("prefetch", None)

            release.set()
            await task

            assert "prefetch" not in provider._context_cache, (
                "stale session load polluted the new session"
            )

        asyncio.run(scenario())


class TestHermesProviderGraphHandlerSnapshot:
    """HERM-3: _handle_graph_search uses the shared snapshot-backed graph.

    Regression: the handler built a private memory-only SimpleGraph, so it
    never saw persisted nodes AND cached that empty graph into
    provider._graph, blinding later _get_graph() callers (route/audit).
    """

    def test_handler_sees_node_from_snapshot(self, tmp_path, monkeypatch):
        from memory_server.settings import get_settings

        snapshot = tmp_path / "graph.json"
        monkeypatch.setenv("MEMORY_SERVER_GRAPH_SNAPSHOT_PATH", str(snapshot))
        get_settings.cache_clear()
        snapshot.write_text(json.dumps({
            "nodes": {
                "alpha": {
                    "id": "alpha", "type": "entity", "name": "AlphaNode",
                    "attributes": {},
                },
            },
            "edges": [],
        }), encoding="utf-8")

        provider = HermesProvider()
        provider.initialize(
            session_id="gh-test",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        try:
            result = provider.handle_tool_call(
                "graph_search", {"entity_id": "alpha"},
            )
            payload = json.loads(result)
            names = [n.get("name") for n in payload.get("nodes", [])]
            assert "AlphaNode" in names, (
                f"handler must see the snapshot node, got nodes={names}"
            )

            # The shared graph is now cached on the provider and contains the
            # snapshot node — not an empty memory-only graph.
            assert provider._graph is not None
            assert provider._graph.get_node("alpha") is not None
        finally:
            provider.shutdown()

    def test_graph_search_does_not_blind_route_with_empty_graph(
        self, tmp_path, monkeypatch,
    ):
        """Calling graph_search first must not replace the snapshot-backed
        graph with an empty one for subsequent shared-graph callers."""
        from memory_server.settings import get_settings

        snapshot = tmp_path / "graph.json"
        monkeypatch.setenv("MEMORY_SERVER_GRAPH_SNAPSHOT_PATH", str(snapshot))
        get_settings.cache_clear()
        snapshot.write_text(json.dumps({
            "nodes": {
                "beta": {
                    "id": "beta", "type": "entity", "name": "BetaNode",
                    "attributes": {},
                },
            },
            "edges": [],
        }), encoding="utf-8")

        provider = HermesProvider()
        provider.initialize(
            session_id="gh-test2",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        try:
            provider.handle_tool_call("graph_search", {"entity_id": "beta"})
            # A subsequent _get_graph consumer (e.g. route) sees the same
            # snapshot-backed graph with the node present.
            from memory_server.plugins.hermes.provider import _get_graph

            graph = _run_async(_get_graph(provider))
            assert graph.get_node("beta") is not None
        finally:
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

    # --- CMMS data consolidation: single shared data root -----------------

    def _repo_root(self) -> str:
        """Return the expected repo root the config should default to."""
        from memory_server.paths import cmms_repo_root

        return str(cmms_repo_root())

    def test_from_dict_defaults_cmms_path_to_repo_root(self):
        """Empty/missing path must default to the CMMS repo root.

        Prevents future profiles from silently creating per-profile data
        dirs (the fragmentation CMMS consolidation removes).
        """
        config = HermesPluginConfig.from_dict({})
        assert config.cmms_path == self._repo_root()

    def test_from_dict_none_defaults_cmms_path_to_repo_root(self):
        """None config must also default cmms_path to the repo root."""
        config = HermesPluginConfig.from_dict(None)
        assert config.cmms_path == self._repo_root()

    def test_from_dict_explicit_path_preserved(self):
        """An explicit path in config is kept as-is."""
        config = HermesPluginConfig.from_dict({"path": "/custom/cmms"})
        assert config.cmms_path == "/custom/cmms"

    def test_from_dict_empty_path_defaults_to_repo_root(self):
        """An explicitly empty path must fall back to the repo root."""
        config = HermesPluginConfig.from_dict({"path": ""})
        assert config.cmms_path == self._repo_root()

    def test_from_env_defaults_cmms_path_to_repo_root(self, monkeypatch):
        """from_env without MEMORY_SERVER_PATH defaults to repo root."""
        monkeypatch.delenv("MEMORY_SERVER_PATH", raising=False)
        config = HermesPluginConfig.from_env()
        assert config.cmms_path == self._repo_root()

    # --- W2: empty MEMORY_SERVER_PATH must behave like unset ---------------

    def test_from_dict_empty_env_defaults_to_repo_root(self, monkeypatch):
        """Empty-string env must not bypass the repo-root default."""
        monkeypatch.setenv("MEMORY_SERVER_PATH", "")
        config = HermesPluginConfig.from_dict({})
        assert config.cmms_path == self._repo_root()
        assert config.cmms_path_source == "default"

    def test_from_dict_empty_env_ignored_when_config_has_path(self, monkeypatch):
        """Empty-string env must not shadow an explicit config path."""
        monkeypatch.setenv("MEMORY_SERVER_PATH", "")
        config = HermesPluginConfig.from_dict({"path": "/custom/cmms"})
        assert config.cmms_path == "/custom/cmms"
        assert config.cmms_path_source == "config"

    def test_from_env_empty_env_defaults_to_repo_root(self, monkeypatch):
        """from_env with empty MEMORY_SERVER_PATH defaults to repo root."""
        monkeypatch.setenv("MEMORY_SERVER_PATH", "")
        config = HermesPluginConfig.from_env()
        assert config.cmms_path == self._repo_root()
        assert config.cmms_path_source == "default"

    # --- W1: env override visibility ----------------------------------------

    def test_from_dict_env_overrides_config_and_tracks_source(self, monkeypatch):
        """A set env var wins over config and is reported as source=env."""
        monkeypatch.setenv("MEMORY_SERVER_PATH", "/env/cmms")
        config = HermesPluginConfig.from_dict({"path": "/cfg/cmms"})
        assert config.cmms_path == "/env/cmms"
        assert config.cmms_path_source == "env"

    def test_from_dict_config_source_tracked(self, monkeypatch):
        """Without env, an explicit config path is source=config."""
        monkeypatch.delenv("MEMORY_SERVER_PATH", raising=False)
        config = HermesPluginConfig.from_dict({"path": "/cfg/cmms"})
        assert config.cmms_path == "/cfg/cmms"
        assert config.cmms_path_source == "config"

    def test_from_dict_use_env_false_ignores_env(self, monkeypatch):
        """use_env=False must validate the raw config value, not env."""
        monkeypatch.setenv("MEMORY_SERVER_PATH", "/env/cmms")
        config = HermesPluginConfig.from_dict({"path": "/cfg/cmms"}, use_env=False)
        assert config.cmms_path == "/cfg/cmms"
        assert config.cmms_path_source == "config"

    # --- W3: single source of truth for repo-root resolution ----------------

    def test_repo_root_single_source_of_truth(self):
        """paths.cmms_repo_root is the one shared resolver; config uses it."""
        from memory_server.paths import cmms_repo_root

        root = str(cmms_repo_root())
        assert root == self._repo_root()
        # from_dict default and validate_shared_root expected both derive
        # from the shared helper, so a layout change cannot diverge.
        assert HermesPluginConfig.from_dict({}).cmms_path == root
        HermesPluginConfig.from_dict({"path": root}).validate_shared_root(expected=root)

    def test_validate_shared_root_accepts_repo_root(self):
        """Repo-root path passes shared-root validation."""
        config = HermesPluginConfig.from_dict({"path": self._repo_root()})
        config.validate_shared_root()

    def test_validate_shared_root_rejects_non_repo_path(self):
        """Non-repo path must be rejected by shared-root validation."""
        config = HermesPluginConfig.from_dict({"path": "/tmp/per-profile"})
        with pytest.raises(ValueError, match="must point at the shared CMMS repo root"):
            config.validate_shared_root()

    def test_validate_shared_root_empty_is_noop(self):
        """Empty cmms_path (direct construction) is allowed by validation."""
        config = HermesPluginConfig()
        config.validate_shared_root()  # no raise
