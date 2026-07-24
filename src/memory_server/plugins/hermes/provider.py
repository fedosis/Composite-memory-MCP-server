"""HermesProvider — native MemoryProvider for CMMS.

Adapts CMMS (Composite Memory MCP Server) to the Hermes MemoryProvider ABC,
giving CMMS access to lifecycle hooks unavailable over MCP:
- prefetch: recall context before each turn
- sync_turn: persist turn observations asynchronously
- on_session_end / on_session_switch: session boundary hooks
- All 14 CMMS tools as native Hermes tool schemas

Uses a background event loop thread to bridge sync MemoryProvider ABC methods
with CMMS's async service layer (same pattern as Hindsight provider).
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from memory_server.plugins.hermes.config import HermesPluginConfig
from memory_server.plugins.hermes.writer import WriterQueue

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Background event loop — one per process, reused across sessions.
# ---------------------------------------------------------------------------

_loop: asyncio.AbstractEventLoop | None = None
_loop_thread: threading.Thread | None = None
_loop_lock = threading.Lock()


def _get_loop() -> asyncio.AbstractEventLoop:
    """Return a long-lived event loop running on a background thread."""
    global _loop, _loop_thread
    with _loop_lock:
        if _loop is not None and _loop.is_running():
            return _loop
        _loop = asyncio.new_event_loop()

        def _run() -> None:
            asyncio.set_event_loop(_loop)
            _loop.run_forever()

        _loop_thread = threading.Thread(
            target=_run, daemon=True, name="cmms-provider-loop"
        )
        _loop_thread.start()
        return _loop


def _run_async(coro, timeout: float = 30.0):
    """Schedule *coro* on the shared loop and block until done."""
    loop = _get_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=timeout)


# ---------------------------------------------------------------------------
# Default config
# ---------------------------------------------------------------------------

DEFAULT_DB_URL = "sqlite+aiosqlite:///data/memory.db"


# ---------------------------------------------------------------------------
# HermesProvider
# ---------------------------------------------------------------------------


class HermesProvider:
    """Native Hermes MemoryProvider for CMMS.

    Bridges the sync MemoryProvider ABC with CMMS's async service layer.
    Uses a background event loop thread for all async operations.

    Tool naming: CMMS tools are exposed WITHOUT the ``mcp_`` prefix
    (e.g. ``search``, ``remember``, ``get_context``).

    Lifecycle:
        1. initialize(session_id, **kwargs) — start engine, writer queue
        2. prefetch(query) — recall context for the upcoming turn
        3. sync_turn(...) — queue turn for async persistence
        4. get_tool_schemas() — return 14 native tool schemas
        5. handle_tool_call(name, args) — route to CMMS service
        6. on_session_switch() / on_session_end() — flush writer queue
        7. shutdown() — clean exit
    """

    name = "memory_server"

    def __init__(self):
        self._initialized = False
        self._shut_down = False
        self._provider = None  # SQLiteProvider
        self._qdrant = None
        self._embedder = None
        self._graph = None
        self._router = None
        self._writer: WriterQueue | None = None
        self._config: HermesPluginConfig | None = None
        self._hermes_home: str = ""
        self._session_id: str = ""
        self._context_cache: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Core lifecycle
    # ------------------------------------------------------------------

    def is_available(self) -> bool:
        """Return True if CMMS dependencies are available.

        After Hermes v0.19, ``is_available()`` is called during discovery
        *before* ``initialize()``.  The method must return ``True`` when
        the CMMS package can be imported (dependencies are present) even
        if the provider hasn't been started yet, and ``False`` only when
        the dependency is genuinely missing or the provider was shut down.

        Does NOT perform DB or network I/O — only checks importability.
        """
        if self._initialized:
            return self._provider is not None
        if self._shut_down:
            return False
        try:
            import memory_server  # noqa: F401
            return True
        except ImportError:
            return False

    def initialize(self, session_id: str, **kwargs) -> None:
        """Initialize the CMMS provider, writer queue, and services.

        kwargs:
            hermes_home (str): The active HERMES_HOME directory path.
            platform (str): "cli", "telegram", "discord", etc.
            agent_context (str): "primary", "subagent", etc.
        """
        if self._initialized:
            logger.debug("HermesProvider already initialized for session %s", session_id)
            return

        self._session_id = session_id
        self._hermes_home = kwargs.get("hermes_home", "")

        # Load config
        config_data = kwargs.get("config", {}) or {}
        self._config = HermesPluginConfig.from_dict(config_data)

        # Resolve DB URL relative to hermes_home if needed
        db_url = (
            self._config.resolve_db_url(self._hermes_home)
            if self._hermes_home
            else self._config.db_url
        )

        # Initialize SQLiteProvider on the background loop
        try:
            self._provider = _run_async(self._init_provider(db_url), timeout=60.0)
        except Exception:
            logger.exception("HermesProvider: failed to initialize SQLiteProvider")
            self._initialized = False
            raise

        # Start writer queue with a write handler that uses the provider
        self._writer = WriterQueue(
            write_callback=self._handle_batch_write,
            flush_interval=self._config.writer.flush_interval,
            max_batch=self._config.writer.max_batch,
        )
        # Start the writer's background task on the shared loop
        _run_async(self._writer.start())

        self._initialized = True
        logger.info(
            "HermesProvider initialized (session=%s, db=%s)",
            session_id,
            db_url,
        )

    async def _init_provider(self, db_url: str):
        """Async initialization of the SQLiteProvider on the background loop."""
        from memory_server.providers.sqlite_provider import SQLiteProvider

        provider = SQLiteProvider(url=db_url)
        await provider.initialize()
        return provider

    def shutdown(self) -> None:
        """Flush writer queue and close the database connection."""
        if not self._initialized:
            return

        logger.info("HermesProvider: shutting down")

        # Flush and stop writer
        if self._writer is not None:
            _run_async(self._writer.shutdown(), timeout=30.0)

        # Close the provider
        if self._provider is not None:
            try:
                _run_async(self._provider.close(), timeout=10.0)
            except Exception:
                logger.exception("HermesProvider: error closing provider")

        self._provider = None
        self._writer = None
        self._initialized = False
        self._shut_down = True
        self._context_cache.clear()
        logger.info("HermesProvider: shutdown complete")

    # ------------------------------------------------------------------
    # System prompt / context injection
    # ------------------------------------------------------------------

    def system_prompt_block(self) -> str:
        """Return static instructions for the system prompt.

        Tells the model about CMMS capabilities.
        """
        return (
            "You have access to the Composite Memory MCP Server (CMMS) as a native "
            "memory provider. Use these tools to store and retrieve information "
            "across sessions: search, remember, learn, get_context, semantic_search, "
            "graph_search, route, audit, metrics, set_belief, get_belief, "
            "resolve_conflict, reflect.\n"
        )

    def queue_prefetch(self, query: str, *, session_id: str = "") -> None:
        """Queue a background context load for the NEXT turn.

        Non-blocking — schedules the async load on the background loop.
        The result is cached and returned by the next prefetch() call.
        """
        if not self._provider:
            return
        try:
            loop = _get_loop()
            asyncio.run_coroutine_threadsafe(
                self._queue_prefetch_async(query, max_results=10),
                loop,
            )
        except Exception:
            logger.exception("HermesProvider: queue_prefetch failed")

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        """Recall relevant context for the upcoming turn.

        Returns cached context from the PREVIOUS queue_prefetch() call.
        Fast — never blocks on I/O. If no cached context, triggers a
        synchronous load as fallback.
        """
        if not self._provider:
            return ""

        # Return cached context if available
        cached = self._context_cache.get("prefetch", "")
        if cached:
            return cached

        # Fallback: sync load (first turn, no prior queue_prefetch)
        try:
            result = _run_async(
                self._prefetch_async(query, max_results=10),
                timeout=10.0,
            )
            if result:
                return result
        except Exception:
            logger.exception("HermesProvider: prefetch failed")
            return ""

        return ""

    async def _queue_prefetch_async(self, query: str, max_results: int = 10) -> None:
        """Async prefetch that caches result in _context_cache."""
        result = await self._prefetch_async(query, max_results=max_results)
        if result:
            self._context_cache["prefetch"] = result

    async def _prefetch_async(self, query: str, max_results: int = 10) -> str:
        """Async prefetch implementation.

        Calls the get_context API and returns a formatted block.
        """
        from memory_server.api.get_context import get_context as get_context_fn

        if not query:
            return ""

        context = await get_context_fn(
            self._provider,
            task=query,
            subject=None,
            max_results=max_results,
        )

        if not context.get("facts") and not context.get("decisions"):
            return ""

        # Format as a system prompt block
        lines: list[str] = []
        lines.append("--- Memory Context ---")
        for fact in context["facts"]:
            lines.append(
                f"  {fact.get('subject', '?')} {fact.get('predicate', '?')} "
                f"{fact.get('object', '?')}"
                f" (confidence: {fact.get('confidence', 1.0):.2f})"
            )
        if context.get("decisions"):
            lines.append("  Decisions:")
            for dec in context["decisions"]:
                lines.append(f"    - {dec.get('context', '?')}: {dec.get('choice', '?')}")
        lines.append("--- End Memory Context ---")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Turn sync (non-blocking)
    # ------------------------------------------------------------------

    def sync_turn(
        self,
        user_content: str,
        assistant_content: str,
        *,
        session_id: str = "",
        messages: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Queue a completed turn for async persistence.

        Does NOT block — the write happens via the WriterQueue background task.
        """
        if not self._writer or not self._provider:
            return

        turn_data = {
            "user_content": user_content,
            "assistant_content": assistant_content,
            "session_id": session_id or self._session_id,
            "timestamp": time.time(),
        }

        _run_async(
            self._writer.add_turn(messages or [turn_data], turn_id=session_id),
            timeout=5.0,
        )

        # Queue background prefetch for the next turn
        if user_content:
            self.queue_prefetch(user_content, session_id=session_id)

    # ------------------------------------------------------------------
    # Session hooks
    # ------------------------------------------------------------------

    def on_session_end(self, messages: List[Dict[str, Any]]) -> None:
        """Flush the writer queue when a session ends."""
        if not self._writer:
            return
        logger.info("HermesProvider: session ended — flushing writer queue")
        try:
            _run_async(self._writer.flush(), timeout=30.0)
        except Exception:
            logger.exception("HermesProvider: on_session_end flush failed")

    def on_session_switch(
        self,
        new_session_id: str,
        *,
        parent_session_id: str = "",
        reset: bool = False,
        rewound: bool = False,
        **kwargs,
    ) -> None:
        """Flush writer queue and clear per-session cache on session switch."""
        if not self._writer:
            return

        logger.info(
            "HermesProvider: session switch %s -> %s (reset=%s, rewound=%s)",
            self._session_id,
            new_session_id,
            reset,
            rewound,
        )

        # Flush pending writes
        try:
            _run_async(self._writer.flush(), timeout=30.0)
        except Exception:
            logger.exception("HermesProvider: on_session_switch flush failed")

        # Update session state
        self._session_id = new_session_id
        if reset:
            self._context_cache.clear()

    # ------------------------------------------------------------------
    # Tool surface
    # ------------------------------------------------------------------

    def get_tool_schemas(self) -> List[Dict[str, Any]]:
        """Return all 14 CMMS tools as native Hermes tool schemas.

        Each schema follows the OpenAI function calling format.
        Tools are exposed WITHOUT the ``mcp_`` prefix — they are
        first-class Hermes tools.
        """
        return [
            {
                "name": "ping",
                "description": "Connectivity check — returns OK if CMMS is alive",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            },
            {
                "name": "search",
                "description": "Search stored facts by keyword text with optional subject/predicate filters",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Free-text keyword to search across subject/predicate/object",
                        },
                        "subject": {
                            "type": "string",
                            "description": "Optional exact subject filter",
                        },
                        "predicate": {
                            "type": "string",
                            "description": "Optional exact predicate filter",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of results (default 50)",
                            "default": 50,
                        },
                    },
                    "required": [],
                },
            },
            {
                "name": "remember",
                "description": "Store a fact with provenance receipt. Writes to SQL and queues async indexing",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "subject": {
                            "type": "string",
                            "description": "The subject of the fact",
                        },
                        "predicate": {
                            "type": "string",
                            "description": "The predicate/relation",
                        },
                        "object": {
                            "type": "string",
                            "description": "The object of the fact",
                        },
                        "confidence": {
                            "type": "number",
                            "description": "Confidence score 0.0-1.0 (default 1.0)",
                            "default": 1.0,
                        },
                        "source": {
                            "type": "string",
                            "description": "Source identifier (default 'user')",
                            "default": "user",
                        },
                    },
                    "required": ["subject", "predicate", "object"],
                },
            },
            {
                "name": "get_context",
                "description": "Retrieve structured context about a task — facts and decisions relevant to the turn",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "task": {
                            "type": "string",
                            "description": "The task description or search query",
                        },
                        "subject": {
                            "type": "string",
                            "description": "Optional subject filter",
                        },
                        "max_results": {
                            "type": "integer",
                            "description": "Maximum number of facts to return (default 10)",
                            "default": 10,
                        },
                    },
                    "required": ["task"],
                },
            },
            {
                "name": "learn",
                "description": "Extract and store facts, decisions, skills, and optionally beliefs from text",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "text": {
                            "type": "string",
                            "description": "Natural language text to extract knowledge from",
                        },
                        "source": {
                            "type": "string",
                            "description": "Source identifier (default 'user')",
                            "default": "user",
                        },
                        "extract_beliefs": {
                            "type": "boolean",
                            "description": "If True, also extract and store beliefs",
                            "default": False,
                        },
                        "min_belief_confidence": {
                            "type": "number",
                            "description": "Minimum confidence to create a belief (default 0.6)",
                            "default": 0.6,
                        },
                    },
                    "required": ["text"],
                },
            },
            {
                "name": "semantic_search",
                "description": "Semantic search — embed a query and find similar facts by vector similarity",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Natural language query text",
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "Maximum number of results (default 10)",
                            "default": 10,
                        },
                        "score_threshold": {
                            "type": "number",
                            "description": "Minimum similarity score 0.0-1.0 (default 0.0)",
                            "default": 0.0,
                        },
                    },
                    "required": [],
                },
            },
            {
                "name": "graph_search",
                "description": "Search the knowledge graph for entities/relations by node lookup, pathfinding, or text",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Text to extract entity references from",
                        },
                        "entity_id": {
                            "type": "string",
                            "description": "Direct node ID lookup",
                        },
                        "source_id": {
                            "type": "string",
                            "description": "Source entity for pathfinding",
                        },
                        "target_id": {
                            "type": "string",
                            "description": "Target entity for pathfinding",
                        },
                    },
                    "required": [],
                },
            },
            {
                "name": "route",
                "description": "Route query through 4-stage router (rules->embeddings->graph->LLM) per ADR-005",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Natural language query text",
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "Maximum semantic search results (default 10)",
                            "default": 10,
                        },
                        "score_threshold": {
                            "type": "number",
                            "description": "Minimum similarity score 0.0-1.0 (default 0.0)",
                            "default": 0.0,
                        },
                    },
                    "required": [],
                },
            },
            {
                "name": "audit",
                "description": "Run a memory audit for consistency, orphans, confidence distribution, and drift",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "audit_type": {
                            "type": "string",
                            "description": "One of 'full', 'consistency', 'orphans', or 'confidence'",
                            "default": "full",
                            "enum": ["full", "consistency", "orphans", "confidence"],
                        },
                    },
                    "required": [],
                },
            },
            {
                "name": "metrics",
                "description": "Return a Prometheus-formatted snapshot of observability metrics",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            },
            {
                "name": "set_belief",
                "description": "Create/reinforce/supersede a belief with evidence. Matching beliefs get reinforced",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "proposition": {
                            "type": "string",
                            "description": "The belief proposition text",
                        },
                        "confidence": {
                            "type": "number",
                            "description": "Confidence score 0.0-1.0 (default 0.5)",
                            "default": 0.5,
                        },
                        "sources": {
                            "type": "string",
                            "description": "JSON array of evidence source dicts with source_type/source_id/weight",
                            "default": "[]",
                        },
                        "tags": {
                            "type": "string",
                            "description": "JSON array of tag strings",
                            "default": "[]",
                        },
                        "source": {
                            "type": "string",
                            "description": "Source identifier (default 'system')",
                            "default": "system",
                        },
                        "replace_belief_id": {
                            "type": "string",
                            "description": "If set, supersede the referenced belief and link this new one",
                        },
                    },
                    "required": ["proposition"],
                },
            },
            {
                "name": "get_belief",
                "description": "Search beliefs — filter by proposition, lifecycle, confidence, tags, source, creator",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "proposition": {
                            "type": "string",
                            "description": "Search proposition text (FTS5 full-text search)",
                        },
                        "lifecycle_state": {
                            "type": "string",
                            "description": "Filter by lifecycle state (default 'active')",
                            "default": "active",
                        },
                        "min_confidence": {
                            "type": "number",
                            "description": "Minimum confidence threshold 0.0-1.0",
                            "default": 0.0,
                        },
                        "tags": {
                            "type": "string",
                            "description": "JSON array of tag strings to filter by",
                        },
                        "source": {
                            "type": "string",
                            "description": "Filter by source identifier",
                        },
                        "creator": {
                            "type": "string",
                            "description": "Filter by creator identifier",
                        },
                        "source_id": {
                            "type": "string",
                            "description": "Filter by source_id in the belief's source_ids list",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of results (default 10, max 100)",
                            "default": 10,
                        },
                    },
                    "required": [],
                },
            },
            {
                "name": "resolve_conflict",
                "description": "Resolve a conflict between two beliefs (keep_a/b/merge/discard_both, or auto-resolve)",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "belief_a_id": {
                            "type": "string",
                            "description": "UUID of the first belief in the conflict",
                        },
                        "belief_b_id": {
                            "type": "string",
                            "description": "UUID of the second belief in the conflict",
                        },
                        "resolution": {
                            "type": "string",
                            "description": "Strategy: keep_a, keep_b, merge, discard_both",
                            "enum": ["keep_a", "keep_b", "merge", "discard_both"],
                        },
                        "new_proposition": {
                            "type": "string",
                            "description": "Proposition for a new merged belief (required for merge)",
                        },
                        "auto_resolve": {
                            "type": "boolean",
                            "description": "When True, auto-resolve by confidence threshold",
                            "default": False,
                        },
                    },
                    "required": ["belief_a_id", "belief_b_id", "resolution"],
                },
            },
            {
                "name": "reflect",
                "description": "Analyse belief store (overview, contradictions, decay, topics, audit, confidence)",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "mode": {
                            "type": "string",
                            "description": "Mode: overview, contradictions, decay, topics, audit, or confidence",
                            "default": "overview",
                            "enum": [
                                "overview", "contradictions", "decay", "topics",
                                "evidence_audit", "confidence",
                            ],
                        },
                        "topic": {
                            "type": "string",
                            "description": "Optional topic/tag filter",
                        },
                        "min_confidence": {
                            "type": "number",
                            "description": "Minimum confidence threshold 0.0-1.0",
                            "default": 0.0,
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Max beliefs to analyse (0 = all, default 50)",
                            "default": 50,
                        },
                    },
                    "required": [],
                },
            },
        ]

    def handle_tool_call(self, tool_name: str, args: Dict[str, Any], **kwargs) -> str:
        """Handle a tool call for one of CMMS's tools.

        Routes to the existing CMMS API function, bypassing the MCP transport layer.
        Returns a JSON string (the tool result).

        Raises ValueError for unknown tool names.
        """
        if not self._provider:
            return json.dumps({"error": "HermesProvider not initialized"})

        handler = self._get_tool_handler(tool_name)
        if handler is None:
            raise ValueError(f"Unknown CMMS tool: {tool_name}")

        try:
            result = _run_async(handler(**args), timeout=60.0)
            return result if isinstance(result, str) else json.dumps(result)
        except Exception as exc:
            logger.exception("HermesProvider: tool call '%s' failed", tool_name)
            return json.dumps({"error": str(exc), "tool": tool_name})

    def _get_tool_handler(self, name: str):
        """Return the async handler function for a given tool name."""
        handlers = {
            "ping": self._handle_ping,
            "search": self._handle_search,
            "remember": self._handle_remember,
            "get_context": self._handle_get_context,
            "learn": self._handle_learn,
            "semantic_search": self._handle_semantic_search,
            "graph_search": self._handle_graph_search,
            "route": self._handle_route,
            "audit": self._handle_audit,
            "metrics": self._handle_metrics,
            "set_belief": self._handle_set_belief,
            "get_belief": self._handle_get_belief,
            "resolve_conflict": self._handle_resolve_conflict,
            "reflect": self._handle_reflect,
        }
        return handlers.get(name)

    # ------------------------------------------------------------------
    # Tool handlers (async — called via _run_async)
    # ------------------------------------------------------------------

    async def _handle_ping(self) -> str:
        return json.dumps({"status": "ok"})

    async def _handle_search(
        self,
        query: str = "",
        subject: str = "",
        predicate: str = "",
        limit: int = 50,
    ) -> str:
        from memory_server.api.search import search as search_fn
        result = await search_fn(
            self._provider,
            query=query,
            subject=subject if subject else None,
            predicate=predicate if predicate else None,
            limit=limit,
        )
        return json.dumps(result)

    async def _handle_remember(
        self,
        subject: str,
        predicate: str,
        object: str,
        confidence: float = 1.0,
        source: str = "user",
    ) -> str:
        from memory_server.api.remember import remember as remember_fn
        result = await remember_fn(
            self._provider,
            subject=subject,
            predicate=predicate,
            object=object,
            confidence=confidence,
            source=source,
        )
        # Serialize Pydantic models
        fact = result["fact"]
        serialized = {
            "receipt": result["receipt"].model_dump(mode="json"),
            "fact": fact.model_dump(mode="json"),
        }
        return json.dumps(serialized)

    async def _handle_get_context(
        self,
        task: str,
        subject: str = "",
        max_results: int = 10,
    ) -> str:
        from memory_server.api.get_context import get_context as get_context_fn
        result = await get_context_fn(
            self._provider,
            task=task,
            subject=subject if subject else None,
            max_results=max_results,
        )
        return json.dumps(result)

    async def _handle_learn(
        self,
        text: str,
        source: str = "user",
        extract_beliefs: bool = False,
        min_belief_confidence: float = 0.6,
    ) -> str:
        from memory_server.api.learn import learn as learn_fn
        result = await learn_fn(
            provider=self._provider,
            text=text,
            source=source,
            extract_beliefs=extract_beliefs,
            min_belief_confidence=min_belief_confidence,
        )
        return json.dumps(result)

    async def _handle_semantic_search(
        self,
        query: str = "",
        top_k: int = 10,
        score_threshold: float = 0.0,
    ) -> str:
        # Lazy-init Qdrant + Embedder
        from memory_server.providers.embedding_provider import SentenceTransformerEmbeddingProvider
        from memory_server.providers.qdrant_provider import QdrantProvider
        from memory_server.router.embedding_router import EmbeddingRouter

        if self._qdrant is None:
            self._qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        if self._embedder is None:
            self._embedder = SentenceTransformerEmbeddingProvider()

        router = EmbeddingRouter(
            vector_provider=self._qdrant,
            embedder=self._embedder,
        )
        results = await router.route(
            query=query,
            top_k=top_k,
            score_threshold=score_threshold,
        )
        return json.dumps(results)

    async def _handle_graph_search(
        self,
        query: str = "",
        entity_id: str = "",
        source_id: str = "",
        target_id: str = "",
    ) -> str:
        from memory_server.providers.graph_provider import SimpleGraph
        from memory_server.router.graph_router import GraphRouter

        if self._graph is None:
            self._graph = SimpleGraph()
        graph_router = GraphRouter(graph=self._graph)
        graph = graph_router.graph

        nodes: list[dict] = []
        edges: list[dict] = []
        paths: list[list[dict]] = []

        if entity_id:
            node = graph.get_node(entity_id)
            if node is not None:
                nodes.append({
                    "id": node.id,
                    "name": node.name,
                    "type": node.type,
                    "attributes": node.attributes,
                })
                neighbors = graph.get_neighbors(entity_id)
                for neighbor_node, edge in neighbors:
                    nodes.append({
                        "id": neighbor_node.id,
                        "name": neighbor_node.name,
                        "type": neighbor_node.type,
                        "attributes": neighbor_node.attributes,
                    })
                    edges.append({
                        "source_id": edge.source_id,
                        "target_id": edge.target_id,
                        "relation": edge.relation,
                        "attributes": edge.attributes,
                    })
        elif source_id and target_id:
            found_paths = graph.find_path(source_id, target_id, max_depth=4)
            for p in found_paths:
                paths.append([
                    {"id": n.id, "name": n.name, "type": n.type}
                    for n in p
                ])
        elif query:
            result = graph_router.query(query)
            nodes = result.get("entities", [])
            edges = result.get("relations", [])
            paths = result.get("paths", [])

        return json.dumps({
            "nodes": nodes,
            "edges": edges,
            "paths": paths,
        })

    async def _handle_route(
        self,
        query: str = "",
        top_k: int = 10,
        score_threshold: float = 0.0,
    ) -> str:
        from memory_server.providers.embedding_provider import SentenceTransformerEmbeddingProvider
        from memory_server.providers.graph_provider import SimpleGraph
        from memory_server.providers.qdrant_provider import QdrantProvider
        from memory_server.router.hybrid_router import HybridRouter

        if self._qdrant is None:
            self._qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        if self._embedder is None:
            self._embedder = SentenceTransformerEmbeddingProvider()
        if self._graph is None:
            self._graph = SimpleGraph()

        router = HybridRouter(
            vector_provider=self._qdrant,
            embedder=self._embedder,
            graph=self._graph,
        )
        result = await router.route(
            query=query,
            top_k=top_k,
            score_threshold=score_threshold,
        )
        return json.dumps(result)

    async def _handle_audit(
        self,
        audit_type: str = "full",
    ) -> str:
        from memory_server.evaluation.auditor import MemoryAuditor
        from memory_server.evaluation.confidence import ConfidenceEngine
        from memory_server.evaluation.validator import Validator
        from memory_server.providers.graph_provider import SimpleGraph

        validator = Validator()
        confidence_engine = ConfidenceEngine()
        graph = self._graph or SimpleGraph()

        auditor = MemoryAuditor(
            validator=validator,
            confidence_engine=confidence_engine,
            graph=graph,
        )
        report = auditor.audit_report(audit_type=audit_type)
        return json.dumps(report)

    async def _handle_metrics(self) -> str:
        from memory_server.evaluation.metrics import generate_latest
        return generate_latest().decode("utf-8")

    async def _handle_set_belief(
        self,
        proposition: str,
        confidence: float = 0.5,
        sources: str = "[]",
        tags: str = "[]",
        source: str = "system",
        replace_belief_id: str = "",
    ) -> str:
        import json as _json
        from datetime import datetime, timezone

        from memory_server.models.belief import Belief
        from memory_server.models.evidence import Evidence
        from memory_server.models.receipt import MemoryReceipt

        parsed_sources = _json.loads(sources) if sources else []
        parsed_tags = _json.loads(tags) if tags else []

        # Check for existing active belief with same proposition
        existing = await self._provider.search_beliefs(
            proposition=proposition,
            lifecycle_state=None,
            limit=100,
        )

        # Find exact match
        match = None
        norm = proposition.strip().lower()
        for b in existing:
            if b.proposition.strip().lower() == norm and b.lifecycle_state == "active":
                match = b
                break

        if match:
            new_confidence = max(0.0, min(1.0, (match.confidence + confidence) / 2))
            await self._provider.update_belief_confidence(match.id, new_confidence)
            await self._provider.update_belief_reinforced_at(match.id)
            receipt = MemoryReceipt(
                id=match.id,
                memory_type="belief",
                source=source,
                created_by=source,
                timestamp=datetime.now(timezone.utc),
                confidence=new_confidence,
            )
            serialized = {
                "belief": match.model_dump(mode="json"),
                "receipt": receipt.model_dump(mode="json"),
                "superseded": None,
                "reinforced": True,
            }
            return json.dumps(serialized)

        # Create the belief
        belief = Belief(
            proposition=proposition,
            confidence=confidence,
            source=source,
            tags=parsed_tags,
            creator=source,
        )

        evidence_list = []
        for s in parsed_sources:
            ev = Evidence(
                belief_id=belief.id,
                source_type=s.get("source_type", "observation"),
                source_id=s.get("source_id", ""),
                weight=s.get("weight", 0.5),
                contributor=source,
            )
            evidence_list.append(ev)

        superseded = None
        if replace_belief_id:
            old_belief = await self._provider.get_belief(replace_belief_id)
            if old_belief:
                superseded = old_belief.model_dump(mode="json")
                await self._provider.update_belief_lifecycle(replace_belief_id, "superseded")
                belief.version = old_belief.version + 1

        receipt = MemoryReceipt(
            id=belief.id,
            memory_type="belief",
            source=source,
            created_by=source,
            timestamp=datetime.now(timezone.utc),
            confidence=confidence,
        )

        await self._provider.create_in_transaction(
            belief=belief,
            evidence_list=evidence_list,
            receipt=receipt,
            outbox_entries=[
                {
                    "record_type": "belief",
                    "record_id": belief.id,
                    "operation": "index_belief",
                    "payload": {
                        "proposition": proposition,
                        "tags": parsed_tags,
                        "confidence": confidence,
                        "source": source,
                    },
                }
            ],
        )

        serialized = {
            "belief": belief.model_dump(mode="json"),
            "receipt": receipt.model_dump(mode="json"),
            "superseded": superseded,
        }

        return json.dumps(serialized)

    async def _handle_get_belief(
        self,
        proposition: str = "",
        lifecycle_state: str = "active",
        min_confidence: float = 0.0,
        tags: str = "",
        source: str = "",
        creator: str = "",
        source_id: str = "",
        limit: int = 10,
    ) -> str:
        import json as _json

        parsed_tags = _json.loads(tags) if tags else None

        results = await self._provider.search_beliefs(
            proposition=proposition or None,
            lifecycle_state=lifecycle_state or None,
            min_confidence=min_confidence if min_confidence > 0 else None,
            tags=parsed_tags,
            source=source or None,
            creator=creator or None,
            limit=min(limit, 100),
        )

        if source_id:
            results = [b for b in results if source_id in b.source_ids]

        serialized = {
            "total": len(results),
            "beliefs": [b.model_dump(mode="json") for b in results],
            "query": {
                "proposition": proposition,
                "lifecycle_state": lifecycle_state,
                "min_confidence": min_confidence,
                "tags": parsed_tags,
                "source": source,
                "creator": creator,
                "source_id": source_id,
                "limit": limit,
            },
        }

        return json.dumps(serialized)

    async def _handle_resolve_conflict(
        self,
        belief_a_id: str,
        belief_b_id: str,
        resolution: str,
        new_proposition: str = "",
        auto_resolve: bool = False,
    ) -> str:
        import uuid
        from datetime import datetime, timezone

        from sqlalchemy.ext.asyncio import AsyncSession
        from storage.repositories import LifecycleRepository

        from memory_server.models.receipt import MemoryReceipt

        belief_a = await self._provider.get_belief(belief_a_id)
        belief_b = await self._provider.get_belief(belief_b_id)
        if belief_a is None or belief_b is None:
            raise ValueError("Both belief_a and belief_b must exist in the store")

        if auto_resolve:
            events = []
            confidence_diff = abs(belief_a.confidence - belief_b.confidence)

            async with AsyncSession(self._provider.engine) as lc_session:
                lifecycle_repo = LifecycleRepository(lc_session)

                if confidence_diff > 0.5:
                    if belief_a.confidence < belief_b.confidence:
                        lower_id, lower_conf = belief_a_id, belief_a.confidence
                        higher_conf = belief_b.confidence
                        lower_state = belief_a.lifecycle_state
                    else:
                        lower_id, lower_conf = belief_b_id, belief_b.confidence
                        higher_conf = belief_a.confidence
                        lower_state = belief_b.lifecycle_state

                    await self._provider.update_belief_lifecycle(lower_id, "superseded")
                    await lifecycle_repo.record_event(
                        memory_id=lower_id,
                        memory_type="belief",
                        from_state=lower_state,
                        to_state="superseded",
                        reason=(f"Auto-resolved — confidence gap "
                                f"({higher_conf:.2f} vs {lower_conf:.2f})"),
                        triggered_by="system",
                    )
                    events.append({
                        "belief_id": lower_id,
                        "from_state": lower_state,
                        "to_state": "superseded",
                        "reason": (f"Auto-resolved — confidence gap "
                                  f"({higher_conf:.2f} vs {lower_conf:.2f})"),
                    })
                else:
                    for bid, bstate in (
                        (belief_a_id, belief_a.lifecycle_state),
                        (belief_b_id, belief_b.lifecycle_state),
                    ):
                        await self._provider.update_belief_lifecycle(bid, "contradicted")
                        await lifecycle_repo.record_event(
                            memory_id=bid,
                            memory_type="belief",
                            from_state=bstate,
                            to_state="contradicted",
                            reason=("Auto-resolved — needs manual review "
                                    "(confidences too close or both low)"),
                            triggered_by="system",
                        )
                        events.append({
                            "belief_id": bid,
                            "from_state": bstate,
                            "to_state": "contradicted",
                            "reason": ("Auto-resolved — needs manual review "
                                      "(confidences too close or both low)"),
                        })

                await lc_session.commit()

            receipt = MemoryReceipt(
                id=str(uuid.uuid4()),
                memory_type="belief",
                source="conflict_resolution",
                created_by="system",
                timestamp=datetime.now(timezone.utc),
            )

            return json.dumps({
                "belief_a": (await self._provider.get_belief(belief_a_id)).model_dump(mode="json"),
                "belief_b": (await self._provider.get_belief(belief_b_id)).model_dump(mode="json"),
                "resolution": resolution,
                "auto_resolved": True,
                "created": None,
                "events": events,
                "receipt": receipt.model_dump(mode="json"),
            })

        # Manual resolution
        events = []
        merged = None

        async with AsyncSession(self._provider.engine) as lc_session:
            lifecycle_repo = LifecycleRepository(lc_session)

            if resolution == "keep_a":
                await self._provider.update_belief_lifecycle(belief_b_id, "discarded")
                await lifecycle_repo.record_event(
                    memory_id=belief_b_id,
                    memory_type="belief",
                    from_state=belief_b.lifecycle_state,
                    to_state="discarded",
                    reason=f"Discarded in favor of {belief_a_id} via conflict resolution",
                    triggered_by="system",
                )
                events.append({
                    "belief_id": belief_b_id,
                    "from_state": belief_b.lifecycle_state,
                    "to_state": "discarded",
                    "reason": f"Discarded in favor of {belief_a_id} via conflict resolution",
                })
            elif resolution == "keep_b":
                await self._provider.update_belief_lifecycle(belief_a_id, "discarded")
                await lifecycle_repo.record_event(
                    memory_id=belief_a_id,
                    memory_type="belief",
                    from_state=belief_a.lifecycle_state,
                    to_state="discarded",
                    reason=f"Discarded in favor of {belief_b_id} via conflict resolution",
                    triggered_by="system",
                )
                events.append({
                    "belief_id": belief_a_id,
                    "from_state": belief_a.lifecycle_state,
                    "to_state": "discarded",
                    "reason": f"Discarded in favor of {belief_b_id} via conflict resolution",
                })
            elif resolution == "merge":
                if not new_proposition:
                    raise ValueError("new_proposition is required when resolution='merge'")
                from memory_server.models.belief import Belief
                from memory_server.models.evidence import Evidence

                merged = Belief(
                    proposition=new_proposition,
                    confidence=min(1.0, (belief_a.confidence + belief_b.confidence) / 2),
                    source="conflict_resolution",
                    creator="system",
                    tags=list(set(belief_a.tags + belief_b.tags)),
                    source_ids=list(set(belief_a.source_ids + belief_b.source_ids)),
                )

                copied_evidence = []
                try:
                    from storage.repositories import EvidenceRepository

                    async with AsyncSession(self._provider.engine) as ev_session:
                        ev_repo = EvidenceRepository(ev_session)
                        for orig_id in (belief_a_id, belief_b_id):
                            ev_rows = await ev_repo.get_by_belief_id(orig_id)
                            for ev in ev_rows:
                                new_ev = Evidence(
                                    belief_id=merged.id,
                                    source_type=ev.source_type,
                                    source_id=ev.source_id,
                                    weight=ev.weight,
                                    contributor=ev.contributor,
                                )
                                copied_evidence.append(new_ev)
                except Exception:
                    logger.warning(
                        "Failed to copy evidence for merged belief %s",
                        merged.id,
                        exc_info=True,
                    )

                merged_receipt = MemoryReceipt(
                    id=merged.id,
                    memory_type="belief",
                    source="conflict_resolution",
                    created_by="system",
                    timestamp=datetime.now(timezone.utc),
                    confidence=merged.confidence,
                )
                await self._provider.create_in_transaction(
                    belief=merged,
                    evidence_list=copied_evidence,
                    receipt=merged_receipt,
                    outbox_entries=[{
                        "record_type": "belief",
                        "record_id": merged.id,
                        "operation": "index_belief",
                        "payload": {
                            "proposition": merged.proposition,
                            "tags": merged.tags,
                            "confidence": merged.confidence,
                            "source": "conflict_resolution",
                        },
                    }],
                )

                await self._provider.update_belief_lifecycle(belief_a_id, "superseded")
                await lifecycle_repo.record_event(
                    memory_id=belief_a_id,
                    memory_type="belief",
                    from_state=belief_a.lifecycle_state,
                    to_state="superseded",
                    reason=f"Merged into {merged.id} via conflict resolution",
                    triggered_by="system",
                )
                events.append({
                    "belief_id": belief_a_id,
                    "from_state": belief_a.lifecycle_state,
                    "to_state": "superseded",
                    "reason": f"Merged into {merged.id} via conflict resolution",
                })

                await self._provider.update_belief_lifecycle(belief_b_id, "superseded")
                await lifecycle_repo.record_event(
                    memory_id=belief_b_id,
                    memory_type="belief",
                    from_state=belief_b.lifecycle_state,
                    to_state="superseded",
                    reason=f"Merged into {merged.id} via conflict resolution",
                    triggered_by="system",
                )
                events.append({
                    "belief_id": belief_b_id,
                    "from_state": belief_b.lifecycle_state,
                    "to_state": "superseded",
                    "reason": f"Merged into {merged.id} via conflict resolution",
                })
            elif resolution == "discard_both":
                for bid, bstate in (
                    (belief_a_id, belief_a.lifecycle_state),
                    (belief_b_id, belief_b.lifecycle_state),
                ):
                    await self._provider.update_belief_lifecycle(bid, "discarded")
                    await lifecycle_repo.record_event(
                        memory_id=bid,
                        memory_type="belief",
                        from_state=bstate,
                        to_state="discarded",
                        reason="Discarded via conflict resolution",
                        triggered_by="system",
                    )
                    events.append({
                        "belief_id": bid,
                        "from_state": bstate,
                        "to_state": "discarded",
                        "reason": "Discarded via conflict resolution",
                    })
            else:
                raise ValueError(
                    f"Unknown resolution: {resolution}. "
                    "Must be one of: keep_a, keep_b, merge, discard_both"
                )

            await lc_session.commit()

        receipt = MemoryReceipt(
            id=str(uuid.uuid4()),
            memory_type="belief",
            source="conflict_resolution",
            created_by="system",
            timestamp=datetime.now(timezone.utc),
        )

        return json.dumps({
            "belief_a": (await self._provider.get_belief(belief_a_id)).model_dump(mode="json"),
            "belief_b": (await self._provider.get_belief(belief_b_id)).model_dump(mode="json"),
            "resolution": resolution,
            "created": merged.model_dump(mode="json") if resolution == "merge" and merged else None,
            "events": events,
            "receipt": receipt.model_dump(mode="json"),
        })

    async def _handle_reflect(
        self,
        mode: str = "overview",
        topic: str = "",
        min_confidence: float = 0.0,
        limit: int = 50,
    ) -> str:
        from memory_server.api.reflect import ReflectEngine

        valid_modes = {
            "overview", "contradictions", "decay", "topics",
            "evidence_audit", "confidence",
        }
        if mode not in valid_modes:
            return json.dumps({
                "error": f"Invalid mode '{mode}'. Must be one of: {', '.join(sorted(valid_modes))}",
            })

        engine = ReflectEngine(self._provider)
        topic_param = topic if topic else None

        method_map = {
            "overview": engine.overview,
            "contradictions": engine.contradictions,
            "decay": engine.decay_analysis,
            "topics": engine.topics,
            "evidence_audit": engine.evidence_audit,
            "confidence": engine.confidence_histogram,
        }

        method = method_map[mode]
        result = await method(
            topic=topic_param,
            min_confidence=min_confidence,
            limit=limit,
        )
        return json.dumps(result)

    # ------------------------------------------------------------------
    # Writer queue batch handler
    # ------------------------------------------------------------------

    async def _handle_batch_write(self, batch: list[tuple[list, str | None]]):
        """Process a batch of turn observations.

        Called by WriterQueue during periodic or explicit flushes.
        Each item is a (messages, turn_id) tuple.
        """
        if not self._provider:
            logger.warning("HermesProvider: cannot write batch — not initialized")
            return

        for messages, turn_id in batch:
            try:
                # Extract text from the turn for CMMS ingestion
                text_content = self._extract_turn_text(messages)
                if text_content:
                    from memory_server.api.learn import learn as learn_fn

                    await learn_fn(
                        provider=self._provider,
                        text=text_content,
                        source=f"hermes_turn_{turn_id or 'unknown'}",
                        extract_beliefs=False,
                    )
            except Exception:
                logger.exception(
                    "HermesProvider: failed to write turn %s in batch",
                    turn_id,
                )

    @staticmethod
    def _extract_turn_text(messages: list | dict) -> str:
        """Extract meaningful text from turn messages for ingestion."""
        if isinstance(messages, dict):
            return messages.get("user_content", "") + "\n" + messages.get("assistant_content", "")

        if isinstance(messages, list):
            parts = []
            for msg in messages:
                if isinstance(msg, dict):
                    content = msg.get("content", "")
                    if isinstance(content, list):
                        for block in content:
                            if isinstance(block, dict) and block.get("type") == "text":
                                parts.append(block.get("text", ""))
                    elif isinstance(content, str):
                        parts.append(content)
            return "\n".join(parts)

        return str(messages)
