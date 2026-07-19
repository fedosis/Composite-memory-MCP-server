# Card 001: Native MemoryProvider Plugin — v2 (post-arch-review)

## Objective

Add a native Hermes MemoryProvider plugin to CMMS as documented in **ADR-013**.
Instead of relying solely on MCP protocol, CMMS becomes a first-class `MemoryProvider` in Hermes — gaining access to lifecycle hooks (`prefetch`, `queue_prefetch`, `sync_turn`, `on_session_end`, `on_session_switch`) that are unavailable over MCP.

## Motivation

Currently CMMS is MCP-only (per ADR-004). This means:
- **No auto-recall**: CMMS cannot push context into the agent's system prompt before each turn (`prefetch`)
- **No auto-retain**: CMMS cannot observe every user turn and persist relevant information automatically (`sync_turn`)
- **No session hooks**: CMMS doesn't know when a session ends, switches, or compresses (`on_session_end`, `on_session_switch`)
- **No tool mirroring**: CMMS tools exist in a separate MCP namespace, not as first-class Hermes tools

Per **ADR-013**, CMMS now supports **two access paths**: MCP (unchanged, for external agents) + Hermes MemoryProvider (new, v0.8). Hindsight (`plugins/memory/hindsight/`) proves this pattern works.

## Data Model — No new tables

Card 001 does NOT add new database tables. It reuses all existing CMMS storage (SQLite, Qdrant, graph) through the existing service layer. The MemoryProvider plugin is a thin adapter that:

1. Registers CMMS as `memory_server` provider in Hermes config
2. Calls existing CMMS services programmatically during lifecycle hooks (bypassing MCP transport per ADR-013)
3. Exposes CMMS tools as first-class Hermes tools (without `mcp_` prefix)

## Hermes MemoryProvider ABC — Real Contract

Based on `agent/memory_provider.py` in Hermes Agent. **All core lifecycle methods are SYNCHRONOUS** — Hermes calls them from the main thread.

```python
class MemoryProvider(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def is_available(self) -> bool: ...
    def initialize(self, session_id: str, **kwargs) -> None: ...
    def shutdown(self) -> None: ...

    # System prompt and turn-level hooks
    def system_prompt_block(self) -> str: ...
    def prefetch(self, query: str, *, session_id: str = "") -> str: ...
    def queue_prefetch(self, query: str, *, session_id: str = "") -> None: ...

    # Turn persistence
    def sync_turn(
        self,
        user_content: str,
        assistant_content: str,
        *,
        session_id: str = "",
        messages: Optional[List[Dict[str, Any]]] = None,
    ) -> None: ...

    # Tool surface
    def get_tool_schemas(self) -> List[Dict[str, Any]]: ...
    def handle_tool_call(self, tool_name: str, args: Dict[str, Any], **kwargs) -> str: ...

    # Session lifecycle
    def on_session_end(self, messages: List[Dict[str, Any]]) -> None: ...
    def on_session_switch(
        self,
        new_session_id: str,
        *,
        parent_session_id: str = "",
        reset: bool = False,
        rewound: bool = False,
        **kwargs,
    ) -> None: ...
```

**Key implementation notes:**
- All lifecycle methods are **sync**. Use background event loop thread (Hindsight pattern) to bridge with CMMS async services.
- `initialize(**kwargs)` receives `hermes_home` (for profile isolation), `platform`, `agent_context` — use these for path resolution.
- `prefetch()` should be **fast** — return cached results. Use `queue_prefetch()` for background loading.
- `sync_turn()` should be **non-blocking** — queue writes via WriterQueue.
- `on_session_switch()` must flush WriterQueue before updating session cache.

## Implementation

### Plugin package structure

```
src/memory_server/
├── plugins/
│   ├── __init__.py
│   ├── hermes/
│   │   ├── __init__.py
│   │   ├── provider.py          # HermesProvider(MemoryProvider) — entry point
│   │   ├── config.py            # HermesPluginConfig dataclass
│   │   └── writer.py            # WriterQueue — async batch writer
│   └── ...
```

Import path: `memory_server.plugins.hermes.provider.HermesProvider`

### HermesProvider implementation

```python
class HermesProvider:
    """Adapter: Hermes MemoryProvider → CMMS internal API."""

    name = "memory_server"

    def initialize(self, session_id: str, **kwargs) -> None:
        # kwargs: hermes_home, platform, agent_context
        self._hermes_home = kwargs.get("hermes_home", "~/.hermes")
        # Start background event loop (Hindsight pattern)
        self._loop = _start_background_loop()
        # Connect to CMMS services
        self._db_url = resolve_db_url(self._hermes_home)
        # Start writer queue
        self._writer = WriterQueue(flush_interval=5.0, max_batch=50)
        self._writer.start(self._loop)

    def system_prompt_block(self) -> str:
        """Static provider info for system prompt."""
        return "Memory provider: CMMS (Composite Memory MCP Server)"

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        """Fast cached recall. Returns cached context from previous queue_prefetch."""
        return self._cached_context or ""

    def queue_prefetch(self, query: str, *, session_id: str = "") -> None:
        """Queue background context load for NEXT turn."""
        asyncio.run_coroutine_threadsafe(
            self._load_context_async(query), self._loop
        )

    def sync_turn(self, user_content: str, assistant_content: str, *,
                   session_id: str = "", messages=None) -> None:
        """Queue turn for async batch writing (non-blocking)."""
        self._writer.add_turn(user_content, assistant_content, messages)

    def get_tool_schemas(self) -> list[dict]:
        """Return all 14 CMMS tools as native Hermes schemas (no mcp_ prefix)."""
        return [...]

    def handle_tool_call(self, tool_name: str, args: dict, **kwargs) -> str:
        """Route to existing CMMS service layer (in-process, no MCP transport)."""
        return self._tool_router.call(tool_name, args)

    def on_session_end(self, messages: list) -> None:
        """Flush writer queue before session closes."""
        self._writer.flush(timeout=30.0)

    def on_session_switch(self, new_session_id: str, *,
                           parent_session_id: str = "",
                           reset: bool = False,
                           rewound: bool = False,
                           **kwargs) -> None:
        """Flush writer queue, update session cache."""
        self._writer.flush(timeout=30.0)
        self._session_cache.clear()

    def shutdown(self) -> None:
        """Flush, stop writer queue, close connections."""
        self._writer.shutdown()
```

### WriterQueue

Async batch writer pattern (ref: Hindsight). **Important: WriterQueue calls `learn()` for each turn** — this triggers FactExtractor + DecisionExtractor + SkillExtractor, each making LLM calls. Cost is proportional to batch size.

```python
class WriterQueue:
    """Non-blocking batch writer with flush-on-switch.

    Each turn in the batch calls learn() which triggers LLM extraction.
    Configure max_batch to control per-flush LLM cost.
    """

    def __init__(self, flush_interval=5.0, max_batch=50):
        self._queue = asyncio.Queue()
        self._flush_interval = flush_interval   # seconds
        self._max_batch = max_batch
        self._task = None

    def start(self, loop):
        self._task = loop.create_task(self._run())

    def add_turn(self, user_content, assistant_content, messages):
        self._queue.put_nowait((user_content, assistant_content, messages))

    def flush(self, timeout=30.0):
        """Synchronous drain of the queue with configurable timeout."""
        future = asyncio.run_coroutine_threadsafe(self._drain(), self._loop)
        future.result(timeout=timeout)

    def shutdown(self):
        """Flush remaining items, then cancel background task."""
        self.flush(timeout=30.0)
        self._task.cancel()
```

**Risks:**
- LLM cost: each `learn()` call consumes tokens. At 50 turns/batch, expect ~50 LLM calls per flush.
- Flush timeout: `learn()` takes ~3s per turn on average, so batch of 50 needs ~150s. Set timeout accordingly (30s default, adjustable).
- Consider batch-learn API (one LLM call per batch) as deferred optimization.

### Timeout policy

| Operation | Timeout | Rationale |
|-----------|---------|-----------|
| Tool call (`handle_tool_call`) | 60s | Interactive — user waiting |
| WriterQueue flush (`on_session_end/switch`) | 30s | Batch of 50 learn() calls @ ~3s each = 150s worst case; timeout at 30s means partial flush |
| Prefetch (`_load_context_async`) | 10s | Non-interactive, fast path |
| Shutdown | 30s | Must drain before exit |

### Config schema

```yaml
# In Hermes config.yaml under memory.providers.memory_server:
memory:
  providers:
    memory_server:
      plugin: memory_server.plugins.hermes.provider.HermesProvider
      enabled: true
      path: ~/memory-server   # auto-resolved relative to hermes_home
      writer:
        flush_interval: 5.0   # seconds between auto-flushes
        max_batch: 50          # max turns per flush
```

Paths resolve relative to `hermes_home` (passed in `initialize(**kwargs)`) for profile isolation.

## Tool schemas

All 14 existing CMMS tools are exposed as native Hermes tools (identical schemas, no `mcp_` prefix):

| MCP Tool | Native name | Same schema? | Notes |
|----------|-------------|-------------|-------|
| ping | ✓ ping | yes | Health check |
| search | ✓ search | yes | Keyword search |
| remember | ✓ remember | yes | Store fact |
| get_context | ✓ get_context | yes | Context retrieval |
| semantic_search | ✓ semantic_search | yes | Vector search |
| learn | ✓ learn | yes | Extract knowledge |
| graph_search | ✓ graph_search | yes | Entity lookup |
| route | ✓ route | yes | Hybrid router |
| audit | ✓ audit | yes | Memory health |
| metrics | ✓ metrics | yes | Prometheus |
| set_belief | ✓ set_belief | yes | Create belief |
| get_belief | ✓ get_belief | yes | Search beliefs |
| resolve_conflict | ✓ resolve_conflict | yes | Conflict resolution |
| reflect | ✓ reflect | yes | Belief analysis |

## Acceptance Criteria

1. ✅ `HermesProvider` meets ABC contract (all methods, correct sync signatures, no no-op stubs)
2. ✅ `is_available()` returns False before `initialize()`, True after
3. ✅ `initialize()` is idempotent (double-init doesn't crash)
4. ✅ `initialize()` failure throws exception (no partially-initialized state)
5. ✅ `system_prompt_block()` returns non-empty string
6. ✅ `prefetch()` returns cached string or "" — never blocks on I/O
7. ✅ `queue_prefetch()` returns immediately (queues background load)
8. ✅ `sync_turn()` adds to writer queue without blocking
9. ✅ No new DB tables — 0 ALTER TABLE / CREATE TABLE
10. ✅ All 14 CMMS tools exposed as native schemas (verified by diff against MCP schemas)
11. ✅ Tool names have no `mcp_` prefix, no special characters
12. ✅ Writer queue starts/stops cleanly, auto-flushes on interval
13. ✅ Writer queue flush drains queue, respects max_batch
14. ✅ `on_session_switch()` flushes writer queue and clears session cache
15. ✅ `on_session_end()` flushes writer queue
16. ✅ `shutdown()` drains all pending writes before stopping
17. ✅ Integration tests pass (mock Hermes ABC, real CMMS services)
18. ✅ Config loads from dict and env vars, resolves paths relative to hermes_home

## Non-goals (explicitly deferred)

- **CLI auto-discovery**: `memory-server install-hermes-plugin` → **Card 002**
- **Batch-learn API**: single LLM call per batch instead of per-turn learn() → deferred
- **Dual provider mode**: CMMS replaces Hermes built-in provider (MemoryManager allows exactly one external provider)
- **Production packaging**: `install.sh` or pip extras → v0.9
