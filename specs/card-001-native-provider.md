# Card 001: Native MemoryProvider Plugin — v1

## Objective

Add a native Hermes MemoryProvider plugin to CMMS. Instead of relying solely on MCP protocol, CMMS becomes a first-class `MemoryProvider` in Hermes — gaining access to lifecycle hooks (`prefetch`, `sync_turn`, `on_session_end`, `on_session_switch`) that are unavailable over MCP.

## Motivation

Currently CMMS is MCP-only. This means:
- **No auto-recall**: CMMS cannot push context into the agent's system prompt before each turn (`prefetch`)
- **No auto-retain**: CMMS cannot observe every user turn and persist relevant information automatically (`sync_turn`)
- **No session hooks**: CMMS doesn't know when a session ends, switches, or compresses
- **No tool mirroring**: CMMS tools exist in a separate MCP namespace, not as first-class Hermes tools

Hindsight (`plugins/memory/hindsight/`) proves this pattern works. CMMS needs the same.

## Data Model — No new tables

Card 001 does NOT add new database tables. It reuses all existing CMMS storage (SQLite, Qdrant/LanceDB, graph) through the existing 14 MCP tools. The MemoryProvider plugin is a thin adapter layer that:

1. Registers CMMS as `memory_server` provider in Hermes config
2. Calls existing CMMS tools programmatically (not over HTTP/MCP) during lifecycle hooks
3. Exposes CMMS tools as first-class Hermes tools (without `mcp_` prefix)

## Hermes MemoryProvider ABC — Contract

Based on `agent/memory_provider.py` and the Hindsight implementation (`plugins/memory/hindsight/`):

```python
class MemoryProvider(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    def is_available(self) -> bool: ...        # no network calls

    async def initialize(self) -> None: ...
    async def shutdown(self) -> None: ...

    # Lifecycle hooks
    async def prefetch(self) -> str | None: ...     # system prompt block
    async def sync_turn(self, messages: list, turn_id: str | None = None) -> None: ...
    async def on_session_end(self) -> None: ...
    async def on_session_switch(
        self, new_session_id: str,
        parent_session_id: str | None = None,
        reset: bool = False,
        rewound: bool = False
    ) -> None: ...

    # Tool surface
    def get_tool_schemas(self) -> list[dict]: ...
    async def handle_tool_call(self, name: str, args: dict) -> str: ...
```

## Implementation Plan

### Step 1: Plugin package structure

Create `plugins/hermes/provider.py` — the MemoryProvider subclass:

```
memory-server/
├── plugins/
│   ├── __init__.py
│   ├── hermes/
│   │   ├── __init__.py
│   │   ├── provider.py          # HermesProvider(MemoryProvider)
│   │   ├── config.py            # Plugin config schema
│   │   └── writer.py            # Writer queue (async batch writer)
│   └── ...
```

Key design decisions:
- **No new DB tables** — reuses existing CMMS storage through the existing service layer
- **Writer queue** — async batch writer for `sync_turn` writes (ref: Hindsight pattern)
- **Mutable config** — `HERMES_CONFIG_PATH` env var or auto-detect via `hermes_home`

### Step 2: HermesProvider implementation

```python
class HermesProvider:
    """Adapter: Hermes MemoryProvider → CMMS internal API."""

    name = "memory_server"

    async def initialize(self):
        # Connect to CMMS storage (same engine, not a new connection)
        self._engine = await create_async_engine(...)
        self._writer = WriterQueue(self._engine)
        # Start background consolidation if configured

    async def prefetch(self) -> str | None:
        """Build system prompt block: recent context, active beliefs, warnings."""
        # Calls existing CMMS get_context() service internally
        context = await self._context_service.get_context(
            task="auto",
            limit=10
        )
        return context.to_system_prompt_block() if context.has_items else None

    async def sync_turn(self, messages, turn_id=None):
        """Observe turn and persist if meaningful."""
        # Writer queue: batch observations, flush periodically or on switch
        await self._writer.add_turn(messages, turn_id)

    async def on_session_switch(self, new_session_id, parent_session_id=None,
                                reset=False, rewound=False):
        """Flush writer queue, clear agent-specific caches."""
        await self._writer.flush()
        self._session_cache.clear()

    async def on_session_end(self):
        """Final flush before session closes."""
        await self._writer.flush()

    def get_tool_schemas(self):
        """Return CMMS MCP tools as native Hermes tool schemas (no mcp_ prefix)."""
        return [
            {
                "name": "remember",
                "description": "Store a fact with provenance",
                "parameters": {...}  # same schema as MCP tool
            },
            # ... all 14 CMMS tools
        ]

    async def handle_tool_call(self, name, args):
        """Route to existing CMMS service, bypassing the MCP transport layer."""
        return await self._tool_router.call(name, args)
```

### Step 3: Writer queue

Async batch writer pattern (ref: Hindsight):

```python
class WriterQueue:
    """Non-blocking batch writer with flush-on-switch."""

    def __init__(self, engine, flush_interval=5.0, max_batch=50):
        self._queue = asyncio.Queue()
        self._flush_interval = flush_interval
        self._max_batch = max_batch
        self._task = None

    async def start(self):
        self._task = asyncio.create_task(self._run())

    async def add_turn(self, messages, turn_id):
        await self._queue.put((messages, turn_id))

    async def flush(self):
        # Drain queue synchronously
        ...

    async def shutdown(self):
        self._task.cancel()
```

### Step 4: Integration test

- Test that HermesProvider can be constructed and initialized
- Test prefetch returns system prompt block (or None when empty)
- Test sync_turn writes to writer queue
- Test on_session_switch flushes the queue
- Test tool schemas match existing MCP tool schemas
- Test handle_tool_call routes correctly

**Do NOT test against a running Hermes instance** — mock the ABC.

## Tool schemas

All 14 existing CMMS tools are exposed as native Hermes tools:

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

Schemas are identical to MCP — only the transport changes (in-process call vs MCP).

## Config schema

```yaml
# In Hermes config.yaml under memory.providers.memory_server:
memory:
  providers:
    memory_server:
      plugin: memory_server.plugins.hermes.provider.HermesProvider
      enabled: true
      path: ~/memory-server  # or auto-discover from installation
      writer:
        flush_interval: 5.0
        max_batch: 50
```

## Acceptance Criteria

1. ✅ `HermesProvider` meets ABC contract (all methods implemented, no no-op stubs)
2. ✅ No new DB tables — 0 ALTER TABLE / CREATE TABLE
3. ✅ `prefetch()` returns valid system prompt block or None (never crashes)
4. ✅ `sync_turn()` adds to writer queue without blocking
5. ✅ `on_session_switch()` flushes writer queue
6. ✅ All 14 CMMS tools exposed as native schemas (verified by diff against MCP schemas)
7. ✅ Writer queue flushes automatically every `flush_interval` seconds
8. ✅ Integration tests pass (mock Hermes ABC, real CMMS services)

## Non-goals (explicitly deferred)

- **Auto-discovery**: CLI registration (`--install-hermes-plugin`) → Card 002
- **Dual provider mode**: Both CMMS (MemoryProvider) and built-in Hermes memory → not needed (MemoryManager allows exactly one external provider)
- **Profile isolation**: Handled by Hermes runtime (passes `hermes_home`) — CMMS just respects it
- **Production profiles/scripts**: `install.sh` or pip → v0.9
