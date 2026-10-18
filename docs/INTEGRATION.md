# Hermes Integration Guide

> **Latest public GitHub release:** `v0.11.0b1` — optional `[hermes]` Native
> MemoryProvider Plugin. Hermes v0.19 provider discovery compatibility is
> documented in the changelog.
> **This branch:** `refactor/runtime-architecture` implements the
> transport-independent runtime ownership below. That work is implemented and
> independently verified, but **not merged and not released**. Package metadata
> on this branch is `0.12.0`; it is not a published GitHub release.
> **ADR:** [ADR-013](ADR.md#adr-013-hermes-memoryprovider-integration--dual-access-path),
> [ADR-017](ADR.md#adr-017-transport-independent-runtime-ownership)

## Overview

CMMS (Composite Memory MCP Server) can integrate with Hermes in **two ways**.
Hermes is not a base runtime dependency of the MCP stdio server; install the
optional `[hermes]` extra only when using the native provider path:

| Path | Transport | Lifecycle Hooks | Auto-Recall | Auto-Retain | Tools |
|------|-----------|-----------------|-------------|-------------|-------|
| **MCP** (external agents) | stdio MCP | ❌ No | ❌ No | ❌ No | `mcp_*` prefixed |
| **Native MemoryProvider** (v0.8+) | In-process | ✅ Yes | ✅ Yes | ✅ Yes | First-class tools |

The **Native MemoryProvider** path exposes Hermes lifecycle features that MCP
stdio does not provide — it enables:

- **Auto-recall** — Hermes calls `prefetch()` before every turn to inject relevant context
- **Auto-retain** — Hermes calls `sync_turn()` after every turn to persist observations
- **Session lifecycle hooks** — `on_session_end()` / `on_session_switch()` flush the writer queue
- **First-class tools** — All 14 CMMS tools are available without the `mcp_` prefix
- **No MCP transport overhead** — Tool calls are in-process, not over HTTP/stdio

## Quick Start

### Prerequisites

- Hermes installed and configured; v0.19 provider discovery is supported by the
  current plugin shim
- Python 3.11+ available in the environment used by the Hermes Gateway
- CMMS installed in that same environment with the `[hermes]`, `[sentence]`,
  and active vector-backend extra
- A valid `~/.hermes/config.yaml`

The native provider is imported *inside the Gateway process*. Installing
dependencies only into the repository's development `.venv` does not help if
the Gateway runs under a different Python interpreter. Run the install command
from the environment that launches the Gateway and check its interpreter with
`python -c 'import sys; print(sys.executable)'`.

For the default local setup (CPU embeddings + LanceDB), from the CMMS repository
root run:

```bash
python -m pip install --index-url https://download.pytorch.org/whl/cpu \
  --extra-index-url https://pypi.org/simple \
  -e ".[hermes,sentence,lancedb]"
```

This installs PyTorch's CPU wheel rather than CUDA libraries. If the configured
backend is Qdrant, use `.[hermes,sentence,qdrant]` instead. For GPU embeddings,
replace the CPU PyTorch index with the matching index from the [official PyTorch
selector](https://pytorch.org/get-started/locally/).

### 1. Install the Plugin

```bash
memory-server install-hermes-plugin
```

This command:
1. Detects Hermes home (`$HERMES_HOME` or `~/.hermes`)
2. Adds a `memory.providers.memory_server` entry to `config.yaml`
3. Sets `memory.provider` to `memory_server`
4. Backs up the previous provider to `.memory-provider-backup`

### 2. Restart the Gateway

```bash
hermes gateway restart
```

### 3. Verify

```bash
hermes memory status
# → Provider: memory_server (Composite Memory MCP Server)
```

Or check the Hermes logs:

```bash
hermes logs --tail 20
# → INFO  HermesProvider initialized (session=..., db=...)
```

Verify the embedding and default vector-backend imports with the same Python
interpreter:

```bash
python -c 'import sentence_transformers, torch, lancedb; print("CMMS semantic dependencies available")'
```

The first embedding call downloads the configured model (default:
`all-MiniLM-L6-v2`) if it is not cached. Existing outbox rows marked `failed`
are terminal after their retry limit; installing dependencies fixes future
indexing but does not automatically replay those historical failures.

## What Changes

### Before → After

| Before | After |
|--------|-------|
| CMMS only accessible via MCP | CMMS is the system **memory provider** |
| Tool calls go through `mcp_*` transport | Tool calls are **in-process** (no HTTP) |
| No auto-recall before agent turns | `prefetch()` injects context **automatically** |
| No auto-retain between turns | `sync_turn()` persists observations |
| Session switches ignored | `on_session_switch()` flushes writer queue |

### Config Changes in `~/.hermes/config.yaml`

The install command adds this entry (using `ruamel.yaml` to preserve all existing comments):

```yaml
memory:
  provider: memory_server               # ← switched from "builtin"
  providers:
    memory_server:                      # ← new provider entry
      plugin: memory_server.plugins.hermes.provider.HermesProvider
      enabled: true
      path: /home/user/memory-server    # ← absolute CMMS path
      writer:
        flush_interval: 5.0
        max_batch: 50
```

### Context injection tuning

The number of facts/decisions CMMS injects into the agent's system prompt
before each turn is configurable. Default is **5**; set it via env var (takes
precedence) or the provider config block:

```bash
# ~/.hermes/.env  — global, applies to ALL profiles (single shared CMMS)
MEMORY_SERVER_MAX_FACTS=10
```

```yaml
# ~/.hermes/config.yaml — per-profile fallback (env wins if both set)
memory:
  providers:
    memory_server:
      max_facts: 10
```

### Backup

The previous `memory.provider` value is saved to `~/.hermes/.memory-provider-backup`
for automatic rollback on uninstall.

## CLI Reference

### `memory-server install-hermes-plugin`

```bash
# Basic install (auto-detect Hermes home)
memory-server install-hermes-plugin

# Dry-run: show what would be written
memory-server install-hermes-plugin --dry-run

# Custom Hermes home (non-default profile)
memory-server install-hermes-plugin --hermes-home ~/.hermes/profiles/coder

# Uninstall
memory-server install-hermes-plugin --uninstall

# Uninstall dry-run
memory-server install-hermes-plugin --uninstall --dry-run
```

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--hermes-home` | Path | `$HERMES_HOME` or `~/.hermes` | Custom Hermes config directory |
| `--dry-run` | Flag | `False` | Show changes without writing |
| `--uninstall` | Flag | `False` | Remove plugin configuration |

#### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | Error (config not found, write failure) |

## How It Works

### Architecture

MCP stdio and the optional Hermes native provider are **separate adapters**.
Each composition reaches CMMS through `RuntimeFactory` (Hermes also uses
`RuntimeManager`). `CMMSRuntime` owns lifecycle, the storage lock, and
resources. The two adapters do **not** share one runtime object; running both
against the same profile root is still one-writer-per-root because of that lock.

```
External Agents                 Hermes Agent
     |                               |
     MCP stdio                  MemoryProvider ABC
     |                          (in-process; not a network transport)
     v                               v
transports/mcp_stdio.py         plugins/hermes/provider.py
MCPComposition                  HermesRuntimeBridge
     |                               |
     RuntimeFactory            RuntimeManager + RuntimeFactory
     |                               |
     +--------- CMMSRuntime ---------+
                    |
         runtime-owned resources
         SQLite (canonical)
         vector / graph (secondary projections)
         OutboxWorker (borrows the SQLite engine; no second worker)
```

`initialize()` does not start `SQLiteProvider` or `WriterQueue` itself. It
resolves a frozen storage layout, then `HermesRuntimeBridge.start()` admits a
runtime through `RuntimeManager` / `RuntimeFactory`. Runtime recipes construct
SQLite, graph, embedder, vector, and the optional outbox worker. After the
runtime is ready, the bridge starts `WriterQueue` and binds it to runtime
shutdown hooks. The provider then uses those runtime-owned handles.

Native extraction is forwarded with the runtime's `submit_native`. Prefetch and
the outbox parent task are runtime-owned (`spawn_owned_task`). The provider does
not create a second outbox worker or lock owner.

MCP stdio follows the same ownership model via `MCPComposition` +
`RuntimeFactory` and wires `MemoryApplication` over the runtime dependencies.
Public MCP tool contracts and config/hooks are unchanged. There is no remote
HTTP/SSE transport.

### Lifecycle Flow

```
Session Start
     │
     ▼
  initialize(session_id, **kwargs)
  ├─ Resolve frozen storage layout
  ├─ HermesRuntimeBridge → RuntimeManager → RuntimeFactory → CMMSRuntime.start()
  ├─ Runtime recipes open SQLite / projections / optional outbox worker
  └─ Bridge starts WriterQueue after the runtime is READY
     │
     ▼  (per turn)
  prefetch(query)
  ├─ Returns cached context from previous queue_prefetch()
  └─ Non-blocking: queued as a runtime-owned task
     │
     ▼  (after model response)
  sync_turn(user_content, assistant_content)
  ├─ Queues turn for async persistence
  └─ Triggers queue_prefetch() for NEXT turn
     │
     ▼  (session boundary)
  on_session_switch(new_session_id)
  ├─ Flushes writer queue
  └─ Updates session cache
     │
     ▼  (session end)
  on_session_end(messages)
  └─ Flushes writer queue
     │
     ▼
  shutdown()
  └─ bridge.close() → runtime.close()
     ├─ Stop admission; pause writer; stop outbox; drain producers
     ├─ Wait for owned tasks and live native work (native work is not
     │  cancelled to justify releasing the root lock)
     ├─ Settle writer custody, then final flush
     └─ If work is still live or custody did not settle: fail closed and
        retain the storage lock / unclosed resources. Otherwise close
        owned stores and release the lock.
```

### Contract Mapping

| Hermes Method | CMMS Service | Description |
|--------------|--------------|-------------|
| `initialize(session_id, **kwargs)` | Runtime bridge + factory | kwargs: `hermes_home`, `platform`, `agent_context`. Starts `CMMSRuntime`; does not open SQLite/WriterQueue directly. |
| `system_prompt_block()` | Static provider info | Memory usage tips |
| `prefetch(query)` | `get_context()` | Fast cached recall |
| `queue_prefetch(query)` | Queue next-turn context load | Runtime-owned background task |
| `sync_turn(user, asst, *, messages)` | `learn()` via writer queue | Async batch; native extraction uses `submit_native` |
| `get_tool_schemas()` | All 14 CMMS tools | No `mcp_` prefix |
| `handle_tool_call(name, args, **kwargs)` | Route to service layer | In-process |
| `on_session_end(messages)` | Flush writer queue | Cleanup |
| `on_session_switch(new_id, *, ...)` | Flush + update cache | Rotation |
| `shutdown()` | `runtime.close()` | Stop admission, settle/flush custody, fail closed and retain the lock if work is still live |

### Writer Queue

The `WriterQueue` (in `plugins/hermes/writer.py`) implements the **non-blocking write pattern**:

1. `sync_turn()` data is added to an `asyncio.Queue`
2. A background task flushes every `flush_interval` seconds (default: 5.0)
3. Explicit `flush()` calls drain the queue immediately (session hooks)
4. Batches are limited to `max_batch` items (default: 50)
5. Failed writes are counted (not retried — logged for observability)

The queue is started by `HermesRuntimeBridge` after `CMMSRuntime` is ready, not
by `HermesProvider.initialize()` directly. On shutdown the runtime stops
admission, pauses the flusher, settles queue custody
(`WriterQueue.settle_custody()` / `QueueCustodySnapshot`), then runs a final
flush. Those types are implementation details of custody tracking; public Hermes
hooks and writer config are unchanged.

## Tools

All 14 CMMS tools are available as native Hermes tools via the MemoryProvider:

| # | Tool | Description |
|---|------|-------------|
| 1 | `ping` | Health check |
| 2 | `search` | Keyword search over facts |
| 3 | `remember` | Store a fact with provenance |
| 4 | `get_context` | Retrieve context for a task |
| 5 | `semantic_search` | Vector similarity search |
| 6 | `learn` | Extract knowledge (facts, decisions, skills, beliefs) |
| 7 | `graph_search` | Entity lookup + pathfinding |
| 8 | `route` | 4-stage hybrid router (rules → embeddings → graph → LLM) |
| 9 | `audit` | Memory health report |
| 10 | `metrics` | Prometheus metrics |
| 11 | `set_belief` | Create, reinforce, or supersede a belief |
| 12 | `get_belief` | Search beliefs with filters |
| 13 | `resolve_conflict` | Resolve belief conflicts |
| 14 | `reflect` | 6-mode belief store analysis |

## Profile Isolation

Each Hermes profile has its own `config.yaml`. To install CMMS for a specific profile:

```bash
memory-server install-hermes-plugin --hermes-home ~/.hermes/profiles/coder
```

The plugin resolves the database path relative to the profile's `hermes_home` directory,
so each profile has isolated storage:

```
~/.hermes/
├── config.yaml                     # default profile
└── profiles/
    └── coder/
        ├── config.yaml             # coder profile
        └── data/
            └── memory.db           # coder's isolated DB
```

## Troubleshooting

### Plugin Doesn't Appear After Install

**Check the config:**

```bash
# Verify provider entry exists
grep -A5 "memory_server" ~/.hermes/config.yaml
```

**Check Hermes logs:**

```bash
hermes logs --tail 30 | grep -i "hermesprovider"
```

**Verify the provider path resolves correctly:**

```bash
python -c "from memory_server.plugins.hermes.provider import HermesProvider; print('OK')"
```

### Rollback Manually

If you need to roll back without using `--uninstall`:

```bash
# 1. Manually restore config.yaml — edit and change:
#    memory.provider → back to previous value (e.g. "builtin")
# 2. Remove the memory.providers.memory_server section
# 3. Restart the gateway:
hermes gateway restart
```

Or use the backup file:

```bash
# Read the old provider
cat ~/.hermes/.memory-provider-backup

# Then uninstall (which restores from backup automatically):
memory-server install-hermes-plugin --uninstall
```

### Config Syntax Error After Install

If `hermes gateway restart` fails with a YAML error:

1. Your config.yaml likely has a pre-existing issue that ruamel.yaml preserved
2. Check syntax: `python -c "import yaml; yaml.safe_load(open('~/.hermes/config.yaml'))"`
3. Alternatively, use `--uninstall` to revert and then re-inspect the config

### Provider Not Initialized

If `hermes memory status` shows an error:

```bash
# Check if the Python import works (path resolution issue)
python -c "import memory_server; print(memory_server.__version__)"

# Verify SQLite provider can initialize (DB path issue)
python -c "from memory_server.providers.sqlite_provider import SQLiteProvider; print('OK')"
```

## Profile-owned storage

Native Hermes providers default to `storage_mode: profile` and resolve SQLite,
LanceDB, graph snapshots, and locks below the active profile home. The
installation `path` is import-only. Intentional sharing requires both
`storage_mode: shared` and an absolute `data_root`; standalone MCP keeps its
working-directory defaults unless `MEMORY_SERVER_DATA_ROOT` is supplied.
Existing escaping projection symlinks are never followed: startup is
degraded-safe and vector/graph recall remains unavailable until migration.

### Migration and recovery operator procedure

Inventory source, target, graph, legacy, and symlink artifacts with `lstat`/no-follow
enumeration; do not follow symlink entries. SQLite is the source of truth and
`rebuild-from-profile-sql` rebuilds projections rather than partitioning or
importing mixed legacy stores. Legacy projections are `preserve-only; not imported`.

Before apply, run doctor and the default dry-run against a synthetic or explicitly
approved operator home. Review source/target identities, schema and integrity
(or `unknown`), `lock_availability: unknown`, disk margin, embedding backend and
estimated duration/records/batches. The dry-run does not call the embedder, use
network, create locks, write configuration, or create a manifest. Remote
embedding requires explicit network/API-cost authorization and the exact resume
plan digest; disclose backend availability, duration, network, quota, and API
cost before approval.

If any WAL/SHM/journal entry is present or ambiguous, BLOCK apply until a normal
external shutdown makes the database clean. Never manually checkpoint. Never
delete WAL/SHM. Never copy WAL/SHM. Stop every mixed-version runtime and
independently verify it stopped before recording the bounded attestation. The
migration command never stops or restarts services.

Apply backs up/quarantines existing target entries and preserves source and
legacy identities. Replacement and rollback are no-write-through operations:
final symlink entries are restored as link entries without touching their
referents. On failure retain the manifest, backup, staging, and quarantine;
resume only with a fresh attestation and the exact embedding resume digest.

After exit 0 inspect `complete`, hashes, counts, and projection verification.
The operator edits YAML separately only after verified data. Start one profile,
verify the structured `cmms.storage_layout` event and projection health, then
roll out the remaining profiles. Root-lock-only protection is complete only
after full rollout.
