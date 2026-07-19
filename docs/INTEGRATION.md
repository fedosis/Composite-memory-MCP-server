# Hermes Integration Guide

> **Version:** v0.8 — Native MemoryProvider Plugin
> **Status:** ✅ Production-ready
> **ADR:** [ADR-013](ADR.md#adr-013-hermes-memoryprovider-integration--dual-access-path)

## Overview

CMMS (Composite Memory MCP Server) can integrate with Hermes in **two ways**:

| Path | Transport | Lifecycle Hooks | Auto-Recall | Auto-Retain | Tools |
|------|-----------|-----------------|-------------|-------------|-------|
| **MCP** (external agents) | stdio MCP | ❌ No | ❌ No | ❌ No | `mcp_*` prefixed |
| **Native MemoryProvider** (v0.8+) | In-process | ✅ Yes | ✅ Yes | ✅ Yes | First-class tools |

The **Native MemoryProvider** path is **strictly more capable** — it enables:

- **Auto-recall** — Hermes calls `prefetch()` before every turn to inject relevant context
- **Auto-retain** — Hermes calls `sync_turn()` after every turn to persist observations
- **Session lifecycle hooks** — `on_session_end()` / `on_session_switch()` flush the writer queue
- **First-class tools** — All 14 CMMS tools are available without the `mcp_` prefix
- **No MCP transport overhead** — Tool calls are in-process, not over HTTP/stdio

## Quick Start

### Prerequisites

- Hermes **v0.8+** installed and configured
- CMMS installed (`pip install -e ".[hermes]"`)
- A valid `~/.hermes/config.yaml`

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

```
External Agents              Hermes Agent
     |                            |
     MCP                     MemoryProvider
     |                            |
     v                            v
server.py                  plugins/hermes/provider.py
     |                            |
     +------ CMMS Service Layer --+
                    |
           Storage (SQLite/Qdrant/Graph)
```

### Lifecycle Flow

```
Session Start
     │
     ▼
  initialize(session_id, **kwargs)
  ├─ Start SQLiteProvider
  └─ Start WriterQueue (background flusher)
     │
     ▼  (per turn)
  prefetch(query)
  ├─ Returns cached context from previous queue_prefetch()
  └─ Non-blocking: queued → loaded asynchronously
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
  ├─ Flush + stop writer
  └─ Close SQLite provider
```

### Contract Mapping

| Hermes Method | CMMS Service | Description |
|--------------|--------------|-------------|
| `initialize(session_id, **kwargs)` | Connect + start writer queue | kwargs: `hermes_home`, `platform`, `agent_context` |
| `system_prompt_block()` | Static provider info | Memory usage tips |
| `prefetch(query)` | `get_context()` | Fast cached recall |
| `queue_prefetch(query)` | Queue next-turn context load | Background, non-blocking |
| `sync_turn(user, asst, *, messages)` | `learn()` via writer queue | Async batch |
| `get_tool_schemas()` | All 14 CMMS tools | No `mcp_` prefix |
| `handle_tool_call(name, args, **kwargs)` | Route to service layer | In-process |
| `on_session_end(messages)` | Flush writer queue | Cleanup |
| `on_session_switch(new_id, *, ...)` | Flush + update cache | Rotation |
| `shutdown()` | Flush, stop, close | Clean exit |

### Writer Queue

The `WriterQueue` (in `plugins/hermes/writer.py`) implements the **non-blocking write pattern**:

1. `sync_turn()` data is added to an `asyncio.Queue`
2. A background task flushes every `flush_interval` seconds (default: 5.0)
3. Explicit `flush()` calls drain the queue immediately (session hooks)
4. Batches are limited to `max_batch` items (default: 50)
5. Failed writes are counted (not retried — logged for observability)

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
