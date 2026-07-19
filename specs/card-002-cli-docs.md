# Card 002: CLI + Auto-Discovery + Docs

## Objective

Add CLI command `memory-server install-hermes-plugin` that registers CMMS as a Hermes MemoryProvider in `config.yaml`, and provide full documentation for the v0.8 integration.

## Motivation

Card 001 delivers the `HermesProvider` class. But it doesn't install itself — without Card 002, the user has to manually:
1. Edit `~/.hermes/config.yaml` to add the plugin path
2. Know the exact class path and config keys
3. Restart the gateway and verify it works

Card 002 makes this a one-command setup.

## CLI Command

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
```

### What it does

1. **Detect Hermes home**: Check `$HERMES_HOME`, fall back to `~/.hermes`
2. **Read existing config.yaml**: Parse current config
3. **Add plugin entry** under `memory.providers.memory_server`:
   ```yaml
   memory:
     providers:
       memory_server:
         plugin: memory_server.plugins.hermes.provider.HermesProvider
         enabled: true
         path: /absolute/path/to/memory-server
         writer:
           flush_interval: 5.0
           max_batch: 50
   ```
4. **Set CMMS as active provider**: Update `memory.provider` to `memory_server`
   (backup existing value)
5. **Print confirmation**: Success or diff
6. **`--dry-run`**: Print what would change without writing
7. **`--uninstall`**: Remove plugin entry, restore previous memory provider

The command uses `ruamel.yaml` to preserve comments and formatting in config.yaml (not PyYAML which destroys them).

### Implementation

```python
# In memory_server/cli.py, extend the typer app:

@app.command()
def install_hermes_plugin(
    hermes_home: str = typer.Option(
        None, "--hermes-home",
        help="Hermes config directory (default: $HERMES_HOME or ~/.hermes)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Show changes without writing"
    ),
    uninstall: bool = typer.Option(
        False, "--uninstall",
        help="Remove plugin configuration"
    ),
):
    """Register CMMS as a native Hermes MemoryProvider plugin."""
    ...
```

## Config Changes

Two mutations to `~/.hermes/config.yaml`:

### 1. Add provider definition

```yaml
memory:
  providers:
    memory_server:          # <-- new entry
      plugin: memory_server.plugins.hermes.provider.HermesProvider
      enabled: true
      path: /home/shtorm/memory-server
      writer:
        flush_interval: 5.0
        max_batch: 50
  # existing providers remain unchanged
```

### 2. Switch active provider

```yaml
memory:
  provider: memory_server    # was: "builtin" or "hindsight"
```

The old value is backed up to `~/.hermes/.memory-provider-backup` for rollback.

## Documentation

### `docs/INTEGRATION.md` — Hermes Integration Guide

Sections:
1. **Overview** — what the native MemoryProvider does vs MCP-only
2. **Quick Start** — `memory-server install-hermes-plugin` → restart gateway
3. **What changes** — config entries, lifecycle hooks
4. **How it works** — prefetch / sync_turn / session hooks (with diagrams)
5. **Tools** — all 14 CMMS tools as native Hermes tools
6. **Troubleshooting** — logs, status check, rollback

### `docs/INTEGRATION.md` example:

````markdown
# Hermes Integration Guide

## Quick Start

```bash
# 1. Install the plugin
memory-server install-hermes-plugin

# 2. Restart Hermes gateway
hermes gateway restart

# 3. Verify
hermes memory status
# → Provider: memory_server (Composite Memory MCP Server)
```

## What Changes

| Before | After |
|--------|-------|
| CMMS only accessible via MCP | CMMS is the system memory provider |
| Tool calls go through MCP transport | Tool calls are in-process (no HTTP) |
| No auto-recall before agent turns | `prefetch()` injects context automatically |
| No auto-retain between turns | `sync_turn()` persists observations |
| Session switches ignored | `on_session_switch()` flushes writer queue |

## Tools

All 14 CMMS tools are available as native Hermes tools.
````

### `README.md` update

Add "Hermes Integration" section with badge:

```markdown
## Hermes Integration

CMMS can run as a native Hermes MemoryProvider plugin — not just over MCP.
This enables auto-recall, auto-retain, and session lifecycle hooks.

```bash
pip install -e ".[hermes]"
memory-server install-hermes-plugin
hermes gateway restart
```

See [Integration Guide](docs/INTEGRATION.md) for details.
```

## Acceptance Criteria

1. ✅ `memory-server install-hermes-plugin` reads and modifies `config.yaml` correctly
2. ✅ `--dry-run` prints diff without writing
3. ✅ `--uninstall` removes plugin config and restores previous provider
4. ✅ Backward compatible: MCP server still works after plugin install
5. ✅ `docs/INTEGRATION.md` covers quick start, config, and troubleshooting
6. ✅ `README.md` has Hermes Integration section
7. ✅ Full round-trip test: install → restart → verify → uninstall → verify rollback

## Non-goals

- **pip package extras**: `pip install -e ".[hermes]"` is a nice-to-have, not required
- **Plugin SDK bundling**: CMMS does not vendor Hermes source — plugin path is a string reference
- **GUI installer**: CLI only
