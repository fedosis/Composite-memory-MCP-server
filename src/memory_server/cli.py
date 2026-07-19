"""CLI entry points for Composite Memory MCP Server (CMMS).

Commands:
  serve                      Start the MCP server (stdio transport)
  install-hermes-plugin      Register CMMS as a Hermes MemoryProvider
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

import typer

from .server import run as run_server

app = typer.Typer()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VALID_CONFIG_SECTION = """\
    {
        "plugin": "memory_server.plugins.hermes.provider.HermesProvider",
        "enabled": true,
        "path": "$CMMS_PATH",
        "writer": {"flush_interval": 5.0, "max_batch": 50}
    }"""

BACKUP_FILE = ".memory-provider-backup"


def _find_hermes_home(provided: str | None) -> str:
    """Resolve Hermes home directory.

    Priority: 1) --hermes-home arg  2) $HERMES_HOME  3) ~/.hermes
    """
    if provided:
        return provided
    env_home = os.environ.get("HERMES_HOME")
    if env_home:
        return env_home
    return str(Path.home() / ".hermes")


def _config_path(hermes_home: str) -> Path:
    return Path(hermes_home) / "config.yaml"


def _backup_path(hermes_home: str) -> Path:
    return Path(hermes_home) / BACKUP_FILE


def _abs_cmms_path() -> str:
    """Return the absolute path to the CMMS package directory."""
    # Resolve from the package location (src/memory_server/cli.py -> project root)
    cli_file = Path(__file__).resolve()
    # Navigate up: cli.py -> memory_server -> src -> project root
    return str(cli_file.parent.parent.parent)


def _current_memory_provider(yaml_data) -> str | None:
    """Read the current ``memory.provider`` value, if any."""
    memory = yaml_data.get("memory")
    if isinstance(memory, dict):
        return memory.get("provider")
    return None


def _set_memory_provider(yaml_data, provider: str) -> str | None:
    """Set ``memory.provider`` and return the old value (if any)."""
    old = _current_memory_provider(yaml_data)
    memory = yaml_data.setdefault("memory", {})
    memory["provider"] = provider
    return old


def _add_provider_entry(yaml_data, cmms_path: str) -> bool:
    """Add ``memory.providers.memory_server`` entry.  Returns True if added."""
    memory = yaml_data.setdefault("memory", {})
    providers = memory.setdefault("providers", {})

    if "memory_server" in providers:
        return False  # already registered

    providers["memory_server"] = {
        "plugin": "memory_server.plugins.hermes.provider.HermesProvider",
        "enabled": True,
        "path": cmms_path,
        "writer": {"flush_interval": 5.0, "max_batch": 50},
    }
    return True


def _remove_provider_entry(yaml_data) -> bool:
    """Remove ``memory.providers.memory_server`` entry.  Returns True if removed."""
    memory = yaml_data.get("memory")
    if not isinstance(memory, dict):
        return False
    providers = memory.get("providers")
    if not isinstance(providers, dict):
        return False
    if "memory_server" not in providers:
        return False
    del providers["memory_server"]
    return True


def _save_config(config_path: Path, yaml_data) -> None:
    """Write the YAML tree back to disk using ruamel.yaml."""
    from ruamel.yaml import YAML

    yaml = YAML()
    yaml.preserve_quotes = True
    with open(config_path, "w") as f:
        yaml.dump(yaml_data, f)


def _load_config(config_path: Path):
    """Load a YAML file with ruamel.yaml, preserving comments and formatting."""
    from ruamel.yaml import YAML

    yaml = YAML()
    yaml.preserve_quotes = True
    if config_path.is_file():
        with open(config_path) as f:
            return yaml.load(f)
    return {}


# ---------------------------------------------------------------------------
# Install / uninstall logic
# ---------------------------------------------------------------------------


def _do_install(
    hermes_home: str,
    dry_run: bool,
    *,
    out,
) -> int:
    """Perform the install (or dry-run preview).  Returns 0 on success."""
    cmms_path = _abs_cmms_path()
    cfg_path = _config_path(hermes_home)
    back_path = _backup_path(hermes_home)

    if not cfg_path.is_file():
        out(f"❌ config.yaml not found: {cfg_path}")
        out(f"   Is {hermes_home} a valid Hermes home directory?")
        return 1

    data = _load_config(cfg_path)

    # --- backup current memory.provider ---
    old_provider = _current_memory_provider(data)

    # --- add provider entry ---
    added = _add_provider_entry(data, cmms_path=cmms_path)

    # --- switch active provider ---
    previous = _set_memory_provider(data, "memory_server")

    if not added:
        out("ℹ️  memory_server provider already registered — updating path")
        memory = data.setdefault("memory", {})
        providers = memory.setdefault("providers", {})
        if "memory_server" in providers:
            providers["memory_server"]["path"] = cmms_path

    if dry_run:
        out(f"🔍 Dry-run — would write to: {cfg_path}")
        out(f"   CMMS path: {cmms_path}")
        out(f"   memory.provider: {previous!r} → 'memory_server'")
        if previous is not None:
            out(f"   Backup old provider to: {back_path}")
        out("\nTarget config.yaml changes:")
        # Dump the modified YAML so the user can review
        from ruamel.yaml import YAML

        y = YAML()
        y.preserve_quotes = True
        y.dump(data, sys.stdout)
        return 0

    # --- backup old provider ---
    if previous is not None:
        try:
            back_path.write_text(previous + "\n")
        except OSError as exc:
            out(f"⚠️  Could not write backup file {back_path}: {exc}")
            out("   Continuing anyway...")

    # --- write ---
    try:
        _save_config(cfg_path, data)
    except OSError as exc:
        out(f"❌ Failed to write {cfg_path}: {exc}")
        return 1

    out(f"✅ CMMS registered as Hermes MemoryProvider in {cfg_path}")
    out(f"   Plugin path: memory_server.plugins.hermes.provider.HermesProvider")
    out(f"   memory.provider: {previous!r} → 'memory_server'")
    if previous is not None:
        out(f"   Backup saved to: {back_path}")
    out("")
    out("👉 Restart Hermes gateway to activate:")
    out("   hermes gateway restart")
    return 0


def _do_uninstall(
    hermes_home: str,
    dry_run: bool,
    *,
    out,
) -> int:
    """Perform the uninstall (or dry-run preview).  Returns 0 on success."""
    cfg_path = _config_path(hermes_home)
    back_path = _backup_path(hermes_home)

    if not cfg_path.is_file():
        out(f"❌ config.yaml not found: {cfg_path}")
        return 1

    data = _load_config(cfg_path)

    removed = _remove_provider_entry(data)
    current_provider = _current_memory_provider(data)

    # Try to restore previous provider from backup
    restored_provider = None
    if back_path.is_file():
        restored_provider = back_path.read_text().strip()

    if not removed:
        out("ℹ️  memory_server provider was not registered — nothing to remove")
        return 0

    if dry_run:
        out(f"🔍 Dry-run — would write to: {cfg_path}")
        if restored_provider:
            out(f"   Restore memory.provider: {current_provider!r} → {restored_provider!r}")
        out(f"   Remove backup file: {back_path}")
        out("\nTarget config.yaml changes:")
        from ruamel.yaml import YAML

        y = YAML()
        y.preserve_quotes = True
        # If we would restore, set the provider
        if restored_provider and current_provider == "memory_server":
            data["memory"]["provider"] = restored_provider
        y.dump(data, sys.stdout)
        return 0

    # Restore previous provider if current is memory_server
    if restored_provider and current_provider == "memory_server":
        memory = data.setdefault("memory", {})
        memory["provider"] = restored_provider
        out(f"   memory.provider restored: {current_provider!r} → {restored_provider!r}")

    # Write
    try:
        _save_config(cfg_path, data)
    except OSError as exc:
        out(f"❌ Failed to write {cfg_path}: {exc}")
        return 1

    # Remove backup file
    try:
        back_path.unlink(missing_ok=True)
    except OSError:
        pass

    out(f"✅ CMMS MemoryProvider removed from {cfg_path}")
    out("")
    out("👉 Restart Hermes gateway to apply changes:")
    out("   hermes gateway restart")
    return 0


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    if ctx.invoked_subcommand is None:
        run_server()


@app.command()
def serve():
    """Start the MCP server (stdio transport)"""
    run_server()


@app.command()
def install_hermes_plugin(
    hermes_home: Optional[str] = typer.Option(
        None,
        "--hermes-home",
        help="Hermes config directory (default: $HERMES_HOME or ~/.hermes)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show changes without writing",
    ),
    uninstall: bool = typer.Option(
        False,
        "--uninstall",
        help="Remove plugin configuration",
    ),
):
    """Register CMMS as a native Hermes MemoryProvider plugin.

    Adds the memory_server provider entry to Hermes config.yaml and sets
    it as the active memory provider. Uses ruamel.yaml to preserve all
    existing comments and formatting.

    Examples:

        memory-server install-hermes-plugin

        memory-server install-hermes-plugin --dry-run

        memory-server install-hermes-plugin --hermes-home ~/.hermes/profiles/coder

        memory-server install-hermes-plugin --uninstall
    """
    resolved = _find_hermes_home(hermes_home)

    if uninstall:
        sys.exit(_do_uninstall(resolved, dry_run, out=typer.echo))
    else:
        sys.exit(_do_install(resolved, dry_run, out=typer.echo))


if __name__ == "__main__":
    app()
