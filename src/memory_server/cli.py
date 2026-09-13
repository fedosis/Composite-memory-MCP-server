"""CLI entry points for Composite Memory MCP Server (CMMS).

Commands:
  serve                      Start the MCP server (stdio transport)
  install-hermes-plugin      Register CMMS as a Hermes MemoryProvider
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Optional, cast
from uuid import uuid4

import typer

from memory_server.paths import (
    StorageLayoutError,
    classify_artifact_nofollow,
    cmms_repo_root,
)
from memory_server.profile_migration import (
    DIAGNOSTIC_CONTRACT,
    Diagnostic,
    MigrationPlan,
    MigrationRequest,
    MigrationStrategy,
    apply_profile_migration,
    exit_code_for_diagnostic,
    plan_profile_migration,
    resume_profile_migration,
    rollback_profile_migration,
)

# Lazy import: server.py imports ``storage`` which may not be installed.
# Only load it when the ``serve`` subcommand is actually invoked.
_run_server = None


def _get_run_server():
    global _run_server
    if _run_server is None:
        from .server import run  # type: ignore[import-untyped]

        _run_server = run
    return _run_server


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

# Explicit marker written to the backup file when install finds NO previous
# ``memory.provider`` key. uninstall maps it back to "remove the key" so the
# pre-install state (absence) is restored exactly (CORE-1/2).
ABSENT_PROVIDER_MARKER = "<absent>"


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


def _is_first_switch(previous: str | None) -> bool:
    """True when install is actually switching AWAY from a different provider
    (or recording that there was no provider key at all).

    False when ``memory.provider`` is already ``memory_server`` — that is a
    re-install, and it must not overwrite the original backup (CORE-1).
    """
    if previous is None:
        return True  # no key yet — absence must be recorded explicitly
    return previous != "memory_server"


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
    cmms_path = str(cmms_repo_root())
    cfg_path = _config_path(hermes_home)
    back_path = _backup_path(hermes_home)

    if not cfg_path.is_file():
        out(f"❌ config.yaml not found: {cfg_path}")
        out(f"   Is {hermes_home} a valid Hermes home directory?")
        return 1

    data = _load_config(cfg_path)

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
        if _is_first_switch(previous):
            if previous is None:
                out(f"   Backup absence marker to: {back_path}")
            else:
                out(f"   Backup old provider to: {back_path}")
        out("\nTarget config.yaml changes:")
        # Dump the modified YAML so the user can review
        from ruamel.yaml import YAML

        y = YAML()
        y.preserve_quotes = True
        y.dump(data, sys.stdout)
        return 0

    # --- backup old provider ---
    # CORE-1/2: backup ONLY on the FIRST switch (previous provider differs
    # from 'memory_server', including the no-key case, which records an
    # explicit absence marker). A re-install over an already-active
    # memory_server provider must NEVER overwrite the backup, otherwise the
    # second install would clobber the original provider and uninstall would
    # restore the wrong value.
    if _is_first_switch(previous):
        backup_value = previous if previous is not None else ABSENT_PROVIDER_MARKER
        try:
            back_path.write_text(backup_value + "\n")
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
    out(f"   Plugin path: memory_server.plugins.hermes.provider.HermesProvider")  # noqa: F541
    out(f"   memory.provider: {previous!r} → 'memory_server'")
    if _is_first_switch(previous):
        if previous is None:
            out(f"   Backup (absence marker) saved to: {back_path}")
        else:
            out(f"   Backup saved to: {back_path}")
    else:
        out(f"   Provider already active — existing backup kept: {back_path}")
    out("")
    out("👉 Restart Hermes gateway to activate:")
    out("   hermes gateway restart")
    return 0


def _restore_previous_provider(
    yaml_data,
    *,
    current_provider: str | None,
    restored_provider: str | None,
    restore_absence: bool,
) -> str:
    """Restore the pre-install ``memory.provider`` state.

    Only acts while ``memory_server`` is still the active provider — if the
    user manually switched to another provider after install, that choice is
    never clobbered. ``restore_absence`` (backup held the absence marker, or
    no backup exists) removes the ``memory.provider`` key entirely, matching
    the pre-install "no key" state (CORE-1/2).

    Returns a human-readable description of the action ('' when nothing was
    done).
    """
    if current_provider != "memory_server":
        return ""
    memory = yaml_data.setdefault("memory", {})
    if restore_absence or restored_provider is None:
        if memory.get("provider") == "memory_server":
            del memory["provider"]
        return "memory.provider removed (restored absence — no prior key)"
    memory["provider"] = restored_provider
    return f"memory.provider restored: {current_provider!r} → {restored_provider!r}"


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

    # Read the backup: a provider name to restore, the absence marker, or
    # nothing (missing file -> absence as the last resort).
    restored_provider: str | None = None
    restore_absence = False
    if back_path.is_file():
        raw = back_path.read_text().strip()
        if raw == ABSENT_PROVIDER_MARKER:
            restore_absence = True
        elif raw:
            restored_provider = raw
    else:
        restore_absence = True  # no recorded prior state -> key was absent

    if not removed:
        out("ℹ️  memory_server provider was not registered — nothing to remove")
        return 0

    action = _restore_previous_provider(
        data,
        current_provider=current_provider,
        restored_provider=restored_provider,
        restore_absence=restore_absence,
    )

    if dry_run:
        out(f"🔍 Dry-run — would write to: {cfg_path}")
        if action:
            out(f"   {action}")
        else:
            out("   memory.provider left unchanged (not memory_server)")
        out(f"   Remove backup file: {back_path}")
        out("\nTarget config.yaml changes:")
        from ruamel.yaml import YAML

        y = YAML()
        y.preserve_quotes = True
        y.dump(data, sys.stdout)
        return 0

    if action:
        out(f"   {action}")

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
# Doctor — read-only raw/effective storage diagnostics
# ---------------------------------------------------------------------------


def _collect_profile_homes(hermes_home: str) -> list[tuple[str, Path]]:
    """Return [(label, home_dir)] for the root config and every profile.

    A profile's HERMES_HOME is ``<hermes_home>/profiles/<name>``; the root
    config lives at ``<hermes_home>/config.yaml``.
    """
    homes: list[tuple[str, Path]] = [("default", Path(hermes_home))]
    profiles_dir = Path(hermes_home) / "profiles"
    if profiles_dir.is_dir():
        for child in sorted(profiles_dir.iterdir()):
            if child.is_dir() and (child / "config.yaml").is_file():
                homes.append((child.name, child))
    return homes


def _profile_data_dirs(home: Path) -> list[Path]:
    """Return legacy store entries, classifying final links without following."""
    from memory_server.paths import inspect_component_chain_nofollow

    found: list[Path] = []
    for rel in ("data/lancedb", "data/graph.json"):
        candidate = home / rel
        if inspect_component_chain_nofollow(candidate, anchor=Path("/"))[-1].kind != "absent":
            found.append(candidate)
    return found


_DOCTOR_HINT = "Run `memory-server migrate-profile-storage` to rebuild unavailable projections."


def _safe_store_diagnostics(layout) -> dict:
    """Inspect only existing regular entries; never initialize a provider/store."""
    result = {
        "sqlite": {"schema": "unknown", "integrity": "unknown", "counts": "unknown"},
        "outbox_counts": {key: "unknown" for key in ("pending", "processing", "completed", "failed")},
        "vector": {"available": "unknown", "count": "unknown", "dimension": "unknown", "coverage": "unknown"},
        "graph": {"available": "unknown", "nodes": "unknown", "edges": "unknown", "structure": "unknown"},
    }
    if layout.sqlite.local_path is not None and classify_artifact_nofollow(layout.sqlite.local_path) == "regular_file":
        path = layout.sqlite.local_path
        if all(
            classify_artifact_nofollow(Path(str(path) + suffix)) == "absent" for suffix in ("-wal", "-shm", "-journal")
        ):
            try:
                uri = f"file:{path}?mode=ro&immutable=1"
                with sqlite3.connect(uri, uri=True) as conn:
                    integrity = conn.execute("PRAGMA integrity_check").fetchone()
                    result["sqlite"]["integrity"] = "ok" if integrity == ("ok",) else "unknown"
                    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    result["sqlite"]["schema"] = "known"
                    counts = {}
                    for table in ("facts", "outbox"):
                        if table in tables:
                            counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    result["sqlite"]["counts"] = counts
                    if "outbox" in tables:
                        for state, count in conn.execute("SELECT status, COUNT(*) FROM outbox GROUP BY status"):
                            if state in result["outbox_counts"]:
                                result["outbox_counts"][state] = count
            except (OSError, sqlite3.Error):
                pass
    graph = layout.graph_snapshot_path
    if classify_artifact_nofollow(graph) == "regular_file":
        try:
            payload = json.loads(graph.read_text(encoding="utf-8"))
            nodes = payload.get("nodes") if isinstance(payload, dict) else None
            edges = payload.get("edges") if isinstance(payload, dict) else None
            if isinstance(nodes, list) and isinstance(edges, list):
                result["graph"] = {"available": True, "nodes": len(nodes), "edges": len(edges), "structure": "known"}
        except (OSError, ValueError, TypeError):
            pass
    if layout.vector.local_path is not None:
        result["vector"]["available"] = classify_artifact_nofollow(layout.vector.local_path) != "absent"
    elif layout.vector.kind == "memory":
        result["vector"]["available"] = False
    return result


def _doctor_report(hermes_home: str, *, env: dict | None = None) -> dict:
    """Return stable, redacted, read-only diagnostics for every CMMS profile."""
    from memory_server.plugins.hermes.config import (
        HermesPluginConfig,
        build_storage_config_report,
    )

    rows = []
    for label, home in _collect_profile_homes(hermes_home):
        cfg_path = _config_path(str(home))
        if not cfg_path.is_file():
            continue
        data = _load_config(cfg_path)
        entry = ((data.get("memory") or {}).get("providers") or {}).get("memory_server") or {}
        if not entry:
            continue
        config_report = build_storage_config_report(entry, include_env=env is None or bool(env))
        raw = dict(config_report.raw)
        effective = dict(config_report.effective)
        raw["path"] = entry.get("path")
        env_path = os.environ.get("MEMORY_SERVER_PATH") if env is None else None
        effective["path"] = env_path or entry.get("path")
        effective_cfg = HermesPluginConfig.from_dict(effective, use_env=False)
        status, code, message = "OK", None, "layout is coherent"
        layout = None
        try:
            layout = effective_cfg.resolve_storage_layout(hermes_home=str(home), settings=None)
            if layout.compatibility:
                status, code, message = "WARN", "W_LEGACY_SPLIT_LAYOUT", "legacy-split-layout"
            if layout.unavailable_projections:
                status, code, message = (
                    "ERROR",
                    "E_PROJECTION_UNAVAILABLE",
                    "projection unavailable without following link",
                )
        except StorageLayoutError as exc:
            status, code, message = "ERROR", exc.code, str(exc)
        canonical = {
            "mode": config_report.effective_origins.get("storage_mode", "default"),
            "root": config_report.effective_origins.get("data_root", "default"),
            "installation": "env" if env_path else ("yaml" if entry.get("path") else "default"),
        }
        rows.append(
            {
                "profile": label,
                "status": status,
                "code": code,
                "message": message,
                "raw": config_report.as_dict()["raw"],
                "effective": config_report.as_dict()["effective"],
                "raw_origins": config_report.as_dict()["raw_origins"],
                "effective_origins": config_report.as_dict()["effective_origins"],
                "canonical_origins": canonical,
                "safe_diagnostics": _safe_store_diagnostics(layout)
                if layout
                else {
                    "sqlite": {"schema": "unknown", "integrity": "unknown", "counts": "unknown"},
                    "outbox_counts": {key: "unknown" for key in ("pending", "processing", "completed", "failed")},
                    "vector": {
                        "available": "unknown",
                        "count": "unknown",
                        "dimension": "unknown",
                        "coverage": "unknown",
                    },
                    "graph": {"available": "unknown", "nodes": "unknown", "edges": "unknown", "structure": "unknown"},
                },
            }
        )
    overall = (
        "ERROR"
        if any(row["status"] == "ERROR" for row in rows)
        else "WARN"
        if any(row["status"] == "WARN" for row in rows)
        else "OK"
    )
    return {"status": overall, "profiles": rows, "migration_hint": _DOCTOR_HINT}


def _do_doctor(
    hermes_home: str,
    *,
    out,
) -> int:
    """Render the read-only report and return 1 for WARN or ERROR."""
    report = _doctor_report(hermes_home)
    for row in report["profiles"]:
        out(f"{row['status']} {row['profile']}: {row['message']}")
        if row["code"]:
            out(f"  code={row['code']}")
    if report["status"] != "OK":
        out(_DOCTOR_HINT)
    return int(report["status"] != "OK")


@app.command("doctor")
def doctor(
    hermes_home: Optional[str] = typer.Option(
        None,
        "--hermes-home",
        help="Hermes config directory (default: $HERMES_HOME or ~/.hermes)",
    ),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Inspect raw/effective StorageLayout without opening stores."""
    resolved = _find_hermes_home(hermes_home)
    report = _doctor_report(resolved)
    if json_output:
        typer.echo(json.dumps(report, sort_keys=True, separators=(",", ":")))
    else:
        _do_doctor(resolved, out=typer.echo)
    sys.exit(1 if report["status"] != "OK" else 0)


def _migration_dry_run_payload(plan: MigrationPlan, request: MigrationRequest) -> dict:
    """Stable dry-run report fields (DETAIL 8); nothing is created on disk."""
    return {
        "schema_version": 1,
        "run_id": request.run_id,
        "mode": "dry-run",
        "strategy": request.strategy,
        "source_sql": plan.source_sql.__dict__,
        "target": {"root": str(plan.layout.data_root)},
        "lock_availability": plan.lock_availability,
        "warnings": [warning.__dict__ for warning in plan.warnings],
        "blockers": [blocker.__dict__ for blocker in plan.blockers],
        "planned_operations": [operation.__dict__ for operation in plan.planned_operations],
        "proposed_manifest_path": str(plan.layout.data_root / ".cmms-migrations" / request.run_id / "manifest.json"),
    }


@app.command("migrate-profile-storage")
def migrate_profile_storage(
    hermes_home: Optional[str] = typer.Option(None, "--hermes-home"),
    source_sql: Optional[Path] = typer.Option(None, "--source-sql"),
    target_root: Optional[Path] = typer.Option(None, "--target-root"),
    strategy: str = typer.Option("rebuild-from-profile-sql", "--strategy"),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
    apply: bool = typer.Option(False, "--apply"),
    confirm_target: Optional[str] = typer.Option(None, "--confirm-target"),
    attest_runtimes_stopped: Optional[str] = typer.Option(None, "--attest-runtimes-stopped"),
    confirm_embedding_plan: Optional[str] = typer.Option(None, "--confirm-embedding-plan"),
    resume: Optional[Path] = typer.Option(None, "--resume"),
    rollback: Optional[Path] = typer.Option(None, "--rollback"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Plan profile storage migration; mutation requires explicit confirmations."""
    home = Path(_find_hermes_home(hermes_home))
    if resume and rollback:
        raise typer.BadParameter("--resume and --rollback are mutually exclusive")
    if (resume or rollback) and not apply:
        raise typer.BadParameter("--apply is required")
    try:
        manifest_path = resume or rollback
        if manifest_path is not None:
            request = MigrationRequest(
                home,
                target_root=target_root,
                mode="resume" if resume else "rollback",
                confirm_target=confirm_target,
                stop_attestation=attest_runtimes_stopped,
            )
            if resume:
                result: dict | object = resume_profile_migration(manifest_path, request)
            else:
                result = rollback_profile_migration(manifest_path, request)
        else:
            request = MigrationRequest(
                home,
                source_sql=source_sql,
                target_root=target_root,
                strategy=cast(MigrationStrategy, strategy),
                run_id=run_id or uuid4().hex,
                mode="apply" if apply else "dry-run",
                confirm_target=confirm_target,
                stop_attestation=attest_runtimes_stopped,
                embedding_plan_digest=confirm_embedding_plan,
            )
            plan = plan_profile_migration(request)
            if apply:
                result = apply_profile_migration(plan).__dict__
            else:
                result = _migration_dry_run_payload(plan, request)
        typer.echo(json.dumps(result, default=str, sort_keys=True, indent=2))
        if not isinstance(result, dict) and getattr(result, "status", "") == "failed":
            raise typer.Exit(3)
    except ValueError as exc:
        raw = str(exc)
        code = raw.split(":", 1)[0].strip()
        if not code.startswith("E_"):
            code = "E_MIGRATION_STAGE_FAILED"
        hint = DIAGNOSTIC_CONTRACT.get(code, {}).get("hint", "Resolve the reported condition before retrying.")
        diagnostic = Diagnostic(code, "error", raw, "storage", hint)
        typer.echo(json.dumps(diagnostic.__dict__, sort_keys=True))
        phase = "manifest" if code.startswith("E_MANIFEST_") else "precondition"
        raise typer.Exit(exit_code_for_diagnostic(code, phase=phase)) from exc


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    if ctx.invoked_subcommand is None:
        _get_run_server()()


@app.command()
def serve():
    """Start the MCP server (stdio transport)"""
    _get_run_server()()


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


@app.command("benchmark-longmemeval")
def benchmark_longmemeval(
    dataset: Path = typer.Argument(..., help="Path to longmemeval_s_cleaned.json"),
    output: Path = typer.Option(
        Path("benchmark-results/longmemeval_builtin.jsonl"),
        "--output",
        "-o",
        help="JSONL output path for traces, target sets, and scores",
    ),
    top_k: int = typer.Option(10, "--top-k", min=1, help="Retrieval cutoff for metrics"),
    limit: int | None = typer.Option(None, "--limit", min=1, help="Optional number of queries to run"),
) -> None:
    """Run the LongMemEval-S built-in baseline with raw/source/canonical scoring."""

    from memory_server.benchmarks.longmemeval import run_builtin_baseline

    summary = run_builtin_baseline(dataset, output_path=output, top_k=top_k, limit=limit)
    typer.echo(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    app()
