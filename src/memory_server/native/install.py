"""Install the standalone Hermes proxy plugin into a ``HERMES_HOME`` tree.

The installer owns exactly two artifacts:

* ``$HERMES_HOME/plugins/memory_server_proxy/`` — the plugin files copied from
  ``plugin-dist/memory_server_proxy/`` (``__pycache__`` never travels);
* ``$HERMES_HOME/memory_server_proxy.json`` — the client sidecar (mode 0600,
  written atomically).

``--activate`` additionally points ``memory.provider`` at the proxy plugin using
``ruamel.yaml`` so comments and formatting in ``config.yaml`` survive; the
previous file is kept as ``config.yaml.bak-<UTC>``. Nothing else in the Hermes
tree is read or written, and ``dry_run`` writes nothing at all.

No secrets are handled here: the sidecar carries only an absolute socket path and
the expected ``store_id``.
"""

from __future__ import annotations

import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PLUGIN_NAME = "memory_server_proxy"
CLIENT_CONFIG_NAME = "memory_server_proxy.json"
SIDECAR_VERSION = 1
PROVIDER_NAME = "memory_server_proxy"


class InstallError(RuntimeError):
    """An installer failure with a stable diagnostic code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


# ------------------------------------------------------------------- paths
def _plugin_dir(hermes_home: Path) -> Path:
    return Path(hermes_home) / "plugins" / PLUGIN_NAME


def _sidecar_path(hermes_home: Path) -> Path:
    return Path(hermes_home) / CLIENT_CONFIG_NAME


def _config_path(hermes_home: Path) -> Path:
    return Path(hermes_home) / "config.yaml"


def _plugin_files(source_dir: Path) -> list[Path]:
    """Return the plugin files to copy, excluding caches and bytecode."""
    source = Path(source_dir)
    if not source.is_dir():
        raise InstallError("E_PLUGIN_SOURCE", f"plugin source directory is missing: {source}")
    files: list[Path] = []
    for path in sorted(source.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(source)
        if "__pycache__" in relative.parts or path.suffix == ".pyc":
            continue
        files.append(path)
    if not files:
        raise InstallError("E_PLUGIN_SOURCE", f"no plugin files found under {source}")
    return files


# ------------------------------------------------------------------ writes
def _atomic_write(path: Path, payload: bytes, *, mode: int) -> None:
    """Write ``payload`` to ``path`` via tmp + ``os.replace`` with an exact mode."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(tmp, flags, mode)
    try:
        try:
            view = memoryview(payload)
            while view:
                written = os.write(fd, view)
                view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _backup(path: Path) -> Path:
    """Copy ``path`` to ``<name>.bak-<UTC>``; never overwrite an existing backup."""
    source = Path(path)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = source.with_name(f"{source.name}.bak-{stamp}")
    suffix = 1
    while target.exists():
        target = source.with_name(f"{source.name}.bak-{stamp}.{suffix}")
        suffix += 1
    shutil.copy2(source, target)
    return target


# ---------------------------------------------------------------- sidecar
def _sidecar_document(socket_path: Path, expected_store_id: str) -> dict[str, Any]:
    socket = Path(socket_path)
    if not socket.is_absolute():
        raise InstallError("E_SOCKET_PATH", f"socket path must be absolute: {socket}")
    try:
        canonical = isinstance(expected_store_id, str) and str(uuid.UUID(expected_store_id)) == expected_store_id
    except (ValueError, AttributeError, TypeError):
        canonical = False
    if not canonical:
        raise InstallError(
            "E_STORE_ID", f"expected_store_id must be a canonical UUID: {expected_store_id!r}"
        )
    return {
        "version": SIDECAR_VERSION,
        "socket_path": str(socket),
        "expected_store_id": expected_store_id,
    }


def read_client_config(hermes_home: Path) -> dict[str, Any] | None:
    """Read the sidecar; ``None`` when missing, unreadable, or not a JSON object."""
    path = _sidecar_path(hermes_home)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def write_client_config(hermes_home: Path, *, socket_path: Path, expected_store_id: str) -> Path:
    """Atomically write the sidecar (mode 0600) and return its path."""
    document = _sidecar_document(socket_path, expected_store_id)
    path = _sidecar_path(hermes_home)
    payload = (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    _atomic_write(path, payload, mode=0o600)
    return path


# -------------------------------------------------------------- activation
def _yaml_module() -> Any | None:
    try:
        from ruamel.yaml import YAML
    except ImportError:  # pragma: no cover - exercised only without the extra
        return None
    return YAML


def _dump_yaml(yaml: Any, data: Any, config_path: Path) -> None:
    """Replace ``config_path`` with the dumped tree, keeping its permission bits."""
    info = os.lstat(config_path)
    tmp = config_path.with_name(f".{config_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as handle:
            yaml.dump(data, handle)
        os.chmod(tmp, info.st_mode & 0o777)
        os.replace(tmp, config_path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _activate_provider(hermes_home: Path) -> dict[str, Any]:
    """Point ``memory.provider`` at the proxy plugin, backing up ``config.yaml``."""
    config_path = _config_path(hermes_home)
    if not config_path.is_file():
        raise InstallError("E_CONFIG_MISSING", f"config.yaml not found: {config_path}")

    yaml_class = _yaml_module()
    if yaml_class is None:
        return {"activated": False, "backup": None, "reason": "yaml unavailable"}

    yaml = yaml_class()
    yaml.preserve_quotes = True
    with open(config_path, encoding="utf-8") as handle:
        data = yaml.load(handle)
    if not isinstance(data, dict):
        data = {}

    memory = data.setdefault("memory", {})
    if not isinstance(memory, dict):
        memory = {}
        data["memory"] = memory
    memory["provider"] = PROVIDER_NAME

    backup = _backup(config_path)
    _dump_yaml(yaml, data, config_path)
    return {"activated": True, "backup": backup, "reason": None}


# -------------------------------------------------------------------- API
def install_proxy_plugin(
    *,
    hermes_home: Path,
    source_dir: Path,
    socket_path: Path,
    expected_store_id: str,
    activate: bool,
    dry_run: bool,
) -> dict[str, Any]:
    """Install the plugin tree and sidecar; optionally activate the provider."""
    home = Path(hermes_home)
    source = Path(source_dir)
    plugin_dir = _plugin_dir(home)
    sidecar = _sidecar_path(home)
    document = _sidecar_document(Path(socket_path), expected_store_id)
    files = _plugin_files(source)

    if dry_run:
        return {
            "plugin_dir": str(plugin_dir),
            "sidecar": str(sidecar),
            "activated": False,
            "backup": None,
            "dry_run": True,
            "would_activate": bool(activate),
            "would_write": [str(plugin_dir / path.relative_to(source)) for path in files] + [str(sidecar)],
        }

    plugin_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(plugin_dir, 0o700)
    for path in files:
        _atomic_write(plugin_dir / path.relative_to(source), path.read_bytes(), mode=0o600)

    write_client_config(home, socket_path=Path(document["socket_path"]), expected_store_id=expected_store_id)

    activated = False
    backup: Path | None = None
    reason: str | None = None
    if activate:
        outcome = _activate_provider(home)
        activated = bool(outcome["activated"])
        backup = outcome["backup"]
        reason = outcome["reason"]

    result: dict[str, Any] = {
        "plugin_dir": str(plugin_dir),
        "sidecar": str(sidecar),
        "activated": activated,
        "backup": str(backup) if backup is not None else None,
    }
    if reason is not None:
        result["reason"] = reason
    return result


def uninstall_proxy_plugin(*, hermes_home: Path, dry_run: bool) -> dict[str, Any]:
    """Remove only ``plugins/memory_server_proxy/`` and the sidecar."""
    home = Path(hermes_home)
    plugin_dir = _plugin_dir(home)
    sidecar = _sidecar_path(home)
    plugin_exists = plugin_dir.is_symlink() or plugin_dir.exists()
    sidecar_exists = sidecar.is_symlink() or sidecar.exists()
    targets = [str(path) for path, present in ((plugin_dir, plugin_exists), (sidecar, sidecar_exists)) if present]

    if dry_run:
        return {
            "plugin_dir": str(plugin_dir),
            "sidecar": str(sidecar),
            "removed": targets,
            "dry_run": True,
        }

    removed: list[str] = []
    if plugin_exists:
        if plugin_dir.is_symlink():
            plugin_dir.unlink()
        else:
            shutil.rmtree(plugin_dir)
        removed.append(str(plugin_dir))
    if sidecar_exists:
        sidecar.unlink()
        removed.append(str(sidecar))
    return {"plugin_dir": str(plugin_dir), "sidecar": str(sidecar), "removed": removed}
