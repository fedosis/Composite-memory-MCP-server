"""Operator CLI for the native owner service and the Hermes proxy installer.

The functions here are the single implementation behind the four
``memory-server native-*``/``install-hermes-proxy`` commands and behind
``python -m memory_server.native.cli`` (used by the shell installer).

Exit codes (stable; documented in ``docs/native-proxy.md``):

``0`` success
``2`` configuration error or service unavailable
``3`` ``FAILED_CLOSING`` — shutdown retained the lock or the socket path
``4`` socket path already occupied (``E_STALE_SOCKET``/``FileExistsError``)
``5`` second owner (``E_LOCK``/``StorageOwnershipError``)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import socket
import sys
from pathlib import Path
from typing import Any, NoReturn

from memory_server.native.config import (
    ServiceConfig,
    ServiceConfigError,
    load_service_config,
    provision_store_id,
)
from memory_server.native.install import (
    PLUGIN_NAME,
    InstallError,
    install_proxy_plugin,
    uninstall_proxy_plugin,
)
from memory_server.native.protocol import HEADER, PROTO_VERSION, ProtocolError, encode_frame
from memory_server.native.schema_v1 import schema_hash
from memory_server.paths import cmms_repo_root

EXIT_OK = 0
EXIT_CONFIG = 2
EXIT_FAILED_CLOSING = 3
EXIT_STALE_SOCKET = 4
EXIT_SECOND_OWNER = 5

_HELLO_CORRELATION_ID = "status"


class _HelloError(RuntimeError):
    """A malformed or truncated reply from the owner socket."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


# ------------------------------------------------------------------ helpers
def _fail(code: str, message: str) -> NoReturn:
    """Print a diagnostic code (no traceback) and exit with the config code."""
    print(f"{code}: {message}", file=sys.stderr)
    raise SystemExit(EXIT_CONFIG)


def exit_code_for(exc: BaseException) -> int:
    """Map a native failure onto the documented CLI exit code."""
    from memory_server.native.backend import BackendStartError
    from memory_server.runtime.errors import StorageOwnershipError

    if isinstance(exc, ServiceConfigError):
        return EXIT_CONFIG
    if isinstance(exc, FileExistsError) or getattr(exc, "code", None) == "E_STALE_SOCKET":
        return EXIT_STALE_SOCKET
    if isinstance(exc, StorageOwnershipError):
        return EXIT_SECOND_OWNER
    if isinstance(exc, BackendStartError):
        if exc.code == "E_LOCK" or isinstance(exc.cause, StorageOwnershipError):
            return EXIT_SECOND_OWNER
        return EXIT_FAILED_CLOSING
    return EXIT_CONFIG


def _plugin_source_dir() -> Path:
    return Path(cmms_repo_root()) / "plugin-dist" / PLUGIN_NAME


# -------------------------------------------------------------- wire client
def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise _HelloError("E_FRAME", "connection closed before the frame completed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_frame_blocking(sock: socket.socket, *, max_bytes: int) -> dict[str, Any]:
    (length,) = HEADER.unpack(_recv_exact(sock, HEADER.size))
    if not 1 <= length <= max_bytes:
        raise _HelloError("E_FRAME", f"frame length {length} outside 1..{max_bytes}")
    try:
        body = _recv_exact(sock, length).decode("utf-8", errors="strict")
        value = json.loads(body)
    except (UnicodeError, ValueError) as exc:
        raise _HelloError("E_FRAME", f"undecodable frame: {exc}") from exc
    if not isinstance(value, dict):
        raise _HelloError("E_VALIDATION", "frame body is not a JSON object")
    return value


def _hello(config: ServiceConfig, *, timeout: float) -> dict[str, Any]:
    """Perform one blocking hello exchange against the owner socket."""
    request = {
        "v": PROTO_VERSION,
        "op": "hello",
        "correlation_id": _HELLO_CORRELATION_ID,
        "schema_hash": schema_hash(),
        "store_id": config.store_id,
        "claimed_home": str(Path.home()),
    }
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(str(config.socket_path))
        sock.sendall(encode_frame(request))
        reply = _read_frame_blocking(sock, max_bytes=config.limits.frame_bytes)
    except OSError as exc:
        _fail("E_UNAVAILABLE", f"cannot talk to {config.socket_path}: {exc}")
    except ProtocolError as exc:
        _fail(exc.code, str(exc))
    except _HelloError as exc:
        _fail(exc.code, str(exc))
    finally:
        sock.close()

    if reply.get("ok") is not True:
        _fail(str(reply.get("code", "E_HELLO_REJECTED")), str(reply.get("message", "hello rejected")))
    return reply


# ---------------------------------------------------------------- commands
def native_provision(config: Path) -> None:
    """Provision (or read back) the stable ``store_id`` of a service JSON."""
    try:
        store_id = provision_store_id(Path(config))
    except ServiceConfigError as exc:
        _fail(exc.code, str(exc))
    except OSError as exc:
        _fail("E_CONFIG_READ", f"cannot provision store_id: {exc}")
    print(json.dumps({"store_id": store_id, "path": str(config)}, sort_keys=True))


def native_serve(config: Path) -> None:
    """Run the single-owner service until it shuts down."""
    try:
        cfg = load_service_config(Path(config))
    except ServiceConfigError as exc:
        _fail(exc.code, str(exc))

    # Lazy: the service pulls the whole provider runtime.
    from memory_server.native import service

    try:
        code = asyncio.run(service.serve(cfg))
    except KeyboardInterrupt:
        code = EXIT_OK
    raise SystemExit(code)


def native_status(config: Path) -> None:
    """Hello the owner service and assert READY + store_id + schema hash."""
    try:
        cfg = load_service_config(Path(config))
    except ServiceConfigError as exc:
        _fail(exc.code, str(exc))

    reply = _hello(cfg, timeout=cfg.limits.connect_timeout_s)
    state = reply.get("state")
    if state == "FAILED_CLOSING":
        print("FAILED_CLOSING", file=sys.stderr)
        raise SystemExit(EXIT_FAILED_CLOSING)
    if state != "READY":
        _fail(str(state or "E_NOT_READY"), "native service is not READY")
    if reply.get("store_id") != cfg.store_id:
        _fail("E_STORE_ID", "service store_id does not match the service config")
    if reply.get("schema_hash") != schema_hash():
        _fail("E_SCHEMA", "service schema hash differs from canonical schema v1")

    print(
        json.dumps(
            {key: reply.get(key) for key in ("state", "store_id", "epoch", "schema_hash")},
            sort_keys=True,
        )
    )
    raise SystemExit(EXIT_OK)


def install_hermes_proxy(
    hermes_home: str,
    socket_path: str,
    expected_store_id: str,
    activate: bool = False,
    dry_run: bool = False,
    uninstall: bool = False,
) -> None:
    """Install (or remove) the proxy plugin in a ``HERMES_HOME`` tree."""
    home = Path(hermes_home)
    try:
        if uninstall:
            result = uninstall_proxy_plugin(hermes_home=home, dry_run=dry_run)
        else:
            result = install_proxy_plugin(
                hermes_home=home,
                source_dir=_plugin_source_dir(),
                socket_path=Path(socket_path),
                expected_store_id=expected_store_id,
                activate=activate,
                dry_run=dry_run,
            )
    except InstallError as exc:
        _fail(exc.code, str(exc))
    except OSError as exc:
        _fail("E_INSTALL_IO", f"installer I/O failure: {exc}")

    print(json.dumps(result, sort_keys=True, indent=2))
    raise SystemExit(EXIT_OK)


# ------------------------------------------------------------------ -m CLI
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="memory_server.native.cli",
        description="Operate the CMMS native owner service and install the Hermes proxy plugin.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    for name, summary in (
        ("provision", "provision the stable store_id in a service JSON"),
        ("serve", "run the single-owner native service"),
        ("status", "hello the running service and verify READY"),
    ):
        command = commands.add_parser(name, help=summary)
        command.add_argument("--config", type=Path, required=True, help="path to the service JSON")

    installer = commands.add_parser("install-hermes-proxy", help="install/uninstall the proxy plugin")
    installer.add_argument("--hermes-home", required=True, help="HERMES_HOME directory")
    installer.add_argument("--socket", required=True, type=Path, help="absolute AF_UNIX socket path")
    installer.add_argument("--expected-store-id", required=True, help="canonical store_id UUID")
    installer.add_argument("--activate", action="store_true", help="set memory.provider in config.yaml")
    installer.add_argument("--dry-run", action="store_true", help="print the plan, write nothing")
    installer.add_argument("--uninstall", action="store_true", help="remove the plugin and sidecar")

    args = parser.parse_args(argv)
    if args.command == "install-hermes-proxy":
        install_hermes_proxy(
            args.hermes_home,
            str(args.socket),
            args.expected_store_id,
            activate=args.activate,
            dry_run=args.dry_run,
            uninstall=args.uninstall,
        )
        return EXIT_OK
    handlers = {"provision": native_provision, "serve": native_serve, "status": native_status}
    handlers[args.command](args.config)
    return EXIT_OK


if __name__ == "__main__":
    main()
