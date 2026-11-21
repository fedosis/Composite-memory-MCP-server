"""CLI and installer tests for the native transport (slice T6).

Everything runs on ``tmp_path``: no live ``$HERMES_HOME``, service config, store,
or lock is touched. The ``native-status`` tests bind a throwaway AF_UNIX socket
under ``/tmp`` (the 107-byte path limit rules out ``tmp_path``).
"""

from __future__ import annotations

import json
import os
import socket
import stat
import struct
import threading
import uuid
from pathlib import Path

import pytest

from memory_server.native.cli import (
    exit_code_for,
    install_hermes_proxy,
    native_provision,
    native_serve,
    native_status,
)
from memory_server.native.config import ServiceConfigError, load_service_config
from memory_server.native.install import (
    CLIENT_CONFIG_NAME,
    InstallError,
    install_proxy_plugin,
    read_client_config,
    uninstall_proxy_plugin,
)
from memory_server.native.protocol import PROTO_VERSION, encode_frame
from memory_server.native.schema_v1 import schema_hash
from tests._native_helpers import make_service_config

HEADER = struct.Struct(">I")


# ----------------------------------------------------------------- helpers
def _recv_exact(conn: socket.socket, size: int) -> bytes:
    buf = b""
    while len(buf) < size:
        chunk = conn.recv(size - len(buf))
        if not chunk:
            raise OSError("connection closed")
        buf += chunk
    return buf


def _read_client_frame(conn: socket.socket) -> dict:
    (length,) = HEADER.unpack(_recv_exact(conn, HEADER.size))
    return json.loads(_recv_exact(conn, length).decode("utf-8"))


def _start_fake_service(
    socket_path: Path, reply_factory, seen: list[dict] | None = None,
) -> tuple[socket.socket, threading.Thread]:
    """Bind ``socket_path`` and answer exactly one hello with ``reply_factory()``."""
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(socket_path))
    server.listen(1)
    server.settimeout(10)

    def serve_once() -> None:
        try:
            conn, _ = server.accept()
        except OSError:
            return
        with conn:
            try:
                frame = _read_client_frame(conn)
                if seen is not None:
                    seen.append(frame)
                conn.sendall(encode_frame(reply_factory()))
            except OSError:
                return

    thread = threading.Thread(target=serve_once, daemon=True)
    thread.start()
    return server, thread


def _source_tree(root: Path) -> Path:
    source = root / "plugin-src"
    source.mkdir(parents=True)
    (source / "__init__.py").write_text(
        '"""fake plugin"""\n\nfrom .provider import MemoryServerProxyProvider\n', encoding="utf-8"
    )
    (source / "provider.py").write_text("class MemoryServerProxyProvider:\n    name = 'memory_server_proxy'\n",
                                        encoding="utf-8")
    (source / "plugin.yaml").write_text("name: memory_server_proxy\n", encoding="utf-8")
    cache = source / "__pycache__"
    cache.mkdir()
    (cache / "provider.cpython-311.pyc").write_bytes(b"\x00")
    return source


# -------------------------------------------------------------- provision
def test_native_provision_command(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = make_service_config(tmp_path)

    native_provision(config)

    payload = json.loads(capsys.readouterr().out)
    assert payload["path"] == str(config)
    store_id = payload["store_id"]
    assert str(uuid.UUID(store_id)) == store_id
    assert load_service_config(config).store_id == store_id
    assert json.loads(config.read_text(encoding="utf-8"))["store_id"] == store_id


# ----------------------------------------------------------------- status
def test_native_status_ready_exit_0(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    store_id = str(uuid.uuid4())
    config = make_service_config(tmp_path, store_id=store_id)
    cfg = load_service_config(config)

    # Assert the actual request, including admitted home, not only the fake reply.
    seen = []
    server, thread = _start_fake_service(
        cfg.socket_path,
        lambda: {
            "v": PROTO_VERSION,
            "ok": True,
            "correlation_id": "status",
            "state": "READY",
            "store_id": cfg.store_id,
            "epoch": str(uuid.uuid4()),
            "schema_hash": schema_hash(),
        },
        seen,
    )
    try:
        with pytest.raises(SystemExit) as info:
            native_status(config)
    finally:
        server.close()
        thread.join(timeout=10)

    assert info.value.code == 0
    assert seen == [{"v": 1, "op": "hello", "correlation_id": "status",
                     "payload": {"schema_hash": schema_hash(), "store_id": store_id,
                                 "claimed_home": str(cfg.allow_homes[0])}}]
    payload = json.loads(capsys.readouterr().out)
    assert payload["state"] == "READY"
    assert payload["store_id"] == store_id


def test_native_status_unavailable_exit_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    store_id = str(uuid.uuid4())
    config = make_service_config(tmp_path, store_id=store_id)

    with pytest.raises(SystemExit) as info:
        native_status(config)

    assert info.value.code == 2
    assert "E_UNAVAILABLE" in capsys.readouterr().err


def test_native_status_rejects_bad_epoch(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    store_id = str(uuid.uuid4())
    config = make_service_config(tmp_path, store_id=store_id)
    cfg = load_service_config(config)

    server, thread = _start_fake_service(
        cfg.socket_path,
        lambda: {
            "v": PROTO_VERSION,
            "ok": True,
            "correlation_id": "status",
            "state": "READY",
            "store_id": cfg.store_id,
            "epoch": "not-a-uuid",
            "schema_hash": schema_hash(),
        },
    )
    try:
        with pytest.raises(SystemExit) as info:
            native_status(config)
    finally:
        server.close()
        thread.join(timeout=10)

    assert info.value.code == 2
    assert "E_VALIDATION" in capsys.readouterr().err


# ------------------------------------------------------------------ serve
def test_native_serve_bad_config_exit_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = tmp_path / "service.json"
    config.write_text("{not valid json", encoding="utf-8")
    os.chmod(config, 0o600)

    with pytest.raises(SystemExit) as info:
        native_serve(config)

    assert info.value.code == 2
    assert "E_CONFIG_JSON" in capsys.readouterr().err


# -------------------------------------------------------------- installer
def test_install_hermes_proxy_dry_run_no_write(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    home = tmp_path / "hermes"
    socket_path = tmp_path / "run" / "memory.sock"

    with pytest.raises(SystemExit) as info:
        install_hermes_proxy(str(home), str(socket_path), str(uuid.uuid4()), dry_run=True)

    assert info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert any(path.endswith("__init__.py") for path in payload["would_write"])
    assert not home.exists()


def test_install_writes_plugin_and_sidecar(tmp_path: Path) -> None:
    home = tmp_path / "hermes"
    source = _source_tree(tmp_path)
    store_id = str(uuid.uuid4())
    socket_path = tmp_path / "run" / "memory.sock"

    result = install_proxy_plugin(
        hermes_home=home,
        source_dir=source,
        socket_path=socket_path,
        expected_store_id=store_id,
        activate=False,
        dry_run=False,
    )

    plugin_dir = home / "plugins" / "memory_server_proxy"
    assert (plugin_dir / "__init__.py").is_file()
    assert (plugin_dir / "provider.py").is_file()
    assert (plugin_dir / "plugin.yaml").is_file()
    assert not (plugin_dir / "__pycache__").exists()

    sidecar = home / CLIENT_CONFIG_NAME
    assert sidecar.is_file()
    assert stat.S_IMODE(os.stat(sidecar).st_mode) == 0o600
    assert read_client_config(home) == {
        "version": 1,
        "socket_path": str(socket_path),
        "expected_store_id": store_id,
    }
    assert result["plugin_dir"] == str(plugin_dir)
    assert result["sidecar"] == str(sidecar)
    assert result["activated"] is False
    assert result["backup"] is None


def test_install_rejects_symlink_plugin_dir(tmp_path: Path) -> None:
    home = tmp_path / "hermes"
    real_dir = tmp_path / "real-target"
    real_dir.mkdir(parents=True)
    plugin_dir = home / "plugins" / "memory_server_proxy"
    plugin_dir.parent.mkdir(parents=True)
    plugin_dir.symlink_to(real_dir)
    source = _source_tree(tmp_path)

    with pytest.raises(InstallError) as excinfo:
        install_proxy_plugin(
            hermes_home=home,
            source_dir=source,
            socket_path=tmp_path / "run" / "memory.sock",
            expected_store_id=str(uuid.uuid4()),
            activate=False,
            dry_run=False,
        )

    assert excinfo.value.code == "E_PLUGIN_SOURCE"
    assert plugin_dir.is_symlink()


def test_uninstall_removes_only_own_files(tmp_path: Path) -> None:
    home = tmp_path / "hermes"
    plugin_dir = home / "plugins" / "memory_server_proxy"
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "__init__.py").write_text("", encoding="utf-8")
    foreign = home / "plugins" / "other" / "keep.txt"
    foreign.parent.mkdir(parents=True)
    foreign.write_text("keep me", encoding="utf-8")
    sidecar = home / CLIENT_CONFIG_NAME
    sidecar.write_text("{}", encoding="utf-8")

    result = uninstall_proxy_plugin(hermes_home=home, dry_run=False)

    assert not plugin_dir.exists()
    assert not sidecar.exists()
    assert foreign.read_text(encoding="utf-8") == "keep me"
    assert set(result["removed"]) == {str(plugin_dir), str(sidecar)}


def test_activate_backs_up_config(tmp_path: Path) -> None:
    from ruamel.yaml import YAML

    home = tmp_path / "hermes"
    home.mkdir()
    config = home / "config.yaml"
    config.write_text("memory:\n  provider: memory_server  # keep me\n", encoding="utf-8")

    result = install_proxy_plugin(
        hermes_home=home,
        source_dir=_source_tree(tmp_path),
        socket_path=tmp_path / "run" / "memory.sock",
        expected_store_id=str(uuid.uuid4()),
        activate=True,
        dry_run=False,
    )

    backups = sorted(home.glob("config.yaml.bak-*"))
    assert len(backups) == 1
    assert result["activated"] is True
    assert result["backup"] == str(backups[0])

    yaml = YAML()
    with open(backups[0], encoding="utf-8") as handle:
        assert yaml.load(handle)["memory"]["provider"] == "memory_server"
    with open(config, encoding="utf-8") as handle:
        assert yaml.load(handle)["memory"]["provider"] == "memory_server_proxy"
    assert "# keep me" in config.read_text(encoding="utf-8")


# ------------------------------------------------------------ exit mapping
def test_exit_code_mapping() -> None:
    class StaleSocketError(RuntimeError):
        code = "E_STALE_SOCKET"

    assert exit_code_for(ServiceConfigError("E_CONFIG_JSON", "bad json")) == 2
    assert exit_code_for(FileExistsError("../memory.sock")) == 4
    assert exit_code_for(StaleSocketError("occupied")) == 4
    assert exit_code_for(ValueError("boom")) == 2
