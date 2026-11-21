"""Unit tests for service JSON v1 (slice T2).

Everything runs on ``tmp_path``: no live CMMS data root, config, or lock is
touched and no service is started.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import tempfile
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

from memory_server.native.config import (
    Limits,
    ServiceConfigError,
    load_service_config,
    preflight_layout,
    provision_store_id,
)
from memory_server.settings import get_settings


@pytest.fixture
def socket_dir():
    """A short, private directory for the AF_UNIX socket path.

    ``tmp_path`` is ~92 bytes deep in this environment; any socket below it
    would exceed the 107-byte limit the validator enforces. The socket
    directory is therefore a short ``/tmp`` scratch dir, removed on teardown.
    """
    path = Path(tempfile.mkdtemp(prefix="cmmst2-", dir="/tmp"))
    path.chmod(0o700)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _dirs(tmp_path: Path, socket_dir: Path) -> tuple[Path, Path, Path, Path]:
    root = tmp_path / "store"
    root.mkdir(exist_ok=True)
    context = tmp_path / "context"
    context.mkdir(exist_ok=True)
    homes = tmp_path / "homes"
    homes.mkdir(exist_ok=True)
    (homes / "hermes").mkdir(exist_ok=True)
    (homes / "coder").mkdir(exist_ok=True)
    return root, context, socket_dir, homes


def _document(root: Path, context: Path, socket_dir: Path, homes: Path, **overrides):
    document: dict = {
        "version": 1,
        "data_root": str(root),
        "storage_context_home": str(context),
        "socket_path": str(socket_dir / "memory.sock"),
        "allow_homes": [str(homes / "hermes"), str(homes / "coder")],
        "backend": {
            "storage_mode": "shared",
            "db_url": f"sqlite+aiosqlite:///{root / 'memory.db'}",
            "vector_backend": "lancedb",
            "lancedb_path": str(root / "lancedb"),
            "graph_snapshot_path": str(root / "graph.json"),
            "vector_collection": "memories",
            "extraction_mode": "llm",
            "llm_model": "deepseek-v4-flash",
            "llm_base_url": "https://api.deepseek.com/v1",
            "max_facts": 5,
            "writer": {"flush_interval": 5.0, "max_batch": 50},
        },
    }
    document.update(overrides)
    return document


def _write(tmp_path: Path, document, *, mode: int = 0o600, name: str = "service.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    os.chmod(path, mode)
    return path


def _write_text(tmp_path: Path, text: str, *, mode: int = 0o600, name: str = "service.json") -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    os.chmod(path, mode)
    return path


def _assert_code(path: Path, code: str) -> None:
    with pytest.raises(ServiceConfigError) as excinfo:
        load_service_config(path)
    assert excinfo.value.code == code, f"expected {code}, got {excinfo.value.code}: {excinfo.value}"


def _valid(tmp_path: Path, socket_dir: Path, *, name: str = "service.json", **overrides) -> Path:
    root, context, sockets, homes = _dirs(tmp_path, socket_dir)
    return _write(tmp_path, _document(root, context, sockets, homes, **overrides), name=name)


def test_load_minimal_config_ok(tmp_path: Path, socket_dir: Path) -> None:
    path = _valid(tmp_path, socket_dir)
    config = load_service_config(path)

    assert config.version == 1
    assert config.store_id == ""
    assert config.data_root == (tmp_path / "store").resolve()
    assert config.storage_context_home == (tmp_path / "context").resolve()
    assert config.socket_path == socket_dir / "memory.sock"
    assert config.allow_homes == (
        (tmp_path / "homes" / "hermes").resolve(),
        (tmp_path / "homes" / "coder").resolve(),
    )
    assert config.backend.storage_mode == "shared"
    assert config.backend.data_root == str((tmp_path / "store").resolve())
    assert config.backend.vector_backend == "lancedb"
    assert config.backend.writer.flush_interval == 5.0
    assert config.backend.writer.max_batch == 50
    assert config.limits == Limits()
    assert config.raw["backend"]["storage_mode"] == "shared"


def test_reject_unknown_key(tmp_path: Path, socket_dir: Path) -> None:
    path = _valid(tmp_path, socket_dir, unexpected=1)
    _assert_code(path, "E_CONFIG_UNKNOWN_KEY")


def test_reject_world_readable_file(tmp_path: Path, socket_dir: Path) -> None:
    path = _valid(tmp_path, socket_dir)
    os.chmod(path, 0o644)
    _assert_code(path, "E_CONFIG_READ")


def test_reject_symlink(tmp_path: Path, socket_dir: Path) -> None:
    target = _valid(tmp_path, socket_dir)
    link = tmp_path / "link.json"
    link.symlink_to(target)
    _assert_code(link, "E_CONFIG_READ")


def test_reject_empty_allow_homes(tmp_path: Path, socket_dir: Path) -> None:
    path = _valid(tmp_path, socket_dir, allow_homes=[])
    _assert_code(path, "E_CONFIG_ALLOW_HOMES")


def test_reject_relative_paths(tmp_path: Path, socket_dir: Path) -> None:
    path = _valid(tmp_path, socket_dir, data_root="store/relative")
    _assert_code(path, "E_CONFIG_PATH")


def test_reject_socket_path_too_long(tmp_path: Path, socket_dir: Path) -> None:
    long_name = "a" * 200 + ".sock"
    long_socket = socket_dir / long_name
    path = _valid(tmp_path, socket_dir, socket_path=str(long_socket))
    assert len(os.fsencode(str(long_socket))) > 107
    _assert_code(path, "E_CONFIG_PATH")


def test_reject_bad_version(tmp_path: Path, socket_dir: Path) -> None:
    path = _valid(tmp_path, socket_dir, version=2)
    _assert_code(path, "E_CONFIG_VERSION")


def test_reject_bad_json(tmp_path: Path) -> None:
    path = _write_text(tmp_path, '{"version": 1, ')
    _assert_code(path, "E_CONFIG_JSON")


def test_reject_duplicate_json_key(tmp_path: Path, socket_dir: Path) -> None:
    root, context, sockets, homes = _dirs(tmp_path, socket_dir)
    text = json.dumps(_document(root, context, sockets, homes), indent=2)
    text = text[: text.rindex("}")] + ',\n  "version": 1\n}'
    path = _write_text(tmp_path, text)
    _assert_code(path, "E_CONFIG_JSON")


def test_reject_backend_enum(tmp_path: Path, socket_dir: Path) -> None:
    root, context, sockets, homes = _dirs(tmp_path, socket_dir)
    document = _document(root, context, sockets, homes)
    document["backend"]["vector_backend"] = "pgvector"
    path = _write(tmp_path, document)
    _assert_code(path, "E_CONFIG_BACKEND")


def test_reject_db_path_outside_root(tmp_path: Path, socket_dir: Path) -> None:
    root, context, sockets, homes = _dirs(tmp_path, socket_dir)
    document = _document(root, context, sockets, homes)
    document["backend"]["db_url"] = f"sqlite+aiosqlite:///{tmp_path / 'foreign.db'}"
    path = _write(tmp_path, document)
    _assert_code(path, "E_CONFIG_PATH")


def test_provision_writes_uuid_once(tmp_path: Path, socket_dir: Path) -> None:
    path = _valid(tmp_path, socket_dir)

    first = provision_store_id(path)
    assert str(uuid.UUID(first)) == first
    assert uuid.UUID(first).version == 4

    before = path.read_bytes()
    before_mtime = path.stat().st_mtime_ns

    second = provision_store_id(path)

    assert second == first
    assert path.read_bytes() == before
    assert path.stat().st_mtime_ns == before_mtime


def test_provision_atomic_and_0600(tmp_path: Path, socket_dir: Path) -> None:
    path = _valid(tmp_path, socket_dir)

    store_id = provision_store_id(path)

    mode = stat.S_IMODE(path.stat().st_mode)
    assert mode == 0o600
    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["store_id"] == store_id
    assert document["version"] == 1
    leftovers = [entry.name for entry in path.parent.iterdir() if ".tmp-" in entry.name]
    assert leftovers == []


def test_preflight_layout_matches_data_root(tmp_path: Path, socket_dir: Path) -> None:
    path = _valid(tmp_path, socket_dir)
    provision_store_id(path)
    config = load_service_config(path)

    layout = preflight_layout(config, get_settings())

    assert layout.mode == "shared"
    assert layout.data_root == config.data_root.resolve()
    assert layout.sqlite.local_path is not None
    assert layout.sqlite.local_path == config.data_root.resolve() / "memory.db"
    assert layout.vector.local_path == config.data_root.resolve() / "lancedb"
    assert layout.graph_snapshot_path == config.data_root.resolve() / "graph.json"
    assert layout.root_lock_path == config.data_root.resolve() / ".cmms-storage.lock"


def test_preflight_rejects_foreign_db_path(tmp_path: Path, socket_dir: Path) -> None:
    path = _valid(tmp_path, socket_dir)
    provision_store_id(path)
    config = load_service_config(path)
    tampered = replace(
        config,
        backend=replace(
            config.backend,
            db_url=f"sqlite+aiosqlite:///{tmp_path / 'foreign.db'}",
        ),
    )

    with pytest.raises(ServiceConfigError) as excinfo:
        preflight_layout(tampered, get_settings())
    assert excinfo.value.code == "E_LAYOUT_MISMATCH"


def test_limits_defaults_and_overrides(tmp_path: Path, socket_dir: Path) -> None:
    path = _valid(tmp_path, socket_dir)
    config = load_service_config(path)

    assert config.limits.frame_bytes == 4 * 1024 * 1024
    assert config.limits.max_connections == 16
    assert config.limits.max_operations == 16
    assert config.limits.hello_timeout_s == 5.0
    assert config.limits.read_idle_s == 5.0
    assert config.limits.connect_timeout_s == 2.0
    assert config.limits.tool_timeout_s == 60.0
    assert config.limits.recall_timeout_s == 10.0
    assert config.limits.enqueue_timeout_s == 5.0
    assert config.limits.flush_timeout_s == 30.0
    assert config.limits.live_turns == 64
    assert config.limits.terminal_failed == 64
    assert config.limits.pre_drain_s == 8.0

    overridden = _valid(tmp_path, socket_dir, name="override.json", limits={"live_turns": 4})
    config = load_service_config(overridden)

    assert config.limits.live_turns == 4
    assert config.limits.frame_bytes == 4 * 1024 * 1024
    assert config.limits.terminal_failed == 64
