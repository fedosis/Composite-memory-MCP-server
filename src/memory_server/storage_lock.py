"""Cross-process shared runtime and exclusive maintenance locks.

POSIX protocol: the upgraded runtime holds ``LOCK_SH`` on
``<data_root>/.cmms-storage.lock``; maintenance holds ``LOCK_EX``. Root order is
canonical (sorted lexical bytes) and release is always in reverse order.
Lock metadata is written only for an exclusive holder. Dry-run inspection of an
existing regular lock is read-only; an unsafe lock entry fails closed.
"""
from __future__ import annotations

import fcntl
import json
import os
import socket
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Literal

from memory_server.paths import classify_artifact_nofollow

LockMode = Literal["shared", "exclusive"]

_LOCK_NAME = ".cmms-storage.lock"
_DEFAULT_TIMEOUT = 5.0
_POLL_INTERVAL_SECONDS = 0.02


@dataclass(frozen=True)
class LockOwner:
    pid: int
    host: str
    mode: LockMode
    started_at: str
    command: str


class StorageLockError(RuntimeError):
    """Fail-closed storage lock error carrying a stable DETAIL code."""

    def __init__(self, code: str, message: str = "storage lock failure") -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class LockInspection:
    availability: str
    owner: LockOwner | None = None
    error: str | None = None


def _lock_path(root: Path) -> Path:
    return Path(root) / _LOCK_NAME


def _owner(mode: LockMode) -> LockOwner:
    return LockOwner(
        os.getpid(),
        socket.gethostname()[:64],
        mode,
        datetime.now(timezone.utc).isoformat(),
        Path(sys.argv[0]).name[:128],
    )


def _acquire(root: Path, mode: LockMode, timeout: float) -> Any:
    root = Path(root)
    if classify_artifact_nofollow(root) not in ("directory", "absent"):
        raise StorageLockError("E_LOCK_ENTRY_UNSAFE")
    root.mkdir(parents=True, exist_ok=True)
    path = _lock_path(root)
    if classify_artifact_nofollow(path) in ("symlink", "special"):
        raise StorageLockError("E_LOCK_ENTRY_UNSAFE")
    try:
        handle = open(
            path,
            "a+b",
            opener=lambda name, flags: os.open(
                name, flags | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600
            ),
        )
    except PermissionError as exc:
        raise StorageLockError("E_LOCK_PERMISSION") from exc
    flag = fcntl.LOCK_SH if mode == "shared" else fcntl.LOCK_EX
    deadline = time.monotonic() + timeout
    while True:
        try:
            fcntl.flock(handle.fileno(), flag | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            if time.monotonic() >= deadline:
                handle.close()
                raise StorageLockError("E_LOCK_TIMEOUT") from None
            time.sleep(_POLL_INTERVAL_SECONDS)
    if mode == "exclusive":
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps(_owner(mode).__dict__, sort_keys=True).encode())
        handle.flush()
        os.fsync(handle.fileno())
    return handle


class RuntimeStorageLock:
    """Shared lock held for the whole native provider / standalone lifespan."""

    def __init__(self, root: Path, handle: Any) -> None:
        self.root = Path(root)
        self._fh = handle

    @classmethod
    def acquire(cls, root: Path, *, timeout: float = _DEFAULT_TIMEOUT) -> "RuntimeStorageLock":
        return cls(root, _acquire(root, "shared", timeout))

    def release(self) -> None:
        if self._fh:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            self._fh.close()
            self._fh = None


class MaintenanceStorageLocks:
    """Exclusive locks for every involved root, acquired in canonical order."""

    def __init__(self, handles: list[Any]) -> None:
        self._handles = handles

    @classmethod
    def acquire(
        cls, roots: Iterable[Path], *, timeout: float = _DEFAULT_TIMEOUT
    ) -> "MaintenanceStorageLocks":
        handles: list[Any] = []
        try:
            for root in sorted(
                {Path(r).absolute() for r in roots}, key=lambda p: os.fsencode(str(p))
            ):
                handles.append(_acquire(root, "exclusive", timeout))
            return cls(handles)
        except Exception:
            for handle in reversed(handles):
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    handle.close()
                except OSError:
                    pass
            raise

    def release(self) -> None:
        for handle in reversed(self._handles):
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
        self._handles = []


def inspect_existing_lock_readonly(root: Path) -> LockInspection:
    """Read-only inspection of an existing regular lock; never creates one."""
    path = _lock_path(Path(root))
    kind = classify_artifact_nofollow(path)
    if kind == "absent":
        return LockInspection("available")
    if kind != "regular_file":
        return LockInspection("unknown", error="unsafe lock entry")
    try:
        with open(path, "rb") as handle:
            raw = handle.read(4096)
        owner = LockOwner(**json.loads(raw)) if raw else None
        return LockInspection("held" if owner else "unknown", owner)
    except (OSError, ValueError, TypeError):
        return LockInspection("unknown", error="unreadable lock metadata")
