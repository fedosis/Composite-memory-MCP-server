"""Cross-process shared/exclusive root locks and no-follow filesystem primitives."""

from __future__ import annotations

import contextlib
import errno
import json
import os
import socket
import stat
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Literal

try:
    import fcntl
except ImportError:  # No silent exclusive-only fallback on unsupported platforms.
    fcntl = None

LockMode = Literal["shared", "exclusive"]
_LOCK_NAME = ".cmms-storage.lock"
_DEFAULT_TIMEOUT = 5.0
_POLL_INTERVAL_SECONDS = 0.02
_MAX_METADATA = 4096
_LOCKING_SUPPORTED = hasattr(fcntl, "flock") and os.name == "posix"


@dataclass(frozen=True)
class LockOwner:
    pid: int
    host: str
    mode: LockMode
    started_at: str
    command: str


class StorageLockError(RuntimeError):
    """Fail-closed storage error carrying a stable diagnostic code."""

    def __init__(self, code: str, message: str = "storage lock failure") -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class LockInspection:
    availability: str
    owner: LockOwner | None = None
    error: str | None = None


def _lexical(path: Path) -> Path:
    value = os.path.expanduser(os.fspath(path))
    if not value or "\x00" in value:
        raise StorageLockError("E_PATH_INVALID")
    return Path(os.path.abspath(os.path.normpath(value)))


def _raise_oserror(exc: OSError, *, directory: bool = False) -> StorageLockError:
    if isinstance(exc, PermissionError) or exc.errno in (errno.EACCES, errno.EPERM):
        return StorageLockError("E_LOCK_PERMISSION" if not directory else "E_PATH_PERMISSION")
    if exc.errno in (errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOTSUP):
        return StorageLockError("E_LOCK_UNSUPPORTED")
    if exc.errno == errno.ENOENT:
        return StorageLockError("E_PATH_ABSENT")
    return StorageLockError("E_PATH_UNSAFE")


def _same_identity(first: os.stat_result, second: os.stat_result) -> bool:
    return (
        first.st_dev == second.st_dev
        and first.st_ino == second.st_ino
        and stat.S_IFMT(first.st_mode) == stat.S_IFMT(second.st_mode)
    )


@contextlib.contextmanager
def open_directory_nofollow(path: Path, *, create: bool = False) -> Iterator[int]:
    """Open every component of *path* with directory/no-follow guarantees.

    Missing components are created only when ``create=True``.  Each opened
    component is checked against an immediately preceding no-follow ``lstat``;
    callers receive an owned descriptor and never a pathname that can race.
    """
    candidate = _lexical(Path(path))
    fd: int | None = None
    try:
        fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        for component in candidate.parts[1:]:
            try:
                before = os.stat(component, dir_fd=fd, follow_symlinks=False)
            except OSError as exc:
                if exc.errno != errno.ENOENT or not create:
                    raise _raise_oserror(exc, directory=True) from exc
                try:
                    os.mkdir(component, mode=0o700, dir_fd=fd)
                except FileExistsError:
                    pass
                except OSError as create_exc:
                    raise _raise_oserror(create_exc, directory=True) from create_exc
                try:
                    before = os.stat(component, dir_fd=fd, follow_symlinks=False)
                except OSError as stat_exc:
                    raise _raise_oserror(stat_exc, directory=True) from stat_exc
            if stat.S_ISLNK(before.st_mode):
                raise StorageLockError("E_PATH_SYMLINK_PARENT")
            if not stat.S_ISDIR(before.st_mode):
                raise StorageLockError("E_PATH_SPECIAL_FILE")
            try:
                next_fd = os.open(
                    component,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=fd,
                )
            except OSError as exc:
                raise _raise_oserror(exc, directory=True) from exc
            try:
                after = os.fstat(next_fd)
                if not _same_identity(before, after):
                    raise StorageLockError("E_ARTIFACT_IDENTITY_CHANGED")
            except Exception:
                os.close(next_fd)
                raise
            os.close(fd)
            fd = next_fd
        if fd is None:
            raise StorageLockError("E_PATH_UNSAFE")
        yield fd
    except StorageLockError:
        raise
    except OSError as exc:
        raise _raise_oserror(exc, directory=True) from exc
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass


@contextlib.contextmanager
def open_file_nofollow(path: Path, *, flags: int = os.O_RDONLY, mode: int = 0o600) -> Iterator[int]:
    """Open a single regular file through its validated parent directory."""
    if flags & (os.O_TRUNC | os.O_CREAT):
        raise StorageLockError("E_PATH_UNSAFE", "create/truncate requires a separate verified write operation")
    candidate = _lexical(Path(path))
    parent, name = candidate.parent, candidate.name
    with open_directory_nofollow(parent) as parent_fd:
        try:
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError as exc:
            raise _raise_oserror(exc) from exc
        if stat.S_ISLNK(before.st_mode):
            raise StorageLockError("E_PATH_SYMLINK_FINAL")
        if not stat.S_ISREG(before.st_mode):
            raise StorageLockError("E_PATH_SPECIAL_FILE")
        if before.st_nlink != 1:
            raise StorageLockError("E_ARTIFACT_IDENTITY_CHANGED")
        try:
            fd = os.open(name, flags | os.O_NOFOLLOW | os.O_CLOEXEC, mode, dir_fd=parent_fd)
        except OSError as exc:
            raise _raise_oserror(exc) from exc
        try:
            after = os.fstat(fd)
            if not _same_identity(before, after) or after.st_nlink != 1:
                raise StorageLockError("E_ARTIFACT_IDENTITY_CHANGED")
            yield fd
        finally:
            os.close(fd)


def read_file_nofollow(path: Path, *, max_bytes: int = 65536) -> bytes:
    """Read at most ``max_bytes`` from a validated regular file."""
    if max_bytes < 0 or max_bytes > 16 * 1024 * 1024:
        raise StorageLockError("E_PATH_READ_BOUNDED")
    with open_file_nofollow(path) as fd:
        return os.read(fd, max_bytes)


# Descriptive aliases used by inventory callers.
open_regular_file_nofollow = open_file_nofollow
read_regular_file_nofollow = read_file_nofollow


def _lock_name() -> str:
    return _LOCK_NAME


def _owner(mode: LockMode) -> LockOwner:
    return LockOwner(
        os.getpid(),
        socket.gethostname()[:64],
        mode,
        datetime.now(timezone.utc).isoformat(),
        Path(sys.argv[0]).name[:128],
    )


def _open_lock(root_fd: int) -> int:
    try:
        try:
            before = os.stat(_LOCK_NAME, dir_fd=root_fd, follow_symlinks=False)
        except FileNotFoundError:
            before = None
        if before is not None:
            if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
                raise StorageLockError("E_LOCK_ENTRY_UNSAFE")
            if before.st_nlink != 1:
                raise StorageLockError("E_LOCK_ENTRY_UNSAFE")
        fd = os.open(
            _LOCK_NAME,
            os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=root_fd,
        )
        after = os.fstat(fd)
        if not stat.S_ISREG(after.st_mode) or after.st_nlink != 1:
            os.close(fd)
            raise StorageLockError("E_LOCK_ENTRY_UNSAFE")
        if before is not None and not _same_identity(before, after):
            os.close(fd)
            raise StorageLockError("E_ARTIFACT_IDENTITY_CHANGED")
        return fd
    except StorageLockError:
        raise
    except OSError as exc:
        raise _raise_oserror(exc) from exc


def _acquire(root: Path, mode: LockMode, timeout: float) -> int:
    if not _LOCKING_SUPPORTED:
        raise StorageLockError("E_LOCK_UNSUPPORTED")
    handle: int | None = None
    try:
        try:
            root_context = open_directory_nofollow(root, create=True)
            with root_context as root_fd:
                handle = _open_lock(root_fd)
                flag = fcntl.LOCK_SH if mode == "shared" else fcntl.LOCK_EX
                deadline = time.monotonic() + max(0.0, timeout)
                while True:
                    try:
                        fcntl.flock(handle, flag | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.monotonic() >= deadline:
                            raise StorageLockError("E_LOCK_TIMEOUT")
                        time.sleep(_POLL_INTERVAL_SECONDS)
                    except OSError as exc:
                        raise _raise_oserror(exc) from exc
                current = os.stat(_LOCK_NAME, dir_fd=root_fd, follow_symlinks=False)
                if not _same_identity(current, os.fstat(handle)) or current.st_nlink != 1:
                    raise StorageLockError("E_ARTIFACT_IDENTITY_CHANGED")
                if mode == "exclusive":
                    payload = json.dumps(_owner(mode).__dict__, sort_keys=True, separators=(",", ":")).encode()
                    if len(payload) > _MAX_METADATA:
                        raise StorageLockError("E_LOCK_METADATA_UNSAFE")
                    os.ftruncate(handle, 0)
                    os.write(handle, payload)
                    os.fsync(handle)
                owned = handle
                handle = None
                return owned
        except StorageLockError as exc:
            if exc.code == "E_PATH_PERMISSION":
                raise StorageLockError("E_LOCK_PERMISSION") from exc
            raise
    finally:
        if handle is not None:
            try:
                os.close(handle)
            except OSError:
                pass


class RuntimeStorageLock:
    """Shared lock held for the whole native provider/standalone lifespan."""

    def __init__(self, root: Path, handle: int) -> None:
        self.root = _lexical(Path(root))
        self._fh: int | None = handle

    @classmethod
    def acquire(cls, root: Path, *, timeout: float = _DEFAULT_TIMEOUT) -> "RuntimeStorageLock":
        return cls(root, _acquire(root, "shared", timeout))

    def release(self) -> None:
        if self._fh is not None:
            try:
                fcntl.flock(self._fh, fcntl.LOCK_UN)
            finally:
                os.close(self._fh)
                self._fh = None


class MaintenanceStorageLocks:
    """Exclusive locks acquired in canonical lexical-byte order."""

    def __init__(self, handles: list[int]) -> None:
        self._handles = handles

    @classmethod
    def acquire(cls, roots: Iterable[Path], *, timeout: float = _DEFAULT_TIMEOUT) -> "MaintenanceStorageLocks":
        canonical = sorted({_lexical(Path(root)) for root in roots}, key=lambda path: os.fsencode(str(path)))
        handles: list[int] = []
        try:
            for root in canonical:
                handles.append(_acquire(root, "exclusive", timeout))
            return cls(handles)
        except Exception:
            for handle in reversed(handles):
                try:
                    fcntl.flock(handle, fcntl.LOCK_UN)
                finally:
                    os.close(handle)
            raise

    def release(self) -> None:
        for handle in reversed(self._handles):
            try:
                fcntl.flock(handle, fcntl.LOCK_UN)
            finally:
                os.close(handle)
        self._handles = []


def inspect_existing_lock_readonly(root: Path) -> LockInspection:
    """Inspect without creating; metadata alone never implies a held lock."""
    if not _LOCKING_SUPPORTED:
        return LockInspection("unknown", error="E_LOCK_UNSUPPORTED")
    candidate = _lexical(Path(root))
    try:
        with open_directory_nofollow(candidate) as root_fd:
            try:
                before = os.stat(_LOCK_NAME, dir_fd=root_fd, follow_symlinks=False)
            except FileNotFoundError:
                return LockInspection("unknown")
            if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                return LockInspection("unknown", error="unsafe lock entry")
            try:
                fd = os.open(_LOCK_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=root_fd)
            except OSError:
                return LockInspection("unknown", error="unreadable lock entry")
            try:
                after = os.fstat(fd)
                if not _same_identity(before, after) or after.st_nlink != 1:
                    return LockInspection("unknown", error="lock identity changed")
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    return LockInspection("available")
                except BlockingIOError:
                    pass
                raw = os.read(fd, _MAX_METADATA + 1)
                if len(raw) > _MAX_METADATA:
                    return LockInspection("unknown", error="metadata too large")
                try:
                    values = json.loads(raw) if raw else None
                    owner = LockOwner(**values) if isinstance(values, dict) else None
                except (ValueError, TypeError, KeyError):
                    owner = None
                return LockInspection("held", owner)
            finally:
                os.close(fd)
    except StorageLockError as exc:
        if exc.code == "E_PATH_ABSENT":
            return LockInspection("unknown")
        return LockInspection("unknown", error=exc.code)
    except OSError:
        return LockInspection("unknown", error="lock inspection failed")
