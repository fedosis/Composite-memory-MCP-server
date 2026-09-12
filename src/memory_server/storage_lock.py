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
from typing import Any, Iterable, Iterator, Literal, Mapping

try:
    import fcntl
except ImportError:  # No silent exclusive-only fallback on unsupported platforms.
    fcntl = None

LockMode = Literal["shared", "exclusive"]
_LOCK_NAME = ".cmms-storage.lock"
_DEFAULT_TIMEOUT = 5.0
DEFAULT_LOCK_TIMEOUT = 5.0
_POLL_INTERVAL_SECONDS = 0.02
_MAX_METADATA = 4096
_LOCKING_SUPPORTED = hasattr(fcntl, "flock") and os.name == "posix"

# S2-04: writer-state inventory bounds and classes. ``PROC_ROOT_DEFAULT`` is the
# only platform inventory source; the classes below are the stable labels every
# caller (and the maintenance preconditions report) must be able to read.
PROC_ROOT_DEFAULT = "/proc"
_DELETED_LABEL_SUFFIX = " (deleted)"
HANDLE_CLASS_LOCK_HOLDER = "upgraded_lock_holder"
HANDLE_CLASS_UPGRADED_WRITER = "upgraded_writer"
HANDLE_CLASS_LEGACY_WRITER = "legacy_writer"
_LOCK_TABLE_NAME = "locks"
_MAX_PID_ENTRIES = 1 << 20


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


def _open_lock(root_fd: int, name: str = _LOCK_NAME) -> int:
    try:
        try:
            before = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
        except FileNotFoundError:
            before = None
        if before is not None:
            if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
                raise StorageLockError("E_LOCK_ENTRY_UNSAFE")
            if before.st_nlink != 1:
                raise StorageLockError("E_LOCK_ENTRY_UNSAFE")
        fd = os.open(
            name,
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


def _acquire(root: Path, mode: LockMode, timeout: float, *, entry_name: str = _LOCK_NAME) -> int:
    if not _LOCKING_SUPPORTED:
        raise StorageLockError("E_LOCK_UNSUPPORTED")
    handle: int | None = None
    try:
        try:
            root_context = open_directory_nofollow(root, create=True)
            with root_context as root_fd:
                handle = _open_lock(root_fd, entry_name)
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
                current = os.stat(entry_name, dir_fd=root_fd, follow_symlinks=False)
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
    """Exclusive locks acquired in canonical lexical-byte order (S1, extended S2-04).

    Lifetime ownership: this object is the sole owner of every descriptor it
    acquired and keeps holding it until :meth:`release`.  A release is refused
    while a mutation-critical section is open (``E_LOCK_RELEASE_UNSAFE``), the
    lock entry is NEVER unlinked or replaced -- an unlink would hand the next
    acquirer a different inode and silently split the lock -- so the protected
    resource stays protected for the whole lifetime and after it.  ``release``
    is idempotent; a released object owns nothing.

    ``graph_lock_path`` additionally locks the applicable graph lock entry
    (DETAIL 3.5/6.3 step 9) through the same no-follow, identity-verified open
    and the same "never replace the inode" rule.
    """

    def __init__(
        self,
        handles: list[int],
        *,
        roots: tuple[Path, ...] = (),
        graph_path: Path | None = None,
        graph_handle: int | None = None,
        graph_created: bool = False,
    ) -> None:
        self._handles = list(handles)
        self._graph_handle = graph_handle
        self.roots = roots
        self.graph_lock_path = graph_path
        self.graph_lock_created = graph_created
        self._critical_sections = 0
        self._released = False

    @classmethod
    def acquire(
        cls,
        roots: Iterable[Path],
        *,
        timeout: float = _DEFAULT_TIMEOUT,
        graph_lock_path: Path | None = None,
    ) -> "MaintenanceStorageLocks":
        canonical = sorted({_lexical(Path(root)) for root in roots}, key=lambda path: os.fsencode(str(path)))
        handles: list[int] = []
        graph_handle: int | None = None
        graph_path: Path | None = None
        graph_created = False
        try:
            for root in canonical:
                handles.append(_acquire(root, "exclusive", timeout))
            if graph_lock_path is not None:
                graph_path = _lexical(Path(graph_lock_path))
                graph_created = not os.path.lexists(graph_path)
                graph_handle = _acquire(
                    graph_path.parent, "exclusive", timeout, entry_name=graph_path.name
                )
            return cls(
                handles,
                roots=tuple(canonical),
                graph_path=graph_path,
                graph_handle=graph_handle,
                graph_created=graph_created,
            )
        except Exception:
            for handle in reversed(handles):
                try:
                    fcntl.flock(handle, fcntl.LOCK_UN)
                finally:
                    os.close(handle)
            if graph_handle is not None:
                try:
                    fcntl.flock(graph_handle, fcntl.LOCK_UN)
                finally:
                    os.close(graph_handle)
            raise

    @property
    def graph_lock_identity(self) -> os.stat_result | None:
        """The identity of the exact graph-lock entry this object holds open.

        DETAIL 10.1: the held inode is never replaced, and a graph lock CREATED
        by this run must be removable after unlock. The identity is taken from
        the OWNED descriptor, so a path swap after acquisition cannot redirect
        the record that post-unlock cleanup acts on, and it is ``None`` when no
        graph lock is held at all.
        """
        if self._graph_handle is None:
            return None
        return os.fstat(self._graph_handle)

    @property
    def held(self) -> bool:
        return bool(self._handles) or self._graph_handle is not None

    @property
    def released(self) -> bool:
        return self._released

    def begin_critical_section(self) -> None:
        """Enter the mutation-critical section this lock protects."""
        if not self.held or self._released:
            raise StorageLockError("E_LOCK_RELEASE_UNSAFE", "no lock is held")
        self._critical_sections += 1

    def end_critical_section(self) -> None:
        if self._critical_sections == 0:
            raise StorageLockError("E_LOCK_RELEASE_UNSAFE", "no critical section is open")
        self._critical_sections -= 1

    def release(self) -> None:
        """Release only when it is safe to release, then never unlink.

        Refused while a critical section is open: a mutation that is still in
        progress must not lose its protection.  Idempotent afterwards.
        """
        if self._released:
            return
        if self._critical_sections:
            raise StorageLockError(
                "E_LOCK_RELEASE_UNSAFE", "a mutation-critical section is still open"
            )
        for handle in reversed(self._handles):
            try:
                fcntl.flock(handle, fcntl.LOCK_UN)
            finally:
                os.close(handle)
        self._handles = []
        if self._graph_handle is not None:
            try:
                fcntl.flock(self._graph_handle, fcntl.LOCK_UN)
            finally:
                os.close(self._graph_handle)
            self._graph_handle = None
        self._released = True

    def __enter__(self) -> "MaintenanceStorageLocks":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> Literal[False]:
        self._critical_sections = 0
        self.release()
        return False


def remove_created_lock_entry(record: Mapping[str, Any]) -> bool:
    """Remove exactly the lock entry a recorded run created, or refuse.

    DETAIL 10.1 post-unlock cleanup. Cleanup may only take back what the run
    itself created, so the record must say ``created`` and must carry the
    ``(device, inode)`` the lock owner actually held open. The recorded identity
    is compared against the entry through a pinned no-follow parent descriptor
    and the entry is unlinked only when BOTH match: a replaced, re-created or
    foreign entry is ``E_ARTIFACT_IDENTITY_CHANGED`` and stays exactly where it
    is, because deleting the lock the next writer holds would split the lock, not
    release it. An entry that is already gone reports ``False`` (nothing was
    removed) instead of raising, and a record that does not claim creation is
    ``E_LOCK_RELEASE_UNSAFE``.
    """
    if not isinstance(record, Mapping):
        raise StorageLockError("E_LOCK_RELEASE_UNSAFE", "the cleanup record is not a mapping")
    if not record.get("created"):
        raise StorageLockError("E_LOCK_RELEASE_UNSAFE", "this run did not create the lock entry")
    path = _lexical(Path(str(record.get("path") or "")))
    device = record.get("device")
    inode = record.get("inode")
    if not path.name or not isinstance(device, int) or not isinstance(inode, int):
        raise StorageLockError("E_LOCK_RELEASE_UNSAFE", "the cleanup record carries no lock entry identity")
    with open_directory_nofollow(path.parent) as parent_fd:
        try:
            info = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise _raise_oserror(exc) from exc
        if (info.st_dev, info.st_ino) != (device, inode):
            raise StorageLockError(
                "E_ARTIFACT_IDENTITY_CHANGED", "the lock entry is not the one this run created"
            )
        os.unlink(path.name, dir_fd=parent_fd)
    return True


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


# ---------------------------------------------------------------------------
# S2-04 writer-state inventory
#
# A root lock alone cannot exclude a writer that predates the lock protocol, so
# maintenance additionally inventories every OTHER process's open descriptors
# through ``/proc/<pid>/fd`` and every advisory lock in ``/proc/locks``. Every
# descriptor is matched by its RAW ``readlink`` string -- the label the kernel
# reports for exactly that entry -- and is never resolved, so a legacy symlink
# referent is matched as the link entry itself and a dangling or hostile
# referent can never be followed. Coverage is fail-closed: any process that
# could hold one of the resources in a namespace that resolves them and whose
# descriptor table cannot be read makes the whole answer
# ``E_WRITER_STATE_UNKNOWN``; a missing inventory source is
# ``E_WRITER_INVENTORY_UNSUPPORTED``. Nothing here ever signals a process.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HandleRecord:
    pid: int
    fd: int
    label_key: str
    raw_label: str
    classification: str


@dataclass(frozen=True)
class LockRecord:
    pid: int
    kind: str
    mode: str
    device: str
    inode: str

    @property
    def key(self) -> str:
        return f"{self.device}:{self.inode}"


@dataclass(frozen=True)
class WriterInventory:
    covered: bool
    code: str | None
    records: tuple[HandleRecord, ...]
    lock_records: tuple[LockRecord, ...]
    scanned_pids: int
    excluded_foreign_namespace: int
    excluded_foreign_credential: int
    gaps: tuple[str, ...]

    @property
    def upgraded_holders(self) -> tuple[HandleRecord, ...]:
        return tuple(record for record in self.records if record.classification == HANDLE_CLASS_LOCK_HOLDER)

    @property
    def upgraded_writers(self) -> tuple[HandleRecord, ...]:
        return tuple(record for record in self.records if record.classification == HANDLE_CLASS_UPGRADED_WRITER)

    @property
    def legacy_writers(self) -> tuple[HandleRecord, ...]:
        return tuple(record for record in self.records if record.classification == HANDLE_CLASS_LEGACY_WRITER)

    @property
    def record_pids(self) -> tuple[int, ...]:
        return tuple(sorted({record.pid for record in self.records}))


def inode_key(info: os.stat_result) -> str:
    """The ``major:minor:inode`` key ``/proc/locks`` prints for this entry."""
    return "%02x:%02x:%d" % (os.major(info.st_dev), os.minor(info.st_dev), info.st_ino)


def parse_lock_table(text: str) -> tuple[LockRecord, ...]:
    """Parse only well-formed ``/proc/locks`` rows; unparseable rows are dropped.

    A dropped row can never hide a writer because the caller additionally
    requires the fd inventory to be complete; this function is a second,
    independent signal.
    """
    records: list[LockRecord] = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 6 or not parts[0].endswith(":"):
            continue
        kind = parts[1].lower()
        mode = parts[3].lower()
        if kind not in ("flock", "posix") or mode not in ("read", "write"):
            continue
        coordinate = parts[5].split(":")
        if len(coordinate) != 3:
            continue
        try:
            pid = int(parts[4])
        except ValueError:
            continue
        records.append(LockRecord(pid, kind, mode, f"{coordinate[0]}:{coordinate[1]}", coordinate[2]))
    return tuple(records)


def read_lock_table(*, proc_root: str | Path = PROC_ROOT_DEFAULT) -> tuple[LockRecord, ...]:
    """Read the system-wide advisory lock table; unreadable is fail-closed."""
    try:
        text = Path(os.path.join(os.fspath(proc_root), _LOCK_TABLE_NAME)).read_text(encoding="utf-8")
    except OSError as exc:
        raise StorageLockError("E_WRITER_STATE_UNKNOWN", f"lock table is unreadable: {exc.strerror}") from exc
    return parse_lock_table(text)


def _own_mount_namespace() -> str | None:
    try:
        return os.readlink("/proc/self/ns/mnt")
    except OSError:
        return None


def _proc_mount_namespace(proc_root: Path, pid: str) -> str | None:
    try:
        return os.readlink(proc_root / pid / "ns" / "mnt")
    except OSError:
        return None


def _proc_uid(proc_root: Path, pid: str) -> int | None:
    try:
        status = (proc_root / pid / "status").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    for line in status.splitlines():
        if line.startswith("Uid:"):
            parts = line.split()
            if len(parts) > 1:
                try:
                    return int(parts[1])
                except ValueError:
                    return None
    return None


def _match_label(raw: str, labels: Mapping[str, str]) -> str | None:
    for key, label in labels.items():
        if raw == label or raw == f"{label}{_DELETED_LABEL_SUFFIX}":
            return key
    return None


def scan_writer_inventory(
    labels: Mapping[str, str],
    *,
    lock_label_keys: Iterable[str] = (),
    proc_root: str | Path = PROC_ROOT_DEFAULT,
    exclude_pids: Iterable[int] = (),
) -> WriterInventory:
    """Complete fail-closed inventory of other processes' matching descriptors.

    ``labels`` maps a stable label key to the RAW label string that must be
    matched byte-for-byte against ``readlink`` output; nothing is followed. A
    process that matches a lock label is an upgraded holder; one that matches a
    resource label while holding no lock label is an old (pre-upgrade) writer;
    one that matches a resource label AND a lock label is an upgraded runtime
    that is still live.
    """
    proc = Path(proc_root)
    excluded = {int(pid) for pid in exclude_pids}
    lock_keys = {str(key) for key in lock_label_keys}
    if not proc.is_dir():
        return WriterInventory(
            False,
            "E_WRITER_INVENTORY_UNSUPPORTED",
            (),
            (),
            0,
            0,
            0,
            (f"process inventory source {proc} is not a directory",),
        )
    own_namespace = _own_mount_namespace()
    own_pid = os.getpid()
    try:
        entries = sorted(os.listdir(proc))
    except OSError as exc:
        return WriterInventory(
            False,
            "E_WRITER_STATE_UNKNOWN",
            (),
            (),
            0,
            0,
            0,
            (f"process inventory source {proc} is unreadable: {exc.strerror}",),
        )
    if len(entries) > _MAX_PID_ENTRIES:
        return WriterInventory(
            False,
            "E_WRITER_STATE_UNKNOWN",
            (),
            (),
            0,
            0,
            0,
            (f"process inventory exceeds {_MAX_PID_ENTRIES} entries",),
        )

    found: list[HandleRecord] = []
    gaps: list[str] = []
    scanned = 0
    excluded_namespace = 0
    excluded_credential = 0
    counted: dict[str, str | None] = {}

    def exclusion_reason(entry: str) -> str | None:
        """Why this pid's descriptors are out of required coverage, if provably so.

        Required coverage is every process that resolves our artifacts in OUR
        mount namespace. A process in another mount namespace resolves labels we
        cannot compare (and its labels are not our paths), and a different-uid
        process cannot open 0600/0700 artifacts it does not own; both are
        recorded with counts instead of silently ignored. A gap in OUR namespace
        with potentially our credentials is never excused.
        """
        nonlocal excluded_namespace, excluded_credential
        if entry in counted:
            return counted[entry]
        reason: str | None
        if _proc_mount_namespace(proc, entry) != own_namespace:
            excluded_namespace += 1
            reason = "foreign_mount_namespace"
        else:
            uid = _proc_uid(proc, entry)
            if uid is not None and uid != os.getuid():
                excluded_credential += 1
                reason = "foreign_credential"
            else:
                reason = None
        counted[entry] = reason
        return reason

    for entry in entries:
        if not entry.isdigit():
            continue
        pid = int(entry)
        if pid == own_pid or pid in excluded:
            continue
        fd_dir = proc / entry / "fd"
        try:
            descriptors = sorted(os.listdir(fd_dir))
        except FileNotFoundError:
            continue
        except OSError as exc:
            if exclusion_reason(entry) is not None:
                continue
            gaps.append(
                f"descriptor table of pid {entry} is not readable while it resolves our"
                f" namespace with our credentials: {exc.strerror}"
            )
            continue
        scanned += 1
        for descriptor in descriptors:
            if not descriptor.isdigit():
                continue
            try:
                raw = os.readlink(fd_dir / descriptor)
            except OSError as exc:
                if exclusion_reason(entry) is not None:
                    continue
                gaps.append(f"descriptor {descriptor} of pid {entry} could not be read: {exc.strerror}")
                continue
            key = _match_label(raw, labels)
            if key is None:
                continue
            found.append(HandleRecord(pid, int(descriptor), key, raw, ""))

    try:
        lock_records = read_lock_table(proc_root=proc)
    except StorageLockError as exc:
        lock_records = ()
        gaps.append(str(exc))

    locked_pids = {record.pid for record in found if record.label_key in lock_keys}
    classified = tuple(
        HandleRecord(
            record.pid,
            record.fd,
            record.label_key,
            record.raw_label,
            HANDLE_CLASS_LOCK_HOLDER
            if record.label_key in lock_keys
            else (HANDLE_CLASS_UPGRADED_WRITER if record.pid in locked_pids else HANDLE_CLASS_LEGACY_WRITER),
        )
        for record in found
    )
    covered = not gaps
    code = None
    if not covered:
        code = "E_WRITER_STATE_UNKNOWN"
    return WriterInventory(
        covered,
        code,
        classified,
        lock_records,
        scanned,
        excluded_namespace,
        excluded_credential,
        tuple(gaps),
    )
