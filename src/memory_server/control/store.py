"""Control-plane persistence only; never opens a profile's memory database.

The private directory is the SQLite pathname security boundary. O_NOFOLLOW pins
and checks the database before SQLite opens it; identity is checked again before
schema access. Python sqlite3 cannot set SQLITE_OPEN_NOFOLLOW: a hostile process
with the same uid can still race that open (see S02 report). No runtime imports.
"""

from __future__ import annotations

import os
import sqlite3
import stat
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from types import MappingProxyType

from memory_server.gateway_contracts.dto import ErrorCode
from memory_server.gateway_contracts.ports import ProfileRecord

_MESSAGES: dict[ErrorCode, str] = {
    "E_VALIDATION": "Invalid control input",
    "E_STORAGE": "Control storage unavailable",
    "E_PERMISSION_DENIED": "Profile access denied",
    "E_PROFILE_NOT_FOUND": "Profile not found",
    "E_PROFILE_DISABLED": "Profile disabled",
    "E_VERSION_CONFLICT": "Profile generation changed",
    "E_PRECONDITION_REQUIRED": "Expected generation required",
}


class ControlError(Exception):
    """Transport maps the C5 code; neither exception text nor paths are exposed."""

    def __init__(self, code: ErrorCode) -> None:
        self.code = code
        super().__init__(_MESSAGES[code])


def _private_directory(root: Path) -> int:
    if not root.is_absolute() or ".." in root.parts or root == Path("/"):
        raise ControlError("E_STORAGE")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        for index, part in enumerate(root.parts[1:], start=1):
            try:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
            except FileNotFoundError:
                # Only the explicit final control directory may be created.
                if index != len(root.parts) - 1:
                    raise
                try:
                    os.mkdir(part, mode=0o700, dir_fd=fd)
                except FileExistsError:
                    pass
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
            os.close(fd)
            fd = child
        info = os.fstat(fd)
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise ControlError("E_STORAGE")
        return fd
    except BaseException:
        os.close(fd)
        raise


class ControlStore:
    """One explicit private root, one SQLite connection; local bootstrap only.

    Registry configuration is seeded privately by ProfileRegistry. There is no
    public registration/root mutation API. Row changes and the control journal
    share BEGIN IMMEDIATE / COMMIT. HTTP idempotency/auth/audit is S06, not here.
    """

    def __init__(self, control_root: Path) -> None:
        self.root = Path(control_root)
        self._connection: sqlite3.Connection | None = None
        self._directory_fd: int | None = None
        self._checkpoint: Callable[[str], None] = lambda phase: None
        db_fd = None
        try:
            self._directory_fd = _private_directory(self.root)
            for name in ("control.sqlite", "control.sqlite-journal", "control.sqlite-wal", "control.sqlite-shm"):
                try:
                    info = os.stat(name, dir_fd=self._directory_fd, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_nlink != 1
                    or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o600
                ):
                    raise ControlError("E_STORAGE")
            db_fd = os.open(
                "control.sqlite",
                os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600,
                dir_fd=self._directory_fd,
            )
            pinned = os.fstat(db_fd)
            if not stat.S_ISREG(pinned.st_mode) or pinned.st_nlink != 1:
                raise ControlError("E_STORAGE")
            # Pin the directory for SQLite and all its sidecars, not ambient cwd.
            filename = f"/proc/self/fd/{self._directory_fd}/control.sqlite"
            self._connection = sqlite3.connect(filename, isolation_level=None, timeout=5)
            current = os.stat("control.sqlite", dir_fd=self._directory_fd, follow_symlinks=False)
            if (pinned.st_dev, pinned.st_ino, pinned.st_nlink) != (current.st_dev, current.st_ino, current.st_nlink):
                raise ControlError("E_STORAGE")
            self._connection.row_factory = sqlite3.Row
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._connection.execute("PRAGMA synchronous=FULL")
            with self._transaction():
                version = self._connection.execute("PRAGMA user_version").fetchone()[0]
                if version not in (0, 1):
                    raise ControlError("E_STORAGE")
                self._connection.execute("""CREATE TABLE IF NOT EXISTS registry_state (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    generation INTEGER NOT NULL CHECK(generation>=1))""")
                self._connection.execute("INSERT OR IGNORE INTO registry_state VALUES (1, 1)")
                self._connection.execute("""CREATE TABLE IF NOT EXISTS profiles (
                    id TEXT PRIMARY KEY, canonical_root TEXT NOT NULL UNIQUE,
                    enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
                    desired_state TEXT NOT NULL CHECK(desired_state IN ('enabled','disabled')),
                    display_name TEXT NOT NULL, description TEXT,
                    generation INTEGER NOT NULL CHECK(generation>=1))""")
                self._connection.execute("""CREATE TABLE IF NOT EXISTS control_journal (
                    generation INTEGER PRIMARY KEY, profile_id TEXT NOT NULL REFERENCES profiles(id),
                    operation TEXT NOT NULL CHECK(operation IN ('enabled','metadata')),
                    desired_state TEXT NOT NULL CHECK(desired_state IN ('enabled','disabled')),
                    enabled INTEGER NOT NULL CHECK(enabled IN (0,1)), display_name TEXT NOT NULL, description TEXT)""")
                self._connection.execute("PRAGMA user_version=1")
        except (OSError, sqlite3.Error, ValueError):
            self.close()
            raise ControlError("E_STORAGE") from None
        except BaseException:
            self.close()
            raise
        finally:
            if db_fd is not None:
                os.close(db_fd)

    def __enter__(self) -> ControlStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        if self._directory_fd is not None:
            os.close(self._directory_fd)
            self._directory_fd = None

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        if self._connection is None:
            raise ControlError("E_STORAGE")
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield
                self._connection.execute("COMMIT")
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise
        except sqlite3.Error:
            raise ControlError("E_STORAGE") from None

    def _configure(self, records: tuple[ProfileRecord, ...]) -> None:
        """Construction-only seeding; existing roots can never be retargeted."""
        with self._transaction():
            existing = {row["id"]: row for row in self._connection.execute("SELECT * FROM profiles")}
            configured = {record.id: record for record in records}
            if existing and (
                set(existing) != set(configured)
                or any(existing[key]["canonical_root"] != str(configured[key].canonical_root) for key in existing)
            ):
                raise ControlError("E_VALIDATION")
            for record in records:
                self._connection.execute(
                    "INSERT OR IGNORE INTO profiles VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        record.id,
                        str(record.canonical_root),
                        int(record.enabled),
                        "enabled" if record.enabled else "disabled",
                        record.display_name,
                        record.description,
                        1,
                    ),
                )

    @staticmethod
    def _record(row: sqlite3.Row) -> ProfileRecord:
        return ProfileRecord(
            row["id"],
            Path(row["canonical_root"]),
            bool(row["enabled"]),
            row["display_name"],
            row["description"],
            MappingProxyType({}),
            row["generation"],
        )

    def _get(self, profile_id: str) -> ProfileRecord:
        if self._connection is None:
            raise ControlError("E_STORAGE")
        try:
            row = self._connection.execute("SELECT * FROM profiles WHERE id=?", (profile_id,)).fetchone()
        except sqlite3.Error:
            raise ControlError("E_STORAGE") from None
        if row is None:
            raise ControlError("E_PROFILE_NOT_FOUND")
        return self._record(row)

    def _mutate(
        self,
        profile_id: str,
        *,
        expected_generation: int,
        operation: str,
        enabled: bool | None = None,
        display_name: str | None = None,
        description: str | None = None,
    ) -> ProfileRecord:
        connection = self._connection
        if connection is None:
            raise ControlError("E_STORAGE")
        with self._transaction():
            before = self._get(profile_id)
            if before.generation != expected_generation:
                raise ControlError("E_VERSION_CONFLICT")
            generation = self._connection.execute(
                "UPDATE registry_state SET generation=generation+1 WHERE singleton=1 RETURNING generation"
            ).fetchone()[0]
            flag = before.enabled if enabled is None else enabled
            name = before.display_name if operation == "enabled" else display_name
            desc = before.description if operation == "enabled" else description
            desired = "enabled" if flag else "disabled"
            cursor = connection.execute(
                "UPDATE profiles SET enabled=?,desired_state=?,display_name=?,description=?,generation=?"
                " WHERE id=? AND generation=?",
                (int(flag), desired, name, desc, generation, profile_id, expected_generation),
            )
            if cursor.rowcount != 1:
                raise ControlError("E_VERSION_CONFLICT")
            self._checkpoint("after_row")
            self._connection.execute(
                "INSERT INTO control_journal VALUES (?, ?, ?, ?, ?, ?, ?)",
                (generation, profile_id, operation, desired, int(flag), name, desc),
            )
            self._checkpoint("after_journal")
            result = self._get(profile_id)
        self._checkpoint("after_commit")
        return result

    def journal(self, *, profile_ids: Sequence[str]) -> tuple[MappingProxyType, ...]:
        """Per-profile control journal rows, scoped exactly like ``resolve``/``list``.

        Internal diagnostics only: not a wire endpoint and not the per-profile memory
        request journal. The caller must pass the profiles it is authorised for, so this
        cannot become an unfiltered dump if a later stage mounts it.
        """
        if self._connection is None:
            raise ControlError("E_STORAGE")
        ids = tuple(dict.fromkeys(profile_ids))
        if not ids:
            return ()
        placeholders = ",".join("?" for _ in ids)
        try:
            return tuple(
                MappingProxyType(dict(row))
                for row in self._connection.execute(
                    f"SELECT * FROM control_journal WHERE profile_id IN ({placeholders})"
                    " ORDER BY generation",
                    ids,
                )
            )
        except sqlite3.Error:
            raise ControlError("E_STORAGE") from None
