"""Bounded control-plane trace storage; never opens a memory database."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from .store import ControlError, _private_directory


class TracesStore:
    def __init__(self, path: Path, *, ttl_seconds: int, max_per_profile: int, max_payload_bytes: int, clock=None):
        self.path = Path(path).absolute()
        if self.path.name != "traces.sqlite3":
            self.path /= "traces.sqlite3"
        self.ttl_seconds = ttl_seconds
        self.max_per_profile = max_per_profile
        self.max_payload_bytes = max_payload_bytes
        self.clock = clock or (lambda: datetime.now(UTC))
        self._lock = threading.Lock()
        self._directory_fd = None
        try:
            self._directory_fd = _private_directory(self.path.parent)
            with self._connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS schema_meta(k TEXT PRIMARY KEY,v INTEGER NOT NULL)")
                row = conn.execute("SELECT v FROM schema_meta WHERE k='version'").fetchone()
                if row is not None and row[0] != 1:
                    raise ControlError("E_STORAGE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS traces(id TEXT PRIMARY KEY,profile_id TEXT NOT NULL,"
                    "request_id TEXT NOT NULL,operation TEXT NOT NULL,created_at TEXT NOT NULL,"
                    "expires_at TEXT NOT NULL,settings_digest TEXT NOT NULL,code_revision TEXT NOT NULL,"
                    "body_json BLOB NOT NULL,body_bytes INTEGER NOT NULL)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS traces_profile_created ON traces(profile_id,created_at DESC,id DESC)"
                )
                conn.execute("INSERT OR IGNORE INTO schema_meta VALUES('version',1)")
                conn.commit()
        except (OSError, sqlite3.Error, ControlError):
            self.close()
            raise ControlError("E_STORAGE") from None

    @staticmethod
    def _safe(info):
        if not (
            stat.S_ISREG(info.st_mode)
            and info.st_nlink == 1
            and info.st_uid == os.geteuid()
            and stat.S_IMODE(info.st_mode) == 0o600
        ):
            raise ControlError("E_STORAGE")

    def _check_sidecars(self):
        for suffix in ("", "-journal", "-wal", "-shm"):
            try:
                info = os.stat(self.path.name + suffix, dir_fd=self._directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            self._safe(info)

    def _identity(self, pinned):
        directory = os.stat(self.path.parent, follow_symlinks=False)
        held = os.fstat(self._directory_fd)
        current = os.stat(self.path.name, dir_fd=self._directory_fd, follow_symlinks=False)
        self._safe(current)
        if (directory.st_dev, directory.st_ino) != (held.st_dev, held.st_ino) or (current.st_dev, current.st_ino) != (
            pinned.st_dev,
            pinned.st_ino,
        ):
            raise ControlError("E_STORAGE")

    @contextmanager
    def _connection(self):
        # Every connection is created, used and closed on its calling thread.
        # The mutex also bounds concurrent to_thread readers and timed-out writers.
        with self._lock:
            fd = conn = None
            try:
                if self._directory_fd is None:
                    raise ControlError("E_STORAGE")
                self._check_sidecars()
                fd = os.open(
                    self.path.name,
                    os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
                    0o600,
                    dir_fd=self._directory_fd,
                )
                pinned = os.fstat(fd)
                self._safe(pinned)
                self._identity(pinned)
                # sqlite3 cannot adopt an fd. The private 0700 directory is the
                # pathname boundary; verify its identity before and after connect.
                conn = sqlite3.connect(self.path, timeout=0.25)
                self._identity(pinned)
                conn.execute("PRAGMA busy_timeout=250")
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=FULL")
                conn.execute("PRAGMA foreign_keys=ON")
                yield conn
            except (sqlite3.Error, OSError):
                raise ControlError("E_STORAGE") from None
            finally:
                if conn is not None:
                    conn.close()
                if fd is not None:
                    os.close(fd)

    def close(self):
        with self._lock:
            if self._directory_fd is not None:
                os.close(self._directory_fd)
                self._directory_fd = None

    def __del__(self):
        if getattr(self, "_directory_fd", None) is not None:
            os.close(self._directory_fd)

    @staticmethod
    def _dt(value):
        return value.astimezone(UTC).isoformat()

    def purge_expired(self, *, now):
        with self._connection() as conn:
            count = conn.execute("DELETE FROM traces WHERE expires_at<=?", (self._dt(now),)).rowcount
            conn.commit()
            return count

    def insert(self, trace: dict, *, now):
        body = json.dumps(trace, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        if len(body) > self.max_payload_bytes:
            raise ControlError("E_VALIDATION")
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM traces WHERE expires_at<=?", (self._dt(now),))
            conn.execute(
                "DELETE FROM traces WHERE profile_id=? AND id NOT IN (SELECT id FROM traces WHERE profile_id=? "
                "ORDER BY created_at DESC,id DESC LIMIT ?)",
                (trace["profile_id"], trace["profile_id"], self.max_per_profile - 1),
            )
            conn.execute(
                "INSERT INTO traces VALUES(?,?,?,?,?,?,?,?,?,?)",
                tuple(
                    self._dt(datetime.fromisoformat(trace[k])) if k in ("created_at", "expires_at") else trace[k]
                    for k in (
                        "id",
                        "profile_id",
                        "request_id",
                        "operation",
                        "created_at",
                        "expires_at",
                        "settings_digest",
                        "code_revision",
                    )
                )
                + (body, len(body)),
            )
            conn.commit()
            return trace["id"]

    def get(self, profile_id, trace_id, *, now):
        with self._connection() as conn:
            conn.execute("DELETE FROM traces WHERE expires_at<=?", (self._dt(now),))
            conn.commit()
            row = conn.execute(
                "SELECT body_json FROM traces WHERE profile_id=? AND id=?", (profile_id, trace_id)
            ).fetchone()
            return None if row is None else json.loads(row[0])

    def page(self, profile_id, *, limit, cursor, now):
        with self._connection() as conn:
            conn.execute("DELETE FROM traces WHERE expires_at<=?", (self._dt(now),))
            conn.commit()
            if cursor is None:
                rows = conn.execute(
                    "SELECT body_json FROM traces WHERE profile_id=? ORDER BY created_at DESC,id DESC LIMIT ?",
                    (profile_id, limit + 1),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT body_json FROM traces WHERE profile_id=? AND (created_at,id) < (?,?) "
                    "ORDER BY created_at DESC,id DESC LIMIT ?",
                    (profile_id, cursor[0], cursor[1], limit + 1),
                ).fetchall()
            return [json.loads(row[0]) for row in rows]
