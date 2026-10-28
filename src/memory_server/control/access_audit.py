"""Private append-only access log; no payloads, credentials or exception text.

Logical segments contain at most segment_rows rows; unexpired history is never
evicted. The authenticated default budgets one row / 10 s across the TTL:
30 * 86400 / 10 = 259200 rows; ceil(259200 / 10000) + 1 = 27 segments.
The spare segment covers partial-segment retention; retain must run at least
once per segment interval. Higher audited rates need explicit capacity planning.
SQLite max_page_count additionally bounds physical growth (default 256 MiB).
Pre-auth denials use a separate file/sequence/page budget (2000 rows, 4 MiB),
never authenticated slots. Saturation drops denial recording, not requests.
Exhaustion of authenticated storage is E_AUDIT_UNAVAILABLE.
S06 owns route integration, S12 owns the atomic canonical outcome journal.
"""

from __future__ import annotations

import math
import os
import re
import sqlite3
import stat
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from typing import get_args
from uuid import uuid4

from memory_server.gateway_contracts.dto import AccessAudit, ErrorCode, Operation
from memory_server.gateway_contracts.ports import AuditOutcome, AuditTicket, Principal, RequestContext

from .store import ControlError, _private_directory

_FIELDS = tuple(AccessAudit.model_fields)
_COLUMNS = ",".join(_FIELDS)
_REQUEST = re.compile(r"req_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
_TOKEN = re.compile(r"[0-9a-f]{32}\Z")
_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
_CODES = frozenset(get_args(ErrorCode))
_OPERATIONS = frozenset(get_args(Operation))
_CLOCK_ID = uuid4().hex  # comparable monotonic clocks only within this process


class AuditError(Exception):
    """Stable C5-only failure, never chains SQLite/path/content diagnostics."""

    def __init__(self, code: ErrorCode = "E_AUDIT_UNAVAILABLE") -> None:
        self.code = code
        super().__init__(code)


def _check(value: str, pattern: re.Pattern) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise AuditError("E_VALIDATION")
    return value


class AccessAuditStore:
    """AccessSink with durable intents, state CAS and injected wall/monotonic clocks.

    finish ignores AuditOutcome.latency_ms: C1 carries that legacy field, but T11
    makes this module's own monotonic measurement authoritative. A restarted
    intent has no comparable process clock; its reconciliation latency is zero.
    Duplicate completion raises E_VERSION_CONFLICT, including concurrent CAS.
    """

    def __init__(
        self,
        root: Path,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        ttl_seconds: float = 30 * 86400,
        segment_rows: int = 10000,
        max_segments: int | None = None,
        max_bytes: int = 256 * 1024 * 1024,
        denial_segment_rows: int = 1000,
        denial_max_segments: int = 2,
        _denial_only: bool = False,
    ) -> None:
        if (
            not math.isfinite(ttl_seconds) or ttl_seconds <= 0
            or type(segment_rows) is not int or segment_rows < 1
            or (max_segments is not None and (type(max_segments) is not int or max_segments < 1))
            or type(denial_segment_rows) is not int or denial_segment_rows < 1
            or type(denial_max_segments) is not int or denial_max_segments < 1
            or type(max_bytes) is not int or max_bytes < 65536
        ):
            raise AuditError("E_VALIDATION")
        self.clock, self.monotonic = clock, monotonic
        self.ttl_seconds = ttl_seconds
        self.segment_rows = segment_rows
        self.max_segments = max_segments if max_segments is not None else math.ceil(ttl_seconds / 10 / segment_rows) + 1
        self._lock = RLock()
        self._denials = None
        self.dropped_denials = 0  # local diagnostic counter; not a new C9 family
        self._denial_failed = False
        self.rollback_refusals = 0
        self._fd = None
        self._db = None
        try:
            self._fd = _private_directory(Path(root))
            for name in ("access.sqlite", "access.sqlite-journal", "access.sqlite-wal", "access.sqlite-shm",
                         "retention.watermark", "retention.next"):
                try:
                    info = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                        or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
                    raise AuditError()
            flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC
            pin = os.open("access.sqlite", flags, 0o600, dir_fd=self._fd)
            try:
                before = os.fstat(pin)
                self._db = sqlite3.connect(
                    f"/proc/self/fd/{self._fd}/access.sqlite", isolation_level=None,
                    timeout=5, check_same_thread=False,
                )
                after = os.stat("access.sqlite", dir_fd=self._fd, follow_symlinks=False)
                if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                    raise AuditError()
            finally:
                os.close(pin)
            self._db.row_factory = sqlite3.Row
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.execute("PRAGMA journal_mode=DELETE")
            page_size = self._db.execute("PRAGMA page_size").fetchone()[0]
            self._db.execute(f"PRAGMA max_page_count={max_bytes // page_size}")
            with self._transaction():
                self._db.execute("""CREATE TABLE IF NOT EXISTS access_audit (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE, timestamp TEXT NOT NULL,
                    request_id TEXT NOT NULL, token_id TEXT, client_id TEXT, profile_id TEXT,
                    operation TEXT NOT NULL, memory_id TEXT, result TEXT NOT NULL,
                    latency_ms REAL NOT NULL, remote_address TEXT,
                    state TEXT NOT NULL CHECK(state IN ('intent','terminal')),
                    created_at REAL NOT NULL, segment INTEGER NOT NULL,
                    started REAL NOT NULL, clock_id TEXT NOT NULL)""")
                self._db.execute("CREATE INDEX IF NOT EXISTS audit_scope ON access_audit(profile_id,seq)")
            os.fsync(self._fd)
            if not _denial_only:
                try:
                    self._denials = AccessAuditStore(
                        Path(root) / "denials", clock=clock, monotonic=monotonic, ttl_seconds=ttl_seconds,
                        segment_rows=denial_segment_rows, max_segments=denial_max_segments,
                        max_bytes=4 * 1024 * 1024, _denial_only=True,
                    )
                except AuditError:
                    self._denial_failed = True
        except (OSError, sqlite3.Error, ControlError):
            self.close()
            raise AuditError() from None
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        with self._lock:
            if self._denials is not None:
                self._denials.close()
                self._denials = None
            if self._db is not None:
                self._db.close()
                self._db = None
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        if self._db is None:
            raise AuditError()
        try:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        except sqlite3.Error:
            raise AuditError() from None

    def _now(self) -> datetime:
        value = self.clock()
        offset = value.utcoffset()
        if offset is None or offset.total_seconds() != 0:
            raise AuditError("E_VALIDATION")
        return value

    def _insert(self, request_id, token_id, client_id, profile_id, operation, result) -> AuditTicket:
        _check(request_id, _REQUEST)
        now = self._now()
        started = self.monotonic()
        if not math.isfinite(started):
            raise AuditError("E_VALIDATION")
        ticket = AuditTicket("aud_" + uuid4().hex, request_id)
        with self._lock, self._transaction():
            seq_row = self._db.execute("SELECT seq FROM sqlite_sequence WHERE name='access_audit'").fetchone()
            seq = (seq_row[0] if seq_row else 0) + 1
            segment = (seq - 1) // self.segment_rows
            first = self._db.execute("SELECT MIN(segment) FROM access_audit").fetchone()[0]
            if first is not None and segment - first >= self.max_segments:
                raise AuditError()
            created_at = max(now.timestamp(), self._watermark())
            self._db.execute(
                f"INSERT INTO access_audit ({_COLUMNS},state,created_at,segment,started,clock_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ticket.id, now.isoformat(), request_id, token_id, client_id, profile_id,
                 operation, None, result, 0.0, None, "intent", created_at, segment, started, _CLOCK_ID),
            )
        return ticket

    async def begin(self, ctx: RequestContext, operation: Operation) -> AuditTicket:
        if operation not in _OPERATIONS:
            raise AuditError("E_VALIDATION")
        _check(ctx.principal.token_id, _TOKEN)
        if ctx.principal.client_id is not None:
            _check(ctx.principal.client_id, _ID)
        if ctx.profile_id is not None:
            _check(ctx.profile_id, _ID)
            if ctx.profile_id not in ctx.principal.profiles:
                raise AuditError("E_PERMISSION_DENIED")
        return self._insert(ctx.request_id, ctx.principal.token_id, ctx.principal.client_id,
                            ctx.profile_id, operation, "intent")

    def _segment_full(self) -> bool:
        seq_row = self._db.execute("SELECT seq FROM sqlite_sequence WHERE name='access_audit'").fetchone()
        segment = (seq_row[0] if seq_row else 0) // self.segment_rows
        first = self._db.execute("SELECT MIN(segment) FROM access_audit").fetchone()[0]
        return first is not None and segment - first >= self.max_segments

    def saturation(self) -> dict[str, bool]:
        """S06 admission probe, not a reservation; unavailable storage is saturated.

        dropped_denials is a process-local monotonic diagnostic counter. No new
        metric family is exported: C9's wire vocabulary remains unchanged.
        """
        with self._lock:
            try:
                authenticated = self._db is None or self._segment_full()
            except sqlite3.Error:
                authenticated = True
            denials = self._denial_failed or self._denials is None
            if self._denials is not None:
                denials = denials or self._denials.saturation()["authenticated"]
            return {"authenticated": authenticated, "denials": denials}

    async def record_unauthenticated(self, request_id: str, reason: ErrorCode) -> AuditTicket | None:
        """Best effort off-protocol seam; None means dropped, never request denial."""
        if reason != "E_AUTH_REQUIRED":
            raise AuditError("E_VALIDATION")
        _check(request_id, _REQUEST)
        with self._lock:
            try:
                if self._denials is None:
                    raise AuditError()
                ticket = self._denials._insert(request_id, None, None, None, "authentication", "intent")
                await self._denials.finish(ticket, AuditOutcome(401, reason, (), 0.0))
                return ticket
            except AuditError:
                self.dropped_denials += 1
                self._denial_failed = True
                return None

    async def finish(self, ticket: AuditTicket, outcome: AuditOutcome) -> None:
        if (type(outcome.status) is not int or not 100 <= outcome.status <= 599
                or (outcome.error_code is not None and outcome.error_code not in _CODES)):
            raise AuditError("E_VALIDATION")
        result = outcome.error_code or ("success" if outcome.status < 400 else "failure")
        with self._lock, self._transaction():
            # request_id is checked before CAS; only the one allowed state UPDATE is issued.
            row = self._db.execute(
                "SELECT request_id,started,clock_id FROM access_audit WHERE id=?", (ticket.id,),
            ).fetchone()
            if row is None or row[0] != ticket.request_id:
                raise AuditError("E_NOT_FOUND")
            elapsed = self.monotonic() - row["started"] if row["clock_id"] == _CLOCK_ID else 0.0
            latency = max(0.0, elapsed * 1000) if math.isfinite(elapsed) else 0.0
            changed = self._db.execute(
                "UPDATE access_audit SET result=?,latency_ms=?,state='terminal' WHERE id=? AND state='intent'",
                (result, latency, ticket.id),
            ).rowcount
            if changed != 1:
                raise AuditError("E_VERSION_CONFLICT")

    async def publish_committed(self, ticket: AuditTicket, outcome: AuditOutcome) -> bool:
        """Publish an already-journaled outcome, never invert its canonical result.

        False leaves the durable intent pending. Caller retains its canonical
        journal's outcome marker and reconciles before another sensitive effect.
        A terminal ticket is already published (C6.5 ticket dedup).
        """
        try:
            await self.finish(ticket, outcome)
            return True
        except AuditError as exc:
            return exc.code == "E_VERSION_CONFLICT"

    def list(
        self, principal: Principal, profile_id: str, *, after_seq: int = 0, limit: int = 50,
    ) -> tuple[AccessAudit, ...]:
        """Internal keyset seam, not a signed public cursor or an HTTP route."""
        if "audit.read" not in principal.permissions or profile_id not in principal.profiles:
            raise AuditError("E_PERMISSION_DENIED")
        if type(limit) is not int or not 1 <= limit <= 200 or type(after_seq) is not int or after_seq < 0:
            raise AuditError("E_VALIDATION")
        with self._lock:
            if self._db is None:
                raise AuditError()
            try:
                rows = self._db.execute(
                    f"SELECT {_COLUMNS} FROM access_audit WHERE profile_id=? AND seq>? ORDER BY seq LIMIT ?",
                    (profile_id, after_seq, limit),
                ).fetchall()
            except sqlite3.Error:
                raise AuditError() from None
        return tuple(AccessAudit.model_validate({**dict(row), "timestamp": datetime.fromisoformat(row["timestamp"])})
                     for row in rows)

    def _watermark(self) -> float:
        """Called only under the SQLite write lock; invalid metadata fails closed."""
        try:
            try:
                fd = os.open("retention.watermark", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self._fd)
            except FileNotFoundError:
                return float("-inf")
            try:
                raw = os.read(fd, 128)
                value = float(raw)
                if not math.isfinite(value):
                    raise AuditError()
                return value
            finally:
                os.close(fd)
        except (OSError, ValueError):
            raise AuditError() from None

    def retain(self) -> int:
        """Refuse backward wall clocks using a durable atomic watermark.

        The SQLite write lock serializes sidecar access. fsync + replace before
        DELETE is conservative on crash. Inserts clamp created_at to this
        watermark, protecting fresh intents created during a wall-clock rollback.
        """
        now = self._now().timestamp()
        try:
            with self._lock, self._transaction():
                if now < self._watermark():
                    self.rollback_refusals += 1
                    raise AuditError("E_VALIDATION")
                flags = os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC
                fd = os.open("retention.next", flags, 0o600, dir_fd=self._fd)
                try:
                    info = os.fstat(fd)
                    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                            or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
                        raise AuditError()
                    data = str(now).encode("ascii")
                    if os.write(fd, data) != len(data):
                        raise AuditError()
                    os.ftruncate(fd, len(data))
                    os.fsync(fd)
                finally:
                    os.close(fd)
                os.replace("retention.next", "retention.watermark", src_dir_fd=self._fd, dst_dir_fd=self._fd)
                os.fsync(self._fd)
                removed = self._db.execute(
                    "DELETE FROM access_audit WHERE created_at < ?", (now - self.ttl_seconds,),
                ).rowcount
            if self._denials is not None:
                try:
                    removed += self._denials.retain()
                    self._denial_failed = False
                except AuditError:
                    self._denial_failed = True
            return removed
        except OSError:
            raise AuditError() from None
