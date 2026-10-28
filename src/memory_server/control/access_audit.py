"""Private append-only access log; no payloads, credentials or exception text.

Logical segments contain at most segment_rows rows; unexpired history is never
evicted. The authenticated default budgets one row / 10 s across the TTL:
30 * 86400 / 10 = 259200 rows; ceil(259200 / 10000) + 1 = 27 segments.
The spare segment covers partial-segment retention; retain must run at least
once per segment interval. Higher audited rates need explicit capacity planning.
The 27-segment default is retained intentionally: 8 segments hold only 80000
rows, below the 259200-row TTL target. At an illustrative 512 B/row including
indexes, 270000 slots need 138240000 B (~132 MiB), exceeding the old 64 MiB.
SQLite max_page_count caps the authenticated file at 256 MiB, with 4 MiB for
denials (260 MiB total database budget). These are disk, not RAM guarantees;
large metadata may hit the byte cap first and fail closed. Each connection has
an explicit 2048 KiB page-cache target (4096 KiB for the pair, not a process RSS
limit); lists materialize at most 200 rows. VACUUM needs temporary disk space.
Pre-auth denials use a separate file/sequence/page budget (2000 rows, 4 MiB),
never authenticated slots. Saturation drops denial recording, not requests.
Exhaustion of authenticated storage is E_AUDIT_UNAVAILABLE.
saturation() is the S06 gate input: exact keys "authenticated" and "denials".
"denials" is true on failure, capacity exhaustion, or durable losses since the
last observation; retain() cannot clear write failure. Only a successful denial
write clears the durable failing flag. No new C9 metric family is introduced.
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
        max_clock_step: float = 3600,
        segment_rows: int = 10000,
        max_segments: int | None = None,
        max_bytes: int = 256 * 1024 * 1024,
        denial_segment_rows: int = 1000,
        denial_max_segments: int = 2,
        _denial_only: bool = False,
    ) -> None:
        if (
            not math.isfinite(ttl_seconds) or ttl_seconds <= 0
            or type(max_clock_step) not in (int, float)
            or not math.isfinite(max_clock_step) or max_clock_step < 1
            or type(segment_rows) is not int or segment_rows < 1
            or (max_segments is not None and (type(max_segments) is not int or max_segments < 1))
            or type(denial_segment_rows) is not int or denial_segment_rows < 1
            or type(denial_max_segments) is not int or denial_max_segments < 1
            or type(max_bytes) is not int or max_bytes < 65536
        ):
            raise AuditError("E_VALIDATION")
        self.clock, self.monotonic = clock, monotonic
        self.ttl_seconds = ttl_seconds
        self.max_clock_step = max_clock_step
        self.clock_step_refusals = 0
        self.segment_rows = segment_rows
        self.max_segments = max_segments if max_segments is not None else math.ceil(ttl_seconds / 10 / segment_rows) + 1
        self._lock = RLock()
        self._denials = None
        self._observed_denial_drops = 0
        self._denial_failed = False
        self.rollback_refusals = 0
        self._fd = None
        self._db = None
        try:
            self._fd = _private_directory(Path(root))
            stale_journal = False
            for name in ("access.sqlite", "access.sqlite-journal", "access.sqlite-wal", "access.sqlite-shm",
                         "retention.watermark", "retention.next"):
                try:
                    info = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                        or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
                    raise AuditError()
                stale_journal |= name == "access.sqlite-journal"
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
            self._db.execute("PRAGMA secure_delete=ON")  # scrub expired identities from freed SQLite cells
            self._db.execute("PRAGMA cache_size=-2048")  # KiB target, not a process RSS limit
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
                if "retention_at" not in {r[1] for r in self._db.execute("PRAGMA table_info(access_audit)")}:
                    self._db.execute("ALTER TABLE access_audit ADD COLUMN retention_at REAL NOT NULL DEFAULT 0")
                self._db.execute("CREATE INDEX IF NOT EXISTS audit_scope ON access_audit(profile_id,seq)")
                self._watermark()
                if _denial_only:
                    self._db.execute("""CREATE TABLE IF NOT EXISTS denial_health (
                        id INTEGER PRIMARY KEY CHECK(id=1), dropped INTEGER NOT NULL,
                        failing INTEGER NOT NULL CHECK(failing IN (0,1)))""")
                    self._db.execute("INSERT OR IGNORE INTO denial_health VALUES (1,0,0)")
            if stale_journal:
                # SQLite recovers hot journals first; scrub before serving.
                self._scrub()
            os.fsync(self._fd)
            if not _denial_only:
                try:
                    self._denials = AccessAuditStore(
                        Path(root) / "denials", clock=clock, monotonic=monotonic, ttl_seconds=ttl_seconds,
                        segment_rows=denial_segment_rows, max_segments=denial_max_segments,
                        max_bytes=4 * 1024 * 1024, max_clock_step=max_clock_step, _denial_only=True,
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
        if self._denials is not None:
            self._denials.close()
            self._denials = None
        with self._lock:
            if self._db is not None:
                self._db.close()
                self._db = None
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None

    @contextmanager
    def _transaction(self, *, exclusive: bool = False) -> Iterator[None]:
        if self._db is None:
            raise AuditError()
        try:
            self._db.execute("BEGIN EXCLUSIVE" if exclusive else "BEGIN IMMEDIATE")
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
            watermark = self._watermark()
            created_at = now.timestamp()
            # Internal rollback guard, not the wall-clock timestamp or watermark.
            # It preserves fresh rollback inserts when the wall clock recovers.
            retention_at = max(created_at, watermark)
            self._db.execute(
                f"INSERT INTO access_audit ({_COLUMNS},state,created_at,segment,started,clock_id,retention_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ticket.id, now.isoformat(), request_id, token_id, client_id, profile_id,
                 operation, None, result, 0.0, None, "intent", created_at, segment, started, _CLOCK_ID, retention_at),
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

        Exact S06 keys: authenticated, denials. Durable losses are observed once
        per store instance, including after restart; ongoing failure stays true.
        Retention cannot acknowledge losses or prove recovery of denial writes.
        """
        with self._lock:
            try:
                authenticated = self._db is None or self._segment_full()
            except sqlite3.Error:
                authenticated = True
        # A slow denial probe must not hold the authenticated lock either.
        denials = self._denial_failed or self._denials is None
        if self._denials is not None:
            target = self._denials
            with target._lock:
                try:
                    dropped, failing = target._health()
                    denials = (denials or bool(failing) or dropped > self._observed_denial_drops
                               or target._segment_full())
                    self._observed_denial_drops = dropped
                except (AuditError, sqlite3.Error):
                    denials = True
        return {"authenticated": authenticated, "denials": denials}

    def _health(self) -> tuple[int, int]:
        """Denial store only, under its own lock; no authenticated I/O."""
        if self._db is None:
            raise AuditError()
        try:
            row = self._db.execute("SELECT dropped,failing FROM denial_health WHERE id=1").fetchone()
            if row is None:
                raise AuditError()
            return row[0], row[1]
        except sqlite3.Error:
            raise AuditError() from None

    @property
    def dropped_denials(self) -> int:
        """Durable denial-store loss count; unavailable metadata fails closed."""
        if self._denials is None:
            raise AuditError()
        with self._denials._lock:
            return self._denials._health()[0]

    def _record_denial_health(self, *, dropped: bool) -> None:
        with self._lock, self._transaction():
            changed = self._db.execute(
                "UPDATE denial_health SET dropped=dropped+?,failing=? WHERE id=1",
                (int(dropped), int(dropped)),
            ).rowcount
            if changed != 1:
                raise AuditError()

    async def record_unauthenticated(self, request_id: str, reason: ErrorCode) -> AuditTicket | None:
        """Best effort off-protocol seam; None means dropped, never request denial."""
        if reason != "E_AUTH_REQUIRED":
            raise AuditError("E_VALIDATION")
        _check(request_id, _REQUEST)
        # Never enter the authenticated lock/connection/sidecar on this path.
        try:
            if self._denials is None:
                raise AuditError()
            ticket = self._denials._insert(request_id, None, None, None, "authentication", "intent")
            await self._denials.finish(ticket, AuditOutcome(401, reason, (), 0.0))
        except AuditError:
            self._denial_failed = True
            if self._denials is not None:
                try:
                    self._denials._record_denial_health(dropped=True)
                except AuditError:
                    pass  # metadata unavailable: keep the gate fail-safe, never claim health
            return None
        try:
            self._denials._record_denial_health(dropped=False)
            self._denial_failed = False
        except AuditError:
            self._denial_failed = True
        return ticket

    async def finish(self, ticket: AuditTicket, outcome: AuditOutcome) -> None:
        if (type(outcome.status) is not int or not 100 <= outcome.status <= 599
                or (outcome.error_code is not None and outcome.error_code not in _CODES)):
            raise AuditError("E_VALIDATION")
        result = outcome.error_code or ("success" if outcome.status < 400 else "failure")
        with self._lock, self._transaction():
            self._watermark()
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
        with self._lock, self._transaction():
            if self._db is None:
                raise AuditError()
            try:
                self._watermark()
                rows = self._db.execute(
                    f"SELECT {_COLUMNS} FROM access_audit WHERE profile_id=? AND seq>? ORDER BY seq LIMIT ?",
                    (profile_id, after_seq, limit),
                ).fetchall()
                return tuple(AccessAudit.model_validate(
                    {**dict(row), "timestamp": datetime.fromisoformat(row["timestamp"])}
                ) for row in rows)
            except (sqlite3.Error, ValueError, TypeError):
                # Includes Pydantic ValidationError; never expose corrupted row content.
                raise AuditError() from None

    def _watermark(self) -> float:
        """Called only under the SQLite write lock; invalid metadata fails closed."""
        try:
            try:
                fd = os.open("retention.watermark", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self._fd)
            except FileNotFoundError:
                if self._db.execute("SELECT 1 FROM access_audit LIMIT 1").fetchone() is not None:
                    raise AuditError()
                value = self._now().timestamp()
                self._write_watermark(value)
                return value
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

    def _write_watermark(self, value: float) -> None:
        """Seed once, then persist only before retention; missing metadata fails closed."""
        try:
            flags = os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC
            fd = os.open("retention.next", flags, 0o600, dir_fd=self._fd)
            try:
                info = os.fstat(fd)
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                        or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
                    raise AuditError()
                data = str(value).encode("ascii")
                if os.write(fd, data) != len(data):
                    raise AuditError()
                os.ftruncate(fd, len(data))
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace("retention.next", "retention.watermark", src_dir_fd=self._fd, dst_dir_fd=self._fd)
            os.fsync(self._fd)
        except OSError:
            raise AuditError() from None

    def _scrub(self) -> None:
        """Own connection, under own lock, with no active transaction.

        VACUUM removes bytes freed by unflagged external connections as well.
        An exclusive SQLite lock prevents unlinking another writer's journal.
        Startup recovery/scrubbing completes before this store is served.
        """
        try:
            self._db.execute("VACUUM")
            with self._transaction(exclusive=True):
                try:
                    fd = os.open("access.sqlite-journal", os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                 dir_fd=self._fd)
                except FileNotFoundError:
                    return
                try:
                    info = os.fstat(fd)
                    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                            or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
                        raise AuditError()
                    os.ftruncate(fd, 0)
                    os.fsync(fd)
                    os.unlink("access.sqlite-journal", dir_fd=self._fd)
                    os.fsync(self._fd)
                finally:
                    os.close(fd)
        except (sqlite3.Error, OSError):
            raise AuditError() from None

    def retain(self) -> int:
        """Apply min(now, watermark + max_clock_step); default cap is one hour.

        Backward clocks refuse E_VALIDATION and count rollback_refusals. Forward
        steps above the cap count clock_step_refusals but apply the bounded step;
        repeated calls converge. Inserts never advance this retention watermark.
        created_at is exact wall time; internal retention_at guards rollback
        inserts until TTL after the previous watermark, including after restart.
        The SQLite write lock serializes sidecar access; fsync precedes DELETE.
        """
        now = self._now().timestamp()
        try:
            with self._lock, self._transaction():
                watermark = self._watermark()
                if now < watermark:
                    self.rollback_refusals += 1
                    raise AuditError("E_VALIDATION")
                effective_now = min(now, watermark + self.max_clock_step)
                if now > effective_now:
                    self.clock_step_refusals += 1
                self._write_watermark(effective_now)
                cutoff = effective_now - self.ttl_seconds
                removed = self._db.execute(
                    "DELETE FROM access_audit WHERE created_at < ? AND retention_at < ?", (cutoff, cutoff),
                ).rowcount
            if removed:
                with self._lock:
                    self._scrub()
            if self._denials is not None:
                try:
                    removed += self._denials.retain()
                except AuditError:
                    self._denial_failed = True
            return removed
        except OSError:
            raise AuditError() from None
