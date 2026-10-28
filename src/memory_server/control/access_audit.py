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
limit); lists materialize at most 200 rows. Retention deletes at most 200 rows
per stream and call on a separate maintenance connection/lock. New databases
use incremental auto-vacuum. Every scrub obligation cleans a bounded window of
8 freelist and 8 btree pages reached from authoritative schema roots. Separately, at most 8 free
pages are compacted once per durable wall-clock 60 seconds when the freelist
exceeds one page. Existing NONE-mode databases require the explicit
migrate_incremental_vacuum() administrative full rewrite.
Age is decided by the boot witness, not by wall-clock arithmetic: a row is
deleted only when its wall age and its boot-relative age both pass the TTL
(rows from a previous boot only after this boot has itself lasted a TTL;
unwitnessed rows are quarantined and never aged automatically). One parent call
deletes at most 200 authenticated plus 200 denial rows; repeated calls drain the
backlog.
Pre-auth denials use a separate file/sequence/page budget (2000 rows, 4 MiB),
never authenticated slots. Saturation drops denial recording, not requests.
Exhaustion of authenticated storage is E_AUDIT_UNAVAILABLE.
saturation() is the S06 gate input: keys "authenticated", "denials", "stale",
"age_seconds", "clock_lag_seconds" and "clock_suspect". Clock lag is
max(0, now-watermark); suspect means raw lag > ttl and remains an admission
signal while capacity cannot yet be reclaimed safely. "denials" is true on
failure, capacity exhaustion, a busy writer, or durable losses since the last
observation. Denial state is an immutable lock-free snapshot; an opportunistic
read-only timeout-zero connection refreshes it, while stale/age_seconds expose
the last successful refresh. retain() cannot clear write failure. Only a
successful denial write clears the durable failing flag. No new C9 family exists.
If both denial-health database writes fail, a private, fsynced marker beside the
denial database preserves the loss across restart. If both that database and its
directory are unwritable, only the process-local fail-closed signal can remain.
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


def _read_boot_epoch() -> str:
    """Kernel boot id; boottime values are only comparable within one boot."""
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    except OSError:
        return ""


def _default_boottime() -> float:
    """Suspend-inclusive monotonic clock; falls back to monotonic when unavailable."""
    try:
        return time.clock_gettime(time.CLOCK_BOOTTIME)  # type: ignore[attr-defined]
    except (AttributeError, OSError):
        return time.monotonic()
_DELETE_CHUNK_ROWS = 200
_VACUUM_PAGES = 8
_VACUUM_MIN_FREE_PAGES = 1
_VACUUM_INTERVAL_SECONDS = 60.0


class AuditError(Exception):
    """Stable C5-only failure, never chains SQLite/path/content diagnostics."""

    def __init__(self, code: ErrorCode = "E_AUDIT_UNAVAILABLE") -> None:
        self.code = code
        super().__init__(code)


class _DenialHealthTamperError(AuditError):
    """Existing denial evidence is missing; the parent must refuse service."""


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
        boottime: Callable[[], float] = _default_boottime,
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
        self.clock, self.monotonic, self.boottime = clock, monotonic, boottime
        self._boot_epoch = _read_boot_epoch()
        if not self._boot_epoch or self._boottime_value() <= 0:
            # Fail closed: without a witness, retention cannot prove age.
            raise AuditError()
        self.ttl_seconds = ttl_seconds
        self.max_clock_step = max_clock_step
        self.clock_step_refusals = 0
        self.max_bytes = max_bytes
        self.segment_rows = segment_rows
        self.max_segments = max_segments if max_segments is not None else math.ceil(ttl_seconds / 10 / segment_rows) + 1
        self._lock = RLock()
        self._maintenance_lock = RLock()
        self._denials = None
        self._observed_denial_drops = 0
        self._denial_failed = False
        self._denial_write_inflight = False
        self._denial_snapshot = (True, 0, time.monotonic(), True)
        self.rollback_refusals = 0
        self.pinned_by_wall_clock = 0  # rows old by the witness, not yet by the wall clock
        self.unknown_provenance_rows = 0  # rows with no usable witness; admin retire only
        self._fd = None
        self._db = None
        self._maintenance_db = None
        try:
            self._fd = _private_directory(Path(root))
            stale_journal = False
            for name in ("access.sqlite", "access.sqlite-journal", "access.sqlite-wal", "access.sqlite-shm",
                         "retention.watermark", "retention.next", "denial.failure", "denial.failure.next"):
                try:
                    info = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                        or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
                    raise AuditError()
                stale_journal |= name == "access.sqlite-journal"
            flags = os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC
            try:
                pin = os.open(
                    "access.sqlite", flags | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=self._fd
                )
                new_database = True
            except FileExistsError:
                pin = os.open("access.sqlite", flags, dir_fd=self._fd)
                new_database = False
            try:
                before = os.fstat(pin)
                if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                        or before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) != 0o600):
                    raise AuditError()
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
            if new_database:
                self._db.execute("PRAGMA auto_vacuum=INCREMENTAL")
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
                    started REAL NOT NULL, clock_id TEXT NOT NULL,
                    booted_at REAL NOT NULL DEFAULT 0, boot_epoch TEXT NOT NULL DEFAULT '')""")
                columns = {r[1] for r in self._db.execute("PRAGMA table_info(access_audit)")}
                if "retention_at" not in columns:
                    self._db.execute("ALTER TABLE access_audit ADD COLUMN retention_at REAL NOT NULL DEFAULT 0")
                if "booted_at" not in columns:
                    self._db.execute("ALTER TABLE access_audit ADD COLUMN booted_at REAL NOT NULL DEFAULT 0")
                if "boot_epoch" not in columns:
                    self._db.execute("ALTER TABLE access_audit ADD COLUMN boot_epoch TEXT NOT NULL DEFAULT ''")
                self._db.execute("CREATE INDEX IF NOT EXISTS audit_scope ON access_audit(profile_id,seq)")
                self._db.execute(
                    "CREATE INDEX IF NOT EXISTS audit_provenance ON access_audit(boot_epoch,booted_at,state)"
                )
                self._db.execute(
                    "CREATE INDEX IF NOT EXISTS audit_segment ON access_audit(segment)"
                )
                self._db.execute(
                    """CREATE INDEX IF NOT EXISTS audit_retention
                        ON access_audit(created_at,retention_at,seq)"""
                )
                self._db.execute("""CREATE TABLE IF NOT EXISTS maintenance_state (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    scrub_pending INTEGER NOT NULL CHECK(scrub_pending IN (0,1)),
                    last_vacuum REAL NOT NULL DEFAULT 0,
                    scrub_cursor INTEGER NOT NULL DEFAULT 1)""")
                if "last_vacuum" not in {
                    r[1] for r in self._db.execute("PRAGMA table_info(maintenance_state)")
                }:
                    self._db.execute(
                        "ALTER TABLE maintenance_state ADD COLUMN last_vacuum REAL NOT NULL DEFAULT 0"
                    )
                if "scrub_cursor" not in {
                    r[1] for r in self._db.execute("PRAGMA table_info(maintenance_state)")
                }:
                    self._db.execute(
                        "ALTER TABLE maintenance_state ADD COLUMN scrub_cursor INTEGER NOT NULL DEFAULT 1"
                    )
                self._db.execute(
                    """INSERT OR IGNORE INTO maintenance_state
                        (id,scrub_pending,last_vacuum,scrub_cursor) VALUES (1,0,0,1)"""
                )
                self._db.execute("""CREATE TABLE IF NOT EXISTS maintenance_scrub_frontier (
                    id INTEGER PRIMARY KEY,
                    root_name TEXT NOT NULL,
                    path TEXT NOT NULL,
                    next_child INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(root_name,path))""")
                self._watermark()
                if _denial_only:
                    if new_database:
                        self._db.execute("""CREATE TABLE denial_health (
                            id INTEGER PRIMARY KEY CHECK(id=1), dropped INTEGER NOT NULL,
                            failing INTEGER NOT NULL CHECK(failing IN (0,1)))""")
                        self._db.execute("INSERT INTO denial_health VALUES (1,0,0)")
                    else:
                        table = self._db.execute(
                            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='denial_health'"
                        ).fetchone()
                        health = None if table is None else self._db.execute(
                            "SELECT dropped,failing FROM denial_health WHERE id=1"
                        ).fetchone()
                        if table is None or health is None:
                            raise _DenialHealthTamperError()
            self._maintenance_db = sqlite3.connect(
                f"/proc/self/fd/{self._fd}/access.sqlite", isolation_level=None,
                timeout=5, check_same_thread=False,
            )
            self._maintenance_db.row_factory = sqlite3.Row
            self._maintenance_db.execute("PRAGMA synchronous=FULL")
            self._maintenance_db.execute("PRAGMA journal_mode=DELETE")
            self._maintenance_db.execute("PRAGMA secure_delete=ON")
            self._maintenance_db.execute("PRAGMA cache_size=-2048")
            self._maintenance_db.execute(f"PRAGMA max_page_count={max_bytes // page_size}")
            scrub_pending = bool(self._db.execute(
                """SELECT scrub_pending OR EXISTS(SELECT 1 FROM maintenance_scrub_frontier)
                    FROM maintenance_state WHERE id=1"""
            ).fetchone()[0])
            if stale_journal or scrub_pending:
                # SQLite recovers hot journals first; pending work is retried
                # after restart before this store can serve.
                self._scrub()
            os.fsync(self._fd)
            if not _denial_only:
                try:
                    self._denials = AccessAuditStore(
                        Path(root) / "denials", clock=clock, monotonic=monotonic, boottime=boottime,
                        ttl_seconds=ttl_seconds,
                        segment_rows=denial_segment_rows, max_segments=denial_max_segments,
                        max_bytes=4 * 1024 * 1024, max_clock_step=max_clock_step, _denial_only=True,
                    )
                    self._refresh_denial_snapshot()
                except _DenialHealthTamperError:
                    # Missing durable loss evidence is tamper, not an optional
                    # denial-recorder outage: never serve through the parent.
                    raise AuditError() from None
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
        with self._maintenance_lock:
            if self._maintenance_db is not None:
                self._maintenance_db.close()
                self._maintenance_db = None
        with self._lock:
            if self._db is not None:
                self._db.close()
                self._db = None
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None

    @contextmanager
    def _transaction(self, *, db: sqlite3.Connection | None = None,
                     exclusive: bool = False) -> Iterator[None]:
        target = self._db if db is None else db
        if target is None:
            raise AuditError()
        try:
            target.execute("BEGIN EXCLUSIVE" if exclusive else "BEGIN IMMEDIATE")
            try:
                yield
                target.execute("COMMIT")
            except BaseException:
                target.execute("ROLLBACK")
                raise
        except sqlite3.Error:
            raise AuditError() from None

    def _now(self) -> datetime:
        value = self.clock()
        offset = value.utcoffset()
        if offset is None or offset.total_seconds() != 0:
            raise AuditError("E_VALIDATION")
        return value

    def _boottime_value(self) -> float:
        """Boot-relative seconds, or 0.0 when the witness is unavailable."""
        try:
            value = float(self.boottime())
        except Exception:
            return 0.0
        return value if math.isfinite(value) and value >= 0 else 0.0

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
            first = self._first_ageable_segment()
            if first is not None and segment - first >= self.max_segments:
                raise AuditError()
            watermark = self._watermark()
            created_at = now.timestamp()
            # Internal rollback guard, not the wall-clock timestamp or watermark.
            # It preserves fresh rollback inserts when the wall clock recovers.
            retention_at = max(created_at, watermark)
            booted_at = self._boottime_value()
            if booted_at <= 0:
                # Fail closed: an unwitnessed row could never be aged and would
                # silently hold capacity (quarantine), so the write is refused.
                raise AuditError()
            self._db.execute(
                f"INSERT INTO access_audit ({_COLUMNS},state,created_at,segment,started,clock_id,retention_at,"
                "booted_at,boot_epoch) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ticket.id, now.isoformat(), request_id, token_id, client_id, profile_id,
                 operation, None, result, 0.0, None, "intent", created_at, segment, started, _CLOCK_ID,
                 retention_at, booted_at, self._boot_epoch),
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

    def _first_ageable_segment(self, db: sqlite3.Connection | None = None) -> int | None:
        """Oldest segment retention can still free.

        Quarantined (unwitnessed) rows are excluded: they must never block the
        recording path, and saturation() reports them separately. The caller may
        pass any connection so every capacity view uses this one predicate.
        """
        handle = self._db if db is None else db
        return handle.execute(
            "SELECT MIN(segment) FROM access_audit WHERE boot_epoch <> '' AND booted_at > 0"
        ).fetchone()[0]

    def _bytes_full(self) -> bool:
        """True when the SQLite page cap leaves no room for another write."""
        page_count = self._db.execute("PRAGMA page_count").fetchone()[0]
        page_size = self._db.execute("PRAGMA page_size").fetchone()[0]
        return (page_count + 1) * page_size > self.max_bytes

    def _segment_full(self) -> bool:
        """Capacity counts only rows retention can actually age.

        Quarantined (unwitnessed) rows are excluded: they must never block the
        recording path, and they are reported separately by saturation().
        """
        seq_row = self._db.execute("SELECT seq FROM sqlite_sequence WHERE name='access_audit'").fetchone()
        segment = (seq_row[0] if seq_row else 0) // self.segment_rows
        first = self._first_ageable_segment()
        return first is not None and segment - first >= self.max_segments

    def retire_unknown_provenance(self, *, older_than_seconds: float | None = None) -> int:
        """Administrative retirement of rows that carry no usable witness.

        Rows written before the witness existed, or while boottime was broken,
        can never be aged automatically. They are quarantined (reported by
        saturation()["unknown_provenance_rows"]) and deleted here on an explicit
        operator call, still honouring a wall-clock age floor.
        """
        age = self.ttl_seconds if older_than_seconds is None else float(older_than_seconds)
        if not math.isfinite(age) or age <= 0:
            raise AuditError("E_VALIDATION")
        cutoff = self._now().timestamp() - age
        try:
            with self._maintenance_lock:
                db = self._maintenance_db
                if db is None:
                    raise AuditError()
                removed = db.execute(
                    "DELETE FROM access_audit WHERE (boot_epoch = '' OR booted_at <= 0)"
                    " AND state = 'terminal' AND created_at < ?",
                    (cutoff,),
                ).rowcount
                if removed:
                    db.execute("UPDATE maintenance_state SET scrub_pending=1 WHERE id=1")
                self.unknown_provenance_rows = db.execute(
                    "SELECT COUNT(*) FROM access_audit WHERE boot_epoch = '' OR booted_at <= 0"
                ).fetchone()[0]
            if removed:
                self._scrub()
            return removed
        except (OSError, sqlite3.Error):
            raise AuditError() from None

    def saturation(self) -> dict[str, bool | float]:
        """S06 admission probe, not a reservation; unavailable storage is saturated.

        Denial snapshot keys include denials, stale and age_seconds. Durable
        losses are observed once per store instance, including after restart;
        ongoing or busy writes stay true. Retention cannot acknowledge losses.
        """
        with self._lock:
            try:
                authenticated = self._db is None or self._segment_full() or self._bytes_full()
                raw_lag = self._now().timestamp() - self._read_watermark()
                clock_lag = max(0.0, raw_lag)
                clock_suspect = raw_lag > self.ttl_seconds
                self.unknown_provenance_rows = self._db.execute(
                    "SELECT COUNT(*) FROM access_audit WHERE boot_epoch = '' OR booted_at <= 0"
                ).fetchone()[0]
                bytes_saturated = self._bytes_full()
            except (AuditError, sqlite3.Error):
                authenticated = True
                clock_lag = math.inf
                clock_suspect = True
                bytes_saturated = True
        self._refresh_denial_snapshot()
        denial_state, _, observed_at, stale = self._denial_snapshot
        inflight = self._denial_write_inflight
        return {
            "authenticated": authenticated,
            "denials": bool(denial_state or inflight or self._denial_failed),
            "stale": bool(stale or inflight),
            "age_seconds": max(0.0, time.monotonic() - observed_at),
            "clock_lag_seconds": clock_lag,
            "clock_suspect": clock_suspect,
            "pinned_by_wall_clock": self.pinned_by_wall_clock,
            "unknown_provenance_rows": self.unknown_provenance_rows,
            "bytes_saturated": bytes_saturated,
        }

    def _refresh_denial_snapshot(self) -> None:
        """Opportunistic timeout-zero read; publish failure conservatively."""
        target = self._denials
        previous = self._denial_snapshot
        if target is None or target._fd is None:
            self._denial_snapshot = (True, previous[1], previous[2], True)
            return
        reader = None
        try:
            reader = sqlite3.connect(
                f"file:/proc/self/fd/{target._fd}/access.sqlite?mode=ro",
                uri=True, isolation_level=None, timeout=0, check_same_thread=False,
            )
            reader.execute("PRAGMA busy_timeout=0")
            health = reader.execute(
                "SELECT dropped,failing FROM denial_health WHERE id=1"
            ).fetchone()
            if health is None:
                raise AuditError()
            marker_dropped, marker_failing = target._denial_failure_marker()
            dropped = health[0] + marker_dropped
            seq_row = reader.execute(
                "SELECT seq FROM sqlite_sequence WHERE name='access_audit'"
            ).fetchone()
            segment = ((seq_row[0] if seq_row else 0) // target.segment_rows)
            first = target._first_ageable_segment(reader)
            full = first is not None and segment - first >= target.max_segments
            new_loss = dropped > self._observed_denial_drops
            if new_loss:
                self._observed_denial_drops = dropped
            self._denial_snapshot = (
                bool(health[1] or marker_failing or full or new_loss),
                dropped,
                time.monotonic(),
                False,
            )
        except (AuditError, OSError, sqlite3.Error):
            self._denial_snapshot = (True, previous[1], previous[2], True)
        finally:
            if reader is not None:
                reader.close()

    def _mark_denial_write_inflight(self) -> None:
        denial_state, dropped, observed_at, _ = self._denial_snapshot
        self._denial_write_inflight = True
        self._denial_snapshot = (bool(denial_state), dropped, observed_at, True)

    def _health(self) -> tuple[int, int]:
        """Denial store only, under its own lock; no authenticated I/O."""
        if self._db is None:
            raise AuditError()
        try:
            row = self._db.execute("SELECT dropped,failing FROM denial_health WHERE id=1").fetchone()
            if row is None:
                raise AuditError()
            marker_dropped, marker_failing = self._denial_failure_marker()
            return row[0] + marker_dropped, max(row[1], marker_failing)
        except sqlite3.Error:
            raise AuditError() from None

    def _denial_failure_marker(self) -> tuple[int, int]:
        """Read the supplemental loss marker; absence is the only healthy case."""
        try:
            try:
                fd = os.open("denial.failure", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                             dir_fd=self._fd)
            except FileNotFoundError:
                return 0, 0
            try:
                info = os.fstat(fd)
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                        or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
                    raise AuditError()
                raw = os.read(fd, 128)
                match = re.fullmatch(rb"(0|[1-9][0-9]*) ([01])\n", raw)
                if match is None:
                    raise AuditError()
                return int(match[1]), int(match[2])
            finally:
                os.close(fd)
        except OSError:
            raise AuditError() from None

    def _write_denial_failure_marker(self, dropped: int, failing: int) -> None:
        """Atomically replace and fsync the private supplemental health record."""
        try:
            flags = os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC
            fd = os.open("denial.failure.next", flags, 0o600, dir_fd=self._fd)
            try:
                info = os.fstat(fd)
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                        or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
                    raise AuditError()
                data = f"{dropped} {failing}\n".encode("ascii")
                if os.write(fd, data) != len(data):
                    raise AuditError()
                os.ftruncate(fd, len(data))
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace("denial.failure.next", "denial.failure",
                       src_dir_fd=self._fd, dst_dir_fd=self._fd)
            os.fsync(self._fd)
        except OSError:
            raise AuditError() from None

    def _record_denial_failure_fallback(self, *, dropped: bool) -> None:
        """Persist failed health metadata independently or in the side marker."""
        try:
            fallback = sqlite3.connect(
                f"/proc/self/fd/{self._fd}/access.sqlite", isolation_level=None,
                timeout=0, check_same_thread=False,
            )
            try:
                fallback.execute("PRAGMA synchronous=FULL")
                fallback.execute("BEGIN IMMEDIATE")
                changed = fallback.execute(
                    "UPDATE denial_health SET dropped=dropped+?,failing=1 WHERE id=1",
                    (int(dropped),),
                ).rowcount
                if changed != 1:
                    raise sqlite3.IntegrityError
                fallback.execute("COMMIT")
                return
            finally:
                fallback.close()
        except sqlite3.Error:
            pass
        with self._lock:
            marker_dropped, _ = self._denial_failure_marker()
            self._write_denial_failure_marker(marker_dropped + int(dropped), 1)

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
        marker_dropped, marker_failing = self._denial_failure_marker()
        if (marker_dropped or marker_failing) and not dropped:
            self._write_denial_failure_marker(marker_dropped, 0)

    async def record_unauthenticated(self, request_id: str, reason: ErrorCode) -> AuditTicket | None:
        """Best effort off-protocol seam; None means dropped, never request denial."""
        if reason != "E_AUTH_REQUIRED":
            raise AuditError("E_VALIDATION")
        _check(request_id, _REQUEST)
        # Never enter the authenticated lock/connection/sidecar on this path.
        self._mark_denial_write_inflight()
        try:
            try:
                if self._denials is None:
                    raise AuditError()
                ticket = self._denials._insert(
                    request_id, None, None, None, "authentication", "intent"
                )
                await self._denials.finish(ticket, AuditOutcome(401, reason, (), 0.0))
            except AuditError:
                self._denial_failed = True
                if self._denials is not None:
                    try:
                        self._denials._record_denial_health(dropped=True)
                    except AuditError:
                        try:
                            self._denials._record_denial_failure_fallback(dropped=True)
                        except AuditError:
                            pass  # process-local gate stays fail-closed
                return None
            try:
                self._denials._record_denial_health(dropped=False)
                self._denial_failed = False
            except AuditError:
                self._denial_failed = True
                try:
                    self._denials._record_denial_failure_fallback(dropped=False)
                except AuditError:
                    pass  # process-local gate stays fail-closed
            return ticket
        finally:
            self._refresh_denial_snapshot()
            self._denial_write_inflight = False

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

    def _watermark(self, db: sqlite3.Connection | None = None) -> float:
        """Called only under the SQLite write lock; invalid metadata fails closed."""
        target = self._db if db is None else db
        try:
            try:
                fd = os.open("retention.watermark", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self._fd)
            except FileNotFoundError:
                if target.execute("SELECT 1 FROM access_audit LIMIT 1").fetchone() is not None:
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

    def _read_watermark(self) -> float:
        """Read an existing sidecar without creating it; failures stay closed."""
        try:
            fd = os.open(
                "retention.watermark", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=self._fd,
            )
            try:
                info = os.fstat(fd)
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                        or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
                    raise AuditError()
                value = float(os.read(fd, 128))
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

    def _scrub_freelist_pages(self, db: sqlite3.Connection, limit: int) -> int:
        """Zero bounded free-page payloads while an EXCLUSIVE lock is held."""
        fd = None
        try:
            fd = os.open("access.sqlite", os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
                         dir_fd=self._fd)
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
                raise AuditError()
            header = os.pread(fd, 100, 0)
            if len(header) != 100 or header[:16] != b"SQLite format 3\0":
                raise AuditError()
            encoded_page_size = int.from_bytes(header[16:18], "big")
            page_size = 65536 if encoded_page_size == 1 else encoded_page_size
            if page_size != db.execute("PRAGMA page_size").fetchone()[0]:
                raise AuditError()
            page_count = int.from_bytes(header[28:32], "big")
            trunk = int.from_bytes(header[32:36], "big")
            scrubbed = 0
            seen = set()
            while trunk and scrubbed < limit:
                if trunk in seen or not 1 <= trunk <= page_count:
                    raise AuditError()
                seen.add(trunk)
                offset = (trunk - 1) * page_size
                page = os.pread(fd, page_size, offset)
                if len(page) != page_size:
                    raise AuditError()
                next_trunk = int.from_bytes(page[:4], "big")
                leaves = int.from_bytes(page[4:8], "big")
                if leaves > (page_size - 8) // 4:
                    raise AuditError()
                metadata_end = 8 + leaves * 4
                if os.pwrite(fd, b"\0" * (page_size - metadata_end),
                             offset + metadata_end) != page_size - metadata_end:
                    raise AuditError()
                scrubbed += 1
                for index in range(leaves):
                    if scrubbed >= limit:
                        break
                    start = 8 + index * 4
                    leaf = int.from_bytes(page[start:start + 4], "big")
                    if not 1 <= leaf <= page_count or leaf in seen:
                        raise AuditError()
                    seen.add(leaf)
                    if os.pwrite(fd, b"\0" * page_size,
                                 (leaf - 1) * page_size) != page_size:
                        raise AuditError()
                    scrubbed += 1
                trunk = next_trunk
            os.fsync(fd)
            return scrubbed
        except OSError:
            raise AuditError() from None
        finally:
            if fd is not None:
                os.close(fd)

    def _scrub_btree_slack(self, db: sqlite3.Connection, start: int, limit: int) -> int:
        """Walk a persisted bounded frontier rooted at authoritative rootpages."""
        fd = None
        try:
            fd = os.open("access.sqlite", os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
                         dir_fd=self._fd)
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
                raise AuditError()
            page_size = db.execute("PRAGMA page_size").fetchone()[0]
            page_count = db.execute("PRAGMA page_count").fetchone()[0]
            if page_count < 1:
                raise AuditError()

            roots = dict(db.execute("""SELECT name,rootpage FROM sqlite_schema
                WHERE name IN ('access_audit','audit_scope','audit_segment','audit_retention',
                               'sqlite_autoindex_access_audit_1') AND rootpage>0"""))
            if db.execute("SELECT 1 FROM maintenance_scrub_frontier LIMIT 1").fetchone() is None:
                db.executemany(
                    """INSERT OR IGNORE INTO maintenance_scrub_frontier
                        (root_name,path,next_child) VALUES (?,'',0)""",
                    ((name,) for name in roots),
                )

            def read_btree(page_number: int):
                if not 1 <= page_number <= page_count:
                    raise AuditError()
                offset = (page_number - 1) * page_size
                page = os.pread(fd, page_size, offset)
                if len(page) != page_size:
                    raise AuditError()
                header_offset = 100 if page_number == 1 else 0
                page_type = page[header_offset]
                if page_type not in (0x02, 0x05, 0x0A, 0x0D):
                    raise AuditError()
                header_size = 12 if page_type in (0x02, 0x05) else 8
                cell_count = int.from_bytes(
                    page[header_offset + 3:header_offset + 5], "big"
                )
                cell_start = int.from_bytes(
                    page[header_offset + 5:header_offset + 7], "big"
                ) or 65536
                pointer_end = header_offset + header_size + cell_count * 2
                if not pointer_end <= cell_start <= page_size:
                    raise AuditError()
                children = []
                if page_type in (0x02, 0x05):
                    for index in range(cell_count):
                        pointer_at = header_offset + header_size + index * 2
                        cell = int.from_bytes(page[pointer_at:pointer_at + 2], "big")
                        if not cell_start <= cell <= page_size - 4:
                            raise AuditError()
                        child = int.from_bytes(page[cell:cell + 4], "big")
                        if not 1 <= child <= page_count:
                            raise AuditError()
                        children.append(child)
                    rightmost = int.from_bytes(
                        page[header_offset + 8:header_offset + 12], "big"
                    )
                    if not 1 <= rightmost <= page_count:
                        raise AuditError()
                    children.append(rightmost)
                return page, offset, header_offset, pointer_end, cell_start, children

            last_page = max(start, 1)
            for _ in range(limit):
                frontier = db.execute("""SELECT id,root_name,path,next_child
                    FROM maintenance_scrub_frontier ORDER BY id LIMIT 1""").fetchone()
                if frontier is None:
                    break
                db.execute(
                    "DELETE FROM maintenance_scrub_frontier WHERE id=?", (frontier[0],)
                )
                page_number = roots.get(frontier[1])
                if page_number is None:
                    continue
                path = [] if not frontier[2] else [int(part) for part in frontier[2].split("/")]
                stale_path = False
                for child_index in path:
                    _, _, _, _, _, children = read_btree(page_number)
                    if not 0 <= child_index < len(children):
                        stale_path = True
                        break
                    page_number = children[child_index]
                if stale_path:
                    continue
                page, offset, header_offset, pointer_end, cell_start, children = read_btree(
                    page_number
                )
                if cell_start > pointer_end:
                    if os.pwrite(fd, b"\0" * (cell_start - pointer_end),
                                 offset + pointer_end) != cell_start - pointer_end:
                        raise AuditError()
                freeblock = int.from_bytes(
                    page[header_offset + 1:header_offset + 3], "big"
                )
                seen = set()
                while freeblock:
                    if freeblock in seen or freeblock + 4 > page_size:
                        raise AuditError()
                    seen.add(freeblock)
                    next_free = int.from_bytes(page[freeblock:freeblock + 2], "big")
                    size = int.from_bytes(page[freeblock + 2:freeblock + 4], "big")
                    if size < 4 or freeblock + size > page_size:
                        raise AuditError()
                    if os.pwrite(fd, b"\0" * (size - 4),
                                 offset + freeblock + 4) != size - 4:
                        raise AuditError()
                    freeblock = next_free
                child_index = frontier[3]
                if child_index < len(children):
                    child_path = f"{frontier[2]}/{child_index}" if frontier[2] else str(child_index)
                    db.execute("""INSERT OR IGNORE INTO maintenance_scrub_frontier
                        (root_name,path,next_child) VALUES (?,?,0)""",
                               (frontier[1], child_path))
                    if child_index + 1 < len(children):
                        db.execute("""INSERT OR IGNORE INTO maintenance_scrub_frontier
                            (root_name,path,next_child) VALUES (?,?,?)""",
                                   (frontier[1], frontier[2], child_index + 1))
                last_page = page_number
            os.fsync(fd)
            return 1 if last_page >= page_count else last_page + 1
        except (OSError, ValueError):
            raise AuditError() from None
        finally:
            if fd is not None:
                os.close(fd)

    def _scrub(self) -> None:
        """Scrub, compact if due, and clear pending in one maintenance commit."""
        db = self._maintenance_db
        if db is None:
            raise AuditError()
        try:
            self._remove_stale_journal(db)
            # The persisted frontier starts at sqlite_schema rootpages; pointer-map,
            # overflow and freelist pages can never enter the btree parser.
            with self._transaction(db=db, exclusive=True):
                cursor = db.execute(
                    "SELECT scrub_cursor FROM maintenance_state WHERE id=1"
                ).fetchone()[0]
                self._scrub_freelist_pages(db, _VACUUM_PAGES // 2)
                next_cursor = self._scrub_btree_slack(
                    db, cursor, _VACUUM_PAGES // 2
                )
                db.execute(
                    "UPDATE maintenance_state SET scrub_cursor=? WHERE id=1",
                    (next_cursor,),
                )
                self._compact(db)
                db.execute(
                    """UPDATE maintenance_state SET scrub_pending=
                        EXISTS(SELECT 1 FROM maintenance_scrub_frontier) WHERE id=1"""
                )
            # The commit bumps the change counter, invalidating clean page caches
            # after safe slack writes, and SQLite removes its own journal.
        except (sqlite3.Error, OSError):
            raise AuditError() from None

    def _remove_stale_journal(self, db: sqlite3.Connection) -> None:
        """Remove only a journal that predates the maintenance transaction."""
        try:
            os.stat("access.sqlite-journal", dir_fd=self._fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        with self._transaction(db=db, exclusive=True):
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

    def _compact(self, db: sqlite3.Connection) -> None:
        """Rate-limit incremental vacuum inside the caller's transaction."""
        now = time.time()
        free_pages = db.execute("PRAGMA freelist_count").fetchone()[0]
        last_vacuum = db.execute(
            "SELECT last_vacuum FROM maintenance_state WHERE id=1"
        ).fetchone()[0]
        if (free_pages <= _VACUUM_MIN_FREE_PAGES
                or now < last_vacuum + _VACUUM_INTERVAL_SECONDS):
            return
        db.execute(f"PRAGMA incremental_vacuum({_VACUUM_PAGES})")
        db.execute(
            "UPDATE maintenance_state SET last_vacuum=? WHERE id=1", (now,)
        )

    def migrate_incremental_vacuum(self) -> None:
        """Explicit administrator-only full rewrite for legacy NONE-mode stores."""
        with self._maintenance_lock:
            db = self._maintenance_db
            if db is None:
                raise AuditError()
            try:
                if db.execute("PRAGMA auto_vacuum").fetchone()[0] == 2:
                    return
                db.execute("PRAGMA auto_vacuum=INCREMENTAL")
                db.execute("VACUUM")
            except sqlite3.Error:
                raise AuditError() from None

    def retain(self) -> int:
        """Age rows by the boot witness, never by wall-clock arithmetic.

        A row is deleted only when created_at is past the wall-clock cut-off AND
        its boot-relative age is at least ttl (same boot), or, for rows from a
        previous boot, only once this boot itself has lasted a full ttl — which
        proves such a row is old enough without trusting the wall clock.
        Clock steps (either direction) are counted but never wedge retention:
        rollback_refusals/clock_step_refusals are observations, not gates.
        Inserts never advance the watermark. The SQLite write lock serializes
        sidecar access; fsync precedes DELETE; each stream deletes at most
        _DELETE_CHUNK_ROWS rows per call on the maintenance connection, which is
        disjoint from authenticated Python locking.
        """
        now = self._now().timestamp()
        try:
            with self._maintenance_lock:
                db = self._maintenance_db
                if db is None or db.execute("PRAGMA auto_vacuum").fetchone()[0] != 2:
                    raise AuditError()
                pending = db.execute(
                    "SELECT scrub_pending FROM maintenance_state WHERE id=1"
                ).fetchone()[0]
                if pending:
                    self._scrub()
                with self._transaction(db=db):
                    watermark = self._watermark(db)
                    if now < watermark:
                        # A corrected clock must not wedge retention: the boot witness,
                        # not the wall watermark, is what protects fresh rows.
                        self.rollback_refusals += 1
                    lag = abs(now - watermark)
                    if lag > self.max_clock_step:
                        self.clock_step_refusals += 1
                    boot_now = self._boottime_value()
                    self._write_watermark(now)
                    cutoff = now - self.ttl_seconds
                    # Same-boot rows are aged by the witness. Rows from another boot (or
                    # written before the witness existed) can only be proven aged once
                    # this boot has itself lasted a full TTL.
                    # Unknown provenance (no boot_epoch, or booted_at<=0) is never
                    # deleted automatically: it is surfaced and retired only by an
                    # explicit administrative call.
                    where_aged = (
                        "((boot_epoch = ? AND booted_at > 0 AND ? - booted_at >= ?)"
                        " OR (boot_epoch <> '' AND boot_epoch <> ? AND ? >= ?))"
                    )
                    removed = db.execute(
                        "DELETE FROM access_audit WHERE created_at < ? AND retention_at < ?"
                        " AND state = 'terminal' AND "
                        + where_aged
                        + " AND seq IN (SELECT seq FROM access_audit WHERE created_at < ? AND retention_at < ?"
                        " AND state = 'terminal' AND "
                        + where_aged
                        + " ORDER BY seq LIMIT ?)",
                        (cutoff, cutoff,
                         self._boot_epoch, boot_now, self.ttl_seconds, self._boot_epoch, boot_now, self.ttl_seconds,
                         cutoff, cutoff,
                         self._boot_epoch, boot_now, self.ttl_seconds, self._boot_epoch, boot_now, self.ttl_seconds,
                         _DELETE_CHUNK_ROWS),
                    ).rowcount
                    self.unknown_provenance_rows = db.execute(
                        "SELECT COUNT(*) FROM access_audit WHERE boot_epoch = '' OR booted_at <= 0"
                    ).fetchone()[0]
                    self.pinned_by_wall_clock = db.execute(
                        """SELECT COUNT(*) FROM access_audit
                        WHERE boot_epoch = ? AND booted_at > 0 AND ? - booted_at >= ? AND created_at >= ?""",
                        (self._boot_epoch, boot_now, self.ttl_seconds, cutoff),
                    ).fetchone()[0]
                    if removed:
                        db.execute(
                            "UPDATE maintenance_state SET scrub_pending=1 WHERE id=1"
                        )
                if removed:
                    self._scrub()
            if self._denials is not None:
                try:
                    removed += self._denials.retain()
                except AuditError:
                    self._denial_failed = True
            return removed
        except OSError:
            raise AuditError() from None
