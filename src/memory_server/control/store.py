"""Control-plane persistence only; never opens a profile's memory database.

The private directory is the SQLite pathname security boundary. O_NOFOLLOW pins
and checks the database before SQLite opens it; identity is checked again before
schema access. Python sqlite3 cannot set SQLITE_OPEN_NOFOLLOW: a hostile process
with the same uid can still race that open (see S02 report). No runtime imports.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from datetime import datetime
from pathlib import Path
from re import findall
from threading import get_ident
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
    "E_AUTH_REQUIRED": "Authentication required",
    "E_IDEMPOTENCY_REQUIRED": "Idempotency key required",
    "E_IDEMPOTENCY_CONFLICT": "Idempotency key payload conflict",
    "E_REQUEST_IN_PROGRESS": "Request already in progress",
    "E_PROFILE_BUSY": "Profile shutdown incomplete",
    "E_TIMEOUT": "Profile control timed out",
    "E_AUDIT_UNAVAILABLE": "Control publication unavailable",
    "E_RUNTIME_CLOSING": "Profile runtime closing",
    "E_RUNTIME_FAILED": "Profile runtime failed",
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


class ControlTransaction(AbstractContextManager):
    """Single-use, owner-thread facade; detached rows may outlive the scope."""

    def __init__(self, store: ControlStore) -> None:
        self._store, self._thread = store, get_ident()
        self._entered = self._active = self._failed = False

    @staticmethod
    def _tokens(sql: str) -> tuple[str, ...]:
        return tuple(t if t.startswith("'") else t.lower() for t in findall(r"'(?:''|[^'])*'|--|/\*|[\w]+|[^\s]", sql))

    @contextmanager
    def _scope(self) -> Iterator[ControlTransaction]:
        try:
            with self._store._transaction():
                self._active = True
                yield self
                if self._failed:
                    raise ControlError("E_STORAGE")
        finally:
            self._active = False

    def __enter__(self) -> ControlTransaction:
        connection = self._store._connection
        if self._entered or self._thread != get_ident() or connection is None or connection.in_transaction:
            raise ControlError("E_STORAGE")
        self._entered = True
        self._context = self._scope()
        return self._context.__enter__()

    def __exit__(self, *exc: object) -> bool | None:
        if not self._active or self._thread != get_ident():
            raise ControlError("E_STORAGE")
        return self._context.__exit__(*exc)

    def _query(self, sql: str, parameters: Sequence[object], *, reader: bool, limit: int | None = None):
        connection = self._store._connection
        if not self._active or self._thread != get_ident() or connection is None or self._failed:
            raise ControlError("E_STORAGE")
        cursor = None
        try:
            tokens = self._tokens(sql)
            allowed = ("select",) if reader else ("insert", "update", "delete")
            if not tokens or tokens[0] not in allowed or any(t in tokens for t in (";", "returning", "--", "/*")):
                raise ControlError("E_STORAGE")
            if limit is not None:
                ceiling = parameters[-1] if tokens[-1:] == ("?",) and parameters else None
                if tokens[-1:] and tokens[-1].isascii() and tokens[-1].isdigit():
                    ceiling = int(tokens[-1])
                if type(limit) is not int or not 1 <= limit <= 1000 or tokens[-2:-1] != ("limit",):
                    raise ControlError("E_STORAGE")
                if type(ceiling) is not int or not 1 <= ceiling <= limit:
                    raise ControlError("E_STORAGE")
            cursor = connection.execute(sql, parameters)
            if not reader:
                return cursor.rowcount
            if limit is None:
                row = cursor.fetchone()
                return MappingProxyType(dict(row)) if row is not None else None
            return tuple(MappingProxyType(dict(row)) for row in cursor.fetchmany(limit))
        except (sqlite3.Error, ControlError):
            self._failed = True
            raise ControlError("E_STORAGE") from None
        finally:
            if cursor is not None:
                cursor.close()

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> int:
        return self._query(sql, parameters, reader=False)

    def fetchone(self, sql: str, parameters: Sequence[object] = ()) -> Mapping[str, object] | None:
        return self._query(sql, parameters, reader=True)

    def fetchmany(self, sql: str, parameters: Sequence[object] = (), *, limit: int) -> tuple[Mapping[str, object], ...]:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ControlError("E_STORAGE")
        return self._query(sql, parameters, reader=True, limit=limit)


class ControlStore:
    """One explicit private root, one SQLite connection; local bootstrap only.

    Registry configuration is seeded privately by ProfileRegistry. There is no
    public registration/root mutation API. Row changes and the control journal
    share BEGIN IMMEDIATE / COMMIT. S06 control reservations are metadata on the
    same generation row; authentication and HTTP orchestration remain external.
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
                # Nullable metadata columns preserve legacy generation rows and
                # keep exactly one journal row per effect (including local CAS).
                columns = {row[1] for row in self._connection.execute("PRAGMA table_info(control_journal)")}
                for name, kind in (
                    ("subject", "TEXT"),
                    ("http_operation", "TEXT"),
                    ("idem_key", "TEXT"),
                    ("payload_sha256", "TEXT"),
                    ("request_id", "TEXT"),
                    ("phase", "TEXT"),
                    ("response_snapshot", "TEXT"),
                    ("outcome_snapshot", "TEXT"),
                    ("expires_at", "TEXT"),
                    ("pending_publication", "INTEGER"),
                    ("ticket_id", "TEXT"),
                ):
                    if name not in columns:
                        self._connection.execute(f"ALTER TABLE control_journal ADD COLUMN {name} {kind}")
                self._connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS control_request_key "
                    "ON control_journal(subject,profile_id,http_operation,idem_key)"
                )
                self._connection.execute(
                    "CREATE INDEX IF NOT EXISTS control_pending ON control_journal(pending_publication,generation)"
                )
                job_schema = (
                    """CREATE TABLE IF NOT EXISTS jobs (
                        id TEXT COLLATE BINARY PRIMARY KEY NOT NULL
                            CHECK(length(id)=48 AND id NOT GLOB '*[^0-9a-f]*'),
                        profile_id TEXT COLLATE BINARY NOT NULL REFERENCES profiles(id) ON DELETE RESTRICT,
                        subject TEXT COLLATE BINARY NOT NULL CHECK(length(subject)>0),
                        token_id TEXT COLLATE BINARY NOT NULL CHECK(length(token_id)>0),
                        operation TEXT COLLATE BINARY NOT NULL CHECK(operation IN
                            ('outbox_replay','vector_rebuild','graph_rebuild','projections_rebuild')),
                        status TEXT COLLATE BINARY NOT NULL DEFAULT 'queued' CHECK(status IN
                            ('queued','running','succeeded','failed','interrupted')),
                        revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
                        progress REAL CHECK(progress IS NULL OR progress BETWEEN 0 AND 1),
                        created_at TEXT COLLATE BINARY NOT NULL
                            CHECK(typeof(created_at)='text' AND length(created_at)=27 AND created_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]Z'),
                        started_at TEXT COLLATE BINARY,
                        finished_at TEXT COLLATE BINARY,
                        updated_at TEXT COLLATE BINARY NOT NULL,
                        request_id TEXT COLLATE BINARY NOT NULL,
                        dto_snapshot TEXT NOT NULL,
                        validated_request TEXT NOT NULL,
                        phase TEXT COLLATE BINARY NOT NULL DEFAULT 'queued'
                            CHECK(phase IN ('queued','claimed','terminal','reconcile')),
                        recovery_metadata TEXT NOT NULL DEFAULT '{"v":1}',
                        slot_held INTEGER NOT NULL DEFAULT 1 CHECK(slot_held IN (0,1)),
                        UNIQUE(id,subject,profile_id),
                        CHECK(status<>'running' OR started_at IS NOT NULL),
                        CHECK(started_at IS NULL OR (typeof(started_at)='text' AND length(started_at)=27 AND started_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]Z' AND started_at>=created_at)),
                        CHECK(finished_at IS NULL OR (typeof(finished_at)='text' AND length(finished_at)=27 AND finished_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]Z' AND finished_at>=created_at AND (started_at IS NULL OR finished_at>=started_at))),
                        CHECK(typeof(updated_at)='text' AND length(updated_at)=27 AND updated_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]Z' AND updated_at>=created_at AND (started_at IS NULL OR updated_at>=started_at) AND (finished_at IS NULL OR updated_at>=finished_at)),
                        CHECK((status IN ('queued','running') AND slot_held=1 AND finished_at IS NULL)
                           OR (status IN ('succeeded','failed','interrupted') AND slot_held=0 AND finished_at IS NOT NULL)),
                        CHECK((phase='queued' AND status='queued')
                           OR (phase='claimed' AND status='running')
                           OR (phase='terminal' AND status IN ('succeeded','failed','interrupted'))
                           OR (phase='reconcile' AND status='interrupted'))
                    )""",  # noqa: E501
                    """CREATE TABLE IF NOT EXISTS job_requests (
                        subject TEXT COLLATE BINARY NOT NULL,
                        profile_id TEXT COLLATE BINARY NOT NULL REFERENCES profiles(id) ON DELETE RESTRICT,
                        route_operation TEXT COLLATE BINARY NOT NULL
                            CHECK(route_operation IN ('outbox.replay','indexes.rebuild')),
                        key TEXT COLLATE BINARY NOT NULL,
                        job_id TEXT COLLATE BINARY NOT NULL,
                        digest TEXT COLLATE BINARY NOT NULL
                            CHECK(length(digest)=64 AND digest NOT GLOB '*[^0-9a-f]*'),
                        accepted_snapshot TEXT NOT NULL,
                        request_id TEXT COLLATE BINARY NOT NULL,
                        ticket_id TEXT COLLATE BINARY NOT NULL,
                        phase TEXT COLLATE BINARY NOT NULL DEFAULT 'committed' CHECK(phase IN ('committed','final')),
                        outcome_snapshot TEXT,
                        pending_outcome INTEGER NOT NULL DEFAULT 1 CHECK(pending_outcome IN (0,1)),
                        created_at TEXT COLLATE BINARY NOT NULL
                            CHECK(typeof(created_at)='text' AND length(created_at)=27 AND created_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]Z'),
                        updated_at TEXT COLLATE BINARY NOT NULL
                            CHECK(typeof(updated_at)='text' AND length(updated_at)=27 AND updated_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]Z' AND updated_at>=created_at),
                        expires_at TEXT COLLATE BINARY NOT NULL
                            CHECK(typeof(expires_at)='text' AND length(expires_at)=27 AND expires_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]Z' AND expires_at>created_at),
                        UNIQUE(subject,profile_id,route_operation,key),
                        FOREIGN KEY(job_id,subject,profile_id) REFERENCES jobs(id,subject,profile_id) ON DELETE RESTRICT,
                        CHECK(phase<>'final' OR outcome_snapshot IS NOT NULL),
                        CHECK(pending_outcome=1 OR phase='final')
                    )""",  # noqa: E501
                    "CREATE INDEX IF NOT EXISTS jobs_scope ON jobs(subject,profile_id,id)",
                    "CREATE INDEX IF NOT EXISTS jobs_recovery ON jobs(status,id)",
                    "CREATE INDEX IF NOT EXISTS jobs_slots ON jobs(slot_held,status,id)",
                    "CREATE UNIQUE INDEX IF NOT EXISTS jobs_profile_active ON jobs(profile_id) WHERE status='running'",
                    "CREATE INDEX IF NOT EXISTS job_requests_expiry "
                    "ON job_requests(phase,pending_outcome,expires_at COLLATE BINARY,job_id)",
                    "CREATE INDEX IF NOT EXISTS job_requests_pending ON job_requests(pending_outcome,job_id)",
                )
                for statement in job_schema:
                    self._connection.execute(statement)
                    name = statement.split()[6 if statement.startswith("CREATE UNIQUE") else 5]
                    actual = self._connection.execute("SELECT sql FROM sqlite_master WHERE name=?", (name,)).fetchone()
                    expected = statement.replace(" IF NOT EXISTS", "")
                    if actual is None or ControlTransaction._tokens(actual[0]) != ControlTransaction._tokens(expected):
                        raise ControlError("E_STORAGE")
                    if statement.startswith("CREATE TABLE"):
                        columns = findall(r"(?m)^\s+(\w+) (TEXT|INTEGER|REAL)([^\n]*)", statement)
                        shape = tuple(
                            (
                                key,
                                kind,
                                int("NOT NULL" in tail),
                                next(iter(findall(r"DEFAULT ('[^']*'|\d+)", tail)), None),
                                int("PRIMARY KEY" in tail),
                            )
                            for key, kind, tail in columns
                        )
                        info = self._connection.execute(
                            'SELECT name,type,"notnull",dflt_value,pk FROM pragma_table_info(?)', (name,)
                        )
                        if tuple(map(tuple, info)) != shape:
                            raise ControlError("E_STORAGE")
                        fks = self._connection.execute("SELECT * FROM pragma_foreign_key_list(?)", (name,)).fetchall()
                        if len(fks) != (1 if name == "jobs" else 4):
                            raise ControlError("E_STORAGE")
                    else:
                        keys = self._connection.execute("SELECT coll FROM pragma_index_xinfo(?) WHERE key=1", (name,))
                        if any(row[0] != "BINARY" for row in keys):
                            raise ControlError("E_STORAGE")
                for table, indexes in (
                    ("jobs", {"jobs_scope", "jobs_recovery", "jobs_slots", "jobs_profile_active"}),
                    ("job_requests", {"job_requests_expiry", "job_requests_pending"}),
                ):
                    listed = self._connection.execute("SELECT name FROM pragma_index_list(?) WHERE origin='c'", (table,))
                    if {row[0] for row in listed} != indexes:
                        raise ControlError("E_STORAGE")
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

    def borrow_transaction(self) -> AbstractContextManager[ControlTransaction]:
        return ControlTransaction(self)

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
        # Q7 deferred: redact at the producer, never by caller discipline.
        # Persisted roots remain private; resolve() uses the admitted binding.
        return ProfileRecord(
            row["id"],
            None,
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

    def _page(
        self, scope: tuple[str, ...], *, limit: int, after: str | None, upper: str | None
    ) -> tuple[tuple[ProfileRecord, ...], str | None]:
        connection = self._connection
        if connection is None:
            raise ControlError("E_STORAGE")
        if upper is not None and upper not in scope:
            raise ControlError("E_VALIDATION")
        placeholders = ",".join("?" for _ in scope) or "NULL"
        try:
            # Deferred/read transaction: upper and rows use the same snapshot.
            connection.execute("BEGIN")
            try:
                if upper is None:
                    row = connection.execute(
                        f"SELECT id FROM profiles WHERE id IN ({placeholders}) ORDER BY id DESC LIMIT 1", scope
                    ).fetchone()
                    upper = row[0] if row else None
                rows = connection.execute(
                    f"SELECT * FROM profiles WHERE id IN ({placeholders}) AND id>? AND id<=? ORDER BY id LIMIT ?",
                    (*scope, after or "", upper or "", limit + 1),
                ).fetchall()
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        except sqlite3.Error:
            raise ControlError("E_STORAGE") from None
        return tuple(self._record(row) for row in rows[:limit]), upper

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
                "INSERT INTO control_journal "
                "(generation,profile_id,operation,desired_state,enabled,display_name,description) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (generation, profile_id, operation, desired, int(flag), name, desc),
            )
            self._checkpoint("after_journal")
            result = self._get(profile_id)
        self._checkpoint("after_commit")
        return result

    @staticmethod
    def control_digest(value: object) -> str:
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def _request(self, subject: str, profile_id: str, operation: str, idem_key: str):
        return self._connection.execute(
            "SELECT *,http_operation AS request_operation FROM control_journal "
            "WHERE subject=? AND profile_id=? AND http_operation=? AND idem_key=?",
            (subject, profile_id, operation, idem_key),
        ).fetchone()

    @staticmethod
    def _request_record(row) -> MappingProxyType:
        value = dict(row)
        value["operation"] = value["http_operation"]
        return MappingProxyType(value)

    def reserve_control(
        self,
        *,
        subject: str,
        operation: str,
        idem_key: str,
        digest: str,
        request_id: str,
        profile_id: str,
        desired_state: str,
        expected_generation: int,
        expires_at: datetime,
        ticket_id: str,
        now: datetime,
        capabilities: tuple[str, ...],
        eligible: bool = True,
        require_intent: Callable[[], None],
    ) -> MappingProxyType:
        """Replay lookup, reservation metadata, CAS and one generation row: one commit."""
        with self._transaction():
            row = self._request(subject, profile_id, operation, idem_key)
            if row is not None:
                expired = datetime.fromisoformat(row["expires_at"]) <= now
                if expired and row["phase"] == "final" and not row["pending_publication"]:
                    # Keep the generation/effect forever, release only its TTL key.
                    self._connection.execute(
                        "UPDATE control_journal SET idem_key=NULL WHERE generation=?", (row["generation"],)
                    )
                else:
                    if row["payload_sha256"] != digest:
                        raise ControlError("E_IDEMPOTENCY_CONFLICT")
                    if row["phase"] != "final":
                        raise ControlError("E_REQUEST_IN_PROGRESS")
                    return self._request_record(row)
            require_intent()
            if (
                not eligible
                or self._connection.execute(
                    "SELECT 1 FROM control_journal WHERE pending_publication=1 AND profile_id=? LIMIT 1",
                    (profile_id,),
                ).fetchone()
            ):
                raise ControlError("E_PROFILE_BUSY")
            before = self._get(profile_id)
            if before.generation != expected_generation:
                raise ControlError("E_VERSION_CONFLICT")
            generation = self._connection.execute(
                "UPDATE registry_state SET generation=generation+1 WHERE singleton=1 RETURNING generation"
            ).fetchone()[0]
            enabled = desired_state == "enabled"
            changed = self._connection.execute(
                "UPDATE profiles SET enabled=?,desired_state=?,generation=? WHERE id=? AND generation=?",
                (int(enabled), desired_state, generation, profile_id, expected_generation),
            )
            if changed.rowcount != 1:
                raise ControlError("E_VERSION_CONFLICT")
            self._checkpoint("after_control_cas")
            snapshot = dict(
                id=profile_id,
                display_name=before.display_name,
                description=before.description,
                enabled=enabled,
                generation=generation,
                runtime_state="not_started",
                capabilities=list(capabilities),
                storage_root=None,
            )
            self._connection.execute(
                "INSERT INTO control_journal "
                "(generation,profile_id,operation,desired_state,enabled,display_name,description,"
                "subject,http_operation,idem_key,payload_sha256,request_id,phase,response_snapshot,"
                "outcome_snapshot,expires_at,pending_publication,ticket_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    generation,
                    profile_id,
                    "enabled",
                    desired_state,
                    int(enabled),
                    before.display_name,
                    before.description,
                    subject,
                    operation,
                    idem_key,
                    digest,
                    request_id,
                    "committed",
                    json.dumps(snapshot, separators=(",", ":")),
                    None,
                    expires_at.isoformat(),
                    1,
                    ticket_id,
                ),
            )
            self._checkpoint("after_control_journal")
            result = self._request_record(self._request(subject, profile_id, operation, idem_key))
        self._checkpoint("after_control_commit")
        return result

    def finish_control(self, row, *, response: object, outcome: object) -> None:
        with self._transaction():
            self._connection.execute(
                "UPDATE control_journal SET phase='final',response_snapshot=?,outcome_snapshot=? "
                "WHERE generation=? AND phase='committed'",
                (
                    json.dumps(response, separators=(",", ":")),
                    json.dumps(outcome, separators=(",", ":")),
                    row["generation"],
                ),
            )

    def control_requests(self, *, limit: int, after_generation: int = 0) -> tuple[MappingProxyType, ...]:
        if self._connection is None:
            raise ControlError("E_STORAGE")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ControlError("E_VALIDATION")
        try:
            return tuple(
                self._request_record(row)
                for row in self._connection.execute(
                    "SELECT * FROM control_journal WHERE pending_publication=1 "
                    "ORDER BY generation<=?,generation LIMIT ?", (after_generation, limit)
                ).fetchall()
            )
        except sqlite3.Error:
            raise ControlError("E_STORAGE") from None

    def mark_published(self, row) -> None:
        with self._transaction():
            self._connection.execute(
                "UPDATE control_journal SET pending_publication=0 WHERE generation=? AND phase='final'",
                (row["generation"],),
            )

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
                    "SELECT generation,profile_id,operation,desired_state,enabled,display_name,description "
                    f"FROM control_journal WHERE profile_id IN ({placeholders}) ORDER BY generation",
                    ids,
                )
            )
        except sqlite3.Error:
            raise ControlError("E_STORAGE") from None
