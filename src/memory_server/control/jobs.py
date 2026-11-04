"""Durable job metadata; the borrowed transaction is the only SQL boundary."""

from __future__ import annotations

import hashlib
import json
import math
import secrets
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from time import monotonic

from pydantic import BaseModel

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import AuditOutcome

from .store import ControlError


class JobError(Exception):
    def __init__(self, code: d.ErrorCode, *, retry_after: int | None = None):
        self.code, self.retry_after = code, retry_after
        super().__init__(code)


def canonical_json(value, *, code="E_REQUEST_TOO_LARGE"):
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    if len(encoded.encode("utf-8")) > 65536:
        raise JobError(code)
    return encoded


def request_digest(operation, payload):
    route = "outbox.replay" if operation == "outbox_replay" else "indexes.rebuild"
    return hashlib.sha256(
        canonical_json(
            {"method": "POST", "operation": route, "path_ids": {}, "payload": payload.model_dump(mode="json")}
        ).encode("utf-8")
    ).hexdigest()


def stamp(now):
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise JobError("E_VALIDATION")
    return now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def validated_payload(operation, payload):
    model = d.MaintenanceRequest if operation == "outbox_replay" else d.RebuildRequest
    if operation not in ("outbox_replay", "vector_rebuild", "graph_rebuild", "projections_rebuild"):
        raise JobError("E_VALIDATION")
    try:
        value = model.model_validate_json(canonical_json(payload))
        if (
            model is d.RebuildRequest
            and set(value.targets)
            != {"vector": {"vector"}, "graph": {"graph"}, "projections": {"vector", "graph"}}[
                operation.removesuffix("_rebuild")
            ]
        ):
            raise ValueError()
        return value
    except (ValueError, TypeError):
        raise JobError("E_VALIDATION") from None


@dataclass(frozen=True)
class Reservation:
    subject: str
    profile_id: str
    route_operation: str
    key: str
    digest: str
    job_id: str
    ticket_id: str
    request_id: str
    accepted: d.JobAccepted
    phase: str
    outcome_snapshot: AuditOutcome | None
    pending_outcome: bool
    expires_at: str
    replayed: bool = False


@dataclass(frozen=True)
class Claim:
    job: d.Job
    subject: str
    revision: int
    payload: d.MaintenanceRequest | d.RebuildRequest


@dataclass(frozen=True)
class RecoveryRecord:
    claim: Claim
    phase: str
    recovery_metadata: str


@dataclass(frozen=True)
class TerminalUpdate:
    status: str
    result: d.JobResult | None
    error_code: d.ErrorCode | None
    now: datetime
    phase: str = "terminal"


_SCOPE = "SELECT * FROM jobs WHERE id=? AND profile_id=? AND subject=? LIMIT 1"
_REQUEST = "SELECT * FROM job_requests WHERE subject=? AND profile_id=? AND route_operation=? AND key=? LIMIT 1"
_TERMINAL = ("succeeded", "failed", "interrupted")


def _key(r):
    return r.subject, r.profile_id, r.route_operation, r.key


def _job(row):
    try:
        job = d.Job.model_validate_json(row["dto_snapshot"])
        canonical_json(job, code="E_STORAGE")
        if set(json.loads(row["dto_snapshot"])) != set(d.Job.model_fields):
            raise ValueError()
        for key in (
            "id",
            "profile_id",
            "token_id",
            "operation",
            "status",
            "progress",
            "created_at",
            "started_at",
            "finished_at",
            "request_id",
        ):
            value = getattr(job, key)
            if (stamp(value) if isinstance(value, datetime) else value) != row[key]:
                raise ValueError()
        if (job.status in _TERMINAL) != (row["slot_held"] == 0):
            raise ValueError()
        return job
    except (ValueError, TypeError, KeyError):
        raise JobError("E_STORAGE") from None


def _reservation(row, replayed=False):
    try:
        accepted = d.JobAccepted.model_validate_json(row["accepted_snapshot"])
        if (accepted.job_id, accepted.request_id) != (row["job_id"], row["request_id"]):
            raise ValueError()
        outcome = json.loads(row["outcome_snapshot"]) if row["outcome_snapshot"] else None
        if outcome is not None:
            outcome["receipt_ids"] = tuple(outcome["receipt_ids"])
            outcome = AuditOutcome(**outcome)
        return Reservation(
            *(
                row[k]
                for k in (
                    "subject",
                    "profile_id",
                    "route_operation",
                    "key",
                    "digest",
                    "job_id",
                    "ticket_id",
                    "request_id",
                )
            ),
            accepted,
            row["phase"],
            outcome,
            bool(row["pending_outcome"]),
            row["expires_at"],
            replayed,
        )
    except (ValueError, TypeError, KeyError):
        raise JobError("E_STORAGE") from None


def _claim(row):
    try:
        return Claim(
            _job(row),
            row["subject"],
            row["revision"],
            validated_payload(row["operation"], json.loads(row["validated_request"])),
        )
    except (ValueError, TypeError, JobError):
        raise JobError("E_STORAGE") from None


class JobMetadata:
    def __init__(self, store):
        self.store = store

    def enqueue(self, ctx, operation, payload, *, digest, ticket, now, ttl_seconds=86400):
        payload = validated_payload(operation, payload)
        created = stamp(now)
        if type(ttl_seconds) is not int or ttl_seconds < 1:
            raise JobError("E_VALIDATION")
        route = "outbox.replay" if operation == "outbox_replay" else "indexes.rebuild"
        key = (ctx.principal.subject, ctx.profile_id, route, str(ctx.idempotency_key))
        try:
            with self.store.borrow_transaction() as tx:
                previous = tx.fetchone(_REQUEST, key)
                if previous:
                    if previous["digest"] != digest:
                        raise JobError("E_IDEMPOTENCY_CONFLICT")
                    if previous["phase"] != "final":
                        raise JobError("E_REQUEST_IN_PROGRESS", retry_after=1)
                    return _reservation(previous, True)
                if len(tx.fetchmany("SELECT id FROM jobs WHERE slot_held=1 LIMIT 128", limit=128)) == 128:
                    raise JobError("E_RATE_LIMIT")
                for _ in range(3):
                    job_id = secrets.token_hex(24)
                    if tx.fetchone("SELECT id FROM jobs WHERE id=? LIMIT 1", (job_id,)) is None:
                        break
                else:
                    raise JobError("E_STORAGE")
                job = d.Job(
                    id=job_id,
                    profile_id=ctx.profile_id,
                    token_id=ctx.principal.token_id,
                    operation=operation,
                    status="queued",
                    progress=None,
                    created_at=now,
                    started_at=None,
                    finished_at=None,
                    request_id=ctx.request_id,
                    result=None,
                    error_code=None,
                )
                accepted = d.JobAccepted(job_id=job_id, status="queued", request_id=ctx.request_id)
                tx.execute(
                    "INSERT INTO jobs (id,profile_id,subject,token_id,operation,created_at,updated_at,"
                    "request_id,dto_snapshot,validated_request) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        job_id,
                        ctx.profile_id,
                        ctx.principal.subject,
                        ctx.principal.token_id,
                        operation,
                        created,
                        created,
                        ctx.request_id,
                        canonical_json(job),
                        canonical_json(payload),
                    ),
                )
                tx.execute(
                    "INSERT INTO job_requests (subject,profile_id,route_operation,key,digest,job_id,ticket_id,"
                    "request_id,accepted_snapshot,created_at,updated_at,expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        *key,
                        digest,
                        job_id,
                        ticket.id,
                        ctx.request_id,
                        canonical_json(accepted),
                        created,
                        created,
                        stamp(now + timedelta(seconds=ttl_seconds)),
                    ),
                )
                reservation = _reservation(tx.fetchone(_REQUEST, key))
            return reservation
        except ControlError:
            raise JobError("E_STORAGE") from None

    def get(self, job_id, profile_id, subject):
        with self.store.borrow_transaction() as tx:
            row = tx.fetchone(_SCOPE, (job_id, profile_id, subject))
            job = _job(row) if row else None
        return job

    def claim(self, job_id, profile_id, subject, expected_revision, *, now):
        with self.store.borrow_transaction() as tx:
            row = tx.fetchone(_SCOPE, (job_id, profile_id, subject))
            if not row or row["status"] != "queued" or row["revision"] != expected_revision:
                return None
            if tx.fetchone("SELECT id FROM jobs WHERE profile_id=? AND status='running' LIMIT 1", (profile_id,)):
                return None
            job = _job(row).model_copy(update={"status": "running", "started_at": now})
            changed = tx.execute(
                "UPDATE jobs SET status='running',phase='claimed',revision=revision+1,started_at=?,updated_at=?,"
                "dto_snapshot=? WHERE id=? AND profile_id=? AND subject=? AND revision=? AND status='queued'",
                (stamp(now), stamp(now), canonical_json(job), job_id, profile_id, subject, expected_revision),
            )
            claim = _claim(tx.fetchone(_SCOPE, (job_id, profile_id, subject))) if changed == 1 else None
        return claim

    def finish(self, job_id, profile_id, subject, expected_revision, expected_status, terminal_update):
        u = terminal_update
        with self.store.borrow_transaction() as tx:
            row = tx.fetchone(_SCOPE, (job_id, profile_id, subject))
            if not row:
                return None
            job = _job(row)
            if job.status in _TERMINAL:
                return job
            if row["revision"] != expected_revision or job.status != expected_status:
                return None
            try:
                if (
                    u.status not in _TERMINAL
                    or u.phase not in ("terminal", "reconcile")
                    or (u.phase == "reconcile" and u.status != "interrupted")
                ):
                    raise ValueError()
                if u.status == "succeeded":
                    verified = d.JobResult.model_validate_json(canonical_json(u.result, code="E_INTERNAL"))
                    if (
                        not 0 <= verified.completed <= verified.attempted <= verified.selected
                        or verified.failed != verified.attempted - verified.completed
                        or u.error_code is not None
                    ):
                        raise ValueError()
                elif u.result is not None or u.error_code is None:
                    raise ValueError()
                updated = d.Job.model_validate_json(
                    canonical_json(
                        job.model_copy(
                            update={
                                "status": u.status,
                                "finished_at": u.now,
                                "result": u.result,
                                "error_code": u.error_code,
                            }
                        ),
                        code="E_INTERNAL",
                    )
                )
            except (ValueError, TypeError):
                raise JobError("E_INTERNAL") from None
            changed = tx.execute(
                "UPDATE jobs SET status=?,phase=?,revision=revision+1,finished_at=?,updated_at=?,slot_held=0,"
                "dto_snapshot=?,recovery_metadata=? "
                "WHERE id=? AND profile_id=? AND subject=? AND revision=? AND status=?",
                (
                    u.status,
                    u.phase,
                    stamp(u.now),
                    stamp(u.now),
                    canonical_json(updated),
                    canonical_json({"v": 1, "phase": u.phase}),
                    job_id,
                    profile_id,
                    subject,
                    expected_revision,
                    expected_status,
                ),
            )
            committed = _job(tx.fetchone(_SCOPE, (job_id, profile_id, subject))) if changed == 1 else None
        return committed

    def complete(self, claim, result, *, now):
        return self.finish(
            claim.job.id,
            claim.job.profile_id,
            claim.subject,
            claim.revision,
            "running",
            TerminalUpdate("succeeded", result, None, now),
        )

    def fail(self, claim, code, *, now):
        return self.finish(
            claim.job.id,
            claim.job.profile_id,
            claim.subject,
            claim.revision,
            "running",
            TerminalUpdate("interrupted" if code == "E_JOB_INTERRUPTED" else "failed", None, code, now),
        )

    def progress(self, claim, value, *, now):
        with self.store.borrow_transaction() as tx:
            row = tx.fetchone(_SCOPE, (claim.job.id, claim.job.profile_id, claim.subject))
            if not row or row["status"] != "running" or row["revision"] != claim.revision:
                return None
            if (
                type(value) not in (float, int)
                or not math.isfinite(value)
                or not 0 <= value <= 1
                or (row["progress"] is not None and value < row["progress"])
            ):
                raise JobError("E_VALIDATION")
            job = _job(row).model_copy(update={"progress": float(value)})
            tx.execute(
                "UPDATE jobs SET progress=?,updated_at=?,revision=revision+1,dto_snapshot=? "
                "WHERE id=? AND profile_id=? AND subject=? AND revision=? AND status='running'",
                (value, stamp(now), canonical_json(job), job.id, job.profile_id, claim.subject, claim.revision),
            )
            updated = _claim(tx.fetchone(_SCOPE, (job.id, job.profile_id, claim.subject)))
        return updated

    @staticmethod
    def _bound(limit, deadline):
        if type(limit) is not int or not 1 <= limit <= 1000 or not math.isfinite(deadline):
            raise JobError("E_VALIDATION")
        return monotonic() < deadline

    def scan_reconcile(self, *, limit=100, deadline):
        if not self._bound(limit, deadline):
            return ()
        with self.store.borrow_transaction() as tx:
            records = tuple(
                RecoveryRecord(_claim(row), row["phase"], row["recovery_metadata"])
                for row in tx.fetchmany(
                    "SELECT * FROM jobs WHERE status='running' ORDER BY id LIMIT ?", (limit,), limit=limit
                )
                if monotonic() < deadline
            )
        return records

    def cleanup_expired(self, *, now, limit=100, deadline):
        if not self._bound(limit, deadline):
            return 0
        removed = 0
        with self.store.borrow_transaction() as tx:
            rows = tx.fetchmany(
                "SELECT * FROM job_requests WHERE phase='final' AND pending_outcome=0 AND expires_at<=? "
                "ORDER BY expires_at,job_id LIMIT ?",
                (stamp(now), limit),
                limit=limit,
            )
            for row in rows:
                if monotonic() >= deadline:
                    break
                deleted = tx.execute(
                    "DELETE FROM job_requests WHERE subject=? AND profile_id=? AND route_operation=? AND key=? "
                    "AND phase='final' AND pending_outcome=0 AND expires_at<=? AND job_id IN "
                    "(SELECT id FROM jobs WHERE slot_held=0 AND status IN ('succeeded','failed','interrupted'))",
                    (*_key(_reservation(row)), stamp(now)),
                )
                if deleted:
                    tx.execute(
                        "UPDATE jobs SET validated_request=?,recovery_metadata=? "
                        "WHERE id=? AND profile_id=? AND subject=? AND slot_held=0",
                        ('{"v":1}', '{"v":1}', row["job_id"], row["profile_id"], row["subject"]),
                    )
                    removed += deleted
        return removed

    def finalize_request(self, reservation, outcome, *, now):
        with self.store.borrow_transaction() as tx:
            tx.execute(
                "UPDATE job_requests SET phase='final',outcome_snapshot=?,updated_at=? "
                "WHERE subject=? AND profile_id=? AND route_operation=? AND key=? AND digest=? AND phase='committed'",
                (canonical_json(asdict(outcome)), stamp(now), *_key(reservation), reservation.digest),
            )
            updated = _reservation(tx.fetchone(_REQUEST, _key(reservation)))
        return updated

    def pending_outcomes(self, *, limit=100, deadline):
        if not self._bound(limit, deadline):
            return ()
        with self.store.borrow_transaction() as tx:
            records = tuple(
                _reservation(row)
                for row in tx.fetchmany(
                    "SELECT * FROM job_requests WHERE pending_outcome=1 ORDER BY job_id LIMIT ?", (limit,), limit=limit
                )
                if monotonic() < deadline
            )
        return records

    def mark_published(self, reservation, *, now):
        with self.store.borrow_transaction() as tx:
            tx.execute(
                "UPDATE job_requests SET pending_outcome=0,updated_at=? "
                "WHERE subject=? AND profile_id=? AND route_operation=? AND key=? AND digest=? AND phase='final'",
                (stamp(now), *_key(reservation), reservation.digest),
            )
