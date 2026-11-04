"""Direct JobPort adapter; authentication precedes scoped metadata access."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from uuid import UUID

from memory_server.control.jobs import JobError, canonical_json, request_digest, validated_payload
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import AuditOutcome, AuditTicket


@dataclass(frozen=True)
class JobService:
    metadata: object
    registry: object
    tokens: object
    intent: object
    publish: object
    attach: object = None
    events: object = None
    clock: object = lambda: datetime.now(UTC)

    async def _authorize(self, ctx, job_id=None):
        if ctx.principal is None:
            raise JobError("E_AUTH_REQUIRED")
        try:
            self.tokens.verify_principal(ctx.principal, now=self.clock())
        except Exception as exc:
            raise JobError(getattr(exc, "code", "E_AUTH_REQUIRED")) from None
        if "index.manage" not in ctx.principal.permissions:
            raise JobError("E_PERMISSION_DENIED")
        if ctx.profile_id is None or ctx.profile_id not in ctx.principal.profiles:
            raise JobError("E_PERMISSION_DENIED")
        if job_id is not None and (type(job_id) is not str or re.fullmatch(r"[0-9a-f]{48}", job_id) is None):
            raise JobError("E_VALIDATION")
        try:
            await self.registry.resolve(ctx.principal, ctx.profile_id)
        except Exception as exc:
            raise JobError(getattr(exc, "code", "E_INTERNAL")) from None

    @staticmethod
    def _deadline(ctx):
        if type(ctx.deadline) not in (int, float) or not math.isfinite(ctx.deadline):
            raise JobError("E_VALIDATION")
        if monotonic() >= ctx.deadline:
            raise JobError("E_TIMEOUT")

    async def get(self, ctx, job_id):
        await self._authorize(ctx, job_id)
        self._deadline(ctx)
        try:
            job = self.metadata.get(job_id, ctx.profile_id, ctx.principal.subject)
        except Exception as exc:
            raise JobError(getattr(exc, "code", "E_STORAGE")) from None
        if job is None:
            raise JobError("E_NOT_FOUND")
        return job

    async def _publish(self, reservation, now):
        if reservation.outcome_snapshot is None:
            reservation = self.metadata.finalize_request(reservation, AuditOutcome(202, None, (), 0.0), now=now)
        await self.publish(AuditTicket(reservation.ticket_id, reservation.request_id), reservation.outcome_snapshot)
        self.metadata.mark_published(reservation, now=now)
        return reservation

    async def enqueue(self, ctx, operation, payload):
        await self._authorize(ctx)
        payload = validated_payload(operation, payload)
        self._deadline(ctx)
        if ctx.idempotency_key is None:
            raise JobError("E_IDEMPOTENCY_REQUIRED")
        if type(ctx.idempotency_key) is not UUID:
            raise JobError("E_VALIDATION")
        if not callable(self.attach):
            raise JobError("E_CAPABILITY_UNAVAILABLE")
        now = self.clock()
        try:
            for pending in self.metadata.pending_outcomes(limit=100, deadline=ctx.deadline):
                await self._publish(pending, now)
            self.metadata.cleanup_expired(now=now, limit=100, deadline=ctx.deadline)
            ticket = await self.intent(ctx, "outbox.replay" if operation == "outbox_replay" else "indexes.rebuild")
        except Exception:
            raise JobError("E_AUDIT_UNAVAILABLE") from None
        reservation = self.metadata.enqueue(
            ctx,
            operation,
            payload,
            digest=request_digest(operation, payload),
            ticket=ticket,
            now=now,
            ttl_seconds=86400,
        )
        if reservation.replayed:
            return reservation.accepted
        try:
            reservation = await self._publish(reservation, now)
        except Exception:
            raise JobError("E_AUDIT_UNAVAILABLE") from None
        if self.events is not None:
            event = d.Event(
                id=reservation.job_id,
                type="job.updated",
                profile_id=ctx.profile_id,
                timestamp=now,
                request_id=reservation.request_id,
                memory_id=None,
                job_id=reservation.job_id,
                severity="info",
                data=d.EventData(
                    revision=0, from_state=None, to_state="queued", operation=operation, pending=None, code=None
                ),
            )
            if len(canonical_json(event.data).encode("utf-8")) > 4096:
                raise JobError("E_INTERNAL")
            await self.events(event)
        await self.attach(ctx, reservation)
        return reservation.accepted
