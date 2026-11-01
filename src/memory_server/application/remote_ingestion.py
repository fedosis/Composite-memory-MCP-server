"""Remote-safe remember orchestration over the request-wide mutation UoW."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from storage.outbox import OutboxEntryORM
from storage.repositories.fact_repo import FactRepository
from storage.repositories.http_request_repo import HttpRequestRepository
from storage.repositories.lifecycle_repo import normalize_version

from memory_server.admission import AdmissionDecision, MemoryAdmissionGate
from memory_server.application.contracts import RememberCommand
from memory_server.application.mutation_uow import MUTATION_REPLY, MutationUoW
from memory_server.extractors.decision_extractor import DecisionExtractor
from memory_server.extractors.fact_extractor import FactExtractor
from memory_server.extractors.noise_filter import filter_facts
from memory_server.extractors.skill_extractor import SkillExtractor
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import AuditOutcome, AuditTicket, GatewayDependencies, RequestContext
from memory_server.services.ingestion_service import check_fact_version, remote_learn_in_session
from memory_server.transports.http.errors import HttpError


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def write_status(result: d.WriteResult) -> int:
    """Return 201 when the result created an item, otherwise 200 for reinforce."""
    reinforced = set(result.reinforced_ids)
    return 201 if any(memory.id not in reinforced for memory in result.memories) else 200


def _history_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return _aware(value)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return _aware(parsed)
    return None


def _history_events(receipt) -> list[d.HistoryEvent]:
    allowed = {"receipt", "lifecycle", "relation", "confidence", "admission"}
    events: list[d.HistoryEvent] = []
    for index, raw in enumerate(receipt.history):
        if not isinstance(raw, dict):
            continue
        kind = raw.get("kind")
        if kind not in allowed:
            continue
        revision = raw.get("revision")
        events.append(
            d.HistoryEvent(
                id=f"{receipt.id}:{index}",
                kind=kind,
                timestamp=_history_timestamp(raw.get("timestamp")),
                actor=None,
                reason=None,
                revision=revision if type(revision) is int else None,
                from_state=raw.get("from_state") if isinstance(raw.get("from_state"), str) else None,
                to_state=raw.get("to_state") if isinstance(raw.get("to_state"), str) else None,
                receipt_id=raw.get("receipt_id") if isinstance(raw.get("receipt_id"), str) else None,
                evidence=None,
            )
        )
    return events


def _memory_dto(fact, receipt_id: str) -> d.Memory:
    revision = normalize_version(fact.version)
    return d.Memory(
        id=fact.id,
        memory_type="fact",
        content=None,
        payload=None,
        confidence=fact.confidence,
        lifecycle_state=fact.lifecycle_state,
        revision=revision,
        source_version=revision,
        source=None,
        creator=None,
        created_at=_aware(fact.created_at),
        updated_at=_aware(fact.updated_at),
        risk_tags=[],
        receipt_id=receipt_id,
        masked=True,
        projection=d.ItemProjection(vector="unknown", graph="unknown", outbox="pending"),
    )


def _receipt_dto(receipt) -> d.Receipt:
    verification = getattr(receipt.verification_status, "value", receipt.verification_status)
    return d.Receipt(
        id=receipt.id,
        memory_type=receipt.memory_type,
        source=receipt.source,
        created_by=receipt.created_by,
        timestamp=_aware(receipt.timestamp),
        confidence=receipt.confidence,
        verification_status=verification,
        lifecycle_state=receipt.lifecycle_state,
        source_version=receipt.version,
        history=_history_events(receipt),
        history_complete=False,
    )


def _admission_dto(decision: AdmissionDecision) -> d.AdmissionResult:
    metadata = decision.metadata
    return d.AdmissionResult(
        admitted=decision.admitted,
        tag=decision.tag.value,
        ttl_days=decision.ttl_days,
        score=decision.score,
        memory_kind=metadata.get("memory_kind") if isinstance(metadata.get("memory_kind"), str) else None,
        authority_level=(
            metadata.get("authority_level") if isinstance(metadata.get("authority_level"), str) else None
        ),
        epistemic_status=(
            metadata.get("epistemic_status") if isinstance(metadata.get("epistemic_status"), str) else None
        ),
        risk_tags=[tag for tag in metadata.get("risk_tags", []) if isinstance(tag, str)],
        reason_codes=list(decision.reason_codes),
    )


class RememberHandler:
    """Mutation handler that uses only the session supplied by ``MutationUoW``."""

    def __init__(self, scope, ctx: RequestContext, decision: AdmissionDecision):
        self._memory = scope.application.memory
        self._ctx = ctx
        self._decision = decision

    async def apply(self, session: AsyncSession, command: d.ProfileMutationCommand) -> d.WriteResult:
        if command.operation != "memories.create" or not isinstance(command, d.COMMAND_TYPES["memories.create"]):
            raise HttpError("E_VALIDATION")
        payload = command.payload
        existing = await FactRepository(session).find_existing(payload.subject, payload.predicate, payload.object)
        reinforced_ids: list[str]
        if existing is not None:
            actual = normalize_version(existing.version)
            if payload.expected_version is None:
                raise HttpError("E_PRECONDITION_REQUIRED", details={"id": existing.id})
            if actual != payload.expected_version:
                raise HttpError(
                    "E_VERSION_CONFLICT", details={"expected": payload.expected_version, "actual": actual}
                )
            reinforced_ids = [existing.id]
        else:
            if payload.expected_version is not None:
                raise HttpError("E_VALIDATION")
            reinforced_ids = []

        result = await self._memory.remember_in_session(
            session,
            RememberCommand(
                subject=payload.subject,
                predicate=payload.predicate,
                object=payload.object,
                confidence=payload.confidence,
                source=payload.source,
                metadata={"admission": self._decision.to_metadata()},
            ),
        )
        fact, receipt = result["fact"], result["receipt"]
        if not reinforced_ids and receipt.history and receipt.history[-1].get("kind") == "reinforce":
            raise HttpError("E_VERSION_CONFLICT")
        outbox_ids = list(
            (
                await session.execute(
                    select(OutboxEntryORM.id).where(OutboxEntryORM.record_id == fact.id).order_by(OutboxEntryORM.id)
                )
            )
            .scalars()
            .all()
        )
        return d.WriteResult(
            request_id=self._ctx.request_id,
            memories=[_memory_dto(fact, receipt.id)],
            receipts=[_receipt_dto(receipt)],
            admission=[_admission_dto(self._decision)],
            outbox_ids=outbox_ids,
            reinforced_ids=reinforced_ids,
        )


@dataclass
class LearnPlan:
    """Extractor candidates that survived the remote admission boundary."""

    facts: list[dict]
    decisions: list[dict]
    skills: list[dict]
    admission: list[AdmissionDecision]


class LearnHandler:
    """CAS preflight and masked mapping on the request-wide injected session."""

    def __init__(self, scope, ctx: RequestContext, *, admitted: LearnPlan, payload: d.LearnRequest):
        self._ctx = ctx
        self._admitted = admitted
        self._payload = payload

    async def apply(self, session: AsyncSession, command: d.ProfileMutationCommand) -> d.WriteResult:
        payload = self._payload
        for candidate in self._admitted.facts:
            existing = await FactRepository(session).find_existing(
                *(candidate.get(k, "") for k in ("subject", "predicate", "object"))
            )
            if existing is not None:
                check_fact_version(existing, payload.expected_versions)
        result = await remote_learn_in_session(
            session,
            text=payload.text,
            source=payload.source,
            admitted=self._admitted,
            extract_beliefs=payload.extract_beliefs,
            min_belief_confidence=payload.min_belief_confidence,
            expected_versions=payload.expected_versions,
        )
        memories = []
        for kind, rows in (
            ("fact", result.facts),
            ("decision", result.decisions),
            ("skill", result.skills),
            ("belief", result.beliefs),
        ):
            for row in rows:
                semantic = kind in {"decision", "skill"}
                revision = None if semantic else normalize_version(row.version)
                memories.append(
                    d.Memory(
                        id=row.id,
                        memory_type=kind,
                        content=None,
                        payload=None,
                        confidence=row.confidence,
                        lifecycle_state=row.lifecycle_state,
                        revision=revision,
                        source_version=row.version if semantic else int(row.version),
                        source=None,
                        creator=None,
                        created_at=_aware(row.created_at),
                        updated_at=_aware(row.updated_at),
                        risk_tags=[],
                        receipt_id=row.id if any(r.id == row.id for r in result.receipts) else None,
                        masked=True,
                        projection=d.ItemProjection(vector="unknown", graph="unknown", outbox="pending"),
                    )
                )
        return d.WriteResult(
            request_id=self._ctx.request_id,
            memories=memories,
            receipts=[_receipt_dto(receipt) for receipt in result.receipts],
            admission=[_admission_dto(decision) for decision in self._admitted.admission],
            outbox_ids=result.outbox_ids,
            reinforced_ids=result.reinforced_ids,
        )


class RemoteIngestionPort:
    """S13 application port with mandatory server-side admission and audit publication."""

    def __init__(
        self,
        deps: GatewayDependencies,
        *,
        ticket_provider: Callable[[], str | None],
        publish_outcome: Callable[[AuditTicket, AuditOutcome], Awaitable[bool]],
        clock: Callable[[], datetime] = _utc_now,
        admission: MemoryAdmissionGate | None = None,
    ) -> None:
        if not callable(ticket_provider) or not callable(publish_outcome) or not callable(clock):
            raise ValueError("invalid remote ingestion dependencies")
        self._deps = deps
        self._ticket_provider = ticket_provider
        self._publish_outcome = publish_outcome
        self._clock = clock
        self._admission = admission or MemoryAdmissionGate()

    async def execute(self, ctx: RequestContext, command: d.IngestionCommand) -> d.WriteResult:
        started = monotonic()
        try:
            if command.operation not in {"memories.create", "learn"} or not isinstance(
                command, d.COMMAND_TYPES[command.operation]
            ):
                raise ValueError
            type(command).model_validate(command.model_dump())
        except Exception:
            raise HttpError("E_VALIDATION") from None
        if "memory.write" not in ctx.principal.permissions:
            raise HttpError("E_PERMISSION_DENIED")
        if ctx.profile_id is None or command.profile_id != ctx.profile_id:
            raise HttpError("E_VALIDATION")
        if ctx.profile_id not in ctx.principal.profiles:
            raise HttpError("E_PERMISSION_DENIED")
        if type(ctx.idempotency_key) is not UUID:
            raise HttpError("E_IDEMPOTENCY_REQUIRED")
        if not ctx.deadline > asyncio.get_running_loop().time():
            raise HttpError("E_TIMEOUT")

        payload = command.payload
        if command.operation == "memories.create":
            decision = self._admission.classify(
                f"{payload.subject} {payload.predicate} {payload.object}",
                source_scope="remote", now=self._clock(), force=False,
            )
            if not decision.admitted:
                raise HttpError("E_ADMISSION_REJECTED")
        else:
            plan = self._learn_candidates(payload)
            if not plan.admission:
                raise HttpError("E_ADMISSION_REJECTED")
        try:
            ticket = self._ticket_provider()
        except Exception:
            raise HttpError("E_AUDIT_UNAVAILABLE") from None
        if type(ticket) is not str or not 1 <= len(ticket) <= 256:
            raise HttpError("E_AUDIT_UNAVAILABLE")
        if self._deps.leases is None:
            raise HttpError("E_RUNTIME_NOT_READY")

        async with self._deps.leases.acquire(ctx) as scope:
            factory = scope.application.memory.session_factory
            uow = MutationUoW(
                factory,
                verify_principal=lambda principal: self._deps.tokens.verify_principal(principal, now=datetime.now(UTC)),
                ticket_provider=self._ticket_provider,
            )
            result = await uow.run(
                ctx,
                command,
                RememberHandler(scope, ctx, decision) if command.operation == "memories.create"
                else LearnHandler(scope, ctx, admitted=plan, payload=payload),
            )
            outcome = AuditOutcome(
                write_status(result),
                None,
                tuple(receipt.id for receipt in result.receipts),
                (monotonic() - started) * 1000,
            )
            reply = MUTATION_REPLY.get()
            replayed = reply is not None and reply.replayed
            if not replayed:
                try:
                    published = await self._publish_outcome(AuditTicket(ticket, ctx.request_id), outcome)
                except Exception:
                    published = False
                if published is True:
                    try:
                        await uow.mark_published(ctx.request_id)
                    except Exception:
                        pass
            else:
                try:
                    await self._reconcile(factory, ctx, command, result, self._publish_outcome)
                except Exception:
                    pass
            return result

    def _learn_candidates(self, payload: d.LearnRequest) -> LearnPlan:
        plan = LearnPlan([], [], [], [])
        candidates = (
            (plan.facts, filter_facts(FactExtractor().extract(payload.text))),
            (plan.decisions, DecisionExtractor().extract(payload.text)),
            (plan.skills, SkillExtractor().extract(payload.text)),
        )
        for target, rows in candidates:
            for candidate in rows:
                if target is plan.facts:
                    text = " ".join(candidate.get(k, "") for k in ("subject", "predicate", "object"))
                elif target is plan.decisions:
                    text = candidate.get("context") or candidate.get("choice", "")
                else:
                    text = candidate.get("purpose", "")
                decision = self._admission.classify(text, source_scope="remote", force=False, now=self._clock())
                if decision.admitted:
                    target.append({**candidate, "_admission": decision.to_metadata()})
                    plan.admission.append(decision)
        return plan

    async def _reconcile(
        self,
        factory: async_sessionmaker[AsyncSession],
        ctx: RequestContext,
        command: d.ProfileMutationCommand,
        result: d.WriteResult,
        publish_outcome: Callable[[AuditTicket, AuditOutcome], Awaitable[bool]],
    ) -> None:
        del result  # Reconciliation trusts only the durable, original response snapshot.
        async with factory() as session, session.begin():
            repo = HttpRequestRepository(session)
            row = await repo.find(
                ctx.principal.subject,
                ctx.profile_id,
                command.operation,
                str(ctx.idempotency_key).lower(),
            )
            if row is None or not row.pending_publication or not row.audit_ticket_id:
                return
            snapshot = d.WriteResult.model_validate_json(row.response_snapshot)
            outcome = AuditOutcome(
                write_status(snapshot),
                None,
                tuple(receipt.id for receipt in snapshot.receipts),
                0.0,
            )
            if await publish_outcome(AuditTicket(row.audit_ticket_id, row.request_id), outcome) is True:
                await repo.mark_published(row.request_id)
