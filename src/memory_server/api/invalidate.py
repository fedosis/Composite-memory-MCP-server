"""Retire a fact or belief with atomic lifecycle-bound successor provenance."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from storage.dedup import ACTIVE_LIFECYCLE_STATES
from storage.models.relation import ClaimRelationORM
from storage.repositories import BeliefRepository, FactRepository
from storage.repositories.relation_repo import RelationRepository
from storage.repositories.relation_revision_repo import RelationEventContext

from memory_server.services.lifecycle_service import (
    LifecycleService,
    LifecycleTransitionRequest,
)

SUPERSEDED_BY = "superseded_by"
_VALID_TYPES = {"fact", "belief"}
_VALID_TARGETS = {"discarded", "superseded", "stale"}


class InvalidateError(ValueError):
    """Caller validation failure; nothing was written."""


async def invalidate(
    provider: Any,
    memory_id: str,
    memory_type: str = "fact",
    reason: str = "",
    to_state: str = "discarded",
    superseded_by: str = "",
    expected_version: str | None = None,
    triggered_by: str = "agent",
    graph: Any = None,
) -> dict[str, Any]:
    """Retire one memory item; optionally record its active successor."""
    memory_type = memory_type.strip().lower()
    if memory_type not in _VALID_TYPES:
        raise InvalidateError("memory_type must be one of: fact, belief")
    if to_state not in _VALID_TARGETS:
        raise InvalidateError("to_state must be one of: discarded, superseded, stale")
    reason = reason.strip()
    if not reason:
        raise InvalidateError("reason must be non-blank")
    successor_id = superseded_by
    if to_state == "superseded" and not successor_id:
        raise InvalidateError("superseded_by is required when to_state='superseded'")
    if successor_id == memory_id:
        raise InvalidateError("a memory cannot supersede itself")

    async with await provider._get_session() as session:
        try:
            context = None
            if successor_id:
                successor = (
                    await FactRepository(session).get(successor_id)
                    if memory_type == "fact"
                    else await BeliefRepository(session).get_by_id(successor_id)
                )
                if successor is None:
                    raise InvalidateError(f"successor {memory_type} '{successor_id}' not found")
                if successor.lifecycle_state not in ACTIVE_LIFECYCLE_STATES:
                    raise InvalidateError(
                        f"successor {memory_type} '{successor_id}' is not active "
                        f"(state: {successor.lifecycle_state})"
                    )
                context = RelationEventContext(str(uuid4()), datetime.now(UTC))
            result = await LifecycleService(provider).transition_in_session(
                session,
                LifecycleTransitionRequest(
                    memory_id=memory_id,
                    memory_type=memory_type,
                    to_state=to_state,
                    reason=reason,
                    triggered_by=triggered_by,
                    expected_version=expected_version,
                ),
                graph=graph,
                event_context=context,
            )
            if successor_id:
                existing = await session.get(
                    ClaimRelationORM, (memory_id, successor_id, SUPERSEDED_BY)
                )
                if existing is None:
                    await RelationRepository(session).create_versioned(
                        memory_id, successor_id, SUPERSEDED_BY, event=context
                    )
            await session.commit()
        except BaseException:
            await session.rollback()
            raise

    return {
        "memory": result.memory.model_dump(mode="json"),
        "event": result.event,
        "propagated": result.propagated,
        "superseded_by": successor_id or None,
    }
