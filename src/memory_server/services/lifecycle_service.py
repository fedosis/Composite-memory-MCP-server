"""Atomic lifecycle transition service for facts and beliefs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession
from storage.repositories import (
    BeliefRepository,
    FactRepository,
    LifecycleRepository,
    RelationRepository,
)
from storage.repositories.lifecycle_repo import LifecycleConflictError, normalize_version

from memory_server.evaluation.validator import (
    is_valid_transition,
    normalize_lifecycle_state,
)
from memory_server.models.receipt import LifecycleState
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.sqlite_provider import SQLiteProvider

logger = logging.getLogger(__name__)

_INVALIDATING_STATES = {
    LifecycleState.SUPERSEDED.value,
    LifecycleState.CONTRADICTED.value,
    LifecycleState.DISCARDED.value,
}

_VALID_MEMORY_TYPES = {"fact", "belief"}


@dataclass(frozen=True)
class LifecycleTransitionRequest:
    """A single lifecycle transition request."""

    memory_id: str
    memory_type: str
    to_state: str
    reason: str = ""
    triggered_by: str = "system"
    expected_version: str | int | None = None


@dataclass(frozen=True)
class LifecycleTransitionResult:
    """Result of a lifecycle transition."""

    memory: Any
    from_state: str
    to_state: str
    event: dict[str, Any]
    propagated: list[dict[str, Any]]


class LifecycleService:
    """Atomic lifecycle transition service.

    Facts and beliefs are transitioned inside one DB session, lifecycle events
    are recorded atomically, and invalidations optionally propagate confidence
    demotions to derived dependents.
    """

    def __init__(self, provider: SQLiteProvider):
        self._provider = provider

    async def transition(
        self,
        memory_id: str,
        memory_type: str,
        to_state: str,
        reason: str = "",
        triggered_by: str = "system",
        expected_version: str | int | None = None,
        graph: SimpleGraph | None = None,
    ) -> LifecycleTransitionResult:
        """Transition one memory item and record the lifecycle event."""
        request = LifecycleTransitionRequest(
            memory_id=memory_id,
            memory_type=memory_type,
            to_state=to_state,
            reason=reason,
            triggered_by=triggered_by,
            expected_version=expected_version,
        )
        results = await self.transition_many([request], graph=graph)
        return results[0]

    async def transition_in_session(
        self,
        session: AsyncSession,
        request: LifecycleTransitionRequest,
        graph: SimpleGraph | None = None,
    ) -> LifecycleTransitionResult:
        """Use the caller's UOW; caller must commit or roll back on any error."""
        results = await self.transition_many_in_session(session, [request], graph=graph)
        return results[0]

    async def transition_many_in_session(
        self,
        session: AsyncSession,
        requests: list[LifecycleTransitionRequest],
        graph: SimpleGraph | None = None,
    ) -> list[LifecycleTransitionResult]:
        """Stage a batch; caller owns commit/rollback of the entire UOW.

        On any error the caller must roll back, not commit earlier writes.
        Results are provisional until that commit succeeds.
        """
        lifecycle_repo = LifecycleRepository(session)
        fact_repo = FactRepository(session)
        belief_repo = BeliefRepository(session)
        results: list[LifecycleTransitionResult] = []
        for request in requests:
            results.append(
                await self._transition_in_session(
                    session=session,
                    lifecycle_repo=lifecycle_repo,
                    fact_repo=fact_repo,
                    belief_repo=belief_repo,
                    request=request,
                    graph=graph,
                )
            )
        return results

    async def transition_many(
        self,
        requests: list[LifecycleTransitionRequest],
        graph: SimpleGraph | None = None,
    ) -> list[LifecycleTransitionResult]:
        """Transition multiple memories in a single transaction."""
        async with await self._provider._get_session() as session:
            try:
                results = await self.transition_many_in_session(session, requests, graph=graph)
                await session.commit()
                return results
            except BaseException:
                await session.rollback()
                raise

    async def _transition_in_session(
        self,
        session: AsyncSession,
        lifecycle_repo: LifecycleRepository,
        fact_repo: FactRepository,
        belief_repo: BeliefRepository,
        request: LifecycleTransitionRequest,
        graph: SimpleGraph | None,
    ) -> LifecycleTransitionResult:
        memory_type = request.memory_type.strip().lower()
        if memory_type not in _VALID_MEMORY_TYPES:
            raise ValueError("memory_type must be one of: fact, belief")
        target_state = self._normalize_state(request.to_state)
        if target_state not in {state.value for state in LifecycleState}:
            raise ValueError(f"Unsupported lifecycle state: {request.to_state}")

        memory, repo = await self._load_memory(session, fact_repo, belief_repo, request.memory_id, memory_type)
        if memory is None or repo is None:
            raise KeyError(f"{memory_type} '{request.memory_id}' not found")

        current_state = self._normalize_state(memory.lifecycle_state)
        self._validate_expected_version(memory.version, request.expected_version, request.memory_id)
        self._validate_transition(current_state, target_state)

        expected_version = self._normalize_version(
            memory.version if request.expected_version is None else request.expected_version
        )
        updated = await lifecycle_repo.transition(
            repo,
            memory_id=request.memory_id,
            memory_type=memory_type,
            from_state=current_state,
            to_state=target_state,
            expected_version=expected_version,
            reason=request.reason or "lifecycle transition",
            triggered_by=request.triggered_by,
        )

        propagated: list[dict[str, Any]] = []
        if target_state in _INVALIDATING_STATES:
            propagated = await self._propagate_dependents(
                session=session,
                lifecycle_repo=lifecycle_repo,
                fact_repo=fact_repo,
                belief_repo=belief_repo,
                invalidated_id=request.memory_id,
                reason="parent_invalidated",
                triggered_by=request.triggered_by,
                graph=graph,
            )

        return LifecycleTransitionResult(
            memory=updated,
            from_state=current_state,
            to_state=target_state,
            event={
                "memory_id": request.memory_id,
                "memory_type": memory_type,
                "from_state": current_state,
                "to_state": target_state,
                "reason": request.reason or "lifecycle transition",
                "triggered_by": request.triggered_by,
            },
            propagated=propagated,
        )

    async def _propagate_dependents(
        self,
        session: AsyncSession,
        lifecycle_repo: LifecycleRepository,
        fact_repo: FactRepository,
        belief_repo: BeliefRepository,
        invalidated_id: str,
        reason: str,
        triggered_by: str,
        graph: SimpleGraph | None,
    ) -> list[dict[str, Any]]:
        dependent_ids: list[str] = []
        seen: set[str] = set()

        try:
            relation_repo = RelationRepository(session)
            for dependent_id in await relation_repo.get_dependents(
                target_id=invalidated_id,
                relation_type="derived_from",
            ):
                if dependent_id not in seen:
                    dependent_ids.append(dependent_id)
                    seen.add(dependent_id)
        except Exception:
            logger.exception("Lifecycle propagation SQL lookup failed")

        if not dependent_ids and graph is not None:
            try:
                edges = graph.search_by_relation("derived_from")
            except Exception:
                logger.exception(
                    "Lifecycle propagation skipped: graph relation search failed"
                )
                return []

            for edge in edges:
                if edge.target_id == invalidated_id and edge.source_id not in seen:
                    dependent_ids.append(edge.source_id)
                    seen.add(edge.source_id)
                elif edge.source_id == invalidated_id and edge.target_id not in seen:
                    # Defensive fallback for legacy reversed edges.
                    dependent_ids.append(edge.target_id)
                    seen.add(edge.target_id)

        propagated: list[dict[str, Any]] = []
        for dependent_id in dependent_ids:
            dependent, dependent_type = await self._load_any_memory(
                session=session,
                fact_repo=fact_repo,
                belief_repo=belief_repo,
                memory_id=dependent_id,
            )
            if dependent is None:
                continue

            new_confidence = max(0.1, round(float(dependent.confidence) * 0.8, 6))
            updated_dependent = await lifecycle_repo.transition(
                belief_repo if dependent_type == "belief" else fact_repo,
                memory_id=dependent_id,
                memory_type=dependent_type,
                from_state=self._normalize_state(dependent.lifecycle_state),
                to_state=self._normalize_state(dependent.lifecycle_state),
                expected_version=self._normalize_version(dependent.version),
                reason=reason,
                triggered_by=triggered_by,
                confidence=new_confidence,
            )
            propagated.append(
                {
                    "memory_id": dependent_id,
                    "memory_type": dependent_type,
                    "confidence": new_confidence,
                    "version": updated_dependent.version if updated_dependent is not None else None,
                    "reason": reason,
                }
            )

        return propagated

    async def _load_memory(
        self,
        session: AsyncSession,
        fact_repo: FactRepository,
        belief_repo: BeliefRepository,
        memory_id: str,
        memory_type: str,
    ) -> tuple[Any | None, Any | None]:
        if memory_type == "fact":
            memory = await fact_repo.get(memory_id)
            return memory, fact_repo
        if memory_type == "belief":
            memory = await belief_repo.get_by_id(memory_id)
            return memory, belief_repo
        return None, None

    async def _load_any_memory(
        self,
        session: AsyncSession,
        fact_repo: FactRepository,
        belief_repo: BeliefRepository,
        memory_id: str,
    ) -> tuple[Any | None, str | None]:
        fact = await fact_repo.get(memory_id)
        if fact is not None:
            return fact, "fact"
        belief = await belief_repo.get_by_id(memory_id)
        if belief is not None:
            return belief, "belief"
        return None, None

    def _validate_expected_version(
        self,
        current_version: Any,
        expected_version: str | int | None,
        memory_id: str,
    ) -> None:
        if expected_version is None:
            return
        if self._normalize_version(current_version) != self._normalize_version(expected_version):
            raise LifecycleConflictError(
                f"expected_version mismatch for {memory_id}: "
                f"expected {expected_version}, got {current_version}"
            )

    def _validate_transition(self, current_state: str, target_state: str) -> None:
        normalized_current = normalize_lifecycle_state(self._normalize_state(current_state))
        normalized_target = normalize_lifecycle_state(self._normalize_state(target_state))
        if not is_valid_transition(normalized_current, normalized_target):
            raise ValueError(
                f"Invalid lifecycle transition: {normalized_current} -> {normalized_target}"
            )

    @staticmethod
    def _normalize_state(state: str | LifecycleState) -> str:
        if isinstance(state, LifecycleState):
            return state.value
        return LifecycleState(state).value

    @staticmethod
    def _normalize_version(version: Any) -> int:
        return normalize_version(version)
