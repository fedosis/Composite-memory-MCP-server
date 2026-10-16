"""Transport-free belief application services."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.application.contracts import (
    BeliefPolicy,
    GetBeliefCommand,
    ResolveConflictCommand,
    SetBeliefCommand,
)
from memory_server.models import Belief
from memory_server.models.evidence import Evidence
from memory_server.models.receipt import MemoryReceipt
from memory_server.services.ingestion_service import reinforce_belief_item
from memory_server.services.lifecycle_service import (
    LifecycleService,
    LifecycleTransitionRequest,
)

logger = logging.getLogger(__name__)


class BeliefService:
    """Belief use cases over a borrowed initialized provider."""

    def __init__(
        self,
        provider: Any,
        *,
        policy: BeliefPolicy | None = None,
        get_graph: Callable[[], Awaitable[Any]] | None = None,
    ) -> None:
        self._provider = provider
        self._policy = policy if policy is not None else BeliefPolicy.capture()
        self._get_graph = get_graph

    @property
    def policy(self) -> BeliefPolicy:
        return self._policy

    async def _graph(self) -> Any | None:
        return await self._get_graph() if self._get_graph is not None else None

    async def set_belief(self, command: SetBeliefCommand) -> dict:
        provider = self._provider
        parsed_sources = command.sources or []
        parsed_tags = command.tags or []

        async with await provider._get_session() as session:
            reinforced = await reinforce_belief_item(
                session,
                proposition=command.proposition,
                new_confidence=command.confidence,
                source=command.source,
                parsed_sources=parsed_sources,
                parsed_tags=parsed_tags,
            )
            if reinforced is not None:
                belief, receipt = reinforced
                await session.commit()
                return {
                    "belief": belief.model_dump(mode="json"),
                    "receipt": receipt.model_dump(mode="json"),
                    "superseded": None,
                    "reinforced": True,
                }

        belief = Belief(
            proposition=command.proposition,
            confidence=command.confidence,
            source=command.source,
            tags=parsed_tags,
            creator=command.source,
        )
        evidence_list = [
            Evidence(
                belief_id=belief.id,
                source_type=source.get("source_type", "observation"),
                source_id=source.get("source_id", ""),
                weight=source.get("weight", 0.5),
                contributor=command.source,
            )
            for source in parsed_sources
        ]
        superseded = None
        old_belief = None
        graph = await self._graph()
        lifecycle_service = LifecycleService(provider)
        if command.replace_belief_id:
            old_belief = await provider.get_belief(command.replace_belief_id)
            if old_belief:
                superseded = old_belief.model_dump(mode="json")
                belief.version = old_belief.version + 1

        receipt = MemoryReceipt(
            id=belief.id,
            memory_type="belief",
            source=command.source,
            created_by=command.source,
            timestamp=datetime.now(timezone.utc),
            confidence=command.confidence,
        )

        async def transaction_callback(session: Any) -> None:
            if not command.replace_belief_id or old_belief is None:
                return
            await lifecycle_service.transition_in_session(
                session,
                LifecycleTransitionRequest(
                    memory_id=command.replace_belief_id,
                    memory_type="belief",
                    to_state="superseded",
                    reason=f"Replaced by {belief.id}",
                    triggered_by=command.source,
                    expected_version=old_belief.version,
                ),
                graph=graph,
            )

        await provider.create_in_transaction(
            belief=belief,
            evidence_list=evidence_list,
            receipt=receipt,
            outbox_entries=[
                {
                    "record_type": "belief",
                    "record_id": belief.id,
                    "operation": "index_belief",
                    "payload": {
                        "proposition": command.proposition,
                        "tags": parsed_tags,
                        "confidence": command.confidence,
                        "source": command.source,
                    },
                }
            ],
            transaction_callback=transaction_callback if old_belief is not None else None,
        )
        return {
            "belief": belief.model_dump(mode="json"),
            "receipt": receipt.model_dump(mode="json"),
            "superseded": superseded,
        }

    async def get_belief(self, command: GetBeliefCommand) -> dict:
        limit = self._policy.belief_search_limit if command.limit is None else command.limit
        results = await self._provider.search_beliefs(
            proposition=command.proposition or None,
            lifecycle_state=command.lifecycle_state or None,
            min_confidence=command.min_confidence if command.min_confidence > 0 else None,
            tags=command.tags,
            source=command.source or None,
            creator=command.creator or None,
            source_id=command.source_id or None,
            limit=min(limit, 100),
        )
        return {
            "total": len(results),
            "beliefs": [belief.model_dump(mode="json") for belief in results],
            "query": {
                "proposition": command.proposition,
                "lifecycle_state": command.lifecycle_state,
                "min_confidence": command.min_confidence,
                "tags": command.tags,
                "source": command.source,
                "creator": command.creator,
                "source_id": command.source_id,
                "limit": limit,
            },
        }

    async def resolve_conflict(self, command: ResolveConflictCommand) -> dict:
        provider = self._provider
        graph = await self._graph()
        lifecycle_service = LifecycleService(provider)
        belief_a = await provider.get_belief(command.belief_a_id)
        belief_b = await provider.get_belief(command.belief_b_id)
        if belief_a is None or belief_b is None:
            raise ValueError("Both belief_a and belief_b must exist in the store")

        if command.auto_resolve:
            confidence_diff = abs(belief_a.confidence - belief_b.confidence)
            if confidence_diff > 0.5:
                if belief_a.confidence < belief_b.confidence:
                    lower_id, higher_conf = command.belief_a_id, belief_b.confidence
                    expected_version = belief_a.version
                else:
                    lower_id, higher_conf = command.belief_b_id, belief_a.confidence
                    expected_version = belief_b.version
                requests = [LifecycleTransitionRequest(
                    memory_id=lower_id,
                    memory_type="belief",
                    to_state="superseded",
                    reason=(
                        f"Auto-resolved — confidence gap ({higher_conf:.2f} vs "
                        f"{(belief_a.confidence if lower_id == command.belief_b_id else belief_b.confidence):.2f})"
                    ),
                    triggered_by="system",
                    expected_version=expected_version,
                )]
            else:
                requests = [
                    LifecycleTransitionRequest(
                        memory_id=command.belief_a_id,
                        memory_type="belief",
                        to_state="contradicted",
                        reason="Auto-resolved — needs manual review (confidences too close or both low)",
                        triggered_by="system",
                        expected_version=belief_a.version,
                    ),
                    LifecycleTransitionRequest(
                        memory_id=command.belief_b_id,
                        memory_type="belief",
                        to_state="contradicted",
                        reason="Auto-resolved — needs manual review (confidences too close or both low)",
                        triggered_by="system",
                        expected_version=belief_b.version,
                    ),
                ]
            results = await lifecycle_service.transition_many(requests, graph=graph)
            events = [
                {"belief_id": result.event["memory_id"], "from_state": result.from_state,
                 "to_state": result.to_state, "reason": result.event["reason"]}
                for result in results
            ]
            receipt = MemoryReceipt(
                id=str(uuid.uuid4()), memory_type="belief", source="conflict_resolution",
                created_by="system", timestamp=datetime.now(timezone.utc),
            )
            return {
                "belief_a": (await provider.get_belief(command.belief_a_id)).model_dump(mode="json"),
                "belief_b": (await provider.get_belief(command.belief_b_id)).model_dump(mode="json"),
                "resolution": command.resolution, "auto_resolved": True, "created": None,
                "events": events, "receipt": receipt.model_dump(mode="json"),
            }

        events = []
        merge_events = None
        merged = None
        requests: list[LifecycleTransitionRequest] = []
        if command.resolution == "keep_a":
            requests = [LifecycleTransitionRequest(
                memory_id=command.belief_b_id, memory_type="belief", to_state="discarded",
                reason=f"Discarded in favor of {command.belief_a_id} via conflict resolution",
                triggered_by="system", expected_version=belief_b.version,
            )]
        elif command.resolution == "keep_b":
            requests = [LifecycleTransitionRequest(
                memory_id=command.belief_a_id, memory_type="belief", to_state="discarded",
                reason=f"Discarded in favor of {command.belief_b_id} via conflict resolution",
                triggered_by="system", expected_version=belief_a.version,
            )]
        elif command.resolution == "merge":
            if not command.new_proposition:
                raise ValueError("new_proposition is required when resolution='merge'")
            merged = Belief(
                proposition=command.new_proposition,
                confidence=min(1.0, (belief_a.confidence + belief_b.confidence) / 2),
                source="conflict_resolution", creator="system",
                tags=list(set(belief_a.tags + belief_b.tags)),
                source_ids=list(set(belief_a.source_ids + belief_b.source_ids)),
            )
            copied_evidence = []
            try:
                from storage.repositories import EvidenceRepository
                async with AsyncSession(provider.engine) as ev_session:
                    ev_repo = EvidenceRepository(ev_session)
                    for original_id in (command.belief_a_id, command.belief_b_id):
                        for ev in await ev_repo.get_by_belief_id(original_id):
                            copied_evidence.append(Evidence(
                                belief_id=merged.id, source_type=ev.source_type,
                                source_id=ev.source_id, weight=ev.weight,
                                contributor=ev.contributor,
                            ))
            except Exception:
                logger.warning("Failed to copy evidence for merged belief %s", merged.id, exc_info=True)
            merged_receipt = MemoryReceipt(
                id=merged.id, memory_type="belief", source="conflict_resolution", created_by="system",
                timestamp=datetime.now(timezone.utc), confidence=merged.confidence,
            )
            async def merge_callback(session: Any) -> None:
                await lifecycle_service.transition_many_in_session(
                    session,
                    [
                        LifecycleTransitionRequest(
                            memory_id=command.belief_a_id, memory_type="belief", to_state="superseded",
                            reason=f"Merged into {merged.id} via conflict resolution", triggered_by="system",
                            expected_version=belief_a.version,
                        ),
                        LifecycleTransitionRequest(
                            memory_id=command.belief_b_id, memory_type="belief", to_state="superseded",
                            reason=f"Merged into {merged.id} via conflict resolution", triggered_by="system",
                            expected_version=belief_b.version,
                        ),
                    ], graph=graph,
                )
            await provider.create_in_transaction(
                belief=merged, evidence_list=copied_evidence, receipt=merged_receipt,
                outbox_entries=[{
                    "record_type": "belief", "record_id": merged.id, "operation": "index_belief",
                    "payload": {"proposition": merged.proposition, "tags": merged.tags,
                                "confidence": merged.confidence, "source": "conflict_resolution"},
                }], transaction_callback=merge_callback,
            )
            requests = [
                LifecycleTransitionRequest(
                    memory_id=command.belief_a_id, memory_type="belief", to_state="superseded",
                    reason=f"Merged into {merged.id} via conflict resolution", triggered_by="system",
                    expected_version=belief_a.version,
                ),
                LifecycleTransitionRequest(
                    memory_id=command.belief_b_id, memory_type="belief", to_state="superseded",
                    reason=f"Merged into {merged.id} via conflict resolution", triggered_by="system",
                    expected_version=belief_b.version,
                ),
            ]
            merge_events = [
                {
                    "belief_id": request.memory_id,
                    "from_state": "active",
                    "to_state": "superseded",
                    "reason": request.reason,
                }
                for request in requests
            ]
        elif command.resolution == "discard_both":
            requests = [
                LifecycleTransitionRequest(
                    memory_id=command.belief_a_id, memory_type="belief", to_state="discarded",
                    reason="Discarded via conflict resolution", triggered_by="system",
                    expected_version=belief_a.version,
                ),
                LifecycleTransitionRequest(
                    memory_id=command.belief_b_id, memory_type="belief", to_state="discarded",
                    reason="Discarded via conflict resolution", triggered_by="system",
                    expected_version=belief_b.version,
                ),
            ]
        else:
            raise ValueError(
                f"Unknown resolution: {command.resolution}. Must be one of: keep_a, keep_b, merge, discard_both"
            )

        if command.resolution == "merge" and merged is not None:
            events = merge_events or []
        else:
            results = await lifecycle_service.transition_many(requests, graph=graph)
            events = [
                {"belief_id": result.event["memory_id"], "from_state": result.from_state,
                 "to_state": result.to_state, "reason": result.event["reason"]}
                for result in results
            ]
        receipt = MemoryReceipt(
            id=str(uuid.uuid4()), memory_type="belief", source="conflict_resolution",
            created_by="system", timestamp=datetime.now(timezone.utc),
        )
        return {
            "belief_a": (await provider.get_belief(command.belief_a_id)).model_dump(mode="json"),
            "belief_b": (await provider.get_belief(command.belief_b_id)).model_dump(mode="json"),
            "resolution": command.resolution,
            "created": merged.model_dump(mode="json") if command.resolution == "merge" and merged else None,
            "events": events, "receipt": receipt.model_dump(mode="json"),
        }
