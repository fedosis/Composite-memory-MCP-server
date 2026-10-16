"""Application lifecycle wrapper delegating to the domain service."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Sequence

from memory_server.application.contracts import LifecycleCommand
from memory_server.services.lifecycle_service import (
    LifecycleService as DomainLifecycleService,
)
from memory_server.services.lifecycle_service import LifecycleTransitionRequest


class LifecycleService:
    """Transport-free wrapper around the domain lifecycle service."""

    def __init__(
        self,
        provider: Any,
        *,
        get_graph: Callable[[], Awaitable[Any]] | None = None,
    ) -> None:
        self._provider = provider
        self._get_graph = get_graph

    async def _graph(self) -> Any | None:
        return await self._get_graph() if self._get_graph is not None else None

    @staticmethod
    def _expected_version(value: str | int | None) -> str | int | None:
        return None if value == "" else value

    @staticmethod
    def _payload(result: Any) -> dict[str, Any]:
        return {
            "memory": result.memory.model_dump(mode="json"),
            "event": result.event,
            "propagated": result.propagated,
        }

    async def transition(self, command: LifecycleCommand) -> dict[str, Any]:
        result = await DomainLifecycleService(self._provider).transition(
            memory_id=command.memory_id,
            memory_type=command.memory_type,
            to_state=command.to_state,
            reason=command.reason,
            triggered_by=command.triggered_by,
            expected_version=self._expected_version(command.expected_version),
            graph=await self._graph(),
        )
        return self._payload(result)

    async def transition_many(
        self, commands: Sequence[LifecycleCommand]
    ) -> list[dict[str, Any]]:
        requests = [
            LifecycleTransitionRequest(
                memory_id=command.memory_id,
                memory_type=command.memory_type,
                to_state=command.to_state,
                reason=command.reason,
                triggered_by=command.triggered_by,
                expected_version=self._expected_version(command.expected_version),
            )
            for command in commands
        ]
        results = await DomainLifecycleService(self._provider).transition_many(
            requests,
            graph=await self._graph(),
        )
        return [self._payload(result) for result in results]
