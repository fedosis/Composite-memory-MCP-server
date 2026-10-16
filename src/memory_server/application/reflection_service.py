"""Application reflection use cases over the legacy reflection engine."""

from __future__ import annotations

from typing import Any

from memory_server.api.reflect import ReflectEngine
from memory_server.application.contracts import ReflectionCommand, ReflectionPolicy


class ReflectionService:
    """Transport-free reflection facade with captured reflection settings."""

    _MODES = {
        "overview": "overview",
        "contradictions": "contradictions",
        "decay": "decay_analysis",
        "topics": "topics",
        "evidence_audit": "evidence_audit",
        "confidence": "confidence_histogram",
    }
    _MODE_TEXT = "confidence, contradictions, decay, evidence_audit, overview, topics"

    def __init__(self, provider: Any, *, policy: ReflectionPolicy | None = None) -> None:
        self._policy = policy if policy is not None else ReflectionPolicy.capture()
        self._engine = ReflectEngine(provider, policy=self._policy)

    @property
    def policy(self) -> ReflectionPolicy:
        return self._policy

    def _arguments(self, command: ReflectionCommand) -> dict[str, Any]:
        return {
            "topic": command.topic or None,
            "min_confidence": (
                self._policy.reflect_min_confidence
                if command.min_confidence is None
                else command.min_confidence
            ),
            "limit": self._policy.reflect_limit if command.limit is None else command.limit,
        }

    async def reflect(self, command: ReflectionCommand) -> dict[str, Any]:
        method_name = self._MODES.get(command.mode)
        if method_name is None:
            return {
                "error": (
                    f"Invalid mode '{command.mode}'. Must be one of: {self._MODE_TEXT}"
                )
            }
        return await getattr(self._engine, method_name)(**self._arguments(command))

    async def relations(self, command: ReflectionCommand) -> dict[str, Any]:
        arguments = self._arguments(command)
        arguments.update(
            context=command.context,
            strict_same_context=command.strict_same_context,
        )
        return await self._engine.relations(**arguments)
