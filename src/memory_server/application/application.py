"""Composition root for the memory application layer (C06).

Transport-free and runtime-agnostic by construction: this module never imports
``memory_server.server``, ``memory_server.transports*``,
``memory_server.plugins*``, ``memory_server.providers*`` or
``memory_server.runtime`` — it never constructs a provider, never reads
``os.environ`` directly, never touches a lock, never starts a runtime and never
calls ``OutboxWorker``.

Construction takes the shared session factory explicitly:
``provider._session_factory`` is exactly what the two ``api/`` facades used
before the move (``api/remember.py:98``, ``api/learn.py:57``) and exactly the
handle the C10/C12 adapters pass from ``RuntimeDependencies.sqlite``
(``runtime/dependencies.py:25``). Nothing here registers anything with a
transport.
"""

from __future__ import annotations

from typing import Any

from memory_server.application.contracts import MemoryPolicy
from memory_server.application.memory_service import MemoryService


class MemoryApplication:
    """Composition root exposing the shared application use cases."""

    def __init__(
        self,
        session_factory: Any,
        *,
        policy: MemoryPolicy | None = None,
    ) -> None:
        self._session_factory = session_factory
        # Captured once here; MemoryService never re-reads it.
        self._policy = policy if policy is not None else MemoryPolicy.capture()
        self.memory = MemoryService(session_factory, policy=self._policy)

    @classmethod
    def from_provider(
        cls,
        provider: Any,
        *,
        policy: MemoryPolicy | None = None,
    ) -> MemoryApplication:
        """Build from any object exposing ``_session_factory`` (duck-typed)."""
        return cls(provider._session_factory, policy=policy)

    @property
    def policy(self) -> MemoryPolicy:
        return self._policy

    def describe(self) -> dict[str, Any]:
        """Secrets-free by construction: no paths, no provider handles."""
        return {"policy": {"max_active_beliefs": self._policy.max_active_beliefs}}
