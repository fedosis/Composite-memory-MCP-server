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

from typing import Any, Awaitable, Callable

from memory_server.application.contracts import (
    GraphPolicy,
    MemoryPolicy,
    RetrievalPolicy,
)
from memory_server.application.errors import UnknownDependencyError
from memory_server.application.graph_service import GraphService
from memory_server.application.memory_service import MemoryService
from memory_server.application.retrieval_service import RetrievalService


class MemoryApplication:
    """Composition root exposing the shared application use cases."""

    def __init__(
        self,
        session_factory: Any,
        *,
        policy: MemoryPolicy | None = None,
        retrieval_provider: Any | None = None,
        retrieval_policy: RetrievalPolicy | None = None,
        graph_policy: GraphPolicy | None = None,
        get_embedding_router: Callable[[], Awaitable[Any]] | None = None,
        get_hybrid_router: Callable[[], Awaitable[Any]] | None = None,
        get_graph_router: Callable[[], Awaitable[Any]] | None = None,
    ) -> None:
        self._session_factory = session_factory
        # Captured once here; MemoryService never re-reads it.
        self._policy = policy if policy is not None else MemoryPolicy.capture()
        self.memory = MemoryService(session_factory, policy=self._policy)
        self._retrieval = None
        if retrieval_provider is not None or get_embedding_router is not None or get_hybrid_router is not None:
            self._retrieval = RetrievalService(
                retrieval_provider,
                policy=retrieval_policy,
                get_embedding_router=get_embedding_router,
                get_hybrid_router=get_hybrid_router,
            )
        self._graph = (
            GraphService(get_graph_router, policy=graph_policy)
            if get_graph_router is not None
            else None
        )

    @classmethod
    def from_provider(
        cls,
        provider: Any,
        *,
        policy: MemoryPolicy | None = None,
        retrieval_policy: RetrievalPolicy | None = None,
        graph_policy: GraphPolicy | None = None,
        get_embedding_router: Callable[[], Awaitable[Any]] | None = None,
        get_hybrid_router: Callable[[], Awaitable[Any]] | None = None,
        get_graph_router: Callable[[], Awaitable[Any]] | None = None,
    ) -> MemoryApplication:
        """Build from any object exposing ``_session_factory`` (duck-typed)."""
        return cls(
            provider._session_factory,
            policy=policy,
            retrieval_provider=provider,
            retrieval_policy=retrieval_policy,
            graph_policy=graph_policy,
            get_embedding_router=get_embedding_router,
            get_hybrid_router=get_hybrid_router,
            get_graph_router=get_graph_router,
        )

    @property
    def policy(self) -> MemoryPolicy:
        return self._policy

    @property
    def retrieval(self) -> RetrievalService:
        if self._retrieval is None:
            raise UnknownDependencyError("retrieval service")
        return self._retrieval

    @property
    def graph(self) -> GraphService:
        if self._graph is None:
            raise UnknownDependencyError("graph service")
        return self._graph

    def describe(self) -> dict[str, Any]:
        """Secrets-free by construction: no paths, no provider handles."""
        return {"policy": {"max_active_beliefs": self._policy.max_active_beliefs}}
