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

from typing import TYPE_CHECKING, Any, Awaitable, Callable, Mapping

from memory_server.application.audit_service import AuditService
from memory_server.application.belief_service import BeliefService
from memory_server.application.contracts import (
    BeliefPolicy,
    GraphPolicy,
    MemoryPolicy,
    ReflectionPolicy,
    RetrievalPolicy,
)
from memory_server.application.errors import UnknownDependencyError
from memory_server.application.graph_service import GraphService
from memory_server.application.memory_service import MemoryService
from memory_server.application.retrieval_service import RetrievalService

if TYPE_CHECKING:
    from memory_server.application.memory_read_service import ReadResources


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
        belief_provider: Any | None = None,
        belief_policy: BeliefPolicy | None = None,
        get_graph: Callable[[], Awaitable[Any]] | None = None,
        lifecycle_get_graph: Callable[[], Awaitable[Any | None]] | None = None,
        reflection_provider: Any | None = None,
        reflection_policy: ReflectionPolicy | None = None,
        relation_provider: Any | None = None,
        lifecycle_provider: Any | None = None,
        audit_validator: Any | None = None,
        audit_confidence_engine: Any | None = None,
        audit_sqlite: Any | None = None,
        audit_vector_provider: Any | None = None,
        audit_state: Mapping[str, Any] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._read_provider = retrieval_provider
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
        self._beliefs = (
            BeliefService(belief_provider, policy=belief_policy, get_graph=get_graph)
            if belief_provider is not None
            else None
        )
        self._reflection = None
        if reflection_provider is not None:
            from memory_server.application.reflection_service import ReflectionService

            self._reflection = ReflectionService(
                reflection_provider,
                policy=reflection_policy,
            )
        self._relations = None
        if get_graph is not None:
            from memory_server.application.relation_service import RelationService

            self._relations = RelationService(
                get_graph=get_graph,
                provider=relation_provider,
            )
        self._lifecycle = None
        if lifecycle_provider is not None:
            from memory_server.application.lifecycle_service import LifecycleService

            self._lifecycle = LifecycleService(
                lifecycle_provider,
                get_graph=lifecycle_get_graph if lifecycle_get_graph is not None else get_graph,
            )
        self._audit = None
        if audit_validator is not None and audit_confidence_engine is not None and audit_state is not None:
            self._audit = AuditService(
                validator=audit_validator,
                confidence_engine=audit_confidence_engine,
                get_graph=get_graph,
                sqlite=audit_sqlite,
                vector_provider=audit_vector_provider,
                persisted_state=audit_state,
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
        belief_policy: BeliefPolicy | None = None,
        get_graph: Callable[[], Awaitable[Any]] | None = None,
        reflection_policy: ReflectionPolicy | None = None,
        relation_provider: Any | None = None,
        lifecycle_provider: Any | None = None,
        audit_validator: Any | None = None,
        audit_confidence_engine: Any | None = None,
        audit_state: Mapping[str, Any] | None = None,
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
            belief_provider=provider,
            belief_policy=belief_policy,
            get_graph=get_graph,
            reflection_provider=provider,
            reflection_policy=reflection_policy,
            relation_provider=provider,
            lifecycle_provider=provider,
            audit_validator=audit_validator,
            audit_confidence_engine=audit_confidence_engine,
            audit_sqlite=provider,
            audit_state=audit_state,
        )

    def read_resources(self) -> ReadResources:
        """Borrow canonical read dependencies without exposing runtime internals."""
        from memory_server.application.memory_read_service import ReadResources

        if self._read_provider is None or self._session_factory is None:
            raise UnknownDependencyError("read resources")
        return ReadResources(self._read_provider, self._session_factory)

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

    @property
    def beliefs(self) -> BeliefService:
        if self._beliefs is None:
            raise UnknownDependencyError("belief service")
        return self._beliefs

    @property
    def reflection(self) -> Any:
        if self._reflection is None:
            raise UnknownDependencyError("reflection service")
        return self._reflection

    @property
    def relations(self) -> Any:
        if self._relations is None:
            raise UnknownDependencyError("relation service")
        return self._relations

    @property
    def lifecycle(self) -> Any:
        if self._lifecycle is None:
            raise UnknownDependencyError("lifecycle service")
        return self._lifecycle

    @property
    def audit(self) -> AuditService:
        if self._audit is None:
            raise UnknownDependencyError("audit service")
        return self._audit

    def describe(self) -> dict[str, Any]:
        """Secrets-free by construction: no paths, no provider handles."""
        return {"policy": {"max_active_beliefs": self._policy.max_active_beliefs}}
