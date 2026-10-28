"""Transport-neutral application composition over an already owned runtime."""
from __future__ import annotations

from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Any

from memory_server.application.application import MemoryApplication
from memory_server.application.contracts import (
    BeliefPolicy,
    GraphPolicy,
    MemoryPolicy,
    ReflectionPolicy,
    RetrievalPolicy,
)
from memory_server.evaluation.auditor import collect_persisted_audit_state
from memory_server.evaluation.confidence import ConfidenceEngine
from memory_server.evaluation.decay import DecayEngine
from memory_server.evaluation.validator import Validator
from memory_server.router.embedding_router import EmbeddingRouter
from memory_server.router.graph_router import GraphRouter
from memory_server.router.hybrid_router import HybridRouter
from memory_server.runtime.errors import RuntimeNotReadyError
from memory_server.runtime.runtime import CMMSRuntime


@dataclass(frozen=True)
class BundlePolicies:
    """Capture application-only settings once, independently of runtime recipes."""

    memory: MemoryPolicy
    retrieval: RetrievalPolicy
    graph: GraphPolicy
    beliefs: BeliefPolicy
    reflection: ReflectionPolicy

    @classmethod
    def capture(cls, settings: Any) -> BundlePolicies:
        reflection = ReflectionPolicy.capture(settings)
        return cls(
            MemoryPolicy.capture(settings), RetrievalPolicy.capture(settings),
            GraphPolicy.capture(settings), BeliefPolicy.capture(settings),
            replace(reflection, ttl_days=MappingProxyType(dict(reflection.ttl_days))),
        )


@dataclass(frozen=True)
class ProfileBundle:
    application: MemoryApplication
    worker_task: Any

    @classmethod
    async def compose(cls, runtime: CMMSRuntime, policies: BundlePolicies) -> ProfileBundle:
        """No publication until snapshot, routers, evaluators and spawn succeed.

        This function is called once by the daemon's retained composition task.
        Snapshot failure precedes all worker spawning. RuntimeFactory remains
        the sole constructor/owner of providers and the worker.
        """
        if runtime.state != "READY":
            raise RuntimeNotReadyError()
        deps, config = runtime.dependencies, runtime.config
        if deps.sqlite is None:
            raise ValueError("E_CAPABILITY_UNAVAILABLE")
        if deps.graph is not None:
            deps.graph.load_snapshot()
        graph_router = (
            getattr(deps.outbox_worker, "_graph_router", None)
            or (GraphRouter(deps.graph, max_path_depth=config.routing["graph_max_path_depth"])
                if deps.graph is not None else None)
        )
        embedding_router = (
            EmbeddingRouter(deps.vector, deps.embedder, collection=config.vector_collection)
            if deps.vector is not None and deps.embedder is not None else None
        )
        hybrid_router = (
            HybridRouter(deps.vector, deps.embedder, graph=deps.graph,
                         collection=config.vector_collection, fts_provider=deps.sqlite)
            if embedding_router is not None and deps.graph is not None else None
        )
        confidence = ConfidenceEngine(settings={
            "confidence_" + key: value for key, value in config.evaluation["confidence"].items()
        })
        validator = Validator(confidence_engine=confidence)
        decay = DecayEngine(validator=validator, settings={
            "ttl_days": config.evaluation["ttl_days"],
            **{"decay_" + key: value for key, value in config.evaluation["decay"].items()},
        })
        audit_state = await collect_persisted_audit_state(sqlite=deps.sqlite, vector_provider=deps.vector)

        def getter(value):
            async def get():
                if runtime.state != "READY":
                    raise RuntimeNotReadyError()
                if value is None:
                    raise ValueError("E_CAPABILITY_UNAVAILABLE")
                return value
            return get

        async def optional_graph():
            if runtime.state != "READY":
                raise RuntimeNotReadyError()
            return deps.graph

        app = MemoryApplication(
            deps.sqlite._session_factory,
            policy=policies.memory, retrieval_provider=deps.sqlite, retrieval_policy=policies.retrieval,
            graph_policy=policies.graph, get_embedding_router=getter(embedding_router),
            get_hybrid_router=getter(hybrid_router), get_graph_router=getter(graph_router),
            belief_provider=deps.sqlite, belief_policy=policies.beliefs, get_graph=getter(deps.graph),
            lifecycle_get_graph=optional_graph, reflection_provider=deps.sqlite, reflection_policy=policies.reflection,
            relation_provider=deps.sqlite, lifecycle_provider=deps.sqlite, audit_validator=validator,
            audit_confidence_engine=confidence, audit_sqlite=deps.sqlite, audit_vector_provider=deps.vector,
            audit_state=audit_state,
        )
        for name, resource in (
            ("graph_router", graph_router), ("embedding_router", embedding_router),
            ("hybrid_router", hybrid_router), ("validator", validator),
            ("confidence_engine", confidence), ("decay_engine", decay),
        ):
            runtime.dependencies = runtime.dependencies.with_resource(name, resource)
        worker_task = None
        if deps.outbox_worker is not None:
            worker_task = runtime.spawn_owned_task(deps.outbox_worker.run, kind="daemon-worker-parent")
        return cls(app, worker_task)
