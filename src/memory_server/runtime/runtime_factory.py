"""Ordered, lazy resource recipes for the neutral CMMS runtime.

Every recipe imports its optional backend INSIDE the function body (the proven
pattern of ``lancedb_provider.py:365-373``, ``embedding_provider.py:132-160`` and
``qdrant_provider.py:14-16``), so creating a runtime never imports
``lancedb``/``pyarrow``/``numpy``/``qdrant_client``/``sentence_transformers``
(``tests/test_ping.py:20-53``) and a missing optional root degrades exactly as it
does today.

The factory records ``self._built`` after each SUCCESSFUL step. That ledger is
the partial-rollback authority: the runtime closes only the steps that were
actually built, in reverse order (``runtime.py:_cleanup(rollback=True)``), and a
step whose own close fails is retained and reported
(``E_RUNTIME_CLEANUP_FAILED``) instead of being reported as a clean rollback.

Ownership: the outbox worker BORROWS ``SQLiteProvider.engine``
(``outbox_worker.py:67-98``) and ``OutboxWorker.close()`` disposes it
(``outbox_worker.py:140-143``), so the runtime never calls it and the engine is
disposed exactly once, through the SQLite provider, last in the close order.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, replace
from types import SimpleNamespace
from typing import Any, Callable, Mapping

from memory_server.runtime.config import RuntimeConfig
from memory_server.runtime.dependencies import RuntimeDependencies
from memory_server.runtime.runtime import CMMSRuntime

# Fixed construction order: SQLite first (the outbox borrows its engine), the
# vector/embedder/graph resources before the worker that consumes them.
RECIPE_ORDER: tuple[str, ...] = ("sqlite", "graph", "embedder", "vector", "outbox_worker")
# Labels whose projection can be marked unavailable (``paths.py:325-334``).
DEGRADABLE: tuple[str, ...] = ("vector", "graph")


@dataclass
class BuildStep:
    """One successfully built recipe step (name + the resource it produced)."""

    name: str
    resource: Any = None


def frozen_settings(config: RuntimeConfig) -> Any:
    """Settings-shaped view of the frozen create-time snapshot (C04).

    The attribute names are exactly the ones the provider constructors read from
    ``get_settings()``, so passing this view to them is a drop-in replacement for
    the ambient resolution: the runtime path resolves no ambient settings after
    ``create()``. Values come from ``RuntimeConfig``; nothing is re-read.
    """
    embedding = config.embedding
    return SimpleNamespace(
        lancedb_path=config.vector_local_path,
        vector_collection=config.vector_collection,
        vector_metric=config.vector_metric,
        vector_size=config.vector_size,
        qdrant_location=config.qdrant_location,
        qdrant_port=config.qdrant_port,
        qdrant_prefer_grpc=config.qdrant_prefer_grpc,
        embedding_model=embedding["model"],
        embedding_device=embedding["device"],
        embedding_batch_size=embedding["batch_size"],
        openai_embedding_model=embedding["openai_model"],
        openai_embedding_vector_size=embedding["openai_vector_size"],
        embedding_base_url=embedding["base_url"],
    )


def _unavailable(runtime: Any, label: str) -> bool:
    return label in getattr(runtime.layout, "unavailable_projections", frozenset())


async def build_sqlite(runtime: Any) -> Any:
    """Real SQLite engine + canonical schema; owned by the runtime."""
    from memory_server.providers.sqlite_provider import SQLiteProvider

    config = runtime.config
    provider = SQLiteProvider(url=config.sqlite_url, busy_timeout_ms=config.sqlite_busy_timeout_ms)
    await provider.initialize()
    return provider


def build_graph(runtime: Any) -> Any:
    """Snapshot-backed graph; an unavailable projection is never opened."""
    if _unavailable(runtime, "graph"):
        return None
    from memory_server.providers.graph_provider import SimpleGraph

    return SimpleGraph(snapshot_path=runtime.config.graph_snapshot_path)


def build_embedder(runtime: Any) -> Any:
    """Embedder selection captured at create() (no model download here)."""
    embedding = runtime.config.embedding
    settings = frozen_settings(runtime.config)
    if embedding["provider"] == "openai":
        from memory_server.providers.embedding_provider import OpenAIEmbeddingProvider
        from memory_server.settings import get_openai_api_key

        return OpenAIEmbeddingProvider(
            model=embedding["openai_model"],
            base_url=embedding["base_url"],
            vector_size=embedding["openai_vector_size"],
            api_key=get_openai_api_key(),
            settings=settings,
        )
    from memory_server.providers.embedding_provider import SentenceTransformerEmbeddingProvider

    return SentenceTransformerEmbeddingProvider(
        model_name=embedding["model"],
        device=embedding["device"],
        batch_size=embedding["batch_size"],
        settings=settings,
    )


def build_vector(runtime: Any) -> Any:
    """Vector backend recipe; the optional root is imported only when invoked."""
    config = runtime.config
    if _unavailable(runtime, "vector"):
        return None
    settings = frozen_settings(config)
    if config.vector_backend == "qdrant":
        import qdrant_client  # noqa: F401  optional root, imported inside the recipe

        from memory_server.providers.qdrant_provider import QdrantProvider

        return QdrantProvider(
            location=config.qdrant_location,
            port=config.qdrant_port,
            prefer_grpc=config.qdrant_prefer_grpc,
            collection=config.vector_collection,
            vector_size=config.vector_size,
            distance=config.vector_metric,
            settings=settings,
        )
    from memory_server.providers.lancedb_provider import LanceDBProvider

    return LanceDBProvider(
        db_path=str(config.vector_local_path),
        table=config.vector_collection,
        metric=config.vector_metric,
        vector_size=config.vector_size,
        settings=settings,
    )


async def build_outbox_worker(runtime: Any) -> Any:
    """Outbox worker over the BORROWED SQLite engine (never owns its disposal)."""
    dependencies = runtime.dependencies
    if dependencies.sqlite is None or dependencies.graph is None:
        # A degraded graph would make GraphRouter invent a second, memory-only
        # graph (graph_router.py:40-42); the runtime never invents a graph.
        return None
    from storage.outbox_worker import OutboxWorker

    from memory_server.router.graph_router import GraphRouter

    config = runtime.config
    graph_router = GraphRouter(
        graph=dependencies.graph,
        max_path_depth=config.routing["graph_max_path_depth"],
    )
    worker = OutboxWorker(
        engine=dependencies.sqlite.engine,
        qdrant=dependencies.vector,
        embedder=dependencies.embedder,
        graph_router=graph_router,
        **dict(config.outbox),
    )

    await worker.initialize()
    return worker


_DEFAULT_RECIPES: Mapping[str, Callable[[Any], Any]] = {
    "sqlite": build_sqlite,
    "graph": build_graph,
    "embedder": build_embedder,
    "vector": build_vector,
    "outbox_worker": build_outbox_worker,
}


class RuntimeFactory:
    """Explicit config snapshot + fixed ordered lazy recipes.

    ``overrides`` replaces a NAMED step (order and laziness are preserved) and is
    the only seam: it exists for the failure/close edges C02 allows, so a partial
    failure can be injected without weakening the production path.
    """

    def __init__(
        self,
        layout: Any,
        *,
        config: RuntimeConfig | None = None,
        settings: Any = None,
        dependencies: RuntimeDependencies | None = None,
        overrides: Mapping[str, Callable[[Any], Any]] | None = None,
        **runtime_kwargs: Any,
    ) -> None:
        self.layout = layout
        self.config = config if config is not None else RuntimeConfig.capture(layout, settings)
        self._overrides = dict(overrides or {})
        unknown = sorted(set(self._overrides) - set(RECIPE_ORDER))
        if unknown:
            raise ValueError(f"unknown recipe override: {unknown}")
        self._built: list[BuildStep] = []
        self.degraded = tuple(
            name for name in DEGRADABLE if name in getattr(layout, "unavailable_projections", frozenset())
        )
        base = dependencies if dependencies is not None else RuntimeDependencies()
        self.runtime = CMMSRuntime.create(
            layout,
            replace(base, recipes=self._recipes(), borrowed_engine=True),
            config=self.config,
            **runtime_kwargs,
        )

    @classmethod
    def create(cls, layout: Any, **kwargs: Any) -> RuntimeFactory:
        """Snapshot the config, build the recipes and return a NEW runtime."""
        return cls(layout, **kwargs)

    def _recording(self, name: str, builder: Callable[[Any], Any]) -> Callable[[Any], Any]:
        async def recipe(runtime: Any) -> Any:
            resource = builder(runtime)
            if inspect.isawaitable(resource):
                resource = await resource
            self._built.append(BuildStep(name=name, resource=resource))
            return resource

        return recipe

    def _recipes(self) -> tuple[tuple[str, Callable[[Any], Any]], ...]:
        return tuple(
            (name, self._recording(name, self._overrides.get(name, _DEFAULT_RECIPES[name]))) for name in RECIPE_ORDER
        )

    @property
    def built(self) -> tuple[tuple[str, Any], ...]:
        """Successfully built steps, in construction order (rollback authority)."""
        return tuple((step.name, step.resource) for step in self._built)

    @property
    def built_names(self) -> tuple[str, ...]:
        return tuple(step.name for step in self._built)

    def describe(self) -> dict[str, Any]:
        """Redacted, secrets-free description of the create-time snapshot."""
        return self.config.describe()
