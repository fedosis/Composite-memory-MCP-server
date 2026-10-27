"""Explicit resource ownership and adapter-neutral shutdown hooks."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Callable, Protocol

if TYPE_CHECKING:
    from memory_server.runtime.runtime import ShutdownReport

# Close order of the resources the runtime OWNS: SQLite last, because the outbox
# worker borrows its engine (``outbox_worker.py:140-143``) and a borrowed engine
# must never be disposed by the borrower. Construction follows the reverse path.
CLOSE_ORDER: tuple[str, ...] = ("vector", "embedder", "graph", "sqlite")


class RuntimeHooks(Protocol):
    async def stop_admission(self) -> None: ...
    async def drain_producers(self, deadline: float) -> None: ...
    async def pause_writer(self) -> None: ...
    async def settle_writer(self, deadline: float) -> Any: ...
    async def flush_final(self, deadline: float) -> Any: ...
    async def invalidate_prefetch_generation(self) -> None: ...


@dataclass(frozen=True)
class RuntimeDependencies:
    sqlite: Any = None
    vector: Any = None
    embedder: Any = None
    graph: Any = None
    graph_router: Any = None
    embedding_router: Any = None
    hybrid_router: Any = None
    outbox_worker: Any = None
    confidence_engine: Any = None
    decay_engine: Any = None
    validator: Any = None
    hooks: Any = None
    owns_close: frozenset[str] = field(default_factory=lambda: frozenset({"sqlite", "vector", "embedder", "graph"}))
    # The outbox borrows SQLiteProvider.engine; it never owns its disposal.
    borrowed_engine: bool = False
    # Neutral construction seam only. Concrete backend recipes belong to C03.
    recipes: tuple[tuple[str, Any], ...] = ()
    # Per-runtime report construction; transports may supply their own shape.
    report_factory: Callable[..., ShutdownReport] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "owns_close", frozenset(self.owns_close))
        object.__setattr__(self, "recipes", tuple(self.recipes))
        names = [name for name, _ in self.recipes]
        if len(names) != len(set(names)) or any(name not in self.__dataclass_fields__ for name in names):
            raise ValueError("invalid dependency recipes")

    def with_resource(self, name: str, resource: Any) -> "RuntimeDependencies":
        """Return a NEW frozen container holding ``resource`` under ``name``.

        The runtime never mutates the snapshot it was created with; a recipe
        result becomes visible only through the returned container.
        """
        if name not in self.__dataclass_fields__:
            raise ValueError(f"unknown dependency: {name}")
        return replace(self, **{name: resource})
