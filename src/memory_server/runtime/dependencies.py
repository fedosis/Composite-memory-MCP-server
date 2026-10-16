"""Explicit resource ownership and adapter-neutral shutdown hooks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


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

    def __post_init__(self) -> None:
        object.__setattr__(self, "owns_close", frozenset(self.owns_close))
        object.__setattr__(self, "recipes", tuple(self.recipes))
        names = [name for name, _ in self.recipes]
        if len(names) != len(set(names)) or any(name not in self.__dataclass_fields__ for name in names):
            raise ValueError("invalid dependency recipes")
