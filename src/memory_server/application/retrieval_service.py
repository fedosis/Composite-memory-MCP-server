"""Transport-free retrieval application services."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from memory_server.api.get_context import get_context
from memory_server.api.search import search
from memory_server.application.contracts import (
    ContextCommand,
    RetrievalPolicy,
    RouteCommand,
    SearchCommand,
    SemanticSearchCommand,
)
from memory_server.application.errors import UnknownDependencyError


class RetrievalService:
    """Application retrieval use cases over borrowed initialized dependencies."""

    def __init__(
        self,
        provider: Any,
        *,
        policy: RetrievalPolicy | None = None,
        get_embedding_router: Callable[[], Awaitable[Any]] | None = None,
        get_hybrid_router: Callable[[], Awaitable[Any]] | None = None,
    ) -> None:
        self._provider = provider
        self._policy = policy if policy is not None else RetrievalPolicy.capture()
        self._get_embedding_router = get_embedding_router
        self._get_hybrid_router = get_hybrid_router

    @property
    def policy(self) -> RetrievalPolicy:
        return self._policy

    @staticmethod
    def _optional(value: str | None) -> str | None:
        return None if value == "" else value

    async def search(self, command: SearchCommand) -> dict:
        limit = self._policy.search_default_limit if command.limit is None else command.limit
        return await search(
            self._provider,
            query=command.query,
            subject=self._optional(command.subject),
            predicate=self._optional(command.predicate),
            limit=limit,
            include_inactive=command.include_inactive,
        )


    async def get_context(self, command: ContextCommand) -> dict:
        max_results = (
            self._policy.context_default_limit
            if command.max_results is None
            else command.max_results
        )

        return await get_context(
            self._provider,
            task=command.task,
            subject=self._optional(command.subject),
            max_results=max_results,
            include_inactive=command.include_inactive,
            overfetch_factor=self._policy.context_dedup_overfetch_factor,
        )


    async def semantic_search(self, command: SemanticSearchCommand) -> dict:
        if self._get_embedding_router is None:
            raise UnknownDependencyError("embedding router")
        router = await self._get_embedding_router()
        top_k = self._policy.semantic_top_k if command.top_k is None else command.top_k
        score_threshold = (
            self._policy.semantic_score_threshold
            if command.score_threshold is None
            else command.score_threshold
        )

        return await router.route(
            query=command.query,
            top_k=top_k,
            score_threshold=score_threshold,
        )


    async def route(self, command: RouteCommand) -> dict:
        if self._get_hybrid_router is None:
            raise UnknownDependencyError("hybrid router")
        router = await self._get_hybrid_router()
        top_k = self._policy.semantic_top_k if command.top_k is None else command.top_k
        score_threshold = (
            self._policy.semantic_score_threshold
            if command.score_threshold is None
            else command.score_threshold
        )

        return await router.route(
            query=command.query,
            top_k=top_k,
            score_threshold=score_threshold,
        )
