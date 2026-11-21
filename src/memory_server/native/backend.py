"""Native owner-service backend seam (slice T3).

Bridges the sessionless native transport to the shared Hermes ``HermesProvider``
runtime, which owns every ``WriterQueue``, ``aiosqlite`` connection, outbox and
LanceDB handle on the process-wide Hermes owner loop
(``memory_server.transports.hermes._get_loop()``).

Every backend method is therefore dispatched onto that owner loop with
``run_coroutine_threadsafe`` and awaited through a ``wrap_future``; a timeout
never cancels the backend coroutine, it only stops waiting for it.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Mapping

from memory_server.native.config import ServiceConfig, build_plugin_config
from memory_server.plugins.hermes.provider import HermesProvider
from memory_server.transports.hermes import _get_loop, _run_async


@dataclass(frozen=True)
class PendingSnapshot:
    """Admission view reported by the native adapter's ``pending_snapshot``."""

    live_pending: int
    terminal_failed: int
    retry_waiting: int
    in_flight: int


class BackendStartError(RuntimeError):
    """A backend start failure with a stable diagnostic code."""

    def __init__(self, code: str, cause: BaseException | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.cause = cause


class NativeBackendAdapter:
    """Owns a single Hermes provider lease and exposes its async seams.

    ``provider`` is an injection seam for unit tests: passing one skips the
    real ``HermesProvider().initialize(...)`` inside ``start()`` so tests can
    drive the adapter with fake objects while still exercising the real owner
    loop. ``runtime`` raises ``BackendStartError("E_NOT_STARTED")`` until a
    provider is bound (by ``start()`` or by injection).
    """

    def __init__(self, config: ServiceConfig, provider: HermesProvider | None = None) -> None:
        self._config = config
        self._provider = provider

    @property
    def runtime(self) -> Any:
        if self._provider is None:
            raise BackendStartError("E_NOT_STARTED")
        return self._provider

    @property
    def backend_loop(self) -> asyncio.AbstractEventLoop:
        return _get_loop()

    @property
    def store_paths(self) -> dict[str, str]:
        """Bare store basenames (no directories), resolved from the service config."""
        from memory_server.settings import get_settings

        plugin_cfg = build_plugin_config(self._config)
        layout = plugin_cfg.resolve_storage_layout(
            hermes_home=str(self._config.storage_context_home),
            settings=get_settings(),
        )
        sqlite = layout.sqlite.local_path
        vector = layout.vector.local_path
        graph = layout.graph_snapshot_path
        return {
            "sqlite": sqlite.name if sqlite is not None else "",
            "vector": vector.name if vector is not None else "",
            "graph": graph.name,
        }

    async def start(self) -> None:
        try:
            await asyncio.to_thread(self._start_sync)
        except BackendStartError:
            raise
        except BaseException as exc:
            raise BackendStartError("E_START_FAILED", cause=exc) from exc

    def _start_sync(self) -> None:
        provider = HermesProvider()
        # HermesProvider.initialize() ожидает dict (внутри HermesPluginConfig.from_dict).
        # asdict() здесь неприменим: HermesPluginConfig содержит mappingproxy.
        backend = self._config.backend
        plugin_config: dict[str, Any] = {
            "storage_mode": backend.storage_mode,
            "data_root": str(self._config.data_root),
            "vector_backend": backend.vector_backend,
            "vector_collection": backend.vector_collection,
            "extraction_mode": backend.extraction_mode,
            "llm_model": backend.llm_model,
            "llm_base_url": backend.llm_base_url,
            "max_facts": backend.max_facts,
            "writer": {
                "flush_interval": backend.writer.flush_interval,
                "max_batch": backend.writer.max_batch,
            },
        }
        if backend.db_url is not None:
            plugin_config["db_url"] = backend.db_url
        if backend.lancedb_path is not None:
            plugin_config["lancedb_path"] = backend.lancedb_path
        if backend.graph_snapshot_path is not None:
            plugin_config["graph_snapshot_path"] = backend.graph_snapshot_path
        provider.initialize(
            "native-service",
            hermes_home=str(self._config.storage_context_home),
            config=plugin_config,
        )
        self._provider = provider

    async def close(self) -> Any:
        return await asyncio.to_thread(self._close_sync)

    def _close_sync(self) -> Any:
        provider = self._provider
        if provider is None:
            return None
        provider.shutdown()
        bridge = getattr(provider, "_bridge", None)
        if bridge is None:
            return None
        return _run_async(bridge.close(), timeout=30.0)

    async def _on_backend_loop(self, coro, *, timeout: float):
        fut = asyncio.run_coroutine_threadsafe(coro, _get_loop())
        wrapped = asyncio.wrap_future(fut)
        done, _ = await asyncio.wait({wrapped}, timeout=timeout)
        if not done:
            wrapped.add_done_callback(lambda f: f.cancelled() or f.exception())
            raise TimeoutError
        return wrapped.result()

    async def execute_tool_async(self, tool_name: str, args: Mapping[str, Any]) -> str:
        return await self._on_backend_loop(
            self._provider.execute_tool_async(tool_name, dict(args)),
            timeout=self._config.limits.tool_timeout_s,
        )

    async def recall_context_async(self, query: str, *, session_id: str, max_results: int = 5) -> str:
        return await self._on_backend_loop(
            self._provider.recall_context_async(query, session_id=session_id, max_results=max_results),
            timeout=self._config.limits.recall_timeout_s,
        )

    async def enqueue_turn_async(self, messages: Any, *, session_id: str) -> None:
        await self._on_backend_loop(
            self._provider.enqueue_observation_async(
                messages,
                session_id=session_id,
                live_limit=self._config.limits.live_turns,
                terminal_limit=self._config.limits.terminal_failed,
            ),
            timeout=self._config.limits.enqueue_timeout_s,
        )

    async def flush_observations_async(self) -> dict[str, int]:
        return await self._on_backend_loop(
            self._provider.flush_observations_async(),
            timeout=self._config.limits.flush_timeout_s,
        )

    async def pending_snapshot(self) -> PendingSnapshot:
        async def _read() -> PendingSnapshot:
            return self._provider._writer.pending_snapshot()

        return await self._on_backend_loop(_read(), timeout=2.0)
