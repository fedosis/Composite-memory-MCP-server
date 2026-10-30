"""Production read/search composition without opening any profile runtime."""

import asyncio
from dataclasses import replace

from memory_server.admission import MemoryAdmissionGate
from memory_server.application.errors import UnknownDependencyError
from memory_server.application.gateway_retrieval import GatewayRetrievalPort, NativeCustody
from memory_server.application.graph_read_service import GraphReadResources, create_graph_read_port
from memory_server.application.memory_read_service import MemoryReadService
from memory_server.gateway_contracts.ports import ProfileSession
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.routes.graph import RelationCursorCodec, create_graph_read_routes
from memory_server.transports.http.routes.memories_read import CursorCodec, create_memory_read_routes
from memory_server.transports.http.routes.retrieval import create_context_routes, create_search_routes


def bind_read(scope):
    try:
        return scope.application.read_resources()
    except UnknownDependencyError:
        raise HttpError("E_RUNTIME_NOT_READY") from None


def bind_graph_read(scope: ProfileSession) -> GraphReadResources:
    return GraphReadResources(bind_read(scope), scope.runtime.dependencies.graph)


class _BoundNativeCustody:
    def __init__(self, scope):
        self._submit = scope.runtime.submit_native
        self._snapshot = scope.runtime.owned_snapshot
        self._handles = []

    async def __call__(self, function, /, *args, kind, **kwargs):
        handle = await self._submit(function, *args, kind=kind, **kwargs)
        self._handles.append(handle)
        return handle

    async def drain(self):
        # Capture once after producers have settled: don't chase future work
        # belonging to other requests/outbox indefinitely.
        _, _, captured = self._snapshot()
        while (any(not handle.terminated for handle in self._handles)
               or any(record.state != "TERMINATED" for record in captured)):
            await asyncio.sleep(0.001)
        for handle in self._handles:
            try:
                await handle.result()
            except Exception:
                # Result was already handled by the source; consume without logs.
                pass


def bind_native(scope: ProfileSession) -> NativeCustody:
    return _BoundNativeCustody(scope)


def compose_read_ports(deps, config):
    if deps.leases is None:
        raise HttpError("E_RUNTIME_NOT_READY")
    codec = CursorCodec.from_path(config.control_path.parent / "http-memory-cursor.key", config.cursor_ttl_seconds)
    reader = MemoryReadService(deps.leases, bind_read, classifier=MemoryAdmissionGate(), cursor_codec=codec)
    specs = (
        create_memory_read_routes(reader, None, cursor_codec=codec) + create_search_routes() + create_context_routes()
    )
    ports = dict(deps.ports)
    ports.update({spec.operation: reader for spec in specs if spec.operation not in {"search", "context"}})
    gateway = GatewayRetrievalPort(deps.leases, bind_read, deps.metrics, native=bind_native)
    ports["search"] = gateway
    ports["context"] = gateway
    relation_codec = RelationCursorCodec.from_path(
        config.control_path.parent / 'http-relation-cursor.key', config.cursor_ttl_seconds)
    graph_reader = create_graph_read_port(deps.leases, bind_graph_read, native=bind_native,
                                          cursor_codec=relation_codec, metrics=deps.metrics)
    graph_specs = create_graph_read_routes(cursor_codec=relation_codec)
    ports.update({spec.operation: graph_reader for spec in graph_specs})
    specs += graph_specs
    return replace(deps, ports=ports), specs
