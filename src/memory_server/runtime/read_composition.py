"""Production read/search composition without opening any profile runtime."""

import asyncio
import hashlib
import json
from dataclasses import replace

from memory_server.admission import MemoryAdmissionGate
from memory_server.application.diagnostics_service import DiagnosticsResources, create_diagnostics_port
from memory_server.application.errors import UnknownDependencyError
from memory_server.application.gateway_retrieval import GatewayRetrievalPort, NativeCustody
from memory_server.application.graph_read_service import GraphReadResources, create_graph_read_port
from memory_server.application.memory_read_service import MemoryReadService
from memory_server.application.retrieval_trace import RetrievalTraceService
from memory_server.control.traces import TracesStore
from memory_server.gateway_contracts.ports import ProfileSession
from memory_server.settings import Settings
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.routes.diagnostics import OutboxCursorCodec, create_diagnostics_routes
from memory_server.transports.http.routes.graph import RelationCursorCodec, create_graph_read_routes
from memory_server.transports.http.routes.memories_read import CursorCodec, create_memory_read_routes
from memory_server.transports.http.routes.retrieval import create_context_routes, create_search_routes
from memory_server.transports.http.routes.traces import TraceCursorCodec, create_trace_routes


def bind_read(scope):
    try:
        return scope.application.read_resources()
    except UnknownDependencyError:
        raise HttpError("E_RUNTIME_NOT_READY") from None


def bind_graph_read(scope: ProfileSession) -> GraphReadResources:
    return GraphReadResources(bind_read(scope), scope.runtime.dependencies.graph)


def bind_diagnostics(scope: ProfileSession) -> DiagnosticsResources:
    canonical = bind_read(scope)
    dependencies = scope.runtime.dependencies
    try:
        vector = dependencies.vector
    except UnknownDependencyError:
        vector = None
    try:
        graph = dependencies.graph
    except UnknownDependencyError:
        graph = None
    try:
        reflection = scope.application.reflection
    except UnknownDependencyError:
        reflection = None
    try:
        audit = scope.application.audit
    except UnknownDependencyError:
        validator = confidence = None
    else:
        try:
            validator = audit._validator
        except UnknownDependencyError:
            validator = None
        try:
            confidence = audit._confidence_engine
        except UnknownDependencyError:
            confidence = None
    return DiagnosticsResources(
        canonical=canonical,
        profile=scope.binding.profile.model_copy(update={"storage_root": None}),
        vector=vector,
        graph=graph,
        reflection=reflection,
        validator=validator,
        confidence=confidence,
        writer_owned=scope.runtime.lock is not None,
    )


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
        while any(not handle.terminated for handle in self._handles) or any(
            record.state != "TERMINATED" for record in captured
        ):
            await asyncio.sleep(0.001)
        for handle in self._handles:
            try:
                await handle.result()
            except Exception:
                # Result was already handled by the source; consume without logs.
                pass


def bind_native(scope: ProfileSession) -> NativeCustody:
    return _BoundNativeCustody(scope)


def bind_traces(deps, config):
    trace_codec = TraceCursorCodec.from_path(
        config.control_path.parent / "http-trace-cursor.key", min(900, config.cursor_ttl_seconds)
    )
    settings = Settings()
    trace_store = TracesStore(
        config.control_path.parent,
        ttl_seconds=settings.trace_ttl_seconds,
        max_per_profile=settings.trace_max_per_profile,
        max_payload_bytes=settings.trace_max_payload_bytes,
    )
    digest = hashlib.sha256(
        json.dumps(
            {
                "top_k": 20,
                "mode": "hybrid",
                "threshold": 0.0,
                "trace_ttl": settings.trace_ttl_seconds,
                "trace_cap": settings.trace_max_per_profile,
                "trace_payload": settings.trace_max_payload_bytes,
                "trace_contrib": settings.trace_max_contributions,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return RetrievalTraceService(
        deps.leases,
        trace_store,
        cursor_codec=trace_codec,
        metrics=deps.metrics,
        max_contributions=settings.trace_max_contributions,
        settings_digest=digest,
    )


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
    traces = bind_traces(deps, config)
    gateway.traces = traces
    ports["search"] = gateway
    ports["context"] = gateway
    relation_codec = RelationCursorCodec.from_path(
        config.control_path.parent / "http-relation-cursor.key", config.cursor_ttl_seconds
    )
    graph_reader = create_graph_read_port(
        deps.leases, bind_graph_read, native=bind_native, cursor_codec=relation_codec, metrics=deps.metrics
    )
    graph_specs = create_graph_read_routes(cursor_codec=relation_codec)
    ports.update({spec.operation: graph_reader for spec in graph_specs})
    specs += graph_specs
    outbox_codec = OutboxCursorCodec.from_path(
        config.control_path.parent / "http-outbox-cursor.key", config.cursor_ttl_seconds
    )
    diagnostics = create_diagnostics_port(
        deps.leases, bind_diagnostics, native=bind_native, cursor_codec=outbox_codec, metrics=deps.metrics
    )
    diagnostic_specs = create_diagnostics_routes(cursor_codec=outbox_codec)
    ports.update({spec.operation: diagnostics for spec in diagnostic_specs})
    specs += diagnostic_specs
    trace_specs = create_trace_routes(cursor_codec=traces.cursor_codec)
    ports.update({spec.operation: traces for spec in trace_specs})
    specs += trace_specs
    return replace(deps, ports=ports), specs
