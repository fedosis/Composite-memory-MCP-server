"""Production read/search composition without opening any profile runtime."""

from dataclasses import replace

from memory_server.admission import MemoryAdmissionGate
from memory_server.application.errors import UnknownDependencyError
from memory_server.application.gateway_retrieval import GatewayRetrievalPort
from memory_server.application.memory_read_service import MemoryReadService
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.routes.memories_read import CursorCodec, create_memory_read_routes
from memory_server.transports.http.routes.retrieval import create_context_routes, create_search_routes


def bind_read(scope):
    try:
        return scope.application.read_resources()
    except UnknownDependencyError:
        raise HttpError("E_RUNTIME_NOT_READY") from None


def compose_read_ports(deps, config):
    if deps.leases is None:
        raise HttpError("E_RUNTIME_NOT_READY")
    codec = CursorCodec.from_path(config.control_path.parent / "http-memory-cursor.key", config.cursor_ttl_seconds)
    reader = MemoryReadService(deps.leases, bind_read, classifier=MemoryAdmissionGate(), cursor_codec=codec)
    specs = (
        create_memory_read_routes(reader, None, cursor_codec=codec) + create_search_routes() + create_context_routes()
    )
    ports = dict(deps.ports)
    ports.update({spec.operation: reader for spec in specs if spec.operation != "search"})
    gateway = GatewayRetrievalPort(deps.leases, bind_read, deps.metrics)
    ports["search"] = gateway
    ports["context"] = gateway
    return replace(deps, ports=ports), specs
