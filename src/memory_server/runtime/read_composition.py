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
from memory_server.application.inspection_service import InspectionService
from memory_server.application.memory_read_service import MemoryReadService
from memory_server.application.remote_beliefs import RemoteBeliefsMutationPort, RemoteBeliefsPort
from memory_server.application.remote_ingestion import RemoteIngestionPort
from memory_server.application.remote_lifecycle import RemoteLifecyclePort
from memory_server.application.remote_relations import RemoteRelationsWritePort
from memory_server.application.retrieval_trace import RetrievalTraceService
from memory_server.control.traces import TracesStore
from memory_server.gateway_contracts.ports import ProfileSession
from memory_server.settings import Settings
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.routes.beliefs import (
    BeliefCursorCodec,
    create_belief_mutation_routes,
    create_belief_read_routes,
)
from memory_server.transports.http.routes.diagnostics import OutboxCursorCodec, create_diagnostics_routes
from memory_server.transports.http.routes.graph import RelationCursorCodec, create_graph_read_routes
from memory_server.transports.http.routes.ingestion import create_ingestion_routes
from memory_server.transports.http.routes.inspectors import create_inspection_routes
from memory_server.transports.http.routes.lifecycle import create_lifecycle_routes
from memory_server.transports.http.routes.memories_read import CursorCodec, create_memory_read_routes
from memory_server.transports.http.routes.profiles import audit_ticket
from memory_server.transports.http.routes.relations_write import create_relation_mutation_routes
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


def compose_ingestion_ports(deps, config):
    """Bind S13 to the Guard ticket without importing transport state in application code."""
    del config
    if deps.leases is None:
        raise HttpError("E_RUNTIME_NOT_READY")

    def ticket():
        current = audit_ticket.get()
        return current.id if current is not None else None

    port = RemoteIngestionPort(
        deps,
        ticket_provider=ticket,
        publish_outcome=deps.access.publish_committed,
    )
    specs = create_ingestion_routes()
    ports = dict(deps.ports)
    ports.update({spec.operation: port for spec in specs})
    return replace(deps, ports=ports), specs


def compose_lifecycle_ports(deps, config):
    """Bind S14 to the current request audit ticket and descriptors."""
    if deps.leases is None:
        raise HttpError("E_RUNTIME_NOT_READY")

    def ticket():
        current = audit_ticket.get()
        return current.id if current is not None else None

    port = RemoteLifecyclePort(
        deps,
        ticket_provider=ticket,
        publish_outcome=deps.access.publish_committed,
        ttl_seconds=config.idempotency_ttl_seconds,
        events=deps.events,
    )
    specs = create_lifecycle_routes()
    ports = dict(deps.ports)
    ports.update({spec.operation: port for spec in specs})
    return replace(deps, ports=ports), specs


def compose_belief_read_ports(deps, config):
    if deps.leases is None:
        raise HttpError("E_RUNTIME_NOT_READY")
    codec = BeliefCursorCodec.from_path(
        config.control_path.parent / "http-belief-cursor.key", config.cursor_ttl_seconds
    )
    port = RemoteBeliefsPort(deps, bind_read, cursor_codec=codec)
    specs = create_belief_read_routes(cursor_codec=codec)
    ports = dict(deps.ports)
    ports.update({spec.operation: port for spec in specs})
    return replace(deps, ports=ports), specs


def compose_belief_mutation_ports(deps, config):
    if deps.leases is None:
        raise HttpError("E_RUNTIME_NOT_READY")

    def ticket():
        current = audit_ticket.get()
        return current.id if current is not None else None

    port = RemoteBeliefsMutationPort(
        deps,
        ticket_provider=ticket,
        publish_outcome=deps.access.publish_committed,
        ttl_seconds=config.idempotency_ttl_seconds,
        events=deps.events,
    )
    specs = create_belief_mutation_routes()
    ports = dict(deps.ports)
    ports.update({spec.operation: port for spec in specs})
    return replace(deps, ports=ports), specs


def compose_relation_mutation_ports(deps, config):
    if deps.leases is None:
        raise HttpError("E_RUNTIME_NOT_READY")

    def ticket():
        current = audit_ticket.get()
        return current.id if current is not None else None

    port = RemoteRelationsWritePort(
        deps,
        ticket_provider=ticket,
        publish_outcome=deps.access.publish_committed,
        ttl_seconds=config.idempotency_ttl_seconds,
        events=deps.events,
    )
    specs = create_relation_mutation_routes()
    ports = dict(deps.ports)
    ports.update({spec.operation: port for spec in specs})
    return replace(deps, ports=ports), specs


def compose_inspection_ports(deps, config):
    del config
    if deps.leases is None:
        raise HttpError("E_RUNTIME_NOT_READY")
    service = InspectionService(deps, bind_read)
    specs = create_inspection_routes()
    ports = dict(deps.ports)
    ports.update({spec.operation: service for spec in specs})
    return replace(deps, ports=ports), specs


def compose_job_ports(deps, config):
    from dataclasses import replace

    from memory_server.application.job_runner import JobCoordinator
    from memory_server.application.job_service import JobService
    from memory_server.control.jobs import JobMetadata
    from memory_server.transports.http.routes.jobs import JobReadAdapter, create_job_routes

    del config
    metadata = JobMetadata(deps.registry.control_store)
    publish_event = deps.events.publish if deps.events is not None else None
    jobs = JobService(
        metadata, deps.registry, deps.tokens, deps.access.begin, deps.access.publish_committed, events=publish_event
    )
    coordinator = JobCoordinator(metadata, deps.leases, authorize=jobs.authorize, events=publish_event)
    # Concrete maintenance runners/fences belong to S19/S20: read-only until injected.
    jobs = replace(jobs, attach=coordinator.attach if coordinator.replay or coordinator.rebuild else None)
    return replace(deps, jobs=jobs, ports={**deps.ports, "jobs.get": JobReadAdapter(jobs)}), create_job_routes()


class JobRecoveryOwner:
    def __init__(self, registry, leases, *, bindings, factory_builder, timeout_seconds):
        import math

        from memory_server.runtime.runtime_factory import RuntimeFactory
        from memory_server.runtime.runtime_manager import RuntimeDescriptor

        if (
            type(bindings) is not tuple
            or not bindings
            or type(timeout_seconds) not in (int, float)
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
            or (factory_builder is not None and not callable(factory_builder))
        ):
            raise ValueError()
        for index, binding in enumerate(bindings):
            root = binding.layout.data_root
            if (
                not root.is_absolute()
                or root != root.resolve()
                or binding.config.layout != binding.layout
                or binding.descriptor
                != RuntimeDescriptor.from_layout(binding.profile.id, layout=binding.layout, config=binding.config)
                or any(
                    root == prior.layout.data_root
                    or binding.profile.id == prior.profile.id
                    or binding.layout == prior.layout
                    or binding.config == prior.config
                    for prior in bindings[:index]
                )
            ):
                raise ValueError()
        self._registry, self._bindings, self._control_store = registry, bindings, registry.control_store
        self._factory_builder = factory_builder or RuntimeFactory.create
        self._timeout_seconds = timeout_seconds
        self._leases = None
        self._runtimes = {}
        self._coordinator = None
        self._closed = False
        if leases is not None:
            self.bind_leases(leases)

    def bind_leases(self, leases) -> None:
        if self._closed or self._leases is not None or self._runtimes:
            raise ValueError()
        if (
            leases.registry is not self._registry
            or leases.registry.control_store is not self._control_store
            or leases.snapshot()
        ):
            raise ValueError()
        self._leases = leases

    def factory_builder(self, layout, *, config, dependencies=None):
        from memory_server.runtime.runtime_manager import RuntimeDescriptor

        if self._closed or self._leases is None:
            raise ValueError()
        matches = [binding for binding in self._bindings if binding.layout == layout and binding.config == config]
        if len(matches) != 1:
            raise ValueError()
        binding = matches[0]
        if binding.descriptor != RuntimeDescriptor.from_layout(binding.profile.id, layout=layout, config=config):
            raise ValueError()
        previous = self._runtimes.get(binding.profile.id)
        if previous is not None and (previous.state != "CLOSED" or previous.lock is not None):
            raise ValueError()
        factory = self._factory_builder(layout, config=config, dependencies=dependencies, profile_id=binding.profile.id)
        runtime = factory.runtime
        if (
            runtime.state != "NEW"
            or runtime.lock is not None
            or runtime.profile_id != binding.profile.id
            or runtime.root != binding.layout.data_root
            or runtime.layout != layout
            or runtime.config != config
        ):
            raise ValueError()
        self._runtimes[binding.profile.id] = runtime
        runtime.configure_startup_recovery(self._startup_recovery, timeout_seconds=self._timeout_seconds)
        return factory

    async def _startup_recovery(self, runtime, *, deadline):
        from memory_server.application.job_runner import JobCoordinator
        from memory_server.control.jobs import JobError, JobMetadata
        from memory_server.runtime.errors import RuntimeNotReadyError

        def startup_custody(custody, *, profile_id):
            runtime = self._runtimes.get(profile_id)
            if self._closed or self._leases is None or runtime is None or runtime is not custody:
                raise RuntimeNotReadyError()
            binding = next((binding for binding in self._bindings if binding.profile.id == profile_id), None)
            if binding is None or runtime.root != binding.layout.data_root:
                raise RuntimeNotReadyError()
            runtime.check_startup_custody(profile_id=profile_id, canonical_root=binding.layout.data_root)
            observation = self._leases.snapshot().get(profile_id)
            if (
                observation is None
                or observation.state != "not_started"
                or observation.admissions
                or observation.stopping
                or (observation.retained and observation.owned)
            ):
                raise RuntimeNotReadyError()

        startup_custody(runtime, profile_id=runtime.profile_id)
        if self._coordinator is None:
            self._coordinator = JobCoordinator(
                JobMetadata(self._control_store), self._leases, authorize=None, startup_custody=startup_custody
            )
        try:
            await self._coordinator.reconcile_startup(profile_id=runtime.profile_id, custody=runtime, deadline=deadline)
        except TimeoutError:
            raise JobError("E_TIMEOUT") from None

    async def close(self, *, deadline) -> None:
        self._closed = True
        if self._coordinator is not None:
            await self._coordinator.close(deadline=deadline)


def create_job_recovery_builder(
    registry, leases, *, bindings, factory_builder=None, timeout_seconds
) -> JobRecoveryOwner:
    return JobRecoveryOwner(
        registry, leases, bindings=bindings, factory_builder=factory_builder, timeout_seconds=timeout_seconds
    )
