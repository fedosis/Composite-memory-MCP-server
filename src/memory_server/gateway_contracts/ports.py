"""C1 injectable Protocols and frozen context records; no implementations.

Foreign C1 composition types remain symbolic forward annotations. Resolve them
with the composition's explicit localns, not by importing concrete neighbours.
S23-S26 mount Protocols describe the Python test ABI, not JS runtime modules.
"""

# ruff: noqa: F821
from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager as AsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from math import isfinite
from pathlib import Path
from types import MappingProxyType
from typing import Literal, Protocol
from uuid import UUID

from . import dto as d

# S06 amendment Q9 (2026-10-28): eligibility declaration, not HTTP orchestration.
# Only E_AUTH_REQUIRED uses the existing identity-free S05a denial callback.
AUDIT_ELIGIBILITY: Mapping[str, Literal["none", "denial", "intent_outcome"]] = MappingProxyType({
    "pre_auth_reject": "none",
    "authentication_failure": "denial",
    "authentication_storage_failure": "none",
    "authenticated_authz_reject": "none",
    "authenticated_validation_reject": "none",
    "authenticated_rate_reject": "none",
    "eligible_execution": "intent_outcome",
    "public_probe": "none",
})

ASGIApp = Callable[
    [Mapping[str, object], Callable[[], Awaitable[dict]], Callable[[dict], Awaitable[None]]], Awaitable[None]
]


@dataclass(frozen=True)
class Principal:
    token_id: str
    subject: str
    client_id: str | None
    profiles: frozenset[str]
    permissions: frozenset[d.Permission]
    credential_generation: int
    expires_at: datetime


@dataclass(frozen=True)
class RequestContext:
    request_id: str
    principal: Principal
    profile_id: str | None
    idempotency_key: UUID | None
    deadline: float  # monotonic


@dataclass(frozen=True)
class ProfileRecord:
    id: str
    canonical_root: Path
    enabled: bool
    display_name: str
    description: str | None
    permissions_metadata: Mapping[str, tuple[d.Permission, ...]]
    generation: int


@dataclass(frozen=True)
class ProfileBinding:
    profile: d.Profile
    layout: StorageLayout
    config: RuntimeConfig
    descriptor: RuntimeDescriptor


@dataclass(frozen=True)
class ProfileSession:
    binding: ProfileBinding
    application: MemoryApplication
    runtime: CMMSRuntime  # only application/composition; HTTP routes must not dereference providers


@dataclass(frozen=True)
class AuditTicket:
    id: str
    request_id: str


@dataclass(frozen=True)
class AuditOutcome:
    status: int
    error_code: d.ErrorCode | None
    receipt_ids: tuple[str, ...]
    latency_ms: float


@dataclass(frozen=True)
class RuntimeObservation:
    state: Literal["not_started", "ready", "closing", "failed"]
    owned: bool
    admissions: int
    stopping: bool
    retained: bool


@dataclass(frozen=True)
class ControlReply:
    status: int
    body: d.Profile | Mapping[str, d.JSONValue]
    replayed: bool
    original_request_id: str


class MetricsSink(Protocol):
    def http_completed(
        self, ctx: RequestContext, operation: d.Operation, status: int, duration_seconds: float
    ) -> None: ...
    def retrieval_completed(
        self,
        ctx: RequestContext,
        stage: Literal["fts", "semantic", "graph", "merge", "context"],
        duration_seconds: float,
        error_code: d.ErrorCode | None,
    ) -> None: ...
    def projection_snapshot(
        self, profile_id: str, pending: int, vector_lag: int | None, graph_lag: int | None
    ) -> None: ...


class MutationHandler(Protocol):
    async def apply(self, session: AsyncSession, command: d.ProfileMutationCommand) -> d.MutationResult: ...


class TraceObserver(Protocol):
    def stage(
        self,
        name: Literal["fts", "semantic", "graph", "context"],
        items: list[d.TraceContribution],
        status: Literal["ran", "skipped", "failed"],
    ) -> None: ...
    def merged(self, items: list[d.TraceContribution], deduplicated: list[d.TraceContribution]) -> None: ...
    def final(self, items: list[d.TraceContribution]) -> None: ...


class GatewayPort(Protocol):
    async def execute(self, ctx: RequestContext, command: d.GatewayCommand) -> d.GatewayResult: ...


class ContractCatalog(Protocol):
    def validate(self, operation: d.Operation, payload: d.JSONValue) -> d.GatewayCommand: ...


class ProfileRegistry(Protocol):
    def available(self) -> bool: ...

    async def resolve(self, principal: Principal, profile_id: str) -> ProfileBinding: ...
    async def page(
        self, principal: Principal, *, limit: int, after: str | None, upper: str | None
    ) -> tuple[tuple[d.Profile, ...], str | None]: ...
    async def record(self, principal: Principal, profile_id: str, *, permission: d.Permission) -> ProfileRecord: ...

    async def control(
        self,
        ctx: RequestContext,
        command: d.ProfilesEnableCommand | d.ProfilesDisableCommand,
        *,
        ticket: AuditTicket,
        leases: ProfileLeases,
        publish: Callable[[AuditTicket, AuditOutcome], Awaitable[bool]],
        now: datetime,
        ttl_seconds: int,
    ) -> ControlReply: ...

    async def reconcile_control(
        self,
        *,
        leases: ProfileLeases,
        publish: Callable[[AuditTicket, AuditOutcome], Awaitable[bool]],
        limit: int,
    ) -> bool: ...


class TokenService(Protocol):
    def available(self) -> bool: ...

    async def authenticate(self, bearer: str, *, now: datetime) -> Principal: ...

    def verify_principal(self, principal: Principal, *, now: datetime) -> None: ...


class ProfileLeases(Protocol):
    def acquire(self, ctx: RequestContext) -> AsyncContextManager[ProfileSession]: ...
    async def stop_profile(self, profile_id: str) -> ShutdownReport | None: ...
    def snapshot(self) -> Mapping[str, RuntimeObservation]: ...


class AccessSink(Protocol):
    async def begin(self, ctx: RequestContext, operation: d.Operation) -> AuditTicket: ...
    async def finish(self, ticket: AuditTicket, outcome: AuditOutcome) -> None: ...


class HttpFactory(Protocol):
    def create_http_app(self, config: HttpConfig, dependencies: GatewayDependencies) -> ASGIApp: ...


class MemoryReadPort(Protocol):
    async def execute(self, ctx: RequestContext, command: d.MemoryReadCommand) -> d.MemoryReadResult: ...


class RetrievalPort(Protocol):
    async def execute(self, ctx: RequestContext, command: d.RetrievalCommand) -> d.RetrievalResult: ...


class GraphReadPort(Protocol):
    async def execute(self, ctx: RequestContext, command: d.GraphReadCommand) -> d.GraphReadResult: ...


class DiagnosticsPort(Protocol):
    async def execute(self, ctx: RequestContext, command: d.DiagnosticsCommand) -> d.DiagnosticsResult: ...


class TracePort(Protocol):
    def observe(
        self, ctx: RequestContext, operation: Literal["search", "context"]
    ) -> AsyncContextManager[TraceObserver]: ...
    async def execute(self, ctx: RequestContext, command: d.TraceCommand) -> d.TraceResult: ...


class MutationExecutor(Protocol):
    async def run(
        self, ctx: RequestContext, command: d.ProfileMutationCommand, handler: MutationHandler
    ) -> d.MutationResult: ...


class IngestionPort(Protocol):
    async def execute(self, ctx: RequestContext, command: d.IngestionCommand) -> d.WriteResult: ...


class LifecyclePort(Protocol):
    async def execute(self, ctx: RequestContext, command: d.LifecycleCommandV1) -> d.TransitionResult: ...


class BeliefsPort(Protocol):
    async def execute(self, ctx: RequestContext, command: d.BeliefsCommand) -> d.BeliefsResult: ...


class RelationsWritePort(Protocol):
    async def execute(self, ctx: RequestContext, command: d.RelationsWriteCommand) -> d.RelationResult: ...


class InspectionPort(Protocol):
    async def execute(self, ctx: RequestContext, command: d.InspectionCommand) -> d.InspectionResult: ...


class JobPort(Protocol):
    async def enqueue(
        self, ctx: RequestContext, operation: d.JobOperation, payload: d.MaintenanceRequest | d.RebuildRequest
    ) -> d.JobAccepted: ...
    async def get(self, ctx: RequestContext, job_id: str) -> d.Job: ...


class ReplayRunner(Protocol):
    async def run(self, session: ProfileSession, job: d.Job, request: d.MaintenanceRequest) -> d.JobResult: ...


class RebuildRunner(Protocol):
    async def run(self, session: ProfileSession, job: d.Job, request: d.RebuildRequest) -> d.JobResult: ...


class EventSource(Protocol):
    def subscribe(
        self, ctx: RequestContext, query: d.EventQuery, last_event_id: str | None
    ) -> AsyncIterator[d.Event]: ...
    async def publish(self, event: d.Event) -> None: ...


class SessionService(Protocol):
    async def exchange(self, bearer: str, request: d.SessionExchange, *, origin: str) -> d.SessionInfo: ...
    async def authenticate(self, cookie: str, *, origin: str, csrf_token: str | None) -> Principal: ...


class StudioClient(Protocol):
    async def request(
        self,
        operation: d.Operation,
        payload: d.JSONValue,
        *,
        profile_id: str | None = None,
        path_ids: Mapping[str, str] | None = None,
    ) -> d.GatewayResult: ...


class StudioMemory(Protocol):
    @staticmethod
    def mount_memory(api: StudioClient, root: Element, capabilities: frozenset[str]) -> Callable[[], None]: ...


class StudioGraph(Protocol):
    @staticmethod
    def mount_graph(api: StudioClient, root: Element, capabilities: frozenset[str]) -> Callable[[], None]: ...


class StudioRetrieval(Protocol):
    @staticmethod
    def mount_retrieval(api: StudioClient, root: Element, capabilities: frozenset[str]) -> Callable[[], None]: ...


class StudioOperations(Protocol):
    @staticmethod
    def mount_operations(api: StudioClient, root: Element, capabilities: frozenset[str]) -> Callable[[], None]: ...


class QualityPort(Protocol):
    async def execute(self, ctx: RequestContext, command: d.QualityCommand) -> d.QualityResult: ...


class BenchmarkPort(Protocol):
    async def execute(self, ctx: RequestContext, command: d.BenchmarkCommand) -> d.BenchmarkResult: ...


class BetaComposition(Protocol):
    def assemble(self, config: HttpConfig, dependencies: GatewayDependencies) -> ASGIApp: ...


class MCPHttpFactory(Protocol):
    def create_mcp_http_app(self, dependencies: GatewayDependencies) -> ASGIApp: ...


@dataclass(frozen=True)
class GatewayDependencies:
    registry: ProfileRegistry
    tokens: TokenService
    access: AccessSink
    metrics: MetricsSink
    ports: Mapping[d.Operation, GatewayPort]
    leases: ProfileLeases | None = None
    jobs: JobPort | None = None
    events: EventSource | None = None
    sessions: SessionService | None = None
    traces: TracePort | None = None
    readiness: Callable[[], Awaitable[bool]] | None = None

    def __post_init__(self) -> None:
        if not set(self.ports) <= d.COMMAND_TYPES.keys():
            raise ValueError("unknown operation")


@dataclass(frozen=True, kw_only=True)
class HttpConfig:
    control_path: Path
    host: str = "127.0.0.1"
    port: int = 8080
    workers: Literal[1] = 1
    reload: Literal[False] = False
    allowed_hosts: tuple[str, ...] = ("127.0.0.1", "localhost")
    trusted_proxy_cidrs: tuple[str, ...] = ()
    allowed_origins: tuple[str, ...] = ()
    max_body_bytes: int = 1048576
    max_header_bytes: int = 16384
    header_timeout_seconds: float = 5.0
    max_connections: int = 128
    max_learn_chars: int = 32768
    max_query_chars: int = 4096
    max_batch: int = 100
    rate_per_minute: int = 120
    rate_burst: int = 20
    retrieval_concurrency: int = 4
    maintenance_concurrency_per_profile: int = 1
    request_timeout_seconds: float = 30
    graph_max_depth: int = 4
    cursor_ttl_seconds: int = 900
    idempotency_ttl_seconds: int = 86400

    def __post_init__(self) -> None:
        validate_http_config(self)


def validate_http_config(config: HttpConfig) -> None:
    """Validate HTTP resource ceilings at construction and point of use.

    Raises ``ValueError`` with an ``E_VALIDATION:`` prefix for every invalid
    configuration, including configurations created through a subclass that
    bypasses ``HttpConfig.__post_init__``.
    """
    if type(config.max_header_bytes) is not int or not 1 <= config.max_header_bytes <= 1048576:
        raise ValueError("E_VALIDATION: invalid max_header_bytes")
    if type(config.max_connections) is not int or not 1 <= config.max_connections <= 65536:
        raise ValueError("E_VALIDATION: invalid max_connections")
    if (
        type(config.header_timeout_seconds) not in (int, float)
        or not isfinite(config.header_timeout_seconds)
        or not 0 < config.header_timeout_seconds <= 300
    ):
        raise ValueError("E_VALIDATION: invalid header_timeout_seconds")
    if (
        type(config.request_timeout_seconds) not in (int, float)
        or not isfinite(config.request_timeout_seconds)
        or config.request_timeout_seconds <= 0
    ):
        raise ValueError("E_VALIDATION: invalid request_timeout_seconds")
    if config.header_timeout_seconds > config.request_timeout_seconds:
        raise ValueError("E_VALIDATION: header_timeout_seconds exceeds request_timeout_seconds")
