"""One-loop daemon composition; HTTP remains an optional, lazy import.

Local registry JSON: {"profiles":[{"id":"alpha","data_root":"/absolute/data"}]}.
Optional profile fields: display_name, description, enabled. No remote root API.
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

import typer


def _http_factory():
    try:
        from memory_server.transports.http.app import create_http_app
    except ImportError:
        return None
    return create_http_app


def load_bindings(path, profiles_root=None):
    from memory_server.gateway_contracts import dto as d
    from memory_server.gateway_contracts.ports import ProfileBinding
    from memory_server.paths import StorageResolutionInputs, resolve_storage_layout
    from memory_server.runtime.config import RuntimeConfig
    from memory_server.runtime.runtime_manager import RuntimeDescriptor
    from memory_server.settings import Settings
    from memory_server.transports.http.routes.profiles import validate_profile_id

    if path.stat().st_size > 1024 * 1024:
        raise ValueError('E_VALIDATION')
    text = path.read_text()
    try:
        data = json.loads(text)  # JSON is also YAML 1.2 and needs no extra parser.
    except json.JSONDecodeError:
        from ruamel.yaml import YAML  # existing optional hermes extra, never unsafe load
        data = YAML(typ='safe').load(text)
    if not isinstance(data, dict) or set(data) != {'profiles'} or not isinstance(data['profiles'], list):
        raise ValueError('E_VALIDATION')
    if len(data['profiles']) > 100:
        raise ValueError('E_VALIDATION')
    bindings = []
    for row in data['profiles']:
        if not isinstance(row, dict) or set(row) - {'id', 'data_root', 'display_name', 'description', 'enabled'}:
            raise ValueError('E_VALIDATION')
        name = validate_profile_id(row['id'])
        root = Path(row['data_root']) if 'data_root' in row else profiles_root / name if profiles_root else None
        if root is None or not root.is_absolute():
            raise ValueError('E_VALIDATION')
        layout = resolve_storage_layout(StorageResolutionInputs(
            mode='standalone', data_root=root, sqlite_url='sqlite+aiosqlite:///memory.db',
            lancedb_path='lancedb', graph_snapshot_path='graph.json', legacy_split_compat=False))
        if profiles_root is not None:
            if not profiles_root.is_absolute() or not layout.data_root.is_relative_to(profiles_root.resolve()):
                raise ValueError('E_VALIDATION')
        config = RuntimeConfig.capture(layout, Settings())
        profile = d.Profile(id=name, display_name=row.get('display_name', name), description=row.get('description'),
                            enabled=row.get('enabled', True), generation=1, runtime_state='not_started',
                            capabilities=(), storage_root=None)
        bindings.append(ProfileBinding(profile, layout, config,
                                       RuntimeDescriptor.from_layout(name, layout=layout, config=config)))
    return tuple(bindings)


def server_config(app, config):
    import uvicorn

    from memory_server.transports.http.limits import protocol_class
    return uvicorn.Config(app, host=config.host, port=config.port, workers=config.workers, reload=config.reload,
                          http=protocol_class(config), ws='none', proxy_headers=False, access_log=False,
                          h11_max_incomplete_event_size=config.max_header_bytes,
                          limit_concurrency=config.max_connections, log_config=None)


async def run_daemon(config, registry_path, profiles_root=None, *, factory=None, server_factory=None):
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)
    # Resolve and reject absence before local stores or sockets are touched.
    if factory is None:
        raise ValueError('E_CAPABILITY_UNAVAILABLE')
    from memory_server.transports.http.limits import validate_config
    validate_config(config)
    if not config.control_path.is_absolute() or config.control_path.name != 'control.sqlite':
        raise ValueError('E_VALIDATION')
    bindings = load_bindings(registry_path, profiles_root)
    from memory_server.application.profile_bundle import BundlePolicies
    from memory_server.control.access_audit import AccessAuditStore
    from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
    from memory_server.control.store import ControlStore
    from memory_server.control.tokens import TokenService
    from memory_server.gateway_contracts.ports import GatewayDependencies, Principal
    from memory_server.runtime.daemon import Daemon
    from memory_server.settings import Settings
    from memory_server.transports.http.app import with_profile_ports
    from memory_server.transports.http.routes.profiles import bind_control_pending
    from memory_server.transports.http.telemetry import BoundedMetricsSink

    with ControlStore(config.control_path.parent) as store:
        access = AccessAuditStore(config.control_path.parent / 'http-access')
        daemon_owner = None
        app = None
        handlers = {}
        loop = asyncio.get_running_loop()
        try:
            tokens = TokenService(store)
            registry = ProfileRegistry(store, RegistryConfiguration(bindings, access, tokens))
            daemon_owner = Daemon(registry, policies=BundlePolicies.capture(Settings()))
            observer = Principal('local-readiness', 'local-readiness', None,
                                 frozenset(b.profile.id for b in bindings), frozenset({'profile.read'}),
                                 1, datetime.now(UTC) + timedelta(days=36500))

            async def readiness():
                if not registry.available() or not tokens.available():
                    return False
                health = access.saturation()
                if any(health[key] for key in ('authenticated', 'denials', 'stale')):
                    return False
                for name, observation in daemon_owner.snapshot().items():
                    record = await registry.record(observer, name, permission='profile.read')
                    if record.enabled and (observation.state != 'ready' or not observation.owned
                                           or observation.stopping or observation.retained):
                        return False
                return True

            deps = GatewayDependencies(registry, tokens, access, BoundedMetricsSink(observer.profiles), {},
                                       leases=daemon_owner, readiness=readiness)
            from memory_server.runtime.read_composition import compose_read_ports

            deps, specs = compose_read_ports(deps, config)
            app = with_profile_ports(factory(config, deps, read_routes=specs),
                                     pending_control=bind_control_pending(registry, store))
            managed_signals = server_factory is None
            if managed_signals:
                import uvicorn
                class ManagedServer(uvicorn.Server):
                    @contextmanager
                    def capture_signals(self):
                        # Uvicorn otherwise re-raises SIGTERM before the owning
                        # composition can execute Daemon.close_all in finally.
                        yield
                server_factory = ManagedServer
            server = server_factory(server_config(app, config))
            if managed_signals:
                for sig in (signal.SIGINT, signal.SIGTERM):
                    handlers[sig] = signal.getsignal(sig)
                    loop.add_signal_handler(sig, server.handle_exit, sig, None)
            await server.serve()
            return 0 if server.started else 2
        finally:
            if app is not None:
                await app.close()
            try:
                if daemon_owner is not None:
                    report = await daemon_owner.close_all()
                    if not report.complete or report.error_code is not None:
                        raise ValueError('E_RUNTIME_FAILED')
            finally:
                access.close()
                for sig, handler in handlers.items():
                    loop.remove_signal_handler(sig)
                    signal.signal(sig, handler)


def daemon(
    http: Annotated[str, typer.Option('--http')] = '127.0.0.1:8080',
    registry: Annotated[Path, typer.Option('--registry')] = ...,
    control_path: Annotated[Path, typer.Option('--control-path')] = ...,
    profiles_root: Annotated[Path | None, typer.Option('--profiles-root')] = None,
    enable_mcp_http: Annotated[bool, typer.Option('--enable-mcp-http')] = False,
) -> None:
    """Serve the six-route, loopback-only HTTP shell from a local JSON registry."""
    factory = None if enable_mcp_http else _http_factory()
    if factory is None or not registry.is_file():
        typer.echo('E_CAPABILITY_UNAVAILABLE', err=True)
        raise typer.Exit(code=2)
    try:
        from memory_server.gateway_contracts.ports import HttpConfig
        host, separator, port = http.rpartition(':')
        if not separator:
            raise ValueError('E_VALIDATION')
        config = HttpConfig(control_path=control_path, host=host.strip('[]'), port=int(port))
        result = asyncio.run(run_daemon(config, registry, profiles_root, factory=factory))
    except Exception:
        typer.echo('E_CAPABILITY_UNAVAILABLE', err=True)
        raise typer.Exit(code=2) from None
    raise typer.Exit(code=result)
