"""Six-route ASGI shell; all requests cross the same closed guard stack."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from time import monotonic
from urllib.parse import urlsplit
from uuid import uuid4

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from memory_server.gateway_contracts.catalog import CATALOG

from .errors import HttpError, error_code, error_response
from .guards import Guard, ReadRouteSpec, secure_headers
from .limits import read_body, validate_config
from .routes import S06_ROUTE_SPECS, ProfilePort
from .routes.profiles import validate_profile_id


class HttpApp:
    def __init__(self, config, dependencies, *, denial=None, publish=None, saturation=None,
                 read_routes: tuple[ReadRouteSpec, ...] = ()):
        validate_config(config)  # F4: exactly once on the received object.
        self.config = config
        self.guard = Guard(config, dependencies, denial=denial, publish=publish, saturation=saturation)
        self.dependencies = dependencies
        self.profile_port = ProfilePort(config, dependencies, self.guard.publish)
        self.guard.cursor_validator = self.profile_port
        self.tasks = set()
        self._idle = asyncio.Event()
        self._idle.set()
        self.read_routes = tuple(read_routes)
        self.routes = []
        # Check the projection against the frozen executable catalogue.
        for spec in S06_ROUTE_SPECS:
            row = CATALOG.by_operation.get(spec.operation)
            if (row is None or row.owner != 'S06'
                    or (spec.method, spec.path, spec.permission) != (row.method, row.route, row.permission)):
                raise ValueError('S06 route specification does not match catalogue')
            route = Route(row.route, self.endpoint, methods=[row.method], name=row.operation)
            route.permission = row.permission
            route.spec = row
            self.routes.append(route)
        for spec in self.read_routes:
            row = CATALOG.by_operation.get(spec.operation)
            if (not isinstance(spec, ReadRouteSpec) or row is None or row.owner != 'S07'
                    or any(route.name == spec.operation or route.path == spec.path for route in self.routes)
                    or (spec.method, spec.path, spec.permission) != (row.method, row.route, row.permission)):
                raise ValueError('S07 route specification does not match catalogue')
            route = Route(row.route, self.endpoint, methods=[row.method], name=row.operation)
            route.permission, route.spec = row.permission, spec
            self.routes.append(route)
        self.router = Starlette(routes=self.routes)

    async def endpoint(self, request):
        state = request.scope['s06']
        if isinstance(state['spec'], ReadRouteSpec):
            return await self.guard.execute_read(
                request, state['spec'], state['body'], state['request_id'], state['deadline'])
        return await self.guard.execute(request, state['spec'], state['body'], state['request_id'], state['deadline'])

    async def close(self):
        self.guard.closing = True
        await self._idle.wait()

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self._dispatch(scope, receive, send)
        task = asyncio.current_task()
        self.tasks.add(task)
        self._idle.clear()
        try:
            await self._dispatch(scope, receive, send)
        finally:
            self.tasks.discard(task)
            if not self.tasks:
                self._idle.set()

    async def _dispatch(self, scope, receive, send):
        if scope['type'] == 'lifespan':
            while True:
                message = await receive()
                if message['type'] == 'lifespan.startup':
                    try:
                        if self.dependencies.leases is not None:
                            await self.dependencies.registry.reconcile_control(
                                leases=self.dependencies.leases, publish=self.guard.publish, limit=1000)
                        await send({'type': 'lifespan.startup.complete'})
                    except Exception:
                        await send({'type': 'lifespan.startup.failed', 'message': 'E_CAPABILITY_UNAVAILABLE'})
                        return
                elif message['type'] == 'lifespan.shutdown':
                    await self.close()
                    await send({'type': 'lifespan.shutdown.complete'})
                    return
            return
        if scope['type'] != 'http':
            return
        request_id = 'req_' + str(uuid4())
        started = monotonic()
        deadline = started + self.config.request_timeout_seconds
        admitted = False
        profile_budget = None
        origin = None
        response = None
        try:
            request = Request(scope, receive)
            origin = request.headers.get('origin')
            hosts = request.headers.getlist('host')
            if len(hosts) != 1 or not hosts[0] or any(c in hosts[0] for c in '/\\@?# ,\t\r\n'):
                raise HttpError('E_VALIDATION')
            try:
                parsed = urlsplit('//' + hosts[0])
                host = parsed.hostname
                _ = parsed.port
            except ValueError:
                raise HttpError('E_VALIDATION') from None
            if host not in self.config.allowed_hosts:
                raise HttpError('E_VALIDATION')
            # Forwarded authority never substitutes for direct Host validation.
            # Only an explicitly allowlisted immediate peer may supply forwarding.
            from ipaddress import ip_address, ip_network
            peer = scope.get('client')
            trusted = False
            if peer and self.config.trusted_proxy_cidrs:
                try:
                    trusted = any(ip_address(peer[0]) in ip_network(cidr)
                                  for cidr in self.config.trusted_proxy_cidrs)
                except ValueError:
                    pass
            if trusted:
                forwarded = request.headers.getlist('x-forwarded-for')
                if len(forwarded) == 1:
                    try:
                        # Walk right-to-left across trusted hops, not attacker-
                        # controlled first entries in an arbitrary proxy chain.
                        hops = [str(ip_address(part.strip())) for part in forwarded[0].split(',')]
                        for candidate in reversed(hops):
                            scope['client'] = (candidate, 0)
                            if not any(ip_address(candidate) in ip_network(cidr)
                                       for cidr in self.config.trusted_proxy_cidrs):
                                break
                    except ValueError:
                        pass
                proto = request.headers.getlist('x-forwarded-proto')
                if len(proto) == 1 and proto[0] in {'http', 'https'}:
                    scope['scheme'] = proto[0]
            raw = scope.get('raw_path', scope['path'].encode()).split(b'?', 1)[0]
            if raw.startswith(b'/api/v1/profiles/') and (b'%' in raw or b'\x00' in raw):
                raise HttpError('E_VALIDATION')
            selected = None
            for route in self.routes:
                match = route.path_regex.fullmatch(scope['path'])
                if match:
                    selected = route
                    scope['path_params'] = match.groupdict()
                    break
            if selected is None:
                raise HttpError('E_NOT_FOUND')
            spec = selected.spec
            profile = scope.get('path_params', {}).get('profile')
            if profile is not None:
                validate_profile_id(profile)
            method = scope['method']
            if method == 'OPTIONS':
                response = Response(status_code=204)
                if origin in self.config.allowed_origins:
                    response.headers['Access-Control-Allow-Methods'] = spec.method + ', HEAD, OPTIONS'
                    response.headers['Access-Control-Allow-Headers'] = 'Authorization, Content-Type, Idempotency-Key'
            elif method != spec.method and not (method == 'HEAD' and spec.method == 'GET'):
                raise HttpError('E_METHOD_NOT_ALLOWED')
            elif spec.permission == 'PUBLIC':
                body = await read_body(scope, receive, self.config)
                if body or request.query_params:
                    raise HttpError('E_VALIDATION')
                available = spec.operation == 'health'
                if not available and self.dependencies.readiness is not None:
                    try:
                        async with asyncio.timeout_at(deadline):
                            available = bool(await self.dependencies.readiness())
                    except Exception:
                        available = False
                response = JSONResponse({'status': 'ok' if available else 'unavailable'},
                                        status_code=200 if available else 503)
            else:
                self.guard.admit()
                admitted = True
                if profile is not None:
                    profile_budget = (profile, spec.permission)
                    ceiling = (self.config.maintenance_concurrency_per_profile if spec.permission == 'profile.manage'
                               else self.config.retrieval_concurrency)
                    if self.guard.profiles.get(profile_budget, 0) >= ceiling:
                        profile_budget = None
                        raise HttpError('E_RATE_LIMIT')
                    self.guard.profiles[profile_budget] = self.guard.profiles.get(profile_budget, 0) + 1
                # The independent admission/connection ceilings bound the process;
                # unauthenticated traffic only spends its source's rate budget.
                source = scope.get('client')
                self.guard.pre_auth.take(source[0] if source else 'unknown-peer')
                async with asyncio.timeout_at(deadline):
                    body = await read_body(scope, receive, self.config)
                    scope['s06'] = dict(spec=spec, body=body, request_id=request_id, deadline=deadline)
                    response = await self.endpoint(Request(scope, receive))
        except TimeoutError:
            response = error_response('E_TIMEOUT', request_id)
        except Exception as exc:
            response = error_response(exc if isinstance(exc, HttpError) else error_code(exc), request_id)
        finally:
            if admitted:
                self.guard.active -= 1
            if profile_budget is not None:
                self.guard.profiles[profile_budget] -= 1
                if not self.guard.profiles[profile_budget]:
                    del self.guard.profiles[profile_budget]
        response = secure_headers(response, request_id, origin, self.config.allowed_origins)
        if scope['method'] == 'HEAD':
            response.body = b''
        await response(scope, receive, send)


def create_http_app(config, dependencies, *, denial=None, publish=None, saturation=None,
                    read_routes: tuple[ReadRouteSpec, ...] = ()):
    """Build without opening profiles; missing operation ports stay unavailable."""
    return HttpApp(config, dependencies, denial=denial, publish=publish, saturation=saturation, read_routes=read_routes)


def with_profile_ports(app, *, pending_control=None):
    """Explicit composition opt-in; control requires its bound custody callback."""
    app.profile_port.pending_control = pending_control
    ports = dict(app.dependencies.ports)
    for spec in S06_ROUTE_SPECS[2:]:
        ports[spec.operation] = app.profile_port
    app.dependencies = replace(app.dependencies, ports=ports)
    app.guard.dependencies = app.dependencies
    return app
