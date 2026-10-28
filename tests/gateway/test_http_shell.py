"""S06 production ASGI -> real registry/token/audit/daemon boundaries."""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from memory_server.application.profile_bundle import BundlePolicies
from memory_server.control.access_audit import AccessAuditStore
from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
from memory_server.control.store import ControlStore
from memory_server.control.tokens import TokenService
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.catalog import CATALOG
from memory_server.gateway_contracts.ports import GatewayDependencies, HttpConfig
from memory_server.runtime.daemon import Daemon
from memory_server.settings import Settings
from memory_server.transports.http.app import create_http_app, with_profile_ports
from memory_server.transports.http.errors import _DEFAULT_STATUS, error_response
from memory_server.transports.http.routes.profiles import bind_control_pending
from memory_server.transports.http.telemetry import BoundedMetricsSink
from tests.gateway.test_registry import binding

CANARY = 'S06_SECRET_CONTENT_PATH_CANARY'


@pytest.fixture
async def shell(tmp_path):
    with ControlStore(tmp_path / 'control') as store:
        access = AccessAuditStore(tmp_path / 'audit')
        tokens = TokenService(store)
        bindings = tuple(binding(tmp_path / CANARY / p, p) for p in ('alpha', 'beta', 'gamma'))
        registry = ProfileRegistry(store, RegistryConfiguration(bindings, access, tokens))
        leases = Daemon(registry, policies=BundlePolicies.capture(Settings()))
        metrics = BoundedMetricsSink(['alpha', 'beta', 'gamma'])
        deps = GatewayDependencies(registry, tokens, access, metrics, {}, leases=leases)
        config = HttpConfig(control_path=tmp_path / 'control')
        app = with_profile_ports(create_http_app(config, deps), pending_control=bind_control_pending(registry, store))
        now = datetime.now(UTC)

        def mint(subject='actor', profiles=('alpha', 'beta', 'gamma'), permissions=('profile.read', 'profile.manage'),
                 expired=False):
            issued = now - timedelta(hours=2) if expired else now
            created = tokens._bootstrap_create(d.TokenCreate(
                subject=subject, profiles=list(profiles), permissions=list(permissions),
                expires_at=issued + timedelta(hours=1)), now=issued)
            return f'cmms_{created.token.id}.{created.secret}', created.token.id

        bearer, token_id = mint()
        state = SimpleNamespace(app=app, config=config, deps=deps, tokens=tokens, store=store,
                                registry=registry, leases=leases, access=access, metrics=metrics,
                                bearer=bearer, token_id=token_id, mint=mint, bindings=bindings)
        try:
            yield state
        finally:
            await app.close()
            assert (await leases.close_all()).complete
            access.close()


async def request(app, path='/api/v1/profiles/alpha', *, method='GET', bearer=None, body=b'', headers=(),
                  query=b'', receive=None, raw_path=None):
    sent = []
    values = [(b'host', b'localhost')]
    values += [(k.lower().encode(), v.encode()) for k, v in headers if k.lower() != 'host']
    host_override = [v for k, v in headers if k.lower() == 'host']
    if host_override:
        values = [(k, v) for k, v in values if k != b'host']
        values += [(b'host', v.encode()) for v in host_override if v]
    if bearer is not None:
        values.append((b'authorization', ('Bearer ' + bearer).encode()))
    if isinstance(body, dict):
        body = json.dumps(body).encode()
        values.append((b'content-type', b'application/json'))
    scope = dict(type='http', asgi={'version': '3.0'}, http_version='1.1', scheme='http',
                 method=method, path=path, raw_path=raw_path or path.encode(), query_string=query,
                 root_path='', headers=values, client=('127.0.0.1', 12345), server=('127.0.0.1', 8080))

    async def once():
        return {'type': 'http.request', 'body': body, 'more_body': False}

    async def send(message):
        sent.append(message)

    await app(scope, receive or once, send)
    start = next(item for item in sent if item['type'] == 'http.response.start')
    payload = b''.join(item.get('body', b'') for item in sent if item['type'] == 'http.response.body')
    return SimpleNamespace(status=start['status'], body=payload,
                           json=json.loads(payload) if payload else None,
                           headers={k.decode().lower(): v.decode() for k, v in start['headers']})


def code(response, expected, status=None):
    assert response.json['error']['code'] == expected, response.body
    assert set(response.json) == {'error'}
    assert set(response.json['error']) == {'code', 'message', 'request_id', 'details'}
    assert response.headers['x-request-id'] == response.json['error']['request_id']
    if status is not None:
        assert response.status == status


def rows(shell):
    return [dict(row) for row in shell.access._db.execute('SELECT * FROM access_audit')]


async def test_f4_received_config_validator_once_per_build(shell, monkeypatch):
    from memory_server.gateway_contracts import ports
    original, calls = ports.validate_http_config, []

    def track(value):
        calls.append(value)
        return original(value)

    monkeypatch.setattr(ports, 'validate_http_config', track)
    create_http_app(shell.config, shell.deps)
    create_http_app(shell.config, shell.deps)
    assert calls == [shell.config, shell.config]


async def test_t1_closed_catalog_and_unknown_paths(shell, monkeypatch):
    calls = []

    async def forbidden(*args, **kwargs):
        calls.append(args)
        pytest.fail('unexpected registry access')

    monkeypatch.setattr(shell.registry, 'resolve', forbidden)
    assert len(shell.app.routes) == 6
    assert all(route.permission in {'PUBLIC', 'profile.read', 'profile.manage'} for route in shell.app.routes)
    assert all((route.permission == 'PUBLIC') == (route.path in {'/healthz', '/readyz'})
               for route in shell.app.routes)
    for path in ('/metrics', '/api/v1/tokens', '/api/v1/profiles/alpha/memories', '/docs', '/api/v1/profiles/'):
        code(await request(shell.app, path, bearer=shell.bearer), 'E_NOT_FOUND', 404)
    assert calls == [] and rows(shell) == []


async def test_t3_health_exact_no_open_no_audit_and_independent_budget(shell, monkeypatch):
    async def forbidden(*args, **kwargs):
        pytest.fail('public probe acquired/opened/audited')

    for method in ('acquire',):
        monkeypatch.setattr(shell.leases, method, forbidden)
    monkeypatch.setattr(shell.access, 'begin', forbidden)
    shell.app.guard.active = 16
    try:
        for path, status, body in [('/healthz', 200, b'{"status":"ok"}'),
                                   ('/readyz', 503, b'{"status":"unavailable"}')]:
            response = await request(shell.app, path)
            assert response.status == status and response.body == body
        async def ready():
            return True
        shell.app.dependencies = replace(shell.app.dependencies, readiness=ready)
        response = await request(shell.app, '/readyz')
        assert response.status == 200 and response.body == b'{"status":"ok"}'
    finally:
        shell.app.guard.active = 0
    assert rows(shell) == [] and shell.leases.snapshot() == {}
    assert all(not b.layout.data_root.exists() for b in shell.bindings)


@pytest.mark.parametrize('kind', ['missing', 'malformed', 'expired', 'revoked', 'scope'])
@pytest.mark.parametrize('method,path', [('GET', '/healthz'), ('GET', '/readyz'), ('GET', '/api/v1/profiles'),
                                      ('GET', '/api/v1/profiles/alpha'),
                                      ('POST', '/api/v1/profiles/alpha/enable'),
                                      ('POST', '/api/v1/profiles/alpha/disable')])
async def test_t4_auth_matrix(shell, kind, method, path):
    bearer = None
    if kind == 'malformed':
        bearer = CANARY
    elif kind == 'expired':
        bearer, _ = shell.mint(expired=True)
    elif kind == 'revoked':
        bearer, ident = shell.mint()
        shell.tokens._unchecked_revoke(shell.tokens._local_context(datetime.now(UTC)), ident,
                                      d.ControlMutation(reason='test', confirmation=True, expected_generation=1),
                                      datetime.now(UTC), local=True)
    elif kind == 'scope':
        bearer, _ = shell.mint(profiles=('beta',), permissions=('memory.read',))
    response = await request(shell.app, path, method=method, bearer=bearer)
    if path in ('/healthz', '/readyz'):
        assert response.status == (200 if path == '/healthz' else 503)
    else:
        code(response, 'E_PERMISSION_DENIED' if kind == 'scope' else 'E_AUTH_REQUIRED',
             403 if kind == 'scope' else 401)
    assert rows(shell) == []


async def test_t5_control_disabled_enable_validation_and_cas(shell):
    payload = dict(reason='test', confirmation=True, expected_generation=1)
    response = await request(shell.app, '/api/v1/profiles/alpha/disable', method='POST', bearer=shell.bearer,
                             body=payload, headers=[('Idempotency-Key', str(uuid4()))])
    assert response.status == 200, response.body
    generation = response.json['generation']
    code(await request(shell.app, bearer=shell.bearer), 'E_PROFILE_DISABLED', 409)
    code(await request(shell.app, '/api/v1/profiles/alpha/enable', method='POST', bearer=shell.bearer,
                       body=payload, headers=[('Idempotency-Key', str(uuid4()))]), 'E_VERSION_CONFLICT', 409)
    for bad in ({**payload, 'confirmation': False}, {k: v for k, v in payload.items() if k != 'expected_generation'}):
        before = len(rows(shell))
        rejected = await request(shell.app, '/api/v1/profiles/alpha/enable', method='POST', bearer=shell.bearer,
                                 body=bad, headers=[('Idempotency-Key', str(uuid4()))])
        code(rejected, 'E_PRECONDITION_REQUIRED' if 'expected_generation' not in bad else 'E_VALIDATION',
             428 if 'expected_generation' not in bad else 422)
        assert len(rows(shell)) == before
    response = await request(shell.app, '/api/v1/profiles/alpha/enable', method='POST', bearer=shell.bearer,
                             body={**payload, 'expected_generation': generation},
                             headers=[('Idempotency-Key', str(uuid4()))])
    assert response.status == 200 and response.json['enabled']
    assert response.json['storage_root'] is None


async def test_t9_audit_intent_before_real_registry_and_finish(shell, monkeypatch):
    original = shell.registry.resolve

    async def inspect(*args, **kwargs):
        assert rows(shell)[-1]['state'] == 'intent'
        return await original(*args, **kwargs)

    monkeypatch.setattr(shell.registry, 'resolve', inspect)
    response = await request(shell.app, bearer=shell.bearer)
    assert response.status == 200, response.body
    assert len(rows(shell)) == 1 and rows(shell)[0]['state'] == 'terminal'
    assert rows(shell)[0]['result'] == 'success'


async def test_t8_t9_backend_failure_sanitized_terminal_and_begin_failure(shell, monkeypatch, caplog):
    async def broken(*args, **kwargs):
        raise RuntimeError(CANARY)
    monkeypatch.setattr(shell.registry, 'resolve', broken)
    failure = await request(shell.app, bearer=shell.bearer)
    code(failure, 'E_INTERNAL', 500)
    assert CANARY.encode() not in failure.body
    assert rows(shell)[0]['state'] == 'terminal' and rows(shell)[0]['result'] == 'E_INTERNAL'
    assert CANARY not in caplog.text + repr(rows(shell))
    from prometheus_client import generate_latest
    assert CANARY.encode() not in generate_latest(shell.metrics._registry)
    before = len(rows(shell))
    monkeypatch.setattr(shell.access, 'begin', broken)
    code(await request(shell.app, bearer=shell.bearer), 'E_AUDIT_UNAVAILABLE', 503)
    assert len(rows(shell)) == before


async def test_t10_t15_t16_identity_free_denial_and_server_request_id(shell, caplog):
    response = await request(shell.app, bearer=CANARY, headers=[('X-Request-ID', CANARY),
                             ('X-Forwarded-For', CANARY), ('X-Forwarded-Host', CANARY)])
    code(response, 'E_AUTH_REQUIRED', 401)
    assert response.headers['x-request-id'].startswith('req_')
    denied = [dict(row) for row in shell.access._denials._db.execute('SELECT * FROM access_audit')]
    assert len(denied) == 1 and rows(shell) == []
    assert all(denied[0][key] is None for key in ('token_id', 'client_id', 'profile_id', 'remote_address'))
    assert CANARY not in repr(denied) + caplog.text + response.body.decode()
    success = await request(shell.app, bearer=shell.bearer, headers=[('X-Request-ID', CANARY)])
    assert success.status == 200
    assert rows(shell)[0]['request_id'] == success.headers['x-request-id'] != CANARY


async def test_t12_ten_replays_single_effect_and_changed_digest(shell):
    key = str(uuid4())
    payload = dict(reason='test', confirmation=True, expected_generation=1)
    responses = [await request(shell.app, '/api/v1/profiles/alpha/disable', method='POST', bearer=shell.bearer,
                               body=payload, headers=[('Idempotency-Key', key)]) for _ in range(10)]
    assert [r.status for r in responses] == [200] * 10
    assert all(r.json == responses[0].json for r in responses)
    assert all(r.headers['idempotency-replayed'] == 'true' for r in responses[1:])
    assert all(r.headers['x-original-request-id'] == responses[0].headers['x-request-id'] for r in responses[1:])
    journal = shell.store.journal(profile_ids=['alpha'])
    assert len(journal) == 1
    code(await request(shell.app, '/api/v1/profiles/alpha/disable', method='POST', bearer=shell.bearer,
                       body={**payload, 'reason': 'different'}, headers=[('Idempotency-Key', key)]),
         'E_IDEMPOTENCY_CONFLICT', 409)


async def test_t18_warm_cache_revocation_checks_real_token_service(shell, monkeypatch):
    calls = []
    original = shell.tokens.verify_principal
    def verify(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(shell.tokens, 'verify_principal', verify)
    assert (await request(shell.app, bearer=shell.bearer)).status == 200
    shell.tokens._unchecked_revoke(shell.tokens._local_context(datetime.now(UTC)), shell.token_id,
                                  d.ControlMutation(reason='test', confirmation=True, expected_generation=1),
                                  datetime.now(UTC), local=True)
    count = len(rows(shell))
    code(await request(shell.app, bearer=shell.bearer), 'E_AUTH_REQUIRED', 401)
    assert calls == [1] and len(rows(shell)) == count


async def test_t11_app_shutdown_drains_and_rejects_new(shell, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    original = shell.registry.resolve
    async def blocked(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)
    monkeypatch.setattr(shell.registry, 'resolve', blocked)
    inflight = asyncio.create_task(request(shell.app, bearer=shell.bearer))
    await entered.wait()
    close = asyncio.create_task(shell.app.close())
    await asyncio.sleep(0)
    code(await request(shell.app, bearer=shell.bearer), 'E_RUNTIME_CLOSING', 503)
    assert not close.done()
    release.set()
    assert (await inflight).status == 200
    await close


async def test_t12_true_concurrent_cas_real_registry(shell, monkeypatch):
    from memory_server.transports.http.app import with_profile_ports
    app = with_profile_ports(create_http_app(replace(shell.config, maintenance_concurrency_per_profile=2), shell.deps),
                             pending_control=bind_control_pending(shell.registry, shell.store))
    original = shell.registry.control
    arrived, both = 0, asyncio.Event()

    async def barrier(*args, **kwargs):
        nonlocal arrived
        arrived += 1
        if arrived == 2:
            both.set()
        await both.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(shell.registry, 'control', barrier)
    async def call():
        return await request(app, '/api/v1/profiles/alpha/disable', method='POST', bearer=shell.bearer,
                             body=dict(reason='race', confirmation=True, expected_generation=1),
                             headers=[('Idempotency-Key', str(uuid4()))])
    results = await asyncio.wait_for(asyncio.gather(call(), call()), 5)
    assert sorted(r.status for r in results) == [200, 409]
    code(next(r for r in results if r.status == 409), 'E_VERSION_CONFLICT', 409)
    assert len(shell.store.journal(profile_ids=['alpha'])) == 1
    await app.close()


async def test_t9_pending_publication_replay_and_recovery(shell, monkeypatch):
    original = shell.app.profile_port.publish
    async def unavailable(*args, **kwargs):
        return False
    shell.app.profile_port.publish = unavailable
    payload = dict(reason='pending', confirmation=True, expected_generation=1)
    key = str(uuid4())
    async def call(key, profile='alpha'):
        return await request(shell.app, f'/api/v1/profiles/{profile}/disable', method='POST', bearer=shell.bearer,
                             body=payload, headers=[('Idempotency-Key', key)])
    first = await call(key)
    assert first.status == 200
    assert rows(shell)[0]['state'] == 'intent'
    replay = await call(key)
    assert replay.status == 200 and replay.json == first.json
    code(await call(str(uuid4())), 'E_PROFILE_BUSY', 409)
    shell.app.profile_port.publish = original
    assert await shell.registry.reconcile_control(leases=shell.leases, publish=original, limit=1000)
    assert rows(shell)[0]['state'] == 'terminal'
    assert len(shell.store.journal(profile_ids=['alpha'])) == 1


async def test_t5_unsafe_drain_cannot_succeed(shell, monkeypatch):
    from memory_server.gateway_contracts.ports import RuntimeObservation
    async def no_report(*args, **kwargs):
        return None
    monkeypatch.setattr(shell.leases, 'snapshot', lambda: {
        'alpha': RuntimeObservation('closing', True, 1, True, True)})
    monkeypatch.setattr(shell.leases, 'stop_profile', no_report)
    response = await request(shell.app, '/api/v1/profiles/alpha/disable', method='POST', bearer=shell.bearer,
                             body=dict(reason='unsafe', confirmation=True, expected_generation=1),
                             headers=[('Idempotency-Key', str(uuid4()))])
    code(response, 'E_PROFILE_BUSY', 409)
    assert not (await shell.registry.record(await shell.tokens.authenticate(shell.bearer, now=datetime.now(UTC)),
                                          'alpha', permission='profile.manage')).enabled
    monkeypatch.undo()


async def test_t11_factory_absence_before_store_and_server_values(shell, tmp_path):
    from memory_server.runtime.daemon_cli import run_daemon, server_config
    with pytest.raises(ValueError, match='E_CAPABILITY_UNAVAILABLE'):
        await run_daemon(shell.config, tmp_path / 'missing.json')
    config = server_config(shell.app, shell.config)
    assert (config.host, config.workers, config.reload) == ('127.0.0.1', 1, False)
    assert config.proxy_headers is False and config.ws == 'none' and config.access_log is False
    assert config.h11_max_incomplete_event_size == 16384 and config.limit_concurrency == 128


async def test_profile_lookup_oracle_and_missing_ports(shell):
    for target, profiles, expected in [('beta', ('alpha',), 'E_PERMISSION_DENIED'),
                                       ('absent', ('alpha',), 'E_PERMISSION_DENIED'),
                                       ('absent', ('absent',), 'E_PROFILE_NOT_FOUND')]:
        bearer, _ = shell.mint(profiles=profiles)
        code(await request(shell.app, '/api/v1/profiles/' + target, bearer=bearer), expected)
    bearer, _ = shell.mint(profiles=('absent',))
    code(await request(shell.app, '/api/v1/profiles/absent/disable', method='POST', bearer=bearer,
                       body=dict(reason='unknown', confirmation=True, expected_generation=1),
                       headers=[('Idempotency-Key', str(uuid4()))]), 'E_PROFILE_NOT_FOUND', 404)
    missing = create_http_app(shell.config, shell.deps)
    code(await request(missing, bearer=shell.bearer), 'E_CAPABILITY_UNAVAILABLE', 503)
    await missing.close()


async def test_native_import_does_not_load_http_shell():
    import os
    import subprocess
    import sys
    result = subprocess.run([sys.executable, '-c',
        'import sys; import memory_server.cli; '
        'assert "memory_server.transports.http.app" not in sys.modules; print("NATIVE_IMPORT_ISOLATED")'],
        env=dict(os.environ), capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'NATIVE_IMPORT_ISOLATED'


async def test_t11_real_cli_subprocess_bind_and_sigterm(shell, tmp_path):
    import os
    import socket
    import subprocess
    import sys
    registry_path = tmp_path / 'registry.json'
    registry_path.write_text(json.dumps({'profiles': [
        {'id': item.profile.id, 'data_root': str(item.layout.data_root)} for item in shell.bindings]}))
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    args = [sys.executable, '-c', 'from memory_server.cli import app; app()', 'daemon',
            '--http', f'127.0.0.1:{port}', '--registry', str(registry_path),
            '--control-path', str(shell.config.control_path / 'control.sqlite')]
    proc = subprocess.Popen(args, env=dict(os.environ), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        async with asyncio.timeout(15):
            while True:
                assert proc.poll() is None, proc.communicate()
                try:
                    reader, writer = await asyncio.open_connection('127.0.0.1', port)
                    break
                except OSError:
                    await asyncio.sleep(.05)
        writer.write(('GET /api/v1/profiles/alpha HTTP/1.1\r\nHost: localhost\r\n'
                      'Authorization: Bearer ' + shell.bearer + '\r\n\r\n').encode())
        await writer.drain()
        response = await asyncio.wait_for(reader.read(), 5)
        writer.close()
        await writer.wait_closed()
        assert b'200 OK' in response and b'"id":"alpha"' in response
        proc.terminate()
        stdout, stderr = await asyncio.to_thread(proc.communicate, timeout=15)
        assert proc.returncode == 0, stderr
        assert shell.bearer.encode() not in stdout + stderr
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate(timeout=5)


async def test_t11_yaml_local_registry_prefix_and_failed_shutdown(tmp_path, monkeypatch):
    from memory_server.runtime.daemon_cli import load_bindings, run_daemon
    path = tmp_path / 'registry.yaml'
    allowed = tmp_path / 'allowed'
    path.write_text('profiles:\n  - id: alpha\n    data_root: ' + str(allowed / 'alpha') + '\n')
    bindings = load_bindings(path, allowed)
    assert bindings[0].profile.id == 'alpha'
    with pytest.raises(ValueError, match='E_VALIDATION'):
        load_bindings(path, tmp_path / 'different')
    original = Daemon.close_all
    async def incomplete(self):
        await original(self)
        return SimpleNamespace(complete=False, error_code='E_RUNTIME_FAILED')
    monkeypatch.setattr(Daemon, 'close_all', incomplete)
    class Server:
        started = True
        def __init__(self, config):
            pass
        async def serve(self):
            pass
    with pytest.raises(ValueError, match='E_RUNTIME_FAILED'):
        await run_daemon(HttpConfig(control_path=tmp_path / 'control' / 'control.sqlite'), path,
                         factory=create_http_app, server_factory=Server)


async def test_t9_committed_busy_keeps_original_audit_pending_with_real_holder(shell):
    from memory_server.gateway_contracts.ports import RequestContext
    from memory_server.runtime.runtime_manager import RuntimeManager
    from tests.gateway.test_daemon import factory
    shell.leases.manager = RuntimeManager(factory_builder=factory)
    principal = await shell.tokens.authenticate(shell.bearer, now=datetime.now(UTC))
    ready, release = asyncio.Event(), asyncio.Event()
    runtimes = []
    async def holder():
        ctx = RequestContext('req_' + str(uuid4()), principal, 'alpha', None,
                             asyncio.get_running_loop().time() + 30)
        async with shell.leases.acquire(ctx) as session:
            runtimes.append(session.runtime)
            ready.set()
            await release.wait()
    held = asyncio.create_task(holder())
    try:
        await asyncio.wait_for(ready.wait(), 5)
        response = await request(shell.app, '/api/v1/profiles/alpha/disable', method='POST', bearer=shell.bearer,
                                 body=dict(reason='active-owner', confirmation=True, expected_generation=1),
                                 headers=[('Idempotency-Key', str(uuid4()))])
        code(response, 'E_PROFILE_BUSY', 409)
        pending = shell.store.control_requests(limit=1000)
        assert len(pending) == 1 and pending[0]['phase'] == 'committed'
        assert pending[0]['pending_publication'] == 1
        assert runtimes[0].lock is not None and shell.leases.snapshot()['alpha'].stopping
        original = next(row for row in rows(shell) if row['id'] == pending[0]['ticket_id'])
        assert original['state'] == 'intent', (
            'Committed control owns a durable pending outcome; HTTP must not terminalize '
            'its original ticket before recovery: ' + repr(original['result']))
    finally:
        release.set()
        await asyncio.wait_for(held, 10)
    assert await shell.registry.reconcile_control(
        leases=shell.leases, publish=shell.access.publish_committed, limit=1000)
    original = next(row for row in rows(shell) if row['id'] == pending[0]['ticket_id'])
    assert original['state'] == 'terminal' and original['result'] == 'success'


async def test_t11_cli_composition_real_server_and_drain(tmp_path):
    import socket

    from memory_server.runtime.daemon_cli import run_daemon
    path = tmp_path / 'registry.json'
    path.write_text(json.dumps({'profiles': [{'id': 'alpha', 'data_root': str(tmp_path / 'alpha')}]}))
    config = HttpConfig(control_path=tmp_path / 'control' / 'control.sqlite')
    observed = []

    class ExerciseServer:
        started = True
        def __init__(self, config):
            self.config = config
        async def serve(self):
            from tests.gateway.test_http_security import wire
            async with wire(self.config.app, config) as port:
                reader, writer = await asyncio.open_connection('127.0.0.1', port)
                writer.write(b'GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n')
                await writer.drain()
                observed.append(await reader.read())
                writer.close()
                await writer.wait_closed()
            assert self.config.app.dependencies.leases.snapshot() == {}

    assert await run_daemon(config, path, factory=create_http_app, server_factory=ExerciseServer) == 0
    assert b'200 OK' in observed[0] and b'{"status":"ok"}' in observed[0]
    assert not (tmp_path / 'alpha').exists()
    # No default socket/listener remained; wire used and closed an ephemeral bind.
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))


@pytest.mark.parametrize("optimized", [False, True])
@pytest.mark.parametrize("field,value", [
    ("permission", "profile.read"), ("method", "GET"),
    ("path", "/tampered"), ("operation", "unknown.operation"),
])
def test_catalog_mismatch_rejected_at_build(tmp_path, optimized, field, value):
    script = '''
import sys
from dataclasses import replace
from types import SimpleNamespace
from memory_server.gateway_contracts.ports import HttpConfig
from memory_server.transports.http import app
specs = list(app.S06_ROUTE_SPECS)
specs[4] = replace(specs[4], **{sys.argv[2]: sys.argv[3]})
app.S06_ROUTE_SPECS = tuple(specs)
access = SimpleNamespace(record_unauthenticated=lambda: None,
                         publish_committed=lambda: None, saturation=lambda: None)
deps = SimpleNamespace(access=access)
try:
    app.create_http_app(HttpConfig(control_path=sys.argv[1]), deps)
except ValueError:
    print("CATALOG_MISMATCH_REJECTED")
else:
    sys.exit("tampered spec accepted")
'''
    result = subprocess.run(
        [sys.executable, *(["-O"] if optimized else []), "-c", script, str(tmp_path), field, value],
        capture_output=True, text=True, env=os.environ.copy(), check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "CATALOG_MISMATCH_REJECTED"


@pytest.mark.parametrize("operation", ["enable", "disable"])
async def test_read_only_token_cannot_reach_control_port(shell, monkeypatch, operation):
    bearer, _ = shell.mint(permissions=("profile.read",))
    calls = []
    original = shell.app.profile_port.execute

    async def inspect(ctx, command):
        if command.operation in {"profiles.enable", "profiles.disable"}:
            calls.append(command.operation)
        return await original(ctx, command)

    monkeypatch.setattr(shell.app.profile_port, "execute", inspect)
    # Prove least privilege with the SAME principal, not the fixture's manager.
    for path in ("/api/v1/profiles", "/api/v1/profiles/alpha"):
        response = await request(shell.app, path, bearer=bearer)
        assert response.status == 200, response.body
    response = await request(
        shell.app, f"/api/v1/profiles/alpha/{operation}", method="POST", bearer=bearer,
        body=dict(reason="read-only", confirmation=True, expected_generation=1),
        headers=[("Idempotency-Key", str(uuid4()))],
    )
    code(response, "E_PERMISSION_DENIED", 403)
    assert calls == []
    assert not shell.store.journal(profile_ids=["alpha"])
    assert not shell.store.control_requests(limit=10)
    assert len(rows(shell)) == 2  # Only the successful reads were admitted.
    route = next(route for route in shell.app.routes if route.name == f"profiles.{operation}")
    assert route.spec is CATALOG.by_operation[f"profiles.{operation}"]


@pytest.mark.parametrize("path,boundary", [
    ("/api/v1/profiles", "page"), ("/api/v1/profiles/alpha", "resolve"),
])
async def test_read_audit_ticket_before_registry(shell, monkeypatch, path, boundary):
    calls = []
    original = getattr(shell.registry, boundary)

    async def inspect(*args, **kwargs):
        calls.append(boundary)
        assert rows(shell)[-1]["state"] == "intent"
        return await original(*args, **kwargs)

    monkeypatch.setattr(shell.registry, boundary, inspect)
    response = await request(shell.app, path, bearer=shell.bearer)
    assert response.status == 200, response.body
    assert calls and rows(shell)[-1]["state"] == "terminal"
    calls.clear()

    async def missing_ticket(*args, **kwargs):
        return None

    monkeypatch.setattr(shell.access, "begin", missing_ticket)
    code(await request(shell.app, path, bearer=shell.bearer), "E_AUDIT_UNAVAILABLE", 503)
    assert calls == []
    assert len(rows(shell)) == 1


async def test_conflict_permission_validation_messages_are_consistent(shell):
    bearer, _ = shell.mint(permissions=("profile.read",))
    payload = dict(reason="test", confirmation=True, expected_generation=2)
    cases = [
        (shell.bearer, payload, "E_VERSION_CONFLICT", 409, "Version conflict"),
        (bearer, payload, "E_PERMISSION_DENIED", 403, "Permission denied"),
        (shell.bearer, {**payload, "confirmation": False}, "E_VALIDATION", 422, "Invalid request"),
    ]
    for token, body, expected, status, message in cases:
        response = await request(
            shell.app, "/api/v1/profiles/alpha/disable", method="POST", bearer=token, body=body,
            headers=[("Idempotency-Key", str(uuid4()))],
        )
        code(response, expected, status)
        assert response.json["error"]["message"] == message
    for name, status in _DEFAULT_STATUS.items():
        error = json.loads(error_response(name, "req_test").body)["error"]
        assert error["message"]
        if status != 500:
            assert "internal server error" not in error["message"].lower()
    assert json.loads(error_response("unknown", "req_test").body)["error"]["message"] == "Internal server error"
