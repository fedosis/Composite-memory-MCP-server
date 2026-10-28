"""Falsifiable S06 edge, body, wire and privacy checks."""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from time import monotonic

import pytest

from memory_server.transports.http.app import create_http_app
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.limits import read_body
from tests.gateway.test_http_shell import CANARY, code, request, rows
from tests.gateway.test_http_shell import shell as shell_fixture


@pytest.fixture(name='shell')
async def inherited_shell(tmp_path):
    async for state in shell_fixture.__wrapped__(tmp_path):
        yield state


@pytest.mark.parametrize('headers,expected', [([('Content-Length', '1048577')], 'E_REQUEST_TOO_LARGE'),
                                             ([('Content-Length', '0')], 'E_VALIDATION'),
                                             ([('Content-Length', '1'), ('Transfer-Encoding', 'chunked')],
                                              'E_VALIDATION')])
async def test_t2_t13_framing_before_auth_audit_storage(shell, monkeypatch, headers, expected):
    calls = []
    async def forbidden(*args, **kwargs):
        calls.append(1)
        pytest.fail('auth before body guard')
    monkeypatch.setattr(shell.tokens, 'authenticate', forbidden)
    code(await request(shell.app, bearer=shell.bearer, headers=headers, body=b'xx'), expected)
    assert rows(shell) == [] and calls == []


async def test_t2_exhausted_concurrency_before_auth_and_audit(shell, monkeypatch):
    async def forbidden(*args, **kwargs):
        pytest.fail('auth before concurrency guard')
    monkeypatch.setattr(shell.tokens, 'authenticate', forbidden)
    shell.app.guard.active = 16
    try:
        code(await request(shell.app, bearer=shell.bearer), 'E_RATE_LIMIT', 429)
    finally:
        shell.app.guard.active = 0
    assert rows(shell) == []


@pytest.mark.parametrize('query', [b'limit=0', b'limit=201', b'limit=2.0', b'limit=-1', b'limit=1&limit=2',
                                  b'foo=1', b'cursor=x&cursor=y', b'limit=%C2%B2'])
async def test_t6_bad_query_no_storage(shell, monkeypatch, query):
    async def forbidden(*args, **kwargs):
        pytest.fail('invalid query reached storage')
    monkeypatch.setattr(shell.registry, 'page', forbidden)
    code(await request(shell.app, '/api/v1/profiles', bearer=shell.bearer, query=query), 'E_VALIDATION', 422)
    assert rows(shell) == []


@pytest.mark.parametrize('value', ['%2e%2e%2f', '%252e%252e%252f', '%00', 'x' * 65])
async def test_t6_bad_ids_no_storage(shell, monkeypatch, value):
    async def forbidden(*args, **kwargs):
        pytest.fail('invalid ID reached storage')
    monkeypatch.setattr(shell.registry, 'resolve', forbidden)
    code(await request(shell.app, '/api/v1/profiles/' + value, bearer=shell.bearer), 'E_VALIDATION', 422)
    assert rows(shell) == []


async def test_t6_cursor_two_real_principals_tamper_and_bounded_page(shell, monkeypatch):
    statements = []
    shell.store._connection.set_trace_callback(statements.append)
    response = await request(shell.app, '/api/v1/profiles', bearer=shell.bearer, query=b'limit=1')
    assert response.status == 200 and len(response.json['items']) == 1 and response.json['has_more']
    cursor = response.json['next_cursor']
    other, _ = shell.mint(subject='other')
    before = len(rows(shell))
    async def forbidden(*args, **kwargs):
        pytest.fail('cursor reached registry before verification')
    monkeypatch.setattr(shell.registry, 'page', forbidden)
    for value, bearer in [(cursor, other), ('x' + cursor, shell.bearer)]:
        code(await request(shell.app, '/api/v1/profiles', bearer=bearer,
                           query=('limit=1&cursor=' + value).encode()), 'E_CURSOR_INVALID', 400)
    assert len(rows(shell)) == before
    selects = [s for s in statements if 'FROM profiles WHERE' in s and 'ORDER BY id LIMIT' in s]
    assert selects and all('LIMIT 2' in s for s in selects)


@pytest.mark.parametrize('origin', [None, 'https://allowed.example', 'https://foreign.example'])
@pytest.mark.parametrize('credentials', [True, False])
async def test_t7_cors_options_head_and_methods(shell, origin, credentials):
    config = replace(shell.config, allowed_origins=('https://allowed.example',))
    app = create_http_app(config, shell.app.dependencies)
    headers = [('Origin', origin)] if origin else []
    bearer = shell.bearer if credentials else None
    for method in ('OPTIONS', 'HEAD', 'PUT', 'TRACE'):
        before = len(rows(shell))
        response = await request(app, '/healthz', method=method, headers=headers, bearer=bearer)
        if method == 'OPTIONS':
            assert response.status == 204
            assert response.headers.get('access-control-allow-methods') == (
                'GET, HEAD, OPTIONS' if origin == 'https://allowed.example' else None)
        elif method == 'HEAD':
            assert response.status == 200 and response.body == b''
        else:
            code(response, 'E_METHOD_NOT_ALLOWED', 405)
        assert response.headers.get('access-control-allow-origin') == (
            origin if origin == 'https://allowed.example' else None)
        assert 'access-control-allow-credentials' not in response.headers
        assert len(rows(shell)) == before
    await app.close()


@pytest.mark.parametrize('host', ['evil.com', 'localhost.evil.com', ''])
async def test_t21_direct_host_cannot_be_overridden(shell, monkeypatch, host):
    async def forbidden(*args, **kwargs):
        pytest.fail('host reject reached tokens')
    monkeypatch.setattr(shell.tokens, 'authenticate', forbidden)
    code(await request(shell.app, bearer=shell.bearer, headers=[('Host', host),
                       ('X-Forwarded-Host', 'localhost')]), 'E_VALIDATION', 422)
    assert rows(shell) == []


@pytest.mark.parametrize('field,value', [('host', '0.0.0.0'), ('host', '::'), ('workers', 2), ('reload', True),
                                       ('allowed_origins', ('*',))])
async def test_t11_bad_factory_config_refused(shell, field, value):
    config = replace(shell.config, **{field: value})
    with pytest.raises(ValueError, match='E_VALIDATION'):
        create_http_app(config, shell.deps)


async def test_t13_unbounded_stream_and_t14_absolute_body_deadline(shell, monkeypatch):
    reads = []
    async def endless():
        reads.append(1)
        return {'type': 'http.request', 'body': b'x' * 65536, 'more_body': True}
    code(await request(shell.app, receive=endless), 'E_REQUEST_TOO_LARGE', 413)
    assert len(reads) == 17 and rows(shell) == []
    app = create_http_app(replace(shell.config, request_timeout_seconds=.1, header_timeout_seconds=.05), shell.deps)
    async def slow():
        await asyncio.sleep(1)
        return {'type': 'http.request', 'body': b'x', 'more_body': True}
    started = monotonic()
    code(await request(app, receive=slow), 'E_TIMEOUT', 504)
    assert .08 <= monotonic() - started < .5
    assert rows(shell) == []


async def test_t20_pre_auth_and_per_token_budgets(shell, monkeypatch):
    app = create_http_app(replace(shell.config, rate_burst=2, rate_per_minute=1), shell.app.dependencies)
    calls = []
    original = shell.tokens.authenticate
    async def track(*args, **kwargs):
        calls.append(1)
        return await original(*args, **kwargs)
    monkeypatch.setattr(shell.tokens, 'authenticate', track)
    for _ in range(2):
        code(await request(app), 'E_AUTH_REQUIRED', 401)
    code(await request(app), 'E_RATE_LIMIT', 429)
    assert calls == []
    # A new public admission bucket does not reset per-token accounting.
    app.guard.pre_auth.buckets.clear()
    assert (await request(app, bearer=shell.bearer)).status == 200
    app.guard.pre_auth.buckets.clear()
    assert (await request(app, bearer=shell.bearer)).status == 200
    app.guard.pre_auth.buckets.clear()
    code(await request(app, bearer=shell.bearer), 'E_RATE_LIMIT', 429)
    other, _ = shell.mint(subject='independent')
    app.guard.pre_auth.buckets.clear()
    assert (await request(app, bearer=other)).status == 200
    await app.close()


async def test_t19_roots_null_in_injected_response(shell):
    class Injected:
        async def execute(self, ctx, command):
            return d.Profile(id='alpha', display_name='alpha', description=None, enabled=True, generation=1,
                             runtime_state='ready', capabilities=(), storage_root=CANARY)
    from memory_server.gateway_contracts import dto as d
    app = create_http_app(shell.config, replace(shell.deps, ports={'profiles.get': Injected()}))
    response = await request(app, bearer=shell.bearer)
    assert response.status == 200 and response.json['storage_root'] is None
    assert CANARY not in response.body.decode()
    await app.close()


async def test_t13_fragmented_headers_reach_pinned_parser(shell):
    from tests.gateway.test_http_security import wire
    async with wire(shell.app, shell.config) as port:
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        writer.write(b'GET /healthz HTTP/1.1\r\nHo')
        await writer.drain()
        await asyncio.sleep(.02)
        writer.write(b'st: localhost\r\n\r\n')
        await writer.drain()
        data = await asyncio.wait_for(reader.read(), 2)
        writer.close()
        await writer.wait_closed()
        assert b'200 OK' in data and b'{"status":"ok"}' in data


@asynccontextmanager
async def wire(app, config):
    import socket

    import uvicorn

    from memory_server.transports.http.limits import protocol_class
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    sock.listen()
    server = uvicorn.Server(uvicorn.Config(app, host=config.host, port=config.port, workers=1, reload=False,
                                          http=protocol_class(config), ws='none', proxy_headers=False,
                                          access_log=False, log_config=None, lifespan='off',
                                          h11_max_incomplete_event_size=config.max_header_bytes))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(.01)
        yield sock.getsockname()[1]
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 5)
        sock.close()


async def test_t14_wire_slow_header_deadline_and_header_cap(shell):
    async with wire(shell.app, shell.config) as port:
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        started = monotonic()
        for _ in range(5):
            writer.write(b'G')
            await writer.drain()
            await asyncio.sleep(.9)
        assert await asyncio.wait_for(reader.read(), 1.5) == b''
        elapsed = monotonic() - started
        assert 4.5 <= elapsed <= 6.0
        print('HEADER_DEADLINE_SECONDS', elapsed)
        writer.close()
        await writer.wait_closed()
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        writer.write(b'GET /healthz HTTP/1.1\r\nHost: localhost\r\nX: ' + b'x' * 16384)
        await writer.drain()
        assert await asyncio.wait_for(reader.read(), 1) == b''
        writer.close()
        await writer.wait_closed()
    assert rows(shell) == []


async def test_t13_wire_cl_te_pipeline_and_connection_cap(shell):
    config = replace(shell.config, max_connections=2)
    async with wire(shell.app, config) as port:
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        writer.write(b'POST /api/v1/profiles/alpha/disable HTTP/1.1\r\nHost: localhost\r\n'
                     b'Content-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n')
        await writer.drain()
        assert await asyncio.wait_for(reader.read(), 1) == b''
        writer.close()
        await writer.wait_closed()
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        writer.write(b'GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n' * 2)
        await writer.drain()
        response = await asyncio.wait_for(reader.read(), 1)
        assert response.count(b'HTTP/1.1 200') == 1
        writer.close()
        await writer.wait_closed()
        connections = [await asyncio.open_connection('127.0.0.1', port) for _ in range(3)]
        assert await asyncio.wait_for(connections[-1][0].read(), 1) == b''
        for _, writer in connections:
            writer.close()
            await writer.wait_closed()
    assert rows(shell) == []


async def test_t14_wire_default_slow_body_deadline(shell):
    async with wire(shell.app, shell.config) as port:
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        writer.write(b'POST /api/v1/profiles/alpha/disable HTTP/1.1\r\nHost: localhost\r\n'
                     b'Transfer-Encoding: chunked\r\n\r\n')
        await writer.drain()
        started = monotonic()
        async def dribble():
            try:
                while True:
                    writer.write(b'1\r\nx\r\n')
                    await writer.drain()
                    await asyncio.sleep(.9)
            except (ConnectionError, OSError):
                pass
        producer = asyncio.create_task(dribble())
        try:
            response = await asyncio.wait_for(reader.read(), 31)
            elapsed = monotonic() - started
            assert b'504 Gateway Timeout' in response and b'E_TIMEOUT' in response
            assert 29 <= elapsed <= 31
            print('BODY_DEADLINE_SECONDS', elapsed)
        finally:
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
            writer.close()
            await writer.wait_closed()
    assert rows(shell) == []


def test_t13_warmed_isolated_64mib_stream_rss(tmp_path):
    import os
    import subprocess
    import sys
    script = '''
import asyncio,json,resource,sys
from pathlib import Path
from tests.gateway.test_http_security import rss_probe
print(json.dumps(asyncio.run(rss_probe(Path(sys.argv[1])))))
'''
    result = subprocess.run([sys.executable, '-c', script, str(tmp_path)],
                            env=dict(os.environ), capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    observed = json.loads(result.stdout)
    print('RSS_STREAM', json.dumps(observed, sort_keys=True))
    assert observed['planned_bytes'] == 64 * 1024 * 1024
    assert observed['incremental_peak_kib'] <= 16 * 1024
    assert observed['status'] == 413 and observed['audit_rows'] == 0


@pytest.mark.parametrize('trusted', [(), ('192.0.2.0/24',), ('127.0.0.0/8',)])
async def test_t15_forwarding_only_from_configured_peer(shell, trusted):
    app = create_http_app(replace(shell.config, trusted_proxy_cidrs=trusted), shell.app.dependencies)
    observed = []
    original = app.endpoint
    async def inspect(request):
        observed.append((request.scope['client'][0], request.scope['scheme']))
        return await original(request)
    app.endpoint = inspect
    response = await request(app, bearer=shell.bearer, headers=[('X-Forwarded-For', '203.0.113.9'),
                             ('X-Forwarded-Proto', 'https')])
    assert response.status == 200
    assert observed == [('203.0.113.9', 'https') if trusted == ('127.0.0.0/8',) else ('127.0.0.1', 'http')]
    assert rows(shell)[-1]['remote_address'] is None
    await app.close()


async def test_t6_cursor_valid_continuation_limit_expiry_and_key_custody(shell, monkeypatch):
    from datetime import UTC, datetime, timedelta
    principal = await shell.tokens.authenticate(shell.bearer, now=datetime.now(UTC))
    first = await request(shell.app, '/api/v1/profiles', bearer=shell.bearer, query=b'limit=1')
    cursor = first.json['next_cursor']
    second = await request(shell.app, '/api/v1/profiles', bearer=shell.bearer,
                           query=('limit=1&cursor=' + cursor).encode())
    assert second.status == 200 and second.json['items'][0]['id'] == 'beta'
    async def forbidden(*args, **kwargs):
        pytest.fail('bad cursor reached registry')
    monkeypatch.setattr(shell.registry, 'page', forbidden)
    code(await request(shell.app, '/api/v1/profiles', bearer=shell.bearer,
                       query=('limit=2&cursor=' + cursor).encode()), 'E_CURSOR_INVALID', 400)
    expired = shell.app.profile_port._cursor(principal, last=['alpha'], upper='gamma', limit=1,
                                             issued=datetime.now(UTC)-timedelta(hours=1))
    code(await request(shell.app, '/api/v1/profiles', bearer=shell.bearer,
                       query=('limit=1&cursor=' + expired).encode()), 'E_CURSOR_EXPIRED', 410)
    key = shell.config.control_path / 'http-profile-cursor.key'
    assert key.stat().st_mode & 0o777 == 0o600
    assert key.read_bytes() != (shell.config.control_path / 'token-cursor.key').read_bytes()


async def test_t13_finite_chunked_size_before_auth(shell):
    code(await request(shell.app, body=b'x' * (1048576 + 1), headers=[('Transfer-Encoding', 'chunked')]),
         'E_REQUEST_TOO_LARGE', 413)
    assert rows(shell) == []


async def test_t11_mutated_and_subclass_config_rejected(shell):
    from memory_server.gateway_contracts.ports import HttpConfig
    config = replace(shell.config)
    object.__setattr__(config, 'workers', 2)
    with pytest.raises(ValueError, match='E_VALIDATION'):
        create_http_app(config, shell.deps)
    class Derived(HttpConfig):
        pass
    with pytest.raises(ValueError, match='E_VALIDATION'):
        create_http_app(Derived(control_path=shell.config.control_path), shell.deps)


async def rss_probe(tmp_path):
    import resource

    from tests.gateway.test_http_shell import shell as fixture
    generator = fixture.__wrapped__(tmp_path)
    state = await anext(generator)
    try:
        async with wire(state.app, state.config) as port:
            # Warm the same loopback parser/app before taking peak-RSS baseline.
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            writer.write(b'GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n')
            await writer.drain()
            assert b'200 OK' in await reader.read()
            writer.close()
            await writer.wait_closed()
            baseline = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            writer.write(b'POST /api/v1/profiles/alpha/disable HTTP/1.1\r\nHost: localhost\r\n'
                         b'Transfer-Encoding: chunked\r\n\r\n')
            await writer.drain()
            chunks_sent = 0
            async def upload():
                nonlocal chunks_sent
                try:
                    for _ in range(1024):
                        writer.write(b'10000\r\n' + b'x' * 65536 + b'\r\n')
                        await writer.drain()
                        chunks_sent += 1
                    writer.write(b'0\r\n\r\n')
                    await writer.drain()
                except (OSError, ConnectionError):
                    pass
            task = asyncio.create_task(upload())
            pieces = []
            try:
                while part := await reader.read(65536):
                    pieces.append(part)
            except ConnectionError:
                pass
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            response = b''.join(pieces)
            assert b'413 Request Entity Too Large' in response, response
            return dict(planned_bytes=64*1024*1024, chunks_sent=chunks_sent, status=413,
                        incremental_peak_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss-baseline,
                        audit_rows=len(rows(state)))
    finally:
        try:
            await anext(generator)
        except StopAsyncIteration:
            pass


async def test_pre_auth_source_isolation_and_process_ceiling(shell):
    app = create_http_app(replace(shell.config, rate_burst=20, rate_per_minute=1), shell.app.dependencies)

    async def from_peer(peer, **kwargs):
        async def source(scope, receive, send):
            scope["client"] = (peer, 12345)
            await app(scope, receive, send)
        return await request(source, "/api/v1/profiles", **kwargs)

    for _ in range(20):
        code(await from_peer("192.0.2.1"), "E_AUTH_REQUIRED", 401)
    assert rows(shell) == []
    response = await from_peer("192.0.2.2", bearer=shell.bearer)
    assert response.status == 200, response.body
    code(await from_peer("192.0.2.1", headers=[("X-Forwarded-For", "192.0.2.99")]), "E_RATE_LIMIT", 429)
    assert (await from_peer("192.0.2.3", bearer=shell.bearer)).status == 200
    assert set(app.guard.pre_auth.buckets) == {"192.0.2.1", "192.0.2.2", "192.0.2.3"}

    # Block real request-body reads, not an artificial active-counter assignment.
    entered, release = asyncio.Event(), asyncio.Event()
    count = 0

    async def blocked_body():
        nonlocal count
        count += 1
        if count == 16:
            entered.set()
        await release.wait()
        return {"type": "http.request", "body": b"", "more_body": False}

    tasks = [asyncio.create_task(from_peer(f"198.51.100.{n}", bearer=shell.bearer, receive=blocked_body))
             for n in range(1, 17)]
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert app.guard.active == 16
        code(await from_peer("203.0.113.1", bearer=shell.bearer), "E_RATE_LIMIT", 429)
        assert "203.0.113.1" not in app.guard.pre_auth.buckets
        assert (await request(app, "/healthz")).status == 200
    finally:
        release.set()
        responses = await asyncio.gather(*tasks)
        await app.close()
    assert all(response.status == 200 for response in responses)
    assert app.guard.active == 0


async def test_raw_chunked_8k_overflow_receives_413(shell):
    config = replace(shell.config, max_body_bytes=1024)
    app = create_http_app(config, shell.deps)
    async with wire(app, config) as port:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"POST /api/v1/profiles/alpha/disable HTTP/1.1\r\nHost: localhost\r\n"
                     b"Transfer-Encoding: chunked\r\n\r\n")
        await writer.drain()
        try:
            for _ in range(32):
                writer.write(b"100\r\n" + b"x" * 256 + b"\r\n")
                await writer.drain()
                await asyncio.sleep(0.002)
            writer.write(b"0\r\n\r\n")
            await writer.drain()
            response = await asyncio.wait_for(reader.read(), 2)
            head, body = response.split(b"\r\n\r\n", 1)
            assert b" 413 " in head, response
            error = json.loads(body)["error"]
            assert error["code"] == "E_REQUEST_TOO_LARGE"
            assert set(error) == {"code", "message", "details", "request_id"}
            assert rows(shell) == []
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError):
                pass
            await app.close()


@pytest.mark.parametrize("mode", ["cap", "deadline"])
async def test_overflow_drain_bounded_by_cap_and_deadline(shell, mode):
    config = replace(shell.config, max_body_bytes=1024)
    reads = 0

    async def receive():
        nonlocal reads
        reads += 1
        if mode == "deadline" and reads > 1:
            await asyncio.sleep(10)
        return {"type": "http.request", "body": b"x" * 8192, "more_body": True}

    started = monotonic()
    with pytest.raises(HttpError) as raised:
        await asyncio.wait_for(read_body({"headers": [(b"transfer-encoding", b"chunked")]}, receive, config), 1)
    assert raised.value.code == "E_REQUEST_TOO_LARGE"
    elapsed = monotonic() - started
    if mode == "cap":
        assert reads == 9  # One overflowing chunk plus at most 64 KiB of drain.
        assert elapsed < 0.5
    else:
        assert reads == 2
        assert 0.2 <= elapsed < 0.6
