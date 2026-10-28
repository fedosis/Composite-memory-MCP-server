"""S03: exercise real SQLite custody, policy, CLI and cross-connection races."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from memory_server.control.store import ControlError, ControlStore
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import Principal, RequestContext

NOW = datetime(2026, 10, 28, 7, tzinfo=UTC)


def modules():
    return importlib.import_module("memory_server.control.tokens"), importlib.import_module(
        "memory_server.control.policy"
    )


def principal(**kw):
    values = dict(
        token_id=uuid4().hex,
        subject="issuer",
        client_id=None,
        profiles=frozenset({"A", "B", "missing"}),
        permissions=frozenset({"token.manage", "memory.read", "memory.write"}),
        credential_generation=1,
        expires_at=NOW + timedelta(days=2),
    )
    values.update(kw)
    return Principal(**values)


def context(*, key=None, owner=None):
    return RequestContext("req_test", owner or principal(), None, key or uuid4(), 1e20)


def payload(**kw):
    values = dict(subject="agent", profiles=["A"], permissions=["memory.read"], expires_at=NOW + timedelta(hours=1))
    values.update(kw)
    return d.TokenCreate(**values)


def run(coroutine):
    return asyncio.run(coroutine)


@pytest.fixture
def custody(tmp_path):
    t, p = modules()
    with ControlStore(tmp_path / "control") as store:
        yield t, p, store, t.TokenService(store)


def bearer(result):
    return f"cmms_{result.token.id}.{result.secret}"


def expect(code, function):
    t, _ = modules()
    with pytest.raises(t.TokenError) as caught:
        function()
    assert caught.value.code == code
    return caught.value


def test_digest_construction_and_all_columns(custody):
    t, _, store, service = custody
    result = run(service.create(context(), payload(), now=NOW))
    row = store._connection.execute("SELECT * FROM tokens").fetchone()
    assert row["digest"] == hashlib.sha256(result.secret.encode("ascii") + t.DIGEST_DOMAIN).digest()
    assert UUID(result.token.id).version == 4
    assert len(base64.urlsafe_b64decode(result.secret + "=")) == 32
    encoded = [result.secret.encode(), base64.b64encode(result.secret.encode()), result.secret.encode().hex().encode()]
    for (table,) in store._connection.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        for item in store._connection.execute(f'SELECT * FROM "{table}"'):
            raw = repr(tuple(item)).encode()
            assert all(value not in raw for value in encoded)
    assert (store.root / "control.sqlite").stat().st_mode & 0o777 == 0o600
    assert store.root.stat().st_mode & 0o777 == 0o700


def test_compare_digest_is_live_boundary(custody, monkeypatch):
    t, _, _, service = custody
    result = run(service.create(context(), payload(), now=NOW))
    assert run(service.authenticate(bearer(result), now=NOW)).subject == "agent"

    def fail(*args):
        raise RuntimeError("compare boundary exercised")

    monkeypatch.setattr(t.hmac, "compare_digest", fail)
    with pytest.raises(RuntimeError, match="compare boundary exercised"):
        run(service.authenticate(bearer(result), now=NOW))


def test_uniform_failures_no_credentials_in_logs(custody, caplog):
    _, _, _, service = custody
    result = run(service.create(context(), payload(), now=NOW))
    valid = bearer(result)
    failures = [
        "x",
        "cmms_short.A",
        "cmms_" + "z" * 32 + "." + "A" * 43,
        "cmms_" + result.token.id + ".***",
        valid + ".x",
        valid + "=",
        "cmms_" + uuid4().hex + "." + result.secret,
        "cmms_" + result.token.id + "." + ("A" * 43 if result.secret != "A" * 43 else "B" * 43),
        "cmms_" + result.token.id + "." + "A" * 42,
        valid[:-1] + "!",
    ]
    errors = [
        expect("E_AUTH_REQUIRED", lambda value=value: run(service.authenticate(value, now=NOW))) for value in failures
    ]
    errors.append(expect("E_AUTH_REQUIRED", lambda: run(service.authenticate(valid, now=payload().expires_at))))
    run(service.revoke(context(), result.token.id, mutation(), now=NOW))
    errors.append(expect("E_AUTH_REQUIRED", lambda: run(service.authenticate(valid, now=NOW))))
    bodies = [error.envelope("req_fixed") for error in errors]
    assert all(body == bodies[0] for body in bodies)
    assert all(error.status == 401 for error in errors)
    assert result.secret not in caplog.text and result.token.id not in caplog.text


def mutation(generation=1):
    return d.ControlMutation(reason="local test", confirmation=True, expected_generation=generation)


def test_policy_scope_no_roles_and_issuer_subset(custody):
    _, policy, _, service = custody
    issuer = principal(profiles=frozenset({"A"}), permissions=frozenset({"token.manage", "memory.read"}))
    child = run(service.create(context(owner=issuer), payload(), now=NOW))
    owner = run(service.authenticate(bearer(child), now=NOW))
    assert owner.profiles == frozenset({"A"}) and owner.permissions == frozenset({"memory.read"})
    assert policy.allows(owner, "memory.read", profile_id="A")
    assert not policy.allows(owner, "memory.write", profile_id="A")
    assert not policy.allows(owner, "memory.read", profile_id="B")
    for request in (payload(profiles=["B"]), payload(permissions=["memory.write"])):
        expect("E_PERMISSION_DENIED", lambda: run(service.create(context(owner=issuer), request, now=NOW)))
    for permission in ("*", "admin", "unknown"):
        with pytest.raises(ValidationError):
            payload(permissions=[permission])
    for profile in ("*", "../A", ""):
        expect("E_VALIDATION", lambda: run(service.create(context(), payload(profiles=[profile]), now=NOW)))
    expect("E_PERMISSION_DENIED", lambda: run(service.create(context(owner=owner), payload(), now=NOW)))


def test_profile_errors_stay_in_real_s02_registry(custody, tmp_path):
    from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration

    _, _, store, service = custody
    # Empty real registry suffices for scope-before-lookup and scoped unknown.
    registry = ProfileRegistry(store, RegistryConfiguration(()))
    result = run(service.create(context(), payload(), now=NOW))
    owner = run(service.authenticate(bearer(result), now=NOW))
    for selected, code in [("B", "E_PERMISSION_DENIED"), ("A", "E_PROFILE_NOT_FOUND")]:
        with pytest.raises(ControlError) as caught:
            run(registry.resolve(owner, selected))
        assert caught.value.code == code
    # Use S02's actual configuration fixture, not a substitute registry.
    from tests.gateway.test_registry import binding

    configured = binding(tmp_path / "profile-a", "A")
    registry = ProfileRegistry(store, RegistryConfiguration((configured,)))
    run(
        registry.set_enabled(
            replace(owner, permissions=frozenset({"profile.manage"})), "A", enabled=False, expected_generation=1
        )
    )
    assert run(service.authenticate(bearer(result), now=NOW)) == owner
    with pytest.raises(ControlError) as caught:
        run(registry.resolve(owner, "A"))
    assert caught.value.code == "E_PROFILE_DISABLED"


def test_query_and_header_credentials_never_logged(custody, caplog):
    _, policy, _, service = custody
    result = run(service.create(context(), payload(), now=NOW))
    for name in ("token", "access_token", "authorization", "bearer", "%74oken"):
        error = expect(
            "E_VALIDATION", lambda: policy.bearer_from_headers([], query_string=f"{name}={bearer(result)}".encode())
        )
        assert error.status == 400
        assert result.secret not in json.dumps(error.envelope("req_test"))
    for headers in ([], [("authorization", "Basic x")], [("authorization", "Bearer x")] * 2):
        expect("E_AUTH_REQUIRED", lambda: policy.bearer_from_headers(headers))
    assert policy.bearer_from_headers([("Authorization", "Bearer " + bearer(result))]) == bearer(result)
    assert result.secret not in caplog.text and result.token.id not in caplog.text


def test_create_replay_ten_and_payload_conflict(custody):
    _, _, store, service = custody
    ctx = context()
    results = [run(service.create(ctx, payload(), now=NOW)) for _ in range(10)]
    assert results[0].secret and all(item.secret is None for item in results[1:])
    assert all(item.token == results[0].token for item in results)
    assert store._connection.execute("SELECT count(*) FROM tokens").fetchone()[0] == 1
    expect("E_IDEMPOTENCY_CONFLICT", lambda: run(service.create(ctx, payload(subject="other"), now=NOW)))
    expect(
        "E_IDEMPOTENCY_REQUIRED", lambda: run(service.create(replace(ctx, idempotency_key=None), payload(), now=NOW))
    )
    restricted = replace(ctx, principal=replace(ctx.principal, permissions=frozenset()))
    expect("E_PERMISSION_DENIED", lambda: run(service.create(restricted, payload(), now=NOW)))


def test_concurrent_create_two_connections(tmp_path):
    t, _ = modules()
    root = tmp_path / "control"
    with ControlStore(root) as store:
        t.TokenService(store)
    barrier = threading.Barrier(2)
    ctx = context()

    def worker(_):
        with ControlStore(root) as store:
            service = t.TokenService(store)
            barrier.wait(timeout=10)
            return run(service.create(ctx, payload(), now=NOW))

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(worker, range(2)))
    assert sum(result.secret is not None for result in results) == 1
    assert len({result.token.id for result in results}) == 1
    with ControlStore(root) as store:
        assert store._connection.execute("SELECT count(*) FROM tokens").fetchone()[0] == 1
        assert store._connection.execute("SELECT count(*) FROM token_journal").fetchone()[0] == 1


def test_revoke_immediate_generation_and_replay(custody):
    _, _, store, service = custody
    result = run(service.create(context(), payload(), now=NOW))
    owner = run(service.authenticate(bearer(result), now=NOW))
    ctx = context()
    revoked = run(service.revoke(ctx, result.token.id, mutation(), now=NOW))
    assert revoked.generation == owner.credential_generation + 1 and revoked.revoked_at == NOW
    assert run(service.revoke(ctx, result.token.id, mutation(), now=NOW)) == revoked
    expect("E_AUTH_REQUIRED", lambda: run(service.authenticate(bearer(result), now=NOW)))
    expect("E_VERSION_CONFLICT", lambda: run(service.revoke(context(), result.token.id, mutation(), now=NOW)))
    assert run(service.revoke(context(), result.token.id, mutation(2), now=NOW)) == revoked
    row = store._connection.execute("SELECT generation,revoked_at FROM tokens").fetchone()
    assert row["generation"] == 2 and row["revoked_at"] is not None


@pytest.mark.parametrize("operation", ["create", "revoke"])
@pytest.mark.parametrize("phase", ["after_row", "after_journal", "after_commit"])
def test_crash_atomicity_reopen(tmp_path, operation, phase):
    t, _ = modules()
    root = tmp_path / "control"
    ctx = context()
    with ControlStore(root) as store:
        service = t.TokenService(store)
        if operation == "revoke":
            created = run(service.create(context(), payload(), now=NOW))

        def crash(actual):
            if phase == actual:
                raise RuntimeError("injected crash")

        service._checkpoint = crash
        with pytest.raises(RuntimeError, match="injected crash"):
            if operation == "create":
                run(service.create(ctx, payload(), now=NOW))
            else:
                run(service.revoke(ctx, created.token.id, mutation(), now=NOW))
    with ControlStore(root) as store:
        service = t.TokenService(store)
        rows = store._connection.execute("SELECT * FROM tokens").fetchall()
        journal = store._connection.execute("SELECT * FROM token_journal").fetchall()
        committed = phase == "after_commit"
        assert len(rows) == (1 if operation == "revoke" or committed else 0)
        assert len(journal) == (1 if operation == "revoke" else 0) + int(committed)
        if operation == "revoke":
            assert rows[0]["generation"] == (2 if committed else 1)
            assert (rows[0]["revoked_at"] is not None) == committed
        else:
            retry = run(service.create(ctx, payload(), now=NOW))
            assert (retry.secret is None) == committed


def test_journal_metadata_canary_scan(custody, caplog, monkeypatch):
    t, _, store, service = custody
    canary_bytes = os.urandom(32)
    monkeypatch.setattr(t.secrets, "token_bytes", lambda size: canary_bytes if size == 32 else os.urandom(size))
    created = run(service.create(context(), payload(), now=NOW))
    secret = created.secret
    assert secret == base64.urlsafe_b64encode(canary_bytes).decode().rstrip("=")
    run(service.authenticate(bearer(created), now=NOW))
    expect("E_AUTH_REQUIRED", lambda: run(service.authenticate(bearer(created) + "x", now=NOW)))
    revoked = run(service.revoke(context(), created.token.id, mutation(), now=NOW))
    listing = run(service.list(context(), d.PageQuery(), now=NOW))
    digest = store._connection.execute("SELECT digest FROM tokens").fetchone()[0]
    rows = [dict(row) for row in store._connection.execute("SELECT * FROM token_journal")]
    raw = repr(rows)
    assert secret not in raw and repr(digest) not in raw and digest.hex() not in raw
    assert all("digest" not in key and "secret" not in key for row in rows for key in row)
    outputs = [
        revoked.model_dump_json(),
        listing.model_dump_json(),
        caplog.text,
        json.dumps(
            expect("E_AUTH_REQUIRED", lambda: run(service.authenticate(bearer(created), now=NOW))).envelope("req_test")
        ),
    ]
    assert all(secret not in value and digest.hex() not in value for value in outputs)
    assert secret.encode() not in (store.root / "control.sqlite").read_bytes()
    print("CANARY_SCAN db=PASS journal=PASS logs=PASS errors=PASS list=PASS revoke=PASS create=ONE_TIME")


def test_expiry_and_clock_validation(custody):
    _, _, _, service = custody
    result = run(service.create(context(), payload(), now=NOW))
    expires = result.token.expires_at
    assert run(service.authenticate(bearer(result), now=expires - timedelta(microseconds=1)))
    expect("E_AUTH_REQUIRED", lambda: run(service.authenticate(bearer(result), now=expires)))
    for invalid in (
        datetime.min,
        datetime.min.replace(tzinfo=UTC),
        NOW.replace(tzinfo=None),
        NOW.astimezone(timezone(timedelta(hours=3))),
    ):
        expect("E_VALIDATION", lambda: run(service.authenticate(bearer(result), now=invalid)))
    expect("E_AUTH_REQUIRED", lambda: run(service.authenticate(bearer(result), now=NOW - timedelta(seconds=1))))
    expect("E_VALIDATION", lambda: run(service.create(context(), payload(expires_at=NOW), now=NOW)))


def test_entropy_64(custody):
    _, _, _, service = custody
    results = [run(service.create(context(), payload(), now=NOW)) for _ in range(64)]
    assert len({value.token.id for value in results}) == len({value.secret for value in results}) == 64
    assert all(
        UUID(value.token.id).version == 4 and len(base64.urlsafe_b64decode(value.secret + "=")) == 32
        for value in results
    )


def test_last_used_bounded_monotonic_failure_no_write(custody):
    _, _, store, service = custody
    result = run(service.create(context(), payload(), now=NOW))
    before = store._connection.total_changes
    for _ in range(20):
        run(service.authenticate(bearer(result), now=NOW))
    assert store._connection.total_changes - before == 1
    run(service.authenticate(bearer(result), now=NOW + timedelta(seconds=60)))
    last = store._connection.execute("SELECT last_used_at FROM tokens").fetchone()[0]
    before = store._connection.total_changes
    run(service.authenticate(bearer(result), now=NOW + timedelta(seconds=1)))
    expect("E_AUTH_REQUIRED", lambda: run(service.authenticate(bearer(result) + "x", now=NOW)))
    assert store._connection.total_changes == before
    assert store._connection.execute("SELECT last_used_at FROM tokens").fetchone()[0] == last


def test_list_permissions_pagination_tamper_binding_expiry(custody):
    _, _, store, service = custody
    for _ in range(5):
        run(service.create(context(), payload(), now=NOW))
    expect(
        "E_PERMISSION_DENIED",
        lambda: run(service.list(context(owner=principal(permissions=frozenset())), d.PageQuery(), now=NOW)),
    )
    ctx = context()
    page = run(service.list(ctx, d.PageQuery(limit=2), now=NOW))
    assert len(page.items) == 2 and page.has_more and page.next_cursor
    second = run(service.list(ctx, d.PageQuery(limit=1, cursor=page.next_cursor), now=NOW))
    assert len(second.items) == 1 and second.has_more
    third = run(service.list(ctx, d.PageQuery(limit=2, cursor=second.next_cursor), now=NOW))
    assert len(third.items) == 2 and not third.has_more and third.next_cursor is None
    assert len({item.id for item in page.items + second.items + third.items}) == 5
    assert all(word not in page.model_dump_json() for word in ("secret", "digest", "verifier"))
    for cursor in ("bad", page.next_cursor + "x"):
        expect("E_CURSOR_INVALID", lambda: run(service.list(ctx, d.PageQuery(cursor=cursor), now=NOW)))
    expect(
        "E_CURSOR_INVALID",
        lambda: run(
            service.list(context(owner=principal(subject="another")), d.PageQuery(cursor=page.next_cursor), now=NOW)
        ),
    )
    expect(
        "E_CURSOR_INVALID",
        lambda: run(
            service.list(
                context(owner=principal(profiles=frozenset({"A"}))), d.PageQuery(cursor=page.next_cursor), now=NOW
            )
        ),
    )
    expect(
        "E_CURSOR_EXPIRED",
        lambda: run(service.list(ctx, d.PageQuery(cursor=page.next_cursor), now=NOW + timedelta(minutes=15))),
    )
    assert (store.root / "token-cursor.key").stat().st_mode & 0o777 == 0o600
    with pytest.raises(ValidationError):
        d.PageQuery(limit=201)


def test_cli_bootstrap_import_no_io_and_one_time(tmp_path, caplog):
    from memory_server.cli import app

    runner = CliRunner()
    root = tmp_path / "control"
    missing = runner.invoke(
        app,
        [
            "token",
            "create",
            "--subject",
            "operator",
            "--profile",
            "A",
            "--permission",
            "token.manage",
            "--expires-at",
            "2099-01-01T00:00:00Z",
        ],
    )
    assert missing.exit_code == 2 and "E_VALIDATION" in missing.output and not root.exists()
    args = [
        "token",
        "create",
        "--control-path",
        str(root / "control.sqlite"),
        "--subject",
        "operator",
        "--profile",
        "A",
        "--permission",
        "token.manage",
        "--permission",
        "memory.reveal",
        "--expires-at",
        "2099-01-01T00:00:00Z",
    ]
    created = runner.invoke(app, args)
    assert created.exit_code == 0, created.output
    response = json.loads(created.stdout)
    secret = response["secret"]
    assert secret and secret not in " ".join(args) and secret not in caplog.text
    listing = runner.invoke(app, ["token", "list", "--control-path", str(root / "control.sqlite")])
    assert listing.exit_code == 0 and secret not in listing.output
    revoked = runner.invoke(
        app, ["token", "revoke", response["token"]["id"], "--control-path", str(root / "control.sqlite")]
    )
    assert revoked.exit_code == 0 and secret not in revoked.output
    assert json.loads(revoked.stdout)["revoked_at"] is not None
    t, _ = modules()
    with ControlStore(root) as store:
        service = t.TokenService(store)
        expect(
            "E_AUTH_REQUIRED",
            lambda: run(service.authenticate("cmms_" + response["token"]["id"] + "." + secret, now=datetime.now(UTC))),
        )
    assert secret.encode() not in (root / "control.sqlite").read_bytes()
    assert runner.invoke(app, args + ["--secret", "not-an-accepted-option"]).exit_code == 2
    empty = tmp_path / "empty"
    empty.mkdir()
    proc = subprocess.run(
        [sys.executable, "-c", "import memory_server.control.token_cli"],
        cwd=empty,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert not list(empty.iterdir())
    print("CLI_CANARY_SCAN argv=PASS db=PASS logs=PASS list=PASS revoke=PASS stdout=ONE_TIME import=NO_IO")


@pytest.mark.parametrize("operation", ["create", "revoke"])
@pytest.mark.parametrize("phase", ["after_row", "after_journal", "after_commit"])
def test_process_death_sqlite_recovery(tmp_path, operation, phase):
    t, _ = modules()
    root = tmp_path / "control"
    with ControlStore(root) as store:
        service = t.TokenService(store)
        created = run(service.create(context(), payload(), now=NOW)) if operation == "revoke" else None
    script = """
import asyncio, os, sys
from pathlib import Path
from datetime import UTC, datetime, timedelta
from memory_server.control.store import ControlStore
from memory_server.control.tokens import TokenService
from memory_server.gateway_contracts import dto as d
root, op, phase, token_id = sys.argv[1:]
now = datetime(2026,10,28,7,tzinfo=UTC)
with ControlStore(Path(root)) as store:
    service = TokenService(store)
    def crash(actual):
        if actual == phase:
            os._exit(73)
    service._checkpoint = crash
    if op == 'create':
        service._bootstrap_create(d.TokenCreate(subject='crash', profiles=['A'],
            permissions=['memory.read'], expires_at=now+timedelta(hours=1)), now=now)
    else:
        service._revoke(service._local_context(now), token_id,
            d.ControlMutation(reason='crash', confirmation=True, expected_generation=1), now, local=True)
"""
    proc = subprocess.run(
        [sys.executable, "-c", script, str(root), operation, phase, created.token.id if created else ""],
        capture_output=True,
    )
    assert proc.returncode == 73
    assert proc.stdout == proc.stderr == b""
    with ControlStore(root) as store:
        t.TokenService(store)
        rows = store._connection.execute("SELECT * FROM tokens").fetchall()
        journal = store._connection.execute("SELECT * FROM token_journal").fetchall()
        committed = phase == "after_commit"
        assert len(rows) == (1 if operation == "revoke" or committed else 0)
        assert len(journal) == (1 if operation == "revoke" else 0) + int(committed)
        assert store._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        if operation == "revoke":
            assert rows[0]["generation"] == (2 if committed else 1)
            assert (rows[0]["revoked_at"] is not None) == committed


def test_authenticate_revoke_serialized_real_connections(tmp_path, monkeypatch):
    t, _ = modules()
    root = tmp_path / "control"
    with ControlStore(root) as store:
        service = t.TokenService(store)
        created = run(service.create(context(), payload(), now=NOW))
    barrier = threading.Barrier(2)
    authenticating = threading.Event()
    revoke_attempted = threading.Event()
    compare = t.hmac.compare_digest

    def paused_compare(left, right):
        if isinstance(left, bytes) and not authenticating.is_set():
            authenticating.set()
            assert revoke_attempted.wait(10)
        return compare(left, right)

    monkeypatch.setattr(t.hmac, "compare_digest", paused_compare)

    def auth_worker():
        with ControlStore(root) as store:
            service = t.TokenService(store)
            barrier.wait(10)
            return run(service.authenticate(bearer(created), now=NOW))

    def revoke_worker():
        with ControlStore(root) as store:
            service = t.TokenService(store)
            barrier.wait(10)
            assert authenticating.wait(10)
            revoke_attempted.set()
            result = run(service.revoke(context(), created.token.id, mutation(), now=NOW))
            expect("E_AUTH_REQUIRED", lambda: run(service.authenticate(bearer(created), now=NOW)))
            return result

    with ThreadPoolExecutor(2) as pool:
        auth_future = pool.submit(auth_worker)
        revoke_future = pool.submit(revoke_worker)
        before, after = auth_future.result(), revoke_future.result()
    assert before.credential_generation < after.generation


def test_pagination_sql_bound_highwater_and_scope(custody):
    _, _, store, service = custody
    for i in range(5):
        run(service.create(context(), payload(profiles=["A" if i % 2 else "B"]), now=NOW))
    ctx = context(owner=principal(profiles=frozenset({"A"})))
    statements = []
    store._connection.set_trace_callback(statements.append)
    first = run(service.list(ctx, d.PageQuery(limit=1), now=NOW))
    assert len(first.items) == 1 and first.has_more
    late = run(service.create(context(), payload(), now=NOW + timedelta(seconds=1)))
    second = run(service.list(ctx, d.PageQuery(limit=1, cursor=first.next_cursor), now=NOW + timedelta(seconds=2)))
    assert len(second.items) == 1 and not second.has_more
    assert late.token.id not in {item.id for item in first.items + second.items}
    assert all(item.profiles == ["A"] for item in first.items + second.items)
    selects = [value for value in statements if value.startswith("SELECT")]
    assert any("LIMIT 2" in value and "json_each" in value for value in selects)
    assert all("LIMIT" in value for value in selects if "FROM tokens WHERE NOT EXISTS" in value)


def test_mutable_dto_revalidated_and_closed_store_fails_safe(custody):
    _, _, store, service = custody
    request = payload()
    request.permissions.append("admin")
    expect("E_VALIDATION", lambda: run(service.create(context(), request, now=NOW)))
    created = run(service.create(context(), payload(), now=NOW))
    store.close()
    error = expect("E_STORAGE", lambda: run(service.authenticate(bearer(created), now=NOW)))
    assert created.secret not in str(error)


@pytest.mark.parametrize("kind", ["short", "mode", "symlink", "hardlink"])
def test_cursor_key_unsafe_fail_closed(tmp_path, kind):
    t, _ = modules()
    root = tmp_path / "control"
    with ControlStore(root) as store:
        t.TokenService(store)
    key = root / "token-cursor.key"
    if kind == "short":
        key.write_bytes(b"short")
    elif kind == "mode":
        key.chmod(0o644)
    elif kind == "symlink":
        key.unlink()
        key.symlink_to(tmp_path / "missing")
    else:
        os.link(key, tmp_path / "extra-key")
    with ControlStore(root) as store:
        expect("E_STORAGE", lambda: t.TokenService(store))


def test_schema_rehearsal_keeps_s02_rows_and_version(custody, tmp_path):
    from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
    from tests.gateway.test_registry import binding

    t, _, store, service = custody
    registry = ProfileRegistry(store, RegistryConfiguration((binding(tmp_path / "profile", "A"),)))
    run(
        registry.set_enabled(
            principal(permissions=frozenset({"profile.manage"})), "A", enabled=False, expected_generation=1
        )
    )
    before = tuple(store._connection.execute("SELECT * FROM profiles").fetchone())
    journal = tuple(store._connection.execute("SELECT * FROM control_journal").fetchone())
    root = store.root
    run(service.create(context(), payload(), now=NOW))
    store.close()
    with ControlStore(root) as reopened:
        t.TokenService(reopened)
        assert tuple(reopened._connection.execute("SELECT * FROM profiles").fetchone()) == before
        assert tuple(reopened._connection.execute("SELECT * FROM control_journal").fetchone()) == journal
        assert reopened._connection.execute("PRAGMA user_version").fetchone()[0] == 1


def test_corrupt_metadata_is_safe_storage_failure(custody):
    _, _, store, service = custody
    created = run(service.create(context(), payload(), now=NOW))
    store._connection.execute("UPDATE tokens SET profiles='not-json'")
    error = expect("E_STORAGE", lambda: run(service.authenticate(bearer(created), now=NOW)))
    assert created.secret not in str(error) and created.token.id not in str(error)


def test_storage_failure_does_not_consume_create_key(custody):
    _, _, store, service = custody
    ctx = context()
    store._connection.execute(
        "CREATE TRIGGER journal_failure BEFORE INSERT ON token_journal "
        "BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END"
    )
    expect("E_STORAGE", lambda: run(service.create(ctx, payload(), now=NOW)))
    assert store._connection.execute("SELECT count(*) FROM tokens").fetchone()[0] == 0
    assert store._connection.execute("SELECT count(*) FROM token_journal").fetchone()[0] == 0
    store._connection.execute("DROP TRIGGER journal_failure")
    assert run(service.create(ctx, payload(), now=NOW)).secret is not None


def test_replay_preserves_original_snapshot_after_revoke(custody):
    _, _, _, service = custody
    ctx = context()
    original = run(service.create(ctx, payload(), now=NOW))
    run(service.authenticate(bearer(original), now=NOW))
    run(service.revoke(context(), original.token.id, mutation(), now=NOW))
    replay = run(service.create(ctx, payload(), now=NOW + timedelta(hours=1)))
    assert replay.token == original.token and replay.secret is None


def test_catalog_execute_real_token_port(custody):
    from memory_server.gateway_contracts.catalog import CATALOG

    _, _, _, service = custody
    now = datetime.now(UTC)
    ctx = context(owner=replace(principal(), expires_at=now + timedelta(hours=2)))
    command = CATALOG.validate(
        "tokens.create",
        dict(
            operation="tokens.create",
            profile_id=None,
            path_ids={},
            payload=payload(expires_at=now + timedelta(hours=1)).model_dump(mode="json"),
        ),
    )
    result = run(service.execute(ctx, command))
    assert result.secret is not None
    listing = run(
        service.execute(
            ctx,
            CATALOG.validate("tokens.list", dict(operation="tokens.list", profile_id=None, path_ids={}, payload={})),
        )
    )
    assert [item.id for item in listing.items] == [result.token.id]
