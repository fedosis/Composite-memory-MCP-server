"""S06 profile HTTP port: bounded reads, cursor custody, and control wiring."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import stat
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import unquote

from pydantic import ValidationError

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import (
    AuditTicket,
    ControlReply,
    GatewayDependencies,
    GatewayPort,
    Principal,
    RequestContext,
)

from ..errors import HttpError

PROFILE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z", re.ASCII)
_CURSOR_VERSION = 1
_CURSOR_BYTES = 32

# The parent binds audit_ticket around the complete eligible execution.  The
# reply deliberately remains available after execute() returns so the parent
# can add replay headers without changing the GatewayResult ABI.
audit_ticket: ContextVar[AuditTicket | None] = ContextVar("audit_ticket", default=None)
control_reply: ContextVar[ControlReply | None] = ContextVar("control_reply", default=None)
control_pending: ContextVar[bool] = ContextVar("control_pending", default=False)


def bind_control_pending(registry, store):
    """Composition-only ticket custody check, not a new frozen port method.

    Profile generation identifies its current journal row. A committed pending
    control fences later effects for that profile, so that row cannot be
    superseded by HTTP while this lookup runs. Read one metadata row, never
    raw profile rows or roots, even when historical journals are arbitrarily long.
    """
    async def pending(ctx, ticket):
        record = await registry.record(ctx.principal, ctx.profile_id, permission="profile.manage")
        rows = store.control_requests(limit=1, after_generation=record.generation - 1)
        return bool(rows and rows[0]["generation"] == record.generation and rows[0]["ticket_id"] == ticket.id)
    return pending


def _fail(code: str, status: int | None = None) -> None:
    raise HttpError(code, status)


def validate_profile_id(value: str) -> str:
    """Validate both the literal path token and its already-decoded value."""
    if not isinstance(value, str) or unquote(value) != value:
        _fail("E_VALIDATION")
    if PROFILE_ID_RE.fullmatch(value) is None:
        _fail("E_VALIDATION")
    return value


def _query_pairs(query_items: Any) -> list[tuple[str, Any]]:
    if hasattr(query_items, "multi_items"):
        return [(str(k), v) for k, v in query_items.multi_items()]
    if isinstance(query_items, Mapping):
        result: list[tuple[str, Any]] = []
        for key, value in query_items.items():
            if isinstance(value, (list, tuple)):
                result.extend((str(key), item) for item in value)
            else:
                result.append((str(key), value))
        return result
    try:
        return [(str(k), v) for k, v in query_items]
    except (TypeError, ValueError):
        _fail("E_VALIDATION")


def validate_query(operation: str, query_items: Any) -> d.PageQuery:
    """Parse the profile query without permissive coercion or repeated keys."""
    if operation != "profiles.list":
        _fail("E_VALIDATION")
    pairs = _query_pairs(query_items)
    allowed = {"limit", "cursor"}
    seen: set[str] = set()
    values: dict[str, Any] = {}
    for key, value in pairs:
        if key not in allowed or key in seen:
            _fail("E_VALIDATION")
        seen.add(key)
        if key == "limit":
            if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
                _fail("E_VALIDATION")
            values[key] = int(value)
        else:
            values[key] = value
    try:
        return d.PageQuery(**values)
    except (ValidationError, TypeError, ValueError):
        _fail("E_VALIDATION")


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _unb64(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _cursor_path(config: Any) -> Path:
    configured = Path(config.control_path)
    return configured if configured.is_dir() else configured.parent


def _load_cursor_key(config: Any) -> bytes:
    directory = _cursor_path(config)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = directory / "http-profile-cursor.key"
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    except FileExistsError:
        pass
    else:
        try:
            key = os.urandom(_CURSOR_BYTES)
            if os.write(fd, key) != _CURSOR_BYTES:
                _fail("E_STORAGE")
            os.fsync(fd)
        finally:
            os.close(fd)
    try:
        fd = os.open(path, flags)
        info = os.fstat(fd)
        key = os.read(fd, _CURSOR_BYTES + 1)
    except (OSError, ValueError):
        _fail("E_STORAGE")
    finally:
        try:
            os.close(fd)
        except UnboundLocalError:
            pass
    secure = (
        stat.S_ISREG(info.st_mode)
        and info.st_nlink == 1
        and info.st_uid == os.geteuid()
        and stat.S_IMODE(info.st_mode) == 0o600
        and len(key) == _CURSOR_BYTES
    )
    if not secure:
        _fail("E_STORAGE")
    return key


def _profile_payload(profile: d.Profile, observation: Any = None) -> d.Profile:
    state = getattr(observation, "state", None) or profile.runtime_state
    if state not in {"not_started", "ready", "closing", "failed"}:
        state = "not_started"
    return profile.model_copy(update={"runtime_state": state, "storage_root": None})


class ProfilePort(GatewayPort):
    """Gateway adapter for exactly the four S06 profile operations."""

    def __init__(self, config: Any, dependencies: GatewayDependencies, publish) -> None:
        self.config = config
        self.dependencies = dependencies
        self.publish = publish
        self.pending_control = None
        self._cursor_key = _load_cursor_key(config)

    def _cursor(
        self,
        principal: Principal,
        *,
        last: list[str],
        upper: str | None,
        limit: int,
        issued: datetime,
        operation: str = "profiles.list",
    ) -> str:
        payload = {
            "v": _CURSOR_VERSION,
            "profile_id": None,
            "subject": principal.subject,
            "operation": operation,
            "filter_sha256": hashlib.sha256(_canonical({})).hexdigest(),
            "last": last,
            "upper": [upper] if upper is not None else None,
            "limit": limit,
            "issued_at": issued.astimezone(UTC).isoformat(),
            "expires_at": (issued + timedelta(seconds=self.config.cursor_ttl_seconds)).astimezone(UTC).isoformat(),
        }
        body = _b64(_canonical(payload))
        return body + "." + _b64(hmac.new(self._cursor_key, body.encode("ascii"), hashlib.sha256).digest())

    def validate_cursor(self, principal: Principal, payload: str) -> dict[str, Any]:
        if not isinstance(payload, str) or payload.count(".") != 1:
            _fail("E_CURSOR_INVALID")
        body, signature = payload.split(".")
        try:
            expected = hmac.new(self._cursor_key, body.encode("ascii"), hashlib.sha256).digest()
            supplied = _unb64(signature)
            raw = _unb64(body)
            data = json.loads(raw.decode("utf-8"))
        except (ValueError, TypeError, UnicodeError, json.JSONDecodeError):
            _fail("E_CURSOR_INVALID")
        if not hmac.compare_digest(expected, supplied) or not isinstance(data, dict):
            _fail("E_CURSOR_INVALID")
        now = datetime.now(UTC)
        try:
            if (
                data.get("v") != 1
                or data.get("subject") != principal.subject
                or data.get("operation") != "profiles.list"
            ):
                _fail("E_CURSOR_INVALID")
            expires = datetime.fromisoformat(data["expires_at"])
            issued = datetime.fromisoformat(data["issued_at"])
            if now >= expires:
                _fail("E_CURSOR_EXPIRED")
            if expires < issued or expires - issued > timedelta(seconds=self.config.cursor_ttl_seconds):
                _fail("E_CURSOR_INVALID")
            if (
                not isinstance(data.get("limit"), int)
                or isinstance(data["limit"], bool)
                or not 1 <= data["limit"] <= 200
            ):
                _fail("E_CURSOR_INVALID")
            if data.get("filter_sha256") != hashlib.sha256(_canonical({})).hexdigest():
                _fail("E_CURSOR_INVALID")
            if not isinstance(data.get("last"), list) or len(data["last"]) != 1:
                _fail("E_CURSOR_INVALID")
            validate_profile_id(data["last"][0])
            if data.get("upper") is not None:
                if not isinstance(data["upper"], list) or len(data["upper"]) != 1:
                    _fail("E_CURSOR_INVALID")
                validate_profile_id(data["upper"][0])
            return data
        except (KeyError, TypeError, ValueError):
            _fail("E_CURSOR_INVALID")

    async def _list(self, ctx: RequestContext, command: Any) -> d.Page[d.Profile]:
        query = command.payload
        cursor = None
        if query.cursor:
            cursor = self.validate_cursor(ctx.principal, query.cursor)
            if query.limit > cursor["limit"]:
                _fail("E_CURSOR_INVALID")
        after = cursor["last"][0] if cursor else None
        upper = cursor["upper"][0] if cursor and cursor["upper"] else None
        requested = query.limit
        items, upper = await self.dependencies.registry.page(ctx.principal, limit=requested, after=after, upper=upper)
        has_more = False
        if len(items) == requested and items and upper and items[-1].id < upper:
            probe, _ = await self.dependencies.registry.page(ctx.principal, limit=1, after=items[-1].id, upper=upper)
            has_more = bool(probe)
        observations = self.dependencies.leases.snapshot() if self.dependencies.leases is not None else {}
        visible = [_profile_payload(item, observations.get(item.id)) for item in items]
        next_cursor = None
        if has_more and visible:
            next_cursor = self._cursor(
                ctx.principal,
                last=[visible[-1].id],
                upper=upper,
                limit=requested,
                issued=datetime.now(UTC),
            )
        return d.Page(items=visible, next_cursor=next_cursor, has_more=has_more, request_id=ctx.request_id)

    async def execute(self, ctx: RequestContext, command: d.GatewayCommand) -> d.GatewayResult:
        control_reply.set(None)
        operation = command.operation
        try:
            ticket = audit_ticket.get()
            if operation in {"profiles.list", "profiles.get", "profiles.enable", "profiles.disable"} and ticket is None:
                _fail("E_AUDIT_UNAVAILABLE")
            if operation == "profiles.list":
                return await self._list(ctx, command)
            if operation == "profiles.get":
                validate_profile_id(command.profile_id)
                binding = await self.dependencies.registry.resolve(ctx.principal, command.profile_id)
                observations = self.dependencies.leases.snapshot() if self.dependencies.leases is not None else {}
                return _profile_payload(binding.profile, observations.get(command.profile_id))
            if operation in {"profiles.enable", "profiles.disable"}:
                if self.dependencies.leases is None or self.pending_control is None:
                    _fail("E_CAPABILITY_UNAVAILABLE")
                # Recovery gates new effects inside registry.control; it must not
                # prevent replay of a final, still-pending publication snapshot.
                await self.dependencies.registry.reconcile_control(
                    leases=self.dependencies.leases, publish=self.publish, limit=1000
                )
                try:
                    reply = await self.dependencies.registry.control(
                        ctx,
                        command,
                        ticket=ticket,
                        leases=self.dependencies.leases,
                        publish=self.publish,
                        now=datetime.now(UTC),
                        ttl_seconds=self.config.idempotency_ttl_seconds,
                    )
                except BaseException as exc:
                    if getattr(exc, "code", None) in {
                        "E_AUTH_REQUIRED", "E_PERMISSION_DENIED", "E_PROFILE_NOT_FOUND", "E_VALIDATION",
                        "E_IDEMPOTENCY_REQUIRED", "E_IDEMPOTENCY_CONFLICT", "E_REQUEST_IN_PROGRESS",
                        "E_VERSION_CONFLICT",
                    }:
                        raise  # Registry rejects these before reserving a new effect.
                    # Preserve original ticket custody after commit, including
                    # cancellation while final snapshot/publication is settling.
                    try:
                        control_pending.set(await self.pending_control(ctx, ticket))
                    except Exception:
                        control_pending.set(True)  # unknown commit state is never false failure
                        _fail("E_AUDIT_UNAVAILABLE")
                    raise
                control_reply.set(reply)
                if isinstance(reply.body, d.Profile):
                    return _profile_payload(reply.body)
                return d.Profile.model_validate(reply.body)
            _fail("E_CAPABILITY_UNAVAILABLE")
        except HttpError:
            raise
        except Exception as exc:
            code = getattr(exc, "code", None)
            if isinstance(code, str):
                _fail(code)
            _fail("E_INTERNAL")


__all__ = ["ProfilePort", "audit_ticket", "control_reply", "validate_profile_id", "validate_query"]
