"""Signed, filter-bound keyset cursor for the unmounted S10A outbox foundation."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import stat
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Callable
from uuid import UUID

from storage.outbox import OutboxKey

from memory_server.application.graph_read_service import wire_json
from memory_server.application.memory_read_service import b64, unb64, utc
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import Principal, RequestContext
from memory_server.transports.http.errors import HttpError


@dataclass(frozen=True)
class OutboxCursorState:
    last: OutboxKey
    upper: OutboxKey
    limit: int
    issued_at: datetime
    expires_at: datetime


def outbox_filter_digest(
    *, subject: str, profile_id: str, operation: str, status: str | None, memory_id: str | None
) -> str:
    return hashlib.sha256(
        wire_json(
            {
                "subject": subject,
                "profile_id": profile_id,
                "operation": operation,
                "filters": {"status": status, "memory_id": memory_id},
            }
        )
    ).hexdigest()


def _digest(principal: Principal, command: d.DiagnosticsCommand) -> str:
    return outbox_filter_digest(
        subject=principal.subject,
        profile_id=command.profile_id,
        operation=command.operation,
        status=command.payload.status,
        memory_id=command.payload.memory_id,
    )


def _parse_key(value: object) -> OutboxKey:
    if type(value) is not list or len(value) != 2 or any(type(item) is not str for item in value):
        raise ValueError
    created_at, ident = value
    if utc(created_at).isoformat(timespec="microseconds") != created_at or str(UUID(ident)) != ident:
        raise ValueError
    return OutboxKey(created_at, ident)


class OutboxCursorCodec:
    def __init__(
        self,
        key: bytes,
        ttl_seconds: int = 900,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if type(key) is not bytes or len(key) != 32 or type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 900:
            raise ValueError("invalid cursor configuration")
        self._key = key
        self.ttl = ttl_seconds
        self.clock = clock or (lambda: datetime.now(UTC))

    @classmethod
    def from_path(cls, path: Path, ttl_seconds: int = 900) -> OutboxCursorCodec:
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 900:
            raise ValueError("invalid cursor configuration")
        path = Path(path)
        try:
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            try:
                fd = os.open(
                    path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                    0o600,
                )
            except FileExistsError:
                pass
            else:
                try:
                    if os.write(fd, os.urandom(32)) != 32:
                        raise OSError
                    os.fsync(fd)
                finally:
                    os.close(fd)
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                info, key = os.fstat(fd), os.read(fd, 33)
                if not (
                    stat.S_ISREG(info.st_mode)
                    and info.st_nlink == 1
                    and info.st_uid == os.geteuid()
                    and stat.S_IMODE(info.st_mode) == 0o600
                    and len(key) == 32
                ):
                    raise OSError
            finally:
                os.close(fd)
        except (OSError, AttributeError):
            raise HttpError("E_STORAGE") from None
        return cls(key, ttl_seconds)

    def issue(
        self,
        ctx: RequestContext,
        command: d.DiagnosticsCommand,
        *,
        last: OutboxKey,
        upper: OutboxKey,
        limit: int,
        now: datetime,
        expires_at: datetime | None = None,
    ) -> str:
        try:
            if command.operation != "outbox.list" or type(limit) is not int or not 1 <= limit <= 200:
                raise ValueError
            last, upper = _parse_key(list(last.__dict__.values())), _parse_key(list(upper.__dict__.values()))
            if last > upper:
                raise ValueError
            now = utc(now)
            expiry = min(now + timedelta(seconds=self.ttl), utc(expires_at)) if expires_at else now + timedelta(
                seconds=self.ttl
            )
            if now >= expiry:
                raise HttpError("E_CURSOR_EXPIRED")
        except HttpError:
            raise
        except (ValueError, TypeError, AttributeError, UnicodeError):
            raise HttpError("E_CURSOR_INVALID") from None
        payload = {
            "v": 1,
            "profile_id": command.profile_id,
            "subject": ctx.principal.subject,
            "operation": command.operation,
            "filter_sha256": _digest(ctx.principal, command),
            "last": [last.created_at, last.id],
            "upper": [upper.created_at, upper.id],
            "limit": limit,
            "issued_at": now.isoformat(),
            "expires_at": expiry.isoformat(),
        }
        body = b64(wire_json(payload))
        return body + "." + b64(hmac.digest(self._key, body.encode("ascii"), "sha256"))

    def validate(
        self,
        principal: Principal,
        command: d.DiagnosticsCommand,
        token: str | None = None,
        *,
        now: datetime,
    ) -> OutboxCursorState:
        token = command.payload.cursor if token is None else token
        try:
            if command.operation != "outbox.list" or not isinstance(token, str) or len(token) > 8192:
                raise ValueError
            body, signature = token.split(".")
            raw, mac = unb64(body), unb64(signature)
            expected = hmac.digest(self._key, body.encode("ascii"), "sha256")
            if len(raw) > 6000 or not hmac.compare_digest(expected, mac):
                raise ValueError
            data = json.loads(raw.decode("utf-8"))
            fields = {
                "v",
                "profile_id",
                "subject",
                "operation",
                "filter_sha256",
                "last",
                "upper",
                "limit",
                "issued_at",
                "expires_at",
            }
            if wire_json(data) != raw or set(data) != fields or type(data["v"]) is not int or data["v"] != 1:
                raise ValueError
            if (data["subject"], data["profile_id"], data["operation"], data["filter_sha256"]) != (
                principal.subject,
                command.profile_id,
                command.operation,
                _digest(principal, command),
            ):
                raise ValueError
            if type(data["limit"]) is not int or not 1 <= command.payload.limit <= data["limit"] <= 200:
                raise ValueError
            last, upper = _parse_key(data["last"]), _parse_key(data["upper"])
            issued, expiry, current = utc(data["issued_at"]), utc(data["expires_at"]), utc(now)
            if (
                last > upper
                or issued.isoformat() != data["issued_at"]
                or expiry.isoformat() != data["expires_at"]
                or issued > current
                or not timedelta(0) < expiry - issued <= timedelta(seconds=self.ttl)
            ):
                raise ValueError
        except (
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
            UnicodeError,
            RecursionError,
            OverflowError,
            HttpError,
        ):
            raise HttpError("E_CURSOR_INVALID") from None
        if current >= expiry:
            raise HttpError("E_CURSOR_EXPIRED")
        return OutboxCursorState(last, upper, data["limit"], issued, expiry)
