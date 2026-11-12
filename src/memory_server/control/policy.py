"""Explicit permission/scope policy; roles never participate in authorization.

The header adapter is transport-independent. It discards query credential values
without logging them; S06 must call it before logging/request dispatch.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from urllib.parse import unquote_plus

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import Principal

_ERRORS = {
    "E_AUTH_REQUIRED": (401, "Authentication required"),
    "E_PERMISSION_DENIED": (403, "Permission denied"),
    "E_VALIDATION": (422, "Invalid token input"),
    "E_STORAGE": (503, "Control storage unavailable"),
    "E_NOT_FOUND": (404, "Token not found"),
    "E_VERSION_CONFLICT": (409, "Credential generation changed"),
    "E_IDEMPOTENCY_REQUIRED": (400, "Idempotency key required"),
    "E_IDEMPOTENCY_CONFLICT": (409, "Idempotency key conflict"),
    "E_CURSOR_INVALID": (400, "Invalid cursor"),
    "E_CURSOR_EXPIRED": (410, "Cursor expired"),
    "E_CAPABILITY_UNAVAILABLE": (503, "Capability unavailable"),
}


class TokenError(Exception):
    """C5 safe error; never carry input, paths, verifier or backend exceptions."""

    def __init__(self, code: d.ErrorCode, *, status: int | None = None) -> None:
        default, message = _ERRORS[code]
        self.code = code
        self.status = default if status is None else status
        super().__init__(message)

    def envelope(self, request_id: str) -> dict:
        return {"error": {"code": self.code, "message": str(self), "request_id": request_id, "details": {}}}


def utc_now(now: datetime) -> datetime:
    if not isinstance(now, datetime) or now.utcoffset() != timedelta(0) or now.replace(tzinfo=None) == datetime.min:
        raise TokenError("E_VALIDATION")
    return now


def principal_from_token(token: d.TokenInfo) -> Principal:
    return Principal(
        token.id,
        token.subject,
        None,
        frozenset(token.profiles),
        frozenset(token.permissions),
        token.generation,
        token.expires_at,
    )


def allows(principal: Principal, permission: d.Permission, *, profile_id: str | None = None) -> bool:
    return permission in principal.permissions and (profile_id is None or profile_id in principal.profiles)


def require_permission(principal: Principal, permission: d.Permission, *, profile_id: str | None = None) -> None:
    if not allows(principal, permission, profile_id=profile_id):
        raise TokenError("E_PERMISSION_DENIED")


def bearer_from_headers(headers: Sequence[tuple[str, str]], *, query_string: bytes = b"") -> str:
    # Inspect names only. Query values never leave this function or enter errors.
    for field in query_string.decode("latin1").split("&"):
        if unquote_plus(field.partition("=")[0]).lower() in {
            "token",
            "access_token",
            "authorization",
            "bearer",
            "secret",
        }:
            raise TokenError("E_VALIDATION", status=400)
    values = [value for name, value in headers if name.lower() == "authorization"]
    if len(values) != 1:
        raise TokenError("E_AUTH_REQUIRED")
    scheme, separator, token = values[0].partition(" ")
    if scheme.lower() != "bearer" or not separator or not token or any(c.isspace() for c in token):
        raise TokenError("E_AUTH_REQUIRED")
    return token
