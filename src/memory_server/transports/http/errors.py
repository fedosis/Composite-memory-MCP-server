"""Sanitized HTTP error mapping and the C5 response envelope."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from starlette.responses import JSONResponse

_DEFAULT_STATUS: dict[str, int] = {
    "E_AUTH_REQUIRED": 401,
    "E_PERMISSION_DENIED": 403,
    "E_PROFILE_NOT_FOUND": 404,
    "E_PROFILE_DISABLED": 409,
    "E_WRITER_ACTIVE": 409,
    "E_VERSION_CONFLICT": 409,
    "E_VALIDATION": 422,
    "E_ADMISSION_REJECTED": 422,
    "E_NOT_FOUND": 404,
    "E_STORAGE": 503,
    "E_VECTOR_BACKEND": 503,
    "E_GRAPH_BACKEND": 503,
    "E_OUTBOX": 503,
    "E_RATE_LIMIT": 429,
    "E_REQUEST_TOO_LARGE": 413,
    "E_IDEMPOTENCY_REQUIRED": 400,
    "E_IDEMPOTENCY_CONFLICT": 409,
    "E_REQUEST_IN_PROGRESS": 409,
    "E_PRECONDITION_REQUIRED": 428,
    "E_CURSOR_INVALID": 400,
    "E_CURSOR_EXPIRED": 410,
    "E_CAPABILITY_UNAVAILABLE": 503,
    "E_RUNTIME_NOT_READY": 503,
    "E_RUNTIME_CLOSING": 503,
    "E_RUNTIME_FAILED": 503,
    "E_PROFILE_BUSY": 409,
    "E_AUDIT_UNAVAILABLE": 503,
    "E_TIMEOUT": 504,
    "E_CSRF": 403,
    "E_UNSUPPORTED_MEDIA_TYPE": 415,
    "E_METHOD_NOT_ALLOWED": 405,
    "E_INTERNAL": 500,
    "E_PROJECTION_UNAVAILABLE": 503,
    "E_MAINTENANCE_CONFLICT": 409,
    "E_JOB_INTERRUPTED": 409,
    "E_EVENT_CURSOR_EXPIRED": 410,
}

_MESSAGES: dict[str, str] = {
    "E_VERSION_CONFLICT": "Version conflict",
    "E_INTERNAL": "Internal server error",
    "E_AUTH_REQUIRED": "Authentication required",
    "E_PERMISSION_DENIED": "Permission denied",
    "E_PROFILE_NOT_FOUND": "Profile not found",
    "E_PROFILE_DISABLED": "Profile disabled",
    "E_VALIDATION": "Invalid request",
    "E_NOT_FOUND": "Not found",
    "E_RATE_LIMIT": "Rate limit exceeded",
    "E_REQUEST_TOO_LARGE": "Request too large",
    "E_TIMEOUT": "Request timed out",
    "E_METHOD_NOT_ALLOWED": "Method not allowed",
    "E_UNSUPPORTED_MEDIA_TYPE": "Unsupported media type",
    "E_CAPABILITY_UNAVAILABLE": "Capability unavailable",
    "E_AUDIT_UNAVAILABLE": "Audit unavailable",
}


class HttpError(Exception):
    """A typed, sanitized error intended for the HTTP boundary."""

    def __init__(
        self,
        code: str,
        status: int | None = None,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        self.code = code if code in _DEFAULT_STATUS else "E_INTERNAL"
        self.status = status
        self.details = _safe_details(details)
        super().__init__(self.code)


def _safe_details(details: Mapping[str, Any] | None) -> dict[str, Any]:
    if not details:
        return {}
    allowed = {"expected", "actual", "limit", "retry_after", "id", "revision"}
    result: dict[str, Any] = {}
    for key, value in details.items():
        if key not in allowed or type(value) not in (str, int, float, bool) or isinstance(value, bool):
            continue
        if isinstance(value, str) and len(value) > 256:
            continue
        result[key] = value
    return result


def error_code(exc: BaseException) -> str:
    """Map known typed failures without exposing exception text."""
    code = getattr(exc, "code", None)
    if code == "E_APPLICATION_INVALID_COMMAND":
        return "E_VALIDATION"
    if code == "E_APPLICATION_UNKNOWN_DEPENDENCY":
        return "E_CAPABILITY_UNAVAILABLE"
    if isinstance(code, str) and code in _DEFAULT_STATUS:
        return code
    if code in {"E_LOCK_TIMEOUT", "E_LOCK_BUSY"}:
        return "E_WRITER_ACTIVE"
    if code in {"E_LOCK_RELEASE_UNCERTAIN", "E_RUNTIME_CLEANUP_FAILED"}:
        return "E_STORAGE"
    name = type(exc).__name__
    if name == "InvalidCommandError":
        return "E_VALIDATION"
    if name == "UnknownDependencyError":
        return "E_CAPABILITY_UNAVAILABLE"
    if name == "LifecycleConflictError":
        return "E_VERSION_CONFLICT"
    if name == "ProjectionUnavailable":
        return "E_PROJECTION_UNAVAILABLE"
    if name == "StorageOwnershipError":
        cause = getattr(exc, "cause", None)
        if getattr(cause, "code", None) in {"E_LOCK_TIMEOUT", "E_LOCK_BUSY"}:
            return "E_WRITER_ACTIVE"
        return "E_STORAGE"
    if isinstance(exc, KeyError):
        return "E_NOT_FOUND"
    return "E_INTERNAL"


def error_response(code: str | HttpError, request_id: str, status: int | None = None) -> JSONResponse:
    """Build the exact C5 JSON envelope with a generic message."""
    details: dict[str, Any] = {}
    if isinstance(code, HttpError):
        details = code.details
        status = status if status is not None else code.status
        code = code.code
    safe_code = code if code in _DEFAULT_STATUS else "E_INTERNAL"
    body: dict[str, Any] = {
        "error": {
            "code": safe_code,
            "message": _MESSAGES.get(safe_code, "Request failed"),
            "request_id": request_id,
            "details": details,
        }
    }
    return JSONResponse(body, status_code=status if status is not None else _DEFAULT_STATUS[safe_code])
