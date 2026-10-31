"""Closed HTTP descriptors and signed cursors for retrieval traces."""

from __future__ import annotations

import base64
import hmac
import json
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Final

from pydantic import ValidationError

from memory_server.application.memory_read_service import canonical, utc
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.catalog import CATALOG
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.guards import ReadRouteSpec


def _b64(v):
    return base64.urlsafe_b64encode(v).decode().rstrip("=")


def _unb64(v):
    raw = base64.b64decode(v + "=" * (-len(v) % 4), altchars=b"-_", validate=True)
    if _b64(raw) != v:
        raise ValueError
    return raw


def validate_trace_id(v):
    if type(v) is not str or not 1 <= len(v) <= 128 or not v.isprintable() or any(c in v for c in "%/\x00"):
        raise HttpError("E_VALIDATION")


class TraceCursorCodec:
    def __init__(self, key, ttl_seconds=900, clock=None):
        if type(key) is not bytes or len(key) != 32 or type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 900:
            raise ValueError
        self._key = key
        self.ttl = ttl_seconds
        self.clock = clock or (lambda: datetime.now(UTC))

    @classmethod
    def from_path(cls, path, ttl_seconds=900):
        path = Path(path)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
            except FileExistsError:
                pass
            else:
                try:
                    if os.write(fd, os.urandom(32)) != 32:
                        raise OSError("short write")
                    os.fsync(fd)
                finally:
                    os.close(fd)
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                st = os.fstat(fd)
                key = os.read(fd, 33)
                if not (
                    stat.S_ISREG(st.st_mode)
                    and st.st_nlink == 1
                    and st.st_uid == os.geteuid()
                    and stat.S_IMODE(st.st_mode) == 0o600
                    and len(key) == 32
                ):
                    raise OSError
            finally:
                os.close(fd)
        except OSError:
            raise HttpError("E_STORAGE") from None
        return cls(key, ttl_seconds)

    def issue(self, ctx, command, *, last, limit, now):
        payload = {
            "v": 1,
            "profile_id": ctx.profile_id,
            "subject": ctx.principal.subject,
            "operation": command.operation,
            "last": list(last),
            "limit": limit,
            "issued_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=self.ttl)).isoformat(),
        }
        body = _b64(canonical(payload))
        return body + "." + _b64(hmac.digest(self._key, body.encode(), "sha256"))

    def validate(self, principal, command, *, now):
        try:
            if type(command.payload.cursor) is not str or len(command.payload.cursor) > 8192:
                raise ValueError
            body, sig = command.payload.cursor.split(".")
            raw = _unb64(body)
            data = json.loads(raw)
            if (
                not hmac.compare_digest(hmac.digest(self._key, body.encode(), "sha256"), _unb64(sig))
                or canonical(data) != raw
                or set(data) != {"v", "profile_id", "subject", "operation", "last", "limit", "issued_at", "expires_at"}
            ):
                raise ValueError
            if type(data["v"]) is not int or (data["v"], data["profile_id"], data["subject"], data["operation"]) != (
                1,
                command.profile_id,
                principal.subject,
                command.operation,
            ):
                raise ValueError
            if (
                type(data["limit"]) is not int
                or not command.payload.limit <= data["limit"] <= 200
                or type(data["last"]) is not list
                or len(data["last"]) != 2
            ):
                raise ValueError
            if not all(type(v) is str for v in data["last"]):
                raise ValueError
            validate_trace_id(data["last"][1])
            if utc(data["last"][0]).isoformat() != data["last"][0]:
                raise ValueError
            expiry = utc(data["expires_at"])
            issued = utc(data["issued_at"])
            if (
                issued.isoformat() != data["issued_at"]
                or expiry.isoformat() != data["expires_at"]
                or issued > now
                or not timedelta(0) < expiry - issued <= timedelta(seconds=self.ttl)
            ):
                raise ValueError
        except Exception:
            raise HttpError("E_CURSOR_INVALID") from None
        if now >= expiry:
            raise HttpError("E_CURSOR_EXPIRED")
        return {"last": tuple(data["last"]), "limit": data["limit"]}


@dataclass(frozen=True)
class TraceRouteSpec(ReadRouteSpec):
    cursor_codec: TraceCursorCodec | None = None


def _parse_get(request, body, principal):
    if body or request.query_params:
        raise HttpError("E_VALIDATION")
    validate_trace_id(request.path_params["id"])
    return d.TracesGetCommand(
        operation="traces.get",
        profile_id=request.path_params["profile"],
        path_ids={"id": request.path_params["id"]},
        payload=d.Empty(),
    )


def _parse_list(request, body, principal):
    if body:
        raise HttpError("E_VALIDATION")
    values = {}
    for key, val in request.query_params.multi_items():
        if key not in {"limit", "cursor"} or key in values:
            raise HttpError("E_VALIDATION")
        if key == "limit" and (not val.isascii() or not val.isdecimal() or len(val) > 3):
            raise HttpError("E_VALIDATION")
        values[key] = int(val) if key == "limit" else val
    try:
        payload = d.PageQuery.model_validate(values, strict=True)
    except (ValidationError, ValueError, TypeError):
        raise HttpError("E_VALIDATION") from None
    return d.TracesListCommand(
        operation="traces.list", profile_id=request.path_params["profile"], path_ids={}, payload=payload
    )


def _validate_trace(value):
    try:
        raw = value.model_dump()
        result = d.Trace.model_validate(raw, strict=True)
        if (
            set(raw)
            != {
                "id",
                "request_id",
                "profile_id",
                "operation",
                "query",
                "stages",
                "stage_status",
                "merge",
                "deduplicated",
                "final",
                "created_at",
                "expires_at",
                "settings_digest",
                "code_revision",
            }
            or result.query is not None
        ):
            raise ValueError
        if any(
            k not in {"fts", "semantic", "graph", "context"} for k in set(result.stages) | set(result.stage_status)
        ) or any(k not in {"ran", "skipped", "failed"} for k in result.stage_status.values()):
            raise ValueError
        if any(
            c.matched_terms or c.relation_path or c.source not in {"fts", "semantic", "graph"}
            for seq in list(result.stages.values()) + [result.merge, result.deduplicated, result.final]
            for c in seq
        ):
            raise ValueError
        return result
    except Exception:
        raise HttpError("E_INTERNAL") from None


def _validate_page(value):
    try:
        page = d.Page[d.TraceSummary].model_validate(value.model_dump(), strict=True)
        if len(page.items) > 200 or page.has_more != (page.next_cursor is not None):
            raise ValueError
        for item in page.items:
            validate_trace_id(item.id)
            if (
                item.created_at > item.expires_at
                or len(item.settings_digest) != 64
                or any(c not in "0123456789abcdef" for c in item.settings_digest)
            ):
                raise ValueError
        return page
    except (ValidationError, ValueError, TypeError, AttributeError, HttpError):
        raise HttpError("E_INTERNAL") from None


TRACE_ROUTE_SPECS: Final[Mapping[str, TraceRouteSpec]] = MappingProxyType(
    {
        "traces.get": TraceRouteSpec(
            "traces.get",
            CATALOG.by_operation["traces.get"].method,
            CATALOG.by_operation["traces.get"].route,
            "memory.read",
            _parse_get,
            _validate_trace,
        ),
        "traces.list": TraceRouteSpec(
            "traces.list",
            CATALOG.by_operation["traces.list"].method,
            CATALOG.by_operation["traces.list"].route,
            "memory.read",
            _parse_list,
            _validate_page,
        ),
    }
)


def create_trace_routes(*, cursor_codec):
    return tuple(
        replace(s, cursor_codec=cursor_codec) if op == "traces.list" else s for op, s in TRACE_ROUTE_SPECS.items()
    )
