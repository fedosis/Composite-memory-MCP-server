"""Signed, filter-bound keyset cursor for the unmounted S10A outbox foundation."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Final
from uuid import UUID

from pydantic import ValidationError
from starlette.requests import Request
from storage.outbox import OutboxKey

from memory_server.application.graph_read_service import wire_json
from memory_server.application.memory_read_service import b64, unb64, utc
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.catalog import CATALOG
from memory_server.gateway_contracts.ports import Principal, RequestContext
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.guards import ReadRouteSpec


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


Parser = Callable[[Request, bytes, Principal], d.DiagnosticsCommand]
ResultValidator = Callable[[d.DiagnosticsResult], d.DiagnosticsResult]


@dataclass(frozen=True)
class DiagnosticsRouteSpec(ReadRouteSpec):
    parse: Parser
    validate_result: ResultValidator
    cursor_codec: OutboxCursorCodec | None = None


def _command(operation: str, request: Request, payload: d.DTO) -> d.DiagnosticsCommand:
    return d.COMMAND_TYPES[operation](
        operation=operation, profile_id=request.path_params["profile"], path_ids={}, payload=payload
    )


def _get(operation: str, model: type[d.DTO], request: Request, body: bytes, principal: Principal):
    if body:
        raise HttpError("E_VALIDATION")
    values: dict[str, object] = {}
    try:
        for key, value in request.query_params.multi_items():
            if key not in model.model_fields or key in values:
                raise ValueError
            if key == "limit":
                if not re.fullmatch(r"[0-9]{1,3}", value):
                    raise ValueError
                values[key] = int(value)
            else:
                values[key] = value
        if model is d.Empty and values:
            raise ValueError
        if model is d.AuditQuery:
            values.setdefault("cursor", None)
        if model is d.OutboxQuery:
            for key in ("cursor", "status", "memory_id"):
                values.setdefault(key, None)
        payload = model.model_validate(values, strict=True)
        if model is d.AuditQuery and payload.cursor is not None:
            raise HttpError("E_CURSOR_INVALID")
        if model is d.OutboxQuery and payload.memory_id is not None:
            value = payload.memory_id
            if not value or len(value.encode()) > 1024 or value.startswith(("rel_", "ent_", "proj_")):
                raise ValueError
        command = _command(operation, request, payload)
        if model is d.OutboxQuery and payload.cursor is not None:
            codec = request.scope["s06"]["spec"].cursor_codec
            codec.validate(principal, command, now=codec.clock())
        return command
    except HttpError:
        raise
    except (ValidationError, ValueError, UnicodeError):
        raise HttpError("E_VALIDATION") from None


def parse_audit(request, body, principal):
    return _get("audit", d.AuditQuery, request, body, principal)


def parse_indexes_status(request, body, principal):
    return _get("indexes.status", d.Empty, request, body, principal)


def parse_outbox_list(request, body, principal):
    return _get("outbox.list", d.OutboxQuery, request, body, principal)


def parse_overview(request, body, principal):
    return _get("overview", d.Empty, request, body, principal)


def parse_reflect(request: Request, body: bytes, principal: Principal):
    del principal
    if request.query_params:
        raise HttpError("E_VALIDATION")
    if request.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json":
        raise HttpError("E_UNSUPPORTED_MEDIA_TYPE")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError
            result[key] = value
        return result

    try:
        values = json.loads(body, object_pairs_hook=pairs, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if not {"topic", "min_confidence", "context"} <= set(values):
            raise ValueError
        payload = d.ReflectRequest.model_validate(values, strict=True)
        return _command("reflect", request, payload)
    except (ValidationError, ValueError, TypeError, UnicodeError, RecursionError):
        raise HttpError("E_VALIDATION") from None


_REFLECT_KEYS = {
    "overview": {
        "sampled_at",
        "selected_count",
        "total_beliefs",
        "lifecycle_counts",
        "confidence",
        "contradiction_count",
        "stale_count",
        "decaying_next_7d",
        "no_evidence_count",
        "evidence_stats_status",
    },
    "contradictions": {"sampled_at", "selected_count", "total", "pairs"},
    "decay": {"sampled_at", "selected_count", "stale_now", "stale_7d", "archived_7d", "forgotten_7d"},
    "topics": {"sampled_at", "selected_count", "topic_count", "untagged_count"},
    "evidence_audit": {
        "sampled_at",
        "selected_count",
        "total",
        "with_evidence",
        "without_evidence",
        "avg_evidence_per_belief",
        "zero_weight_entries",
        "evidence_stats_status",
    },
    "confidence": {"sampled_at", "selected_count", "histogram", "lowest_count", "beliefs"},
    "relations": {"sampled_at", "selected_count", "total", "contradictions", "entailments", "neutral_pairs", "pairs"},
}
_ERROR_CODES = {
    "E_TIMEOUT",
    "E_STORAGE",
    "E_VECTOR_BACKEND",
    "E_GRAPH_BACKEND",
    "E_PROJECTION_UNAVAILABLE",
    "E_CAPABILITY_UNAVAILABLE",
}


def validate_audit_result(value):
    result = d.AuditResult.model_validate(value.model_dump(), strict=True)
    if not set(result.sections) <= {
        "lifecycle",
        "receipts",
        "relations",
        "indexes",
        "confidence",
        "consistency",
        "graph_orphans",
    }:
        raise HttpError("E_INTERNAL")
    return result


def validate_index_status(value):
    return d.IndexStatus.model_validate(value.model_dump(), strict=True)


def validate_outbox_page(value):
    result = d.Page[d.OutboxItem].model_validate(value.model_dump(), strict=True)
    if any(item.last_error_code is not None and item.last_error_code not in _ERROR_CODES for item in result.items):
        raise HttpError("E_INTERNAL")
    return result


def validate_overview(value):
    return d.Overview.model_validate(value.model_dump(), strict=True)


def validate_reflect_result(value):
    result = d.ReflectResult.model_validate(value.model_dump(), strict=True)
    if result.mode not in _REFLECT_KEYS or set(result.sections) != _REFLECT_KEYS[result.mode]:
        raise HttpError("E_INTERNAL")
    forbidden = {
        "proposition",
        "proposition_a",
        "proposition_b",
        "tag",
        "recommendation",
        "by_topics",
        "by_tag_stale",
        "by_source_type",
        "evidence_stats_error",
        "detection_method",
    }

    def walk(item):
        if isinstance(item, dict):
            if forbidden & set(item):
                raise HttpError("E_INTERNAL")
            for nested in item.values():
                walk(nested)
        elif isinstance(item, list):
            for nested in item:
                walk(nested)

    walk(result.sections)
    return result


DIAGNOSTICS_ROUTE_SPECS: Final = MappingProxyType(
    {
        operation: DiagnosticsRouteSpec(
            operation,
            CATALOG.by_operation[operation].method,
            CATALOG.by_operation[operation].route,
            CATALOG.by_operation[operation].permission,
            parser,
            validator,
        )
        for operation, parser, validator in (
            ("audit", parse_audit, validate_audit_result),
            ("indexes.status", parse_indexes_status, validate_index_status),
            ("outbox.list", parse_outbox_list, validate_outbox_page),
            ("overview", parse_overview, validate_overview),
            ("reflect", parse_reflect, validate_reflect_result),
        )
    }
)


def create_diagnostics_routes(*, cursor_codec: OutboxCursorCodec) -> tuple[DiagnosticsRouteSpec, ...]:
    return tuple(
        replace(spec, cursor_codec=cursor_codec) if operation == "outbox.list" else spec
        for operation, spec in DIAGNOSTICS_ROUTE_SPECS.items()
    )
