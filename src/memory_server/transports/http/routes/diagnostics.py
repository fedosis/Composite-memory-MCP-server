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
            expiry = (
                min(now + timedelta(seconds=self.ttl), utc(expires_at))
                if expires_at
                else now + timedelta(seconds=self.ttl)
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


_ERROR_CODES = {
    "E_TIMEOUT",
    "E_STORAGE",
    "E_VECTOR_BACKEND",
    "E_GRAPH_BACKEND",
    "E_PROJECTION_UNAVAILABLE",
    "E_CAPABILITY_UNAVAILABLE",
}
_AUDIT_RESULT_KEYS = {"findings", "sections", "sampled_at", "complete"}
_AUDIT_PAGE_KEYS = {"items", "next_cursor", "has_more", "request_id"}
_AUDIT_FINDING_KEYS = {"id", "category", "severity", "object_kind", "object_id", "code", "aggregate"}
_AUDIT_SECTIONS = {"lifecycle", "receipts", "relations", "indexes", "confidence", "consistency", "graph_orphans"}
_OUTBOX_PAGE_KEYS = {"items", "next_cursor", "has_more", "request_id"}
_OUTBOX_ITEM_KEYS = {
    "event_id",
    "memory_id",
    "record_type",
    "operation",
    "status",
    "attempts",
    "target_backend",
    "last_error_code",
    "next_retry",
    "created_at",
    "processed_at",
}
_OUTBOX_OPERATIONS = {"index_fact", "index_belief", "index_decision", "index_skill"}


def _canonical_uuid(value):
    try:
        return type(value) is str and str(UUID(value)) == value
    except ValueError:
        return False


def validate_audit_result(value):
    raw = value.model_dump()
    if not isinstance(raw, dict):
        raise HttpError("E_INTERNAL")
    findings = raw.get("findings", {})
    if (
        set(raw) != _AUDIT_RESULT_KEYS
        or not isinstance(findings, dict)
        or set(findings) != _AUDIT_PAGE_KEYS
        or not isinstance(findings.get("items"), list)
        or any(not isinstance(item, dict) or set(item) != _AUDIT_FINDING_KEYS for item in findings["items"])
        or not isinstance(raw.get("sections"), dict)
        or not set(raw["sections"]) <= _AUDIT_SECTIONS
    ):
        raise HttpError("E_INTERNAL")
    try:
        result = d.AuditResult.model_validate(raw, strict=True)
    except ValidationError:
        raise HttpError("E_INTERNAL") from None
    errors = {"STATE_METADATA_INVALID", "CONFIDENCE_INVALID", "RELATION_ENDPOINT_INVALID", "RELATION_TYPE_INVALID"}
    warnings = {
        "RECEIPT_STATE_MISMATCH",
        "RECEIPT_MISSING",
        "RECEIPT_TYPE_MISMATCH",
        "RECEIPT_ORPHAN",
        "CONFIDENCE_LOW",
        "CONFIDENCE_STATE_MISMATCH",
        "VECTOR_COUNT_MISMATCH",
        "OUTBOX_FAILED",
        "OUTBOX_STATUS_INVALID",
    }
    if (
        any(state not in {"ok", "warning", "unavailable"} for state in result.sections.values())
        or result.findings.has_more
        or result.findings.next_cursor is not None
        or any(
            item.category not in _AUDIT_SECTIONS - {"graph_orphans"}
            or item.code not in errors | warnings
            or item.severity != ("error" if item.code in errors else "warning")
            or not re.fullmatch(
                r"s10:(?:" + "|".join(sorted(_AUDIT_SECTIONS)) + "):" + item.code + r":[1-9][0-9]*", item.id
            )
            or (item.aggregate and (item.object_kind is not None or item.object_id is not None))
            or (not item.aggregate and (item.object_kind is None or not _canonical_uuid(item.object_id)))
            for item in result.findings.items
        )
    ):
        raise HttpError("E_INTERNAL")
    return result


def validate_outbox_page(value):
    raw = value.model_dump()
    if (
        not isinstance(raw, dict)
        or set(raw) != _OUTBOX_PAGE_KEYS
        or not isinstance(raw.get("items"), list)
        or any(not isinstance(item, dict) or set(item) != _OUTBOX_ITEM_KEYS for item in raw["items"])
    ):
        raise HttpError("E_INTERNAL")
    try:
        result = d.Page[d.OutboxItem].model_validate(raw, strict=True)
    except ValidationError:
        raise HttpError("E_INTERNAL") from None
    if any(
        item.operation not in _OUTBOX_OPERATIONS
        or not _canonical_uuid(item.event_id)
        or not _canonical_uuid(item.memory_id)
        or item.target_backend is not None
        or item.next_retry is not None
        or (item.last_error_code is not None and item.last_error_code not in _ERROR_CODES)
        for item in result.items
    ):
        raise HttpError("E_INTERNAL")
    return result


def validate_index_status(value):
    try:
        result = d.IndexStatus.model_validate(value.model_dump(), strict=True)
    except ValidationError:
        raise HttpError("E_INTERNAL") from None
    raw = value.model_dump()
    if set(raw) != {
        "sqlite_records",
        "vector_records",
        "graph_relations",
        "vector_lag",
        "graph_lag",
        "outbox_pending",
        "outbox_failed",
        "graph_consistency",
        "last_success",
        "sampled_at",
    }:
        raise HttpError("E_INTERNAL")
    graph = raw["graph_consistency"]
    if type(graph) is not dict or set(graph) != set(d.GraphConsistency.model_fields):
        raise HttpError("E_INTERNAL")
    if (
        graph["projection"] not in {"unknown", "unavailable"}
        or any(graph[key] is not None for key in set(graph) - {"canonical_store", "projection"})
        or any(raw[key] is not None for key in ("graph_relations", "graph_lag", "vector_lag", "last_success"))
    ):
        raise HttpError("E_INTERNAL")
    for key in ("sqlite_records", "vector_records", "outbox_pending", "outbox_failed"):
        if raw[key] is not None and (type(raw[key]) is not int or raw[key] < 0):
            raise HttpError("E_INTERNAL")
    return result


def validate_overview(value):
    try:
        result = d.Overview.model_validate(value.model_dump(), strict=True)
    except ValidationError:
        raise HttpError("E_INTERNAL") from None
    raw = value.model_dump()
    if set(raw) != {
        "profile",
        "facts",
        "beliefs",
        "decisions",
        "skills",
        "lifecycle_counts",
        "unresolved_conflicts",
        "audit_warnings",
        "indexes",
        "schema_revision",
        "last_migration",
        "writer_owned",
    }:
        raise HttpError("E_INTERNAL")
    if set(raw["lifecycle_counts"]) != {
        "candidate",
        "validated",
        "active",
        "stale",
        "archived",
        "forgotten",
        "superseded",
        "contradicted",
        "discarded",
        "unknown",
    }:
        raise HttpError("E_INTERNAL")
    profile = raw["profile"]
    if set(profile) != set(d.Profile.model_fields):
        raise HttpError("E_INTERNAL")
    if profile.get("storage_root") is not None:
        raise HttpError("E_INTERNAL")
    index_raw = raw["indexes"]
    if (
        type(index_raw) is not dict
        or set(index_raw) != set(d.IndexStatus.model_fields)
        or type(index_raw["graph_consistency"]) is not dict
        or set(index_raw["graph_consistency"]) != set(d.GraphConsistency.model_fields)
    ):
        raise HttpError("E_INTERNAL")
    validate_index_status(result.indexes)
    if result.audit_warnings is not None or result.last_migration is not None:
        raise HttpError("E_INTERNAL")
    if any(
        type(count) is not int or count < 0
        for count in (
            result.facts,
            result.beliefs,
            result.decisions,
            result.skills,
            result.unresolved_conflicts,
            *raw["lifecycle_counts"].values(),
        )
    ):
        raise HttpError("E_INTERNAL")
    return result


def validate_reflect_result(value):
    from memory_server.application.diagnostics_service import validate_reflect_sections

    try:
        raw = value.model_dump()
        if set(raw) != set(d.ReflectResult.model_fields):
            raise HttpError("E_INTERNAL")
        result = d.ReflectResult.model_validate(raw, strict=True)
        validate_reflect_sections(result.mode, result.sections)
        return result
    except (ValidationError, ValueError, TypeError, AttributeError, KeyError):
        raise HttpError("E_INTERNAL") from None


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
