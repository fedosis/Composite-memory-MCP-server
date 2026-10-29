"""Six catalog-checked S07 descriptors; permission-first strict preflight."""

from __future__ import annotations

import binascii
import hmac
import json
import os
import stat
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path

from pydantic import ValidationError

from memory_server.application.memory_read_service import (
    HistoryKey,
    ReadKey,
    b64,
    canonical,
    filter_digest,
    key_order,
    unb64,
    utc,
)
from memory_server.control.policy import require_permission
from memory_server.gateway_contracts import dto as d
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.guards import ReadRouteSpec

__all__ = ["CursorCodec", "create_memory_read_routes"]


class CursorCodec:
    """Independent MAC key; validate syntax/binding/order before genuine expiry."""

    def __init__(self, key: bytes, ttl_seconds=900, *, clock=None):
        if type(key) is not bytes or len(key) != 32 or type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 900:
            raise ValueError("invalid cursor configuration")
        self._key, self.ttl = key, ttl_seconds
        self.clock = clock or (lambda: datetime.now(UTC))

    @classmethod
    def from_path(cls, path: Path, ttl_seconds=900):
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
                info, key = os.fstat(fd), os.read(fd, 33)
                if not (
                    stat.S_ISREG(info.st_mode)
                    and info.st_nlink == 1
                    and info.st_uid == os.geteuid()
                    and stat.S_IMODE(info.st_mode) == 0o600
                    and len(key) == 32
                ):
                    raise OSError("unsafe custody")
            finally:
                os.close(fd)
        except OSError:
            raise HttpError("E_STORAGE") from None
        return cls(key, ttl_seconds)

    def issue(self, ctx, command, *, last, upper, limit, now):
        def wire(key):
            return [key.created_at, key.memory_type, key.id] if isinstance(key, ReadKey) else [key.timestamp, key.id]

        payload = dict(
            v=1,
            profile_id=ctx.profile_id,
            subject=ctx.principal.subject,
            operation=command.operation,
            filter_sha256=filter_digest(command),
            last=wire(last),
            upper=wire(upper),
            limit=limit,
            issued_at=now.astimezone(UTC).isoformat(),
            expires_at=(now + timedelta(seconds=self.ttl)).astimezone(UTC).isoformat(),
        )
        body = b64(canonical(payload))
        return body + "." + b64(hmac.digest(self._key, body.encode(), "sha256"))

    def validate(self, principal, command, token=None, *, now):
        token = command.payload.cursor if token is None else token
        try:
            if not isinstance(token, str) or len(token) > 8192:
                raise ValueError
            body, sig = token.split(".")
            raw, signature = unb64(body), unb64(sig)
            if not hmac.compare_digest(hmac.digest(self._key, body.encode(), "sha256"), signature):
                raise ValueError
            data = json.loads(raw)
            if canonical(data) != raw or set(data) != {
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
            }:
                raise ValueError
            if type(data["v"]) is not int or data["v"] != 1:
                raise ValueError
            if (data["profile_id"], data["subject"], data["operation"], data["filter_sha256"]) != (
                command.profile_id,
                principal.subject,
                command.operation,
                filter_digest(command),
            ):
                raise ValueError
            if type(data["limit"]) is not int or not command.payload.limit <= data["limit"] <= 200:
                raise ValueError

            def parse(value):
                if type(value) is not list:
                    raise ValueError
                if command.operation == "memories.list":
                    if len(value) != 3 or not all(type(v) is str for v in value):
                        raise ValueError
                    if value[1] not in ("fact", "belief", "decision", "skill") or not value[2]:
                        raise ValueError
                    if utc(value[0]).isoformat(timespec="microseconds") != value[0]:
                        raise ValueError
                    return ReadKey(*value)
                if command.operation != "history.list" or len(value) != 2 or type(value[1]) is not str:
                    raise ValueError
                if value[0] is not None and (
                    type(value[0]) is not str or utc(value[0]).isoformat(timespec="microseconds") != value[0]
                ):
                    raise ValueError
                ordinal_raw = unb64(value[1])
                ordinal = json.loads(ordinal_raw)
                if (
                    canonical(ordinal) != ordinal_raw
                    or type(ordinal) is not list
                    or len(ordinal) != 3
                    or ordinal[0] not in ("receipt", "lifecycle")
                    or type(ordinal[1]) is not str
                    or not ordinal[1]
                    or type(ordinal[2]) is not int
                    or ordinal[2] < 0
                    or (ordinal[0] == "lifecycle" and ordinal[2] != 0)
                ):
                    raise ValueError
                return HistoryKey(*value)

            last, upper = parse(data["last"]), parse(data["upper"])
            if key_order(last) > key_order(upper):
                raise ValueError
            issued, expiry = utc(data["issued_at"]), utc(data["expires_at"])
            if (
                issued.isoformat() != data["issued_at"]
                or expiry.isoformat() != data["expires_at"]
                or issued > now
                or not timedelta(0) < expiry - issued <= timedelta(seconds=self.ttl)
            ):
                raise ValueError
        except (ValueError, KeyError, TypeError, AttributeError, OverflowError, binascii.Error):
            raise HttpError("E_CURSOR_INVALID") from None
        if now >= expiry:
            raise HttpError("E_CURSOR_EXPIRED")
        return {"last": last, "upper": upper, "limit": data["limit"]}


def _parse(operation, codec, request, body, principal):
    values = {}
    model = {
        "memories.list": d.MemoryListQuery,
        "history.list": d.PageQuery,
        "lookup": d.LookupQuery,
        "memories.reveal": d.RevealRequest,
    }.get(operation, d.Empty)
    try:
        if operation == "memories.reveal":
            if request.query_params:
                raise HttpError("E_VALIDATION")
            if request.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json":
                raise HttpError("E_UNSUPPORTED_MEDIA_TYPE")
            payload = model.model_validate_json(body)
        else:
            if body:
                raise HttpError("E_VALIDATION")
            lists = {"memory_types", "lifecycle_states", "tags", "risk_tags"}
            for key, value in request.query_params.multi_items():
                if key not in model.model_fields or (key in values and key not in lists):
                    raise HttpError("E_VALIDATION")
                if key in lists:
                    values.setdefault(key, []).append(value)
                elif key == "limit":
                    if not value.isascii() or not value.isdecimal() or len(value) > 3:
                        raise HttpError("E_VALIDATION")
                    values[key] = int(value)
                elif key in ("min_confidence", "max_confidence"):
                    values[key] = float(value)
                else:
                    values[key] = value
            if operation == "memories.list":
                for name in (
                    "cursor",
                    "source",
                    "authority_level",
                    "epistemic_status",
                    "min_confidence",
                    "max_confidence",
                    "created_from",
                    "created_to",
                    "updated_from",
                    "updated_to",
                    "valid_at",
                ):
                    values.setdefault(name, None)
            # JSON validation parses UTC datetime strings; ints were explicitly parsed above.
            payload = model.model_validate_json(json.dumps(values))
        paths = {"id": request.path_params["id"]} if "id" in request.path_params else {}
        command = d.COMMAND_TYPES[operation](
            operation=operation, profile_id=request.path_params["profile"], path_ids=paths, payload=payload
        )
    except (ValidationError, ValueError, TypeError, OverflowError):
        raise HttpError("E_VALIDATION") from None
    if operation == "memories.list":
        if any(getattr(payload, key) for key in ("source", "tags", "risk_tags", "authority_level", "epistemic_status")):
            raise HttpError("E_PERMISSION_DENIED")
        if (
            payload.min_confidence is not None
            and payload.max_confidence is not None
            and payload.min_confidence > payload.max_confidence
        ):
            raise HttpError("E_VALIDATION")
        for prefix in ("created", "updated"):
            lo, hi = getattr(payload, prefix + "_from"), getattr(payload, prefix + "_to")
            if lo is not None and hi is not None and lo > hi:
                raise HttpError("E_VALIDATION")
    if operation == "lookup" and payload.kind in ("request", "relation"):
        require_permission(
            principal, "audit.read" if payload.kind == "request" else "relation.read", profile_id=command.profile_id
        )
    if getattr(payload, "cursor", None):
        codec.validate(principal, command, now=codec.clock())
    return command


def _result(model, value):
    return model.model_validate(value.model_dump())


def create_memory_read_routes(port, guard, *, cursor_codec):
    """Bind codec explicitly; execution still uses dependencies.ports exclusively."""
    port.cursor_codec = cursor_codec
    specs = (
        ("memories.list", "GET", "/api/v1/profiles/{profile}/memories", "memory.read", d.Page[d.Memory]),
        ("memories.get", "GET", "/api/v1/profiles/{profile}/memories/{id}", "memory.read", d.Memory),
        ("receipts.get", "GET", "/api/v1/profiles/{profile}/memories/{id}/receipt", "memory.read", d.Receipt),
        (
            "history.list",
            "GET",
            "/api/v1/profiles/{profile}/memories/{id}/history",
            "memory.read",
            d.Page[d.HistoryEvent],
        ),
        ("lookup", "GET", "/api/v1/profiles/{profile}/lookup", "memory.read", d.LookupResult),
        ("memories.reveal", "POST", "/api/v1/profiles/{profile}/memories/{id}/reveal", "memory.reveal", d.Memory),
    )
    return tuple(
        ReadRouteSpec(op, method, path, permission, partial(_parse, op, cursor_codec), partial(_result, result))
        for op, method, path, permission, result in specs
    )
