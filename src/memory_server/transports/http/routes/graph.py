"""Closed S09 descriptors and independently signed relation cursors."""

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

from pydantic import ValidationError
from starlette.requests import Request
from storage.repositories.relation_repo import RelationKey

from memory_server.application.graph_read_service import decode_relation_id, validate_memory_id, wire_json
from memory_server.application.memory_read_service import b64, unb64, utc
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.catalog import CATALOG
from memory_server.gateway_contracts.ports import Principal, RequestContext
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.guards import ReadRouteSpec


@dataclass(frozen=True)
class RelationCursor:
    last: RelationKey
    upper: RelationKey
    limit: int
    issued_at: datetime
    expires_at: datetime


def relation_filter_digest(*, subject: str, profile_id: str, operation: str, memory_id: str | None) -> str:
    return hashlib.sha256(
        wire_json(dict(subject=subject, profile_id=profile_id, operation=operation, memory_id=memory_id, filters={}))
    ).hexdigest()


def _digest(principal, command):
    return relation_filter_digest(
        subject=principal.subject,
        profile_id=command.profile_id,
        operation=command.operation,
        memory_id=command.path_ids.get("id"),
    )


class RelationCursorCodec:
    def __init__(self, key: bytes, ttl_seconds: int = 900, *, clock: Callable[[], datetime] | None = None) -> None:
        if type(key) is not bytes or len(key) != 32 or type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 900:
            raise ValueError("invalid cursor configuration")
        self._key, self.ttl = key, ttl_seconds
        self.clock = clock or (lambda: datetime.now(UTC))

    @classmethod
    def from_path(cls, path: Path, ttl_seconds: int = 900) -> RelationCursorCodec:
        path = Path(path)
        try:
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
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
        command: d.GraphReadCommand,
        *,
        last: RelationKey,
        upper: RelationKey,
        limit: int,
        now: datetime,
        expires_at: datetime | None = None,
    ) -> str:
        expiry = min(now + timedelta(seconds=self.ttl), expires_at) if expires_at else now + timedelta(seconds=self.ttl)
        if now >= expiry:
            raise HttpError("E_CURSOR_EXPIRED")
        payload = dict(
            v=1,
            profile_id=command.profile_id,
            subject=ctx.principal.subject,
            operation=command.operation,
            filter_sha256=_digest(ctx.principal, command),
            last=list(vars(last).values()),
            upper=list(vars(upper).values()),
            limit=limit,
            issued_at=now.astimezone(UTC).isoformat(),
            expires_at=expiry.astimezone(UTC).isoformat(),
        )
        body = b64(wire_json(payload))
        return body + "." + b64(hmac.digest(self._key, body.encode("ascii"), "sha256"))

    def validate(
        self, principal: Principal, command: d.GraphReadCommand, token: str | None = None, *, now: datetime
    ) -> RelationCursor:
        token = command.payload.cursor if token is None else token
        try:
            if not isinstance(token, str) or len(token) > 8192:
                raise ValueError
            body, signature = token.split(".")
            raw, mac = unb64(body), unb64(signature)
            if len(raw) > 6000 or not hmac.compare_digest(hmac.digest(self._key, body.encode("ascii"), "sha256"), mac):
                raise ValueError
            data = json.loads(raw.decode("utf-8"))
            if wire_json(data) != raw or set(data) != {
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
            if (data["subject"], data["profile_id"], data["operation"], data["filter_sha256"]) != (
                principal.subject,
                command.profile_id,
                command.operation,
                _digest(principal, command),
            ):
                raise ValueError
            if type(data["limit"]) is not int or not 1 <= command.payload.limit <= data["limit"] <= 200:
                raise ValueError

            def parse(value):
                if type(value) is not list or len(value) != 4 or any(type(v) is not str for v in value):
                    raise ValueError
                if utc(value[0]).isoformat(timespec="microseconds") != value[0]:
                    raise ValueError
                # The same strict tuple grammar as rel_ IDs, not a SQLite fallback.
                from memory_server.application.graph_read_service import relation_id

                relation_id(*value[1:])
                return RelationKey(*value)

            last, upper = parse(data["last"]), parse(data["upper"])
            issued, expiry = utc(data["issued_at"]), utc(data["expires_at"])
            if (
                last > upper
                or issued.isoformat() != data["issued_at"]
                or expiry.isoformat() != data["expires_at"]
                or issued > now
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
        if now >= expiry:
            raise HttpError("E_CURSOR_EXPIRED")
        return RelationCursor(last, upper, data["limit"], issued, expiry)


Parser = Callable[[Request, bytes, Principal], d.GraphReadCommand]
ResultValidator = Callable[[d.GraphReadResult], d.GraphReadResult]


@dataclass(frozen=True)
class GraphReadRouteSpec(ReadRouteSpec):
    parse: Parser
    validate_result: ResultValidator
    cursor_codec: RelationCursorCodec | None = None


def parse_get_values(request: Request, body: bytes, *, model: type[d.DTO], lists: frozenset[str]) -> dict[str, object]:
    if body:
        raise HttpError("E_VALIDATION")
    values = {}
    for key, value in request.query_params.multi_items():
        if key not in model.model_fields or (key in values and key not in lists):
            raise HttpError("E_VALIDATION")
        if key in lists:
            entries = values.setdefault(key, [])
            entries.append(value)
            if len(entries) > 32 or len(value) > 256 or len(value.encode("utf-8")) > 256:
                raise HttpError("E_VALIDATION")
        elif key in {"limit", "depth", "edge_limit"}:
            if not re.fullmatch(r"[0-9]{1,4}", value):
                raise HttpError("E_VALIDATION")
            values[key] = int(value)
        else:
            values[key] = value
    for key in lists & values.keys():
        values[key] = list(dict.fromkeys(values[key]))
    return values


def _parse(operation, request, body, principal):
    model = {
        "relations.for_memory": d.PageQuery,
        "relations.list": d.PageQuery,
        "graph.neighborhood": d.NeighborhoodQuery,
        "graph.path": d.PathRequest,
    }.get(operation, d.Empty)
    try:
        if operation == "graph.path":
            payload = parse_path_body(request, body)
        else:
            values = parse_get_values(
                request,
                body,
                model=model,
                lists=frozenset({"relation_types", "memory_types", "lifecycle_states"})
                if model is d.NeighborhoodQuery
                else frozenset(),
            )
            if model is d.NeighborhoodQuery:
                values.setdefault("memory_id", None)
                values.setdefault("entity_id", None)
                from memory_server.models.receipt import LifecycleState

                if any(v not in {"fact", "belief", "decision", "skill"} for v in values.get("memory_types", [])):
                    raise ValueError
                if any(v not in {state.value for state in LifecycleState} for v in values.get("lifecycle_states", [])):
                    raise ValueError
            payload = model.model_validate(values, strict=True)
        paths = {k: v for k, v in request.path_params.items() if k != "profile"}
        if operation == "relations.get":
            decode_relation_id(paths["id"])
        elif operation == "relations.for_memory":
            validate_memory_id(paths["id"])
        command = d.COMMAND_TYPES[operation](
            operation=operation, profile_id=request.path_params["profile"], path_ids=paths, payload=payload
        )
        if operation in {"relations.list", "relations.for_memory"} and payload.cursor is not None:
            codec = request.scope["s06"]["spec"].cursor_codec
            codec.validate(principal, command, now=codec.clock())
        return command
    except (ValidationError, ValueError, UnicodeError):
        raise HttpError("E_VALIDATION") from None


def parse_path_body(request: Request, body: bytes) -> d.PathRequest:
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

    def invalid(_):
        raise ValueError

    try:
        return d.PathRequest.model_validate(
            json.loads(body, object_pairs_hook=pairs, parse_constant=invalid), strict=True
        )
    except (ValueError, ValidationError, UnicodeError, RecursionError):
        raise HttpError("E_VALIDATION") from None


def parse_relations_for_memory(request: Request, body: bytes, principal: Principal) -> d.GraphReadCommand:
    return _parse("relations.for_memory", request, body, principal)


def parse_relations_list(request: Request, body: bytes, principal: Principal) -> d.GraphReadCommand:
    return _parse("relations.list", request, body, principal)


def parse_relations_get(request: Request, body: bytes, principal: Principal) -> d.GraphReadCommand:
    return _parse("relations.get", request, body, principal)


def parse_graph_neighborhood(request: Request, body: bytes, principal: Principal) -> d.GraphReadCommand:
    return _parse("graph.neighborhood", request, body, principal)


def parse_graph_path(request: Request, body: bytes, principal: Principal) -> d.GraphReadCommand:
    return _parse("graph.path", request, body, principal)


def parse_graph_entity(request: Request, body: bytes, principal: Principal) -> d.GraphReadCommand:
    return _parse("graph.entity", request, body, principal)


def validate_relation_page(value: d.GraphReadResult) -> d.Page[d.Relation]:
    return d.Page[d.Relation].model_validate(value.model_dump(), strict=True)


def validate_relation(value: d.GraphReadResult) -> d.Relation:
    return d.Relation.model_validate(value.model_dump(), strict=True)


def validate_graph_result(value: d.GraphReadResult) -> d.GraphResult:
    return d.GraphResult.model_validate(value.model_dump(), strict=True)


GRAPH_READ_ROUTE_SPECS: Final = MappingProxyType(
    {
        operation: GraphReadRouteSpec(
            operation,
            CATALOG.by_operation[operation].method,
            CATALOG.by_operation[operation].route,
            CATALOG.by_operation[operation].permission,
            parser,
            validator,
        )
        for operation, parser, validator in (
            ("relations.for_memory", parse_relations_for_memory, validate_relation_page),
            ("relations.list", parse_relations_list, validate_relation_page),
            ("relations.get", parse_relations_get, validate_relation),
            ("graph.neighborhood", parse_graph_neighborhood, validate_graph_result),
            ("graph.path", parse_graph_path, validate_graph_result),
            ("graph.entity", parse_graph_entity, validate_graph_result),
        )
    }
)


def create_graph_read_routes(*, cursor_codec: RelationCursorCodec) -> tuple[GraphReadRouteSpec, ...]:
    return tuple(
        replace(spec, cursor_codec=cursor_codec) if op != "relations.get" else spec
        for op, spec in GRAPH_READ_ROUTE_SPECS.items()
        if op.startswith("relations.")
    )
