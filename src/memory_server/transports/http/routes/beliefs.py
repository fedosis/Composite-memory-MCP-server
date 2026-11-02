"""Sealed S15 read descriptors, MAC cursors, and fresh masked results."""

from __future__ import annotations

import binascii
import hashlib
import hmac
import json
from dataclasses import dataclass, replace
from datetime import UTC, timedelta
from types import MappingProxyType
from typing import Callable

from pydantic import ValidationError
from starlette.requests import Request

from memory_server.application.memory_read_service import b64, canonical, filter_digest, unb64, utc
from memory_server.application.remote_beliefs import (
    BeliefReadResult,
    decode_conflict_id,
    validate_belief_id,
    validate_read_command,
)
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.catalog import CATALOG
from memory_server.gateway_contracts.ports import Principal
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.guards import ReadRouteSpec
from memory_server.transports.http.routes.memories_read import CursorCodec

# isort: split
from pydantic import ConfigDict, TypeAdapter

from memory_server.application.remote_beliefs import validate_belief_mutation_command
from memory_server.application.remote_ingestion import write_status
from memory_server.transports.http.routes.ingestion import MutationRouteSpec


def _digest(command):
    if command.operation != "conflicts.list":
        return filter_digest(command)
    return hashlib.sha256(
        canonical({"filters": {}, "path_ids": {}, "pair_policy": "s15a-active-contradicted-contradiction-v1"})
    ).hexdigest()


class BeliefCursorCodec(CursorCodec):
    def issue(self, ctx, command, *, last, upper, limit, now):
        data = dict(
            v=1,
            profile_id=ctx.profile_id,
            subject=ctx.principal.subject,
            operation=command.operation,
            filter_sha256=_digest(command),
            last=list(last),
            upper=list(upper),
            limit=limit,
            issued_at=now.astimezone(UTC).isoformat(),
            expires_at=(now + timedelta(seconds=self.ttl)).astimezone(UTC).isoformat(),
        )
        body = b64(canonical(data))
        return body + "." + b64(hmac.digest(self._key, body.encode(), "sha256"))

    def validate(self, principal, command, token=None, *, now):
        token = command.payload.cursor if token is None else token
        try:
            if type(token) is not str or len(token) > 8192:
                raise ValueError
            body, signature = token.split(".")
            raw = unb64(body)
            if not hmac.compare_digest(hmac.digest(self._key, body.encode(), "sha256"), unb64(signature)):
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

            def position(value):
                if type(value) is not list or not all(type(v) is str for v in value):
                    raise ValueError
                if command.operation == "beliefs.list":
                    if len(value) != 3 or value[1] != "belief":
                        raise ValueError
                    times, ids = (value[0],), (value[2],)
                elif command.operation == "conflicts.list" and len(value) == 4 and value[1] < value[3]:
                    times, ids = (value[0], value[2]), (value[1], value[3])
                else:
                    raise ValueError
                for timestamp in times:
                    if utc(timestamp).isoformat(timespec="microseconds") != timestamp:
                        raise ValueError
                for ident in ids:
                    validate_belief_id(ident)
                return tuple(value)

            last, upper = position(data["last"]), position(data["upper"])
            if last > upper or (data["profile_id"], data["subject"], data["operation"], data["filter_sha256"]) != (
                command.profile_id,
                principal.subject,
                command.operation,
                _digest(command),
            ):
                raise ValueError
            if type(data["limit"]) is not int or not 1 <= command.payload.limit <= data["limit"] <= 200:
                raise ValueError
            issued, expiry = utc(data["issued_at"]), utc(data["expires_at"])
            if (
                issued.isoformat() != data["issued_at"]
                or expiry.isoformat() != data["expires_at"]
                or issued > now
                or not timedelta(0) < expiry - issued <= timedelta(seconds=self.ttl)
            ):
                raise ValueError
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError, binascii.Error, HttpError):
            raise HttpError("E_CURSOR_INVALID") from None
        if now >= expiry:
            raise HttpError("E_CURSOR_EXPIRED")
        return {"last": last, "upper": upper, "limit": data["limit"]}


def _parse(operation, request, body, principal):
    if body or request.headers.getlist("if-match"):
        raise HttpError("E_VALIDATION")
    model = {"beliefs.list": d.BeliefListQuery, "beliefs.get": d.Empty, "conflicts.list": d.PageQuery}[operation]
    values = {}
    try:
        for key, value in request.query_params.multi_items():
            if key not in model.model_fields or (key in values and key != "tags"):
                raise ValueError
            if key == "tags":
                values.setdefault(key, []).append(value)
            elif key == "limit":
                if not value.isascii() or not value.isdecimal() or len(value) > 3:
                    raise ValueError
                values[key] = int(value)
            elif key == "min_confidence":
                values[key] = float(value)
            else:
                values[key] = value
        if operation == "beliefs.list":
            for key in ("cursor", "proposition", "source", "creator", "source_id"):
                values.setdefault(key, None)
        payload = model.model_validate(values, strict=True)
        path_ids = {"id": request.path_params["id"]} if operation == "beliefs.get" else {}
        command = d.COMMAND_TYPES[operation](
            operation=operation, profile_id=request.path_params["profile"], path_ids=path_ids, payload=payload
        )
    except (ValidationError, ValueError, TypeError, KeyError):
        raise HttpError("E_VALIDATION") from None
    command = validate_read_command(command)
    if getattr(payload, "cursor", None) is not None:
        codec = request.scope["s06"]["spec"].cursor_codec
        codec.validate(principal, command, now=codec.clock())
    return command


def parse_beliefs_list(request: Request, body: bytes, principal: Principal) -> d.BeliefsListCommand:
    return _parse("beliefs.list", request, body, principal)


def parse_beliefs_get(request: Request, body: bytes, principal: Principal) -> d.BeliefsGetCommand:
    return _parse("beliefs.get", request, body, principal)


def parse_conflicts_list(request: Request, body: bytes, principal: Principal) -> d.ConflictsListCommand:
    return _parse("conflicts.list", request, body, principal)


def validate_belief(value: BeliefReadResult) -> d.Memory:
    try:
        item = d.Memory.model_validate(value.model_dump(), strict=True)
        validate_belief_id(item.id)
        if (
            item.memory_type != "belief"
            or type(item.revision) is not int
            or item.revision < 1
            or type(item.source_version) is not int
            or item.source_version != item.revision
        ):
            raise ValueError
        return item.model_copy(
            update={"content": None, "payload": None, "source": None, "creator": None, "masked": True, "risk_tags": []}
        )
    except (ValidationError, ValueError, TypeError, AttributeError, HttpError):
        raise HttpError("E_INTERNAL") from None


def _evidence(value):
    return d.EvidenceSummary(
        method=None,
        sources=[],
        session_id=None,
        confidence=value.confidence,
        source_date=None,
        derived_from=[],
        claim_type=None,
    )


def _page(value, conflict):
    try:
        page = (d.Page[d.Conflict] if conflict else d.Page[d.Memory]).model_validate(value.model_dump(), strict=True)
        if (
            len(page.items) > 200
            or page.has_more != (page.next_cursor is not None)
            or (page.has_more and not page.items)
            or not page.request_id
            or (page.next_cursor is not None and not 1 <= len(page.next_cursor) <= 8192)
        ):
            raise ValueError
        if len({item.id for item in page.items}) != len(page.items):
            raise ValueError
        items = []
        for item in page.items:
            if not conflict:
                items.append(validate_belief(item))
                continue
            a, b = validate_belief(item.belief_a), validate_belief(item.belief_b)
            if (
                decode_conflict_id(item.id) != (a.id, b.id)
                or item.kind != "contradiction"
                or item.analysis_complete is not False
            ):
                raise ValueError
            items.append(
                d.Conflict(
                    id=item.id,
                    belief_a=a,
                    belief_b=b,
                    evidence_a=[_evidence(v) for v in item.evidence_a],
                    evidence_b=[_evidence(v) for v in item.evidence_b],
                    kind=item.kind,
                    allowed_actions=list(item.allowed_actions),
                    analysis_complete=False,
                )
            )
        return page.model_copy(update={"items": items})
    except (ValidationError, ValueError, TypeError, AttributeError, HttpError):
        raise HttpError("E_INTERNAL") from None


def validate_beliefs_page(value: BeliefReadResult) -> d.Page[d.Memory]:
    return _page(value, False)


def validate_conflicts_page(value: BeliefReadResult) -> d.Page[d.Conflict]:
    return _page(value, True)


def belief_etag(value: BeliefReadResult) -> str:
    item = validate_belief(value)
    return f'"m:belief:{item.id}:{item.revision}"'


def _no_etag(value: BeliefReadResult) -> None:
    return None


@dataclass(frozen=True)
class BeliefReadRouteSpec(ReadRouteSpec):
    cursor_codec: BeliefCursorCodec | None = None
    etag: Callable[[BeliefReadResult], str | None] = _no_etag


def _spec(operation, parse, validate, etag=_no_etag):
    row = CATALOG.by_operation[operation]
    return BeliefReadRouteSpec(operation, row.method, row.route, row.permission, parse, validate, etag=etag)


BELIEF_READ_ROUTE_SPECS = MappingProxyType(
    {
        "beliefs.list": _spec("beliefs.list", parse_beliefs_list, validate_beliefs_page),
        "beliefs.get": _spec("beliefs.get", parse_beliefs_get, validate_belief, belief_etag),
        "conflicts.list": _spec("conflicts.list", parse_conflicts_list, validate_conflicts_page),
    }
)


def create_belief_read_routes(*, cursor_codec: BeliefCursorCodec) -> tuple[BeliefReadRouteSpec, ...]:
    return tuple(
        replace(spec, cursor_codec=cursor_codec) if op != "beliefs.get" else spec
        for op, spec in BELIEF_READ_ROUTE_SPECS.items()
    )


def parse_beliefs_set(request: Request, body: bytes, profile_id: str) -> d.BeliefsSetCommand:
    if request.headers.getlist("if-match"):
        raise HttpError("E_VALIDATION")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError
            result[key] = value
        return result

    def nonfinite(value):
        raise ValueError

    try:
        raw = json.loads(body.decode("utf-8"), object_pairs_hook=pairs, parse_constant=nonfinite)
        if type(raw) is not dict or set(raw) - set(d.BeliefRequest.model_fields):
            raise ValueError
        for name, field in d.BeliefRequest.model_fields.items():
            if name in raw:
                TypeAdapter(
                    field.rebuild_annotation(), config=ConfigDict(strict=True, allow_inf_nan=False)
                ).validate_python(raw[name])
            elif field.is_required():
                raise ValueError
        payload = d.BeliefRequest.model_validate(raw, strict=True)
        command = d.BeliefsSetCommand(operation="beliefs.set", profile_id=profile_id, path_ids={}, payload=payload)
    except (ValidationError, ValueError, TypeError, KeyError, RecursionError):
        raise HttpError("E_VALIDATION") from None
    return validate_belief_mutation_command(command)


BELIEF_MUTATION_ROUTE_SPECS = MappingProxyType(
    {
        "beliefs.set": MutationRouteSpec(
            "beliefs.set",
            CATALOG.by_operation["beliefs.set"].method,
            CATALOG.by_operation["beliefs.set"].route,
            CATALOG.by_operation["beliefs.set"].permission,
            parse_beliefs_set,
            write_status,
        ),
    }
)


def create_belief_mutation_routes() -> tuple[MutationRouteSpec, ...]:
    return (BELIEF_MUTATION_ROUTE_SPECS["beliefs.set"],)
