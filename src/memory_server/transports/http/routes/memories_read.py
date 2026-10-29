"""S07 route descriptors and strict read request parsing."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Literal

from pydantic import ValidationError
from starlette.requests import Request

from memory_server.gateway_contracts import dto as d
from memory_server.transports.http.errors import HttpError


@dataclass(frozen=True)
class ReadRouteSpec:
    operation: str
    method: Literal["GET", "POST"]
    path: str
    permission: str
    parse: Callable[[Request, bytes, Any], Any]
    validate_result: Callable[[Any], Any]


def _query(request: Request) -> dict[str, Any]:
    pairs = list(request.query_params.multi_items())
    if len({key for key, _ in pairs}) != len(pairs):
        raise HttpError("E_VALIDATION")
    values: dict[str, Any] = {}
    for key, value in pairs:
        if key == "limit":
            if not value.isascii() or not value.isdecimal():
                raise HttpError("E_VALIDATION")
            values[key] = int(value)
        elif key in {
            "cursor",
            "source",
            "authority_level",
            "epistemic_status",
            "valid_at",
            "created_from",
            "created_to",
            "updated_from",
            "updated_to",
            "min_confidence",
            "max_confidence",
        }:
            values[key] = value
        elif key in {"memory_types", "lifecycle_states", "tags", "risk_tags"}:
            values.setdefault(key, []).append(value)
        else:
            raise HttpError("E_VALIDATION")
    return values


def _make(operation: str, profile: str, path_ids: dict[str, str], payload: Any) -> Any:
    return d.COMMAND_TYPES[operation](operation=operation, profile_id=profile, path_ids=path_ids, payload=payload)


def _validate(model: Any) -> Callable[[Any], Any]:
    return lambda value: model.model_validate(value.model_dump() if hasattr(value, "model_dump") else value)


def _parse_list(request: Request, body: bytes, principal: Any) -> Any:
    if body:
        raise HttpError("E_VALIDATION")
    try:
        return _make("memories.list", request.path_params["profile"], {}, d.MemoryListQuery(**_query(request)))
    except (ValidationError, TypeError, ValueError):
        raise HttpError("E_VALIDATION") from None


def _parse_empty(operation: str, request: Request, body: bytes, principal: Any) -> Any:
    if body or request.query_params:
        raise HttpError("E_VALIDATION")
    return _make(operation, request.path_params["profile"], {"id": request.path_params["id"]}, d.Empty())


def _parse_history(request: Request, body: bytes, principal: Any) -> Any:
    if body:
        raise HttpError("E_VALIDATION")
    try:
        payload = d.PageQuery(**_query(request))
    except (ValidationError, TypeError, ValueError):
        raise HttpError("E_VALIDATION") from None
    return _make("history.list", request.path_params["profile"], {"id": request.path_params["id"]}, payload)


def _parse_reveal(request: Request, body: bytes, principal: Any) -> Any:
    if request.query_params or request.headers.get("content-type", "").split(";")[0].lower() != "application/json":
        raise HttpError("E_UNSUPPORTED_MEDIA_TYPE")
    try:
        payload = d.RevealRequest.model_validate(json.loads(body))
    except (ValidationError, TypeError, ValueError, json.JSONDecodeError):
        raise HttpError("E_VALIDATION") from None
    return _make("memories.reveal", request.path_params["profile"], {"id": request.path_params["id"]}, payload)


def create_memory_read_routes(port: Any, guard: Any, *, cursor_codec: Any = None) -> tuple[ReadRouteSpec, ...]:
    result = (
        ReadRouteSpec(
            "memories.list",
            "GET",
            "/api/v1/profiles/{profile}/memories",
            "memory.read",
            _parse_list,
            _validate(d.Page[d.Memory]),
        ),
        ReadRouteSpec(
            "memories.get",
            "GET",
            "/api/v1/profiles/{profile}/memories/{id}",
            "memory.read",
            lambda r, b, p: _parse_empty("memories.get", r, b, p),
            _validate(d.Memory),
        ),
        ReadRouteSpec(
            "receipts.get",
            "GET",
            "/api/v1/profiles/{profile}/memories/{id}/receipt",
            "memory.read",
            lambda r, b, p: _parse_empty("receipts.get", r, b, p),
            _validate(d.Receipt),
        ),
        ReadRouteSpec(
            "history.list",
            "GET",
            "/api/v1/profiles/{profile}/memories/{id}/history",
            "memory.read",
            _parse_history,
            _validate(d.Page[d.HistoryEvent]),
        ),
        ReadRouteSpec(
            "lookup",
            "GET",
            "/api/v1/profiles/{profile}/lookup",
            "memory.read",
            lambda r, b, p: _make("lookup", r.path_params["profile"], {}, d.LookupQuery(**_query(r))),
            _validate(d.LookupResult),
        ),
        ReadRouteSpec(
            "memories.reveal",
            "POST",
            "/api/v1/profiles/{profile}/memories/{id}/reveal",
            "memory.reveal",
            _parse_reveal,
            _validate(d.Memory),
        ),
    )
    if port is None:
        raise TypeError("memory read port required")
    return result
