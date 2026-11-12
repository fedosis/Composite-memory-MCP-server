# ruff: noqa: E501
"""Canonical S14 lifecycle route descriptors and strict request parser."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from types import MappingProxyType

from pydantic import ValidationError
from starlette.requests import Request

from memory_server.application.remote_lifecycle import validate_lifecycle_id
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.catalog import CATALOG
from memory_server.transports.http.errors import HttpError

from .ingestion import MutationRouteSpec

_IF_MATCH = re.compile(r'"m:(fact|belief):([0-9a-f-]{36}):([1-9][0-9]*)"')


def _parse(request: Request, body: bytes, profile_id: str, operation: str) -> d.Command:
    memory_id = validate_lifecycle_id(request.path_params.get("id"))
    try:
        payload = CATALOG.by_operation[operation].request_type.model_validate_json(body)
    except ValidationError as exc:
        errors = exc.errors(include_url=False, include_input=False)
        if not errors or any(error["type"] != "missing" or error["loc"] != ("expected_version",) for error in errors):
            raise HttpError("E_VALIDATION") from None
        try:
            raw = json.loads(body)
            if not isinstance(raw, dict) or not isinstance(raw.get("reason"), str) or not raw["reason"].strip():
                raise HttpError("E_VALIDATION")
        except HttpError:
            raise
        except (ValueError, TypeError):
            raise HttpError("E_VALIDATION") from None
        missing_version = True
    else:
        if not payload.reason.strip():
            raise HttpError("E_VALIDATION")
        missing_version = False
    matches = request.headers.getlist("if-match")
    if len(matches) > 1:
        raise HttpError("E_VALIDATION")
    tag = matches[0] if matches else None
    parsed = _IF_MATCH.fullmatch(tag) if tag is not None else None
    if tag is not None and parsed is None:
        raise HttpError("E_VALIDATION")
    if parsed is not None:
        validate_lifecycle_id(parsed.group(2))
    if missing_version:
        raise HttpError("E_PRECONDITION_REQUIRED", details={})
    if tag is not None and tag != f'"m:{payload.memory_type}:{memory_id}:{payload.expected_version}"':
        raise HttpError("E_VALIDATION")
    return d.COMMAND_TYPES[operation](
        operation=operation, profile_id=profile_id, path_ids={"id": memory_id}, payload=payload
    )


def _parse_transition(request: Request, body: bytes, profile_id: str) -> d.Command:
    return _parse(request, body, profile_id, "memories.transition")


def _parse_invalidate(request: Request, body: bytes, profile_id: str) -> d.Command:
    return _parse(request, body, profile_id, "memories.invalidate")


def _status(result: d.TransitionResult) -> int:
    del result
    return 200


LIFECYCLE_ROUTE_SPECS: Mapping[str, MutationRouteSpec] = MappingProxyType(
    {
        "memories.transition": MutationRouteSpec(
            "memories.transition",
            "POST",
            "/api/v1/profiles/{profile}/memories/{id}/transition",
            "memory.transition",
            _parse_transition,
            _status,
        ),
        "memories.invalidate": MutationRouteSpec(
            "memories.invalidate",
            "POST",
            "/api/v1/profiles/{profile}/memories/{id}/invalidate",
            "memory.invalidate",
            _parse_invalidate,
            _status,
        ),
    }
)


def create_lifecycle_routes() -> tuple[MutationRouteSpec, ...]:
    return (LIFECYCLE_ROUTE_SPECS["memories.transition"], LIFECYCLE_ROUTE_SPECS["memories.invalidate"])
