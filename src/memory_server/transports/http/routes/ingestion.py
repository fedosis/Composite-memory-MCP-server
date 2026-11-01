# ruff: noqa: E501
"""Canonical S14 lifecycle mutation route descriptors."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from pydantic import ValidationError
from starlette.requests import Request

from memory_server.application.remote_ingestion import write_status
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.catalog import CATALOG
from memory_server.transports.http.errors import HttpError


@dataclass(frozen=True)
class MutationRouteSpec:
    operation: str
    method: Literal["POST"]
    path: str
    permission: d.Permission
    parse: Callable[[Request, bytes, str], d.Command]
    status: Callable[[d.DTO], int]


def _parse_remember(request: Request, body: bytes, profile_id: str) -> d.Command:
    del request
    entry = CATALOG.by_operation["memories.create"]
    try:
        payload = entry.request_type.model_validate_json(body)
    except ValidationError:
        raise HttpError("E_VALIDATION") from None
    return d.COMMAND_TYPES["memories.create"](operation="memories.create", profile_id=profile_id, path_ids={}, payload=payload)


def _parse_learn(request: Request, body: bytes, profile_id: str) -> d.Command:
    del request
    entry = CATALOG.by_operation["learn"]
    try:
        payload = entry.request_type.model_validate_json(body)
    except ValidationError:
        raise HttpError("E_VALIDATION") from None
    return d.COMMAND_TYPES["learn"](operation="learn", profile_id=profile_id, path_ids={}, payload=payload)


INGESTION_ROUTE_SPECS: Mapping[str, MutationRouteSpec] = MappingProxyType({
    "memories.create": MutationRouteSpec("memories.create", "POST", "/api/v1/profiles/{profile}/memories", "memory.write", _parse_remember, write_status),
    "learn": MutationRouteSpec("learn", "POST", "/api/v1/profiles/{profile}/learn", "memory.write", _parse_learn, write_status),
})


def create_ingestion_routes() -> tuple[MutationRouteSpec, ...]:
    return (INGESTION_ROUTE_SPECS["memories.create"], INGESTION_ROUTE_SPECS["learn"])
