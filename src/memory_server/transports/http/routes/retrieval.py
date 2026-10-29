"""S08 retrieval routes and the transport-free gateway adapter."""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import ValidationError

from memory_server.gateway_contracts import dto as d
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.guards import ReadRouteSpec


@dataclass(frozen=True)
class RetrievalRouteSpec(ReadRouteSpec):
    """Canonical S08 route specification."""


def _parse_search(request, body, principal):
    if request.query_params:
        raise HttpError("E_VALIDATION")
    if request.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json":
        raise HttpError("E_UNSUPPORTED_MEDIA_TYPE")
    try:
        query = d.SearchRequest.model_validate_json(body)
    except (ValidationError, ValueError, TypeError):
        raise HttpError("E_VALIDATION") from None
    if not query.query.isprintable():
        raise HttpError("E_VALIDATION")
    filters = query.filters
    # C4/T2: see ops/CONTRACTS_AMENDMENT_C4_T2.md; these filters remain unsupported.
    if filters.source is not None or filters.tags:
        raise HttpError("E_VALIDATION")
    if query.trace or query.mode in ("semantic", "graph"):
        raise HttpError("E_CAPABILITY_UNAVAILABLE")
    return d.COMMAND_TYPES["search"](
        operation="search", profile_id=request.path_params["profile"], path_ids={}, payload=query
    )


def _parse_context(request, body, principal):
    if request.query_params:
        raise HttpError("E_VALIDATION")
    if request.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json":
        raise HttpError("E_UNSUPPORTED_MEDIA_TYPE")
    try:
        query = d.ContextRequest.model_validate_json(body)
    except (ValidationError, ValueError, TypeError):
        raise HttpError("E_VALIDATION") from None
    if not query.task.isprintable() or (query.subject is not None and not query.subject.isprintable()):
        raise HttpError("E_VALIDATION")
    if query.trace:
        raise HttpError("E_CAPABILITY_UNAVAILABLE")
    return d.COMMAND_TYPES["context"](
        operation="context", profile_id=request.path_params["profile"], path_ids={}, payload=query
    )


def _result_search(value):
    return d.SearchResult.model_validate(value.model_dump())


def _result_context(value):
    return d.ContextResult.model_validate(value.model_dump())


_SEARCH_SPEC = RetrievalRouteSpec(
    "search", "POST", "/api/v1/profiles/{profile}/search", "memory.read", _parse_search, _result_search
)
_CONTEXT_SPEC = RetrievalRouteSpec(
    "context", "POST", "/api/v1/profiles/{profile}/context", "memory.read", _parse_context, _result_context
)
RETRIEVAL_ROUTE_SPECS = {"search": _SEARCH_SPEC, "context": _CONTEXT_SPEC}


def create_search_routes():
    return (_SEARCH_SPEC,)


def create_context_routes():
    return (_CONTEXT_SPEC,)


# Compatibility aliases used by older slice tests and imports.
_parse = _parse_search
_result = _result_search

from memory_server.application.gateway_retrieval import GatewayRetrievalPort  # noqa: E402

KeywordRetrievalPort = GatewayRetrievalPort
