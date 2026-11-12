"""Closed, read-only POST descriptors for S17 inspection."""

from __future__ import annotations

import base64
import json
import math
from dataclasses import dataclass
from datetime import timedelta
from types import MappingProxyType
from uuid import UUID

from pydantic import BaseModel, ValidationError

from memory_server.gateway_contracts import dto as d
from memory_server.models.receipt import LifecycleState
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.guards import ReadRouteSpec


@dataclass(frozen=True)
class InspectionRouteSpec(ReadRouteSpec):
    pass


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _finite(value, depth=0):
    if depth > 32:
        raise ValueError("JSON depth exceeded")
    if type(value) is float and not math.isfinite(value):
        raise ValueError("nonfinite number")
    if type(value) is dict:
        for key, child in value.items():
            key.encode("utf-8", errors="strict")
            _finite(child, depth + 1)
    elif type(value) is list:
        for child in value:
            _finite(child, depth + 1)
    elif type(value) is str:
        value.encode("utf-8", errors="strict")


def _reject_constant(value):
    raise ValueError("nonfinite number")


def _decode(body):
    try:
        value = json.loads(
            body.decode("utf-8", errors="strict"), object_pairs_hook=_pairs, parse_constant=_reject_constant
        )
        if type(value) is not dict:
            raise ValueError("object required")
        _finite(value)
        return value
    except (ValueError, TypeError, RecursionError, OverflowError, UnicodeError):
        raise HttpError("E_VALIDATION") from None


def _uuid(value):
    if type(value) is not str or str(UUID(value)) != value:
        raise ValueError("canonical UUID required")
    return value


def _decode_handle(value, kind):
    prefix = "rel_" if kind == "relation" else "conf_"
    limit, raw_limit = (2048, 1024) if kind == "relation" else (261, 128)
    if type(value) is not str or len(value) > limit or not value.startswith(prefix):
        raise ValueError("invalid handle")
    encoded = value[len(prefix) :]
    raw = base64.b64decode(encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
    if len(raw) > raw_limit:
        raise ValueError("oversize handle")
    parts = json.loads(raw.decode("utf-8"))
    if type(parts) is not list or len(parts) != (3 if kind == "relation" else 2):
        raise ValueError("invalid tuple")
    _uuid(parts[0])
    _uuid(parts[1])
    if kind == "relation":
        if parts[2] not in ("supports", "contradicts", "derives", "derived_from"):
            raise ValueError("unsupported relation")
        if parts[2] == "derived_from":
            raise ValueError("read-only relation")
    elif parts[0] >= parts[1]:
        raise ValueError("unordered pair")
    canonical = (
        base64.urlsafe_b64encode(json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode())
        .decode()
        .rstrip("=")
    )
    if prefix + canonical != value:
        raise ValueError("noncanonical handle")
    return tuple(parts)


def _preview_matrix(payload):
    op = payload.operation
    if op == "transition":
        if payload.to_state not in {state.value for state in LifecycleState}:
            raise ValueError("invalid state")
    elif payload.to_state is not None:
        raise ValueError("unexpected state")
    if op == "relation_delete":
        _decode_handle(payload.relation_id, "relation")
    elif payload.relation_id is not None:
        raise ValueError("unexpected relation")
    if op == "resolve":
        if payload.memory_type != "belief":
            raise ValueError("belief required")
        _decode_handle(payload.conflict_id, "conflict")
    elif payload.conflict_id is not None:
        raise ValueError("unexpected conflict")


def _body(request, body):
    if request.scope.get("query_string", b"") != b"":
        raise HttpError("E_VALIDATION")
    media = request.headers.getlist("content-type")
    if len(media) > 1:
        raise HttpError("E_VALIDATION")
    if not media or media[0].split(";", 1)[0].strip().lower() != "application/json":
        raise HttpError("E_UNSUPPORTED_MEDIA_TYPE")
    return _decode(body)


def _parse_admission(request, body, principal):
    values = _body(request, body)
    try:
        values = values
        payload = d.AdmissionRequest.model_validate(values, strict=True)
        return d.COMMAND_TYPES["admission.preview"](
            operation="admission.preview", profile_id=request.path_params["profile"], path_ids={}, payload=payload
        )
    except (ValidationError, ValueError, TypeError):
        raise HttpError("E_VALIDATION") from None


def _parse_preview(request, body, principal):
    values = _body(request, body)
    try:
        values = values
        payload = d.PreviewRequest.model_validate(values, strict=True)
        _preview_matrix(payload)
        ident = _uuid(request.path_params["id"])
        return d.COMMAND_TYPES["mutations.preview"](
            operation="mutations.preview",
            profile_id=request.path_params["profile"],
            path_ids={"id": ident},
            payload=payload,
        )
    except (ValidationError, ValueError, TypeError, UnicodeError):
        raise HttpError("E_VALIDATION") from None


def _fields(value):
    """Copy fields without trusting model_dump or instance revalidation shortcuts."""
    if isinstance(value, BaseModel):
        fields = type(value).model_fields
        if set(value.__dict__) != set(fields) or getattr(value, "__pydantic_extra__", None):
            raise ValueError("corrupt model fields")
        return {name: _fields(value.__dict__[name]) for name in fields}
    if type(value) is dict:
        return {key: _fields(child) for key, child in value.items()}
    if type(value) is list:
        return [_fields(child) for child in value]
    return value


def _bounded(value):
    encoded = json.dumps(
        value.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    if len(encoded) > 65536:
        raise HttpError("E_STORAGE", details={"limit": 65536})


def _result_admission(value):
    try:
        if type(value) is not d.AdmissionResult:
            raise ValueError("wrong result")
        fields = _fields(value)
        for key in ("risk_tags", "reason_codes"):
            if type(fields[key]) is not list or any(type(item) is not str for item in fields[key]):
                raise ValueError("string list required")
        if type(fields["admitted"]) is not bool:
            raise ValueError("bool required")
        if fields["ttl_days"] is not None and type(fields["ttl_days"]) is not int:
            raise ValueError("integer required")
        if type(fields["score"]) not in (int, float) or not math.isfinite(fields["score"]):
            raise ValueError("finite score required")
        result = d.AdmissionResult.model_validate(fields, strict=True)
        _bounded(result)
        return result
    except (ValidationError, ValueError, TypeError, OverflowError, UnicodeError, RecursionError):
        raise HttpError("E_INTERNAL") from None


def _result_preview(value):
    try:
        if type(value) is not d.PreviewResult or type(value.projections) is not d.ItemProjection:
            raise ValueError("wrong result")
        result = d.PreviewResult.model_validate(_fields(value), strict=True)
        _uuid(result.memory_id)
        ids = result.affected_ids
        if not 1 <= len(ids) <= 100 or len(ids) != len(set(ids)) or result.memory_id not in ids:
            raise ValueError("invalid affected IDs")
        for ident in ids:
            _uuid(ident)
        if type(result.affected_count) is not int or result.affected_count != len(ids):
            raise ValueError("invalid count")
        if (
            type(result.count_exact) is not bool
            or type(result.truncated) is not bool
            or result.count_exact == result.truncated
        ):
            raise ValueError("invalid truncation")
        keys = set(result.observed_versions)
        extra = keys - set(ids)
        if not set(ids) <= keys or len(extra) > 1:
            raise ValueError("invalid observed keys")
        if extra:
            source, target, _ = _decode_handle(next(iter(extra)), "relation")
            if result.memory_id not in (source, target) or not {source, target} <= set(ids):
                raise ValueError("unbound relation")
        if any(type(revision) is not int or revision < 1 for revision in result.observed_versions.values()):
            raise ValueError("invalid revision")
        vocabulary = {state.value for state in LifecycleState} | {
            "invalidate",
            "relation_delete",
            "keep_a",
            "keep_b",
            "merge",
            "mark_contradicted",
            "discard_both",
            "defer",
        }
        if (
            len(set(result.allowed_actions)) != len(result.allowed_actions)
            or not set(result.allowed_actions) <= vocabulary
        ):
            raise ValueError("invalid actions")
        if result.expires_at.utcoffset() != timedelta(0) or result.preview_only is not True:
            raise ValueError("invalid snapshot metadata")
        _bounded(result)
        return result
    except (ValidationError, ValueError, TypeError, OverflowError, UnicodeError, RecursionError):
        raise HttpError("E_INTERNAL") from None


INSPECTION_ROUTE_SPECS = MappingProxyType(
    {
        "admission.preview": InspectionRouteSpec(
            "admission.preview",
            "POST",
            "/api/v1/profiles/{profile}/admission/preview",
            "memory.write",
            _parse_admission,
            _result_admission,
        ),
        "mutations.preview": InspectionRouteSpec(
            "mutations.preview",
            "POST",
            "/api/v1/profiles/{profile}/memories/{id}/preview",
            "memory.read",
            _parse_preview,
            _result_preview,
        ),
    }
)


def create_inspection_routes():
    return tuple(INSPECTION_ROUTE_SPECS.values())
