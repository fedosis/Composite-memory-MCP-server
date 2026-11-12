"""Closed S16 relation mutation wire surface."""

import json
import re
from types import MappingProxyType

from pydantic import TypeAdapter, ValidationError
from starlette.requests import Request

from memory_server.application.graph_read_service import decode_relation_id, relation_id
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.catalog import CATALOG
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.routes.ingestion import MutationRouteSpec


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _constant(value):
    raise ValueError


def _parse(request, body, profile_id, deleted):
    try:
        path = {"id": request.path_params["id"]} if deleted else {}
        key = decode_relation_id(path["id"]) if deleted else None
        headers = request.headers.getlist("if-match")
        if not deleted and headers:
            raise ValueError
        tag_revision = None
        if deleted and headers:
            if len(headers) != 1:
                raise ValueError
            match = re.fullmatch(r'"r:(rel_[A-Za-z0-9_-]+):([1-9][0-9]*)"', headers[0])
            if match is None or match[1] != path["id"]:
                raise ValueError
            tag_revision = int(match[2])
        values = json.loads(body.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant)
        model = d.RelationDeleteRequest if deleted else d.RelationRequest
        if type(values) is not dict or set(values) - set(model.model_fields):
            raise ValueError
        for name, field in model.model_fields.items():
            if name not in ("expected_versions", "expected_version"):
                TypeAdapter(field.rebuild_annotation()).validate_python(values.get(name), strict=True)
        if not values["reason"].strip():
            raise ValueError
        if not deleted:
            key = (values["source_id"], values["target_id"], values["relation_type"])
            relation_id(*key)
        if key[2] == "derived_from":
            raise ValueError
        versions = values.get("expected_versions", {})
        if (
            type(versions) is not dict
            or set(versions) - set(key[:2])
            or any(type(v) is not int or v < 1 for v in versions.values())
        ):
            raise ValueError
        if "expected_version" in values:
            version = values["expected_version"]
            if type(version) is not int or version < 1 or (tag_revision is not None and tag_revision != version):
                raise ValueError
        if set(versions) != set(key[:2]) or (deleted and "expected_version" not in values):
            raise HttpError("E_PRECONDITION_REQUIRED")
        op = "relations.delete" if deleted else "relations.create"
        return d.COMMAND_TYPES[op](
            operation=op, profile_id=profile_id, path_ids=path, payload=model.model_validate(values, strict=True)
        )
    except (ValueError, TypeError, KeyError, UnicodeError, ValidationError):
        raise HttpError("E_VALIDATION") from None


def _create(request: Request, body: bytes, profile_id: str):
    return _parse(request, body, profile_id, False)


def _delete(request: Request, body: bytes, profile_id: str):
    return _parse(request, body, profile_id, True)


def relation_status(result, deleted):
    try:
        if type(result) is not d.RelationResult or type(result.relation) is not d.Relation:
            raise ValueError
        for model in (result, result.relation):
            if set(model.__dict__) != set(type(model).model_fields) or model.model_extra:
                raise ValueError
        checked = d.RelationResult.model_validate(result.model_dump(), strict=True)
        rel = checked.relation
        if checked.deleted is not deleted or rel.id != relation_id(rel.source_id, rel.target_id, rel.relation_type):
            raise ValueError
        if rel.relation_type == "derived_from":
            raise ValueError
        return 200 if deleted else 201
    except (ValueError, TypeError, AttributeError, ValidationError):
        raise HttpError("E_INTERNAL") from None


def _created(result):
    return relation_status(result, False)


def _deleted(result):
    return relation_status(result, True)


RELATION_MUTATION_ROUTE_SPECS = MappingProxyType(
    {
        op: MutationRouteSpec(
            op, CATALOG.by_operation[op].method, CATALOG.by_operation[op].route, "relation.write", parse, status
        )
        for op, parse, status in (("relations.create", _create, _created), ("relations.delete", _delete, _deleted))
    }
)


def create_relation_mutation_routes():
    return tuple(RELATION_MUTATION_ROUTE_SPECS.values())
