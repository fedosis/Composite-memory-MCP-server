"""Bounded, strict AF_UNIX JSON wire v1."""

from __future__ import annotations

import asyncio
import json
import re
import struct
import uuid
from pathlib import Path
from typing import Any, Mapping

from memory_server.native.schema_v1 import TOOL_NAMES

PROTO_VERSION: int = 1
MAX_FRAME_BYTES: int = 4 * 1024 * 1024
HEADER: struct.Struct = struct.Struct(">I")
OP_HELLO = "hello"
OP_STATUS = "status"
OP_TOOL_CALL = "tool_call"
OP_RECALL = "recall_context"
OP_ENQUEUE = "enqueue_turn"
OP_FLUSH = "flush_observations"
OP_NAMES: frozenset[str] = frozenset({OP_STATUS, OP_TOOL_CALL, OP_RECALL, OP_ENQUEUE, OP_FLUSH})
_CID = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_MESSAGES = {
    "E_STORE_ID": "store id mismatch",
    "E_SCHEMA": "schema mismatch",
    "E_FRAME": "frame rejected",
    "E_BACKLOG": "backlog limit",
    "E_UNCERTAIN": "outcome unknown",
    "E_VALIDATION": "invalid request",
    "E_PROFILE_DENIED": "profile not admitted",
    "E_CONNECTION_LIMIT": "connection limit",
    "E_TIMEOUT": "timeout",
    "E_UNAVAILABLE": "service unavailable",
    "E_NOT_READY": "service not ready",
    "E_PEERCRED_UNAVAILABLE": "peer credentials unavailable",
    "E_INTERNAL": "internal error",
}


class ProtocolError(Exception):
    def __init__(self, code: str, message: str = "", detail_code: str | None = None) -> None:
        super().__init__(message or _MESSAGES.get(code, ""))
        self.code = code
        self.detail_code = detail_code


class FrameRejectedError(ProtocolError):
    """Zero-length or over-cap request frame: close without any response frame."""

    def __init__(self) -> None:
        super().__init__("E_FRAME", detail_code="invalid_frame")


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError("E_VALIDATION")
        result[key] = value
    return result


def _check_depth(value: Any, depth: int = 0) -> None:
    if isinstance(value, dict):
        if depth > 32:
            raise ProtocolError("E_VALIDATION")
        for item in value.values():
            _check_depth(item, depth + 1)
    elif isinstance(value, list):
        if depth > 32:
            raise ProtocolError("E_VALIDATION")
        for item in value:
            _check_depth(item, depth + 1)


def _reject_nan(value: str) -> Any:
    # NaN/Infinity/-Infinity are valid JSON per ``json.loads`` but forbidden on
    # the wire (§5.2 "без NaN/Infinity"). Classified as E_VALIDATION (§6.4).
    raise ProtocolError("E_VALIDATION")


def encode_frame(payload: Mapping[str, Any]) -> bytes:
    body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
    if not 1 <= len(body) <= MAX_FRAME_BYTES:
        raise ProtocolError("E_FRAME")
    return HEADER.pack(len(body)) + body


async def read_frame(reader: asyncio.StreamReader, *, max_bytes: int = MAX_FRAME_BYTES) -> dict[str, Any]:
    try:
        (length,) = HEADER.unpack(await reader.readexactly(HEADER.size))
        if not 1 <= length <= max_bytes:
            # Zero/over-cap length: amplification guard, close without a reply.
            raise FrameRejectedError()
        body = (await reader.readexactly(length)).decode("utf-8", errors="strict")
        value = json.loads(body, object_pairs_hook=_no_duplicates, parse_constant=_reject_nan)
    except (asyncio.IncompleteReadError, UnicodeError, ValueError, RecursionError) as exc:
        raise ProtocolError("E_FRAME", detail_code="invalid_frame") from exc
    if not isinstance(value, dict):
        raise ProtocolError("E_VALIDATION")
    _check_depth(value)
    return value


def response_ok(correlation_id: str, **fields: Any) -> dict[str, Any]:
    return {"v": PROTO_VERSION, "ok": True, "correlation_id": correlation_id, **fields}


def response_error(
    correlation_id: str | None, code: str, message: str = "", detail_code: str | None = None,
) -> dict[str, Any]:
    result = {"v": PROTO_VERSION, "ok": False, "correlation_id": correlation_id,
              "code": code, "message": _MESSAGES.get(code, "internal error")}
    if detail_code is not None:
        result["detail_code"] = detail_code
    return result


def _version(payload: Mapping[str, Any]) -> None:
    if type(payload.get("v")) is not int or payload["v"] != PROTO_VERSION:
        raise ProtocolError("E_SCHEMA")


def _cid(payload: Mapping[str, Any]) -> str:
    cid = payload.get("correlation_id")
    if not isinstance(cid, str) or not _CID.fullmatch(cid):
        raise ProtocolError("E_VALIDATION")
    return cid


def validate_hello(
    payload: Mapping[str, Any], *, expected_store_id: str, schema_hash: str,
    allow_homes: tuple[Path, ...],
) -> None:
    _version(payload)
    if set(payload) != {"v", "op", "correlation_id", "payload"}:
        raise ProtocolError("E_VALIDATION")
    if payload["op"] != OP_HELLO:
        raise ProtocolError("E_VALIDATION")
    _cid(payload)
    fields = payload["payload"]
    if not isinstance(fields, dict) or set(fields) != {"schema_hash", "store_id", "claimed_home"}:
        raise ProtocolError("E_VALIDATION")
    store_id = fields["store_id"]
    try:
        canonical = isinstance(store_id, str) and str(uuid.UUID(store_id)) == store_id
    except (ValueError, AttributeError, TypeError):
        canonical = False
    if not canonical or store_id != expected_store_id:
        raise ProtocolError("E_STORE_ID")
    claim_hash = fields["schema_hash"]
    if not isinstance(claim_hash, str) or not _HASH.fullmatch(claim_hash) or claim_hash != schema_hash:
        raise ProtocolError("E_SCHEMA")
    home = fields["claimed_home"]
    if not isinstance(home, str) or "\x00" in home or not Path(home).is_absolute() or ".." in Path(home).parts:
        raise ProtocolError("E_PROFILE_DENIED")
    try:
        admitted = Path(home).resolve(strict=False) in allow_homes
    except (OSError, RuntimeError):
        admitted = False
    if not admitted:
        raise ProtocolError("E_PROFILE_DENIED")


def validate_request(payload: Mapping[str, Any]) -> tuple[str, str, dict[str, Any]]:
    _version(payload)
    if set(payload) != {"v", "op", "correlation_id", "session_id", "payload"}:
        raise ProtocolError("E_VALIDATION")
    cid = _cid(payload)
    op = payload["op"]
    session_id = payload["session_id"]
    fields = payload["payload"]
    if not isinstance(op, str) or op not in OP_NAMES:
        raise ProtocolError("E_VALIDATION")
    if not isinstance(session_id, str) or len(session_id) > 128 or "\x00" in session_id or not isinstance(fields, dict):
        raise ProtocolError("E_VALIDATION")
    return op, cid, fields


def validate_tool_call(fields: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    name = fields.get("name")
    if set(fields) != {"name", "args"} or not isinstance(name, str) or name not in TOOL_NAMES:
        raise ProtocolError("E_VALIDATION")
    if not isinstance(fields["args"], Mapping):
        raise ProtocolError("E_VALIDATION")
    return name, dict(fields["args"])


def tool_result_envelope(tool_name: str, result: str) -> dict[str, Any]:
    return {"v": PROTO_VERSION, "ok": True, "result": result}


_MAX_QUERY_CHARS: int = 8192
_MIN_MAX_RESULTS: int = 1
_MAX_MAX_RESULTS: int = 20


def validate_recall(fields: Mapping[str, Any]) -> tuple[str, int]:
    """Validate a ``recall_context`` payload (§5.5): query 0..8192, max_results 1..20."""
    if set(fields) - {"query", "max_results"} or "query" not in fields:
        raise ProtocolError("E_VALIDATION")
    query = fields["query"]
    if not isinstance(query, str) or len(query) > _MAX_QUERY_CHARS:
        raise ProtocolError("E_VALIDATION")
    count = fields.get("max_results", 5)
    if type(count) is not int or not _MIN_MAX_RESULTS <= count <= _MAX_MAX_RESULTS:
        raise ProtocolError("E_VALIDATION")
    return query, count


def validate_enqueue(fields: Mapping[str, Any]) -> Any:
    """Validate an ``enqueue_turn`` payload (§5.5): both accepted shapes.

    Accepts ``{"messages": <object|array>}`` or ``{"user_content", "assistant_content"}``.
    """
    if set(fields) == {"messages"}:
        messages = fields["messages"]
        if not isinstance(messages, (dict, list)):
            raise ProtocolError("E_VALIDATION")
        return messages
    if set(fields) == {"user_content", "assistant_content"}:
        user = fields["user_content"]
        assistant = fields["assistant_content"]
        if not isinstance(user, str) or not isinstance(assistant, str):
            raise ProtocolError("E_VALIDATION")
        return {"user_content": user, "assistant_content": assistant}
    raise ProtocolError("E_VALIDATION")


_READY_KEYS: frozenset[str] = frozenset({"v", "ok", "correlation_id", "state", "store_id", "epoch", "schema_hash"})


def validate_ready_reply(
    reply: Mapping[str, Any], *, correlation_id: str, expected_store_id: str, schema_hash: str,
) -> str:
    """Validate a hello READY response and return its ``epoch`` (canonical UUID4)."""
    if not isinstance(reply, Mapping) or set(reply) != _READY_KEYS:
        raise ProtocolError("E_VALIDATION")
    if type(reply["v"]) is not int or reply["v"] != PROTO_VERSION:
        raise ProtocolError("E_SCHEMA")
    if reply["ok"] is not True:
        raise ProtocolError("E_VALIDATION")
    if reply["correlation_id"] != correlation_id:
        raise ProtocolError("E_VALIDATION")
    if reply["state"] != "READY":
        raise ProtocolError("E_NOT_READY")
    store_id = reply["store_id"]
    try:
        canonical = isinstance(store_id, str) and str(uuid.UUID(store_id)) == store_id
    except (ValueError, AttributeError, TypeError):
        canonical = False
    if not canonical or store_id != expected_store_id:
        raise ProtocolError("E_STORE_ID")
    claim_hash = reply["schema_hash"]
    if not isinstance(claim_hash, str) or not _HASH.fullmatch(claim_hash) or claim_hash != schema_hash:
        raise ProtocolError("E_SCHEMA")
    epoch = reply["epoch"]
    try:
        canonical_epoch = (
            isinstance(epoch, str)
            and str(uuid.UUID(epoch)) == epoch
            and uuid.UUID(epoch).version == 4
        )
    except (ValueError, AttributeError, TypeError):
        canonical_epoch = False
    if not canonical_epoch:
        raise ProtocolError("E_VALIDATION")
    return epoch
