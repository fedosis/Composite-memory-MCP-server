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
_HASH = re.compile(r"[0-9a-fA-F]{64}\Z")
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
    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or _MESSAGES.get(code, ""))
        self.code = code


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


def encode_frame(payload: Mapping[str, Any]) -> bytes:
    body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
    if not 1 <= len(body) <= MAX_FRAME_BYTES:
        raise ProtocolError("E_FRAME")
    return HEADER.pack(len(body)) + body


async def read_frame(reader: asyncio.StreamReader, *, max_bytes: int = MAX_FRAME_BYTES) -> dict[str, Any]:
    try:
        (length,) = HEADER.unpack(await reader.readexactly(HEADER.size))
        if not 1 <= length <= max_bytes:
            raise ProtocolError("E_FRAME")
        body = (await reader.readexactly(length)).decode("utf-8", errors="strict")
        value = json.loads(body, object_pairs_hook=_no_duplicates)
    except (asyncio.IncompleteReadError, UnicodeError, ValueError, RecursionError) as exc:
        raise ProtocolError("E_FRAME") from exc
    if not isinstance(value, dict):
        raise ProtocolError("E_VALIDATION")
    _check_depth(value)
    return value


def response_ok(correlation_id: str, **fields: Any) -> dict[str, Any]:
    return {"v": PROTO_VERSION, "ok": True, "correlation_id": correlation_id, **fields}


def response_error(correlation_id: str | None, code: str, message: str = "") -> dict[str, Any]:
    return {"v": PROTO_VERSION, "ok": False, "correlation_id": correlation_id,
            "code": code, "message": _MESSAGES.get(code, "internal error")}


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
    if set(payload) != {"v", "op", "correlation_id", "schema_hash", "store_id", "claimed_home"}:
        raise ProtocolError("E_VALIDATION")
    if payload["op"] != OP_HELLO:
        raise ProtocolError("E_VALIDATION")
    _cid(payload)
    store_id = payload["store_id"]
    try:
        canonical = isinstance(store_id, str) and str(uuid.UUID(store_id)) == store_id
    except (ValueError, AttributeError, TypeError):
        canonical = False
    if not canonical or store_id != expected_store_id:
        raise ProtocolError("E_STORE_ID")
    claim_hash = payload["schema_hash"]
    if not isinstance(claim_hash, str) or not _HASH.fullmatch(claim_hash) or claim_hash != schema_hash:
        raise ProtocolError("E_SCHEMA")
    home = payload["claimed_home"]
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
    if not isinstance(session_id, str) or len(session_id) > 128 or not isinstance(fields, dict):
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
