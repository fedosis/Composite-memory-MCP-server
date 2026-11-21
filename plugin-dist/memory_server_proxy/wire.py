"""Standalone stdlib copy of the CMMS native AF_UNIX wire v1 contract.

The proxy plugin must not import ``memory_server``, so this module re-declares the
frame/envelope constants and codecs byte-for-byte. The duplication is deliberate
and is kept honest by parity assertions in ``tests/test_native_protocol.py``.

Unlike the owner's asyncio ``read_frame``, the client uses a synchronous blocking
reader because the plugin lives inside Hermes' sync hooks.
"""

from __future__ import annotations

import hashlib
import json
import socket
import struct
import uuid
from typing import Any, Mapping

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


class WireError(Exception):
    """Wire-level protocol error carrying a stable ``code``."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or _MESSAGES.get(code, "internal error"))
        self.code = code


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise WireError("E_VALIDATION")
        result[key] = value
    return result


def _check_depth(value: Any, depth: int = 0) -> None:
    if isinstance(value, dict):
        if depth > 32:
            raise WireError("E_VALIDATION")
        for item in value.values():
            _check_depth(item, depth + 1)
    elif isinstance(value, list):
        if depth > 32:
            raise WireError("E_VALIDATION")
        for item in value:
            _check_depth(item, depth + 1)


def _reject_nan(value: str) -> Any:
    # NaN/Infinity are valid JSON per ``json.loads`` but forbidden on the wire.
    raise WireError("E_VALIDATION")


def encode_frame(payload: Mapping[str, Any]) -> bytes:
    body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
    if not 1 <= len(body) <= MAX_FRAME_BYTES:
        raise WireError("E_FRAME")
    return HEADER.pack(len(body)) + body


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    chunks: list[bytes] = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise WireError("E_FRAME")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_frame_blocking(sock: socket.socket, *, max_bytes: int = MAX_FRAME_BYTES) -> dict[str, Any]:
    try:
        (length,) = HEADER.unpack(_recv_exact(sock, HEADER.size))
        if not 1 <= length <= max_bytes:
            raise WireError("E_FRAME")
        body = _recv_exact(sock, length).decode("utf-8", errors="strict")
        value = json.loads(body, object_pairs_hook=_no_duplicates, parse_constant=_reject_nan)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise WireError("E_FRAME") from exc
    if not isinstance(value, dict):
        raise WireError("E_VALIDATION")
    _check_depth(value)
    return value


def response_ok(correlation_id: str, **fields: Any) -> dict[str, Any]:
    return {"v": PROTO_VERSION, "ok": True, "correlation_id": correlation_id, **fields}


def response_error(correlation_id: str | None, code: str, message: str = "") -> dict[str, Any]:
    return {"v": PROTO_VERSION, "ok": False, "correlation_id": correlation_id,
            "code": code, "message": _MESSAGES.get(code, "internal error")}


def schema_hash_from_artifacts(tools: list[dict], prompt: str) -> str:
    """Recompute the canonical schema hash (same algorithm as ``native.gen_schema``)."""
    payload = {"v": PROTO_VERSION, "tools": tools, "prompt": prompt}
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def hello_payload(*, schema_hash: str, store_id: str, claimed_home: str) -> dict[str, Any]:
    """The hello payload fields; the caller nests them under ``payload``."""
    return {"schema_hash": schema_hash, "store_id": store_id, "claimed_home": claimed_home}


def peer_cred(sock: socket.socket) -> tuple[int, int, int]:
    """Linux ``SO_PEERCRED`` peer identity ``(pid, uid, gid)`` — stdlib only."""
    if not hasattr(socket, "SO_PEERCRED"):
        raise WireError("E_PEERCRED_UNAVAILABLE")
    try:
        data = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
        return struct.unpack("3i", data)
    except (OSError, struct.error) as exc:
        raise WireError("E_PEERCRED_UNAVAILABLE") from exc


_READY_KEYS: frozenset[str] = frozenset({"v", "ok", "correlation_id", "state", "store_id", "epoch", "schema_hash"})


def validate_ready(
    reply: Mapping[str, Any], *, correlation_id: str, expected_store_id: str, schema_hash: str,
) -> str:
    """Validate a hello READY response and return its ``epoch`` (canonical UUID4)."""
    if not isinstance(reply, Mapping) or set(reply) != _READY_KEYS:
        raise WireError("E_VALIDATION")
    if type(reply["v"]) is not int or reply["v"] != PROTO_VERSION:
        raise WireError("E_SCHEMA")
    if reply["ok"] is not True:
        raise WireError("E_VALIDATION")
    if reply["correlation_id"] != correlation_id:
        raise WireError("E_VALIDATION")
    if reply["state"] != "READY":
        raise WireError("E_NOT_READY")
    store_id = reply["store_id"]
    try:
        canonical = isinstance(store_id, str) and str(uuid.UUID(store_id)) == store_id
    except (ValueError, AttributeError, TypeError):
        canonical = False
    if not canonical or store_id != expected_store_id:
        raise WireError("E_STORE_ID")
    claim_hash = reply["schema_hash"]
    if not isinstance(claim_hash, str) or len(claim_hash) != 64 or not all(c in "0123456789abcdef" for c in claim_hash):
        raise WireError("E_SCHEMA")
    if claim_hash != schema_hash:
        raise WireError("E_SCHEMA")
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
        raise WireError("E_VALIDATION")
    return epoch
