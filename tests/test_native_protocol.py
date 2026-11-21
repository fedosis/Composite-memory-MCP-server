"""Wire v1 contract checks without a real store."""

import asyncio
import json
import os
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from memory_server.native import service as native_service
from memory_server.native.backend import NativeBackendAdapter
from memory_server.native.config import load_service_config
from memory_server.native.protocol import (
    HEADER,
    MAX_FRAME_BYTES,
    FrameRejectedError,
    ProtocolError,
    encode_frame,
    read_frame,
    response_error,
    response_ok,
    tool_result_envelope,
    validate_enqueue,
    validate_hello,
    validate_ready_reply,
    validate_recall,
    validate_request,
    validate_tool_call,
)
from memory_server.runtime.runtime import ShutdownReport
from tests._native_helpers import make_service_config


def reader_for(data):
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


def raw(body):
    return HEADER.pack(len(body)) + body


async def test_frame_roundtrip_at_cap():
    # JSON string envelope overhead is fixed; binary body length reaches the cap exactly.
    overhead = len(encode_frame({"x": ""})) - HEADER.size
    payload = {"x": "a" * (MAX_FRAME_BYTES - overhead)}
    frame = encode_frame(payload)
    assert len(frame) - HEADER.size == MAX_FRAME_BYTES
    assert await read_frame(reader_for(frame)) == payload


async def test_frame_over_cap_rejected():
    with pytest.raises(ProtocolError, match="frame rejected") as exc:
        await read_frame(reader_for(HEADER.pack(MAX_FRAME_BYTES + 1)))
    assert exc.value.code == "E_FRAME"


async def test_zero_length_rejected():
    with pytest.raises(ProtocolError) as exc:
        await read_frame(reader_for(HEADER.pack(0)))
    assert exc.value.code == "E_FRAME"


async def test_duplicate_json_key_rejected():
    with pytest.raises(ProtocolError) as exc:
        await read_frame(reader_for(raw(b'{"a":{"b":1,"b":2}}')))
    assert exc.value.code == "E_VALIDATION"


@pytest.mark.parametrize("change", [{"op": "unknown"}, {"extraneous": 1}])
def test_unknown_op_rejected(change):
    request = {"v": 1, "op": "status", "correlation_id": "cid", "session_id": "", "payload": {}}
    request.update(change)
    with pytest.raises(ProtocolError) as exc:
        validate_request(request)
    assert exc.value.code == "E_VALIDATION"


def test_unknown_envelope_field_rejected():
    with pytest.raises(ProtocolError) as exc:
        validate_request({"v": 1, "op": "status", "correlation_id": "id", "session_id": "",
                          "payload": {}, "extra": True})
    assert exc.value.code == "E_VALIDATION"


def test_unknown_tool_rejected():
    with pytest.raises(ProtocolError) as exc:
        validate_tool_call({"name": "add_relation", "args": {}})
    assert exc.value.code == "E_VALIDATION"


def hello(home: Path):
    return {"v": 1, "op": "hello", "correlation_id": "cid",
            "payload": {"store_id": str(uuid.uuid4()), "schema_hash": "a" * 64,
                        "claimed_home": str(home)}}


def test_hello_store_id_mismatch(tmp_path):
    claim = hello(tmp_path)
    with pytest.raises(ProtocolError) as exc:
        validate_hello(claim, expected_store_id=str(uuid.uuid4()), schema_hash="a" * 64,
                       allow_homes=(tmp_path,))
    assert exc.value.code == "E_STORE_ID"


def test_hello_schema_mismatch(tmp_path):
    claim = hello(tmp_path)
    with pytest.raises(ProtocolError) as exc:
        validate_hello(claim, expected_store_id=claim["payload"]["store_id"], schema_hash="b" * 64,
                       allow_homes=(tmp_path,))
    assert exc.value.code == "E_SCHEMA"


def test_hello_home_not_allowed(tmp_path):
    claim = hello(tmp_path)
    with pytest.raises(ProtocolError) as exc:
        validate_hello(claim, expected_store_id=claim["payload"]["store_id"], schema_hash="a" * 64,
                       allow_homes=(tmp_path / "elsewhere",))
    assert exc.value.code == "E_PROFILE_DENIED"


def test_hello_rejects_uppercase_hash(tmp_path):
    claim = hello(tmp_path)
    claim["payload"]["schema_hash"] = "A" * 64
    with pytest.raises(ProtocolError) as exc:
        validate_hello(claim, expected_store_id=claim["payload"]["store_id"], schema_hash="A" * 64,
                       allow_homes=(tmp_path,))
    assert exc.value.code == "E_SCHEMA"


def test_hello_requires_nested_payload(tmp_path):
    claim = hello(tmp_path)
    # The real owner must not accept the legacy flat envelope or extra payload fields.
    for invalid in ({**claim["payload"], "v": 1, "op": "hello", "correlation_id": "cid"},
                    {**claim, "payload": {**claim["payload"], "extra": True}}):
        with pytest.raises(ProtocolError) as exc:
            validate_hello(invalid, expected_store_id=claim["payload"]["store_id"], schema_hash="a" * 64,
                           allow_homes=(tmp_path,))
        assert exc.value.code == "E_VALIDATION"


async def test_tool_result_passthrough():
    result = "  Привет 🌍\n  "
    envelope = tool_result_envelope("ping", result)
    assert envelope == {"v": 1, "ok": True, "result": result}
    assert (await read_frame(reader_for(encode_frame(envelope))))["result"].encode() == result.encode()


async def test_top_level_array_is_validation_not_frame():
    with pytest.raises(ProtocolError) as exc:
        await read_frame(reader_for(raw(json.dumps([1, 2]).encode())))
    assert exc.value.code == "E_VALIDATION"


async def test_owner_listener_ready_and_inode_safe_shutdown(tmp_path, monkeypatch):
    config = load_service_config(make_service_config(tmp_path, store_id=str(uuid.uuid4())))
    layout = SimpleNamespace(
        unavailable_projections=frozenset(),
        sqlite=SimpleNamespace(local_path=config.data_root / "memory.db"),
        vector=SimpleNamespace(local_path=config.data_root / "lancedb"),
        graph_snapshot_path=config.data_root / "graph.json",
    )
    monkeypatch.setattr(native_service, "preflight_layout", lambda config, settings: layout)
    monkeypatch.setattr(native_service, "get_settings", lambda: None)

    class Adapter:
        async def start(self):
            pass

        async def pending_snapshot(self):
            return SimpleNamespace(live_pending=2, terminal_failed=1)

        async def close(self):
            return ShutdownReport()

    owner = native_service.NativeOwnerService(config, cast(NativeBackendAdapter, Adapter()))
    await owner.start()
    assert owner.state == native_service.STATE_READY
    assert config.socket_path.exists()
    try:
        reader, writer = await asyncio.open_unix_connection(str(config.socket_path))
        from memory_server.native.schema_v1 import schema_hash

        greeting = hello(config.allow_homes[0])
        greeting["payload"]["store_id"] = config.store_id
        greeting["payload"]["schema_hash"] = schema_hash()
        writer.write(encode_frame(greeting))
        await writer.drain()
        ready = await read_frame(reader)
        assert ready["state"] == "READY" and ready["epoch"] == owner.epoch
        writer.write(encode_frame({"v": 1, "op": "status", "correlation_id": "req",
                                   "session_id": "", "payload": {}}))
        await writer.drain()
        status = await read_frame(reader)
        assert status["pending"] == {"live_pending": 2, "terminal_failed": 1}
        assert status["store_paths"] == {"sqlite": "memory.db", "vector": "lancedb", "graph": "graph.json"}
        writer.close()
        await writer.wait_closed()
    finally:
        await owner.stop()
    assert owner.state == native_service.STATE_CLOSED
    assert not config.socket_path.exists()


async def test_owner_failed_close_retains_socket(tmp_path, monkeypatch):
    config = load_service_config(make_service_config(tmp_path, store_id=str(uuid.uuid4())))
    monkeypatch.setattr(native_service, "preflight_layout", lambda config, settings: None)
    monkeypatch.setattr(native_service, "get_settings", lambda: None)

    class Adapter:
        async def start(self):
            pass

        async def close(self):
            return ShutdownReport(lock_retained=True)

    owner = native_service.NativeOwnerService(config, cast(NativeBackendAdapter, Adapter()))
    await owner.start()
    await owner.stop()
    assert owner.state == native_service.STATE_FAILED_CLOSING
    assert owner.exit_code == 3
    assert config.socket_path.exists()
    config.socket_path.unlink()  # only the throwaway test socket


# ----------------------------------------------------------- v12 additions
async def test_frame_over_cap_is_frame_rejected():
    with pytest.raises(FrameRejectedError):
        await read_frame(reader_for(HEADER.pack(MAX_FRAME_BYTES + 1)))


async def test_bad_utf8_is_invalid_frame():
    body = b"\xff\xfe\xfa"
    with pytest.raises(ProtocolError) as exc:
        await read_frame(reader_for(raw(body)))
    assert exc.value.code == "E_FRAME"
    assert exc.value.detail_code == "invalid_frame"


async def test_nan_rejected_as_validation():
    with pytest.raises(ProtocolError) as exc:
        await read_frame(reader_for(raw(b'{"a": NaN}')))
    assert exc.value.code == "E_VALIDATION"


def test_response_error_detail_code():
    assert response_error("cid", "E_FRAME", detail_code="invalid_frame")["detail_code"] == "invalid_frame"
    assert "detail_code" not in response_error("cid", "E_BACKLOG")


def test_session_id_nul_rejected():
    with pytest.raises(ProtocolError) as exc:
        validate_request({"v": 1, "op": "status", "correlation_id": "id",
                          "session_id": "a\x00b", "payload": {}})
    assert exc.value.code == "E_VALIDATION"


def test_recall_validation_bounds():
    assert validate_recall({"query": "", "max_results": 1}) == ("", 1)
    assert validate_recall({"query": "q", "max_results": 20}) == ("q", 20)
    assert validate_recall({"query": "x" * 8192}) == ("x" * 8192, 5)
    for bad in (
        {"query": "x" * 8193},
        {"query": "q", "max_results": 0},
        {"query": "q", "max_results": 21},
        {"query": 5},
        {"query": "q", "extra": 1},
    ):
        with pytest.raises(ProtocolError) as exc:
            validate_recall(bad)
        assert exc.value.code == "E_VALIDATION"


def test_enqueue_validation_forms():
    assert validate_enqueue({"messages": [{"role": "user", "content": "hi"}]}) == [
        {"role": "user", "content": "hi"}
    ]
    assert validate_enqueue({"messages": {"user_content": "u", "assistant_content": "a"}}) == {
        "user_content": "u", "assistant_content": "a"
    }
    assert validate_enqueue({"user_content": "u", "assistant_content": "a"}) == {
        "user_content": "u", "assistant_content": "a"
    }
    for bad in (
        {"messages": "not-a-list"},
        {"messages": 5},
        {"user_content": "u"},
        {"user_content": "u", "assistant_content": 5},
        {},
    ):
        with pytest.raises(ProtocolError) as exc:
            validate_enqueue(bad)
        assert exc.value.code == "E_VALIDATION"


def test_validate_ready_reply_ok():
    store_id = str(uuid.uuid4())
    reply = {"v": 1, "ok": True, "correlation_id": "cid", "state": "READY",
             "store_id": store_id, "epoch": str(uuid.uuid4()), "schema_hash": "a" * 64}
    assert validate_ready_reply(reply, correlation_id="cid", expected_store_id=store_id,
                                schema_hash="a" * 64) == reply["epoch"]


def test_validate_ready_reply_rejects():
    store_id = str(uuid.uuid4())
    base = {"v": 1, "ok": True, "correlation_id": "cid", "state": "READY",
            "store_id": store_id, "epoch": str(uuid.uuid4()), "schema_hash": "a" * 64}
    with pytest.raises(ProtocolError) as exc:
        validate_ready_reply({**base, "extra": 1}, correlation_id="cid",
                             expected_store_id=store_id, schema_hash="a" * 64)
    assert exc.value.code == "E_VALIDATION"
    with pytest.raises(ProtocolError) as exc:
        validate_ready_reply({**base, "correlation_id": "other"}, correlation_id="cid",
                             expected_store_id=store_id, schema_hash="a" * 64)
    assert exc.value.code == "E_VALIDATION"
    with pytest.raises(ProtocolError) as exc:
        validate_ready_reply({**base, "epoch": "not-a-uuid"}, correlation_id="cid",
                             expected_store_id=store_id, schema_hash="a" * 64)
    assert exc.value.code == "E_VALIDATION"
    with pytest.raises(ProtocolError) as exc:
        validate_ready_reply({**base, "store_id": str(uuid.uuid4())}, correlation_id="cid",
                             expected_store_id=store_id, schema_hash="a" * 64)
    assert exc.value.code == "E_STORE_ID"


class _FakeWriter:
    def __init__(self) -> None:
        self.buf = bytearray()

    def write(self, data: bytes) -> None:
        self.buf.extend(data)

    async def drain(self) -> None:
        pass


async def test_oversized_result_emits_result_too_large(tmp_path, monkeypatch):
    config = load_service_config(make_service_config(tmp_path, store_id=str(uuid.uuid4())))
    monkeypatch.setattr(native_service, "preflight_layout", lambda config, settings: None)
    monkeypatch.setattr(native_service, "get_settings", lambda: None)

    class Adapter:
        async def start(self):
            pass

    owner = native_service.NativeOwnerService(config, cast(NativeBackendAdapter, Adapter()))
    writer = _FakeWriter()
    huge = "x" * (MAX_FRAME_BYTES + 1)
    await owner._send(writer, response_ok("cid", result=huge))
    body = bytes(writer.buf)[HEADER.size:]
    frame = json.loads(body.decode("utf-8"))
    assert frame["ok"] is False
    assert frame["code"] == "E_FRAME"
    assert frame["detail_code"] == "result_too_large"
    assert frame["message"] == "frame rejected"


async def test_start_rollback_closes_adapter_and_unlinks(tmp_path, monkeypatch):
    config = load_service_config(make_service_config(tmp_path, store_id=str(uuid.uuid4())))
    monkeypatch.setattr(native_service, "preflight_layout", lambda config, settings: None)
    monkeypatch.setattr(native_service, "get_settings", lambda: None)

    closed: dict[str, bool] = {}

    class Adapter:
        async def start(self):
            pass

        async def close(self):
            closed["adapter"] = True
            return ShutdownReport()

    owner = native_service.NativeOwnerService(config, cast(NativeBackendAdapter, Adapter()))

    def _boom_chmod(path, mode):
        raise OSError("chmod denied")

    monkeypatch.setattr(native_service.os, "chmod", _boom_chmod)

    with pytest.raises(OSError):
        await owner.start()

    # A post-start failure rolls back: adapter closed, own inode removed (§9.2).
    assert closed.get("adapter") is True
    assert owner.state == native_service.STATE_CLOSED
    assert not config.socket_path.exists()
