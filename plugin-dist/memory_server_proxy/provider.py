"""MemoryServerProxyProvider — a ``MemoryProvider`` that proxies the CMMS native owner.

Lifecycle: one short AF_UNIX session per operation (connect -> hello -> op ->
response -> close). No long-lived sockets, no server-side session cache. The owner
service is never closed by the client. ``is_available`` and ``unavailable_reason``
never touch the socket: they only read the sidecar and the bundled schema artifacts.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import os
import socket
import threading
import uuid
from pathlib import Path
from typing import Any

from agent.memory_provider import MemoryProvider

from . import settings
from . import wire

CONNECT_TIMEOUT = 2.0
HELLO_TIMEOUT = 5.0
ENQUEUE_TIMEOUT = 5.0
RECALL_TIMEOUT = 10.0
TOOL_TIMEOUT = 60.0
FLUSH_TIMEOUT = 30.0
MAX_RECALL_RESULTS = 5

_PLUGIN_DIR = Path(__file__).resolve().parent
_SCHEMA_PATH = _PLUGIN_DIR / "schema_v1.json"
_PROMPT_PATH = _PLUGIN_DIR / "system_prompt.txt"


class MemoryServerProxyProvider(MemoryProvider):
    """Standalone AF_UNIX client for an external CMMS native service."""

    def __init__(self) -> None:
        self._home = ""
        self._session_id = ""
        self._sidecar: dict | None = None
        self._schemas: list[dict] | None = None
        self._prompt: str | None = None
        self._cache: dict[str, tuple] = {}
        self._generation = 0
        self._epoch: str | None = None
        self._worker: threading.Thread | None = None
        self._pending_query: str | None = None
        self._worker_stop = threading.Event()
        self._stats = {"sent": 0, "errors": 0, "uncertain": 0, "dropped": 0}
        self._lock = threading.Lock()

    # -- Host contract ----------------------------------------------------

    @property
    def name(self) -> str:
        return "memory_server_proxy"

    def backup_paths(self) -> list[str]:
        return []

    def recall_status(self) -> None:
        return None

    # -- Internal helpers -------------------------------------------------

    def _resolve_home(self) -> str:
        return self._home or os.environ.get("HERMES_HOME", "") or ""

    def _read_sidecar(self) -> dict:
        home = self._resolve_home()
        if not home:
            raise ValueError("memory_server_proxy config missing (no HERMES_HOME)")
        data = settings.read_sidecar(home)
        if data is None:
            raise ValueError("memory_server_proxy config missing")
        settings.validate_sidecar(data)
        self._sidecar = data
        return data

    def _load_artifacts(self) -> list[dict]:
        """Lazily load schema_v1.json + system_prompt.txt and verify the hash."""
        if self._schemas is not None:
            return self._schemas
        try:
            document = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
            tools = document.get("tools")
            prompt = document.get("prompt")
            stored = document.get("schema_hash")
            if not isinstance(tools, list) or not isinstance(prompt, str) or not isinstance(stored, str):
                raise ValueError("malformed schema_v1.json")
            if wire.schema_hash_from_artifacts(tools, prompt) != stored:
                raise ValueError("schema artifact hash mismatch")
            prompt_text = _PROMPT_PATH.read_text(encoding="utf-8")
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            self._schemas = []
            if self._prompt is None:
                self._prompt = ""
            return self._schemas
        self._schemas = list(tools)
        self._prompt = prompt_text
        return self._schemas

    def _canonical_prompt(self) -> str:
        # system_prompt.txt == canonical prompt + one trailing newline (gen_schema).
        if self._prompt is None:
            self._load_artifacts()
        prompt = self._prompt or ""
        return prompt[:-1] if prompt.endswith("\n") else prompt

    def _schema_hash_value(self) -> str:
        self._load_artifacts()
        if not self._schemas:
            return ""
        return wire.schema_hash_from_artifacts(self._schemas, self._canonical_prompt())

    def _config_digest(self) -> str:
        home = self._resolve_home()
        if not home:
            return ""
        try:
            raw = (Path(home) / settings.CLIENT_CONFIG_NAME).read_bytes()
        except OSError:
            return ""
        return hashlib.sha256(raw).hexdigest()

    def _cid(self) -> str:
        return uuid.uuid4().hex

    def _record_epoch(self, new_epoch: str) -> None:
        with self._lock:
            if new_epoch and new_epoch != self._epoch:
                self._epoch = new_epoch
                self._cache.clear()

    def _open(self) -> socket.socket:
        """Connect + hello; returns the open socket or raises ``WireError``."""
        sidecar = self._sidecar
        if not sidecar:
            raise wire.WireError("E_UNAVAILABLE", "no sidecar")
        socket_path = sidecar.get("socket_path", "")
        store_id = sidecar.get("expected_store_id", "")
        schema_hash = self._schema_hash_value()
        if not schema_hash:
            raise wire.WireError("E_UNAVAILABLE", "no schema")
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        except OSError:
            raise wire.WireError("E_UNAVAILABLE", "socket create failed")
        try:
            sock.settimeout(CONNECT_TIMEOUT)
            sock.connect(socket_path)
        except OSError:
            sock.close()
            raise wire.WireError("E_UNAVAILABLE", "connect failed")
        try:
            sock.settimeout(HELLO_TIMEOUT)
            hello = {
                "v": wire.PROTO_VERSION,
                "op": wire.OP_HELLO,
                "correlation_id": self._cid(),
                **wire.hello_payload(schema_hash=schema_hash, store_id=store_id,
                                     claimed_home=self._home),
            }
            sock.sendall(wire.encode_frame(hello))
            resp = wire.read_frame_blocking(sock)
        except (OSError, wire.WireError):
            sock.close()
            raise
        if not resp.get("ok"):
            sock.close()
            raise wire.WireError(resp.get("code") or "E_UNAVAILABLE")
        epoch = resp.get("epoch")
        if isinstance(epoch, str):
            self._record_epoch(epoch)
        return sock

    def _request(self, op: str, payload: dict, *, timeout: float, session_id: str | None = None) -> dict:
        sock = self._open()
        try:
            sock.settimeout(timeout)
            frame = {
                "v": wire.PROTO_VERSION,
                "op": op,
                "correlation_id": self._cid(),
                "session_id": self._session_id if session_id is None else session_id,
                "payload": payload,
            }
            sock.sendall(wire.encode_frame(frame))
            return wire.read_frame_blocking(sock)
        finally:
            sock.close()

    def _availability(self) -> tuple[bool, str]:
        if self._sidecar is None:
            try:
                self._read_sidecar()
            except ValueError as exc:
                return False, str(exc)
            except OSError as exc:
                return False, f"memory_server_proxy config unreadable: {exc}"
        if not self._load_artifacts():
            return False, "schema artifact hash mismatch"
        return True, ""

    # -- Lifecycle --------------------------------------------------------

    def is_available(self) -> bool:
        return self._availability()[0]

    def unavailable_reason(self) -> str:
        return self._availability()[1]

    def initialize(self, session_id: str, **kwargs) -> None:
        home = kwargs.get("hermes_home")
        if home:
            self._home = str(home)
        self._session_id = session_id or ""
        try:
            self._read_sidecar()
        except (ValueError, OSError):
            self._sidecar = None
        self._load_artifacts()
        try:
            sock = self._open()
        except wire.WireError:
            # Offline owner: retain the surface, count the error, never raise.
            self._stats["errors"] += 1
            return
        sock.close()

    def system_prompt_block(self) -> str:
        self._load_artifacts()
        return self._prompt or ""

    def get_tool_schemas(self) -> list[dict]:
        return list(self._load_artifacts())

    def identity_signature(self) -> dict:
        self._load_artifacts()
        if self._sidecar is None:
            try:
                self._read_sidecar()
            except (ValueError, OSError):
                pass
        sidecar = self._sidecar or {}
        return {
            "provider": "memory_server_proxy",
            "home": self._home,
            "config": self._config_digest(),
            "store_id": sidecar.get("expected_store_id", ""),
            "schema_hash": self._schema_hash_value(),
        }

    # -- Recall -----------------------------------------------------------

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        sid = session_id or self._session_id
        with self._lock:
            entry = self._cache.get("prefetch")
            generation = self._generation
            epoch = self._epoch
        if entry is not None and entry[0] == query and entry[1] == sid:
            if entry[3] == generation and entry[4] == epoch and epoch is not None:
                return entry[2]
        return ""

    def queue_prefetch(self, query: str, *, session_id: str = "") -> None:
        sid = session_id or self._session_id
        with self._lock:
            if self._worker is not None and self._worker.is_alive():
                self._pending_query = query  # one replacement request
                return
            self._pending_query = None
            self._worker_stop.clear()
            generation = self._generation
            ctx = contextvars.copy_context()
            worker = threading.Thread(
                target=lambda: ctx.run(self._prefetch_worker, query, sid, generation),
                daemon=True,
                name="memory_server_proxy-prefetch",
            )
            self._worker = worker
            worker.start()

    def _recall(self, query: str, sid: str) -> str:
        try:
            resp = self._request(wire.OP_RECALL, {"query": query, "max_results": MAX_RECALL_RESULTS},
                                 timeout=RECALL_TIMEOUT, session_id=sid)
        except (wire.WireError, OSError):
            return ""
        if not resp.get("ok"):
            return ""
        result = resp.get("result")
        return result if isinstance(result, str) else ""

    def _publish(self, query: str, sid: str, text: str, generation: int) -> bool:
        with self._lock:
            if generation == self._generation and sid == self._session_id:
                self._cache["prefetch"] = (query, sid, text, generation, self._epoch)
                return True
            return False

    def _prefetch_worker(self, query: str, sid: str, generation: int) -> None:
        if self._worker_stop.is_set():
            return
        text = self._recall(query, sid)
        if not self._worker_stop.is_set():
            self._publish(query, sid, text, generation)
        with self._lock:
            pending = self._pending_query
            self._pending_query = None
            if pending is not None and not self._worker_stop.is_set():
                self._worker_stop.clear()
                next_generation = self._generation
                ctx = contextvars.copy_context()
                worker = threading.Thread(
                    target=lambda: ctx.run(self._prefetch_worker, pending, self._session_id,
                                           next_generation),
                    daemon=True,
                    name="memory_server_proxy-prefetch",
                )
                self._worker = worker
                worker.start()
            else:
                self._worker = None

    # -- Writes -----------------------------------------------------------

    def sync_turn(self, user_content: str, assistant_content: str, *, session_id: str = "",
                  messages: list[dict] | None = None) -> None:
        sid = session_id or self._session_id
        if not isinstance(messages, list):
            messages = [
                {"role": "user", "content": user_content},
                {"role": "assistant", "content": assistant_content},
            ]
        try:
            sock = self._open()
        except wire.WireError:
            self._stats["dropped"] += 1
            return
        try:
            sock.settimeout(ENQUEUE_TIMEOUT)
            frame = {
                "v": wire.PROTO_VERSION,
                "op": wire.OP_ENQUEUE,
                "correlation_id": self._cid(),
                "session_id": sid,
                "payload": {"messages": messages},
            }
            sock.sendall(wire.encode_frame(frame))
            try:
                resp = wire.read_frame_blocking(sock)
            except (wire.WireError, OSError):
                # Sent but no authoritative response: terminal, never retry.
                self._stats["uncertain"] += 1
                return
            if not resp.get("ok"):
                self._stats["dropped"] += 1
            else:
                self._stats["sent"] += 1
        finally:
            sock.close()

    def on_session_end(self, messages: list[dict]) -> None:
        try:
            self._request(wire.OP_FLUSH, {}, timeout=FLUSH_TIMEOUT)
        except (wire.WireError, OSError):
            self._stats["dropped"] += 1

    def on_session_switch(self, new_session_id: str, *, parent_session_id: str = "",
                          reset: bool = False, rewound: bool = False, **kwargs) -> None:
        try:
            self._request(wire.OP_FLUSH, {}, timeout=FLUSH_TIMEOUT)
        except (wire.WireError, OSError):
            self._stats["dropped"] += 1
        with self._lock:
            self._generation += 1
            self._cache.clear()
            self._session_id = new_session_id

    def shutdown(self) -> None:
        self._worker_stop.set()
        worker = self._worker
        if worker is not None and worker.is_alive():
            worker.join(timeout=2.0)
        with self._lock:
            self._worker = None
            self._pending_query = None

    # -- Tool dispatch ----------------------------------------------------

    def handle_tool_call(self, tool_name: str, args: dict, **kwargs) -> str:
        try:
            resp = self._request(wire.OP_TOOL_CALL, {"name": tool_name, "args": dict(args)},
                                 timeout=TOOL_TIMEOUT)
        except (wire.WireError, OSError):
            return json.dumps({"error": "memory_server_proxy unavailable",
                               "code": "E_UNAVAILABLE", "tool": tool_name})
        if not resp.get("ok") or "result" not in resp:
            return json.dumps({"error": "memory_server_proxy unavailable",
                               "code": "E_UNAVAILABLE", "tool": tool_name})
        result = resp.get("result")
        return result if isinstance(result, str) else json.dumps(result)

    # -- Config -----------------------------------------------------------

    def get_config_schema(self) -> list[dict]:
        return [
            {"key": "socket_path", "type": "text", "required": True,
             "description": "Absolute path to the CMMS native AF_UNIX socket."},
            {"key": "expected_store_id", "type": "text", "required": True,
             "description": "Canonical UUID of the CMMS store the native service owns."},
        ]

    def save_config(self, values: dict, hermes_home: str) -> None:
        settings.write_sidecar(hermes_home, socket_path=values.get("socket_path", ""),
                               expected_store_id=values.get("expected_store_id", ""))

    def read_config_fields(self, hermes_home: str) -> dict:
        data = settings.read_sidecar(hermes_home) or {}
        return {
            "values": {
                "socket_path": data.get("socket_path", ""),
                "expected_store_id": data.get("expected_store_id", ""),
            },
            "placeholders": {},
        }

    def write_config_fields(self, values: dict, hermes_home: str) -> None:
        current = settings.read_sidecar(hermes_home) or {}
        settings.write_sidecar(
            hermes_home,
            socket_path=values.get("socket_path", current.get("socket_path", "")),
            expected_store_id=values.get("expected_store_id", current.get("expected_store_id", "")),
        )
