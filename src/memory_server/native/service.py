"""Single-owner AF_UNIX listener and bounded native request admission."""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import stat
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any

from memory_server.native.backend import BackendStartError, NativeBackendAdapter
from memory_server.native.config import ServiceConfig, ServiceConfigError, preflight_layout
from memory_server.native.peercred import PeerCredentialError, require_peer_uid
from memory_server.native.protocol import (
    OP_ENQUEUE,
    OP_FLUSH,
    OP_RECALL,
    OP_STATUS,
    OP_TOOL_CALL,
    FrameRejectedError,
    ProtocolError,
    encode_frame,
    read_frame,
    response_error,
    response_ok,
    validate_enqueue,
    validate_hello,
    validate_recall,
    validate_request,
    validate_tool_call,
)
from memory_server.native.schema_v1 import schema_hash
from memory_server.plugins.hermes.writer import WriterBacklogError
from memory_server.runtime.errors import StorageOwnershipError
from memory_server.runtime.runtime import ShutdownReport
from memory_server.settings import get_settings

STATE_STARTING = "STARTING"
STATE_READY = "READY"
STATE_CLOSING = "CLOSING"
STATE_FAILED_CLOSING = "FAILED_CLOSING"
STATE_CLOSED = "CLOSED"


_MUTATING_TOOLS = frozenset({"remember", "learn", "invalidate", "set_belief", "resolve_conflict"})


class StaleSocketError(RuntimeError):
    code = "E_STALE_SOCKET"


@dataclass
class ServiceStats:
    accepted: int = 0
    rejected: int = 0
    errors: int = 0
    active: int = 0


class NativeOwnerService:
    def __init__(self, config: ServiceConfig, adapter: NativeBackendAdapter, *, clock=time.monotonic) -> None:
        self.config = config
        self.adapter = adapter
        self.clock = clock
        self.stats = ServiceStats()
        self._state = STATE_STARTING
        self._epoch = ""
        self._server: asyncio.AbstractServer | None = None
        self._socket_identity: tuple[int, int] | None = None
        self._connections: set[asyncio.Task[Any]] = set()
        self._operations = asyncio.Semaphore(config.limits.max_operations)
        self._layout: Any = None
        self.exit_code = 0

    @property
    def state(self) -> str:
        return self._state

    @property
    def epoch(self) -> str:
        return self._epoch

    async def start(self) -> None:
        self._layout = preflight_layout(self.config, get_settings())
        await self.adapter.start()
        try:
            await self._bind_and_ready()
        except BaseException:
            await self._rollback_start()
            raise

    async def _bind_and_ready(self) -> None:
        try:
            os.lstat(self.config.socket_path)
        except FileNotFoundError:
            pass
        else:
            raise StaleSocketError("E_STALE_SOCKET")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(self.config.socket_path))
            # Record identity immediately after bind so rollback can reclaim
            # our own inode even if chmod/start_unix_server later fails.
            info = os.lstat(self.config.socket_path)
            self._socket_identity = (info.st_dev, info.st_ino)
            kwargs: dict[str, Any] = {"sock": listener, "backlog": 16}
            # Since Python 3.13 asyncio otherwise unlinks the socket on close,
            # including FAILED_CLOSING where the pathname must be retained.
            if sys.version_info >= (3, 13):
                kwargs["cleanup_socket"] = False
            self._server = await asyncio.start_unix_server(self._handle_conn, **kwargs)
        except BaseException:
            listener.close()
            raise
        try:
            os.chmod(self.config.socket_path, 0o600)
        except BaseException:
            self._server.close()
            await self._server.wait_closed()
            raise
        self._epoch = str(uuid.uuid4())
        self._state = STATE_READY

    async def _rollback_start(self) -> None:
        """Undo a partially-started owner after ``adapter.start()`` succeeded."""
        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None
        if self._socket_identity is not None:
            try:
                info = os.lstat(self.config.socket_path)
                if stat.S_ISSOCK(info.st_mode) and (info.st_dev, info.st_ino) == self._socket_identity:
                    os.unlink(self.config.socket_path)
            except (FileNotFoundError, OSError):
                pass
        try:
            await self.adapter.close()
        except Exception:
            pass
        self._state = STATE_CLOSED

    async def run(self) -> int:
        try:
            await self.start()
        except ServiceConfigError:
            self.exit_code = 2
            return self.exit_code
        except StaleSocketError:
            self.exit_code = 4
            return self.exit_code
        except StorageOwnershipError:
            self.exit_code = 5
            return self.exit_code
        except BackendStartError as exc:
            self.exit_code = 5 if isinstance(exc.cause, StorageOwnershipError) else 3
            return self.exit_code
        except Exception:
            self.exit_code = 3
            return self.exit_code
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        assert task is not None
        # asyncio.run handles SIGINT, not SIGTERM. Cancel the serving task so
        # run() reaches stop() rather than letting the default signal kill us.
        try:
            loop.add_signal_handler(signal.SIGTERM, task.cancel)
            term_handler = True
        except (NotImplementedError, RuntimeError, ValueError):
            term_handler = False
        try:
            assert self._server is not None
            await self._server.serve_forever()
        except asyncio.CancelledError:
            pass
        finally:
            if term_handler:
                loop.remove_signal_handler(signal.SIGTERM)
            await self.stop()
        return self.exit_code

    async def stop(self, *, deadline: float | None = None) -> ShutdownReport:
        if self._state in (STATE_CLOSED, STATE_FAILED_CLOSING):
            return self._close_report
        self._state = STATE_CLOSING
        if self._server is not None:
            self._server.close()
        waiting = set(self._connections)
        waiting.discard(asyncio.current_task())
        if waiting:
            budget = self.config.limits.pre_drain_s
            if deadline is not None:
                budget = min(budget, max(0.0, deadline - self.clock()))
            await asyncio.wait(waiting, timeout=budget)
        try:
            report = await self.adapter.close()
        except Exception:
            report = ShutdownReport(code="E_RUNTIME_CLEANUP_FAILED", lock_retained=True)
        if report is None:
            report = ShutdownReport()
        self._close_report = report
        if report.lock_retained or report.retained_resources or report.code not in (None, "E_RUNTIME_FLUSH_FAILED"):
            self._state = STATE_FAILED_CLOSING
            self.exit_code = 3
            return report
        if self._socket_identity is not None:
            try:
                info = os.lstat(self.config.socket_path)
            except FileNotFoundError:
                info = None
            except OSError:
                self._state = STATE_FAILED_CLOSING
                self.exit_code = 3
                return report
            if info is not None:
                if stat.S_ISSOCK(info.st_mode) and (info.st_dev, info.st_ino) == self._socket_identity:
                    os.unlink(self.config.socket_path)
                else:
                    # Foreign/substituted pathname: leave it, still a clean close (§9.3).
                    print(
                        f"native-serve: socket pathname no longer ours, left in place: "
                        f"{self.config.socket_path}",
                        file=sys.stderr,
                    )
        self._state = STATE_CLOSED
        self.exit_code = 0
        return report

    def status(self) -> dict[str, Any]:
        layout = self._layout
        unavailable = sorted(layout.unavailable_projections) if layout is not None else []
        paths = {"sqlite": "", "vector": "", "graph": ""}
        if layout is not None:
            paths = {
                "sqlite": layout.sqlite.local_path.name if layout.sqlite.local_path is not None else "",
                "vector": layout.vector.local_path.name if layout.vector.local_path is not None else "",
                "graph": layout.graph_snapshot_path.name,
            }
        return {"state": self._state, "store_id": self.config.store_id, "epoch": self._epoch,
                "projections": {"unavailable": unavailable, "degraded": bool(unavailable)},
                "store_paths": paths}

    async def _status_payload(self) -> dict[str, Any]:
        snapshot = await self.adapter.pending_snapshot()
        return {
            **self.status(),
            "rejected": self.stats.rejected,
            "pending": {"live_pending": snapshot.live_pending,
                        "terminal_failed": snapshot.terminal_failed},
        }

    async def _send(self, writer: asyncio.StreamWriter, payload: dict[str, Any]) -> None:
        try:
            frame = encode_frame(payload)
        except ProtocolError as exc:
            if exc.code != "E_FRAME":
                raise
            # Oversized response (tool result > frame cap): E_FRAME/result_too_large.
            cid = payload.get("correlation_id") if isinstance(payload, dict) else None
            frame = encode_frame(response_error(cid, "E_FRAME", detail_code="result_too_large"))
        writer.write(frame)
        await asyncio.wait_for(writer.drain(), timeout=0.1)

    async def _handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if self.stats.active >= self.config.limits.max_connections:
            self.stats.rejected += 1
            try:
                await self._send(writer, response_error(None, "E_CONNECTION_LIMIT"))
            except (Exception, asyncio.CancelledError):
                pass
            writer.close()
            return
        self.stats.active += 1
        if task is not None:
            self._connections.add(task)
        cid: str | None = None
        try:
            if self._state != STATE_READY:
                raise ProtocolError("E_NOT_READY")
            sock = writer.get_extra_info("socket")
            require_peer_uid(sock, os.geteuid())
            hello = await asyncio.wait_for(
                read_frame(reader, max_bytes=self.config.limits.frame_bytes),
                timeout=self.config.limits.hello_timeout_s,
            )
            validate_hello(hello, expected_store_id=self.config.store_id,
                           schema_hash=schema_hash(), allow_homes=self.config.allow_homes)
            cid = hello["correlation_id"]
            await self._send(writer, response_ok(cid, state=STATE_READY, store_id=self.config.store_id,
                                                 epoch=self._epoch, schema_hash=schema_hash()))
            request = await asyncio.wait_for(
                read_frame(reader, max_bytes=self.config.limits.frame_bytes),
                timeout=self.config.limits.read_idle_s,
            )
            cid = request.get("correlation_id") if isinstance(request.get("correlation_id"), str) else None
            op, cid, fields = validate_request(request)
            session_id = request["session_id"]
            try:
                await asyncio.wait_for(self._operations.acquire(), timeout=self._slot_budget(op))
            except TimeoutError:
                # Admission slot not acquired within the op deadline: the op was
                # never dispatched, so a retry-safe code is correct (§6.2/§6.3).
                self.stats.errors += 1
                await self._best_effort(writer, response_error(cid, "E_UNAVAILABLE"))
                return
            try:
                result = await self._dispatch(op, cid, session_id, fields)
            finally:
                self._operations.release()
            await self._send(writer, result)
            self.stats.accepted += 1
        except PeerCredentialError:
            self.stats.rejected += 1
            cid = None
            await self._best_effort(writer, response_error(None, "E_PROFILE_DENIED"))
        except FrameRejectedError:
            # Amplification guard: zero/over-cap frame, close without a reply (§5.2 p.1).
            self.stats.rejected += 1
        except (ProtocolError, TimeoutError) as exc:
            self.stats.rejected += 1
            if isinstance(exc, ProtocolError):
                code = exc.code
                detail_code = exc.detail_code
            else:
                code = "E_TIMEOUT"
                detail_code = None
            await self._best_effort(writer, response_error(cid, code, detail_code=detail_code))
        except Exception:
            self.stats.errors += 1
            await self._best_effort(writer, response_error(cid, "E_INTERNAL"))
        finally:
            self.stats.active -= 1
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=0.1)
            except (Exception, asyncio.CancelledError):
                pass
            if task is not None:
                self._connections.discard(task)

    async def _best_effort(self, writer: asyncio.StreamWriter, payload: dict[str, Any]) -> None:
        try:
            await self._send(writer, payload)
        except (Exception, asyncio.CancelledError):
            pass

    def _slot_budget(self, op: str) -> float:
        """Per-operation deadline for the ``max_operations`` admission slot (§5.6)."""
        limits = self.config.limits
        if op == OP_TOOL_CALL:
            return limits.tool_timeout_s
        if op == OP_RECALL:
            return limits.recall_timeout_s
        if op == OP_ENQUEUE:
            return limits.enqueue_timeout_s
        if op == OP_FLUSH:
            return limits.flush_timeout_s
        return limits.read_idle_s

    async def _dispatch(self, op: str, cid: str, session_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        try:
            if op == OP_STATUS:
                return response_ok(cid, **await self._status_payload())
            if op == OP_TOOL_CALL:
                name, args = validate_tool_call(fields)
                return response_ok(cid, result=await self.adapter.execute_tool_async(name, args))
            if op == OP_RECALL:
                query, count = validate_recall(fields)
                return response_ok(cid, result=await self.adapter.recall_context_async(
                    query, session_id=session_id, max_results=count))
            if op == OP_ENQUEUE:
                messages = validate_enqueue(fields)
                await self.adapter.enqueue_turn_async(messages, session_id=session_id)
                return response_ok(cid, accepted=True)
            if op == OP_FLUSH:
                if fields:
                    raise ProtocolError("E_VALIDATION")
                return response_ok(cid, **await self.adapter.flush_observations_async())
            raise ProtocolError("E_VALIDATION")
        except WriterBacklogError as exc:
            # The sub-reason was captured from the same snapshot that rejected the
            # enqueue; do not re-read pending_snapshot() here (§6.1).
            detail = exc.detail_code or "live_limit"
            return {**response_error(cid, "E_BACKLOG"), "detail_code": detail, "accepted": False}
        except TimeoutError:
            uncertain = op in (OP_ENQUEUE, OP_FLUSH) or (
                op == OP_TOOL_CALL and fields.get("name") in _MUTATING_TOOLS
            )
            return response_error(cid, "E_UNCERTAIN" if uncertain else "E_TIMEOUT")
        except ProtocolError as exc:
            return response_error(cid, exc.code)
        except Exception:
            self.stats.errors += 1
            return response_error(cid, "E_INTERNAL")


async def serve(config: ServiceConfig) -> int:
    return await NativeOwnerService(config, NativeBackendAdapter(config)).run()
