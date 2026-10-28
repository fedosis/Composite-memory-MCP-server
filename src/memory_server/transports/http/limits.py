"""HTTP resource ceilings and bounded ASGI request-body helpers."""

from __future__ import annotations

import asyncio
import ipaddress
from collections.abc import Awaitable, Callable
from time import monotonic
from typing import Any
from urllib.parse import urlsplit

from uvicorn.protocols.http.h11_impl import H11Protocol

from memory_server.gateway_contracts import ports
from memory_server.gateway_contracts.ports import HttpConfig

from .errors import HttpError


def validate_config(config: HttpConfig) -> None:
    """Validate the frozen contract plus S06's transport-level constraints."""
    ports.validate_http_config(config)
    if type(config.workers) is not int or config.workers != 1:
        raise ValueError("E_VALIDATION: workers must be 1")
    if type(config.reload) is not bool or config.reload:
        raise ValueError("E_VALIDATION: reload must be false")
    if not _is_loopback(config.host):
        raise ValueError("E_VALIDATION: host must be loopback")
    if type(config.port) is not int or not 1 <= config.port <= 65535:
        raise ValueError("E_VALIDATION: invalid port")
    if not config.allowed_hosts or any(not host or "*" in host for host in config.allowed_hosts):
        raise ValueError("E_VALIDATION: invalid allowed_hosts")
    for cidr in config.trusted_proxy_cidrs:
        try:
            ipaddress.ip_network(cidr, strict=True)
        except (ValueError, TypeError):
            raise ValueError("E_VALIDATION: invalid trusted proxy CIDR") from None
    for origin in config.allowed_origins:
        parsed = urlsplit(origin)
        if (parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.path
                or parsed.query or parsed.fragment or parsed.username is not None):
            raise ValueError("E_VALIDATION: invalid allowed origin")
        if "*" in origin:
            raise ValueError("E_VALIDATION: wildcard origins are forbidden")
    positive_ints = (
        "max_body_bytes", "max_learn_chars", "max_query_chars", "max_batch",
        "rate_per_minute", "rate_burst", "retrieval_concurrency",
        "maintenance_concurrency_per_profile", "graph_max_depth",
        "cursor_ttl_seconds", "idempotency_ttl_seconds",
    )
    for name in positive_ints:
        value = getattr(config, name)
        if type(value) is not int or value <= 0:
            raise ValueError(f"E_VALIDATION: invalid {name}")


def _is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def protocol_class(config: HttpConfig) -> type[H11Protocol]:
    """Return a pinned-h11 protocol class carrying this exact configuration."""
    validate_config(config)

    class ConfiguredHeaderGateProtocol(HeaderGateProtocol):
        _http_config = config

    ConfiguredHeaderGateProtocol.__name__ = "HeaderGateProtocol"
    return ConfiguredHeaderGateProtocol


class HeaderGateProtocol(H11Protocol):
    """Bound initial headers/connections before handing bytes to uvicorn."""

    _http_config: HttpConfig | None = None

    def __init__(self, config: Any, server_state: Any, app_state: dict[str, Any], _loop: Any = None) -> None:
        self._http_config = config if isinstance(config, HttpConfig) else self._http_config
        super().__init__(config, server_state, app_state, _loop)  # type: ignore[arg-type]
        self._header_bytes = bytearray()
        self._header_done = False
        self._response_completed = False
        self._header_timer: asyncio.TimerHandle | None = None

    def connection_made(self, transport: asyncio.Transport) -> None:
        config = self._http_config
        if config is not None and len(self.server_state.connections) >= config.max_connections:
            self.transport = transport
            transport.close()
            return
        super().connection_made(transport)
        if config is not None:
            self._header_timer = self.loop.call_later(config.header_timeout_seconds, self._header_timeout)

    def _header_timeout(self) -> None:
        if not self._header_done and not self.transport.is_closing():
            self.transport.close()

    def data_received(self, data: bytes) -> None:
        if self.transport is None or self.transport.is_closing():
            return
        if self._response_completed:
            self.transport.close()
            return
        config = self._http_config
        if not self._header_done:
            self._header_bytes.extend(data)
            marker = self._header_bytes.find(b"\r\n\r\n")
            if marker < 0:
                if config is not None and len(self._header_bytes) > config.max_header_bytes:
                    self.transport.close()
                return
            header_end = marker + 4
            if config is not None and header_end > config.max_header_bytes:
                self.transport.close()
                return
            raw_headers = bytes(self._header_bytes[:header_end])
            if _has_conflicting_framing(raw_headers):
                self.transport.close()
                return
            data = bytes(self._header_bytes)
            self._header_bytes.clear()
            self._header_done = True
            if self._header_timer is not None:
                self._header_timer.cancel()
                self._header_timer = None
        super().data_received(data)

    def on_response_complete(self) -> None:
        self._response_completed = True
        if self.transport is not None and not self.transport.is_closing():
            self.transport.close()
        super().on_response_complete()

    def connection_lost(self, exc: Exception | None) -> None:
        if self._header_timer is not None:
            self._header_timer.cancel()
            self._header_timer = None
        super().connection_lost(exc)


def _has_conflicting_framing(raw: bytes) -> bool:
    header_block = raw.split(b"\r\n\r\n", 1)[0]
    names = {line.split(b":", 1)[0].strip().lower() for line in header_block.split(b"\r\n")[1:] if b":" in line}
    return b"content-length" in names and b"transfer-encoding" in names


async def read_body(
    scope: dict[str, Any],
    receive: Callable[[], Awaitable[dict[str, Any]]],
    config: HttpConfig,
) -> bytes:
    """Read and count ASGI body bytes under a size and absolute deadline."""
    headers = [(bytes(k).lower(), bytes(v).strip()) for k, v in scope.get("headers", [])]
    lengths = [value for key, value in headers if key == b"content-length"]
    transfer = [value for key, value in headers if key == b"transfer-encoding"]
    if lengths and transfer:
        raise HttpError("E_VALIDATION")
    declared: int | None = None
    if lengths:
        try:
            parsed = [int(value, 10) for value in lengths]
        except (TypeError, ValueError):
            raise HttpError("E_VALIDATION") from None
        if any(value < 0 for value in parsed) or len(set(parsed)) != 1:
            raise HttpError("E_VALIDATION")
        declared = parsed[0]
        if declared > config.max_body_bytes:
            raise HttpError("E_REQUEST_TOO_LARGE")

    deadline = monotonic() + config.request_timeout_seconds
    chunks: list[bytes] = []
    observed = 0
    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise HttpError("E_TIMEOUT")
        try:
            message = await asyncio.wait_for(receive(), timeout=remaining)
        except asyncio.TimeoutError:
            raise HttpError("E_TIMEOUT") from None
        if message.get("type") == "http.disconnect":
            raise HttpError("E_VALIDATION")
        if message.get("type") != "http.request":
            raise HttpError("E_VALIDATION")
        chunk = message.get("body", b"")
        if not isinstance(chunk, bytes):
            raise HttpError("E_VALIDATION")
        observed += len(chunk)
        if observed > config.max_body_bytes:
            raise HttpError("E_REQUEST_TOO_LARGE")
        chunks.append(chunk)
        if not message.get("more_body", False):
            break
    if declared is not None and observed != declared:
        raise HttpError("E_VALIDATION")
    return b"".join(chunks)
