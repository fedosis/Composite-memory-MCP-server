"""Bounded, profile-scoped memory reads and fail-closed projections.

The service deliberately receives its lease and read binder.  It never reaches
through a runtime/application object to discover a provider, and every response
is projected into a new DTO before it crosses the transport boundary.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import MemoryReadPort, ProfileLeases, RequestContext
from memory_server.transports.http.errors import HttpError

ReadClassification = Literal["clean", "sensitive", "unknown"]


@dataclass(frozen=True)
class ReadResources:
    provider: Any
    session_factory: Any


@dataclass(frozen=True)
class ReadRecord:
    memory: Any
    memory_type: str
    receipt: Any | None = None
    history: tuple[Any, ...] = ()


@dataclass(frozen=True)
class ReadKey:
    created_at: str
    memory_type: str
    id: str


@dataclass(frozen=True)
class HistorySlice:
    events: tuple[Any, ...]
    has_more: bool
    incomplete: bool
    upper: Any | None


@dataclass(frozen=True)
class MemorySlice:
    records: tuple[ReadRecord, ...]
    has_more: bool
    upper: ReadKey | None


class CursorCodec:
    """Canonical signed cursors bound to the authenticated read identity."""

    def __init__(self, key: bytes, ttl_seconds: int = 900):
        if len(key) < 32 or not 1 <= ttl_seconds <= 900:
            raise ValueError("invalid cursor configuration")
        self._key, self.ttl = key, ttl_seconds

    @staticmethod
    def _b64(value: bytes) -> str:
        return base64.urlsafe_b64encode(value).decode().rstrip("=")

    @staticmethod
    def _unb64(value: str) -> bytes:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))

    def issue(
        self, ctx: RequestContext, command: Any, *, last: ReadKey, upper: ReadKey, limit: int, now: datetime
    ) -> str:
        payload = {
            "v": 1,
            "profile_id": ctx.profile_id,
            "subject": ctx.principal.subject,
            "operation": command.operation,
            "filter_sha256": _filter_digest(command),
            "last": [last.created_at, last.memory_type, last.id],
            "upper": [upper.created_at, upper.memory_type, upper.id],
            "limit": limit,
            "issued_at": now.astimezone(UTC).isoformat(),
            "expires_at": (now + timedelta(seconds=self.ttl)).astimezone(UTC).isoformat(),
        }
        body = self._b64(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())
        sig = self._b64(hmac.new(self._key, body.encode(), hashlib.sha256).digest())
        return body + "." + sig

    def validate(self, principal: Any, command: Any, token: str, *, now: datetime) -> dict[str, Any]:
        try:
            body, sig = token.split(".")
            raw = self._unb64(body)
            data = json.loads(raw)
            expected = hmac.new(self._key, body.encode(), hashlib.sha256).digest()
        except (ValueError, TypeError, UnicodeError, json.JSONDecodeError):
            raise HttpError("E_CURSOR_INVALID") from None
        if not hmac.compare_digest(expected, self._unb64(sig)) or not isinstance(data, dict):
            raise HttpError("E_CURSOR_INVALID")
        if (
            data.get("v"),
            data.get("profile_id"),
            data.get("subject"),
            data.get("operation"),
            data.get("filter_sha256"),
        ) != (1, command.profile_id, principal.subject, command.operation, _filter_digest(command)):
            raise HttpError("E_CURSOR_INVALID")
        try:
            expiry = datetime.fromisoformat(data["expires_at"])
            issued = datetime.fromisoformat(data["issued_at"])
        except (KeyError, TypeError, ValueError):
            raise HttpError("E_CURSOR_INVALID") from None
        if now >= expiry:
            raise HttpError("E_CURSOR_EXPIRED")
        if expiry < issued or expiry - issued > timedelta(seconds=self.ttl):
            raise HttpError("E_CURSOR_INVALID")
        if type(data.get("limit")) is not int or not 1 <= data["limit"] <= 200:
            raise HttpError("E_CURSOR_INVALID")
        if data["limit"] < command.payload.limit:
            raise HttpError("E_CURSOR_INVALID")
        if not (isinstance(data.get("last"), list) and len(data["last"]) == 3):
            raise HttpError("E_CURSOR_INVALID")
        return data


def _filter_digest(command: Any) -> str:
    payload = command.payload.model_dump(mode="json") if hasattr(command.payload, "model_dump") else command.payload
    payload = {k: v for k, v in payload.items() if k != "cursor"} if isinstance(payload, dict) else payload
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def classify_read_content(record: ReadRecord, gate: Any) -> ReadClassification:
    """Visit all exposed content; unknown, malformed and over-budget are masked."""
    values: list[str] = []
    try:

        def visit(value: Any) -> None:
            if isinstance(value, str):
                values.append(value)
            elif isinstance(value, dict):
                for item in value.values():
                    visit(item)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    visit(item)
            elif value is not None and not isinstance(value, (int, float, bool)):
                raise ValueError("unsupported content")

        visit(getattr(record.memory, "__dict__", record.memory))
        visit(getattr(record.receipt, "__dict__", record.receipt))
        if sum(map(len, values)) > 32768:
            return "unknown"
        for value in values:
            if getattr(gate, "admit", None) is None or not gate.admit(value, force=False, scope="server"):
                return "sensitive"
        tags = getattr(record.memory, "risk_tags", ()) or ()
        if tags:
            return "sensitive"
        return "clean"
    except Exception:
        return "unknown"


def project_for_caller(
    ctx: RequestContext, operation: str, record: ReadRecord, classification: ReadClassification
) -> Any:
    """Construct a fresh masked DTO; only clean authorized reveal can be raw."""
    reveal = (
        operation == "memories.reveal" and "memory.reveal" in ctx.principal.permissions and classification == "clean"
    )
    value = record.memory
    if isinstance(value, d.Memory):
        if reveal:
            return value.model_copy(deep=True)
        return value.model_copy(
            update={
                "content": None,
                "payload": None,
                "source": None,
                "creator": None,
                "source_version": value.source_version if type(value.source_version) in (str, int) else None,
                "risk_tags": [],
                "masked": True,
            },
            deep=True,
        )
    if isinstance(value, d.Receipt):
        if reveal:
            return value.model_copy(deep=True)
        return value.model_copy(
            update={
                "source": None,
                "created_by": None,
                "source_version": None,
                "history": [],
                "history_complete": False,
            },
            deep=True,
        )
    if isinstance(value, d.HistoryEvent):
        return value.model_copy(update={"actor": None, "reason": None, "evidence": None}, deep=True)
    return value


def check_reveal_revision(memory_type: str, expected_revision: int | None, observed_revision: int | None) -> None:
    if expected_revision is None:
        if memory_type in {"fact", "belief"}:
            raise HttpError("E_PRECONDITION_REQUIRED")
        return
    if observed_revision is None:
        raise HttpError("E_CAPABILITY_UNAVAILABLE")
    if expected_revision != observed_revision:
        raise HttpError("E_VERSION_CONFLICT")


class MemoryReadService(MemoryReadPort):
    """Application port implementation; upper is not an insertion watermark.

    upper is the maximum matching creation key at issue time, not an insertion
    watermark; backdated inserts at or below upper may appear; pages are not an
    MVCC snapshot.
    """

    def __init__(
        self, leases: ProfileLeases, bind_read: Any, *, classifier: Any, cursor_codec: CursorCodec | None = None
    ):
        self.leases, self.bind_read, self.classifier, self.cursor_codec = leases, bind_read, classifier, cursor_codec

    async def execute(self, ctx: RequestContext, command: Any) -> Any:
        if command.operation in {"memories.list", "memories.get", "receipts.get", "history.list", "lookup"}:
            return await self._read(ctx, command)
        if command.operation == "memories.reveal":
            if "memory.reveal" not in ctx.principal.permissions:
                raise HttpError("E_PERMISSION_DENIED")
            return await self._read(ctx, command)
        raise HttpError("E_VALIDATION")

    async def _read(self, ctx: RequestContext, command: Any) -> Any:
        if command.operation == "lookup" and command.payload.kind in {"request", "relation"}:
            raise HttpError("E_CAPABILITY_UNAVAILABLE")
        async with self.leases.acquire(ctx) as scope:
            resources = self.bind_read(scope)
            async with resources.session_factory() as session:
                provider = resources.provider
                if command.operation == "memories.list":
                    query = command.payload
                    if any(
                        getattr(query, name)
                        for name in ("source", "tags", "risk_tags", "authority_level", "epistemic_status")
                    ):
                        raise HttpError("E_PERMISSION_DENIED")
                    result = await provider.read_memory_page(session, query, after=None, upper=None)
                    items = [
                        project_for_caller(ctx, command.operation, r, classify_read_content(r, self.classifier))
                        for r in result.records
                    ]
                    return d.Page[d.Memory](
                        items=items, next_cursor=None, has_more=result.has_more, request_id=ctx.request_id
                    )
                record = await provider.read_memory(
                    session,
                    command.path_ids.get("id"),
                    command.payload.memory_type if hasattr(command.payload, "memory_type") else None,
                )
                if command.operation == "memories.reveal":
                    check_reveal_revision(
                        record.memory_type,
                        command.payload.expected_revision,
                        getattr(record.memory, "revision", None),
                    )
                if command.operation == "receipts.get":
                    record = ReadRecord(record.receipt, record.memory_type, record.receipt)
                if command.operation == "history.list":
                    events = tuple(
                        project_for_caller(ctx, command.operation, ReadRecord(e, record.memory_type), "sensitive")
                        for e in record.history
                    )
                    return d.Page[d.HistoryEvent](
                        items=list(events), next_cursor=None, has_more=False, request_id=ctx.request_id
                    )
                projected = project_for_caller(
                    ctx, command.operation, record, classify_read_content(record, self.classifier)
                )
                if command.operation == "lookup":
                    return d.LookupResult(
                        kind=command.payload.kind, id=command.payload.id, target=projected, found=True
                    )
                return projected


def compose_memory_read_port(
    leases: ProfileLeases, bind_read: Any, *, classifier: Any, traces: Any = None
) -> MemoryReadPort:
    return MemoryReadService(leases, bind_read, classifier=classifier)
