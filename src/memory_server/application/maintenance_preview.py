"""Detached maintenance plans; storage and authentication remain caller-owned."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import re
import stat
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Literal

from memory_server.control.jobs import JobError, stamp
from memory_server.control.store import ControlError
from memory_server.gateway_contracts.dto import MaintenancePlan
from memory_server.gateway_contracts.ports import RequestContext

DOMAIN = "cmms.outbox.maintenance-plan.v1"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class MaintenanceRow:
    id: str
    record_id: str
    record_type: str
    operation: str
    status: str
    retry_count: int
    created_at: datetime
    processed_at: datetime | None


@dataclass(frozen=True)
class MaintenanceReadSnapshot:
    profile_id: str
    registry_generation: int
    rows: tuple[MaintenanceRow, ...]


@dataclass(frozen=True)
class MaintenancePlanMaterial:
    v: int
    domain: str
    profile_id: str
    registry_generation: int
    subject: str
    selection: Literal["failed", "pending"]
    selected_ids: tuple[str, ...]
    expected_count: int
    rows: tuple[MaintenanceRow, ...]
    generation: str
    issued_at: datetime
    expiry: datetime


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def require(condition, code="E_VALIDATION"):
    if not condition:
        raise JobError(code)


def integer(value, low, high):
    require(type(value) is int and low <= value <= high)
    return value


def epoch_us(value):
    stamp(value)
    delta = value - EPOCH
    return integer((delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds, 0, 253402300799999999)


def row_generation(rows, selection):
    require(type(rows) is tuple and len(rows) <= 101, "E_INTERNAL")
    values = []
    for row in rows:
        require(type(row) is MaintenanceRow, "E_INTERNAL")
        require(
            all(
                type(getattr(row, field)) is str for field in ("id", "record_id", "record_type", "operation", "status")
            ),
            "E_INTERNAL",
        )
        require(row.status == selection and type(row.retry_count) is int and row.retry_count >= 0, "E_INTERNAL")
        require(re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", row.id) is not None, "E_INTERNAL")
        try:
            values.append(
                [
                    row.id,
                    row.record_id,
                    row.record_type,
                    row.operation,
                    row.status,
                    row.retry_count,
                    stamp(row.created_at),
                    None if row.processed_at is None else stamp(row.processed_at),
                ]
            )
        except (JobError, TypeError, ValueError):
            raise JobError("E_INTERNAL") from None
    require(len({row.id for row in rows}) == len(rows), "E_INTERNAL")
    require(list(rows) == sorted(rows, key=lambda row: (row.created_at, row.id)), "E_INTERNAL")
    return hashlib.sha256(canonical(values)).hexdigest()


def projection(material):
    value = asdict(material)
    del value["rows"]
    value["issued_at"], value["expiry"] = stamp(material.issued_at), stamp(material.expiry)
    return value


def encoded(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def decoded(segment):
    require(re.fullmatch(r"[A-Za-z0-9_-]+", segment) is not None)
    value = base64.b64decode(segment + "=" * (-len(segment) % 4), altchars=b"-_", validate=True)
    require(encoded(value) == segment)
    return value


def pairs(items):
    require(len(dict(items)) == len(items))
    return dict(items)


class MaintenancePlanCodec:
    def __init__(self, key: bytes, *, ttl_seconds: int = 900) -> None:
        require(key is not None, "E_CAPABILITY_UNAVAILABLE")
        require(type(key) is bytes and len(key) == 32)
        self._key = key
        self.ttl_seconds = integer(ttl_seconds, 1, 900)

    def _material(self, material):
        require(type(material) is MaintenancePlanMaterial)
        integer(material.v, 1, 1)
        integer(material.registry_generation, 1, 9223372036854775807)
        integer(material.expected_count, 0, 100)
        require(material.domain == DOMAIN, "E_MAINTENANCE_CONFLICT")
        require(type(material.profile_id) is str and type(material.subject) is str)
        require(type(material.selected_ids) is tuple)
        try:
            MaintenancePlan(
                selection_digest="",
                selected_ids=list(material.selected_ids),
                selected_count=material.expected_count,
                expires_at=material.expiry,
                generation=material.generation,
                selection=material.selection,
            )
        except ValueError:
            raise JobError("E_VALIDATION") from None
        require(material.expected_count == len(material.rows), "E_MAINTENANCE_CONFLICT")
        require(tuple(row.id for row in material.rows) == material.selected_ids, "E_MAINTENANCE_CONFLICT")
        require(row_generation(material.rows, material.selection) == material.generation, "E_MAINTENANCE_CONFLICT")
        issued, expiry = epoch_us(material.issued_at), epoch_us(material.expiry)
        require(expiry - issued == self.ttl_seconds * 1000000, "E_MAINTENANCE_CONFLICT")
        return {"v": 1, "i": issued, "e": expiry, "r": material.registry_generation, "g": material.generation}

    def _mac(self, material):
        return hmac.new(
            self._key, DOMAIN.encode("ascii") + b"\x00" + canonical(projection(material)), hashlib.sha256
        ).digest()

    def issue(self, material: MaintenancePlanMaterial) -> str:
        header = self._material(material)
        return encoded(canonical(header)) + "." + encoded(self._mac(material))

    def verify(
        self,
        token: str,
        *,
        ctx: RequestContext,
        selection: Literal["failed", "pending"],
        selected_ids: tuple[str, ...],
        expected_count: int,
        registry_generation: int,
        rows: tuple[MaintenanceRow, ...],
        now: datetime,
    ) -> MaintenancePlanMaterial:
        try:
            require(type(token) is str and token.isascii() and len(token) <= 512)
            parts = token.split(".")
            require(len(parts) == 2)
            raw, supplied = (decoded(part) for part in parts)
            require(len(raw) <= 384 and len(supplied) == 32)
            header = json.loads(raw, object_pairs_hook=pairs)
            require(type(header) is dict and set(header) == {"v", "i", "e", "r", "g"})
            require(canonical(header) == raw)
            integer(header["v"], 1, 1)
            for field in ("i", "e"):
                integer(header[field], 0, 253402300799999999)
            integer(header["r"], 1, 9223372036854775807)
            require(type(header["g"]) is str and re.fullmatch(r"[0-9a-f]{64}", header["g"]) is not None)
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise JobError("E_VALIDATION") from None
        require(type(selected_ids) is tuple)
        integer(expected_count, 0, 100)
        integer(registry_generation, 1, 9223372036854775807)
        try:
            MaintenancePlan(
                selection_digest="",
                selected_ids=list(selected_ids),
                selected_count=expected_count,
                expires_at=now,
                generation=header["g"],
                selection=selection,
            )
        except ValueError:
            raise JobError("E_VALIDATION") from None
        require(header["e"] - header["i"] == self.ttl_seconds * 1000000, "E_MAINTENANCE_CONFLICT")
        require(header["i"] <= epoch_us(now) < header["e"], "E_MAINTENANCE_CONFLICT")
        require(header["r"] == registry_generation, "E_MAINTENANCE_CONFLICT")
        require(type(rows) is tuple and len(rows) == expected_count, "E_MAINTENANCE_CONFLICT")
        require(all(type(row) is MaintenanceRow and row.status == selection for row in rows), "E_MAINTENANCE_CONFLICT")
        material = MaintenancePlanMaterial(
            1,
            DOMAIN,
            ctx.profile_id,
            registry_generation,
            ctx.principal.subject,
            selection,
            selected_ids,
            expected_count,
            rows,
            header["g"],
            EPOCH + timedelta(microseconds=header["i"]),
            EPOCH + timedelta(microseconds=header["e"]),
        )
        self._material(material)
        require(hmac.compare_digest(self._mac(material), supplied), "E_MAINTENANCE_CONFLICT")
        return material


class MaintenancePreviewProvider:
    def __init__(
        self,
        *,
        authorize: Callable[[RequestContext], Awaitable[None]],
        read_rows: Callable[..., Awaitable[MaintenanceReadSnapshot]],
        codec: MaintenancePlanCodec,
        clock: Callable[[], datetime],
    ) -> None:
        require(all(value is not None for value in (authorize, read_rows, codec, clock)), "E_CAPABILITY_UNAVAILABLE")
        self.authorize, self.read_rows, self.codec, self.clock = authorize, read_rows, codec, clock

    async def preview(
        self, ctx: RequestContext, *, selection: Literal["failed", "pending"], limit: int = 100
    ) -> MaintenancePlan:
        await self.authorize(ctx)
        require(type(selection) is str and selection in ("failed", "pending"))
        integer(limit, 1, 100)
        require(type(ctx.deadline) in (int, float) and math.isfinite(ctx.deadline))
        require(monotonic() < ctx.deadline, "E_TIMEOUT")
        try:
            snapshot = await self.read_rows(ctx, selection=selection, limit=limit)
        except TimeoutError:
            raise JobError("E_TIMEOUT") from None
        except ControlError:
            raise JobError("E_STORAGE") from None
        require(monotonic() < ctx.deadline, "E_TIMEOUT")
        require(type(snapshot) is MaintenanceReadSnapshot and snapshot.profile_id == ctx.profile_id, "E_INTERNAL")
        require(
            type(snapshot.registry_generation) is int and 1 <= snapshot.registry_generation <= 9223372036854775807,
            "E_INTERNAL",
        )
        row_generation(snapshot.rows, selection)
        require(len(snapshot.rows) <= limit + 1, "E_INTERNAL")
        rows, now = snapshot.rows[:limit], self.clock()
        generation = row_generation(rows, selection)
        material = MaintenancePlanMaterial(
            1,
            DOMAIN,
            snapshot.profile_id,
            snapshot.registry_generation,
            ctx.principal.subject,
            selection,
            tuple(row.id for row in rows),
            len(rows),
            rows,
            generation,
            now,
            now + timedelta(seconds=self.codec.ttl_seconds),
        )
        return MaintenancePlan(
            selection_digest=self.codec.issue(material),
            selected_ids=list(material.selected_ids),
            selected_count=len(rows),
            expires_at=material.expiry,
            generation=generation,
            selection=selection,
        )


def load_maintenance_plan_key(root_fd) -> bytes:
    try:
        root = os.fstat(root_fd)
        require(
            stat.S_ISDIR(root.st_mode) and root.st_uid == os.geteuid() and stat.S_IMODE(root.st_mode) == 0o700,
            "E_STORAGE",
        )
        fd = os.open(
            "maintenance-plan-v1.key", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=root_fd
        )
        try:
            info = os.fstat(fd)
            require(
                stat.S_ISREG(info.st_mode)
                and info.st_uid == os.geteuid()
                and stat.S_IMODE(info.st_mode) == 0o600
                and info.st_nlink == 1,
                "E_STORAGE",
            )
            key = os.read(fd, 33)
            require(len(key) == 32, "E_STORAGE")
            return key
        finally:
            os.close(fd)
    except (OSError, TypeError, ValueError):
        raise JobError("E_STORAGE") from None
