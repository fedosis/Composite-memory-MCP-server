"""Configuration-only profile registry implementing the C1 resolve port.

No factories, runtime implementations, leases or writer locks are imported. The
composition supplies frozen ProfileBindings; existing pure path admission rules
are reused without opening any memory database. Runtime re-admission is S04.
"""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import (
    AuditOutcome,
    AuditTicket,
    ControlReply,
    Principal,
    ProfileBinding,
    ProfileLeases,
    ProfileRecord,
    RequestContext,
)
from memory_server.paths import (
    StorageLayoutError,
    StorageResolutionInputs,
    inspect_component_chain_nofollow,
    resolve_storage_layout,
)

from .store import ControlError, ControlStore

if TYPE_CHECKING:
    from .access_audit import AccessAuditStore
    from .tokens import TokenService

# Process-local exclusion only (workers=1), not a cross-process lock.
_ACTIVE_CONTROLS: set[tuple[str, int]] = set()

_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", re.ASCII)


def _validate_id(profile_id: str) -> None:
    if not isinstance(profile_id, str) or _ID.fullmatch(profile_id) is None:
        raise ControlError("E_VALIDATION")


def _overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


@dataclass(frozen=True)
class RegistryConfiguration:
    """Frozen local composition input, never derived from HTTP request data."""

    bindings: tuple[ProfileBinding, ...]

    def __post_init__(self) -> None:
        if type(self.bindings) is not tuple or len(self.bindings) > 100:
            raise ControlError("E_VALIDATION")


class ProfileRegistry:
    """Only existing configured IDs can be resolved or mutated.

    Principal authentication/expiry/revocation belong to S03. Resolve checks
    explicit profile scope itself; permission-specific requests are guarded by
    their owner. List and local mutations additionally require profile.read and
    profile.manage respectively. No wildcard grants or root-setting methods.
    """

    def __init__(
        self, store: ControlStore, configuration: RegistryConfiguration, *,
        audit_store: AccessAuditStore | None = None, tokens: TokenService | None = None
    ) -> None:
        self._store = store
        self._audit_store = audit_store
        self._tokens = tokens
        self._configuration = configuration
        self._reconcile_after = 0
        bindings: dict[str, ProfileBinding] = {}
        records = []
        identities = []
        try:
            for binding in configuration.bindings:
                profile = binding.profile
                _validate_id(profile.id)
                if profile.id in bindings:
                    raise ControlError("E_VALIDATION")
                layout = binding.layout
                # Reuse runtime's pure no-follow admission (including sidecars,
                # lock targets and hardlinks); never normalize an unsafe link.
                admitted = resolve_storage_layout(
                    StorageResolutionInputs(
                        mode=layout.mode,
                        profile_home=layout.profile_home,
                        data_root=layout.data_root,
                        sqlite_url=layout.sqlite.configured_url,
                        vector_backend=layout.vector.backend,
                        lancedb_path=layout.vector.local_path or "lancedb",
                        graph_snapshot_path=layout.graph_snapshot_path,
                        qdrant_location=layout.vector.qdrant_location or ":memory:",
                        vector_collection=layout.vector.collection,
                        origins=layout.origins,
                        legacy_split_compat=False,
                    )
                )
                if admitted != layout or binding.config.layout != layout or binding.descriptor.profile_id != profile.id:
                    raise ControlError("E_VALIDATION")
                root = admitted.data_root
                if _overlap(root, store.root):
                    raise ControlError("E_VALIDATION")
                chain = inspect_component_chain_nofollow(root, anchor=Path("/"))
                final = chain[-1]
                physical = (final.device, final.inode) if final.kind == "directory" else None
                ancestors = {(node.device, node.inode) for node in chain if node.kind == "directory"}
                for previous_root, previous_physical, previous_ancestors in identities:
                    if (
                        _overlap(root, previous_root)
                        or physical is not None
                        and physical in previous_ancestors
                        or previous_physical is not None
                        and previous_physical in ancestors
                    ):
                        raise ControlError("E_VALIDATION")
                identities.append((root, physical, ancestors))
                # Copy even the mapping so caller-owned configuration cannot
                # mutate an admitted layout's origins after construction.
                frozen_layout = replace(layout, origins=MappingProxyType(dict(layout.origins)))
                bindings[profile.id] = replace(binding, layout=frozen_layout)
                records.append(
                    ProfileRecord(
                        profile.id,
                        root,
                        profile.enabled,
                        profile.display_name,
                        profile.description,
                        MappingProxyType({}),
                        1,
                    )
                )
        except (StorageLayoutError, OSError, ValueError):
            raise ControlError("E_VALIDATION") from None
        store._configure(tuple(records))
        self._bindings = MappingProxyType(bindings)

    def _authorise(self, principal: Principal, profile_id: str, permission: d.Permission | None = None) -> None:
        _validate_id(profile_id)
        # Check scope BEFORE any existence or database lookup, even unknown IDs.
        if profile_id not in principal.profiles or (permission is not None and permission not in principal.permissions):
            raise ControlError("E_PERMISSION_DENIED")
        if profile_id not in self._bindings:
            raise ControlError("E_PROFILE_NOT_FOUND")

    @staticmethod
    def _generation(expected_generation: int | None) -> int:
        if expected_generation is None:
            raise ControlError("E_PRECONDITION_REQUIRED")
        if type(expected_generation) is not int or expected_generation < 1:
            raise ControlError("E_VALIDATION")
        return expected_generation

    async def resolve(self, principal: Principal, profile_id: str) -> ProfileBinding:
        self._authorise(principal, profile_id)
        record = self._store._get(profile_id)
        if not record.enabled:
            raise ControlError("E_PROFILE_DISABLED")
        binding = self._bindings[profile_id]
        profile = d.Profile(
            id=record.id,
            display_name=record.display_name,
            description=record.description,
            enabled=record.enabled,
            generation=record.generation,
            runtime_state="not_started",
            capabilities=binding.profile.capabilities,
            storage_root=None,
        )
        return replace(binding, profile=profile)

    async def list(self, principal: Principal) -> tuple[ProfileRecord, ...]:
        if "profile.read" not in principal.permissions:
            raise ControlError("E_PERMISSION_DENIED")
        return tuple(
            self._store._get(profile_id) for profile_id in sorted(self._bindings) if profile_id in principal.profiles
        )

    def _profile(self, record: ProfileRecord) -> d.Profile:
        return d.Profile(
            id=record.id,
            display_name=record.display_name,
            description=record.description,
            enabled=record.enabled,
            generation=record.generation,
            runtime_state="not_started",
            capabilities=self._bindings[record.id].profile.capabilities,
            storage_root=None,
        )

    async def page(
        self, principal: Principal, *, limit: int, after: str | None, upper: str | None
    ) -> tuple[tuple[d.Profile, ...], str | None]:
        if "profile.read" not in principal.permissions:
            raise ControlError("E_PERMISSION_DENIED")
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ControlError("E_VALIDATION")
        for position in (after, upper):
            if position is not None:
                _validate_id(position)
        if after is not None and (upper is None or after > upper):
            raise ControlError("E_VALIDATION")
        scope = tuple(sorted(set(self._bindings) & principal.profiles))
        records, bound = self._store._page(scope, limit=limit, after=after, upper=upper)
        return tuple(self._profile(record) for record in records), bound

    async def record(self, principal: Principal, profile_id: str, *, permission: d.Permission) -> ProfileRecord:
        if permission not in {"profile.read", "profile.manage"}:
            raise ControlError("E_VALIDATION")
        self._authorise(principal, profile_id, permission)
        return replace(self._store._get(profile_id), canonical_root=None)

    def available(self) -> bool:
        """S06 Q5: probe the loaded control store, never resolve/open profiles."""
        connection = self._store._connection
        if connection is None:
            return False
        try:
            connection.execute("SELECT id,generation FROM profiles LIMIT 1").fetchone()
        except sqlite3.Error:
            return False
        try:
            return not self._store.control_requests(limit=1)
        except ControlError:
            return False

    @staticmethod
    def _safe_observation(observation) -> bool:
        return observation is None or (
            not observation.owned
            and not observation.admissions
            and not observation.stopping
            and not observation.retained
            and observation.state == "not_started"
        )

    async def _complete_control(self, row, leases: ProfileLeases, *, deadline: float) -> bool:
        observation = leases.snapshot().get(row["profile_id"])
        if row["desired_state"] == "enabled":
            return self._safe_observation(observation) or (
                observation.state == "ready"
                and observation.owned
                and not observation.stopping
                and not observation.retained
            )
        async with asyncio.timeout_at(deadline):
            # Direct await enters stop_profile before the first suspension: do
            # not wrap it in a task/wait_for before admission is closed.
            report = await leases.stop_profile(row["profile_id"])
        after = leases.snapshot().get(row["profile_id"])
        if report is None:
            return self._safe_observation(observation) and self._safe_observation(after)
        return (
            report.code is None
            and not report.failed
            and not report.unsettled
            and not report.errors
            and not report.retained_resources
            and not report.lock_retained
            and self._safe_observation(after)
        )

    def _finalize_control(self, row, leases: ProfileLeases):
        body = d.Profile.model_validate_json(row["response_snapshot"])
        observation = leases.snapshot().get(row["profile_id"])
        body = body.model_copy(
            update={"runtime_state": observation.state if observation is not None else "not_started"}
        )
        outcome = {"status": 200, "error_code": None, "receipt_ids": [], "latency_ms": 0.0}
        self._store.finish_control(row, response=body.model_dump(mode="json"), outcome=outcome)
        return body, AuditOutcome(200, None, (), 0.0)

    async def _publish_control(self, row, outcome, publish, *, deadline: float) -> None:
        try:
            async with asyncio.timeout_at(deadline):
                published = await publish(AuditTicket(row["ticket_id"], row["request_id"]), outcome)
            if published:
                self._store.mark_published(row)
        except Exception:
            # The already-final result is not replaced by a publication error.
            # Cancellation still propagates, leaving the durable marker.
            pass

    def _require_intent(self, ctx: RequestContext, operation: str, ticket: AuditTicket) -> None:
        """Fail E_AUDIT_UNAVAILABLE unless our bound S05a store owns this intent.

        This concrete composition check adds no AccessSink method. Replay uses
        its original durable ticket and does not admit a new audited execution.
        """
        audit = self._audit_store
        if audit is None or audit._db is None:
            raise ControlError("E_AUDIT_UNAVAILABLE")
        try:
            row = audit._db.execute(
                "SELECT 1 FROM access_audit WHERE id=? AND request_id=? AND state='intent' "
                "AND token_id=? AND profile_id=? AND operation=? AND client_id IS ? LIMIT 1",
                (ticket.id, ctx.request_id, ctx.principal.token_id, ctx.profile_id, operation, ctx.principal.client_id),
            ).fetchone()
        except sqlite3.Error:
            raise ControlError("E_AUDIT_UNAVAILABLE") from None
        if row is None:
            raise ControlError("E_AUDIT_UNAVAILABLE")

    async def control(
        self,
        ctx: RequestContext,
        command: d.ProfilesEnableCommand | d.ProfilesDisableCommand,
        *,
        ticket: AuditTicket,
        leases: ProfileLeases,
        publish: Callable[[AuditTicket, AuditOutcome], Awaitable[bool]],
        now: datetime,
        ttl_seconds: int,
    ) -> ControlReply:
        from uuid import UUID

        from .policy import utc_now

        utc_now(now)
        if now >= ctx.principal.expires_at:
            raise ControlError("E_AUTH_REQUIRED")
        if not isinstance(command, (d.ProfilesEnableCommand, d.ProfilesDisableCommand)):
            raise ControlError("E_VALIDATION")
        try:
            command = type(command).model_validate(command.model_dump())
        except ValueError:
            raise ControlError("E_VALIDATION") from None
        profile_id = command.profile_id
        self._authorise(ctx.principal, profile_id, "profile.manage")
        if ctx.profile_id != profile_id or ticket.request_id != ctx.request_id:
            raise ControlError("E_VALIDATION")
        if ctx.idempotency_key is None:
            raise ControlError("E_IDEMPOTENCY_REQUIRED")
        if not isinstance(ctx.idempotency_key, UUID) or type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 604800:
            raise ControlError("E_VALIDATION")
        if ctx.deadline <= asyncio.get_running_loop().time():
            raise ControlError("E_TIMEOUT")
        # Revalidate cached authority before any port access or replay hydration.
        if self._store._connection is None:
            raise ControlError("E_STORAGE")
        prior = self._store._request(ctx.principal.subject, profile_id, command.operation, str(ctx.idempotency_key))
        if prior is not None:
            from .policy import TokenError

            if self._tokens is None:
                raise ControlError("E_AUTH_REQUIRED")
            try:
                self._tokens.verify_principal(ctx.principal, now=now)
            except TokenError as exc:
                raise ControlError(exc.code) from None
        enabled = isinstance(command, d.ProfilesEnableCommand)
        observation = leases.snapshot().get(profile_id)
        eligible = (
            not enabled
            or self._safe_observation(observation)
            or (
                observation.state == "ready"
                and observation.owned
                and not observation.stopping
                and not observation.retained
            )
        )
        digest = self._store.control_digest(
            {
                "method": "POST",
                "route": f"/api/v1/profiles/{profile_id}/{'enable' if enabled else 'disable'}",
                "path_ids": {"profile": profile_id},
                "payload": command.payload.model_dump(mode="json"),
            }
        )
        row = self._store.reserve_control(
            subject=ctx.principal.subject,
            profile_id=profile_id,
            operation=command.operation,
            idem_key=str(ctx.idempotency_key),
            digest=digest,
            request_id=ctx.request_id,
            desired_state="enabled" if enabled else "disabled",
            expected_generation=command.payload.expected_generation,
            expires_at=now + timedelta(seconds=ttl_seconds),
            ticket_id=ticket.id,
            now=now,
            capabilities=tuple(self._bindings[profile_id].profile.capabilities),
            eligible=eligible,
            require_intent=lambda: self._require_intent(ctx, command.operation, ticket),
        )
        marker = (str(self._store.root), row["generation"])
        if row["phase"] == "final":
            # Original ticket and original snapshot; never hydrate current state.
            body = d.Profile.model_validate_json(row["response_snapshot"])
            if row["pending_publication"]:
                data = json.loads(row["outcome_snapshot"])
                outcome = AuditOutcome(
                    data["status"], data["error_code"], tuple(data["receipt_ids"]), data["latency_ms"]
                )
                await self._publish_control(row, outcome, publish, deadline=ctx.deadline)
            return ControlReply(200, body, True, row["request_id"])
        _ACTIVE_CONTROLS.add(marker)
        try:
            try:
                safe = await self._complete_control(row, leases, deadline=ctx.deadline)
            except Exception:
                safe = False
            if not safe:
                raise ControlError("E_PROFILE_BUSY")
            body, outcome = self._finalize_control(row, leases)
            await self._publish_control(row, outcome, publish, deadline=ctx.deadline)
            return ControlReply(200, body, False, row["request_id"])
        finally:
            _ACTIVE_CONTROLS.discard(marker)

    async def reconcile_control(
        self, *, leases: ProfileLeases, publish: Callable[[AuditTicket, AuditOutcome], Awaitable[bool]], limit: int
    ) -> bool:
        """Bounded round-robin recovery; active exclusion is process-local, workers=1."""
        rows = self._store.control_requests(limit=limit, after_generation=self._reconcile_after)
        for row in rows:
            self._reconcile_after = row["generation"]
            marker = (str(self._store.root), row["generation"])
            if marker in _ACTIVE_CONTROLS:
                continue
            _ACTIVE_CONTROLS.add(marker)
            try:
                deadline = asyncio.get_running_loop().time() + 5.0
                if row["phase"] == "committed":
                    if not await self._complete_control(row, leases, deadline=deadline):
                        continue
                    _, outcome = self._finalize_control(row, leases)
                else:
                    data = json.loads(row["outcome_snapshot"])
                    outcome = AuditOutcome(
                        data["status"], data["error_code"], tuple(data["receipt_ids"]), data["latency_ms"]
                    )
                await self._publish_control(row, outcome, publish, deadline=deadline)
            except Exception:
                continue
            finally:
                _ACTIVE_CONTROLS.discard(marker)
        return not self._store.control_requests(limit=1)

    async def set_enabled(
        self, principal: Principal, profile_id: str, *, enabled: bool, expected_generation: int | None
    ) -> ProfileRecord:
        self._authorise(principal, profile_id, "profile.manage")
        generation = self._generation(expected_generation)
        if type(enabled) is not bool:
            raise ControlError("E_VALIDATION")
        return self._store._mutate(profile_id, expected_generation=generation, operation="enabled", enabled=enabled)

    async def set_metadata(
        self,
        principal: Principal,
        profile_id: str,
        *,
        display_name: str,
        description: str | None,
        expected_generation: int | None,
    ) -> ProfileRecord:
        self._authorise(principal, profile_id, "profile.manage")
        generation = self._generation(expected_generation)
        if not isinstance(display_name, str) or (description is not None and not isinstance(description, str)):
            raise ControlError("E_VALIDATION")
        return self._store._mutate(
            profile_id,
            expected_generation=generation,
            operation="metadata",
            display_name=display_name,
            description=description,
        )
