"""Configuration-only profile registry implementing the C1 resolve port.

No factories, runtime implementations, leases or writer locks are imported. The
composition supplies frozen ProfileBindings; existing pure path admission rules
are reused without opening any memory database. Runtime re-admission is S04.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import Principal, ProfileBinding, ProfileRecord
from memory_server.paths import (
    StorageLayoutError,
    StorageResolutionInputs,
    inspect_component_chain_nofollow,
    resolve_storage_layout,
)

from .store import ControlError, ControlStore

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

    def __init__(self, store: ControlStore, configuration: RegistryConfiguration) -> None:
        self._store = store
        self._configuration = configuration
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
