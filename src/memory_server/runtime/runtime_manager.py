"""Single-loop descriptor admission and READY-only runtime startup."""

from __future__ import annotations

import asyncio
import math
import os
from dataclasses import dataclass, fields, is_dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping

from memory_server.paths import StorageLayoutError, inspect_component_chain_nofollow
from memory_server.runtime.config import RuntimeConfig
from memory_server.runtime.dependencies import RuntimeDependencies
from memory_server.runtime.errors import CMMSRuntimeError
from memory_server.runtime.runtime import CMMSRuntime, ensure_layout_eligible
from memory_server.runtime.runtime_factory import RuntimeFactory

E_RUNTIME_MANAGER_LOOP_REQUIRED = "E_RUNTIME_MANAGER_LOOP_REQUIRED"
E_RUNTIME_MANAGER_WRONG_LOOP = "E_RUNTIME_MANAGER_WRONG_LOOP"
E_RUNTIME_DESCRIPTOR_CONFLICT = "E_RUNTIME_DESCRIPTOR_CONFLICT"
E_RUNTIME_DESCRIPTOR_VALUE_UNSUPPORTED = "E_RUNTIME_DESCRIPTOR_VALUE_UNSUPPORTED"
E_RUNTIME_RESOURCE_ALIAS = "E_RUNTIME_RESOURCE_ALIAS"
E_RUNTIME_RESOURCE_IDENTITY_CHANGED = "E_RUNTIME_RESOURCE_IDENTITY_CHANGED"
E_RUNTIME_FACTORY_BUILD_FAILED = "E_RUNTIME_FACTORY_BUILD_FAILED"
E_RUNTIME_START_FAILED = "E_RUNTIME_START_FAILED"


class RuntimeManagerError(CMMSRuntimeError):
    """Stable, redacted manager admission/startup failure."""

    def __init__(self, code: str) -> None:
        super().__init__(code)


def _unsupported() -> RuntimeManagerError:
    return RuntimeManagerError(E_RUNTIME_DESCRIPTOR_VALUE_UNSUPPORTED)


def _canonical(value: Any) -> tuple:
    if value is None:
        return ("none",)
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, int):
        return ("int", value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _unsupported()
        return ("float", value)
    if isinstance(value, str):
        return ("str", value)
    if isinstance(value, Path):
        return ("path", os.path.abspath(os.path.normpath(str(value))))
    if isinstance(value, Mapping):
        items = []
        for key, item in value.items():
            normalized_key = _canonical(key)
            if normalized_key[0] not in {"none", "bool", "int", "float", "str", "path"}:
                raise _unsupported()
            items.append((normalized_key, _canonical(item)))
        return ("mapping", tuple(sorted(items)))
    if isinstance(value, (tuple, list)):
        return ("sequence", tuple(_canonical(item) for item in value))
    if isinstance(value, (set, frozenset)):
        return ("set", tuple(sorted(_canonical(item) for item in value)))
    if is_dataclass(value):
        params = getattr(type(value), "__dataclass_params__", None)
        if params is None or not params.frozen:
            raise _unsupported()
        return (
            "dataclass",
            type(value).__module__,
            type(value).__qualname__,
            tuple((field.name, _canonical(getattr(value, field.name))) for field in fields(value)),
        )
    if isinstance(value, MappingProxyType):
        raise _unsupported()
    raise _unsupported()


def _lexical(path: Path) -> Path:
    return Path(os.path.abspath(os.path.normpath(str(path))))


def _resource_paths(layout: Any) -> tuple[Path, ...]:
    paths: list[Path] = []
    root = getattr(layout, "data_root", None)
    if root is not None:
        paths.append(_lexical(root))
    sqlite = getattr(layout, "sqlite", None)
    sqlite_path = getattr(sqlite, "local_path", None)
    if sqlite_path is not None:
        paths.append(_lexical(sqlite_path))
        paths.extend(_lexical(Path(str(sqlite_path) + suffix)) for suffix in ("-wal", "-shm", "-journal"))
    vector = getattr(layout, "vector", None)
    vector_path = getattr(vector, "local_path", None)
    if vector_path is not None:
        paths.append(_lexical(vector_path))
    for name in ("graph_snapshot_path", "graph_lock_path", "root_lock_path"):
        path = getattr(layout, name, None)
        if path is not None:
            paths.append(_lexical(path))
    return tuple(dict.fromkeys(paths))


def _evidence(paths: tuple[Path, ...]) -> dict[Path, tuple]:
    result: dict[Path, tuple] = {}
    for path in paths:
        try:
            identity = inspect_component_chain_nofollow(path, anchor=Path("/"))[-1]
        except StorageLayoutError as exc:
            raise RuntimeManagerError(E_RUNTIME_RESOURCE_IDENTITY_CHANGED) from exc
        result[path] = (identity.kind, identity.device, identity.inode, identity.mode, identity.nlink)
    return result


def _reject_unsafe(evidence: dict[Path, tuple]) -> None:
    for identity in evidence.values():
        if identity[0] in {"symlink", "special"} or (identity[0] == "regular_file" and identity[4] != 1):
            raise RuntimeManagerError(E_RUNTIME_RESOURCE_ALIAS)


def _overlap(left: Path, right: Path) -> bool:
    try:
        return os.path.commonpath((str(left), str(right))) in {str(left), str(right)}
    except ValueError:
        return False


@dataclass(frozen=True, eq=False)
class RuntimeDescriptor:
    profile_id: str
    layout_identity: tuple
    config_identity: tuple
    __hash__ = None

    @classmethod
    def from_layout(cls, profile_id: str, *, layout: Any, config: RuntimeConfig) -> "RuntimeDescriptor":
        if not isinstance(profile_id, str) or not profile_id:
            raise RuntimeManagerError(E_RUNTIME_DESCRIPTOR_CONFLICT)
        layout_identity = _canonical(layout)
        config_identity = _canonical(config)
        if getattr(config, "layout", layout) is not None and _canonical(getattr(config, "layout")) != layout_identity:
            raise RuntimeManagerError(E_RUNTIME_DESCRIPTOR_CONFLICT)
        return cls(profile_id, layout_identity, config_identity)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RuntimeDescriptor) and (
            self.profile_id,
            self.layout_identity,
            self.config_identity,
        ) == (other.profile_id, other.layout_identity, other.config_identity)


@dataclass
class _Entry:
    descriptor: RuntimeDescriptor
    layout: Any
    factory: RuntimeFactory | None = None
    task: asyncio.Task | None = None
    state: str = "STARTING"
    failure_code: str | None = None
    evidence: dict[Path, tuple] | None = None


class RuntimeManager:
    """Own one reservation and one startup task per equivalent descriptor."""

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        factory_builder: Callable[..., RuntimeFactory] = RuntimeFactory.create,
    ) -> None:
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError as exc:
                raise RuntimeManagerError(E_RUNTIME_MANAGER_LOOP_REQUIRED) from exc
        self._loop = loop
        self._factory_builder = factory_builder
        self._entries: list[_Entry] = []

    def _check_loop(self) -> None:
        try:
            current = asyncio.get_running_loop()
        except RuntimeError as exc:
            raise RuntimeManagerError(E_RUNTIME_MANAGER_WRONG_LOOP) from exc
        if current is not self._loop:
            raise RuntimeManagerError(E_RUNTIME_MANAGER_WRONG_LOOP)

    def _validate(self, descriptor: RuntimeDescriptor, layout: Any, config: RuntimeConfig) -> RuntimeDescriptor:
        candidate = RuntimeDescriptor.from_layout(descriptor.profile_id, layout=layout, config=config)
        if descriptor != candidate:
            raise RuntimeManagerError(E_RUNTIME_DESCRIPTOR_CONFLICT)
        ensure_layout_eligible(layout)
        return candidate

    def _reserve(self, descriptor: RuntimeDescriptor, layout: Any) -> _Entry:
        candidate_paths = _resource_paths(layout)
        candidate_evidence = _evidence(candidate_paths)
        _reject_unsafe(candidate_evidence)
        for entry in self._entries:
            if entry.descriptor.profile_id == descriptor.profile_id and entry.descriptor != descriptor:
                raise RuntimeManagerError(E_RUNTIME_DESCRIPTOR_CONFLICT)
            if entry.descriptor == descriptor:
                if entry.state != "FAILED":
                    self._check_identity(entry)
                return entry
            assert entry.evidence is not None
            for left in candidate_paths:
                for right in entry.evidence:
                    if _overlap(left, right):
                        raise RuntimeManagerError(E_RUNTIME_RESOURCE_ALIAS)
                    left_identity = candidate_evidence[left]
                    right_identity = entry.evidence[right]
                    if left_identity[0] == right_identity[0] == "absent":
                        continue
                    if left_identity[0] != "absent" and left_identity == right_identity:
                        raise RuntimeManagerError(E_RUNTIME_RESOURCE_ALIAS)
        entry = _Entry(descriptor, layout, evidence=candidate_evidence)
        self._entries.append(entry)
        return entry

    def _check_identity(self, entry: _Entry) -> None:
        current = _evidence(tuple(entry.evidence or {}))
        if current != (entry.evidence or {}):
            raise RuntimeManagerError(E_RUNTIME_RESOURCE_IDENTITY_CHANGED)
        _reject_unsafe(current)

    async def _start(self, entry: _Entry) -> CMMSRuntime:
        try:
            assert entry.factory is not None
            runtime = await entry.factory.runtime.start()
            if runtime.state != "READY" or not runtime.started:
                raise RuntimeManagerError(E_RUNTIME_START_FAILED)
            entry.state = "READY"
            entry.evidence = _evidence(tuple(entry.evidence or {}))
            return runtime
        except BaseException as exc:
            entry.state = "FAILED"
            entry.failure_code = getattr(exc, "code", E_RUNTIME_START_FAILED)
            raise

    async def get(
        self,
        descriptor: RuntimeDescriptor,
        *,
        layout: Any,
        config: RuntimeConfig,
        dependencies: RuntimeDependencies | None = None,
    ) -> CMMSRuntime:
        self._check_loop()
        candidate = self._validate(descriptor, layout, config)
        entry = self._reserve(candidate, layout)
        if entry.state == "READY":
            assert entry.factory is not None
            return entry.factory.runtime
        if entry.state == "FAILED":
            raise RuntimeManagerError(entry.failure_code or E_RUNTIME_START_FAILED)
        if entry.task is None:
            try:
                entry.factory = self._factory_builder(layout, config=config, dependencies=dependencies)
            except BaseException as exc:
                entry.state = "FAILED"
                entry.failure_code = E_RUNTIME_FACTORY_BUILD_FAILED
                raise RuntimeManagerError(E_RUNTIME_FACTORY_BUILD_FAILED) from exc
            entry.task = asyncio.create_task(self._start(entry))
        return await asyncio.shield(entry.task)

    async def start_profile(
        self,
        descriptor: RuntimeDescriptor,
        *,
        layout: Any,
        config: RuntimeConfig,
        dependencies: RuntimeDependencies | None = None,
    ) -> CMMSRuntime:
        return await self.get(descriptor, layout=layout, config=config, dependencies=dependencies)
