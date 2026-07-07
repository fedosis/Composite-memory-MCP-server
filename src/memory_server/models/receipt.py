"""MemoryReceipt model — provenance metadata for every memory operation."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class VerificationStatus(str, Enum):
    """Verification status of a memory entry."""

    UNVERIFIED = "unverified"
    CANDIDATE = "candidate"
    VALIDATED = "validated"
    TRUSTED = "trusted"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


class LifecycleState(str, Enum):
    """Lifecycle state of a memory item — v0.6 spec.

    States flow forward only:
        candidate → validated → active → stale → archived → forgotten

    Each state is terminal for backward transitions — once promoted,
    an item can only move forward in the lifecycle.
    """

    CANDIDATE = "candidate"
    VALIDATED = "validated"
    ACTIVE = "active"
    STALE = "stale"
    ARCHIVED = "archived"
    FORGOTTEN = "forgotten"

    @classmethod
    def _missing_(cls, value: object) -> "LifecycleState | None":
        """Handle backward compatibility with old lifecycle values.

        Map:
            "trusted"    → "active"
            "deprecated" → "stale"
        """
        compat: dict[str, str] = {
            "trusted": "active",
            "deprecated": "stale",
        }
        if isinstance(value, str) and value.lower() in compat:
            return cls(compat[value.lower()])
        return None


class MemoryReceipt(BaseModel):
    """Provenance receipt for every memory operation.

    Per ADR-008: every memory object must carry source, creator,
    timestamp, confidence, verification status, and history.

    Canonical fields per spec: id, type, content, source, creator,
    created_at, updated_at, confidence, verification_status,
    lifecycle_state, version.
    """

    id: str
    memory_type: str
    source: str
    created_by: str
    timestamp: datetime
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    verification_status: VerificationStatus = VerificationStatus.UNVERIFIED
    history: list[Any] = []
    updated_at: datetime = Field(default_factory=lambda: datetime.now())
    lifecycle_state: str = "active"
    version: str = "0.1.0"

    model_config = ConfigDict(from_attributes=True)
