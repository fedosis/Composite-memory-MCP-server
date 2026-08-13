"""MemoryReceipt model — provenance metadata for every memory operation."""

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ReceiptEvidence(BaseModel):
    """Schema contract for ``metadata.evidence`` entries stored in history.

    The envelope stored in ``MemoryReceipt.history`` stays free-form, but the
    ``metadata.evidence`` payload follows this contract:

    - method: str
    - sources: list[str]
    - session_id: str
    - confidence: float
    - source_date: str
    - derived_from: list[str]
    - claim_type: fact|authority|state

    Only ``derived_from`` and ``claim_type`` are enforced by the remember
    tool today; all other metadata keys remain pass-through.
    """

    method: str | None = None
    sources: list[str] = Field(default_factory=list)
    session_id: str | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    source_date: str | None = None
    derived_from: list[str] = Field(default_factory=list)
    claim_type: Literal["fact", "authority", "state"] | None = None

    model_config = ConfigDict(extra="allow")

    @field_validator("derived_from", mode="before")
    @classmethod
    def _validate_derived_from(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("derived_from must be a list[str]")
        if any(not isinstance(item, str) or not item for item in value):
            raise ValueError("derived_from must be a list[str]")
        return value

    @field_validator("claim_type", mode="before")
    @classmethod
    def _normalize_claim_type(cls, value: Any) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("claim_type must be one of: fact, authority, state")
        normalized = value.strip().lower()
        if normalized not in {"fact", "authority", "state"}:
            raise ValueError("claim_type must be one of: fact, authority, state")
        return normalized


class VerificationStatus(str, Enum):
    """Verification status of a memory entry."""

    UNVERIFIED = "unverified"
    CANDIDATE = "candidate"
    VALIDATED = "validated"
    TRUSTED = "trusted"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


class LifecycleState(str, Enum):
    """Lifecycle state of a memory item — v0.6 spec with belief states.

    States flow forward only:
        candidate → validated → active → stale → archived → forgotten

    Belief-specific states (Card 001):
        active ↔ superseded | contradicted | discarded
        superseded → stale
        contradicted → stale | discarded
        discarded → archived

    Each state is terminal for backward transitions — once promoted,
    an item can only move forward in the lifecycle.
    """

    CANDIDATE = "candidate"
    VALIDATED = "validated"
    ACTIVE = "active"
    STALE = "stale"
    ARCHIVED = "archived"
    FORGOTTEN = "forgotten"
    # Belief-specific states
    SUPERSEDED = "superseded"
    CONTRADICTED = "contradicted"
    DISCARDED = "discarded"

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
    history: list[Any] = Field(default_factory=list)
    updated_at: datetime = Field(default_factory=lambda: datetime.now())
    lifecycle_state: str = "active"
    version: str = "0.1.0"

    model_config = ConfigDict(from_attributes=True)
