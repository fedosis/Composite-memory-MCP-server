"""Decision model — a chosen solution with its reasoning."""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class Decision(BaseModel):
    """A decision representing a chosen solution and its reasoning.

    Example: Choose Caddy over Nginx for better Docker integration.

    Canonical fields per spec: id, type, content, source, creator,
    created_at, updated_at, confidence, verification_status,
    lifecycle_state, version.
    """

    id: str
    context: str
    choice: str
    rejected_alternatives: list[str] = []
    reason: str
    source: Optional[str] = None
    creator: str = "system"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    verification_status: str = "candidate"
    lifecycle_state: str = "active"
    version: str = "0.1.0"

    model_config = ConfigDict(from_attributes=True)
