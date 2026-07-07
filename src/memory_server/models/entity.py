"""Entity model — represents a knowledge object (server, project, etc.)."""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class Entity(BaseModel):
    """A knowledge entity representing a real-world object.

    Examples: server, software, vehicle, project.

    Canonical fields per spec: id, type, content, source, creator,
    created_at, updated_at, confidence, verification_status,
    lifecycle_state, version.
    """

    id: str
    type: str
    name: str
    attributes: dict = {}
    source: Optional[str] = None
    creator: str = "system"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    verification_status: str = "candidate"
    lifecycle_state: str = "active"
    version: str = "0.1.0"

    model_config = ConfigDict(from_attributes=True)
