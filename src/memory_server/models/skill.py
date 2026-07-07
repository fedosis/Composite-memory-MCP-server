"""Skill model — procedural knowledge with versioning."""

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field


class Skill(BaseModel):
    """Procedural knowledge with steps, constraints, and validation.

    Contains purpose, steps, constraints, and validation criteria.
    """

    id: str
    name: str
    version: str = "1.0.0"
    purpose: str
    steps: list[str] = Field(..., min_length=1)
    constraints: list[str] = []
    validation: list[str] = []
    success_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    created_at: datetime = datetime.now(timezone.utc)

    model_config = ConfigDict(from_attributes=True)
