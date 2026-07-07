"""Entity model — represents a knowledge object (server, project, etc.)."""

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict


class Entity(BaseModel):
    """A knowledge entity representing a real-world object.

    Examples: server, software, vehicle, project.
    """

    id: str
    type: str
    name: str
    attributes: dict = {}
    created_at: datetime = datetime.now(timezone.utc)
    updated_at: datetime = datetime.now(timezone.utc)

    model_config = ConfigDict(from_attributes=True)
