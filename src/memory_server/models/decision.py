"""Decision model — a chosen solution with its reasoning."""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict


class Decision(BaseModel):
    """A decision representing a chosen solution and its reasoning.

    Example: Choose Caddy over Nginx for better Docker integration.
    """

    id: str
    context: str
    choice: str
    rejected_alternatives: list[str] = []
    reason: str
    source: Optional[str] = None
    created_at: datetime = datetime.now(timezone.utc)

    model_config = ConfigDict(from_attributes=True)
