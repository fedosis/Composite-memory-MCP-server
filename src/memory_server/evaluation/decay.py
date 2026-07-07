"""Decay engine — time-based confidence decay and lifecycle state transitions.

Per-type TTLs control how long different memory types live before
they transition through the lifecycle:

    active  ──[70% TTL]──→  stale  ──[100% TTL]──→  archived  ──[200% TTL]──→  forgotten
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from memory_server.evaluation.validator import Validator
from memory_server.models.receipt import LifecycleState

# Per-type TTLs (in days for production, hours for testing)
PER_TYPE_TTL: dict[str, float] = {
    "fact": 90.0,       # 90 days
    "decision": 180.0,  # 180 days
    "skill": 365.0,     # 365 days
    "entity": 365.0,    # 365 days
}

DEFAULT_ARCHIVE_THRESHOLD = 0.2

# Lifecycle state TTL ratios
STALE_RATIO = 0.7       # 70% of TTL → active → stale
ARCHIVE_RATIO = 1.0     # 100% of TTL → stale → archived
FORGOTTEN_RATIO = 2.0   # 200% of TTL → archived → forgotten


class DecayEngine:
    """Manages time-based decay and lifecycle state transitions.

    Args:
        per_type_ttl: Override the default per-type TTLs (in *days*).
        archive_threshold: Confidence below this triggers archival.
        validator: Optional Validator to drive lifecycle transitions.
    """

    def __init__(
        self,
        per_type_ttl: dict[str, float] | None = None,
        archive_threshold: float = DEFAULT_ARCHIVE_THRESHOLD,
        validator: Validator | None = None,
    ) -> None:
        self._per_type_ttl = dict(PER_TYPE_TTL)
        if per_type_ttl:
            self._per_type_ttl.update(per_type_ttl)
        self._archive_threshold = archive_threshold
        self._validator = validator or Validator()
        # Items registered for decay tracking
        self._items: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        item_id: str,
        item_type: str,
        created_at: datetime | None = None,
        lifecycle_state: str | None = None,
    ) -> None:
        """Register an item for decay tracking."""
        self._items[item_id] = {
            "id": item_id,
            "type": item_type,
            "created_at": created_at or datetime.now(timezone.utc),
            "confidence": 1.0,
            "lifecycle_state": lifecycle_state or "active",
        }
        # Also register with the validator
        self._validator.register(
            item_id,
            initial_status=LifecycleState(lifecycle_state) if lifecycle_state else LifecycleState.ACTIVE,
            confidence=1.0,
        )

    def update_confidence(self, item_id: str, confidence: float) -> None:
        """Update the stored confidence for an item."""
        if item_id in self._items:
            self._items[item_id]["confidence"] = max(0.0, min(1.0, confidence))

    def update_lifecycle_state(self, item_id: str, state: str) -> None:
        """Update the stored lifecycle state for an item."""
        if item_id in self._items:
            self._items[item_id]["lifecycle_state"] = state

    def get_lifecycle_state(self, item_id: str) -> str | None:
        """Get the current lifecycle state for a registered item."""
        item = self._items.get(item_id)
        if item:
            # Check the validator for the authoritative state
            try:
                status = self._validator.get_status(item_id)
                return status["status"]
            except KeyError:
                return item.get("lifecycle_state")
        return None

    def set_validator(self, validator: Validator) -> None:
        """Set the validator instance (used during initialization)."""
        self._validator = validator

    # ------------------------------------------------------------------
    # Decay calculations
    # ------------------------------------------------------------------

    def decay(self, item: dict[str, Any] | None = None) -> float:
        """Compute the confidence after applying time decay.

        Takes an *item* dict with keys ``type``, ``created_at``, and ``confidence``.
        If ``None`` is passed, looks up the item from internal registry by ``id``
        if present, or returns 0.0.

        Returns:
            New confidence in [0.0, 1.0].
        """
        if item is None:
            return 0.0

        item_type = item.get("type", "fact")
        created_at = item.get("created_at")
        current_conf = item.get("confidence", 1.0)
        return self._apply_decay(current_conf, item_type, created_at)

    def should_archive(self, item: dict[str, Any] | None = None) -> bool:
        """Check if an item should be archived.

        An item should be archived if its decayed confidence is below
        the threshold **or** its age exceeds its TTL.

        Args:
            item: Dict with keys ``type``, ``created_at``, ``confidence``, ``id``.
                  If None, returns False.

        Returns:
            True if the item should be archived.
        """
        if item is None:
            return False

        item_type = item.get("type", "fact")
        created_at = item.get("created_at")
        current_conf = item.get("confidence", 1.0)
        decayed_conf = self._apply_decay(current_conf, item_type, created_at)

        if decayed_conf < self._archive_threshold:
            return True

        if created_at is not None:
            age_days = self._age_in_days(created_at)
            ttl = self._per_type_ttl.get(item_type, 90.0)
            if age_days > ttl:
                return True

        return False

    def get_expired(self) -> list[dict[str, Any]]:
        """Return all registered items that are past their TTL.

        Returns:
            List of item dicts whose age exceeds their type's TTL.
        """
        expired: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        for item_id, item in self._items.items():
            created_at = item["created_at"]
            age_days = (now - created_at).total_seconds() / 86400.0
            ttl = self._per_type_ttl.get(item["type"], 90.0)
            if age_days > ttl:
                expired.append(dict(item))
        return expired

    # ------------------------------------------------------------------
    # Lifecycle state transitions (triggered by time)
    # ------------------------------------------------------------------

    def tick(self, item_id: str) -> str | None:
        """Evaluate a single item and apply lifecycle state transition if needed.

        Determines what state the item should be in based on its age vs TTL:

        - Age >= 200% TTL → archived → forgotten
        - Age >= 100% TTL → stale → archived
        - Age >= 70% TTL  → active → stale

        Returns the new lifecycle state string, or None if no transition.
        """
        item = self._items.get(item_id)
        if item is None:
            return None

        created_at = item["created_at"]
        age_days = self._age_in_days(created_at)
        ttl = self._per_type_ttl.get(item["type"], 90.0)
        if ttl <= 0:
            return None

        age_ratio = age_days / ttl

        # Get current state from validator
        try:
            status = self._validator.get_status(item_id)
            current_state = status["status"]
        except KeyError:
            current_state = item.get("lifecycle_state", "active")

        new_state: str | None = None

        # Check transitions in forward order
        if current_state in ("active",) and age_ratio >= STALE_RATIO:
            try:
                self._validator.mark_stale(item_id, reason=f"Decay: {age_ratio:.1%} of TTL")
                new_state = "stale"
            except (ValueError, KeyError):
                pass

        elif current_state in ("stale",) and age_ratio >= ARCHIVE_RATIO:
            try:
                self._validator.archive(item_id, reason=f"TTL expired: {age_ratio:.1%} of TTL")
                new_state = "archived"
            except (ValueError, KeyError):
                pass

        elif current_state in ("archived",) and age_ratio >= FORGOTTEN_RATIO:
            try:
                self._validator.forget(item_id, reason=f"Extended TTL expired: {age_ratio:.1%} of TTL")
                new_state = "forgotten"
            except (ValueError, KeyError):
                pass

        if new_state:
            self._items[item_id]["lifecycle_state"] = new_state

        return new_state

    def tick_all(self) -> list[dict[str, Any]]:
        """Evaluate all registered items and apply any lifecycle transitions.

        Returns:
            List of dicts with ``id`` and ``new_state`` for items that transitioned.
        """
        transitions: list[dict[str, Any]] = []
        for item_id in list(self._items.keys()):
            new_state = self.tick(item_id)
            if new_state:
                transitions.append({"id": item_id, "new_state": new_state})
        return transitions

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_decay(
        self,
        confidence: float,
        item_type: str,
        created_at: datetime | None,
    ) -> float:
        """Apply exponential decay to a confidence score.

        The decay factor is ``2^(-age/TTL)`` with a floor of 0.1.
        """
        if created_at is None:
            return confidence
        age_days = self._age_in_days(created_at)
        ttl = self._per_type_ttl.get(item_type, 90.0)
        if ttl <= 0:
            return confidence
        ratio = age_days / ttl
        decay_factor = max(0.1, 2.0 ** (-ratio))
        return confidence * decay_factor

    @staticmethod
    def _age_in_days(created_at: datetime | None) -> float:
        """Compute age in days. Returns 0 for None."""
        if created_at is None:
            return 0.0
        now = datetime.now(timezone.utc)
        delta = now - created_at
        return max(0.0, delta.total_seconds() / 86400.0)

    def get_ttl(self, item_type: str) -> float:
        """Get the TTL for a given item type."""
        return self._per_type_ttl.get(item_type, 90.0)
