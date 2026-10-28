"""The closed S06 route surface and profile-route composition seam."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .profiles import ProfilePort, audit_ticket, control_reply, validate_profile_id, validate_query


@dataclass(frozen=True)
class RouteSpec:
    method: str
    path: str
    operation: str
    permission: str


# This tuple is intentionally exhaustive: later owner stages mount their own
# factories and must not become reachable through the S06 application.
S06_ROUTE_SPECS = (
    RouteSpec("GET", "/healthz", "health", "PUBLIC"),
    RouteSpec("GET", "/readyz", "ready", "PUBLIC"),
    RouteSpec("GET", "/api/v1/profiles", "profiles.list", "profile.read"),
    RouteSpec("GET", "/api/v1/profiles/{profile}", "profiles.get", "profile.read"),
    RouteSpec("POST", "/api/v1/profiles/{profile}/enable", "profiles.enable", "profile.manage"),
    RouteSpec("POST", "/api/v1/profiles/{profile}/disable", "profiles.disable", "profile.manage"),
)


def create_profile_routes(port: ProfilePort, guard: Any = None) -> tuple[RouteSpec, ...]:
    """Return the four S06 profile declarations for the parent app binder.

    ``guard`` is accepted as the parent-owned seam so this module never imports
    auth, limits, audit, or Starlette application code.  The parent binds these
    specs together with the two public health endpoints.
    """
    if not isinstance(port, ProfilePort) and not hasattr(port, "execute"):
        raise TypeError("profile GatewayPort required")
    return S06_ROUTE_SPECS[2:]


def create_routes(port: ProfilePort, guard: Any = None) -> tuple[RouteSpec, ...]:
    """Return the exact six route declarations owned by S06."""
    create_profile_routes(port, guard)
    return S06_ROUTE_SPECS


__all__ = [
    "ProfilePort",
    "RouteSpec",
    "S06_ROUTE_SPECS",
    "audit_ticket",
    "control_reply",
    "create_profile_routes",
    "create_routes",
    "validate_profile_id",
    "validate_query",
]
