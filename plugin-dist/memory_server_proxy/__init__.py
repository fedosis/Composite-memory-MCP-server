"""memory_server_proxy — standalone AF_UNIX client for an external CMMS native service.

The plugin implements the Hermes ``MemoryProvider`` contract over a short-lived
AF_UNIX session (connect -> hello -> op -> response -> close) against the CMMS
native owner service. It is stdlib-only (plus the host ``agent.memory_provider``
ABC) and never imports the ``memory_server`` backend.
"""

from __future__ import annotations

from .provider import MemoryServerProxyProvider

__all__ = ["MemoryServerProxyProvider", "register"]


def register(ctx) -> None:
    """Register a NEW provider instance on every call (multiplexing contract)."""
    ctx.register_memory_provider(MemoryServerProxyProvider())
