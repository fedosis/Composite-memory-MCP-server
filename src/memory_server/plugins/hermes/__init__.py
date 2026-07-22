"""Hermes MemoryProvider plugin for CMMS.

This package provides a native MemoryProvider implementation that allows
CMMS to integrate directly with Hermes as a first-class memory backend,
gaining access to lifecycle hooks (prefetch, sync_turn, on_session_end,
on_session_switch) unavailable over MCP.

# Usage (in Hermes config.yaml):
#  memory:
#    provider: memory_server
#    providers:
#      memory_server:
#        plugin: memory_server.plugins.hermes.provider.HermesProvider
#        enabled: true
#
# Or via the user plugin shim (deployed to ~/.hermes/plugins/memory_server/):
#   Hermes discovers the plugin, imports this package, calls register(ctx).
"""

import logging

from memory_server.plugins.hermes.config import HermesPluginConfig
from memory_server.plugins.hermes.provider import HermesProvider

logger = logging.getLogger(__name__)


def register(ctx) -> None:
    """Register CMMS HermesProvider with Hermes plugin system.

    Hermes _load_provider_from_dir() calls this function after importing the
    module. The ctx object is a _ProviderCollector that captures the provider
    instance via register_memory_provider().
    """
    provider = HermesProvider()
    ctx.register_memory_provider(provider)
    logger.info("CMMS HermesProvider registered via register()")


__all__ = ["HermesProvider", "HermesPluginConfig", "register"]
