"""Hermes MemoryProvider plugin for CMMS.

This package provides a native MemoryProvider implementation that allows
CMMS to integrate directly with Hermes as a first-class memory backend,
gaining access to lifecycle hooks (prefetch, sync_turn, on_session_end,
on_session_switch) unavailable over MCP.

Usage (in Hermes config.yaml):
  memory:
    providers:
      memory_server:
        plugin: memory_server.plugins.hermes.provider.HermesProvider
        enabled: true
"""

from memory_server.plugins.hermes.config import HermesPluginConfig
from memory_server.plugins.hermes.provider import HermesProvider

__all__ = ["HermesProvider", "HermesPluginConfig"]
