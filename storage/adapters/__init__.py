"""Storage adapters — backward-compatible wrappers for existing code.

These adapters bridge the old monolithic SQLiteProvider interface
to the new storage/models + storage/repositories architecture.
"""

from storage.adapters.legacy_provider import LegacySQLiteProviderAdapter

__all__ = ["LegacySQLiteProviderAdapter"]
