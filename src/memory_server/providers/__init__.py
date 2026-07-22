"""Providers package for vector and graph backends."""


def __getattr__(name: str):
    """Lazy-import provider classes — avoids pulling in optional deps (numpy, lancedb)
    at package-import time for clean-install 'memory-server serve'.
    """
    if name == "LanceDBProvider":
        from memory_server.providers.lancedb_provider import LanceDBProvider

        return LanceDBProvider
    if name == "QdrantProvider":
        from memory_server.providers.qdrant_provider import QdrantProvider

        return QdrantProvider
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["LanceDBProvider", "QdrantProvider"]
