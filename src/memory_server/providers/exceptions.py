"""Typed exceptions for vector database providers.

Provider boundary contract (Card 3a): every public method of
LanceDBProvider / QdrantProvider converts backend failures into
ProviderWriteError (write/create/delete/optimize) or ProviderSearchError
(search/scroll/list/count). No raw backend exception escapes the boundary.
"""


class ProviderError(Exception):
    """Base class for all vector provider errors."""


class ProviderWriteError(ProviderError):
    """Backend write/create/delete/optimize failure."""


class ProviderSearchError(ProviderError):
    """Backend search/scroll/list/count failure."""
