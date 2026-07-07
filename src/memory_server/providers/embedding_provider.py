"""Embedding provider interface and implementations.

Provides an abstract EmbeddingProvider with:
- MockEmbeddingProvider for testing (deterministic hash-based vectors)
- SentenceTransformerEmbeddingProvider for local all-MiniLM-L6-v2
- OpenAIEmbeddingProvider for remote embeddings via OpenAI API
"""

from __future__ import annotations

import hashlib
import logging
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class EmbeddingProvider(ABC):
    """Abstract interface for embedding providers.

    Implementations must provide embed() and embed_batch() methods.
    """

    @abstractmethod
    def embed(self, text: str) -> list[float]:
        """Embed a single text string into a vector.

        Args:
            text: Input text to embed.

        Returns:
            List of floats representing the embedding vector.
        """
        ...

    @abstractmethod
    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple text strings into vectors.

        Args:
            texts: List of input texts to embed.

        Returns:
            List of embedding vectors, one per input text.
        """
        ...


class MockEmbeddingProvider(EmbeddingProvider):
    """Deterministic mock embedding provider for testing.

    Generates pseudo-random vectors based on text hash, producing consistent
    outputs for the same input within the same session.

    Args:
        vector_size: Dimensionality of generated vectors (default 384).
        seed: Random seed for reproducibility (default 42).
    """

    def __init__(self, vector_size: int = 384, seed: int = 42) -> None:
        self._vector_size = vector_size
        self._seed = seed
        self._cache: dict[str, list[float]] = {}

    def _hash_to_vector(self, text: str) -> list[float]:
        """Generate a deterministic vector from text using hash."""
        # Combine text hash with seed for reproducibility
        h = hashlib.sha256(f"{self._seed}:{text}".encode()).digest()
        # Repeat hash bytes to fill vector_size
        floats = []
        for i in range(self._vector_size):
            byte_val = h[i % len(h)]
            floats.append(float(byte_val) / 256.0)
        return floats

    def embed(self, text: str) -> list[float]:
        if text not in self._cache:
            self._cache[text] = self._hash_to_vector(text)
        return self._cache[text].copy()

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [self.embed(t) for t in texts]


# ------------------------------------------------------------------ #
# SentenceTransformer (local) provider
# ------------------------------------------------------------------ #


class SentenceTransformerEmbeddingProvider(EmbeddingProvider):
    """Local embedding provider using sentence-transformers.

    Uses the all-MiniLM-L6-v2 model by default (384-dim, efficient).
    The model is loaded on first use (lazy initialization).

    Args:
        model_name: HuggingFace model name (default "all-MiniLM-L6-v2").
        device: Device to run on ("cpu", "cuda", or None for auto).
        batch_size: Batch size for embed_batch (default 32).
    """

    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        device: str | None = None,
        batch_size: int = 32,
    ) -> None:
        self._model_name = model_name
        self._device = device
        self._batch_size = batch_size
        self._model: Any = None

    @staticmethod
    def _is_available() -> bool:
        """Check if sentence-transformers is installed."""
        try:
            import sentence_transformers  # noqa: F401
            return True
        except ImportError:
            return False

    def _get_model(self):
        """Lazy-load the sentence-transformers model."""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer

                self._model = SentenceTransformer(
                    self._model_name,
                    device=self._device,
                )
                logger.info(
                    "Loaded SentenceTransformer model '%s' on %s",
                    self._model_name,
                    self._model.device,
                )
            except ImportError as exc:
                raise ImportError(
                    "sentence-transformers is not installed. "
                    "Install with: pip install 'composite-memory-mcp-server[sentence]'"
                ) from exc
        return self._model

    def embed(self, text: str) -> list[float]:
        model = self._get_model()
        embedding = model.encode(text, normalize_embeddings=True)
        return embedding.tolist()

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        model = self._get_model()
        embeddings = model.encode(
            texts,
            normalize_embeddings=True,
            batch_size=self._batch_size,
            show_progress_bar=False,
        )
        return [e.tolist() for e in embeddings]


# ------------------------------------------------------------------ #
# OpenAI (remote) provider
# ------------------------------------------------------------------ #


class OpenAIEmbeddingProvider(EmbeddingProvider):
    """Remote embedding provider using OpenAI-compatible API.

    Args:
        model: Model name (default "text-embedding-3-small").
        api_key: OpenAI API key (default: OPENAI_API_KEY env var).
        base_url: API base URL (default: https://api.openai.com/v1).
        vector_size: Expected vector dimensionality (default: 1536 for text-embedding-3-small).
    """

    def __init__(
        self,
        model: str = "text-embedding-3-small",
        api_key: str | None = None,
        base_url: str | None = None,
        vector_size: int = 1536,
    ) -> None:
        self._model = model
        self._api_key = api_key
        self._base_url = base_url
        self._vector_size = vector_size
        self._client: Any = None

    @staticmethod
    def _is_available() -> bool:
        """Check if openai package is installed."""
        try:
            import openai  # noqa: F401
            return True
        except ImportError:
            return False

    def _get_client(self):
        """Lazy-init the OpenAI client."""
        if self._client is None:
            try:
                from openai import OpenAI

                self._client = OpenAI(
                    api_key=self._api_key,
                    base_url=self._base_url,
                )
            except ImportError as exc:
                raise ImportError(
                    "openai package is not installed. "
                    "Install with: pip install openai"
                ) from exc
        return self._client

    def embed(self, text: str) -> list[float]:
        client = self._get_client()
        resp = client.embeddings.create(model=self._model, input=text)
        return resp.data[0].embedding

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        client = self._get_client()
        resp = client.embeddings.create(model=self._model, input=texts)
        # Sort by index to preserve input order
        sorted_data = sorted(resp.data, key=lambda x: x.index)
        return [d.embedding for d in sorted_data]
