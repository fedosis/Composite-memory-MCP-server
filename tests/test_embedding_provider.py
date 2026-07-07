"""Tests for embedding provider (Card 009)."""

import numpy as np
import pytest

from memory_server.providers.embedding_provider import (
    EmbeddingProvider,
    MockEmbeddingProvider,
    SentenceTransformerEmbeddingProvider,
)


class TestEmbeddingProviderInterface:
    """Verify the abstract interface contracts."""

    def test_abstract_class_cannot_be_instantiated(self):
        with pytest.raises(TypeError):
            EmbeddingProvider()  # type: ignore

    def test_mock_provider_base_class(self):
        provider = MockEmbeddingProvider()
        assert isinstance(provider, EmbeddingProvider)


class TestMockEmbeddingProvider:
    """Test the mock embedding provider used in testing."""

    DEFAULT_SIZE = 384

    def test_embed_returns_list_of_floats(self):
        provider = MockEmbeddingProvider(vector_size=self.DEFAULT_SIZE)
        result = provider.embed("hello world")
        assert isinstance(result, list)
        assert len(result) == self.DEFAULT_SIZE
        assert all(isinstance(v, float) for v in result)

    def test_embed_consistent_output(self):
        provider = MockEmbeddingProvider(vector_size=self.DEFAULT_SIZE)
        result1 = provider.embed("hello world")
        result2 = provider.embed("hello world")
        assert result1 == result2

    def test_embed_different_inputs_different_vectors(self):
        provider = MockEmbeddingProvider(vector_size=self.DEFAULT_SIZE)
        result1 = provider.embed("hello world")
        result2 = provider.embed("goodbye world")
        assert result1 != result2

    def test_embed_empty_string(self):
        provider = MockEmbeddingProvider(vector_size=self.DEFAULT_SIZE)
        result = provider.embed("")
        assert isinstance(result, list)
        assert len(result) == self.DEFAULT_SIZE

    def test_embed_batch_returns_list_of_vectors(self):
        provider = MockEmbeddingProvider(vector_size=self.DEFAULT_SIZE)
        texts = ["hello", "world", "test"]
        results = provider.embed_batch(texts)
        assert isinstance(results, list)
        assert len(results) == 3
        for r in results:
            assert len(r) == self.DEFAULT_SIZE
            assert all(isinstance(v, float) for v in r)

    def test_embed_batch_empty_list(self):
        provider = MockEmbeddingProvider(vector_size=self.DEFAULT_SIZE)
        results = provider.embed_batch([])
        assert results == []

    def test_embed_batch_single_item(self):
        provider = MockEmbeddingProvider(vector_size=self.DEFAULT_SIZE)
        results = provider.embed_batch(["single"])
        assert len(results) == 1
        assert len(results[0]) == self.DEFAULT_SIZE

    def test_custom_vector_size(self):
        provider = MockEmbeddingProvider(vector_size=128)
        result = provider.embed("test")
        assert len(result) == 128

    def test_reproducible_seed(self):
        provider1 = MockEmbeddingProvider(vector_size=64, seed=42)
        provider2 = MockEmbeddingProvider(vector_size=64, seed=42)
        assert provider1.embed("same text") == provider2.embed("same text")

        provider3 = MockEmbeddingProvider(vector_size=64, seed=99)
        assert provider1.embed("same text") != provider3.embed("same text")


@pytest.mark.skipif(
    not SentenceTransformerEmbeddingProvider._is_available(),
    reason="sentence-transformers not installed",
)
class TestSentenceTransformerEmbeddingProvider:
    """Test the real SentenceTransformer embedding provider.

    These tests require sentence-transformers to be installed.
    """

    @pytest.fixture
    def provider(self):
        return SentenceTransformerEmbeddingProvider()

    def test_embed_returns_list(self, provider):
        result = provider.embed("hello world")
        assert isinstance(result, list)
        assert len(result) == 384  # all-MiniLM-L6-v2
        assert all(isinstance(v, float) for v in result)

    def test_embed_normalized(self, provider):
        """all-MiniLM-L6-v2 produces normalized vectors (L2 norm ≈ 1)."""
        result = provider.embed("hello world")
        arr = np.array(result)
        norm = np.linalg.norm(arr)
        assert abs(norm - 1.0) < 0.001

    def test_embed_batch(self, provider):
        texts = ["hello", "world", "test embedding"]
        results = provider.embed_batch(texts)
        assert len(results) == 3
        for r in results:
            assert len(r) == 384
            assert all(isinstance(v, float) for v in r)

    def test_embed_batch_empty(self, provider):
        assert provider.embed_batch([]) == []

    def test_semantic_similarity(self, provider):
        """Similar texts have higher cosine similarity than dissimilar ones."""
        vec_a = provider.embed("I love programming in Python")
        vec_b = provider.embed("Python is a great programming language")
        vec_c = provider.embed("The weather is nice today")

        arr_a = np.array(vec_a)
        arr_b = np.array(vec_b)
        arr_c = np.array(vec_c)

        sim_ab = float(np.dot(arr_a, arr_b))
        sim_ac = float(np.dot(arr_a, arr_c))

        assert sim_ab > sim_ac, "Similar texts should have higher similarity"

    def test_model_name_configurable(self):
        provider = SentenceTransformerEmbeddingProvider(model_name="all-MiniLM-L6-v2")
        result = provider.embed("test")
        assert len(result) == 384
