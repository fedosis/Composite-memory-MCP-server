"""Tests for FactExtractor (Card 012)."""

import pytest

from memory_server.extractors.fact_extractor import FactExtractor


class TestFactExtractor:
    """Tests for the FactExtractor class."""

    def test_extract_single_fact(self):
        """Extract a single fact from text using default regex extractor."""
        extractor = FactExtractor()
        facts = extractor.extract("Python is great")
        assert len(facts) == 1
        assert facts[0]["subject"] == "Python"
        assert facts[0]["predicate"] == "is"
        assert facts[0]["object"] == "great"
        assert facts[0]["confidence"] == 0.5

    def test_extract_multiple_facts(self):
        """Extract multiple facts from one text."""
        extractor = FactExtractor()
        facts = extractor.extract("Docker is container. Linux is kernel.")
        assert len(facts) == 2
        assert facts[0]["subject"] == "Docker"
        assert facts[1]["subject"] == "Linux"

    def test_empty_input(self):
        """Empty input returns empty list."""
        extractor = FactExtractor()
        facts = extractor.extract("")
        assert facts == []

    def test_no_match_returns_empty(self):
        """Input with no 'X is Y' pattern returns empty list."""
        extractor = FactExtractor()
        facts = extractor.extract("Hello world!")
        assert facts == []

    def test_confidence_scoring_regex(self):
        """Default regex extractor should return 0.5 confidence."""
        extractor = FactExtractor()
        facts = extractor.extract("Python is great")
        assert facts[0]["confidence"] == 0.5

    def test_confidence_scoring_llm(self):
        """When an LLM extractor is used, confidence should be 0.7-0.9."""

        def mock_llm(text: str) -> list[dict]:
            return [{"subject": "Python", "predicate": "created_by", "object": "Guido"}]

        extractor = FactExtractor(llm_extractor=mock_llm)
        facts = extractor.extract("Python created_by Guido (LLM extracted)")
        assert len(facts) >= 1
        assert 0.7 <= facts[0]["confidence"] <= 0.9

    def test_llm_confidence_default(self):
        """LLM extractor uses default confidence 0.85."""

        def mock_llm(text: str) -> list[dict]:
            return [{"subject": "A", "predicate": "relates_to", "object": "B"}]

        extractor = FactExtractor(llm_extractor=mock_llm)
        facts = extractor.extract("A relates to B")
        assert facts[0]["confidence"] == 0.85

    def test_mixed_extraction(self):
        """Regex extracts 'is' facts; LLM may add more."""
        # If both regex and LLM return results, should include both
        def mock_llm(text: str) -> list[dict]:
            return [{"subject": "Python", "predicate": "created_by", "object": "Guido"}]

        extractor = FactExtractor(llm_extractor=mock_llm)
        facts = extractor.extract("Python is great. LLM:: Python created_by Guido")
        # Regex finds "Python is great"; LLM finds "Python created_by Guido"
        assert len(facts) >= 2

    def test_spo_triple_structure(self):
        """Each fact must have subject, predicate, object keys."""
        extractor = FactExtractor()
        facts = extractor.extract("Caddy is web-server")
        assert len(facts) == 1
        f = facts[0]
        assert "subject" in f
        assert "predicate" in f
        assert "object" in f
        assert "confidence" in f

    def test_regex_only_mode(self):
        """Without LLM, only regex is used."""
        extractor = FactExtractor()
        facts = extractor.extract("Python feels dynamic")
        assert facts == []

    def test_whitespace_and_punctuation(self):
        """Facts with extra whitespace and punctuation are handled."""
        extractor = FactExtractor()
        facts = extractor.extract("PostgreSQL  is   relational.  MySQL  is   fast!")
        assert len(facts) == 2
        assert facts[0]["subject"] == "PostgreSQL"
        assert facts[0]["predicate"] == "is"
        assert facts[1]["subject"] == "MySQL"

    def test_both_regex_and_llm_confidence(self):
        """Regex facts get 0.5, LLM facts get 0.85 (default)."""

        def mock_llm(text: str) -> list[dict]:
            return [{"subject": "X", "predicate": "uses", "object": "Y"}]

        extractor = FactExtractor(llm_extractor=mock_llm)
        facts = extractor.extract("A is B. X uses Y.")
        # A is B = regex (0.5), X uses Y = LLM (0.85)
        for f in facts:
            if f["predicate"] == "is":
                assert f["confidence"] == 0.5
            else:
                assert f["confidence"] == 0.85
