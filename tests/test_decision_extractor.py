"""Tests for DecisionExtractor (Card 013)."""

import pytest

from memory_server.extractors.decision_extractor import DecisionExtractor


class TestDecisionExtractor:
    """Tests for the DecisionExtractor class."""

    def test_extract_single_decision(self):
        """Extract a single decision using default pattern."""
        extractor = DecisionExtractor()
        decisions = extractor.extract("We decided to use Caddy because it has better Docker integration")
        assert len(decisions) == 1
        d = decisions[0]
        assert d["choice"] == "use Caddy"
        assert d["reason"] == "it has better Docker integration"
        assert d["context"] == ""

    def test_extract_multiple_decisions(self):
        """Extract multiple decisions from one text."""
        extractor = DecisionExtractor()
        decisions = extractor.extract(
            "We decided to use Caddy because it is simpler. "
            "We decided to deploy on Docker because of portability."
        )
        assert len(decisions) == 2
        assert decisions[0]["choice"] == "use Caddy"
        assert decisions[1]["choice"] == "deploy on Docker"

    def test_no_decision_found(self):
        """Text with no decision pattern returns empty list."""
        extractor = DecisionExtractor()
        decisions = extractor.extract("The weather is nice today.")
        assert decisions == []

    def test_empty_input(self):
        """Empty input returns empty list."""
        extractor = DecisionExtractor()
        decisions = extractor.extract("")
        assert decisions == []

    def test_decision_with_context(self):
        """Decision with optional LLM context field."""
        def mock_llm(text: str) -> list[dict]:
            return [{
                "context": "Web server selection",
                "choice": "Caddy",
                "alternatives": ["Nginx", "Apache"],
                "reason": "Docker integration",
            }]

        extractor = DecisionExtractor(llm_extractor=mock_llm)
        decisions = extractor.extract("We should use Caddy")
        assert len(decisions) == 1
        d = decisions[0]
        assert d["context"] == "Web server selection"
        assert d["choice"] == "Caddy"
        assert d["reason"] == "Docker integration"
        assert 0.7 <= d["confidence"] <= 0.9

    def test_decision_with_alternatives(self):
        """Decision with rejected alternatives."""
        def mock_llm(text: str) -> list[dict]:
            return [{
                "context": "Database selection",
                "choice": "PostgreSQL",
                "alternatives": ["MySQL", "SQLite"],
                "reason": "ACID compliance",
            }]

        extractor = DecisionExtractor(llm_extractor=mock_llm)
        decisions = extractor.extract("Use PostgreSQL")
        assert len(decisions) == 1
        alts = decisions[0].get("alternatives", [])
        assert "MySQL" in alts
        assert "SQLite" in alts
        assert decisions[0]["confidence"] == 0.85

    def test_regex_confidence_default(self):
        """Default regex extractor assigns 0.5 confidence."""
        extractor = DecisionExtractor()
        decisions = extractor.extract("decided to refactor because code was messy")
        assert decisions[0]["confidence"] == 0.5

    def test_no_alternatives_in_regex(self):
        """Regex-based extraction doesn't populate alternatives."""
        extractor = DecisionExtractor()
        decisions = extractor.extract("We decided to rewrite because legacy was slow")
        assert len(decisions) == 1
        assert decisions[0].get("alternatives", []) == []

    def test_whitespace_handling(self):
        """Extra whitespace around decision text is handled."""
        extractor = DecisionExtractor()
        decisions = extractor.extract("  decided   to   upgrade   because   needed   speed  ")
        assert len(decisions) == 1
        assert decisions[0]["choice"] == "upgrade"
