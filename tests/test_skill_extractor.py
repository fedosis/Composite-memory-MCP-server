"""Tests for SkillExtractor (Card 014)."""

import pytest

from memory_server.extractors.skill_extractor import SkillExtractor


class TestSkillExtractor:
    """Tests for the SkillExtractor class."""

    def test_extract_single_skill(self):
        """Extract a single skill using default pattern."""
        extractor = SkillExtractor()
        skills = extractor.extract("to deploy docker, do: 1) pull image, 2) run container")
        assert len(skills) == 1
        s = skills[0]
        assert s["purpose"] == "deploy docker"
        assert "pull image" in s["steps"]
        assert "run container" in s["steps"]
        assert s["confidence"] == 0.5

    def test_skill_step_parsing(self):
        """Steps are correctly parsed from numbered list."""
        extractor = SkillExtractor()
        skills = extractor.extract(
            "to backup database, do: 1) connect to db, 2) export data, 3) compress file"
        )
        assert len(skills) == 1
        steps = skills[0]["steps"]
        assert len(steps) == 3
        assert steps[0] == "connect to db"
        assert steps[1] == "export data"
        assert steps[2] == "compress file"

    def test_constraint_detection_lowercase(self):
        """Constraints prefixed with 'must' are detected."""
        extractor = SkillExtractor()
        skills = extractor.extract(
            "to deploy, do: 1) build, 2) push. must have 10GB free, must run on port 80"
        )
        assert len(skills) == 1
        constraints = skills[0].get("constraints", [])
        assert "have 10GB free" in constraints
        assert "run on port 80" in constraints

    def test_no_skill_found(self):
        """Text with no skill pattern returns empty list."""
        extractor = SkillExtractor()
        skills = extractor.extract("The weather is nice today.")
        assert skills == []

    def test_empty_input(self):
        """Empty input returns empty list."""
        extractor = SkillExtractor()
        skills = extractor.extract("")
        assert skills == []

    def test_llm_skill_extraction(self):
        """LLM-backed skill extraction returns richer skill objects."""

        def mock_llm(text: str) -> list[dict]:
            return [{
                "purpose": "deploy docker",
                "steps": ["git pull", "docker compose build", "docker compose up -d"],
                "constraints": ["needs 10GB free"],
            }]

        extractor = SkillExtractor(llm_extractor=mock_llm)
        skills = extractor.extract("Steps to deploy docker")
        assert len(skills) == 1
        s = skills[0]
        assert s["purpose"] == "deploy docker"
        assert len(s["steps"]) == 3
        assert 0.7 <= s["confidence"] <= 0.9

    def test_constraint_with_regex_skill(self):
        """Constraints from regex extraction are captured."""
        extractor = SkillExtractor()
        skills = extractor.extract(
            "to test, do: 1) run pytest. must install deps first"
        )
        assert len(skills) == 1
        assert "install deps first" in skills[0]["constraints"]

    def test_skill_without_constraints(self):
        """Skill without constraints returns empty constraints list."""
        extractor = SkillExtractor()
        skills = extractor.extract("to run, do: 1) start")
        assert skills[0].get("constraints", []) == []

    def test_multi_word_purpose(self):
        """Purpose can be multiple words."""
        extractor = SkillExtractor()
        skills = extractor.extract(
            "to set up monitoring stack, do: 1) install prometheus, 2) configure grafana"
        )
        assert skills[0]["purpose"] == "set up monitoring stack"
