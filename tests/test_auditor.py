"""Tests for MemoryAuditor (Card 025).

Phase 7 expansion adds: orphan records, missing receipts, lifecycle
violations, confidence flags, SQL/vector drift, SQL/graph drift, and
full report coverage.
"""

import pytest

from memory_server.evaluation.auditor import MemoryAuditor
from memory_server.evaluation.validator import Validator

# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture
def auditor() -> MemoryAuditor:
    v = Validator()
    return MemoryAuditor(validator=v)


@pytest.fixture
def populated_auditor() -> MemoryAuditor:
    """Auditor with a mix of facts at various states."""
    v = Validator()
    # Register a variety of facts
    v.register("high-conf", confidence=0.95)
    v.register("mid-conf", confidence=0.65)
    v.register("low-conf", confidence=0.25)
    v.register("zero-conf", confidence=0.0)
    v.register("validated-good", confidence=0.85)

    # Promote some
    v.validate("validated-good")
    v.set_corroboration_count("validated-good", 2)
    v.trust("validated-good")

    return MemoryAuditor(validator=v)


@pytest.fixture
def auditor_with_receipts() -> MemoryAuditor:
    """Auditor with known receipts for some items."""
    v = Validator()
    v.register("fact-a", confidence=0.9)
    v.register("fact-b", confidence=0.8)
    v.register("fact-c", confidence=0.7)  # no receipt — orphan

    receipt_ids = {"fact-a", "fact-b"}
    return MemoryAuditor(validator=v, receipt_ids=receipt_ids)


@pytest.fixture
def auditor_with_lifecycle_violations() -> MemoryAuditor:
    """Auditor with items in invalid lifecycle states."""
    v = Validator()
    v.register("good-item", confidence=0.9)
    v.register("valid-item", confidence=0.8)

    # Register items with raw states that are invalid (not in LifecycleState)
    # We need to directly manipulate the store to inject bad states
    v._store["bad-state"] = {
        "fact_id": "bad-state",
        "status": "invalid_status",
        "confidence": 0.5,
        "history": [],
        "corroboration_count": 0,
        "conflict_count": 0,
    }
    v._store["unknown-state"] = {
        "fact_id": "unknown-state",
        "status": "foobar",
        "confidence": 0.5,
        "history": [],
        "corroboration_count": 0,
        "conflict_count": 0,
    }

    return MemoryAuditor(validator=v)


# ------------------------------------------------------------------
# Legacy tests — must remain passing for backward compatibility
# ------------------------------------------------------------------

class TestMemoryAuditor:
    """MemoryAuditor — consistency, orphans, confidence, report."""

    # --- consistency check ---

    def test_consistency_empty(self, auditor):
        """Empty validator → no warnings."""
        assert auditor.audit_consistency() == []

    def test_consistency_zero_confidence_not_deprecated(self, populated_auditor):
        """Zero-confidence fact not deprecated triggers warning."""
        warnings = populated_auditor.audit_consistency()
        zero_warnings = [w for w in warnings if "zero-conf" in w]
        assert len(zero_warnings) >= 1
        assert "confidence 0.0" in zero_warnings[0]

    def test_consistency_stale_high_confidence(self, auditor):
        """Stale fact with high confidence triggers warning."""
        auditor._validator.register("d1", confidence=0.8)
        auditor._validator.deprecate("d1")
        warnings = auditor.audit_consistency()
        dep_warnings = [w for w in warnings if "d1" in w]
        assert len(dep_warnings) >= 1
        assert "Stale" in dep_warnings[0]

    def test_consistency_clean_high_confidence(self, populated_auditor):
        """High-confidence trusted fact should not warn."""
        warnings = populated_auditor.audit_consistency()
        trusted_warnings = [w for w in warnings if "validated-good" in w]
        assert len(trusted_warnings) == 0

    # --- orphan detection ---

    def test_orphans_no_graph(self, auditor):
        """No graph provider → skipped message."""
        orphans = auditor.audit_orphans()
        assert len(orphans) == 1
        assert "No graph provider" in orphans[0]

    def test_orphans_with_graph(self):
        """Orphan detection with graph finds nodes with no neighbors."""
        from memory_server.providers.graph_provider import SimpleGraph

        v = Validator()
        g = SimpleGraph()
        g.add_node("a", "entity", "NodeA")
        g.add_node("b", "entity", "NodeB")
        g.add_edge("a", "b", "connects")

        auditor = MemoryAuditor(validator=v, graph=g)
        orphans = auditor.audit_orphans()
        expected = [n for n in g.get_all_nodes() if not g.get_neighbors(n.id)]
        assert len(orphans) == len(expected)

    def test_orphans_all_linked(self):
        """Fully connected graph → no orphans."""
        from memory_server.providers.graph_provider import SimpleGraph

        v = Validator()
        g = SimpleGraph()
        g.add_node("a", "entity", "A")
        g.add_node("b", "entity", "B")
        g.add_edge("a", "b", "linked")

        auditor = MemoryAuditor(validator=v, graph=g)
        orphans = auditor.audit_orphans()
        assert len(orphans) == 0

    # --- confidence distribution ---

    def test_confidence_empty(self, auditor):
        """Empty validator → empty stats."""
        stats = auditor.audit_confidence()
        assert stats["total"] == 0
        assert stats["buckets"] == {}
        assert stats["low_confidence"] == []

    def test_confidence_distribution(self, populated_auditor):
        """Distribution correctly counts items per bucket."""
        stats = populated_auditor.audit_confidence()
        assert stats["total"] == 5
        assert stats["buckets"]["0.0-0.3"] >= 1  # zero-conf, low-conf
        assert stats["buckets"]["0.85-1.0"] >= 2  # high-conf, validated-good

    def test_confidence_low_confidence_list(self, populated_auditor):
        """Low-confidence items are listed."""
        stats = populated_auditor.audit_confidence()
        low = stats["low_confidence"]
        # zero-conf (0.0) and low-conf (0.25) are both < 0.3
        assert len(low) >= 1
        assert "zero-conf" in low or "low-conf" in low

    # --- audit report ---

    def test_report_full(self, populated_auditor):
        """Full report includes all sections."""
        report = populated_auditor.audit_report("full")
        assert report["audit_type"] == "full"
        assert "warnings" in report
        assert "errors" in report
        assert "stats" in report

    def test_report_consistency(self, populated_auditor):
        """Consistency-only report."""
        report = populated_auditor.audit_report("consistency")
        assert report["audit_type"] == "consistency"
        assert isinstance(report["warnings"], list)
        assert "stats" in report  # empty but present

    def test_report_confidence(self, populated_auditor):
        """Confidence-only report includes stats."""
        report = populated_auditor.audit_report("confidence")
        assert report["audit_type"] == "confidence"
        assert report["stats"]["confidence"]["total"] == 5

    def test_report_orphans(self, auditor):
        """Orphan-only report."""
        report = auditor.audit_report("orphans")
        assert report["audit_type"] == "orphans"

    def test_report_warning_for_low_confidence(self, populated_auditor):
        """Low-confidence items trigger a warning."""
        report = populated_auditor.audit_report("full")
        low_warnings = [w for w in report["warnings"] if "low-confidence" in w]
        assert len(low_warnings) >= 1


# ------------------------------------------------------------------
# Phase 7: New audit checks
# ------------------------------------------------------------------

class TestPhase7OrphanRecords:
    """Phase 7 — orphan records detection."""

    def test_orphan_detection_no_receipt_store(self, auditor):
        """No receipt store → warning message."""
        orphans = auditor.check_orphan_records()
        assert len(orphans) >= 1
        assert "No receipt store" in orphans[0]

    def test_orphan_detection_flags_missing(self, auditor_with_receipts):
        """Create fact without receipt → auditor flags it."""
        orphans = auditor_with_receipts.check_orphan_records()
        # fact-c has no receipt
        assert "fact-c" in orphans
        # fact-a and fact-b have receipts
        assert "fact-a" not in orphans
        assert "fact-b" not in orphans

    def test_orphan_detection_clean(self):
        """All items have receipts → no orphans."""
        v = Validator()
        v.register("x", confidence=0.9)
        v.register("y", confidence=0.8)
        a = MemoryAuditor(validator=v, receipt_ids={"x", "y"})
        assert a.check_orphan_records() == []


class TestPhase7MissingReceipts:
    """Phase 7 — missing receipts detection."""

    def test_missing_receipts_no_store(self, auditor):
        """No receipt store → warning message."""
        results = auditor.check_missing_receipts()
        assert len(results) >= 1
        assert "No receipt store" in results[0]

    def test_missing_receipts_flags_items(self, auditor_with_receipts):
        """Items without receipts are flagged."""
        missing = auditor_with_receipts.check_missing_receipts()
        assert "fact-c" in missing
        assert "fact-a" not in missing

    def test_missing_receipts_all_match(self):
        """All items have receipts → empty list."""
        v = Validator()
        v.register("a", confidence=0.9)
        v.register("b", confidence=0.8)
        a = MemoryAuditor(validator=v, receipt_ids={"a", "b"})
        assert a.check_missing_receipts() == []


class TestPhase7LifecycleViolations:
    """Phase 7 — lifecycle state validation."""

    def test_lifecycle_violations_empty(self, auditor):
        """Empty validator → no violations."""
        assert auditor.check_lifecycle_violations() == []

    def test_lifecycle_violations_clean(self, populated_auditor):
        """Valid states → no violations."""
        violations = populated_auditor.check_lifecycle_violations()
        assert violations == []

    def test_lifecycle_violations_detected(self, auditor_with_lifecycle_violations):
        """Items with invalid states are flagged."""
        violations = auditor_with_lifecycle_violations.check_lifecycle_violations()
        assert len(violations) >= 2
        violation_ids = [v for v in violations if "bad-state" in v or "unknown-state" in v]
        assert len(violation_ids) >= 2


class TestPhase7ConfidenceFlags:
    """Phase 7 — confidence issues detection."""

    def test_confidence_flags_empty(self, auditor):
        """Empty validator → no flags."""
        assert auditor.check_confidence_issues() == []

    def test_confidence_flags_all_high(self):
        """All high confidence → no flags."""
        v = Validator()
        v.register("a", confidence=0.9)
        v.register("b", confidence=0.75)
        a = MemoryAuditor(validator=v)
        assert a.check_confidence_issues() == []

    def test_confidence_flags_low_items(self, populated_auditor):
        """Create items with low confidence → auditor flags them."""
        flags = populated_auditor.check_confidence_issues()
        # zero-conf (0.0) and low-conf (0.25) both < 0.3
        flagged_ids = [f.split("'")[1] for f in flags]
        assert "zero-conf" in flagged_ids
        assert "low-conf" in flagged_ids
        assert "high-conf" not in flagged_ids


class TestPhase7DriftDetection:
    """Phase 7 — SQL/vector and SQL/graph drift."""

    def test_drift_no_providers(self, auditor):
        """No SQLite/Qdrant → unavailable warning."""
        warnings, stats = auditor.check_sql_vector_drift()
        assert len(warnings) >= 1
        assert "unavailable" in warnings[0]
        assert stats["sql_facts"] is None
        assert stats["qdrant_points"] is None

    def test_drift_no_graph(self, auditor):
        """No graph → unavailable warning."""
        warnings, stats = auditor.check_sql_graph_drift()
        assert len(warnings) >= 1
        assert "unavailable" in warnings[0]
        assert stats["sql_facts"] is None
        assert stats["graph_nodes"] is None

    def test_drift_no_mismatch(self):
        """Equal counts → no drift."""
        v = Validator()
        v.register("a", confidence=0.9)
        v.register("b", confidence=0.8)

        # Mock SQLite with count_facts
        class MockSQLite:
            def count_facts(self) -> int:
                return 2

        # Mock Qdrant with count_points
        class MockQdrant:
            def count_points(self) -> int:
                return 2

        a = MemoryAuditor(validator=v, sqlite=MockSQLite(), qdrant=MockQdrant())
        warnings, stats = a.check_sql_vector_drift()
        assert warnings == []
        assert stats["drift_pct"] == 0.0

    def test_drift_mismatch_detected(self):
        """Mismatched counts → drift warning."""
        v = Validator()
        v.register("a", confidence=0.9)
        v.register("b", confidence=0.8)
        v.register("c", confidence=0.7)

        class MockSQLite:
            def count_facts(self) -> int:
                return 3

        class MockQdrant:
            def count_points(self) -> int:
                return 1

        a = MemoryAuditor(validator=v, sqlite=MockSQLite(), qdrant=MockQdrant())
        warnings, stats = a.check_sql_vector_drift()
        assert len(warnings) >= 1
        assert "drift" in warnings[0].lower()
        assert stats["drift_pct"] > 0

    def test_drift_graph_mismatch(self):
        """SQL/graph count mismatch → drift warning."""
        v = Validator()
        v.register("a", confidence=0.9)

        from memory_server.providers.graph_provider import SimpleGraph
        g = SimpleGraph()
        g.add_node("n1", "entity", "Node1")
        g.add_node("n2", "entity", "Node2")
        g.add_node("n3", "entity", "Node3")

        class MockSQLite:
            def count_facts(self) -> int:
                return 1

        a = MemoryAuditor(validator=v, sqlite=MockSQLite(), graph=g)
        warnings, stats = a.check_sql_graph_drift()
        assert len(warnings) >= 1
        assert "drift" in warnings[0].lower()
        assert stats["drift_pct"] > 0

    def test_drift_graph_matches(self):
        """SQL/graph counts match → no drift."""
        v = Validator()
        v.register("a", confidence=0.9)
        v.register("b", confidence=0.8)

        from memory_server.providers.graph_provider import SimpleGraph
        g = SimpleGraph()
        g.add_node("n1", "entity", "Node1")
        g.add_node("n2", "entity", "Node2")

        class MockSQLite:
            def count_facts(self) -> int:
                return 2

        a = MemoryAuditor(validator=v, sqlite=MockSQLite(), graph=g)
        warnings, stats = a.check_sql_graph_drift()
        assert warnings == []
        assert stats["drift_pct"] == 0.0


class TestPhase7FullReport:
    """Phase 7 — full audit report with all sections."""

    def test_full_report_all_sections_present(self, auditor_with_receipts):
        """Verify all sections present in audit_report()."""
        report = auditor_with_receipts.audit_report("full")
        # Core keys
        assert "audit_type" in report
        assert report["audit_type"] == "full"
        assert "warnings" in report
        assert "errors" in report
        assert "stats" in report

        # Stats structure
        stats = report["stats"]
        assert "total_facts" in stats
        assert "total_decisions" in stats
        assert "total_skills" in stats
        assert "total_receipts" in stats
        assert "total_graph_nodes" in stats
        assert "total_qdrant_points" in stats
        assert "sql_vector_drift" in stats
        assert "sql_graph_drift" in stats

        # Drift stats structure
        svd = stats["sql_vector_drift"]
        assert "sql_facts" in svd
        assert "qdrant_points" in svd
        assert "drift_pct" in svd

        sgd = stats["sql_graph_drift"]
        assert "sql_facts" in sgd
        assert "graph_nodes" in sgd
        assert "drift_pct" in sgd

    def test_full_report_counts(self, auditor_with_receipts):
        """Full report stats reflect actual counts."""
        report = auditor_with_receipts.audit_report("full")
        stats = report["stats"]
        assert stats["total_facts"] == 3       # fact-a, fact-b, fact-c
        assert stats["total_receipts"] == 2    # fact-a, fact-b
        assert stats["total_graph_nodes"] == 0  # no graph
        assert stats["total_qdrant_points"] == 0  # no qdrant

    def test_full_report_with_mocks(self):
        """Full report with mocks includes drift stats."""
        v = Validator()
        v.register("a", confidence=0.9)

        from memory_server.providers.graph_provider import SimpleGraph
        g = SimpleGraph()
        g.add_node("n1", "entity", "N1")
        g.add_node("n2", "entity", "N2")

        class MockSQLite:
            def count_facts(self) -> int:
                return 1

        class MockQdrant:
            def count_points(self) -> int:
                return 3

        a = MemoryAuditor(
            validator=v,
            sqlite=MockSQLite(),
            qdrant=MockQdrant(),
            graph=g,
            receipt_ids={"a"},
        )
        report = a.audit_report("full")
        stats = report["stats"]

        assert stats["total_facts"] == 1
        assert stats["total_receipts"] == 1
        assert stats["total_graph_nodes"] == 2
        assert stats["total_qdrant_points"] == 3
        assert stats["sql_vector_drift"]["drift_pct"] > 0
        assert stats["sql_graph_drift"]["drift_pct"] > 0
        assert len(report["warnings"]) >= 1
