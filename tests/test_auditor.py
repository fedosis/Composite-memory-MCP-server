"""Tests for MemoryAuditor (Card 025)."""

import pytest

from memory_server.evaluation.auditor import MemoryAuditor
from memory_server.evaluation.validator import Validator


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

    def test_consistency_deprecated_high_confidence(self, auditor):
        """Deprecated fact with high confidence triggers warning."""
        auditor._validator.register("d1", confidence=0.8)
        auditor._validator.deprecate("d1")
        warnings = auditor.audit_consistency()
        dep_warnings = [w for w in warnings if "d1" in w]
        assert len(dep_warnings) >= 1
        assert "Deprecated" in dep_warnings[0]

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
        # No incoming edges to NodeA (except from itself? No, edges are directed)
        # a has outgoing to b, so a has neighbors. b has no outgoing but has incoming from a.
        # get_neighbors checks both directions, so b also has neighbors
        # Let me check...
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
        # a has neighbor b, b has neighbor a (get_neighbors checks both directions)
        # So both have neighbors → no orphans
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
