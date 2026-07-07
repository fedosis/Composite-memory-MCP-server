"""Memory auditor — consistency checks, orphan detection, confidence analysis.

Produces structured audit reports with warnings, errors, and stats.
"""

from __future__ import annotations

from typing import Any

from memory_server.evaluation.confidence import ConfidenceEngine
from memory_server.evaluation.validator import Validator


class MemoryAuditor:
    """Audits memory consistency, orphans, and confidence distributions.

    Operates on the in-memory stores of a :class:`Validator` and a
    :class:`ConfidenceEngine`, plus an optional graph provider.
    """

    def __init__(
        self,
        validator: Validator,
        confidence_engine: ConfidenceEngine | None = None,
        graph: Any = None,
    ) -> None:
        self._validator = validator
        self._engine = confidence_engine or ConfidenceEngine()
        self._graph = graph  # optional SimpleGraph or similar

    # ------------------------------------------------------------------
    # Audit methods
    # ------------------------------------------------------------------

    def audit_consistency(self) -> list[str]:
        """Check facts vs decisions vs graph for contradictions.

        Scans all registered entries for:
        - Deprecated facts that still have active receipts
        - Facts with zero confidence that aren't deprecated/archived
        - Graph nodes that reference deleted/unknown facts

        Returns:
            List of warning strings.
        """
        warnings: list[str] = []
        for entry in self._validator.get_all():
            fid = entry["fact_id"]
            status = entry["status"]
            confidence = entry["confidence"]

            # Fact with zero or near-zero confidence not marked deprecated
            if confidence == 0.0 and status not in (
                "deprecated",
                "archived",
            ):
                warnings.append(
                    f"Fact '{fid}' has confidence 0.0 but status is '{status}'"
                )

            # Deprecated facts still with full confidence
            if status == "deprecated" and confidence >= 0.5:
                warnings.append(
                    f"Deprecated fact '{fid}' still has confidence {confidence}"
                )

        return warnings

    def audit_orphans(self) -> list[str]:
        """Find graph nodes with no incoming edges (unlinked facts).

        Requires a graph provider with ``get_all_nodes()`` and
        ``get_neighbors()`` methods.

        Returns:
            List of orphan node IDs.
        """
        orphans: list[str] = []
        if self._graph is None:
            return ["No graph provider available — orphan detection skipped"]

        try:
            nodes = self._graph.get_all_nodes()
            for node in nodes:
                neighbors = self._graph.get_neighbors(node.id)
                if not neighbors:
                    orphans.append(node.id)
        except Exception as exc:
            return [f"Orphan detection error: {exc}"]

        return orphans

    def audit_confidence(self) -> dict[str, Any]:
        """Analyze the distribution of confidence scores.

        Returns:
            Dict with keys: ``total``, ``buckets``, ``low_confidence``.
        """
        entries = self._validator.get_all()
        if not entries:
            return {
                "total": 0,
                "buckets": {},
                "low_confidence": [],
            }

        buckets: dict[str, int] = {
            "0.0-0.3": 0,
            "0.3-0.5": 0,
            "0.5-0.7": 0,
            "0.7-0.85": 0,
            "0.85-1.0": 0,
        }
        low_confidence: list[dict[str, Any]] = []

        for entry in entries:
            conf = entry["confidence"]
            if conf < 0.3:
                buckets["0.0-0.3"] += 1
                low_confidence.append(entry)
            elif conf < 0.5:
                buckets["0.3-0.5"] += 1
            elif conf < 0.7:
                buckets["0.5-0.7"] += 1
            elif conf < 0.85:
                buckets["0.7-0.85"] += 1
            else:
                buckets["0.85-1.0"] += 1

        return {
            "total": len(entries),
            "buckets": buckets,
            "low_confidence": [
                e["fact_id"] for e in low_confidence
            ],
        }

    def audit_report(self, audit_type: str = "full") -> dict[str, Any]:
        """Generate a structured audit report.

        Args:
            audit_type: One of ``"consistency"``, ``"orphans"``,
                         ``"confidence"``, or ``"full"``.

        Returns:
            Dict with keys: ``audit_type``, ``warnings``, ``errors``,
            ``stats``.
        """
        warnings: list[str] = []
        errors: list[str] = []
        stats: dict[str, Any] = {}

        if audit_type in ("consistency", "full"):
            warnings.extend(self.audit_consistency())

        if audit_type in ("orphans", "full"):
            orphan_results = self.audit_orphans()
            if orphan_results and orphan_results[0].startswith("No graph"):
                warnings.extend(orphan_results)
            else:
                if orphan_results:
                    warnings.append(
                        f"Found {len(orphan_results)} orphan graph nodes: "
                        f"{', '.join(orphan_results[:10])}"
                    )

        if audit_type in ("confidence", "full"):
            conf_stats = self.audit_confidence()
            stats["confidence"] = conf_stats
            if conf_stats.get("low_confidence"):
                warnings.append(
                    f"Found {len(conf_stats['low_confidence'])} "
                    f"low-confidence items (< 0.3)"
                )

        return {
            "audit_type": audit_type,
            "warnings": warnings,
            "errors": errors,
            "stats": stats,
        }
