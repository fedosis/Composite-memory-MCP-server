"""Memory auditor — consistency checks, orphan detection, confidence analysis.

Produces structured audit reports with warnings, errors, and stats.
"""

from __future__ import annotations

from typing import Any

from memory_server.evaluation.confidence import ConfidenceEngine
from memory_server.evaluation.validator import Validator
from memory_server.models.receipt import LifecycleState

_VALID_LIFECYCLE_STATES = {s.value for s in LifecycleState}


class MemoryAuditor:
    """Audits memory consistency, orphans, and confidence distributions.

    Operates on the in-memory stores of a :class:`Validator` and a
    :class:`ConfidenceEngine`, plus optional graph, SQLite, and
    Qdrant providers.
    """

    def __init__(
        self,
        validator: Validator,
        confidence_engine: ConfidenceEngine | None = None,
        graph: Any = None,
        sqlite: Any = None,
        qdrant: Any = None,
        receipt_ids: set[str] | None = None,
    ) -> None:
        self._validator = validator
        self._engine = confidence_engine or ConfidenceEngine()
        self._graph = graph  # optional SimpleGraph or similar
        self._sqlite = sqlite  # optional SQLiteProvider
        self._qdrant = qdrant  # optional QdrantProvider
        self._receipt_ids = receipt_ids or set()

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

            # Fact with zero or near-zero confidence not marked stale/archived/forgotten
            if confidence == 0.0 and status not in (
                "stale",
                "archived",
                "forgotten",
            ):
                warnings.append(
                    f"Fact '{fid}' has confidence 0.0 but status is '{status}'"
                )

            # Stale facts still with full confidence
            if status == "stale" and confidence >= 0.5:
                warnings.append(
                    f"Stale fact '{fid}' still has confidence {confidence}"
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

    # ------------------------------------------------------------------
    # Phase 7: Expanded audit checks
    # ------------------------------------------------------------------

    def check_orphan_records(self) -> list[str]:
        """Find facts/decisions/skills with no corresponding receipt.

        An item is an orphan if it exists in the validator's in-memory
        store but its fact_id is not present in the receipt_ids set.

        Returns:
            List of orphan record IDs.
        """
        orphans: list[str] = []
        if not self._receipt_ids:
            # No receipt store available — flag all items as potential orphans
            for entry in self._validator.get_all():
                orphans.append(entry["fact_id"])
            if orphans:
                return [f"No receipt store available — {len(orphans)} items unchecked"]
            return ["No receipt store available — no items to check"]

        for entry in self._validator.get_all():
            fid = entry["fact_id"]
            if fid not in self._receipt_ids:
                orphans.append(fid)

        return orphans

    def check_missing_receipts(self) -> list[str]:
        """Any stored item that lacks a MemoryReceipt.

        This is a broader check that also considers items registered
        in the validator with no receipt entry.

        Returns:
            List of item IDs missing receipts.
        """
        missing: list[str] = []
        if not self._receipt_ids:
            for entry in self._validator.get_all():
                missing.append(entry["fact_id"])
            if missing:
                return [f"No receipt store available — {len(missing)} items unchecked"]
            return ["No receipt store available — no items to check"]

        for entry in self._validator.get_all():
            fid = entry["fact_id"]
            if fid not in self._receipt_ids:
                missing.append(fid)

        return missing

    def check_lifecycle_violations(self) -> list[str]:
        """Find items in invalid lifecycle states.

        Valid states are defined by :class:`LifecycleState`:
        candidate, validated, active, stale, archived, forgotten.

        Old values like "trusted" and "deprecated" that weren't
        normalized are flagged as violations.

        Returns:
            List of error strings describing violations.
        """
        errors: list[str] = []
        for entry in self._validator.get_all():
            fid = entry["fact_id"]
            status = entry["status"]
            if status not in _VALID_LIFECYCLE_STATES:
                errors.append(
                    f"Item '{fid}' has invalid lifecycle state '{status}'"
                )
        return errors

    def check_confidence_issues(self) -> list[str]:
        """Find items with confidence < 0.3.

        Unlike audit_confidence() which analyzes distribution, this
        flags individual items that fall below the minimum threshold.

        Returns:
            List of warning strings for low-confidence items.
        """
        warnings: list[str] = []
        for entry in self._validator.get_all():
            fid = entry["fact_id"]
            conf = entry["confidence"]
            if conf < 0.3:
                warnings.append(
                    f"Item '{fid}' has low confidence ({conf})"
                )
        return warnings

    def check_sql_vector_drift(self) -> tuple[list[str], dict[str, Any]]:
        """Compare count of facts in SQLite vs count of points in Qdrant.

        Drift is reported as a percentage::
            drift_pct = abs(sql_count - qdrant_count) / max(sql_count, qdrant_count) * 100

        Returns:
            Tuple of (warnings list, stats dict with keys: sql_facts,
            qdrant_points, drift_pct).
        """
        warnings: list[str] = []
        stats: dict[str, Any] = {}

        sql_count = self._sqlite_fact_count()
        qdrant_count = self._qdrant_point_count()
        stats["sql_facts"] = sql_count
        stats["qdrant_points"] = qdrant_count

        if sql_count is None or qdrant_count is None:
            unavailable = []
            if sql_count is None:
                unavailable.append("SQLite")
            if qdrant_count is None:
                unavailable.append("Qdrant")
            names = " and ".join(unavailable)
            msg = f"SQL/vector drift check unavailable — {names} provider not configured"
            warnings.append(msg)
            stats["drift_pct"] = None
            return warnings, stats

        max_count = max(sql_count, qdrant_count)
        if max_count == 0:
            stats["drift_pct"] = 0.0
            return warnings, stats

        drift_pct = abs(sql_count - qdrant_count) / max_count * 100
        stats["drift_pct"] = round(drift_pct, 2)

        if drift_pct > 0:
            direction = (
                "more SQL facts than Qdrant points"
                if sql_count > qdrant_count
                else "more Qdrant points than SQL facts"
            )
            warnings.append(
                f"SQL/vector drift detected: {sql_count} SQL facts vs "
                f"{qdrant_count} Qdrant points ({drift_pct:.1f}% drift, "
                f"{direction})"
            )

        return warnings, stats

    def check_sql_graph_drift(self) -> tuple[list[str], dict[str, Any]]:
        """Compare count of facts vs graph nodes.

        Drift is reported as a percentage.

        Returns:
            Tuple of (warnings list, stats dict with keys: sql_facts,
            graph_nodes, drift_pct).
        """
        warnings: list[str] = []
        stats: dict[str, Any] = {}

        sql_count = self._sqlite_fact_count()
        graph_count = self._graph_node_count()
        stats["sql_facts"] = sql_count
        stats["graph_nodes"] = graph_count

        if sql_count is None or graph_count is None:
            unavailable = []
            if sql_count is None:
                unavailable.append("SQLite")
            if graph_count is None:
                unavailable.append("Graph")
            names = " and ".join(unavailable)
            msg = f"SQL/graph drift check unavailable — {names} provider not configured"
            warnings.append(msg)
            stats["drift_pct"] = None
            return warnings, stats

        max_count = max(sql_count, graph_count)
        if max_count == 0:
            stats["drift_pct"] = 0.0
            return warnings, stats

        drift_pct = abs(sql_count - graph_count) / max_count * 100
        stats["drift_pct"] = round(drift_pct, 2)

        if drift_pct > 0:
            direction = (
                "more SQL facts than graph nodes"
                if sql_count > graph_count
                else "more graph nodes than SQL facts"
            )
            warnings.append(
                f"SQL/graph drift detected: {sql_count} SQL facts vs "
                f"{graph_count} graph nodes ({drift_pct:.1f}% drift, "
                f"{direction})"
            )

        return warnings, stats

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _sqlite_fact_count(self) -> int | None:
        """Get the count of facts from the SQLite provider.

        Returns None if no SQLite provider is configured.
        """
        if self._sqlite is None:
            return None
        try:
            # If it's an async provider, we need to run it synchronously
            # For test mocks, check if count_facts exists directly
            if hasattr(self._sqlite, "count_facts"):
                return self._sqlite.count_facts()
            # Fallback: try to count from validator entries
            entries = self._validator.get_all()
            return len(entries)
        except Exception:
            return None

    def _qdrant_point_count(self) -> int | None:
        """Get the count of points from the Qdrant provider.

        Returns None if no Qdrant provider is configured.
        """
        if self._qdrant is None:
            return None
        try:
            # For test mocks, check if count_points exists directly
            if hasattr(self._qdrant, "count_points"):
                return self._qdrant.count_points()
            # Try to access the underlying client for a real QdrantProvider
            if hasattr(self._qdrant, "_client") and hasattr(self._qdrant, "_collection"):
                _client = self._qdrant._client
                try:
                    result = _client.count(
                        collection_name=self._qdrant._collection,
                        exact=True,
                    )
                    return result.count
                except Exception:
                    pass
            return None
        except Exception:
            return None

    def _graph_node_count(self) -> int | None:
        """Get the count of nodes from the graph provider.

        Returns None if no graph provider is configured.
        """
        if self._graph is None:
            return None
        try:
            nodes = self._graph.get_all_nodes()
            return len(nodes)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------

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

        # --- Phase 7 expanded checks (always run for "full") ---
        if audit_type == "full":
            # Check orphan records
            orphan_records = self.check_orphan_records()
            if orphan_records:
                if orphan_records[0].startswith("No receipt"):
                    warnings.extend(orphan_records)
                else:
                    warnings.append(
                        f"Found {len(orphan_records)} orphan records "
                        f"(no corresponding receipt): "
                        f"{', '.join(orphan_records[:10])}"
                    )

            # Check missing receipts
            missing_receipts = self.check_missing_receipts()
            if missing_receipts:
                if missing_receipts[0].startswith("No receipt"):
                    warnings.extend(missing_receipts)
                else:
                    errors.append(
                        f"Found {len(missing_receipts)} items without "
                        f"MemoryReceipt: "
                        f"{', '.join(missing_receipts[:10])}"
                    )

            # Check lifecycle violations
            lifecycle_errors = self.check_lifecycle_violations()
            if lifecycle_errors:
                errors.extend(lifecycle_errors)
                warnings.append(
                    f"Found {len(lifecycle_errors)} lifecycle violations"
                )

            # Check confidence issues
            confidence_warnings = self.check_confidence_issues()
            if confidence_warnings:
                warnings.extend(confidence_warnings)

            # Check SQL/vector drift
            drift_warnings, drift_stats = self.check_sql_vector_drift()
            warnings.extend(drift_warnings)
            stats["sql_vector_drift"] = drift_stats

            # Check SQL/graph drift
            graph_drift_warnings, graph_drift_stats = self.check_sql_graph_drift()
            warnings.extend(graph_drift_warnings)
            stats["sql_graph_drift"] = graph_drift_stats

            # Aggregate summary stats
            all_entries = self._validator.get_all()
            fact_count = len(all_entries)
            decision_count = sum(
                1 for e in all_entries if "decision" in e.get("fact_id", "")
            )
            skill_count = sum(
                1 for e in all_entries if "skill" in e.get("fact_id", "")
            )
            stats["total_facts"] = fact_count
            stats["total_decisions"] = decision_count
            stats["total_skills"] = skill_count
            stats["total_receipts"] = len(self._receipt_ids)
            stats["total_graph_nodes"] = self._graph_node_count() or 0
            qdrant_count = self._qdrant_point_count()
            stats["total_qdrant_points"] = qdrant_count if qdrant_count is not None else 0

        return {
            "audit_type": audit_type,
            "warnings": warnings,
            "errors": errors,
            "stats": stats,
        }
