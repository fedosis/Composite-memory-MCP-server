"""Application boundary for persisted memory audits."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Mapping

from memory_server.application.contracts import AuditCommand
from memory_server.evaluation.auditor import MemoryAuditor


class AuditService:
    """Run the existing auditor over caller-owned dependencies and state."""

    def __init__(
        self,
        *,
        validator: Any,
        confidence_engine: Any,
        get_graph: Callable[[], Awaitable[Any]] | None,
        sqlite: Any | None,
        vector_provider: Any | None,
        persisted_state: Mapping[str, Any],
    ) -> None:
        self._validator = validator
        self._confidence_engine = confidence_engine
        self._get_graph = get_graph
        self._sqlite = sqlite
        self._vector_provider = vector_provider
        self._sqlite_counts = dict(persisted_state["sqlite_counts"])
        self._receipt_ids = set(persisted_state["receipt_ids"])
        records = persisted_state["records"]
        self._records = list(records) if records is not None else None
        self._vector_point_count = persisted_state["vector_point_count"]

    async def audit(self, command: AuditCommand) -> dict[str, Any]:
        graph = await self._get_graph() if self._get_graph is not None else None
        auditor = MemoryAuditor(
            validator=self._validator,
            confidence_engine=self._confidence_engine,
            graph=graph,
            sqlite=self._sqlite,
            qdrant=self._vector_provider,
            receipt_ids=set(self._receipt_ids),
            sqlite_counts=dict(self._sqlite_counts),
            vector_point_count=self._vector_point_count,
            persisted_records=list(self._records) if self._records is not None else None,
        )
        return auditor.audit_report(audit_type=command.audit_type)
