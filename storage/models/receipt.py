"""MemoryReceipt ORM model — canonical SQL storage for receipts."""

from datetime import datetime

from sqlalchemy import DateTime, Float, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from memory_server.models import MemoryReceipt, VerificationStatus
from storage.base import Base, utcnow


class MemoryReceiptORM(Base):
    """SQLAlchemy ORM model for MemoryReceipts — canonical fields."""

    __tablename__ = "receipts"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    memory_type: Mapped[str] = mapped_column(String, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_by: Mapped[str] = mapped_column(String, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    verification_status: Mapped[str] = mapped_column(String, default="unverified")
    history: Mapped[str] = mapped_column(Text, default="[]")
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    lifecycle_state: Mapped[str] = mapped_column(String, default="active")
    version: Mapped[str] = mapped_column(String, default="0.1.0")

    def to_pydantic(self) -> MemoryReceipt:
        import json

        return MemoryReceipt(
            id=self.id,
            memory_type=self.memory_type,
            source=self.source,
            created_by=self.created_by,
            timestamp=self.timestamp,
            confidence=self.confidence,
            verification_status=VerificationStatus(self.verification_status),
            history=json.loads(self.history),
            updated_at=self.updated_at,
            lifecycle_state=self.lifecycle_state,
            version=self.version,
        )

    @classmethod
    def from_pydantic(cls, receipt: MemoryReceipt) -> "MemoryReceiptORM":
        import json

        return cls(
            id=receipt.id,
            memory_type=receipt.memory_type,
            source=receipt.source,
            created_by=receipt.created_by,
            timestamp=receipt.timestamp,
            confidence=receipt.confidence,
            verification_status=receipt.verification_status.value,
            history=json.dumps(receipt.history),
            updated_at=receipt.updated_at,
            lifecycle_state=receipt.lifecycle_state,
            version=receipt.version,
        )
