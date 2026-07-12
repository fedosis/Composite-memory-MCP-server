"""Receipt repository — CRUD operations for memory receipts."""

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.models import MemoryReceipt
from storage.models.receipt import MemoryReceiptORM


class ReceiptRepository:
    """Repository for receipt CRUD operations."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(self, receipt: MemoryReceipt) -> MemoryReceipt:
        orm = MemoryReceiptORM.from_pydantic(receipt)
        self._session.add(orm)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()

    async def get(self, receipt_id: str) -> Optional[MemoryReceipt]:
        result = await self._session.get(MemoryReceiptORM, receipt_id)
        return result.to_pydantic() if result else None

    async def search(
        self,
        memory_type: Optional[str] = None,
        source: Optional[str] = None,
        limit: int = 50,
    ) -> list[MemoryReceipt]:
        stmt = select(MemoryReceiptORM)
        if memory_type is not None:
            stmt = stmt.where(MemoryReceiptORM.memory_type == memory_type)
        if source is not None:
            stmt = stmt.where(MemoryReceiptORM.source == source)
        stmt = stmt.limit(limit).order_by(MemoryReceiptORM.timestamp.desc())
        result = await self._session.execute(stmt)
        return [row.to_pydantic() for row in result.scalars().all()]
