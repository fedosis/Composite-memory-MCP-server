"""Session-owned journal access; never commits or logs SQL/parameters."""

import sqlite3
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.transports.http.errors import HttpError
from storage.models.http_request import HttpRequestJournalORM as Journal


@dataclass(frozen=True)
class JournalRow:
    id: int
    subject: str
    profile_id: str
    operation: str
    idem_key: str
    digest: str
    state: str
    response_snapshot: str
    result_refs: str
    request_id: str
    audit_ticket_id: str | None
    pending_publication: bool
    created_at: datetime
    expires_at: datetime


class HttpRequestRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def find(self, subject: str, profile_id: str, operation: str, idem_key: str) -> JournalRow | None:
        row = (
            await self._session.execute(
                select(Journal).where(
                    Journal.subject == subject,
                    Journal.profile_id == profile_id,
                    Journal.operation == operation,
                    Journal.idem_key == idem_key,
                )
            )
        ).scalar_one_or_none()
        if row is None:
            return None
        return JournalRow(
            id=row.id,
            subject=row.subject,
            profile_id=row.profile_id,
            operation=row.operation,
            idem_key=row.idem_key,
            digest=row.digest,
            state=row.state,
            response_snapshot=row.response_snapshot,
            result_refs=row.result_refs,
            request_id=row.request_id,
            audit_ticket_id=row.audit_ticket_id,
            pending_publication=bool(row.pending_publication),
            created_at=row.created_at,
            expires_at=row.expires_at,
        )

    async def pending(self, profile_id: str) -> bool:
        result = await self._session.execute(
            select(Journal.id)
            .where(
                Journal.profile_id == profile_id,
                Journal.pending_publication == 1,
            )
            .limit(1)
        )
        return result.first() is not None

    async def add(
        self,
        *,
        subject: str,
        profile_id: str,
        operation: str,
        idem_key: str,
        digest: str,
        response_snapshot: str,
        result_refs: str,
        request_id: str,
        audit_ticket_id: str | None,
        pending_publication: int,
        created_at: datetime,
        expires_at: datetime,
    ) -> Journal:
        row = Journal(
            subject=subject,
            profile_id=profile_id,
            operation=operation,
            idem_key=idem_key,
            digest=digest,
            response_snapshot=response_snapshot,
            result_refs=result_refs,
            request_id=request_id,
            audit_ticket_id=audit_ticket_id,
            pending_publication=pending_publication,
            created_at=created_at,
            expires_at=expires_at,
        )
        self._session.add(row)
        return row

    async def purge_expired(self, now: datetime, *, limit: int) -> int:
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("invalid purge limit")
        expired = (
            select(Journal.id)
            .where(
                Journal.expires_at <= now,
                Journal.pending_publication == 0,
            )
            .order_by(Journal.expires_at, Journal.id)
            .limit(limit)
        )
        try:
            result = await self._session.execute(
                delete(Journal).where(Journal.id.in_(expired)), execution_options={"synchronize_session": False}
            )
        except sqlite3.Error:
            raise HttpError("E_STORAGE") from None
        return result.rowcount

    async def delete(self, row: JournalRow) -> None:
        if row.pending_publication:
            raise ValueError("pending journal row")
        await self._session.execute(delete(Journal).where(Journal.id == row.id))

    async def mark_published(self, request_id: str) -> bool:
        result = await self._session.execute(
            update(Journal)
            .where(
                Journal.request_id == request_id,
                Journal.pending_publication == 1,
            )
            .values(pending_publication=0)
        )
        return result.rowcount == 1
