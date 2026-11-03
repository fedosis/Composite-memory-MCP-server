"""Borrowed-session revision CAS and fail-closed occupied-schema validation."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import inspect, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateTable

from storage.models.relation import ClaimRelationORM
from storage.models.relation_revision import RelationRevisionORM as R

MAX_REVISION = 9223372036854775807


class RelationRevisionStorageError(SQLAlchemyError):
    def __init__(self):
        super().__init__("Relation revision storage invariant failed")


class RelationRevisionConflict(Exception):  # noqa: N818 - frozen stage-local ABI
    def __init__(self, expected, actual, id):
        self.expected, self.actual, self.id = expected, actual, id
        super().__init__("Relation revision conflict")


def instant(value):
    try:
        dt = datetime.fromisoformat(value) if type(value) is str else value
        return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    except (ValueError, TypeError, AttributeError):
        raise RelationRevisionStorageError() from None


def positive(value):
    if type(value) is not int or not 1 <= value <= MAX_REVISION:
        raise RelationRevisionStorageError()
    return value


@dataclass(frozen=True)
class RelationEventContext:
    event_id: str
    changed_at: datetime
    request_id: str | None = None
    actor: str | None = None
    action_receipt_id: str | None = None

    def validate(self):
        try:
            if type(self.event_id) is not str or str(UUID(self.event_id)) != self.event_id:
                raise ValueError
            trio = (self.request_id, self.actor, self.action_receipt_id)
            if not (all(v is None for v in trio) or all(type(v) is str and v for v in trio)):
                raise ValueError
            if self.action_receipt_id is not None and (
                str(UUID(self.action_receipt_id)) != self.action_receipt_id or self.action_receipt_id == self.event_id
            ):
                raise ValueError
            instant(self.changed_at)
        except (ValueError, TypeError, AttributeError):
            raise RelationRevisionStorageError() from None


@dataclass(frozen=True)
class RevisionState:
    key: tuple[str, str, str]
    revision: int
    deleted: bool
    relation_created_at: datetime
    changed_at: datetime
    event_id: str
    request_id: str | None
    actor: str | None
    action_receipt_id: str | None


@dataclass(frozen=True)
class RevisionPreState:
    canonical_present: bool
    canonical_created_at: datetime | None
    latest: RevisionState | None

    @property
    def revision(self):
        return self.latest.revision if self.latest else (1 if self.canonical_present else None)

    def next_revision(self):
        current = self.revision
        return positive(current + 1) if current is not None else 1


def _state(row):
    positive(row.revision)
    if type(row.deleted) is not int or row.deleted not in (0, 1):
        raise RelationRevisionStorageError()
    return RevisionState(
        (row.source_id, row.target_id, row.relation_type),
        row.revision,
        bool(row.deleted),
        instant(row.relation_created_at),
        instant(row.changed_at),
        row.event_id,
        row.request_id,
        row.actor,
        row.action_receipt_id,
    )


class RelationRevisionRepository:
    def __init__(self, session):
        self.session = session

    async def latest(self, key):
        row = (
            await self.session.execute(
                select(R)
                .where(R.source_id == key[0], R.target_id == key[1], R.relation_type == key[2])
                .order_by(R.revision.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        return _state(row) if row else None

    async def state(self, key, canonical_created_at):
        latest = await self.latest(key)
        created = instant(canonical_created_at) if canonical_created_at is not None else None
        if latest and (
            (created is None) != latest.deleted or (created is not None and created != latest.relation_created_at)
        ):
            raise RelationRevisionStorageError()
        return RevisionPreState(created is not None, created, latest)

    async def verify_before(self, key, before):
        canonical = await self.session.get(ClaimRelationORM, key, populate_existing=True)
        observed = await self.state(key, canonical.created_at if canonical else None)
        if observed != before:
            raise RelationRevisionConflict(before.revision, observed.revision, key)
        before.next_revision()

    async def append(self, key, *, before, deleted, relation_created_at, event):
        event.validate()
        revision = before.next_revision()
        created = instant(relation_created_at)
        if type(deleted) is not bool or deleted != before.canonical_present:
            raise RelationRevisionStorageError()
        latest = await self.latest(key)
        if latest != before.latest:
            raise RelationRevisionConflict(before.revision, latest.revision if latest else None, key)
        canonical = await self.session.get(ClaimRelationORM, key, populate_existing=True)
        if (deleted and (canonical is not None or created != before.canonical_created_at)) or (
            not deleted and (canonical is None or instant(canonical.created_at) != created)
        ):
            raise RelationRevisionStorageError()
        row = R(
            source_id=key[0],
            target_id=key[1],
            relation_type=key[2],
            revision=revision,
            deleted=int(deleted),
            relation_created_at=created.replace(tzinfo=None),
            changed_at=instant(event.changed_at).replace(tzinfo=None),
            event_id=event.event_id,
            request_id=event.request_id,
            actor=event.actor,
            action_receipt_id=event.action_receipt_id,
        )
        self.session.add(row)
        await self.session.flush()
        return _state(row)

    async def revisions_for_rows(self, rows, *, maximum):
        if type(maximum) is not int or maximum not in (200, 1000) or len(rows) > maximum:
            raise RelationRevisionStorageError()
        keyed = {(r.source_id, r.target_id, r.relation_type): r for r in rows}
        result, keys = {}, list(keyed)
        for start in range(0, len(keys), 128):
            chunk = keys[start : start + 128]
            params = {f"{axis}{i}": value for i, key in enumerate(chunk) for axis, value in zip("str", key)}
            values = ",".join(f"(:s{i},:t{i},:r{i})" for i in range(len(chunk)))
            query = text(
                "WITH inputs(source_id,target_id,relation_type) AS (VALUES " + values + ") "
                "SELECT i.*,r.revision,r.deleted,r.relation_created_at FROM inputs i "
                "LEFT JOIN relation_revisions r "
                "ON r.source_id=i.source_id AND r.target_id=i.target_id AND r.relation_type=i.relation_type "
                "AND r.revision=(SELECT h.revision FROM relation_revisions h WHERE h.source_id=i.source_id "
                "AND h.target_id=i.target_id AND h.relation_type=i.relation_type ORDER BY h.revision DESC LIMIT 1)"
            )
            for row in (await self.session.execute(query, params)).all():
                key = tuple(row[:3])
                if row.revision is not None and (
                    row.deleted != 0 or instant(row.relation_created_at) != instant(keyed[key].created_at)
                ):
                    raise RelationRevisionStorageError()
                result[key] = positive(row.revision) if row.revision is not None else 1
        return result


def _tokens(sql):
    if type(sql) is not str or len(sql.encode("utf-8")) > 65536:
        raise RelationRevisionStorageError()
    pattern = r"\s+|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|`[^`]*`|\[[^\]]*\]|[A-Za-z_][A-Za-z_0-9]*|[0-9]+|[(),=;]"
    matches = list(re.finditer(pattern, sql))
    if "".join(m.group() for m in matches) != sql:
        raise RelationRevisionStorageError()
    tokens = []
    for match in matches:
        token = match.group()
        if token.isspace():
            continue
        if token[0] in '"`[':
            token = token[1:-1]
        tokens.append(token if token.startswith("'") else token.upper())
    return tokens[:-1] if tokens and tokens[-1] == ";" else tokens


def validate_occupied_relation_revisions(connection):
    fail = RelationRevisionStorageError
    temp = connection.exec_driver_sql(
        ("SELECT 1 FROM sqlite_temp_master WHERE name='relation_revisions' OR tbl_name='relation_revisions' LIMIT 1")
    ).first()
    entry = connection.exec_driver_sql(
        "SELECT type,sql FROM sqlite_master WHERE name='relation_revisions' LIMIT 1"
    ).first()
    if temp:
        raise fail()
    if entry is None:
        return
    if entry.type != "table" or _tokens(entry.sql) != _tokens(
        str(CreateTable(R.__table__).compile(dialect=connection.dialect))
    ):
        raise fail()
    if connection.exec_driver_sql(
        "SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='relation_revisions' LIMIT 1"
    ).first():
        raise fail()
    inspector = inspect(connection)
    columns = inspector.get_columns("relation_revisions")
    expected = list(R.__table__.columns)
    if len(columns) != 11 or any(
        c["name"] != e.name or str(c["type"]) != str(e.type) or c["nullable"] != e.nullable or c["default"] is not None
        for c, e in zip(columns, expected)
    ):
        raise fail()
    xinfo = connection.exec_driver_sql("PRAGMA main.table_xinfo('relation_revisions')").all()
    if len(xinfo) != 11 or any(r[6] != 0 for r in xinfo):
        raise fail()
    if inspector.get_pk_constraint("relation_revisions")["constrained_columns"] != [
        "source_id",
        "target_id",
        "relation_type",
        "revision",
    ]:
        raise fail()
    indices = connection.exec_driver_sql("PRAGMA main.index_list('relation_revisions')").all()
    if len(indices) != 2 or {r[3] for r in indices} != {"pk", "u"}:
        raise fail()
    for index in indices:
        if index[2] != 1 or index[4] != 0 or not re.fullmatch(r"sqlite_autoindex_relation_revisions_[12]", index[1]):
            raise fail()
        shape = connection.exec_driver_sql(f"PRAGMA main.index_xinfo('{index[1]}')").all()
        keys = [r for r in shape if r[5]]
        names = ["event_id"] if index[3] == "u" else ["source_id", "target_id", "relation_type", "revision"]
        if [r[2] for r in keys] != names or any(r[3] != 0 or r[4] != "BINARY" for r in keys):
            raise fail()
