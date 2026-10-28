"""Digest-only bearer custody in S02's private control.sqlite, no profile I/O.

Verifier = SHA-256(ASCII base64url secret || DIGEST_DOMAIN); there is no digest
key. The separate 0600 token-cursor.key signs C7 cursors only. Additive token
schema lives here: S02's profile journal/schema/version remain untouched.
Synchronous short SQLite transactions contain no await and serialize auth with
revoke. Callers own the UTC clock and must reauthenticate each request/SSE item.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import stat
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import get_args
from uuid import UUID, uuid4

from pydantic import ValidationError

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import Principal, RequestContext

from .policy import TokenError, principal_from_token, require_permission, utc_now
from .store import ControlError, ControlStore

DIGEST_DOMAIN = b"\x00cmms.bearer.v1"
_BEARER = re.compile(r"cmms_([0-9a-f]{32})\.([A-Za-z0-9_-]{42}[AEIMQUYcgkosw048])", re.ASCII)
_PROFILE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", re.ASCII)
_META = "id,subject,profiles,permissions,created_at,expires_at,last_used_at,revoked_at,generation"


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _stamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _unb64(value: str) -> bytes:
    return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)


def _validate(model, value):
    try:
        # Revalidate even DTO instances: S01's request DTOs are mutable.
        return model.model_validate(value.model_dump() if isinstance(value, model) else value)
    except (ValidationError, ValueError, TypeError):
        raise TokenError("E_VALIDATION") from None


class TokenService:
    """C1 authentication plus C3 token control; bootstrap is local-only/private.

    No authentication cache. token_journal has a unique subject/op/key and
    immutable metadata snapshot; reservation, row and journal commit together.
    C6's 24h window permits a new effect only after expiry of the original key.
    """

    def __init__(self, store: ControlStore, *, last_used_interval: timedelta = timedelta(seconds=60)) -> None:
        if not isinstance(last_used_interval, timedelta) or last_used_interval < timedelta(seconds=1):
            raise TokenError("E_VALIDATION")
        self._store = store
        self._interval = last_used_interval
        self._checkpoint = lambda phase: None
        with self._atomic():
            self._db.execute("""CREATE TABLE IF NOT EXISTS tokens (
                id TEXT PRIMARY KEY, digest BLOB NOT NULL CHECK(length(digest)=32),
                subject TEXT NOT NULL, profiles TEXT NOT NULL, permissions TEXT NOT NULL,
                created_at TEXT NOT NULL, expires_at TEXT NOT NULL, last_used_at TEXT,
                revoked_at TEXT, generation INTEGER NOT NULL CHECK(generation>=1))""")
            self._db.execute("""CREATE INDEX IF NOT EXISTS token_order ON tokens(created_at,id)""")
            self._db.execute("""CREATE TABLE IF NOT EXISTS token_journal (
                subject TEXT NOT NULL, operation TEXT NOT NULL, key TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL, token_id TEXT NOT NULL REFERENCES tokens(id),
                snapshot TEXT NOT NULL, created_at TEXT NOT NULL, expires_at TEXT NOT NULL,
                PRIMARY KEY(subject,operation,key))""")
            self._cursor_key = self._load_cursor_key()

    @property
    def _db(self) -> sqlite3.Connection:
        if self._store._connection is None:
            raise TokenError("E_STORAGE")
        return self._store._connection

    @contextmanager
    def _atomic(self):
        try:
            with self._store._transaction():
                yield
        except (ControlError, sqlite3.Error, OSError):
            raise TokenError("E_STORAGE") from None

    def _load_cursor_key(self) -> bytes:
        directory = self._store._directory_fd
        flags = os.O_NOFOLLOW | os.O_CLOEXEC
        try:
            fd = os.open("token-cursor.key", os.O_WRONLY | os.O_CREAT | os.O_EXCL | flags, 0o600, dir_fd=directory)
        except FileExistsError:
            pass
        else:
            try:
                key = secrets.token_bytes(32)
                if os.write(fd, key) != 32:
                    raise TokenError("E_STORAGE")
                os.fsync(fd)
                os.fsync(directory)
            finally:
                os.close(fd)
        fd = os.open("token-cursor.key", os.O_RDONLY | flags, dir_fd=directory)
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise TokenError("E_STORAGE")
            key = os.read(fd, 33)
            if len(key) != 32:
                raise TokenError("E_STORAGE")
            return key
        finally:
            os.close(fd)

    @staticmethod
    def _info(row: sqlite3.Row) -> d.TokenInfo:
        try:
            value = {key: row[key] for key in _META.split(",")}
            for key in ("profiles", "permissions"):
                value[key] = json.loads(value[key])
            for key in ("created_at", "expires_at", "last_used_at", "revoked_at"):
                value[key] = datetime.fromisoformat(value[key]) if value[key] is not None else None
            return d.TokenInfo(**value)
        except (ValueError, TypeError, KeyError):
            raise TokenError("E_STORAGE") from None

    async def authenticate(self, bearer: str, *, now: datetime) -> Principal:
        utc_now(now)
        match = _BEARER.fullmatch(bearer) if isinstance(bearer, str) else None
        if match is None:
            raise TokenError("E_AUTH_REQUIRED")
        token_id, secret = match.groups()
        try:
            decoded = _unb64(secret)
        except ValueError:
            raise TokenError("E_AUTH_REQUIRED") from None
        if len(decoded) != 32:
            raise TokenError("E_AUTH_REQUIRED")
        digest = hashlib.sha256(secret.encode("ascii") + DIGEST_DOMAIN).digest()
        with self._atomic():
            row = self._db.execute("SELECT * FROM tokens WHERE id=?", (token_id,)).fetchone()
            matches = hmac.compare_digest(digest, row["digest"] if row is not None else bytes(32))
            if row is None or not matches:
                raise TokenError("E_AUTH_REQUIRED")
            token = self._info(row)
            if token.revoked_at is not None or now >= token.expires_at or now < token.created_at:
                raise TokenError("E_AUTH_REQUIRED")
            if token.last_used_at is None or now - token.last_used_at >= self._interval:
                self._db.execute(
                    "UPDATE tokens SET last_used_at=? WHERE id=? AND generation=? AND revoked_at IS NULL",
                    (_stamp(now), token_id, token.generation),
                )
            principal = principal_from_token(token)
        return principal

    def verify_principal(self, principal: Principal, *, now: datetime) -> None:
        """Revalidate a cached Principal against the store.

        The transport boundary (S06) MUST call this for every request that reuses a
        cached Principal: revocation bumps the credential generation, and C2.1 allows
        caching only with generation invalidation. S03 exposes the check; the caching
        layer lives in S06.
        """
        utc_now(now)
        if now >= principal.expires_at:
            raise TokenError("E_AUTH_REQUIRED")
        row = self._db.execute(
            "SELECT generation, revoked_at FROM tokens WHERE id=?", (principal.token_id,)
        ).fetchone()
        if row is None or row["revoked_at"] is not None or row["generation"] != principal.credential_generation:
            raise TokenError("E_AUTH_REQUIRED")

    def _authority(self, ctx: RequestContext, now: datetime) -> None:
        utc_now(now)
        if now >= ctx.principal.expires_at:
            raise TokenError("E_AUTH_REQUIRED")
        require_permission(ctx.principal, "token.manage")
        if ctx.profile_id is not None:
            raise TokenError("E_VALIDATION")

    @staticmethod
    def _key(ctx: RequestContext) -> str:
        if ctx.idempotency_key is None:
            raise TokenError("E_IDEMPOTENCY_REQUIRED")
        if not isinstance(ctx.idempotency_key, UUID):
            raise TokenError("E_VALIDATION")
        return str(ctx.idempotency_key)

    def _replay(self, ctx: RequestContext, op: str, fingerprint: str, now: datetime) -> d.TokenInfo | None:
        key = self._key(ctx)
        row = self._db.execute(
            "SELECT * FROM token_journal WHERE subject=? AND operation=? AND key=?", (ctx.principal.subject, op, key)
        ).fetchone()
        if row is None:
            return None
        if now >= datetime.fromisoformat(row["expires_at"]):
            self._db.execute(
                "DELETE FROM token_journal WHERE subject=? AND operation=? AND key=?", (ctx.principal.subject, op, key)
            )
            return None
        if not hmac.compare_digest(row["payload_sha256"], fingerprint):
            raise TokenError("E_IDEMPOTENCY_CONFLICT")
        return d.TokenInfo.model_validate_json(row["snapshot"])

    @staticmethod
    def _fingerprint(op: str, payload: d.DTO, token_id: str | None = None) -> str:
        route = "/api/v1/tokens" if token_id is None else f"/api/v1/tokens/{token_id}/revoke"
        value = {
            "method": "POST",
            "route": route,
            "path_ids": {} if token_id is None else {"id": token_id},
            "payload": payload.model_dump(mode="json"),
        }
        return hashlib.sha256(_json(value).encode()).hexdigest()

    def _journal(self, ctx: RequestContext, op: str, fingerprint: str, token: d.TokenInfo, now: datetime) -> None:
        self._db.execute(
            "INSERT INTO token_journal VALUES (?,?,?,?,?,?,?,?)",
            (
                ctx.principal.subject,
                op,
                self._key(ctx),
                fingerprint,
                token.id,
                token.model_dump_json(),
                _stamp(now),
                _stamp(now + timedelta(hours=24)),
            ),
        )
        self._checkpoint("after_journal")

    async def create(self, ctx: RequestContext, payload: d.TokenCreate, *, now: datetime) -> d.TokenCreated:
        self._authority(ctx, now)
        request = _validate(d.TokenCreate, payload)
        if any(_PROFILE.fullmatch(profile) is None for profile in request.profiles):
            raise TokenError("E_VALIDATION")
        if (
            not set(request.profiles) <= ctx.principal.profiles
            or not set(request.permissions) <= ctx.principal.permissions
        ):
            raise TokenError("E_PERMISSION_DENIED")
        if request.expires_at > ctx.principal.expires_at:
            raise TokenError("E_VALIDATION")
        return self._unchecked_create(ctx, request, now)

    def _unchecked_create(self, ctx: RequestContext, request: d.TokenCreate, now: datetime) -> d.TokenCreated:
        utc_now(now)
        if any(_PROFILE.fullmatch(profile) is None for profile in request.profiles):
            raise TokenError("E_VALIDATION")
        fingerprint = self._fingerprint("tokens.create", request)
        with self._atomic():
            replay = self._replay(ctx, "tokens.create", fingerprint, now)
            if replay is not None:
                return d.TokenCreated(token=replay, secret=None)
            if request.expires_at <= now:
                raise TokenError("E_VALIDATION")
            token = d.TokenInfo(
                id=uuid4().hex,
                subject=request.subject,
                profiles=request.profiles,
                permissions=request.permissions,
                created_at=now,
                expires_at=request.expires_at,
                last_used_at=None,
                revoked_at=None,
                generation=1,
            )
            secret = _b64(secrets.token_bytes(32))
            digest = hashlib.sha256(secret.encode("ascii") + DIGEST_DOMAIN).digest()
            self._db.execute(
                "INSERT INTO tokens VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    token.id,
                    digest,
                    token.subject,
                    _json(token.profiles),
                    _json(token.permissions),
                    _stamp(now),
                    _stamp(token.expires_at),
                    None,
                    None,
                    1,
                ),
            )
            self._checkpoint("after_row")
            self._journal(ctx, "tokens.create", fingerprint, token, now)
        self._checkpoint("after_commit")
        return d.TokenCreated(token=token, secret=secret)

    async def revoke(
        self, ctx: RequestContext, token_id: str, payload: d.ControlMutation, *, now: datetime
    ) -> d.TokenInfo:
        self._authority(ctx, now)
        return self._unchecked_revoke(ctx, token_id, payload, now, local=False)

    def _unchecked_revoke(self, ctx, token_id, payload, now, *, local):
        utc_now(now)
        if not isinstance(token_id, str) or re.fullmatch("[0-9a-f]{32}", token_id, re.ASCII) is None:
            raise TokenError("E_VALIDATION")
        request = _validate(d.ControlMutation, payload)
        fingerprint = self._fingerprint("tokens.revoke", request, token_id)
        with self._atomic():
            row = self._db.execute(f"SELECT {_META} FROM tokens WHERE id=?", (token_id,)).fetchone()
            if row is None:
                raise TokenError("E_NOT_FOUND")
            before = self._info(row)
            if not local and not set(before.profiles) <= ctx.principal.profiles:
                # C2.4: scoped ids outside the caller's profiles are indistinguishable from absent.
                raise TokenError("E_NOT_FOUND")
            replay = self._replay(ctx, "tokens.revoke", fingerprint, now)
            if replay is not None:
                return replay
            if before.generation != request.expected_generation:
                raise TokenError("E_VERSION_CONFLICT")
            if before.revoked_at is None:
                changed = self._db.execute(
                    "UPDATE tokens SET revoked_at=?,generation=generation+1 "
                    "WHERE id=? AND generation=? AND revoked_at IS NULL",
                    (_stamp(now), token_id, request.expected_generation),
                )
                if changed.rowcount != 1:
                    raise TokenError("E_VERSION_CONFLICT")
            self._checkpoint("after_row")
            result = self._info(self._db.execute(f"SELECT {_META} FROM tokens WHERE id=?", (token_id,)).fetchone())
            self._journal(ctx, "tokens.revoke", fingerprint, result, now)
        self._checkpoint("after_commit")
        return result

    def _cursor(self, value: dict) -> str:
        encoded = _b64(_json(value).encode())
        return encoded + "." + _b64(hmac.digest(self._cursor_key, encoded.encode(), "sha256"))

    def _read_cursor(self, cursor: str, binding: dict, now: datetime) -> dict:
        try:
            if len(cursor) > 8192:
                raise ValueError
            encoded, signature = cursor.split(".")
            if not hmac.compare_digest(hmac.digest(self._cursor_key, encoded.encode(), "sha256"), _unb64(signature)):
                raise ValueError
            value = json.loads(_unb64(encoded))
            if set(value) != set(binding) | {"last", "upper", "issued_at", "expires_at"}:
                raise ValueError
            if any(value[key] != expected for key, expected in binding.items()):
                raise ValueError
            for key in ("last", "upper"):
                if (
                    not isinstance(value[key], list)
                    or len(value[key]) != 2
                    or not all(isinstance(item, str) for item in value[key])
                ):
                    raise ValueError
            issued = utc_now(datetime.fromisoformat(value["issued_at"]))
            expires = utc_now(datetime.fromisoformat(value["expires_at"]))
            if now < issued or expires - issued != timedelta(minutes=15):
                raise ValueError
        except (ValueError, TypeError, KeyError, UnicodeError, TokenError):
            raise TokenError("E_CURSOR_INVALID") from None
        if now >= expires:
            raise TokenError("E_CURSOR_EXPIRED")
        return value

    async def list(self, ctx: RequestContext, query: d.PageQuery, *, now: datetime) -> d.Page[d.TokenInfo]:
        self._authority(ctx, now)
        return self._list(ctx, query, now, local=False)

    def _list(self, ctx, query, now, *, local):
        utc_now(now)
        query = _validate(d.PageQuery, query)
        profiles = _json(sorted(ctx.principal.profiles))
        filters = hashlib.sha256(_json({"profiles": profiles, "local": local}).encode()).hexdigest()
        binding = dict(
            v=1, profile_id=None, subject=ctx.principal.subject, operation="tokens.list", filter_sha256=filters
        )
        value = self._read_cursor(query.cursor, binding, now) if query.cursor else None
        scope = "1" if local else "NOT EXISTS (SELECT 1 FROM json_each(tokens.profiles) p "
        if not local:
            scope += "WHERE p.value NOT IN (SELECT value FROM json_each(?)))"
        params = [] if local else [profiles]
        with self._atomic():
            if value is None:
                upper = self._db.execute(
                    f"SELECT created_at,id FROM tokens WHERE {scope} ORDER BY created_at DESC,id DESC LIMIT 1", params
                ).fetchone()
                if upper is None:
                    return d.Page[d.TokenInfo](items=[], next_cursor=None, has_more=False, request_id=ctx.request_id)
                value = dict(
                    binding,
                    upper=list(upper),
                    last=["", ""],
                    issued_at=_stamp(now),
                    expires_at=_stamp(now + timedelta(minutes=15)),
                )
            rows = self._db.execute(
                f"SELECT {_META} FROM tokens WHERE {scope} AND (created_at,id)>(?,?) "
                "AND (created_at,id)<=(?,?) ORDER BY created_at,id LIMIT ?",
                [*params, *value["last"], *value["upper"], query.limit + 1],
            ).fetchall()
        more = len(rows) > query.limit
        items = [self._info(row) for row in rows[: query.limit]]
        if more:
            value["last"] = [rows[query.limit - 1]["created_at"], rows[query.limit - 1]["id"]]
        return d.Page[d.TokenInfo](
            items=items, next_cursor=self._cursor(value) if more else None, has_more=more, request_id=ctx.request_id
        )

    @staticmethod
    def _local_context(now: datetime) -> RequestContext:
        # Private CLI root of trust, never used by create/list/revoke public paths.
        principal = Principal(
            "local-bootstrap",
            "local-bootstrap",
            None,
            frozenset(),
            frozenset(get_args(d.Permission)),
            1,
            now + timedelta(minutes=1),
        )
        return RequestContext("req_" + str(uuid4()), principal, None, uuid4(), float("inf"))

    def _bootstrap_create(self, payload: d.TokenCreate, *, now: datetime) -> d.TokenCreated:
        return self._unchecked_create(self._local_context(now), _validate(d.TokenCreate, payload), now)

    async def execute(self, ctx: RequestContext, command: d.GatewayCommand) -> d.GatewayResult:
        now = datetime.now(UTC)
        if command.profile_id is not None:
            raise TokenError("E_VALIDATION")
        if command.operation == "tokens.create":
            return await self.create(ctx, command.payload, now=now)
        if command.operation == "tokens.list":
            return await self.list(ctx, command.payload, now=now)
        if command.operation == "tokens.revoke":
            return await self.revoke(ctx, command.path_ids["id"], command.payload, now=now)
        raise TokenError("E_CAPABILITY_UNAVAILABLE")
