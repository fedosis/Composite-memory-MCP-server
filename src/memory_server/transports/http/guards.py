"""Closed HTTP authentication, admission and audit boundary."""
from __future__ import annotations

import asyncio
import hashlib
from collections import OrderedDict
from datetime import UTC, datetime
from time import monotonic
from uuid import UUID

from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from memory_server.control.policy import bearer_from_headers, require_permission
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import AuditOutcome, RequestContext

from .errors import HttpError, error_code, error_response
from .routes.profiles import audit_ticket, control_pending, control_reply, validate_profile_id, validate_query


class Budget:
    """Bounded token bucket table; no user-controlled metric labels."""

    def __init__(self, config):
        self.config = config
        self.buckets = OrderedDict()

    def take(self, key):
        now = monotonic()
        balance, then = self.buckets.pop(key, (float(self.config.rate_burst), now))
        balance = min(self.config.rate_burst, balance + (now - then) * self.config.rate_per_minute / 60)
        accepted = balance >= 1
        self.buckets[key] = (balance - 1 if accepted else balance, now)
        if len(self.buckets) > 1024:
            self.buckets.popitem(last=False)
        if not accepted:
            raise HttpError("E_RATE_LIMIT")


class Guard:
    def __init__(self, config, dependencies, *, denial=None, publish=None, saturation=None):
        self.config, self.dependencies = config, dependencies
        self.denial = denial or getattr(dependencies.access, "record_unauthenticated", None)
        self.publish = publish or getattr(dependencies.access, "publish_committed", None)
        self.saturation = saturation or getattr(dependencies.access, "saturation", None)
        if not all(callable(item) for item in (self.denial, self.publish, self.saturation)):
            raise ValueError("E_CAPABILITY_UNAVAILABLE: explicit audit bindings required")
        self.pre_auth = Budget(config)
        self.per_token = Budget(config)
        self.cache = OrderedDict()
        self.active = 0
        self.profiles = {}
        self.closing = False

    async def authenticate(self, request, request_id):
        try:
            bearer = bearer_from_headers(list(request.headers.items()), query_string=request.scope['query_string'])
            key = hashlib.sha256(bearer.encode()).digest()
            now = datetime.now(UTC)
            principal = self.cache.pop(key, None)
            if principal is None:
                principal = await self.dependencies.tokens.authenticate(bearer, now=now)
            else:
                self.dependencies.tokens.verify_principal(principal, now=now)
            self.cache[key] = principal
            if len(self.cache) > 256:
                self.cache.popitem(last=False)
            return principal
        except Exception as exc:
            code = error_code(exc)
            if code == 'E_AUTH_REQUIRED':
                try:
                    await self.denial(request_id, 'E_AUTH_REQUIRED')
                except Exception:
                    pass  # Bound store retains its failing denial diagnostic.
            raise HttpError(code, getattr(exc, 'status', None)) from None

    def admit(self):
        if self.closing:
            raise HttpError('E_RUNTIME_CLOSING')
        if self.active >= 16:
            raise HttpError('E_RATE_LIMIT')
        self.active += 1

    async def execute(self, request: Request, spec, body: bytes, request_id: str, deadline: float):
        principal = await self.authenticate(request, request_id)
        profile = request.path_params.get('profile')
        if profile is not None:
            validate_profile_id(profile)
        require_permission(principal, spec.permission, profile_id=profile)
        idem = None
        if hasattr(spec, 'parse'):
            payload_command = spec.parse(request, body, principal)
            command = payload_command
        elif spec.operation == 'profiles.list':
            if body:
                raise HttpError('E_VALIDATION')
            payload = validate_query(spec.operation, request.query_params)
        elif spec.operation == 'profiles.get':
            if body or request.query_params:
                raise HttpError('E_VALIDATION')
            payload = d.Empty()
        else:
            if request.query_params:
                raise HttpError('E_VALIDATION')
            if request.headers.get('content-type', '').split(';')[0].strip().lower() != 'application/json':
                raise HttpError('E_UNSUPPORTED_MEDIA_TYPE')
            keys = request.headers.getlist('idempotency-key')
            if not keys:
                raise HttpError('E_IDEMPOTENCY_REQUIRED')
            if len(keys) != 1:
                raise HttpError('E_VALIDATION')
            try:
                idem = UUID(keys[0])
            except ValueError:
                raise HttpError('E_VALIDATION') from None
            try:
                payload = d.ControlMutation.model_validate_json(body)
            except ValidationError as exc:
                if any(item['type'] == 'missing' and item['loc'] == ('expected_generation',)
                       for item in exc.errors()):
                    raise HttpError('E_PRECONDITION_REQUIRED') from None
                raise HttpError('E_VALIDATION') from None
        if not hasattr(spec, 'parse'):
            command = d.COMMAND_TYPES[spec.operation](
                operation=spec.operation, profile_id=profile, path_ids={}, payload=payload)
        port = self.dependencies.ports.get(spec.operation)
        if port is None:
            raise HttpError('E_CAPABILITY_UNAVAILABLE')
        if not hasattr(spec, 'parse') and spec.operation == 'profiles.list' and payload.cursor:
            # Validation remains before audit and every registry/port call.
            cursor = self.cursor_validator.validate_cursor(principal, payload.cursor)
            if payload.limit > cursor['limit']:
                raise HttpError('E_CURSOR_INVALID')
        self.per_token.take(principal.token_id)
        ctx = RequestContext(request_id, principal, profile, idem, deadline)
        try:
            ticket = await self.dependencies.access.begin(ctx, spec.operation)
        except Exception:
            raise HttpError('E_AUDIT_UNAVAILABLE') from None
        bound = audit_ticket.set(ticket)
        reply_token = control_reply.set(None)
        pending_token = control_pending.set(False)
        started = monotonic()
        response, code = None, None
        cancelled = False
        try:
            result = await port.execute(ctx, command)
            # Validate injected DTOs as well; only closed typed success shapes.
            if spec.operation == 'profiles.list':
                result = d.Page[d.Profile].model_validate(result.model_dump())
                result = result.model_copy(update={'items': [p.model_copy(update={'storage_root': None})
                                                            for p in result.items]})
            else:
                result = d.Profile.model_validate(result.model_dump()).model_copy(update={'storage_root': None})
            response = JSONResponse(result.model_dump(mode='json'))
            reply = control_reply.get()
            if reply is not None and reply.replayed:
                response.headers['Idempotency-Replayed'] = 'true'
                response.headers['X-Original-Request-ID'] = reply.original_request_id
        except asyncio.CancelledError:
            code, cancelled = 'E_TIMEOUT', True
            response = error_response(code, request_id)
        except Exception as exc:
            code = error_code(exc)
            response = error_response(exc if isinstance(exc, HttpError) else code, request_id)
        finally:
            # Registry.control owns publication of the original durable ticket.
            # A replay still terminates this request's independently admitted intent.
            reply = control_reply.get()
            journal_owned = control_pending.get() or (reply is not None and not reply.replayed)
            try:
                if not journal_owned:
                    await asyncio.shield(self.dependencies.access.finish(
                        ticket, AuditOutcome(response.status_code if response else 500, code, (),
                                             (monotonic() - started) * 1000)))
            except Exception:
                if reply is None:
                    response = error_response('E_AUDIT_UNAVAILABLE', request_id)
            finally:
                audit_ticket.reset(bound)
                control_reply.reset(reply_token)
                control_pending.reset(pending_token)
        self.dependencies.metrics.http_completed(ctx, spec.operation, response.status_code, monotonic() - started)
        if cancelled:
            raise asyncio.CancelledError
        return response

    async def execute_read(self, request, spec, body: bytes, request_id: str, deadline: float):
        """Named S07 seam; read descriptors use the same execute lifecycle."""
        return await self.execute(request, spec, body, request_id, deadline)


def secure_headers(response: Response, request_id: str, origin: str | None, allowed_origins):
    response.headers['X-Request-ID'] = request_id
    response.headers['Cache-Control'] = 'no-store'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['Referrer-Policy'] = 'no-referrer'
    response.headers['Connection'] = 'close'
    if origin in allowed_origins:
        response.headers['Access-Control-Allow-Origin'] = origin
        response.headers['Vary'] = 'Origin'
    if response.status_code == 429:
        response.headers['Retry-After'] = '1'
    return response
