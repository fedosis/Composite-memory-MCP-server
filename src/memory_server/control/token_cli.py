"""Local token bootstrap root of trust; importing this module performs no I/O.

--control-path names S02's control.sqlite in an explicit private directory.
No default store, environment credential, password or secret argument exists.
Only create stdout contains the one-time secret (the bearer secret component;
combine with token.id as cmms_<id>.<secret>). HTTP callers cannot use this path.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer
from pydantic import ValidationError

from memory_server.gateway_contracts import dto as d

from .policy import TokenError, utc_now
from .store import ControlError, ControlStore
from .tokens import TokenService

app = typer.Typer(help="Local token bootstrap over an explicit private control.sqlite.")


def _store(control_path: Path | None) -> ControlStore:
    if control_path is None or not control_path.is_absolute() or control_path.name != "control.sqlite":
        raise TokenError("E_VALIDATION")
    return ControlStore(control_path.parent)


def _error(error: TokenError | ControlError) -> None:
    # Do not serialize exception text, paths, input or Pydantic validation detail.
    typer.echo(json.dumps({"error": {"code": error.code, "message": "Token command failed"}}), err=True)
    raise typer.Exit(2)


@app.command("create")
def create(
    subject: Annotated[str, typer.Option("--subject")],
    profile: Annotated[list[str], typer.Option("--profile")],
    permission: Annotated[list[str], typer.Option("--permission")],
    expires_at: Annotated[str, typer.Option("--expires-at")],
    control_path: Annotated[Path | None, typer.Option("--control-path")] = None,
) -> None:
    """Explicit local root-of-trust issuance; output secret exactly once."""
    try:
        request = d.TokenCreate.model_validate_json(
            json.dumps(dict(subject=subject, profiles=profile, permissions=permission, expires_at=expires_at))
        )
        utc_now(request.expires_at)
        now = datetime.now(UTC)
        if request.expires_at <= now:
            raise TokenError("E_VALIDATION")
        with _store(control_path) as store:
            created = TokenService(store)._bootstrap_create(request, now=now)
        typer.echo(created.model_dump_json())
    except (ValidationError, ValueError):
        _error(TokenError("E_VALIDATION"))
    except (TokenError, ControlError) as error:
        _error(error)


@app.command("list")
def list_tokens(
    control_path: Annotated[Path | None, typer.Option("--control-path")] = None,
    limit: Annotated[int, typer.Option("--limit")] = 50,
    cursor: Annotated[str | None, typer.Option("--cursor")] = None,
) -> None:
    """Paginated metadata only; no verifier or secret is read for output."""
    try:
        now = datetime.now(UTC)
        with _store(control_path) as store:
            service = TokenService(store)
            page = service._list(service._local_context(now), d.PageQuery(limit=limit, cursor=cursor), now, local=True)
        typer.echo(page.model_dump_json())
    except ValidationError:
        _error(TokenError("E_VALIDATION"))
    except (TokenError, ControlError) as error:
        _error(error)


@app.command("revoke")
def revoke(
    token_id: Annotated[str, typer.Argument()],
    control_path: Annotated[Path | None, typer.Option("--control-path")] = None,
) -> None:
    """Irreversible local revoke, including durable post-transaction readback."""
    try:
        now = datetime.now(UTC)
        with _store(control_path) as store:
            service = TokenService(store)
            with service._atomic():
                row = service._db.execute("SELECT generation FROM tokens WHERE id=?", (token_id,)).fetchone()
                if row is None:
                    raise TokenError("E_NOT_FOUND")
            request = d.ControlMutation(
                reason="Local CLI revocation", confirmation=True, expected_generation=row["generation"]
            )
            service._revoke(service._local_context(now), token_id, request, now, local=True)
            with service._atomic():
                result = service._info(service._db.execute("SELECT * FROM tokens WHERE id=?", (token_id,)).fetchone())
                if result.revoked_at is None:
                    raise TokenError("E_STORAGE")
        typer.echo(result.model_dump_json())
    except (TokenError, ControlError) as error:
        _error(error)
