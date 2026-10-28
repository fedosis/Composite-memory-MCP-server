"""Fail-closed daemon CLI: no listener exists before the S06 secure shell."""
from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer


def daemon(
    http: Annotated[str, typer.Option("--http")] = "127.0.0.1:8080",
    registry: Annotated[Path, typer.Option("--registry")] = ...,
    control_path: Annotated[Path, typer.Option("--control-path")] = ...,
    profiles_root: Annotated[Path | None, typer.Option("--profiles-root")] = None,
    enable_mcp_http: Annotated[bool, typer.Option("--enable-mcp-http")] = False,
) -> None:
    """Start a neutral daemon (HTTP shell is unavailable until S06)."""
    # Do not read any store/config, import a runner or create a socket. Both
    # absent HttpFactory and unsupported MCP HTTP are C1 diagnostic exit 2.
    typer.echo("E_CAPABILITY_UNAVAILABLE", err=True)
    raise typer.Exit(code=2)
