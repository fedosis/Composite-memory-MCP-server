"""CLI entry point for Composite Memory MCP Server."""

import typer

from memory_server.server import create_server, run_server_stdio

app = typer.Typer(
    name="memory-server",
    help="Composite Memory MCP Server — agent-independent memory service",
)


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    version: bool = typer.Option(False, "--version", "-V", help="Show version"),
):
    """Start the MCP server or show version."""
    if version:
        from memory_server import __version__

        typer.echo(f"memory-server v{__version__}")
        raise typer.Exit()

    if ctx.invoked_subcommand is None:
        # No subcommand → start MCP server in stdio mode
        server = create_server()
        run_server_stdio(server)


@app.command()
def ping():
    """Quick connectivity check (without starting a full MCP server)."""
    typer.echo('{"status": "ok"}')


if __name__ == "__main__":
    app()
