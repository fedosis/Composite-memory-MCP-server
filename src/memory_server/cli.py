import typer

from .server import run as run_server

app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    if ctx.invoked_subcommand is None:
        run_server()


@app.command()
def serve():
    """Start the MCP server (stdio transport)"""
    run_server()


if __name__ == "__main__":
    app()
