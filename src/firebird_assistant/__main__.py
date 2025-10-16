import typer
from typing import Optional
from rich import print
from .logging_setup import setup_logging
from .config import ExportConfig, BackupConfig, AnalyzeConfig
from . import __version__
from . import fb_utils

app = typer.Typer(add_completion=False, help="Firebird Assistant CLI — backups, exports, health checks")

@app.callback()
def main(
    ctx: typer.Context,
    verbose: int = typer.Option(0, "--verbose", "-v", count=True, help="Increase verbosity (-v/-vv)"),
    version: Optional[bool] = typer.Option(None, "--version", callback=lambda v: _version_callback(v), is_eager=True, help="Show version and exit"),
):
    setup_logging(verbose)

def _version_callback(value: Optional[bool]):
    if value:
        typer.echo(f"firebird-assistant {__version__}")
        raise typer.Exit()

@app.command()
def export(
    dsn: str = typer.Option(..., "--dsn", help="Firebird DSN: host=...;database=...;user=...;password=..."),
    sql: str = typer.Option(..., "--sql", help="SQL query to export"),
    output: str = typer.Option(..., "-o", "--output", help="Output CSV file path"),
    chunk_size: int = typer.Option(10_000, "--chunk-size", help="Rows per batch"),
    delimiter: str = typer.Option(",", "--delimiter", help="CSV delimiter"),
    quotechar: str = typer.Option('"', "--quotechar", help="CSV quote char"),
    no_header: bool = typer.Option(False, "--no-header", help="Do not write header row"),
):
    """Export a SQL query to CSV in streaming mode."""
    cfg = ExportConfig(dsn=dsn, sql=sql, output=output, chunk_size=chunk_size, delimiter=delimiter, quotechar=quotechar, header=(not no_header))
    rows = fb_utils.stream_query_to_csv(**cfg.model_dump())
    print(f"[bold green]Export complete[/]: {rows} rows -> [cyan]{output}[/]")

@app.command()
def backup(
    dsn: str = typer.Option(..., "--dsn", help="Firebird DSN"),
    output: str = typer.Option(..., "-o", "--output", help="Backup file (.fbk)"),
):
    """(MVP) Placeholder for backup command. Use gbak externally for now."""
    cfg = BackupConfig(dsn=dsn, output=output)
    print("[yellow]Backup stub:[/] saving from DSN to", cfg.output)
    print("This is a placeholder. In v0.1 we recommend running gbak manually. Future release will wrap it.")

@app.command()
def analyze(
    dsn: str = typer.Option(..., "--dsn", help="Firebird DSN"),
):
    """Show a quick health summary (MVP)."""
    cfg = AnalyzeConfig(dsn=dsn)
    summary = fb_utils.quick_health_summary(cfg.dsn)
    if not summary:
        print("[red]No summary available (permissions or DB version).[/]")
    else:
        print("[bold]Health summary:[/]")
        for k, v in summary.items():
            print(f" - [cyan]{k}[/]: {v}")

if __name__ == "__main__":
    app()
