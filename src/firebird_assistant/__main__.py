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
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    version: Optional[bool] = typer.Option(
        None, "--version",
        callback=lambda v: _version_callback(v),
        is_eager=True,
        help="Show version and exit",
    ),
):
    setup_logging(1 if verbose else 0)

def _version_callback(value: Optional[bool]):
    if value:
        typer.echo(f"firebird-assistant {__version__}")
        raise typer.Exit()


@app.command()
def backup(
    dsn: str = typer.Option(..., "--dsn", help="Firebird DSN e.g. localhost:C:\\data\\db.fdb"),
    output: str = typer.Option(..., "-o", "--output", help="Backup file (.fbk)"),
    user: Optional[str] = typer.Option(None, "--user", help="User (overrides DSN)"),
    password: Optional[str] = typer.Option(None, "--password", help="Password (overrides DSN)"),
    gbak_path: Optional[str] = typer.Option(None, "--gbak-path", help="Path to gbak executable"),
    compress: bool = typer.Option(False, "--compress", help="Use gbak -zip if supported"),
    no_auto_gbak: bool = typer.Option(False, "--no-auto-gbak", help="Disable auto selection of gbak by server version"),
):
    """Run a Firebird backup using gbak (auto-select gbak version by server)."""
    from .gbak_runner import run_backup, GbakError
    try:
        run_backup(
            dsn=dsn,
            output=output,
            user=user,
            password=password,
            gbak_path=gbak_path,
            auto_select=(not no_auto_gbak),
            compress=compress,
        )
        print(f"[bold green]Backup complete[/] -> [cyan]{output}[/]")
    except GbakError as e:
        print(f"[red]Backup failed:[/]\n{e}")
        raise typer.Exit(code=1)
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

@app.command()
def sql(
    dsn: str = typer.Option(..., "--dsn"),
    query: Optional[str] = typer.Option(None, "--sql"),
    file: Optional[str] = typer.Option(None, "-f", "--file"),
    allow_multi_values: bool = typer.Option(False, "--allow-multi-values"),
    user: Optional[str] = typer.Option(None, "--user"),
    password: Optional[str] = typer.Option(None, "--password"),
):
    
    if not query and not file:
        raise typer.BadParameter("Provide --sql or --file")
    count = execute_sql_file(dsn, file, allow_multi_values, user, password) if file else execute_sql(dsn, query, allow_multi_values, user, password)
    print(f"[bold green]Executed[/] {count} statement(s).")

@app.command()
def export(
    dsn: str = typer.Option(..., "--dsn"),
    sql: str = typer.Option(..., "--sql"),
    output: str = typer.Option(..., "-o", "--output"),
    chunk_size: int = typer.Option(10_000, "--chunk-size"),
    delimiter: str = typer.Option(",", "--delimiter"),
    quotechar: str = typer.Option('"', "--quotechar"),
    no_header: bool = typer.Option(False, "--no-header"),
    user: Optional[str] = typer.Option(None, "--user"),
    password: Optional[str] = typer.Option(None, "--password"),
):
    from .fb_utils import stream_query_to_csv     
    rows = stream_query_to_csv(
        dsn, sql, output,
        chunk_size, delimiter, quotechar,
        header=(not no_header),
        user=user, password=password
    )
    print(f"[bold green]Export complete[/]: {rows} rows -> [cyan]{output}[/]")
    
@app.command()
def detect(
    dsn: str = typer.Option(..., "--dsn"),
    user: Optional[str] = typer.Option(None, "--user"),
    password: Optional[str] = typer.Option(None, "--password"),
):
    from .fb_utils import detect_server_version, server_major
    ver = detect_server_version(dsn, user, password)
    print(f"Server version: {ver} (major={server_major(ver)})")

if __name__ == "__main__":
    app()

