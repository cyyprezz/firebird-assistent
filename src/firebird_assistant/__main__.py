import typer
from typing import Optional
from rich import print
from .logging_setup import setup_logging
from .config import ExportConfig, BackupConfig, AnalyzeConfig
from . import __version__
from . import fb_utils
from . import profiles as prof

app = typer.Typer(add_completion=False, help="Firebird Assistant CLI — backups, exports, health checks")
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


def _resolve_with_profile(
    profile: Optional[str],
    dsn: Optional[str],
    user: Optional[str],
    role: Optional[str] = None,
    charset: Optional[str] = None,
    *,
    prompt_password: bool = False,
) -> tuple[str, Optional[str], Optional[str], Optional[str]]:
    """
    Resolve DSN/user/role/charset using a saved profile if provided.
    Returns (dsn, user, role, password) where password may be prompted if requested.
    """
    pwd: Optional[str] = None
    if profile:
        p = prof.get_profile(profile)
        if not p:
            raise typer.BadParameter(f"Unknown profile: {profile}")
        dsn = dsn or p.dsn
        user = user or p.user
        role = role or p.role
        charset = charset or p.charset
        if prompt_password and pwd is None:
            pwd = typer.prompt("Password", hide_input=True, default="")
    # Without profile, optionally prompt if demanded but only when dsn provided
    if prompt_password and pwd is None and (dsn or user):
        pwd = typer.prompt("Password", hide_input=True, default="")
    if not dsn:
        raise typer.BadParameter("DSN is required (provide --dsn or --profile)")
    return dsn, user, role, pwd


@app.command()
def backup(
    profile: Optional[str] = typer.Option(None, "--profile", help="Saved connection profile"),
    dsn: Optional[str] = typer.Option(None, "--dsn", help="Firebird DSN e.g. localhost:C:\\data\\db.fdb"),
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
        dsn_res, user_res, _, pwd_res = _resolve_with_profile(profile, dsn, user, prompt_password=bool(profile) and password is None)
        run_backup(
            dsn=dsn_res,
            output=output,
            user=user_res,
            password=password if password is not None else pwd_res,
            gbak_path=gbak_path,
            auto_select=(not no_auto_gbak),
            compress=compress,
        )
        print(f"[bold green]Backup complete[/] -> [cyan]{output}[/]")
    except GbakError as e:
        print(f"[red]Backup failed:[/]\n{e}")
        raise typer.Exit(code=1)

@app.command()
def restore(
    profile: Optional[str] = typer.Option(None, "--profile", help="Saved connection profile"),
    backup: str = typer.Option(..., "-i", "--input", help="Backup file (.fbk)"),
    dsn: Optional[str] = typer.Option(None, "--dsn", help="Database DSN / path to restore into"),
    user: Optional[str] = typer.Option(None, "--user", help="User (overrides DSN)"),
    password: Optional[str] = typer.Option(None, "--password", help="Password (overrides DSN)"),
    gbak_path: Optional[str] = typer.Option(None, "--gbak-path", help="Path to gbak executable"),
    replace_existing: bool = typer.Option(False, "--replace-existing", help="Replace existing database instead of creating a new one"),
    no_auto_gbak: bool = typer.Option(False, "--no-auto-gbak", help="Disable auto selection of gbak by server version"),
):
    """Restore a Firebird database from a backup using gbak."""
    from .gbak_runner import run_restore, GbakError
    try:
        dsn_res, user_res, _, pwd_res = _resolve_with_profile(profile, dsn, user, prompt_password=bool(profile) and password is None)
        run_restore(
            backup_file=backup,
            dsn=dsn_res,
            user=user_res,
            password=password if password is not None else pwd_res,
            gbak_path=gbak_path,
            auto_select=(not no_auto_gbak),
            replace=replace_existing,
        )
        print(f"[bold green]Restore complete[/] -> [cyan]{dsn_res}[/]")
    except GbakError as e:
        print(f"[red]Restore failed:[/]\n{e}")
        raise typer.Exit(code=1)
@app.command()
def analyze(
    profile: Optional[str] = typer.Option(None, "--profile", help="Saved connection profile"),
    dsn: Optional[str] = typer.Option(None, "--dsn", help="Firebird DSN"),
    user: Optional[str] = typer.Option(None, "--user"),
    password: Optional[str] = typer.Option(None, "--password"),
):
    """Show a quick health summary (MVP)."""
    dsn_res, user_res, _, pwd_res = _resolve_with_profile(profile, dsn, user, prompt_password=bool(profile) and password is None)
    cfg = AnalyzeConfig(dsn=dsn_res)
    summary = fb_utils.quick_health_summary(cfg.dsn, user=user_res, password=password if password is not None else pwd_res)
    if not summary:
        print("[red]No summary available (permissions or DB version).[/]")
    else:
        print("[bold]Health summary:[/]")
        for k, v in summary.items():
            print(f" - [cyan]{k}[/]: {v}")

@app.command()
def sql(
    profile: Optional[str] = typer.Option(None, "--profile", help="Saved connection profile"),
    dsn: Optional[str] = typer.Option(None, "--dsn"),
    query: Optional[str] = typer.Option(None, "--sql"),
    file: Optional[str] = typer.Option(None, "-f", "--file"),
    allow_multi_values: bool = typer.Option(False, "--allow-multi-values"),
    user: Optional[str] = typer.Option(None, "--user"),
    password: Optional[str] = typer.Option(None, "--password"),
):
    if not query and not file:
        raise typer.BadParameter("Provide --sql or --file")
    dsn_res, user_res, _, pwd_res = _resolve_with_profile(profile, dsn, user, prompt_password=bool(profile) and password is None)
    count = (
        fb_utils.execute_sql_file(dsn_res, file, allow_multi_values, user_res, password if password is not None else pwd_res)
        if file
        else fb_utils.execute_sql(dsn_res, query, allow_multi_values, user_res, password if password is not None else pwd_res)
    )
    print(f"[bold green]Executed[/] {count} statement(s).")

@app.command()
def export(
    profile: Optional[str] = typer.Option(None, "--profile", help="Saved connection profile"),
    dsn: Optional[str] = typer.Option(None, "--dsn"),
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
    dsn_res, user_res, _, pwd_res = _resolve_with_profile(profile, dsn, user, prompt_password=bool(profile) and password is None)
    rows = stream_query_to_csv(
        dsn_res, sql, output,
        chunk_size, delimiter, quotechar,
        header=(not no_header),
        user=user_res, password=password if password is not None else pwd_res
    )
    print(f"[bold green]Export complete[/]: {rows} rows -> [cyan]{output}[/]")
    
@app.command()
def detect(
    profile: Optional[str] = typer.Option(None, "--profile", help="Saved connection profile"),
    dsn: Optional[str] = typer.Option(None, "--dsn"),
    user: Optional[str] = typer.Option(None, "--user"),
    password: Optional[str] = typer.Option(None, "--password"),
):
    from .fb_utils import detect_server_version, server_major
    dsn_res, user_res, _, pwd_res = _resolve_with_profile(profile, dsn, user, prompt_password=bool(profile) and password is None)
    ver = detect_server_version(dsn_res, user_res, password if password is not None else pwd_res)
    print(f"Server version: {ver} (major={server_major(ver)})")


@app.command()
def shell(
    profile: Optional[str] = typer.Option(None, "--profile", help="Saved connection profile"),
    dsn: Optional[str] = typer.Option(None, "--dsn", help="Firebird DSN"),
    user: Optional[str] = typer.Option(None, "--user"),
    password: Optional[str] = typer.Option(None, "--password"),
):
    """Interactive session with a persistent connection."""
    from .connection import open_dsn
    from .gbak_runner import run_backup, GbakError
    from .fb_utils import stream_query_to_csv, detect_server_version

    dsn_res, user_res, _, pwd_res = _resolve_with_profile(
        profile, dsn, user, prompt_password=bool(profile) and password is None
    )
    pwd_use = password if password is not None else pwd_res
    try:
        with open_dsn(dsn_res, user=user_res, password=pwd_use) as conn:
            print(f"[bold]Connected[/] engine={conn.engine()} ver={conn.server_version()}")
            print("Type SQL (end with ';') or commands: :help, :export, :backup, :detect, :analyze, :quit")
            buffer: list[str] = []
            while True:
                try:
                    line = input("fba> ").strip()
                except EOFError:
                    break
                if not line:
                    continue
                if line.startswith(":"):
                    parts = line.split()
                    cmd = parts[0][1:].lower()
                    if cmd in ("quit", "exit", "q"):
                        break
                    if cmd == "help":
                        print(":export <output.csv> <SQL ending with ;>")
                        print(":backup -o <file.fbk>")
                        print(":detect | :analyze | :quit")
                        continue
                    if cmd == "export":
                        if len(parts) < 3:
                            print("[red]Usage[/]: :export <output.csv> <SQL...> ;")
                            continue
                        out = parts[1]
                        sql_text = line.split(None, 2)[2]
                        if not sql_text.strip().endswith(";"):
                            print("[red]SQL must end with ';'[/]")
                            continue
                        try:
                            rows = stream_query_to_csv(
                                dsn_res, sql_text.rstrip(";"), out, user=user_res, password=pwd_use
                            )
                            print(f"[green]Exported[/] {rows} rows -> {out}")
                        except Exception as e:
                            print(f"[red]Export error:[/] {e}")
                        continue
                    if cmd == "backup":
                        # Expect: :backup -o file.fbk
                        try:
                            if "-o" in parts:
                                idx = parts.index("-o")
                                out = parts[idx + 1]
                            else:
                                print("[red]Usage[/]: :backup -o <file.fbk>")
                                continue
                            run_backup(
                                dsn=dsn_res, output=out, user=user_res, password=pwd_use, auto_select=True
                            )
                            print(f"[green]Backup complete[/] -> {out}")
                        except (IndexError, GbakError, Exception) as e:
                            print(f"[red]Backup error:[/] {e}")
                        continue
                    if cmd == "detect":
                        ver2 = detect_server_version(dsn_res, user=user_res, password=pwd_use)
                        print(f"Server version: {ver2}")
                        continue
                    if cmd == "analyze":
                        summ = fb_utils.quick_health_summary(dsn_res, user=user_res, password=pwd_use)
                        if not summ:
                            print("[yellow]No summary available.[/]")
                        else:
                            print("[bold]Health summary:[/]")
                            for k, v in summ.items():
                                print(f" - {k}: {v}")
                        continue
                    print(f"[yellow]Unknown command[/]: {cmd}")
                    continue
                # SQL accumulation
                buffer.append(line)
                if line.endswith(";"):
                    sql_text = "\n".join(buffer)
                    buffer.clear()
                    try:
                        cur = conn._raw.cursor()
                        try:
                            cur.execute(sql_text)
                            desc = getattr(cur, "description", None)
                            if desc:
                                cols = [getattr(d, "name", None) or d[0] for d in desc]
                                print("| "+" | ".join(str(c) for c in cols))
                                fetched = 0
                                while True:
                                    rows = cur.fetchmany(50)
                                    if not rows:
                                        break
                                    for r in rows:
                                        print("| "+" | ".join(str(x) for x in r))
                                        fetched += 1
                                    if fetched >= 200:
                                        print("[yellow]Output truncated at 200 rows.[/]")
                                        break
                            else:
                                conn._raw.commit()
                                print("[green]OK[/]")
                        finally:
                            cur.close()
                    except Exception as e:
                        print(f"[red]SQL error:[/] {e}")
    except Exception as e:
        print(f"[red]Connect failed:[/] {e}")


conn = typer.Typer(help="Manage saved connection profiles")
app.add_typer(conn, name="conn")


@conn.command("add")
def conn_add(
    name: str = typer.Argument(...),
    dsn: str = typer.Option(..., "--dsn"),
    user: Optional[str] = typer.Option(None, "--user"),
    role: Optional[str] = typer.Option(None, "--role"),
    charset: str = typer.Option("UTF8", "--charset"),
):
    """Add or update a connection profile (no password stored)."""
    store = prof.load_store()
    store.add(prof.ConnectionProfile(name=name, dsn=dsn, user=user, role=role, charset=charset))
    prof.save_store(store)
    print(f"[green]Saved profile[/]: {name}")


@conn.command("list")
def conn_list():
    """List saved connection profiles."""
    store = prof.load_store()
    if not store.profiles:
        print("[yellow]No profiles saved.[/]")
        return
    for name, p in store.profiles.items():
        print(f"- [cyan]{name}[/]: dsn={p.dsn} user={p.user or ''} role={p.role or ''} charset={p.charset}")


@conn.command("show")
def conn_show(name: str = typer.Argument(...)):
    p = prof.get_profile(name)
    if not p:
        print(f"[red]Profile not found:[/] {name}")
        raise typer.Exit(code=1)
    print(f"name={p.name}\ndsn={p.dsn}\nuser={p.user or ''}\nrole={p.role or ''}\ncharset={p.charset}")


@conn.command("rm")
def conn_rm(name: str = typer.Argument(...)):
    store = prof.load_store()
    if name not in store.profiles:
        print(f"[red]Profile not found:[/] {name}")
        raise typer.Exit(code=1)
    store.remove(name)
    prof.save_store(store)
    print(f"[green]Removed profile[/]: {name}")


@conn.command("rename")
def conn_rename(old: str = typer.Argument(...), new: str = typer.Argument(...)):
    store = prof.load_store()
    try:
        store.rename(old, new)
    except KeyError:
        print(f"[red]Profile not found:[/] {old}")
        raise typer.Exit(code=1)
    except ValueError:
        print(f"[red]Target profile exists:[/] {new}")
        raise typer.Exit(code=1)
    prof.save_store(store)
    print(f"[green]Renamed profile[/]: {old} -> {new}")

if __name__ == "__main__":
    app()
