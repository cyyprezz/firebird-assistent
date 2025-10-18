# dbapi.py – vereinheitlicht Connection für firebird-driver (FB>=3) & firebirdsql (FB 2.5)
from __future__ import annotations
from typing import Optional, Any, Tuple
import re

def _parse_dsn(dsn: str) -> Tuple[Optional[str], str]:
    """
    Unterstützt:
    - classic: host:path (Windows: host:C:\\path\\db.fdb)
    - key=val: host=...;database=...;...
    - nur Pfad: C:\\path\\db.fdb
    Rückgabe: (host|None, database_path)
    """
    low = dsn.lower().strip()
    if ";" in dsn and "database=" in low:
        parts = dict(p.split("=",1) for p in dsn.split(";") if "=" in p)
        host = parts.get("host")
        database = parts.get("database") or ""
        return (host, database)
    if ":" in dsn and not low.startswith("database="):
        host, path = dsn.split(":", 1)
        return (host or None, path)
    return (None, dsn)  # nur Pfad

def try_connect_firebird_driver(dsn: str, user: Optional[str], password: Optional[str]):
    from firebird.driver import connect
    return connect(dsn, user=user, password=password)

def try_connect_firebirdsql(dsn: str, user: Optional[str], password: Optional[str]):
    import firebirdsql
    host, database = _parse_dsn(dsn)
    # firebirdsql braucht host/database getrennt; bei None host → localhost
    kwargs = {
        "host": host or "localhost",
        "database": database,
        "user": user or "SYSDBA",
        "password": password or "",
        "charset": "UTF8",
    }
    return firebirdsql.connect(**kwargs)

def connect_unified(dsn: str, user: Optional[str], password: Optional[str]):
    """
    Versuche erst firebird-driver (FB>=3), dann firebirdsql (FB 2.5+).
    Gibt ein Connection-Objekt zurück, auf dem .cursor(), .commit() etc. funktionieren.
    """
    # 1) firebird-driver
    try:
        con = try_connect_firebird_driver(dsn, user, password)
        return con, "firebird-driver"
    except Exception:
        pass
    # 2) firebirdsql
    con = try_connect_firebirdsql(dsn, user, password)  # lässt Fehler hochgehen
    return con, "firebirdsql"
