"""
Utility functions for the Firebird Assistant.

This module wraps common database actions such as exporting query results
to CSV, executing arbitrary SQL, and detecting server version. It uses
the unified connection layer provided by :mod:`firebird_assistant.connection`
when available and falls back to legacy approaches when necessary.
"""

import csv
import logging
import subprocess
import shutil
import os
import re
from typing import Optional

# Import unified connection helpers
from .connection import open_dsn, FirebirdError
from .dbapi import connect_unified

logger = logging.getLogger(__name__)


def stream_query_to_csv(
    dsn: str,
    sql: str,
    output: str,
    chunk_size: int = 10_000,
    delimiter: str = ",",
    quotechar: str = '"',
    header: bool = True,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> int:
    """
    Stream the results of a SQL query to a CSV file.

    This function attempts to use the unified Firebird connection layer to
    execute the query in a memory-efficient way. If the unified layer is
    unavailable or fails, it falls back to the legacy :func:`connect_unified`.

    :param dsn: Firebird DSN (host[:/port]:path or local path).
    :param sql: SQL query to execute.
    :param output: Path to the output CSV file.
    :param chunk_size: Number of rows per fetch.
    :param delimiter: CSV field delimiter.
    :param quotechar: CSV quote character.
    :param header: Whether to write column names as the first row.
    :param user: Optional username.
    :param password: Optional password.
    :return: Number of rows exported.
    """
    rowcount = 0
    # Try unified connection
    try:
        with open_dsn(dsn, user=user, password=password) as conn:
            cur = conn._raw.cursor()  # use raw cursor for streaming
            cur.execute(sql)
            cols = [getattr(d, "name", None) or (d[0] if isinstance(d, (list, tuple)) else str(i))
                    for i, d in enumerate(getattr(cur, "description", []) or [])]
            with open(output, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=delimiter, quotechar=quotechar)
                if header and cols:
                    w.writerow(cols)
                while True:
                    rows = cur.fetchmany(chunk_size)
                    if not rows:
                        break
                    w.writerows(rows)
                    rowcount += len(rows)
            logger.info("CSV export done via %s: %s rows -> %s", conn.engine(), rowcount, output)
        return rowcount
    except Exception as e:
        logger.warning("stream_query_to_csv fallback due to error: %s", e)
    # Fallback: use legacy connect_unified
    con, driver = connect_unified(dsn, user, password)
    try:
        cur = con.cursor()
        cur.execute(sql)
        cols = [getattr(d, "name", None) or (d[0] if isinstance(d, (list, tuple)) else str(i))
                for i, d in enumerate(getattr(cur, "description", []) or [])]
        with open(output, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=delimiter, quotechar=quotechar)
            if header and cols:
                w.writerow(cols)
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                w.writerows(rows)
                rowcount += len(rows)
        logger.info("CSV export done via %s: %s rows -> %s", driver, rowcount, output)
    finally:
        try:
            con.close()
        except Exception:
            pass
    return rowcount


def execute_sql(
    dsn: str,
    sql: str,
    allow_multi_values: bool = False,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> int:
    """
    Execute one or more SQL statements separated by semicolons.

    Uses the unified connection layer if available; otherwise falls back to
    the legacy :func:`connect_unified`.
    """
    raw_stmts = [s.strip() for s in sql.split(";") if s.strip()]
    stmts = [st + ";" for st in raw_stmts]
    try:
        with open_dsn(dsn, user=user, password=password) as conn:
            cur = conn._raw.cursor()
            for st in stmts:
                cur.execute(st)
            conn._raw.commit()
            logger.info("Executed %d statements via %s", len(stmts), conn.engine())
            return len(stmts)
    except Exception as e:
        logger.warning("execute_sql fallback due to error: %s", e)
    # Fallback
    con, driver = connect_unified(dsn, user, password)
    try:
        cur = con.cursor()
        for st in stmts:
            cur.execute(st)
        con.commit()
        logger.info("Executed %d statements via %s", len(stmts), driver)
        return len(stmts)
    finally:
        try:
            con.close()
        except Exception:
            pass


def detect_server_version(
    dsn: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> str:
    """
    Detect the Firebird server version.

    First attempts to use the unified connection layer; if it fails, falls back
    to querying via ``fbsvcmgr``.

    :return: Version string like "3.0.10" or "2.5.9". Returns "0.0.0" if detection fails.
    """
    # Attempt via unified connection
    try:
        with open_dsn(dsn, user=user, password=password) as conn:
            return conn.server_version()
    except Exception:
        pass
    # Fallback (FB 2.5 etc.): fbsvcmgr info_server_version
    svc = _service_target_from_dsn(dsn)  # host:service_mgr
    fbsvcmgr = _find_fbsvcmgr()
    cmd = [fbsvcmgr, svc]
    if user:
        cmd += ["user", user]
    if password:
        cmd += ["password", password]
    cmd += ["info_server_version"]
    try:
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT, timeout=10)
        m = re.search(r"Firebird\s+(\d+\.\d+(\.\d+)?)", out)
        if m:
            return m.group(1)
    except Exception:
        return "0.0.0"
    return "0.0.0"


_MULTI_VALUES_RE = re.compile(
    r"^\s*INSERT\s+INTO\s+([A-Z0-9_\$]+)\s*\(([^)]+)\)\s*VALUES\s*(\(.+\))\s*;\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _expand_multi_values_insert(sql: str) -> list[str]:
    """
    Expand an INSERT statement with multiple value tuples into individual INSERTs.

    Example::

      INSERT INTO t (a,b) VALUES (1,2), (3,4);

    becomes two statements.
    """
    m = _MULTI_VALUES_RE.match(sql.strip().rstrip(";") + ";")
    if not m:
        return [sql]
    table = m.group(1)
    cols = m.group(2)
    values_block = m.group(3)
    parts = []
    depth = 0
    start = 0
    s = values_block.strip()
    for i, ch in enumerate(s):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(s[start:i].strip())
            start = i + 1
    parts.append(s[start:].strip())
    stmts = [f"INSERT INTO {table} ({cols}) VALUES {p.strip().rstrip(',')};" for p in parts if p]
    return stmts


def execute_sql_file(
    dsn: str,
    path: str,
    allow_multi_values: bool = False,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> int:
    """
    Execute SQL from a file. See :func:`execute_sql` for details.
    """
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    return execute_sql(dsn, sql, allow_multi_values=allow_multi_values, user=user, password=password)


def _parse_dsn(dsn: str):
    """
    Parse a Firebird DSN into (host, port, path).

    Supports:
      - 'C:\\path\\db.fdb' (local, XNET)
      - 'localhost:C:\\path\\db.fdb'
      - 'localhost/3053:C:\\path\\db.fdb'
    Returns (host, port, path). For local attaches, returns (None, None, fullpath).
    """
    if re.search(r'^[A-Za-z]:\\', dsn):
        return (None, None, dsn)
    if ':' in dsn:
        hostpart, path = dsn.split(':', 1)
        if '/' in hostpart:
            host, port = hostpart.split('/', 1)
        else:
            host, port = hostpart, None
        return (host or None, port or None, path)
    return (None, None, dsn)


def _service_target_from_dsn(dsn: str) -> str:
    """
    Build the fbsvcmgr target from a DSN.

      - local/XNET: 'service_mgr'
      - TCP without port: 'host:service_mgr'
      - TCP with port:  'host/port:service_mgr'
    """
    host, port, _ = _parse_dsn(dsn)
    if host is None:
        return 'service_mgr'
    if port:
        return f'{host}/{port}:service_mgr'
    return f'{host}:service_mgr'


def _find_fbsvcmgr() -> str:
    """
    Locate the fbsvcmgr executable.
    """
    p = shutil.which("fbsvcmgr")
    if p:
        return p
    candidates = [
        r"C:\\Program Files\\Firebird\\Firebird_5_0\\fbsvcmgr.exe",
        r"C:\\Program Files\\Firebird\\Firebird_4_0\\fbsvcmgr.exe",
        r"C:\\Program Files\\Firebird\\Firebird_3_0\\fbsvcmgr.exe",
        r"C:\\Program Files\\Firebird\\Firebird_2_5\\fbsvcmgr.exe",
        "/opt/firebird/bin/fbsvcmgr", "/usr/bin/fbsvcmgr", "/usr/local/firebird/bin/fbsvcmgr",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    return "fbsvcmgr"