"""
Unified Firebird connection layer with automatic version detection.

This module provides a context-managed connection wrapper that supports both
Firebird 3.0+ (via ``firebird-driver``) and Firebird 2.5 (via ``firebirdsql``).

Features:
  - Auto-detection of the server version to select the appropriate driver.
  - Unified methods for querying and executing SQL.
  - Streaming CSV export with adjustable batch size and delimiter.
  - Explicit error types for better diagnostics.

Example usage::

    from firebird_assistant.connection import connect

    with connect(host="localhost", port=3050, database="/path/to/db.fdb",
                 user="SYSDBA", password="masterkey") as conn:
        print("Engine:", conn.engine())
        print("Server version:", conn.server_version())
        rows = conn.query("SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG = 0")
        for (name,) in rows:
            print(name.strip())
        with open("export.csv", "w", encoding="utf-8") as f:
            conn.export_table_to_csv("MY_TABLE", f)
"""

from __future__ import annotations

import contextlib
import csv
import io
import re
from dataclasses import dataclass
import inspect
import logging
from typing import Any, Iterable, List, Optional, Tuple, Literal, Dict

try:
    # Firebird 3.0+ driver
    from firebird.driver import connect as fb_connect  # type: ignore
    _HAS_FB_DRIVER = True
except Exception:
    _HAS_FB_DRIVER = False

try:
    # Firebird 2.5 driver
    import firebirdsql  # type: ignore
    _HAS_FIREBIRDSQL = True
except Exception:
    _HAS_FIREBIRDSQL = False

Engine = Literal["firebird-driver", "firebirdsql"]


@dataclass
class FBAuth:
    """Authentication parameters for connecting to a Firebird server."""
    user: str
    password: str
    role: Optional[str] = None


@dataclass
class FBConnParams:
    """
    Connection parameters for a Firebird server.

    :param host: Server hostname (can be None for local attachments).
    :param port: Server port number.
    :param database: Path to the database or alias name.
    :param charset: Character set to use; defaults to UTF8.
    :param auth: Optional authentication information.
    :param connect_timeout: Connection timeout in seconds.
    """
    host: str
    port: int
    database: str
    charset: str = "UTF8"
    auth: Optional[FBAuth] = None
    connect_timeout: Optional[int] = 15


class FirebirdError(RuntimeError):
    """Raised when connection or execution fails."""
    pass


class FirebirdConnection:
    """
    Unified connection wrapper.

    Use :meth:`open` to obtain a context-managed instance. Depending on the detected
    server version and installed drivers, it will use either ``firebird-driver``
    (for Firebird 3.0+) or ``firebirdsql`` (for Firebird 2.5).

    Methods:
      - ``query(sql, params=None)``: Return all rows from a SELECT.
      - ``execute(sql, params=None)``: Execute a DML statement and commit.
      - ``export_table_to_csv(table, stream, ...)``: Stream a table to CSV.
      - ``server_version()``: Return the server version string.
      - ``engine()``: Return the name of the underlying driver.
    """

    def __init__(self, params: FBConnParams, engine: Engine, raw_conn: Any):
        self._params = params
        self._engine = engine
        self._raw = raw_conn

    @classmethod
    def open(cls, params: FBConnParams) -> "FirebirdConnection":
        """
        Establish a connection to the database.

        The logic is:
          1. If ``firebird-driver`` is available, attempt to connect.
          2. Detect the server version. If it's 2.5, prefer ``firebirdsql``.
          3. If no suitable driver is installed, raise a :class:`FirebirdError`.
        """
        # Attempt firebird-driver first
        if _HAS_FB_DRIVER:
            # Try to connect using firebird-driver. If connect itself fails, fallback.
            try:
                raw = _connect_firebird_driver(params)
            except Exception:
                if _HAS_FIREBIRDSQL:
                    raw_sql = _connect_firebirdsql(params)
                    return cls(params, "firebirdsql", raw_sql)
                raise

            # Try to detect version, but do not fallback just because detection fails
            try:
                ver = _detect_server_version_driver(raw)
                if _is_25(ver):
                    # Prefer firebirdsql for 2.5 servers
                    if not _HAS_FIREBIRDSQL:
                        raise FirebirdError(
                            "Server is Firebird 2.5, but 'firebirdsql' is not installed. "
                            "Please install it via 'pip install firebirdsql'."
                        )
                    try:
                        raw.close()
                    except Exception:
                        pass
                    raw_sql = _connect_firebirdsql(params)
                    return cls(params, "firebirdsql", raw_sql)
            except Exception:
                # Ignore detection errors; proceed with firebird-driver
                pass

            return cls(params, "firebird-driver", raw)
        # No firebird-driver available; require firebirdsql
        if not _HAS_FIREBIRDSQL:
            raise FirebirdError(
                "Neither 'firebird-driver' nor 'firebirdsql' is installed. "
                "Install at least one driver: 'pip install firebird-driver' (FB 3-5) "
                "or 'pip install firebirdsql' (FB 2.5)."
            )
        raw_sql = _connect_firebirdsql(params)
        return cls(params, "firebirdsql", raw_sql)

    def __enter__(self) -> "FirebirdConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying connection."""
        try:
            self._raw.close()
        except Exception:
            pass

    def engine(self) -> Engine:
        """Return the driver in use."""
        return self._engine

    def server_version(self) -> str:
        """Return the server version string."""
        if self._engine == "firebird-driver":
            return _detect_server_version_driver(self._raw)
        return _detect_server_version_sql(self._raw)

    def query(self, sql: str, params: Optional[Iterable[Any]] = None) -> List[Tuple[Any, ...]]:
        """Execute a SELECT statement and return all rows."""
        if self._engine == "firebird-driver":
            return _query_driver(self._raw, sql, params)
        return _query_sql(self._raw, sql, params)

    def execute(self, sql: str, params: Optional[Iterable[Any]] = None) -> int:
        """Execute a DML statement and return the number of affected rows."""
        if self._engine == "firebird-driver":
            return _execute_driver(self._raw, sql, params)
        return _execute_sql(self._raw, sql, params)

    def export_table_to_csv(
        self,
        table: str,
        stream: io.TextIOBase,
        *,
        delimiter: str = ";",
        header: bool = True,
        batch_size: int = 5000,
        where: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> int:
        """
        Stream the entire table to a CSV file.

        :param table: Table name (unquoted identifier).
        :param stream: Text stream to write CSV to.
        :param delimiter: Field separator.
        :param header: Whether to write column names as the first row.
        :param batch_size: Number of rows to fetch per batch.
        :param where: Optional WHERE clause (without ``WHERE``).
        :param order_by: Optional ORDER BY clause (without ``ORDER BY``).
        :return: Number of rows exported.
        """
        if not _is_identifier(table):
            raise FirebirdError(f"Invalid table name: {table!r}")

        sql = f'SELECT * FROM "{table}"'
        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += f" ORDER BY {order_by}"

        if self._engine == "firebird-driver":
            return _export_driver(self._raw, sql, stream, delimiter, header, batch_size)
        return _export_sql(self._raw, sql, stream, delimiter, header, batch_size)


def _is_25(version_str: str) -> bool:
    """Return True if the version string indicates Firebird 2.5."""
    vs = version_str.strip().lower()
    return vs.startswith("2.5") or "firebird 2.5" in vs or "wi-v2.5" in vs


def _detect_server_version_driver(conn) -> str:
    """Detect the server version using firebird-driver."""
    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT rdb$get_context('SYSTEM','ENGINE_VERSION') FROM rdb$database")
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0]).strip()
        finally:
            cur.close()
    except Exception:
        pass
    # Fallback: MON$ATTACHMENTS
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT MON$SERVER_VERSION
                FROM MON$ATTACHMENTS
                WHERE MON$ATTACHMENT_ID = CURRENT_CONNECTION
                """
            )
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0]).strip()
        finally:
            cur.close()
    except Exception as e:
        raise FirebirdError(f"Could not determine server version (firebird-driver): {e}") from e
    return "unknown"


def _detect_server_version_sql(conn) -> str:
    """Detect the server version using firebirdsql."""
    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT rdb$get_context('SYSTEM','ENGINE_VERSION') FROM rdb$database")
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0]).strip()
        finally:
            cur.close()
    except Exception:
        pass
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT MON$SERVER_VERSION
                FROM MON$ATTACHMENTS
                WHERE MON$ATTACHMENT_ID = CURRENT_CONNECTION
                """
            )
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0]).strip()
        finally:
            cur.close()
    except Exception as e:
        raise FirebirdError(f"Could not determine server version (firebirdsql): {e}") from e
    return "unknown"


def _query_driver(conn, sql: str, params: Optional[Iterable[Any]]) -> List[Tuple]:
    cur = conn.cursor()
    try:
        cur.execute(sql, tuple(params) if params is not None else None)
        return cur.fetchall()
    finally:
        cur.close()


def _query_sql(conn, sql: str, params: Optional[Iterable[Any]]) -> List[Tuple]:
    cur = conn.cursor()
    try:
        cur.execute(sql, tuple(params) if params is not None else None)
        return cur.fetchall()
    finally:
        cur.close()


def _execute_driver(conn, sql: str, params: Optional[Iterable[Any]]) -> int:
    cur = conn.cursor()
    try:
        cur.execute(sql, tuple(params) if params is not None else None)
        conn.commit()
        return cur.rowcount if cur.rowcount is not None else 0
    finally:
        cur.close()


def _execute_sql(conn, sql: str, params: Optional[Iterable[Any]]) -> int:
    cur = conn.cursor()
    try:
        cur.execute(sql, tuple(params) if params is not None else None)
        conn.commit()
        return cur.rowcount if cur.rowcount is not None else 0
    finally:
        cur.close()


def _export_driver(
    conn,
    sql: str,
    stream: io.TextIOBase,
    delimiter: str,
    header: bool,
    batch_size: int,
) -> int:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        columns = [d[0] for d in cur.description]
        writer = csv.writer(stream, delimiter=delimiter, lineterminator="\n")
        if header:
            writer.writerow(columns)
        rows_written = 0
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            writer.writerows(rows)
            rows_written += len(rows)
        return rows_written
    finally:
        cur.close()


def _export_sql(
    conn,
    sql: str,
    stream: io.TextIOBase,
    delimiter: str,
    header: bool,
    batch_size: int,
) -> int:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        columns = [d[0] for d in cur.description]
        writer = csv.writer(stream, delimiter=delimiter, lineterminator="\n")
        if header:
            writer.writerow(columns)
        rows_written = 0
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            writer.writerows(rows)
            rows_written += len(rows)
        return rows_written
    finally:
        cur.close()


def _connect_firebird_driver(params: FBConnParams):
    if not _HAS_FB_DRIVER:
        raise FirebirdError("'firebird-driver' is not installed.")
    dsn = f"{params.host}/{params.port}:{params.database}"
    # Use positional DSN (matches many installs). Be conservative with kwargs for compatibility.
    # Try with (user, password, role, charset). If TypeError, retry without charset. If still TypeError,
    # try database/host/port keywords as a last resort.
    user = params.auth.user if params.auth else None
    pwd = params.auth.password if params.auth else None
    role = params.auth.role if params.auth else None

    try:
        kwargs: Dict[str, Any] = {}
        if user is not None:
            kwargs["user"] = user
        if pwd is not None:
            kwargs["password"] = pwd
        if role:
            kwargs["role"] = role
        # charset may not be supported on some builds; include first, then fallback
        kwargs_with_charset = dict(kwargs)
        kwargs_with_charset["charset"] = params.charset
        return fb_connect(dsn, **kwargs_with_charset)
    except TypeError:
        try:
            return fb_connect(dsn, **kwargs)
        except TypeError:
            # Last-resort: use separate keywords
            kw_alt: Dict[str, Any] = dict(database=params.database, host=params.host, port=params.port)
            kw_alt.update(kwargs)
            return fb_connect(**kw_alt)


def _connect_firebirdsql(params: FBConnParams):
    if not _HAS_FIREBIRDSQL:
        raise FirebirdError("'firebirdsql' is not installed.")
    kw: Dict[str, Any] = dict(
        host=params.host,
        port=params.port,
        database=params.database,
        charset=params.charset,
    )
    if params.auth:
        kw.update(user=params.auth.user, password=params.auth.password)
        if params.auth.role:
            kw.update(role=params.auth.role)
    if params.connect_timeout:
        kw.update(timeout=params.connect_timeout)
    return firebirdsql.connect(**kw)


def _is_identifier(name: str) -> bool:
    """Check if a string is a valid unquoted SQL identifier."""
    return all(ch.isalnum() or ch in ("_", "$") for ch in name)


def connect(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    *,
    role: Optional[str] = None,
    charset: str = "UTF8",
    timeout: int = 15,
) -> FirebirdConnection:
    """
    Convenience helper to quickly open a unified connection.

    :param host: Server hostname.
    :param port: Server port.
    :param database: Database file path or alias.
    :param user: Username.
    :param password: Password.
    :param role: Optional role.
    :param charset: Character set (default: UTF8).
    :param timeout: Connection timeout in seconds.
    :return: A :class:`FirebirdConnection` instance.
    """
    params = FBConnParams(
        host=host, port=port, database=database,
        charset=charset, connect_timeout=timeout,
        auth=FBAuth(user=user, password=password, role=role),
    )
    return FirebirdConnection.open(params)


def open_dsn(
    dsn: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    *,
    role: Optional[str] = None,
    charset: str = "UTF8",
    timeout: int = 15,
) -> FirebirdConnection:
    """
    Convenience helper to open a connection from a classic DSN string.

    A DSN may be one of the following forms:

      - ``C:\\path\\db.fdb`` (local path)
      - ``localhost:C:\\path\\db.fdb`` (host + path)
      - ``localhost/3050:C:\\path\\db.fdb`` (host/port + path)

    :param dsn: Classic Firebird DSN.
    :param user: Username (optional).
    :param password: Password (optional).
    :param role: Optional role.
    :param charset: Character set (default: UTF8).
    :param timeout: Connection timeout in seconds.
    :return: A :class:`FirebirdConnection` instance.
    """
    # Parse classic DSN
    host: Optional[str]
    port: Optional[int]
    database: str
    if re.match(r"^[A-Za-z]:\\", dsn):  # Windows local path
        host, port, database = None, None, dsn
    elif ":" in dsn:
        host_part, database = dsn.split(":", 1)
        if "/" in host_part:
            host, port_str = host_part.split("/", 1)
            try:
                port = int(port_str)
            except ValueError:
                port = None
        else:
            host = host_part
            port = None
    else:
        host, port, database = None, None, dsn
    # Default host/port
    conn_host = host or "localhost"
    conn_port = port or 3050
    auth = FBAuth(user=user or "SYSDBA", password=password or "", role=role) if user or password or role else None
    params = FBConnParams(host=conn_host, port=conn_port, database=database, charset=charset, auth=auth, connect_timeout=timeout)
    return FirebirdConnection.open(params)

