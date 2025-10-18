import csv, logging, subprocess, shutil, os, re
from typing import Optional
from .dbapi import connect_unified
from firebird.driver import connect

logger = logging.getLogger(__name__)


def stream_query_to_csv(
    dsn: str, sql: str, output: str, chunk_size: int = 10_000,
    delimiter: str = ",", quotechar: str = '"', header: bool = True,
    user: Optional[str]=None, password: Optional[str]=None
) -> int:
    rowcount = 0
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

def quick_health_summary(dsn: str) -> dict:
    """Return a minimal health summary.
    NOTE: MVP: extend with MON$ tables & OIT/OAT later.
    """
    summary = {}
    with connect(dsn) as con:
        cur = con.cursor()
        try:
            cur.execute("""
                SELECT
                  rdb$get_context('SYSTEM','DB_NAME') as DB_NAME,
                  (SELECT COUNT(*) FROM RDB$RELATIONS) as TABLES
            """)
            row = cur.fetchone()
            if row:
                summary["db_name"] = row[0]
                summary["tables"] = int(row[1])
        except Exception as e:
            logger.warning("Could not fetch basic metadata: %s", e)

    return summary


_MULTI_VALUES_RE = re.compile(
    r"^\s*INSERT\s+INTO\s+([A-Z0-9_\$]+)\s*\(([^)]+)\)\s*VALUES\s*(\(.+\))\s*;\s*$",
    re.IGNORECASE | re.DOTALL,
)

def _expand_multi_values_insert(sql: str) -> list[str]:
    """
    Nimmt ein INSERT mit mehreren Value-Tuples und erzeugt Einzel-INSERTs.
    Beispiel (nicht von Firebird unterstützt):
      INSERT INTO t (a,b) VALUES (1,2), (3,4);
    => 2 Statements.
    """
    m = _MULTI_VALUES_RE.match(sql.strip().rstrip(";") + ";")
    if not m:
        return [sql]
    table = m.group(1)
    cols = m.group(2)
    values_block = m.group(3)

    # split naiv an "),", aber Klammern erhalten
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

def execute_sql(dsn: str, sql: str, allow_multi_values: bool=False,
                user: Optional[str]=None, password: Optional[str]=None) -> int:
    # (Multi-Values-Splitting wie zuvor …)
    raw_stmts = [s.strip() for s in sql.split(";") if s.strip()]
    stmts = [st+";" for st in raw_stmts]
    con, driver = connect_unified(dsn, user, password)
    try:
        cur = con.cursor()
        for st in stmts:
            cur.execute(st)
        con.commit()
        logger.info("Executed %d statements via %s", len(stmts), driver)
        return len(stmts)
    finally:
        try: con.close()
        except Exception: pass

def execute_sql_file(dsn: str, path: str, allow_multi_values: bool = False) -> int:
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    return execute_sql(dsn, sql, allow_multi_values=allow_multi_values)

def detect_server_version(dsn: str, user: Optional[str]=None, password: Optional[str]=None) -> str:
    # 1) Versuch: firebird-driver (FB>=3)
    try:
        from firebird.driver import connect as fb_connect
        with fb_connect(dsn, user=user, password=password) as con:
            cur = con.cursor()
            try:
                cur.execute("SELECT rdb$get_context('SYSTEM','ENGINE_VERSION') FROM RDB$DATABASE")
                row = cur.fetchone()
                if row and row[0]:
                    return str(row[0]).strip()
            except Exception:
                pass
            info = getattr(con, "info", None)
            if info and getattr(info, "server_version", None):
                m = re.search(r"(\d+\.\d+(\.\d+)?)", str(info.server_version))
                if m:
                    return m.group(1)
    except Exception:
        pass

    # 2) Fallback (FB 2.5 etc.): fbsvcmgr info_server_version
    svc = _service_target_from_dsn(dsn)  # host:service_mgr
    fbsvcmgr = _find_fbsvcmgr()
    cmd = [fbsvcmgr, svc]
    if user: cmd += ["user", user]
    if password: cmd += ["password", password]
    cmd += ["info_server_version"]
    try:
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT, timeout=10)
        m = re.search(r"Firebird\s+(\d+\.\d+(\.\d+)?)", out)
        if m:
            return m.group(1)
    except Exception:
        return "0.0.0"
    return "0.0.0"

# viel zu simple gedacht -> überarbeiten!!!!!
def _find_fbsvcmgr() -> str:
    p = shutil.which("fbsvcmgr")
    if p: return p
    candidates = [
        r"C:\Program Files\Firebird\Firebird_5_0\fbsvcmgr.exe",
        r"C:\Program Files\Firebird\Firebird_4_0\fbsvcmgr.exe",
        r"C:\Program Files\Firebird\Firebird_3_0\fbsvcmgr.exe",
        r"C:\Program Files\Firebird\Firebird_2_5\fbsvcmgr.exe",
        "/opt/firebird/bin/fbsvcmgr", "/usr/bin/fbsvcmgr", "/usr/local/firebird/bin/fbsvcmgr",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    return "fbsvcmgr"

def server_major(version_str: str) -> int:
    m = re.match(r"^\s*(\d+)", version_str or "")
    return int(m.group(1)) if m else 0

def _parse_dsn(dsn: str):
    """
    Unterstützt:
      - 'C:\\path\\db.fdb' (lokal, XNET)
      - 'localhost:C:\\path\\db.fdb'
      - 'localhost/3053:C:\\path\\db.fdb'
    Gibt (host, port, path) zurück. Bei lokalem Attach: (None, None, fullpath)
    """
    # Windows-Lokalpfad (z. B. 'C:\...') enthält ':\' – NICHT mit host:db verwechseln
    if re.search(r'^[A-Za-z]:\\', dsn):
        return (None, None, dsn)
    # host[:/port]:path
    if ':' in dsn:
        hostpart, path = dsn.split(':', 1)
        if '/' in hostpart:
            host, port = hostpart.split('/', 1)
        else:
            host, port = hostpart, None
        return (host or None, port or None, path)
    # Fallback: behandeln wie lokal
    return (None, None, dsn)


def _service_target_from_dsn(dsn: str) -> str:
    """
    Baut das fbsvcmgr-Ziel:
      - lokal/XNET: 'service_mgr'
      - TCP ohne Port: 'host:service_mgr'
      - TCP mit Port:  'host/port:service_mgr'
    """
    host, port, _ = _parse_dsn(dsn)
    if host is None:
        return 'service_mgr'
    if port:
        return f'{host}/{port}:service_mgr'
    return f'{host}:service_mgr'