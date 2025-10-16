import csv
import logging
from firebird.driver import connect

logger = logging.getLogger(__name__)

def stream_query_to_csv(dsn: str, sql: str, output: str, chunk_size: int = 10_000,
                        delimiter: str = ",", quotechar: str = '"', header: bool = True) -> int:
    """Stream a query to CSV without loading everything into memory.
    Returns number of rows written.
    """
    rowcount = 0
    with connect(dsn) as con:
        cur = con.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]

        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=delimiter, quotechar=quotechar)
            if header:
                writer.writerow(cols)
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                writer.writerows(rows)
                rowcount += len(rows)
                logger.debug("wrote %d rows (total=%d)", len(rows), rowcount)
    logger.info("CSV export done: %s rows -> %s", rowcount, output)
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
