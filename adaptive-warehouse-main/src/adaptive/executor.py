"""Guarded DDL execution for the adaptive optimizer.

This is the component the proposal's Risks section is about. Issuing index
DDL against a table that is simultaneously serving OLAP queries and absorbing
ETL writes is exactly how an "optimizer" turns into an outage. Every
safeguard here maps to a stated mitigation:

  * CREATE INDEX CONCURRENTLY   -- never take an exclusive lock that blocks
                                   readers or the ETL stream.
  * lock_timeout                -- if the lock cannot be acquired quickly,
                                   abort the adaptation instead of queueing
                                   behind (and blocking) other work.
  * statement_timeout           -- bound the total build time.
  * index-count cap             -- bound cumulative write amplification, so
                                   the "cure" cannot become the disease.

Partitioned tables need special handling
----------------------------------------
PostgreSQL rejects CREATE INDEX CONCURRENTLY on a partitioned parent:

    ERROR: cannot create index on partitioned table "fact_sales" concurrently

The supported pattern, implemented in `_create_partitioned_concurrently`, is:

  1. build the index CONCURRENTLY on every leaf partition,
  2. create an index on ONLY the parent (fast, metadata-only, briefly locked),
  3. ATTACH each leaf index to the parent index.

Once every partition is attached the parent index automatically becomes
valid. This keeps the whole operation non-blocking apart from step 2's very
short catalog lock.
"""
import time

from src.config import ADAPTIVE

ADAPTIVE_PREFIX = "adx_"          # marks indexes this engine created


def index_name(table: str, column: str) -> str:
    """Deterministic name so we can recognise and clean up our own indexes."""
    return f"{ADAPTIVE_PREFIX}{table}_{column}"[:63]


def _apply_guards(cur):
    """Bound how long any adaptation step may block."""
    timeout = ADAPTIVE["ddl_timeout_ms"]
    cur.execute(f"SET lock_timeout = {timeout};")
    cur.execute(f"SET statement_timeout = {timeout * 12};")


def _is_partitioned(cur, table: str) -> bool:
    cur.execute("SELECT relkind = 'p' FROM pg_class WHERE relname = %s", (table,))
    row = cur.fetchone()
    return bool(row and row[0])


def _leaf_partitions(cur, table: str):
    cur.execute("""
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = %s
        ORDER BY c.relname
    """, (table,))
    return [r[0] for r in cur.fetchall()]


def _create_plain_concurrently(cur, table, column, name):
    cur.execute(f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{name}" '
                f'ON "{table}" ("{column}")')


def _create_partitioned_concurrently(cur, table, column, name):
    """Three-step non-blocking build for a partitioned parent table."""
    leaves = _leaf_partitions(cur, table)
    if not leaves:
        raise RuntimeError(f"'{table}' is partitioned but has no partitions")

    leaf_indexes = []
    for leaf in leaves:
        leaf_idx = f"{name}_{leaf}"[:63]
        cur.execute(f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{leaf_idx}" '
                    f'ON "{leaf}" ("{column}")')
        leaf_indexes.append(leaf_idx)

    # Parent index starts invalid; attaching all children validates it.
    cur.execute(f'CREATE INDEX IF NOT EXISTS "{name}" '
                f'ON ONLY "{table}" ("{column}")')
    for leaf_idx in leaf_indexes:
        cur.execute(f'ALTER INDEX "{name}" ATTACH PARTITION "{leaf_idx}"')


def create_index(conn, table: str, column: str):
    """Create an adaptive index. Returns (name, duration_ms).

    Raises on failure; the caller decides whether that is fatal or just a
    skipped adaptation. The connection MUST be in autocommit mode, because
    CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    """
    name = index_name(table, column)
    started = time.time()
    with conn.cursor() as cur:
        _apply_guards(cur)
        if _is_partitioned(cur, table):
            _create_partitioned_concurrently(cur, table, column, name)
        else:
            _create_plain_concurrently(cur, table, column, name)
        cur.execute("ANALYZE %s" % f'"{table}"')
    return name, (time.time() - started) * 1000.0


def drop_index(conn, name: str):
    """Drop an adaptive index. Returns duration_ms.

    Partitioned (parent) indexes cannot be dropped CONCURRENTLY -- PostgreSQL
    rejects it -- so those fall back to a plain DROP, which is fast because it
    is a catalog operation. Dropping the parent automatically removes its
    attached leaf indexes.
    """
    started = time.time()
    with conn.cursor() as cur:
        _apply_guards(cur)
        cur.execute("SELECT relkind FROM pg_class WHERE relname = %s", (name,))
        row = cur.fetchone()
        if row is None:
            return 0.0                      # already gone
        is_partitioned_index = (row[0] == "I")
        if is_partitioned_index:
            cur.execute(f'DROP INDEX IF EXISTS "{name}"')
        else:
            cur.execute(f'DROP INDEX CONCURRENTLY IF EXISTS "{name}"')
    return (time.time() - started) * 1000.0


def list_adaptive_indexes(cur):
    """All indexes this engine created, with their usage counts.

    Only parent/standalone indexes are returned -- the per-partition leaf
    indexes are an implementation detail and are dropped with their parent.
    """
    cur.execute("""
        SELECT i.relname AS index_name,
               t.relname AS table_name,
               COALESCE(s.idx_scan, 0) AS scans
        FROM pg_class i
        JOIN pg_index x     ON x.indexrelid = i.oid
        JOIN pg_class t     ON t.oid = x.indrelid
        JOIN pg_namespace n ON n.oid = i.relnamespace
        LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
        WHERE n.nspname = 'public'
          AND i.relname LIKE %s
          AND i.relname NOT LIKE %s
        ORDER BY scans ASC
    """, (ADAPTIVE_PREFIX + "%", ADAPTIVE_PREFIX + "%\\_fact\\_sales\\_%"))
    return [{"index": r[0], "table": r[1], "scans": r[2]} for r in cur.fetchall()]


def drop_all_adaptive_indexes(conn, verbose: bool = True):
    """Remove every index created by the engine, restoring the static baseline.

    Called between experiment runs so each adaptive run starts from the same
    physical design as its static counterpart -- otherwise later runs would
    inherit earlier runs' indexes and the comparison would be meaningless.
    """
    dropped = 0
    with conn.cursor() as cur:
        # Order matters: drop parent/standalone indexes first. Dropping a
        # partitioned parent index automatically removes the leaf indexes
        # attached to it, and leaves cannot be dropped on their own.
        cur.execute("""
            SELECT i.relname, i.relkind
            FROM pg_class i
            JOIN pg_namespace n ON n.oid = i.relnamespace
            WHERE n.nspname = 'public' AND i.relname LIKE %s
            ORDER BY CASE WHEN i.relkind = 'I' THEN 0 ELSE 1 END
        """, (ADAPTIVE_PREFIX + "%",))
        names = [r[0] for r in cur.fetchall()]

    for name in names:
        # A leaf may already have vanished with its parent.
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_class WHERE relname = %s", (name,))
            if cur.fetchone() is None:
                continue
        try:
            drop_index(conn, name)
            dropped += 1
        except Exception as exc:
            if verbose:
                print(f"  [drop warning] {name}: {exc}")
    if verbose and dropped:
        print(f"  reset: dropped {dropped} adaptive index object(s)")
    return dropped
