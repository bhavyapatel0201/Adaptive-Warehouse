"""Adaptive partition optimization.

The proposal's second optimization axis: physical design that adapts its
*partitioning*, not just its indexes, in response to what the workload and
the ETL stream are actually doing.

The problem this solves
-----------------------
A range-partitioned fact table is defined over a fixed set of boundaries. A
continuously-ingesting ETL pipeline eventually writes rows past the newest
boundary, and those rows land in the DEFAULT catch-all partition. That is bad
for three reasons, the third of which is genuinely blocking:

  1. The default partition grows without bound, so it stops behaving like a
     partitioned table at all -- no pruning *within* it, and none of the
     operational wins (cheap drops, per-partition vacuum/index maintenance).
  2. Its statistics get worse as it becomes a mixed bag of date ranges.
  3. **It blocks the fix.** Once the default partition holds rows for a given
     range, PostgreSQL refuses to attach a proper partition covering that
     range:

         ERROR: updated partition constraint for default partition
                "fact_sales_default" would be violated by some row

     So the warehouse gets stuck: the longer you leave it, the more data has
     to be migrated to recover, and the migration needs a lock.

Two mechanisms
--------------
* **Proactive provisioning** (preferred). Watch how close the ingestion
  frontier -- the maximum partition-key value present -- is to the newest
  boundary. When it gets close, create the next partition *before* any row
  needs it. This is pure DDL on an empty table: instant, no data movement, no
  meaningful lock. Nothing ever reaches the default partition.

* **Reactive drain** (recovery). If rows are already sitting in the default
  partition, migrate them into a properly-bounded partition and attach it.
  This one does move data and does need an exclusive lock, so it is guarded
  by the same lock_timeout as index DDL and is deliberately the fallback
  path, not the primary one.

The asymmetry between these two is itself a finding worth reporting:
adaptation that anticipates the workload is nearly free, while adaptation
that reacts to a problem already in progress is expensive and contends with
the very ETL stream that caused it.
"""
import time

from src.config import ADAPTIVE

# Partition-key layout: date_key is an integer surrogate in YYYYMMDD form,
# and partitions are yearly, so year Y spans [Y*10000+0101, (Y+1)*10000+0101).
def year_bounds(year: int):
    return year * 10000 + 101, (year + 1) * 10000 + 101


def year_of_key(date_key: int) -> int:
    return date_key // 10000


def partition_name(parent: str, year: int) -> str:
    return f"{parent}_{year}"


def _apply_guards(cur):
    timeout = ADAPTIVE["ddl_timeout_ms"]
    cur.execute(f"SET lock_timeout = {timeout};")
    cur.execute(f"SET statement_timeout = {timeout * 12};")


# ---------------------------------------------------------------------
# Inspection
# ---------------------------------------------------------------------
def get_partitions(cur, parent: str):
    """Return each partition with its parsed range bounds.

    pg_get_expr gives the bound as text like
        FOR VALUES FROM ('20240101') TO ('20250101')
    or  DEFAULT
    so we parse the integers back out rather than guessing from the name.
    """
    cur.execute("""
        SELECT c.relname,
               pg_get_expr(c.relpartbound, c.oid) AS bound,
               COALESCE(s.n_live_tup, 0)          AS live_rows
        FROM pg_inherits i
        JOIN pg_class c  ON c.oid = i.inhrelid
        JOIN pg_class p  ON p.oid = i.inhparent
        LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
        WHERE p.relname = %s
        ORDER BY c.relname
    """, (parent,))
    out = []
    for name, bound, rows in cur.fetchall():
        entry = {"name": name, "bound": bound, "rows": rows,
                 "is_default": False, "lower": None, "upper": None}
        if bound and "DEFAULT" in bound.upper():
            entry["is_default"] = True
        elif bound:
            digits = []
            token = ""
            for ch in bound:
                if ch.isdigit():
                    token += ch
                elif token:
                    digits.append(int(token)); token = ""
            if token:
                digits.append(int(token))
            if len(digits) >= 2:
                entry["lower"], entry["upper"] = digits[0], digits[1]
        out.append(entry)
    return out


def get_ingestion_frontier(cur, parent: str, key_col: str = "date_key"):
    """Highest partition-key value currently present (how far ETL has reached)."""
    cur.execute(f"SELECT max({key_col}) FROM {parent}")
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def get_default_contents(cur, parent: str, key_col: str = "date_key"):
    """Which years have rows stranded in the default partition, and how many."""
    parts = get_partitions(cur, parent)
    default = next((p for p in parts if p["is_default"]), None)
    if default is None:
        return {}
    cur.execute(f"""
        SELECT ({key_col} / 10000) AS yr, count(*)
        FROM {default['name']}
        GROUP BY 1 ORDER BY 1
    """)
    return {int(y): int(n) for y, n in cur.fetchall()}


def covered_years(cur, parent: str):
    """Years already covered by a real (non-default) partition."""
    years = set()
    for p in get_partitions(cur, parent):
        if p["is_default"] or p["lower"] is None:
            continue
        years.add(year_of_key(p["lower"]))
    return years


# ---------------------------------------------------------------------
# Mechanism 1: proactive provisioning
# ---------------------------------------------------------------------
def needs_provisioning(cur, parent: str, key_col: str = "date_key",
                       lead_days: int = None):
    """Return the year that should be provisioned next, or None.

    Triggers when the ingestion frontier comes within `lead_days` of the end
    of the newest covered year. Provisioning early is the whole point: an
    empty partition costs nothing to create, while draining a full default
    partition later is expensive.
    """
    if lead_days is None:
        lead_days = ADAPTIVE.get("partition_lead_days", 45)

    frontier = get_ingestion_frontier(cur, parent, key_col)
    if frontier is None:
        return None

    years = covered_years(cur, parent)
    if not years:
        return None
    newest = max(years)

    # Approximate "days remaining in the newest covered year" using the
    # YYYYMMDD layout: good enough for a threshold, and avoids date parsing.
    frontier_year = year_of_key(frontier)
    if frontier_year > newest:
        return newest + 1                      # already past the frontier

    frontier_month = (frontier // 100) % 100
    frontier_day = frontier % 100
    days_elapsed = (frontier_month - 1) * 30 + frontier_day
    days_remaining = 360 - days_elapsed

    if frontier_year == newest and days_remaining <= lead_days:
        return newest + 1
    return None


def provision_partition(conn, parent: str, year: int):
    """Create and attach an EMPTY partition for `year`. Returns duration_ms.

    Cheap by construction: the new partition holds no rows, so ATTACH does
    not have to scan or validate anything, and the catalog lock is brief.
    """
    lower, upper = year_bounds(year)
    name = partition_name(parent, year)
    started = time.time()
    with conn.cursor() as cur:
        _apply_guards(cur)
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{name}" '
                    f'(LIKE "{parent}" INCLUDING ALL)')
        cur.execute(f'ALTER TABLE "{parent}" ATTACH PARTITION "{name}" '
                    f'FOR VALUES FROM ({lower}) TO ({upper})')
    return (time.time() - started) * 1000.0


def ensure_date_dimension(conn, year: int, dim_table: str = "dim_date"):
    """Extend the date dimension to cover `year`.

    Not strictly partition management, but the fact table has a foreign key
    to dim_date, so ETL writing into a newly provisioned year fails outright
    unless the dimension covers it too. Real warehouses maintain the date
    dimension ahead of ingestion for exactly this reason.
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {dim_table} WHERE year = %s", (year,))
        if cur.fetchone()[0] > 0:
            return 0
        cur.execute(f"""
            INSERT INTO {dim_table}
                (date_key, full_date, day, month, quarter, year,
                 day_of_week, is_weekend, season)
            SELECT to_char(d,'YYYYMMDD')::int, d::date,
                   extract(day from d)::smallint,
                   extract(month from d)::smallint,
                   extract(quarter from d)::smallint,
                   extract(year from d)::smallint,
                   (extract(isodow from d) - 1)::smallint,
                   extract(isodow from d) >= 6,
                   CASE WHEN extract(month from d) IN (12,1,2) THEN 'Winter'
                        WHEN extract(month from d) IN (3,4,5)  THEN 'Spring'
                        WHEN extract(month from d) IN (6,7,8)  THEN 'Summer'
                        ELSE 'Fall' END
            FROM generate_series(
                make_date(%s,1,1), make_date(%s,12,31), interval '1 day') d
            ON CONFLICT (date_key) DO NOTHING
        """, (year, year))
        return cur.rowcount


# ---------------------------------------------------------------------
# Mechanism 2: reactive drain of the default partition
# ---------------------------------------------------------------------
def drain_default(conn, parent: str, year: int, key_col: str = "date_key"):
    """Migrate `year`'s rows out of the default partition into a real one.

    This is the recovery path, and it is deliberately more expensive than
    provisioning: rows must be copied out of the default partition and
    deleted from it before the new partition can be attached, and that work
    contends with the ETL stream still writing to the same table.

    Runs as a single transaction so a failure mid-migration cannot leave rows
    duplicated across two partitions.
    """
    lower, upper = year_bounds(year)
    name = partition_name(parent, year)

    started = time.time()
    moved = 0
    # Explicit transaction: autocommit must be off for the swap to be atomic.
    prev_autocommit = conn.autocommit
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            _apply_guards(cur)
            cur.execute("""
                SELECT c.relname FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_class p ON p.oid = i.inhparent
                WHERE p.relname = %s
                  AND pg_get_expr(c.relpartbound, c.oid) ILIKE '%%DEFAULT%%'
            """, (parent,))
            row = cur.fetchone()
            if row is None:
                raise RuntimeError(f"{parent} has no DEFAULT partition to drain")
            default_name = row[0]

            cur.execute(f'CREATE TABLE IF NOT EXISTS "{name}" '
                        f'(LIKE "{parent}" INCLUDING ALL)')
            cur.execute(f'''
                WITH moved AS (
                    DELETE FROM "{default_name}"
                    WHERE {key_col} >= %s AND {key_col} < %s
                    RETURNING *
                )
                INSERT INTO "{name}"
                OVERRIDING SYSTEM VALUE
                SELECT * FROM moved
            ''', (lower, upper))
            moved = cur.rowcount
            cur.execute(f'ALTER TABLE "{parent}" ATTACH PARTITION "{name}" '
                        f'FOR VALUES FROM ({lower}) TO ({upper})')
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = prev_autocommit
    return moved, (time.time() - started) * 1000.0


# ---------------------------------------------------------------------
# Mechanism 3: hot-partition awareness (bounding index overhead)
# ---------------------------------------------------------------------
def get_hot_partitions(cur, parent: str, baseline=None, min_share: float = 0.05):
    """Partitions the workload is actually touching, by scan activity.

    Used to avoid building adaptive indexes on cold historical partitions.
    Every index on a partition is a structure the ETL stream has to maintain
    on write, so indexing partitions nobody queries is pure overhead -- which
    is precisely the cost the proposal says adaptation must keep constrained.

    `baseline` is an optional {name: (seq, idx)} snapshot so activity is
    measured over the current run rather than the table's whole lifetime.
    """
    cur.execute("""
        SELECT c.relname,
               COALESCE(s.seq_scan,0) + COALESCE(s.idx_scan,0) AS scans
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
        WHERE p.relname = %s
    """, (parent,))
    rows = cur.fetchall()

    activity = {}
    for name, scans in rows:
        if baseline and name in baseline:
            b_seq, b_idx = baseline[name]
            scans = max(0, scans - (b_seq + b_idx))
        activity[name] = scans

    total = sum(activity.values())
    if total == 0:
        return sorted(activity)               # no signal: treat all as hot
    return sorted([n for n, s in activity.items() if s / total >= min_share])


def snapshot_partition_activity(cur, parent: str):
    """Baseline for get_hot_partitions deltas."""
    cur.execute("""
        SELECT c.relname, COALESCE(s.seq_scan,0), COALESCE(s.idx_scan,0)
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
        WHERE p.relname = %s
    """, (parent,))
    return {n: (seq, idx) for n, seq, idx in cur.fetchall()}
