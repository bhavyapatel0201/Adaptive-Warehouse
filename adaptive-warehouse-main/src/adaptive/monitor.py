"""Workload monitor: turns raw database telemetry into indexing candidates.

This is the "Closed-Loop Monitoring" half of the optimization kernel described
in the proposal. It reads two catalog sources and combines them:

  1. pg_stat_statements  -- normalized query text + call counts + exec time.
     We parse the SQL to find which (table, column) pairs appear in WHERE
     predicates, and weight each pair by the total execution time of the
     queries that use it. Time-weighting matters: a column used by one very
     slow query is a better index candidate than one used by many fast ones.

  2. pg_stat_user_tables -- sequential vs index scan counts per table.
     A table already served well by indexes is a poor candidate even if its
     columns appear in predicates.

The SQL parsing is deliberately regex-based rather than a full parser. It
handles the query shapes this project generates (single-level SELECT with
explicit aliases and JOINs) and simply ignores anything it cannot resolve,
which is the safe failure mode -- an unrecognised predicate yields no
candidate rather than a wrong one.
"""
import re
from collections import defaultdict

# `FROM tablename alias` / `JOIN tablename alias`  (alias optional, AS optional)
_ALIAS_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([a-z_][a-z0-9_]*)\s+(?:AS\s+)?([a-z_][a-z0-9_]*)?",
    re.IGNORECASE,
)

# Everything between WHERE and the next clause keyword.
_WHERE_RE = re.compile(
    r"\bWHERE\b(.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|\bHAVING\b|$)",
    re.IGNORECASE | re.DOTALL,
)

# `alias.column <comparison>` inside a WHERE clause.
_PREDICATE_RE = re.compile(
    r"\b([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)\s*(?:=|>=|<=|>|<|<>|!=|LIKE|IN|BETWEEN)",
    re.IGNORECASE,
)

# Join conditions look like `a.col = b.col`; those are already covered by the
# baseline FK indexes and shouldn't be treated as filter predicates.
_JOIN_COND_RE = re.compile(
    r"\b[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\s*=\s*[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*",
    re.IGNORECASE,
)

# Reserved words that can appear where an alias would be, and must not be
# mistaken for one.
_NOT_ALIASES = {"on", "where", "group", "order", "limit", "having", "join",
                "inner", "left", "right", "full", "outer", "as", "using"}


def parse_predicate_columns(sql: str):
    """Return the set of (table, column) pairs used as filters in `sql`."""
    # Map alias -> real table name.
    alias_map = {}
    for table, alias in _ALIAS_RE.findall(sql):
        if alias and alias.lower() not in _NOT_ALIASES:
            alias_map[alias.lower()] = table.lower()
        alias_map.setdefault(table.lower(), table.lower())  # unaliased use

    found = set()
    for where_body in _WHERE_RE.findall(sql):
        # Strip pure join conditions so we only keep real filters.
        filters_only = _JOIN_COND_RE.sub(" ", where_body)
        for alias, column in _PREDICATE_RE.findall(filters_only):
            table = alias_map.get(alias.lower())
            if table:
                found.add((table, column.lower()))
    return found


def get_table_scan_stats(cur):
    """Per-table sequential vs index scan counts.

    Row counts come from pg_class.reltuples (maintained by ANALYZE) rather
    than pg_stat_user_tables.n_live_tup, because the latter resets to zero
    whenever statistics are reset -- which would make every table look tiny
    and suppress all candidates.
    """
    cur.execute("""
        SELECT s.relname,
               COALESCE(s.seq_scan, 0)  AS seq_scan,
               COALESCE(s.idx_scan, 0)  AS idx_scan,
               GREATEST(COALESCE(c.reltuples, 0), 0)::bigint AS est_rows
        FROM pg_stat_user_tables s
        JOIN pg_class c ON c.oid = s.relid
    """)
    stats = {}
    for relname, seq_scan, idx_scan, est_rows in cur.fetchall():
        total = seq_scan + idx_scan
        stats[relname] = {
            "seq_scan": seq_scan,
            "idx_scan": idx_scan,
            "rows": est_rows,
            "seq_ratio": (seq_scan / total) if total else 0.0,
        }

    # Roll leaf-partition activity up into the partitioned parent.
    #
    # PostgreSQL records scans against the individual partitions, not the
    # parent, so a partitioned fact table always reports seq_scan = 0 and
    # therefore a sequential-scan ratio of zero -- making it look perfectly
    # optimized even while every partition is being scanned end to end.
    # Queries are written against the parent, so that is the name the SQL
    # parser produces and the level at which indexes must be created.
    cur.execute("""
        SELECT p.relname AS parent, c.relname AS child
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        JOIN pg_namespace n ON n.oid = p.relnamespace
        WHERE n.nspname = 'public' AND p.relkind = 'p'
    """)
    children = {}
    for parent, child in cur.fetchall():
        children.setdefault(parent, []).append(child)

    for parent, kids in children.items():
        seq = sum(stats[k]["seq_scan"] for k in kids if k in stats)
        idx = sum(stats[k]["idx_scan"] for k in kids if k in stats)
        rows = sum(stats[k]["rows"] for k in kids if k in stats)
        total = seq + idx
        stats[parent] = {
            "seq_scan": seq,
            "idx_scan": idx,
            "rows": max(rows, stats.get(parent, {}).get("rows", 0)),
            "seq_ratio": (seq / total) if total else 0.0,
        }
    return stats


def snapshot_scan_stats(cur):
    """Capture current scan counters, to be used as a delta baseline.

    pg_stat_user_tables counters are cumulative since the database started.
    Using them raw makes the engine reason about the table's entire history
    instead of the workload running right now -- a table that was heavily
    index-scanned last week will look healthy even while it is sequentially
    scanning every query today. Taking a snapshot at engine start and
    measuring the delta fixes that, without destroying statistics that other
    tooling may rely on (as a global pg_stat_reset() would).
    """
    return {name: (s["seq_scan"], s["idx_scan"])
            for name, s in get_table_scan_stats(cur).items()}


def get_scan_stats_delta(cur, baseline):
    """Scan stats measured relative to a `snapshot_scan_stats` baseline."""
    current = get_table_scan_stats(cur)
    out = {}
    for name, s in current.items():
        base_seq, base_idx = baseline.get(name, (0, 0))
        seq = max(0, s["seq_scan"] - base_seq)
        idx = max(0, s["idx_scan"] - base_idx)
        total = seq + idx
        out[name] = {
            "seq_scan": seq,
            "idx_scan": idx,
            "rows": s["rows"],
            "seq_ratio": (seq / total) if total else 0.0,
        }
    return out


def get_column_selectivity(cur):
    """Estimate what fraction of rows a filter on each column would match.

    Uses the planner's own statistics (pg_stats.n_distinct), so the engine
    reasons about selectivity the same way the optimizer does:

        n_distinct > 0  -> absolute count of distinct values
        n_distinct < 0  -> distinct values as a negative fraction of rows
        n_distinct = 0  -> unknown (not yet analyzed)

    This matters because an index on a column with a handful of values
    (a 4-value `season`, an 8-value `brand`) cannot beat a sequential scan --
    the planner will correctly ignore it -- yet the index still costs storage
    and slows every INSERT from the ETL stream. Filtering these out is what
    keeps adaptation overhead constrained rather than merely additive.
    """
    cur.execute("""
        SELECT s.tablename, s.attname, s.n_distinct,
               GREATEST(COALESCE(c.reltuples, 0), 1)::bigint AS est_rows
        FROM pg_stats s
        JOIN pg_class c ON c.relname = s.tablename
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.schemaname
        WHERE s.schemaname = 'public'
    """)
    sel = {}
    for table, column, n_distinct, est_rows in cur.fetchall():
        if n_distinct is None or n_distinct == 0:
            frac = None                        # unknown; treated as neutral
        elif n_distinct < 0:
            # Negative means "distinct values as a fraction of row count", so
            # -0.43 describes a column where 43% of rows hold a distinct value
            # -- a highly selective column, not an unselective one. The number
            # of distinct values is |n_distinct| * rows, and an equality
            # filter matches roughly one of them.
            distinct_count = max(1.0, abs(n_distinct) * est_rows)
            frac = 1.0 / distinct_count
        else:
            frac = 1.0 / n_distinct            # equality hits ~1/n of rows
        sel[(table.lower(), column.lower())] = frac
    return sel


def get_existing_indexes(cur):
    """Return {(table, column): indexname} for all single-column indexes."""
    cur.execute("""
        SELECT t.relname            AS table_name,
               a.attname            AS column_name,
               i.relname            AS index_name
        FROM pg_index x
        JOIN pg_class i     ON i.oid = x.indexrelid
        JOIN pg_class t     ON t.oid = x.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_attribute a ON a.attrelid = t.oid
                           AND a.attnum = x.indkey[0]
        WHERE n.nspname = 'public'
          AND x.indnatts = 1
    """)
    return {(t.lower(), c.lower()): i for t, c, i in cur.fetchall()}


def collect_candidates(cur, min_calls: int = 5, scan_baseline=None):
    """Rank (table, column) pairs as index candidates.

    Returns a list of dicts sorted by score (highest first), each containing
    the table, column, how much execution time the predicate is associated
    with, and the table's sequential-scan ratio.

    If `scan_baseline` (from snapshot_scan_stats) is given, scan ratios are
    computed as deltas since that snapshot rather than lifetime totals.
    """
    cur.execute("""
        SELECT query, calls, total_exec_time
        FROM pg_stat_statements
        WHERE calls >= %s
          AND query ILIKE '%%WHERE%%'
    """, (min_calls,))
    rows = cur.fetchall()

    weight = defaultdict(float)     # (table, col) -> accumulated exec time
    calls_by = defaultdict(int)
    for query, calls, total_time in rows:
        cols = parse_predicate_columns(query)
        if not cols:
            continue
        # Split the query's cost evenly across the predicates it uses, so a
        # query filtering on 3 columns doesn't triple-count its own runtime.
        share = (total_time or 0.0) / len(cols)
        for key in cols:
            weight[key] += share
            calls_by[key] += calls

    scan_stats = (get_scan_stats_delta(cur, scan_baseline)
                  if scan_baseline is not None else get_table_scan_stats(cur))
    existing = get_existing_indexes(cur)
    selectivity = get_column_selectivity(cur)

    candidates = []
    for (table, column), w in weight.items():
        if table not in scan_stats:
            continue                                  # not a real user table
        if (table, column) in existing:
            continue                                  # already indexed
        st = scan_stats[table]
        if st["rows"] < 1000:
            continue                                  # too small to matter
        # Selectivity of a filter on this column, as the planner sees it.
        # Partitioned parents have no pg_stats rows of their own, so fall
        # back to any partition's estimate for the same column.
        frac = selectivity.get((table, column))
        if frac is None:
            for (t2, c2), f2 in selectivity.items():
                if c2 == column and t2.startswith(table + "_") and f2 is not None:
                    frac = f2
                    break

        # Score rewards expensive predicates on tables that are scanning a lot.
        score = w * (0.25 + st["seq_ratio"])
        candidates.append({
            "table": table,
            "column": column,
            "weight_ms": round(w, 1),
            "calls": calls_by[(table, column)],
            "seq_ratio": round(st["seq_ratio"], 3),
            "rows": st["rows"],
            "selectivity": (round(frac, 4) if frac is not None else None),
            "score": round(score, 1),
        })

    candidates.sort(key=lambda c: c["score"], reverse=True)
    return candidates


def reset_statement_stats(cur):
    """Reset pg_stat_statements so each run observes only its own workload."""
    try:
        cur.execute("SELECT pg_stat_statements_reset();")
    except Exception:
        pass  # extension may not be installed; monitor degrades gracefully
