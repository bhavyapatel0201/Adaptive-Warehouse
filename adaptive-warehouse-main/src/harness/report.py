"""Summarize experiment results from the log tables.

Prints a per-configuration table of latency percentiles, sequential-scan
share, and plan-diversity (a proxy for plan regressions). When both static
and adaptive runs exist for a config, it also prints the delta -- which is
the headline result the report needs.

Run:
    python -m src.harness.report
    python -m src.harness.report --csv results.csv
"""
import argparse
import csv

from src.db import connect

SUMMARY_SQL = """
SELECT r.config_id,
       r.mode,
       r.workload,
       r.etl_rate,
       count(*)                                              AS samples,
       round(avg(s.latency_ms)::numeric, 1)                  AS avg_ms,
       round((percentile_cont(0.5) WITHIN GROUP
              (ORDER BY s.latency_ms))::numeric, 1)          AS median_ms,
       round((percentile_cont(0.95) WITHIN GROUP
              (ORDER BY s.latency_ms))::numeric, 1)          AS p95_ms,
       round(avg(s.planning_ms)::numeric, 2)                 AS plan_ms,
       round(100.0 * sum(CASE WHEN s.used_seqscan THEN 1 ELSE 0 END)
             / count(*), 1)                                  AS seqscan_pct,
       count(DISTINCT s.plan_hash)                           AS distinct_plans
FROM query_sample s
JOIN experiment_run r USING (run_id)
GROUP BY r.config_id, r.mode, r.workload, r.etl_rate
ORDER BY r.config_id, r.mode
"""

# Compare the two modes per config, averaging across repetitions.
COMPARE_SQL = """
WITH per_mode AS (
    SELECT r.config_id, r.mode, avg(s.latency_ms) AS avg_ms
    FROM query_sample s JOIN experiment_run r USING (run_id)
    GROUP BY r.config_id, r.mode
)
SELECT st.config_id,
       round(st.avg_ms::numeric, 1)                       AS static_ms,
       round(ad.avg_ms::numeric, 1)                       AS adaptive_ms,
       round((100.0 * (st.avg_ms - ad.avg_ms)
              / NULLIF(st.avg_ms, 0))::numeric, 1)        AS improvement_pct
FROM per_mode st
JOIN per_mode ad
  ON ad.config_id = st.config_id AND ad.mode = 'adaptive'
WHERE st.mode = 'static'
ORDER BY st.config_id
"""

# Per-query breakdown. This is the most informative view: adaptive indexing
# helps selective drill-down queries enormously and broad aggregate scans not
# at all, and an aggregate median hides that completely.
PER_QUERY_SQL = """
SELECT s.query_name,
       count(*) FILTER (WHERE r.mode = 'static')                    AS static_n,
       round((avg(s.latency_ms) FILTER (WHERE r.mode = 'static'))::numeric, 1)
                                                                    AS static_ms,
       round((avg(s.latency_ms) FILTER (WHERE r.mode = 'adaptive'))::numeric, 1)
                                                                    AS adaptive_ms,
       round((100.0 * (avg(s.latency_ms) FILTER (WHERE r.mode = 'static')
                     - avg(s.latency_ms) FILTER (WHERE r.mode = 'adaptive'))
              / NULLIF(avg(s.latency_ms) FILTER (WHERE r.mode = 'static'), 0)
             )::numeric, 1)                                          AS improve_pct
FROM query_sample s
JOIN experiment_run r USING (run_id)
GROUP BY s.query_name
HAVING count(*) FILTER (WHERE r.mode = 'adaptive') > 0
ORDER BY improve_pct DESC NULLS LAST
"""

# Partition health: how much data ended up stranded in the catch-all
# partition, and how the two adaptation paths compare in cost.
PARTITION_SQL = """
SELECT a.action_type,
       count(*)                                   AS n,
       round(avg(a.duration_ms)::numeric, 1)      AS avg_ms,
       round(max(a.duration_ms)::numeric, 1)      AS max_ms
FROM adaptive_action a
WHERE a.action_type IN ('provision_partition', 'drain_default',
                        'provision_failed', 'drain_failed')
GROUP BY a.action_type
ORDER BY a.action_type
"""

PARTITION_LAYOUT_SQL = """
SELECT c.relname AS partition,
       CASE WHEN pg_get_expr(c.relpartbound, c.oid) ILIKE '%%DEFAULT%%'
            THEN 'DEFAULT (catch-all)' ELSE 'ranged' END AS kind,
       COALESCE(s.n_live_tup, 0) AS approx_rows
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class p ON p.oid = i.inhparent
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE p.relname = 'fact_sales'
ORDER BY c.relname
"""

ACTIONS_SQL = """
SELECT r.config_id, a.action_type, count(*) AS n,
       round(avg(a.duration_ms)::numeric, 1) AS avg_ms
FROM adaptive_action a JOIN experiment_run r USING (run_id)
GROUP BY r.config_id, a.action_type
ORDER BY r.config_id, a.action_type
"""


def _print_table(headers, rows):
    if not rows:
        print("  (no data)")
        return
    widths = [max(len(str(h)), *(len(str(r[i])) for r in rows))
              for i, h in enumerate(headers)]
    line = "  ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
    print(line)
    print("-" * len(line))
    for r in rows:
        print("  ".join(str(v).ljust(widths[i]) for i, v in enumerate(r)))


def fetch(sql):
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [d[0] for d in cur.description], cur.fetchall()
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description="Report experiment results.")
    ap.add_argument("--csv", help="also write the summary to this CSV file")
    args = ap.parse_args()

    print("\n================ PER-CONFIGURATION SUMMARY ================")
    headers, rows = fetch(SUMMARY_SQL)
    _print_table(headers, rows)

    if args.csv:
        with open(args.csv, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(headers)
            w.writerows(rows)
        print(f"\n  summary written to {args.csv}")

    print("\n================ STATIC vs ADAPTIVE ================")
    ch, crows = fetch(COMPARE_SQL)
    if crows:
        _print_table(ch, crows)
        print("\n  positive improvement_pct = adaptive was faster")
    else:
        print("  (no adaptive runs yet -- available after Phase 3)")

    print("\n================ PER-QUERY: STATIC vs ADAPTIVE ================")
    ph, prows = fetch(PER_QUERY_SQL)
    if prows:
        _print_table(ph, prows)
        print("\n  Selective drill-down queries should show large gains;")
        print("  broad aggregate scans should show ~0% (a full scan is already optimal).")
    else:
        print("  (no adaptive runs yet -- available after Phase 3)")

    print("\n================ PARTITION ADAPTATION ================")
    qh, qrows = fetch(PARTITION_SQL)
    if qrows:
        _print_table(qh, qrows)
        print("\n  provision_partition = created ahead of need (empty, cheap)")
        print("  drain_default       = recovered rows already stranded (costly)")
    else:
        print("  (no partition actions recorded)")

    lh, lrows = fetch(PARTITION_LAYOUT_SQL)
    if lrows:
        print("\n  current fact_sales partition layout:")
        _print_table(lh, lrows)
        stranded = [r for r in lrows if "DEFAULT" in str(r[1]) and (r[2] or 0) > 0]
        if stranded:
            print("\n  WARNING: rows are sitting in the DEFAULT partition.")
            print("  These block creating the partition they belong in, and")
            print("  the longer they accumulate the more expensive recovery is.")

    print("\n================ ADAPTIVE ACTIONS ================")
    ah, arows = fetch(ACTIONS_SQL)
    _print_table(ah, arows)
    print()


if __name__ == "__main__":
    main()
