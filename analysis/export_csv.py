"""Export experiment results to CSV files for the written report.

Produces three files in analysis/exports/ that map directly onto tables you
would put in the paper:

  summary_by_config.csv   one row per config x mode -- latency percentiles,
                          seq-scan share, plan diversity
  per_query.csv           one row per query template -- static vs adaptive
                          latency and % improvement (the headline result)
  adaptive_actions.csv    every index the engine built or dropped, with the
                          reason it decided to and how long it took

Run:
    python -m analysis.export_csv
"""
import argparse
import csv
import os

from src.db import connect

QUERIES = {
    "summary_by_config.csv": """
        SELECT r.config_id, r.mode, r.workload, r.etl_rate,
               count(*)                                             AS samples,
               round(avg(s.latency_ms)::numeric, 1)                 AS avg_ms,
               round((percentile_cont(0.5) WITHIN GROUP
                      (ORDER BY s.latency_ms))::numeric, 1)         AS median_ms,
               round((percentile_cont(0.95) WITHIN GROUP
                      (ORDER BY s.latency_ms))::numeric, 1)         AS p95_ms,
               round(avg(s.planning_ms)::numeric, 2)                AS planning_ms,
               count(DISTINCT s.plan_hash)                          AS distinct_plans
        FROM query_sample s JOIN experiment_run r USING (run_id)
        GROUP BY r.config_id, r.mode, r.workload, r.etl_rate
        ORDER BY r.workload, r.etl_rate, r.mode
    """,
    "per_query.csv": """
        SELECT s.query_name,
               round((avg(s.latency_ms) FILTER (WHERE r.mode='static'))::numeric,1)
                                                                    AS static_ms,
               round((avg(s.latency_ms) FILTER (WHERE r.mode='adaptive'))::numeric,1)
                                                                    AS adaptive_ms,
               round((100.0 * (avg(s.latency_ms) FILTER (WHERE r.mode='static')
                             - avg(s.latency_ms) FILTER (WHERE r.mode='adaptive'))
                      / NULLIF(avg(s.latency_ms) FILTER (WHERE r.mode='static'),0)
                     )::numeric, 1)                                 AS improvement_pct,
               count(*) FILTER (WHERE r.mode='static')              AS static_n,
               count(*) FILTER (WHERE r.mode='adaptive')            AS adaptive_n
        FROM query_sample s JOIN experiment_run r USING (run_id)
        GROUP BY s.query_name
        ORDER BY improvement_pct DESC NULLS LAST
    """,
    "adaptive_actions.csv": """
        SELECT r.config_id, r.workload, r.etl_rate,
               a.action_type, a.target, a.reason,
               round(a.duration_ms::numeric, 1) AS duration_ms,
               a.succeeded, a.ts
        FROM adaptive_action a JOIN experiment_run r USING (run_id)
        ORDER BY a.ts
    """,
}


def main():
    ap = argparse.ArgumentParser(description="Export results to CSV.")
    ap.add_argument("--outdir", default="analysis/exports")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    conn = connect(autocommit=True)
    try:
        for filename, sql in QUERIES.items():
            with conn.cursor() as cur:
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
            path = os.path.join(args.outdir, filename)
            with open(path, "w", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(cols)
                w.writerows(rows)
            print(f"  wrote {path} ({len(rows)} rows)")
    finally:
        conn.close()
    print("\nOpen these in Excel for your report tables.")


if __name__ == "__main__":
    main()
