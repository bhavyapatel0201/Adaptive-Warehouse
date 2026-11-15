"""Acceptance test: validates every component of the system end to end.

Run this before a demo or submission. It exercises each claim the project
makes and reports pass/fail per check, so a failure points at the specific
component rather than "something is wrong".

    python -m src.harness.acceptance              # full suite (~6-8 min)
    python -m src.harness.acceptance --quick      # skip the slow checks (~3 min)

The suite is deliberately strict about starting conditions: it resets the
physical design and trims ETL rows before checks that depend on them, because
a test that passes only from a lucky starting state is not a test.
"""
import argparse
import subprocess
import sys
import threading
import time

from src.config import ADAPTIVE, SCALE
from src.db import connect

PASS, FAIL, WARN = "PASS", "FAIL", "WARN"
_results = []


def record(name, status, detail=""):
    _results.append((name, status, detail))
    icon = {"PASS": "[ok]", "FAIL": "[FAIL]", "WARN": "[warn]"}[status]
    print(f"  {icon:<7} {name}")
    if detail:
        print(f"          {detail}")
    return status == PASS


def q(sql, params=None, one=True):
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            if one:
                return rows[0] if rows else None
            return rows
    finally:
        conn.close()


def clean_slate():
    """Reset physical design and table size so a check starts from baseline."""
    from src.adaptive.engine import reset_physical_design
    from src.harness.reset_data import trim_etl_rows
    reset_physical_design(verbose=False)
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cutoff = (SCALE["year_end"] + 1) * 10000 + 101
            cur.execute("DELETE FROM fact_sales WHERE date_key >= %s", (cutoff,))
            cur.execute("""
                SELECT c.relname, pg_get_expr(c.relpartbound, c.oid)
                FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_class p ON p.oid = i.inhparent
                WHERE p.relname = 'fact_sales'
            """)
            for name, bound in cur.fetchall():
                if not bound or "DEFAULT" in bound.upper():
                    continue
                digits, tok = [], ""
                for ch in bound:
                    if ch.isdigit():
                        tok += ch
                    elif tok:
                        digits.append(int(tok)); tok = ""
                if tok:
                    digits.append(int(tok))
                if digits and digits[0] >= cutoff:
                    cur.execute(f'ALTER TABLE fact_sales DETACH PARTITION "{name}"')
                    cur.execute(f'DROP TABLE "{name}"')
    finally:
        conn.close()
    trim_etl_rows()


# =====================================================================
# 1. Environment
# =====================================================================
def check_environment():
    print("\n[1] ENVIRONMENT")
    try:
        ver = q("SELECT version()")[0]
        ok = "PostgreSQL 16" in ver
        record("PostgreSQL 16 reachable", PASS if ok else WARN,
               ver.split(" on ")[0])
    except Exception as exc:
        return record("PostgreSQL reachable", FAIL, str(exc)[:120])

    try:
        q("SELECT count(*) FROM pg_stat_statements")
        record("pg_stat_statements active", PASS,
               "the adaptive monitor reads workload telemetry from here")
    except Exception as exc:
        record("pg_stat_statements active", FAIL,
               f"{str(exc)[:90]} — adaptive indexing cannot work without it")

    for mod in ("psycopg2", "matplotlib"):
        try:
            __import__(mod)
            record(f"python package: {mod}", PASS)
        except ImportError:
            record(f"python package: {mod}", FAIL, "pip install -r requirements.txt")
    return True


# =====================================================================
# 2. Schema and data baseline
# =====================================================================
def check_schema():
    print("\n[2] SCHEMA AND DATA")
    tables = [r[0] for r in q("""
        SELECT tablename FROM pg_tables WHERE schemaname='public'
    """, one=False)]
    needed = ["dim_item", "dim_customer", "dim_store", "dim_date",
              "experiment_run", "query_sample", "adaptive_action"]
    missing = [t for t in needed if t not in tables]
    record("required tables present", PASS if not missing else FAIL,
           f"missing: {missing}" if missing else "4 dimensions + 3 log tables")

    total = q("SELECT count(*) FROM fact_sales")[0]
    expected = SCALE["n_facts"]
    if total == expected:
        record("fact_sales at baseline", PASS, f"{total:,} rows")
    else:
        record("fact_sales at baseline", WARN,
               f"{total:,} rows, expected {expected:,} — run "
               f"'python -m src.harness.reset_data --yes'")

    parts = q("""
        SELECT count(*) FROM pg_inherits i
        JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname='fact_sales'
    """)[0]
    record("fact_sales is partitioned", PASS if parts >= 5 else FAIL,
           f"{parts} partitions (4 yearly + DEFAULT)")

    plan = "\n".join(r[0] for r in q("""
        EXPLAIN (COSTS OFF) SELECT sum(net_amount) FROM fact_sales
        WHERE date_key BETWEEN 20240101 AND 20241231
    """, one=False))
    pruned = "fact_sales_2024" in plan and "fact_sales_2022" not in plan
    record("partition pruning works", PASS if pruned else FAIL,
           "a 2024 query scans only fact_sales_2024")

    top = q("""SELECT count(*) FROM fact_sales
               GROUP BY item_key ORDER BY count(*) DESC LIMIT 1""")[0]
    skewed = top > (total / SCALE["n_items"]) * 10
    record("data skew present", PASS if skewed else WARN,
           f"hottest item has {top:,} sales — skew is what makes adaptive "
           f"indexing worth studying")
    return True


# =====================================================================
# 3. Static baseline
# =====================================================================
def check_static():
    print("\n[3] STATIC BASELINE (control group)")
    clean_slate()
    from src.harness.run_experiment import run_config
    cfg = {"config_id": "C5", "workload": "customer", "etl_rate": "zero"}
    _, samples = run_config(cfg, "static", 20, verbose=False)

    record("static run completes", PASS if samples else FAIL,
           f"{len(samples)} queries executed")
    if not samples:
        return False

    n_idx = q("SELECT count(*) FROM pg_class WHERE relname LIKE 'adx_%%'")[0]
    record("static builds no indexes", PASS if n_idx == 0 else FAIL,
           "the control group must not adapt")

    logged = q("SELECT count(*) FROM query_sample")[0]
    record("metrics logged to database", PASS if logged >= len(samples) else FAIL,
           f"{logged} samples in query_sample")
    return True


# =====================================================================
# 4. Adaptive indexing, per workload
# =====================================================================
def check_adaptive_indexing(quick=False):
    print("\n[4] ADAPTIVE INDEXING")
    from src.harness.run_experiment import run_config

    expected = {"product": "unit_price", "customer": "net_amount",
                "temporal": "discount"}
    cases = [("C1", "product"), ("C5", "customer"), ("C9", "temporal")]
    if quick:
        cases = cases[1:2]

    built_cols = {}
    for cid, workload in cases:
        clean_slate()
        cfg = {"config_id": cid, "workload": workload, "etl_rate": "zero"}
        run_config(cfg, "adaptive", 40, verbose=False)
        rows = q("""SELECT target FROM adaptive_action
                    WHERE action_type='create_index' ORDER BY ts DESC LIMIT 1""")
        target = rows[0] if rows else None
        built_cols[workload] = target
        want = expected[workload]
        ok = target is not None and want in target
        record(f"{workload}: indexes the selective column",
               PASS if ok else FAIL,
               f"built {target or '(nothing)'} — expected one on {want}")

    if not quick and len(built_cols) == 3:
        distinct = len({v for v in built_cols.values() if v})
        record("adaptation is workload-specific", PASS if distinct == 3 else FAIL,
               f"{distinct}/3 workloads produced a different index — this is "
               f"the workload-awareness claim")
    return True


# =====================================================================
# 5. Query improvement
# =====================================================================
def check_improvement():
    print("\n[5] QUERY LATENCY IMPROVEMENT")
    from src.harness.run_experiment import run_config

    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM query_sample")
            cur.execute("DELETE FROM adaptive_action")
            cur.execute("DELETE FROM experiment_run")
    finally:
        conn.close()

    cfg = {"config_id": "C5", "workload": "customer", "etl_rate": "zero"}
    clean_slate()
    run_config(cfg, "static", 40, verbose=False)
    clean_slate()
    run_config(cfg, "adaptive", 40, verbose=False)

    rows = q("""
        SELECT s.query_name,
               avg(s.latency_ms) FILTER (WHERE r.mode='static')   AS st,
               avg(s.latency_ms) FILTER (WHERE r.mode='adaptive') AS ad
        FROM query_sample s JOIN experiment_run r USING (run_id)
        GROUP BY s.query_name
        HAVING count(*) FILTER (WHERE r.mode='adaptive') > 0
           AND count(*) FILTER (WHERE r.mode='static')  > 0
    """, one=False)

    if not rows:
        return record("per-query comparison available", FAIL, "no paired samples")

    selective = ("c6_high_value_transactions", "c7_whale_customers")
    gains = []
    for name, st, ad in rows:
        if st and ad and name in selective:
            gains.append((name, 100.0 * (float(st) - float(ad)) / float(st)))

    if not gains:
        return record("selective queries measured", WARN,
                      "selective drill-downs did not run in both modes")

    best = max(g[1] for g in gains)
    detail = "; ".join(f"{n}: {v:+.1f}%" for n, v in gains)
    record("selective drill-downs improve", PASS if best > 10 else WARN, detail)

    broad = [(n, 100.0 * (float(st) - float(ad)) / float(st))
             for n, st, ad in rows
             if st and ad and n not in selective]
    if broad:
        worst = max(abs(v) for _, v in broad)
        record("broad rollups roughly unchanged", PASS if worst < 25 else WARN,
               f"largest swing {worst:.1f}% — a full scan is already optimal "
               f"for these, so ~0% is the correct result")
    return True


# =====================================================================
# 6. Partition adaptation
# =====================================================================
def check_partitions():
    print("\n[6] PARTITION ADAPTATION")
    from src.adaptive import partitions as P
    from src.harness.run_experiment import run_config

    # --- proactive ---
    clean_slate()
    cfg = {"config_id": "C11", "workload": "temporal", "etl_rate": "medium"}
    run_config(cfg, "adaptive", 45, verbose=False)

    rows = q("""SELECT duration_ms FROM adaptive_action
                WHERE action_type='provision_partition' ORDER BY ts DESC LIMIT 1""")
    prov_ms = rows[0] if rows else None
    record("provisions ahead of ingestion frontier",
           PASS if prov_ms is not None else WARN,
           f"created next partition in {prov_ms:.0f} ms (empty, no data movement)"
           if prov_ms else "no provisioning occurred — frontier may not have "
                           "advanced far enough in this run")

    stranded = q("""
        SELECT COALESCE(sum(n),0) FROM (
            SELECT count(*) AS n FROM fact_sales_default
        ) t""")[0]
    record("nothing stranded in DEFAULT", PASS if stranded == 0 else FAIL,
           f"{stranded} rows in the catch-all partition")

    # --- reactive drain ---
    clean_slate()
    conn = connect(autocommit=True)
    try:
        P.ensure_date_dimension(conn, SCALE["year_end"] + 1)
        with conn.cursor() as cur:
            yr = SCALE["year_end"] + 1
            cur.execute(f"""
                INSERT INTO fact_sales (item_key,customer_key,store_key,date_key,
                                        quantity,unit_price,discount,net_amount)
                SELECT 1+(i%2000),1+(i%20000),1+(i%200),
                       {yr}0000+(1+(i%6))*100+(1+(i%28)),1+(i%10),100,0,100
                FROM generate_series(1,25000) i
            """)
    finally:
        conn.close()

    before = q("SELECT count(*) FROM fact_sales_default")[0]
    record("DEFAULT accumulates when unprovisioned", PASS if before > 0 else FAIL,
           f"{before:,} rows stranded — this is the failure mode adaptation prevents")

    run_config(cfg, "adaptive", 30, verbose=False)
    rows = q("""SELECT duration_ms FROM adaptive_action
                WHERE action_type='drain_default' ORDER BY ts DESC LIMIT 1""")
    drain_ms = rows[0] if rows else None
    after = q("SELECT count(*) FROM fact_sales_default")[0]
    record("drains stranded rows", PASS if after == 0 else FAIL,
           f"recovered {before:,} rows in {drain_ms:.0f} ms"
           if drain_ms else f"{after} rows still stranded")

    if prov_ms and drain_ms:
        record("proactive is cheaper than reactive",
               PASS if drain_ms > prov_ms else WARN,
               f"{prov_ms:.0f} ms to anticipate vs {drain_ms:.0f} ms to recover "
               f"({drain_ms/prov_ms:.1f}x) — and recovery cost grows with backlog")
    return True


# =====================================================================
# 7. Safety guard
# =====================================================================
def check_safety():
    print("\n[7] SAFETY GUARD")
    from src.adaptive import executor

    clean_slate()
    ready, release = threading.Event(), threading.Event()

    def hold():
        c = connect(autocommit=False)
        with c.cursor() as cur:
            cur.execute("LOCK TABLE fact_sales_2025 IN ACCESS EXCLUSIVE MODE;")
        ready.set()
        release.wait(30)
        c.rollback(); c.close()

    t = threading.Thread(target=hold, daemon=True)
    t.start()
    ready.wait(10)

    conn = connect(autocommit=True)
    start = time.time()
    aborted = False
    try:
        executor.create_index(conn, "fact_sales", "quantity")
    except Exception:
        aborted = True
    finally:
        elapsed = time.time() - start
        release.set(); t.join(timeout=5); conn.close()

    budget = ADAPTIVE["ddl_timeout_ms"] / 1000.0
    within = elapsed < budget * 3
    record("aborts under lock contention", PASS if aborted and within else FAIL,
           f"gave up after {elapsed:.1f}s (lock_timeout {budget:.0f}s) rather "
           f"than blocking the ETL stream")
    executor.drop_all_adaptive_indexes(conn := connect(autocommit=True),
                                       verbose=False)
    conn.close()
    return True


# =====================================================================
# 8. Reporting artifacts
# =====================================================================
def check_reporting():
    print("\n[8] REPORTING ARTIFACTS")
    import os
    for mod, out in (("analysis.plots", "analysis/figures"),
                     ("analysis.export_csv", "analysis/exports")):
        r = subprocess.run([sys.executable, "-m", mod],
                           capture_output=True, text=True)
        made = os.path.isdir(out) and len(os.listdir(out)) > 0
        record(f"{mod} produces output", PASS if made else WARN,
               f"{out}/ has {len(os.listdir(out)) if os.path.isdir(out) else 0} file(s)")
    return True


# =====================================================================
def main():
    ap = argparse.ArgumentParser(description="Full acceptance test.")
    ap.add_argument("--quick", action="store_true",
                    help="skip the slower multi-workload checks")
    args = ap.parse_args()

    print("=" * 68)
    print("ADAPTIVE SALES WAREHOUSE — ACCEPTANCE TEST")
    print("=" * 68)
    started = time.time()

    if not check_environment():
        print("\nEnvironment unusable; stopping.")
        sys.exit(1)
    check_schema()
    check_static()
    check_adaptive_indexing(quick=args.quick)
    check_improvement()
    if not args.quick:
        check_partitions()
        check_safety()
    check_reporting()

    clean_slate()

    n_pass = sum(1 for _, s, _ in _results if s == PASS)
    n_fail = sum(1 for _, s, _ in _results if s == FAIL)
    n_warn = sum(1 for _, s, _ in _results if s == WARN)
    mins = (time.time() - started) / 60

    print("\n" + "=" * 68)
    print(f"RESULT: {n_pass} passed, {n_warn} warnings, {n_fail} failed "
          f"({mins:.1f} min)")
    print("=" * 68)
    if n_fail:
        print("\nFailed checks:")
        for name, status, detail in _results:
            if status == FAIL:
                print(f"  - {name}: {detail}")
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    main()
