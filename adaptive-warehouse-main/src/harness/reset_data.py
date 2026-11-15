"""Reset fact_sales to its original synthetic-data size.

The ETL injector permanently adds rows to fact_sales every time it runs --
that's the point, it simulates real ingestion. But it means the table keeps
growing across repeated experiment sessions. Run this before a batch of runs
you intend to compare, so every run starts from the same table size.

This does NOT touch dimensions -- only fact_sales rows beyond the original
synthetic load are removed.

Identifying which rows are "original"
-------------------------------------
The data generator loads every row inside a single transaction, so all of
them share one exact `created_at` value (Postgres `now()` is transaction
time, not statement time). Each ETL batch commits separately and therefore
carries its own distinct timestamp, and no single batch comes close to the
size of the initial load.

So the original load is simply the timestamp with the most rows against it,
and everything else is ingested data. That is what this module keys on.

An earlier version used a time window instead ("anything more than two
minutes after the oldest row"). That silently under-trimmed: once a reset
removed the rows it could see, the oldest *remaining* timestamp became an
ETL batch, and every subsequent batch within two minutes of it looked
original. The residue accumulated a little more with each session, so the
baseline drifted upward and configurations stopped being comparable to one
another. Keying on the modal timestamp is exact and does not degrade with
repeated use.

Run:
    python -m src.harness.reset_data
    python -m src.harness.reset_data --yes
"""
import argparse

from src.config import SCALE
from src.db import connect

def baseline_timestamp(cur):
    """The created_at shared by the original generated rows, or None.

    Returns the timestamp carrying the most rows -- the single transaction
    the data generator used. Distinct from every ETL batch, which commits
    individually.
    """
    cur.execute("""
        SELECT created_at, count(*) AS n
        FROM fact_sales
        GROUP BY created_at
        ORDER BY n DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    return row[0] if row else None


def inspect(cur):
    """Return (total_rows, removable_rows, baseline_timestamp)."""
    cur.execute("SELECT count(*) FROM fact_sales")
    total = cur.fetchone()[0]
    if total == 0:
        return 0, 0, None
    baseline = baseline_timestamp(cur)
    cur.execute("SELECT count(*) FROM fact_sales WHERE created_at <> %s",
                (baseline,))
    removable = cur.fetchone()[0]
    return total, removable, baseline


def trim_etl_rows(conn=None, vacuum=True):
    """Delete ETL-injected rows, returning a result dict.

    Returns {"before", "after", "removed", "note"}. `note` is set when the
    table cannot be trimmed back to the expected baseline, so callers
    (including the dashboard) can surface the reason instead of silently
    reporting success.
    """
    own = conn is None
    if own:
        conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            before, removable, baseline = inspect(cur)

            if baseline is None:
                return {"before": before, "after": before, "removed": 0,
                        "note": "fact_sales is empty"}

            if removable:
                cur.execute("DELETE FROM fact_sales WHERE created_at <> %s",
                            (baseline,))
                if vacuum:
                    # VACUUM (not just ANALYZE) reclaims the dead tuples this
                    # DELETE just created. Without it a big delete leaves
                    # bloated pages that slow the NEXT run's sequential scans,
                    # which would masquerade as an "adaptive improvement" that
                    # is really just cleanup.
                    cur.execute("VACUUM ANALYZE fact_sales;")
                else:
                    cur.execute("ANALYZE fact_sales;")

            cur.execute("SELECT count(*) FROM fact_sales")
            after = cur.fetchone()[0]

        note = None
        expected = SCALE["n_facts"]
        if after != expected:
            direction = "above" if after > expected else "below"
            note = (
                f"fact_sales holds {after} rows, {abs(after - expected)} "
                f"{direction} the expected {expected} baseline. The original "
                f"generated rows appear to have been altered. Reload a clean "
                f"baseline with: psql -f sql/01_schema.sql then "
                f"python -m src.data_gen.generate"
            )
        return {"before": before, "after": after,
                "removed": before - after, "note": note}
    finally:
        if own:
            conn.close()


def main():
    ap = argparse.ArgumentParser(
        description="Trim fact_sales back to its pre-ETL baseline size.")
    ap.add_argument("--yes", action="store_true", help="skip confirmation")
    args = ap.parse_args()

    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            before, removable, baseline = inspect(cur)

        if baseline is None:
            print("fact_sales is empty; nothing to reset.")
            return

        if removable == 0:
            print(f"fact_sales already at baseline ({before} rows). Nothing to do.")
            if before != SCALE["n_facts"]:
                print(f"NOTE: expected {SCALE['n_facts']} rows at baseline.")
                print("      Reload a clean baseline with:")
                print("        psql -f sql/01_schema.sql")
                print("        python -m src.data_gen.generate")
            return

        print(f"Current rows: {before}. ETL-injected rows to remove: {removable}.")
        if not args.yes:
            ans = input("Proceed? [y/N] ")
            if ans.strip().lower() not in ("y", "yes"):
                print("Aborted.")
                return

        print("Reclaiming space (VACUUM)... this may take a few seconds.")
        result = trim_etl_rows(conn)
        print(f"Done. fact_sales: {result['before']} -> {result['after']} rows.")
        if result["note"]:
            print("NOTE: " + result["note"])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
