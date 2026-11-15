"""Clear experiment results without touching the warehouse data.

Useful between trial runs so your final report only contains the runs you
actually mean to publish.

Run:
    python -m src.harness.reset_results
    python -m src.harness.reset_results --yes    # skip confirmation
"""
import argparse

from src.db import connect


def main():
    ap = argparse.ArgumentParser(description="Delete all experiment results.")
    ap.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    args = ap.parse_args()

    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM experiment_run;")
            n = cur.fetchone()[0]
            if n == 0:
                print("No experiment runs to delete.")
                return
            if not args.yes:
                ans = input(f"Delete {n} experiment run(s) and all their samples? [y/N] ")
                if ans.strip().lower() not in ("y", "yes"):
                    print("Aborted.")
                    return
            # order matters: children first
            cur.execute("DELETE FROM query_sample;")
            cur.execute("DELETE FROM adaptive_action;")
            cur.execute("DELETE FROM experiment_run;")
            print(f"Deleted {n} run(s). Warehouse data (fact_sales, dims) untouched.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
