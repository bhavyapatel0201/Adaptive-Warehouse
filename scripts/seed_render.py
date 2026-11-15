"""Seed a remote (Render) database from your local one, for the read-only demo.

The hosted dashboard reads the same tables the local one does, so this dumps
them locally and loads them into the remote database DATABASE_URL points at.

What gets copied:
  * the star schema + data (dim_*, fact_sales and its partitions)
  * the experiment results (experiment_run, query_sample, adaptive_action)

What does NOT get copied:
  * pg_stat_statements (a server extension, not your data)
  * anything the adaptive engine would need to RUN -- the remote instance
    only displays results.

Usage (from your machine, with the local Docker DB running):

    # 1. get the External Database URL from the Render dashboard, then:
    set RENDER_DATABASE_URL=postgresql://user:pass@host/dbname   (Windows)
    export RENDER_DATABASE_URL=postgresql://...                  (mac/Linux)

    # 2. run the seeder
    python -m scripts.seed_render

It uses pg_dump/pg_restore under the hood via plain SQL COPY, so no extra
tools are required beyond psycopg2.
"""
import os
import io
import sys

import psycopg2

LOCAL = {
    "host": os.environ.get("PGHOST", "localhost"),
    "port": int(os.environ.get("PGPORT", "5433")),
    "dbname": os.environ.get("PGDATABASE", "salesdw"),
    "user": os.environ.get("PGUSER", "wh"),
    "password": os.environ.get("PGPASSWORD", "wh"),
}

REMOTE_URL = os.environ.get("RENDER_DATABASE_URL")

# Order matters: dimensions and run-parents before the tables that
# reference them.
TABLES = [
    "dim_item", "dim_customer", "dim_store", "dim_date",
    "fact_sales",                       # parent; COPY handles partition routing
    "experiment_run", "query_sample", "adaptive_action",
]

SCHEMA_FILES = ["sql/01_schema.sql", "sql/02_baseline_indexes.sql",
                "sql/03_experiment_log.sql"]


def _run_sql_file(cur, path):
    with open(path, "r", encoding="utf-8") as fh:
        sql = fh.read()
    # strip psql meta-commands (\dt, \echo) the driver can't run
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("\\")]
    cur.execute("\n".join(lines))


def main():
    if not REMOTE_URL:
        print("ERROR: set RENDER_DATABASE_URL to your Render External "
              "Database URL first.")
        print("Find it in the Render dashboard under your database -> "
              "'External Database URL'.")
        sys.exit(1)

    print("Connecting to local database...")
    local = psycopg2.connect(**LOCAL)
    local.autocommit = True

    print("Connecting to Render database...")
    remote = psycopg2.connect(REMOTE_URL)
    remote.autocommit = True

    try:
        # 1. Build the schema on the remote side.
        print("Creating schema on remote...")
        with remote.cursor() as rcur:
            for f in SCHEMA_FILES:
                print(f"  applying {f}")
                _run_sql_file(rcur, f)

        # 2. Copy each table's data across via an in-memory buffer.
        for table in TABLES:
            buf = io.StringIO()
            with local.cursor() as lcur:
                lcur.copy_expert(
                    f"COPY {table} TO STDOUT WITH (FORMAT csv)", buf)
            rows = buf.getvalue().count("\n")
            buf.seek(0)
            with remote.cursor() as rcur:
                # fact_sales is partitioned; truncate cascades to partitions
                rcur.execute(f"TRUNCATE {table} CASCADE")
                rcur.copy_expert(
                    f"COPY {table} FROM STDIN WITH (FORMAT csv)", buf)
            print(f"  copied {table}: ~{rows} rows")

        # 3. Refresh planner stats so the dashboard's size/row figures are sane.
        with remote.cursor() as rcur:
            rcur.execute("ANALYZE")
        print("\nDone. The Render dashboard will now show these results.")
    finally:
        local.close()
        remote.close()


if __name__ == "__main__":
    main()
