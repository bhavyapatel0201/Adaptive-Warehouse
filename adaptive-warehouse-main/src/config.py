"""Central configuration for the Adaptive Sales Warehouse project.

Reads connection settings from environment variables (see .env.example)
with sensible local defaults so `python -m src.<module>` just works.
"""
import os

# --- Database connection -------------------------------------------------
DB = {
    "host":     os.environ.get("PGHOST", "localhost"),
    # NOTE: defaults to 5433 because 5432 is often already taken by a local
    # PostgreSQL install on Windows. Matches the port mapping in
    # docker-compose.yml. Override with the PGPORT environment variable.
    "port":     int(os.environ.get("PGPORT", "5433")),
    "dbname":   os.environ.get("PGDATABASE", "salesdw"),
    "user":     os.environ.get("PGUSER", "wh"),
    "password": os.environ.get("PGPASSWORD", "wh"),
}

# --- Synthetic data generation scale ------------------------------------
# Kept small by default so you can iterate fast. Bump for the final run.
SCALE = {
    "n_items":     int(os.environ.get("N_ITEMS", "2000")),
    "n_customers": int(os.environ.get("N_CUSTOMERS", "20000")),
    "n_stores":    int(os.environ.get("N_STORES", "200")),
    "n_facts":     int(os.environ.get("N_FACTS", "500000")),
    "year_start":  2022,
    "year_end":    2025,     # inclusive; must match partitions in 01_schema.sql
}

# --- ETL ingestion rates (rows per batch, seconds between batches) -------
ETL_RATES = {
    "zero":   {"batch": 0,    "interval": 1.0},
    "low":    {"batch": 200,  "interval": 2.0},
    "medium": {"batch": 1000, "interval": 1.0},
    "high":   {"batch": 5000, "interval": 0.5},
}

# How far the simulated ingestion clock advances per ETL batch, in days.
# A real pipeline continuously loads current data, so the newest partition
# eventually fills and the next one must exist. Set to 0 to keep ingestion
# inside the existing partitions (control condition for partition adaptation).
ETL_ADVANCE_DAYS_PER_BATCH = float(
    os.environ.get("ETL_ADVANCE_DAYS_PER_BATCH", "1.5"))

# --- The 12-run experiment matrix (Table II in the proposal) -------------
WORKLOADS = ["product", "customer", "temporal"]
_RATE_ORDER = ["zero", "low", "medium", "high"]

EXPERIMENT_MATRIX = [
    {
        "config_id": f"C{i + 1}",
        "workload": wl,
        "etl_rate": rate,
    }
    for i, (wl, rate) in enumerate(
        (wl, rate) for wl in WORKLOADS for rate in _RATE_ORDER
    )
]

# --- Adaptive engine tuning ---------------------------------------------
ADAPTIVE = {
    "poll_interval_s":     10,     # how often the monitor samples pg_stat_statements
    "seqscan_threshold":   0.30,   # create index if >30% of scans on a col are seq scans
    "min_calls":           5,      # ignore columns seen fewer than this many times
    "ddl_timeout_ms":      5000,   # abort adaptation if locks can't be acquired
    "max_indexes":         12,     # cap total adaptive indexes to bound overhead
    # Skip columns where a filter would match more than this fraction of rows.
    # A sequential scan beats an index well before 10% selectivity on a large
    # fact table, so indexing such columns costs ETL write throughput for no
    # query gain. Tightened from 0.15 after observing that 8-12 value columns
    # (brand, month, quantity) were being indexed with no measurable benefit.
    "max_selectivity":     0.05,
    # --- partition adaptation ---
    # Provision the next partition once ingestion comes within this many days
    # of the end of the newest covered range. Creating an empty partition is
    # nearly free; draining a full DEFAULT partition later is not.
    "partition_lead_days":  45,
    # Only index partitions carrying at least this share of scan activity.
    # Indexes on cold partitions cost ETL write throughput for no query gain.
    "hot_partition_share":  0.05,
    # Master switch for partition adaptation, so the two optimization axes
    # (indexing vs partitioning) can be evaluated in isolation.
    "adapt_partitions":     True,
    "partitioned_table":    "fact_sales",
}
