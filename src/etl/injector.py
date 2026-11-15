"""Incremental ETL injector.

Simulates continuous transactional ingestion into the warehouse while
analytical queries run concurrently -- the "concurrent ETL" condition that
the proposal identifies as under-explored in existing adaptive-indexing work.

Runs as a background thread so the harness can drive queries and ingestion at
the same time against the same database.

Rates (from config.ETL_RATES):
    zero   - no ingestion (quiescent control)
    low    - 200 rows every 2.0 s
    medium - 1000 rows every 1.0 s
    high   - 5000 rows every 0.5 s

Newly ingested rows are written with recent date_keys so they land in the
current partition, mimicking real "hot tail" ingestion.

Run standalone:
    python -m src.etl.injector --rate medium --seconds 15
"""
import argparse
import csv
import datetime as dt
import io
import random
import threading
import time

from src.config import ETL_ADVANCE_DAYS_PER_BATCH, ETL_RATES, SCALE
from src.db import connect


class ETLInjector(threading.Thread):
    """Background thread that COPYs batches of new fact rows into the warehouse."""

    def __init__(self, rate: str, stop_event: threading.Event, verbose: bool = True,
                 advance_days_per_batch: float = None):
        super().__init__(daemon=True)
        if rate not in ETL_RATES:
            raise ValueError(f"Unknown ETL rate '{rate}'. Expected one of {list(ETL_RATES)}")
        self.rate_name = rate
        self.batch = ETL_RATES[rate]["batch"]
        self.interval = ETL_RATES[rate]["interval"]
        self.stop_event = stop_event
        self.verbose = verbose
        self.rows_inserted = 0
        self.batches = 0
        self.errors = 0
        # How fast the ingestion frontier moves forward in simulated time.
        # A real pipeline continuously loads "today's" data, so the newest
        # partition eventually fills and the next one is needed. With 0 the
        # injector just refills the newest existing year and the partition
        # layout never has to change -- useful as a control condition.
        self.advance = (ETL_ADVANCE_DAYS_PER_BATCH
                        if advance_days_per_batch is None
                        else advance_days_per_batch)
        self._sim_day = 0.0
        self._dim_years = set()

    # -- simulated ingestion clock ---------------------------------------
    def _frontier_date(self):
        """Current simulated ingestion date as (year, month, day)."""
        base = dt.date(SCALE["year_end"], 12, 1)
        d = base + dt.timedelta(days=int(self._sim_day))
        return d.year, d.month, d.day

    def _ensure_dim_date(self, conn, year):
        """Make sure dim_date covers `year` before writing facts for it.

        The fact table has a foreign key to dim_date, so ingestion into a new
        year fails outright unless the dimension is extended first. Extending
        it is the ETL job's responsibility in a real warehouse, which is why
        it lives here rather than in the adaptive engine.
        """
        if year in self._dim_years:
            return
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM dim_date WHERE year = %s LIMIT 1", (year,))
            if cur.fetchone() is None:
                cur.execute("""
                    INSERT INTO dim_date (date_key, full_date, day, month,
                                          quarter, year, day_of_week,
                                          is_weekend, season)
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
                    FROM generate_series(make_date(%s,1,1),
                                         make_date(%s,12,31),
                                         interval '1 day') d
                    ON CONFLICT (date_key) DO NOTHING
                """, (year, year))
        self._dim_years.add(year)

    # -- row synthesis ---------------------------------------------------
    def _make_batch(self):
        """Build a CSV buffer of `batch` new sale rows."""
        buf = io.StringIO()
        w = csv.writer(buf)
        if self.advance > 0:
            fy, fm, fd = self._frontier_date()
        else:
            fy = SCALE["year_end"]
        for _ in range(self.batch):
            if self.advance > 0:
                year, month, day = fy, fm, fd
            else:
                year = fy
                month = random.randint(1, 12)
                day = random.randint(1, 28)
            date_key = year * 10000 + month * 100 + day
            qty = random.randint(1, 10)
            unit = round(random.uniform(5, 2000), 2)
            disc = round(unit * random.choice([0, 0, 0, 0.05, 0.1, 0.2]), 2)
            net = round(qty * (unit - disc), 2)
            w.writerow([
                random.randint(1, SCALE["n_items"]),
                random.randint(1, SCALE["n_customers"]),
                random.randint(1, SCALE["n_stores"]),
                date_key, qty, unit, disc, net,
            ])
        buf.seek(0)
        return buf

    # -- thread body -----------------------------------------------------
    def run(self):
        if self.batch == 0:
            if self.verbose:
                print("  etl[zero]: quiescent, no ingestion")
            return

        conn = connect(autocommit=True)
        cols = ("item_key, customer_key, store_key, date_key, "
                "quantity, unit_price, discount, net_amount")
        try:
            while not self.stop_event.is_set():
                start = time.time()
                try:
                    if self.advance > 0:
                        self._ensure_dim_date(conn, self._frontier_date()[0])
                    with conn.cursor() as cur:
                        cur.copy_expert(
                            f"COPY fact_sales ({cols}) FROM STDIN WITH (FORMAT csv)",
                            self._make_batch(),
                        )
                    self.rows_inserted += self.batch
                    self.batches += 1
                    self._sim_day += self.advance
                except Exception as exc:
                    self.errors += 1
                    if self.verbose:
                        print(f"  [etl error] {exc}")
                # sleep the remainder of the interval, waking early on stop
                elapsed = time.time() - start
                self.stop_event.wait(max(0.0, self.interval - elapsed))
        finally:
            conn.close()
            if self.verbose:
                print(f"  etl[{self.rate_name}]: {self.rows_inserted} rows "
                      f"in {self.batches} batches, {self.errors} errors")


def main():
    ap = argparse.ArgumentParser(description="Run the ETL injector standalone.")
    ap.add_argument("--rate", required=True, choices=list(ETL_RATES))
    ap.add_argument("--seconds", type=float, default=15)
    args = ap.parse_args()

    stop = threading.Event()
    inj = ETLInjector(args.rate, stop)
    inj.start()
    time.sleep(args.seconds)
    stop.set()
    inj.join(timeout=10)


if __name__ == "__main__":
    main()
