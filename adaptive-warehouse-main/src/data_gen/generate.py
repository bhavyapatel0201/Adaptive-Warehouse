"""Synthetic data generator for the Global Sales Data Warehouse.

Populates dim_item, dim_customer, dim_store, dim_date and the partitioned
fact_sales table with reproducible, realistically-skewed data.

Why synthetic instead of TPC-DS dsdgen?
  - dsdgen requires a licensed download from tpc.org (not always convenient),
    whereas this generator is self-contained and runs anywhere.
  - The schema is TPC-DS-shaped (star schema, same dimension roles), so you
    can swap in real TPC-DS data later for the final report without changing
    the workload/ETL/adaptive code.

Skew matters: sales are concentrated on a minority of items, customers, and
stores (Zipf-like), and on recent dates. That skew is what makes adaptive
indexing worth studying -- a few hot access paths dominate the workload.

Run:  python -m src.data_gen.generate
"""
import csv
import datetime as dt
import io
import random

from src.config import SCALE
from src.db import connect

random.seed(42)  # reproducible

CATEGORIES = {
    "Electronics": ["Phones", "Laptops", "Audio", "Cameras"],
    "Home": ["Kitchen", "Furniture", "Decor", "Bedding"],
    "Apparel": ["Men", "Women", "Kids", "Footwear"],
    "Grocery": ["Beverages", "Snacks", "Produce", "Dairy"],
    "Sports": ["Fitness", "Outdoor", "Cycling", "Team"],
}
BRANDS = ["Acme", "Globex", "Initech", "Umbrella", "Soylent", "Stark", "Wayne", "Wonka"]
SEGMENTS = ["consumer", "business", "premium"]
STORE_TYPES = ["flagship", "outlet", "online", "popup"]
COUNTRIES = {
    "Canada": ["Ontario", "Quebec", "BC", "Alberta"],
    "USA": ["California", "Texas", "New York", "Florida"],
    "UK": ["England", "Scotland", "Wales"],
    "India": ["Maharashtra", "Karnataka", "Delhi"],
}
CITIES = ["Windsor", "Toronto", "Vancouver", "Austin", "London", "Mumbai", "Seattle", "Ottawa"]
FIRST = ["Alex", "Sam", "Jordan", "Priya", "Wei", "Maria", "John", "Aisha", "Liam", "Noor"]
LAST = ["Patel", "Smith", "Chen", "Garcia", "Khan", "Brown", "Singh", "Lee", "Kumar", "Diaz"]


def _copy(conn, table, columns, rows):
    """Fast bulk load via COPY from an in-memory CSV buffer."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    for r in rows:
        writer.writerow(r)
    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)",
            buf,
        )


def _zipf_picker(n, s=1.3):
    """Return a function that samples 1..n with a Zipf-like skew (hot minority)."""
    weights = [1.0 / (i ** s) for i in range(1, n + 1)]
    total = sum(weights)
    cum, acc = [], 0.0
    for w in weights:
        acc += w / total
        cum.append(acc)

    def pick():
        x = random.random()
        # binary search
        lo, hi = 0, n - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if cum[mid] < x:
                lo = mid + 1
            else:
                hi = mid
        return lo + 1
    return pick


def gen_dim_item(n):
    rows = []
    cats = list(CATEGORIES.items())
    for k in range(1, n + 1):
        cat, subs = random.choice(cats)
        rows.append([
            k, f"Item {k}", cat, random.choice(subs),
            random.choice(BRANDS), round(random.uniform(5, 2000), 2),
        ])
    return rows


def gen_dim_customer(n):
    rows = []
    for k in range(1, n + 1):
        country, regions = random.choice(list(COUNTRIES.items()))
        reg_date = dt.date(SCALE["year_start"], 1, 1) + dt.timedelta(
            days=random.randint(0, 365 * 3)
        )
        rows.append([
            k, random.choice(FIRST), random.choice(LAST),
            f"user{k}@example.com", random.choice(SEGMENTS),
            country, random.choice(regions), random.choice(CITIES), reg_date,
        ])
    return rows


def gen_dim_store(n):
    rows = []
    for k in range(1, n + 1):
        country, regions = random.choice(list(COUNTRIES.items()))
        rows.append([
            k, f"Store {k}", random.choice(STORE_TYPES),
            country, random.choice(regions), random.choice(CITIES),
        ])
    return rows


def gen_dim_date(y0, y1):
    rows, seen = [], set()
    d = dt.date(y0, 1, 1)
    end = dt.date(y1, 12, 31)
    seasons = {12: "Winter", 1: "Winter", 2: "Winter", 3: "Spring", 4: "Spring",
               5: "Spring", 6: "Summer", 7: "Summer", 8: "Summer",
               9: "Fall", 10: "Fall", 11: "Fall"}
    while d <= end:
        key = d.year * 10000 + d.month * 100 + d.day
        if key not in seen:
            seen.add(key)
            q = (d.month - 1) // 3 + 1
            rows.append([
                key, d, d.day, d.month, q, d.year,
                d.weekday(), d.weekday() >= 5, seasons[d.month],
            ])
        d += dt.timedelta(days=1)
    return rows


def gen_fact_sales(n, date_keys, n_items, n_customers, n_stores):
    """Yield fact rows with Zipf skew on item/customer and recency skew on date."""
    pick_item = _zipf_picker(n_items, s=1.4)
    pick_cust = _zipf_picker(n_customers, s=1.2)
    # recency skew: weight later dates more heavily
    date_keys = sorted(date_keys)
    dn = len(date_keys)
    date_weights = [(i + 1) ** 1.5 for i in range(dn)]
    dw_total = sum(date_weights)
    date_cum, acc = [], 0.0
    for w in date_weights:
        acc += w / dw_total
        date_cum.append(acc)

    def pick_date():
        x = random.random()
        lo, hi = 0, dn - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if date_cum[mid] < x:
                lo = mid + 1
            else:
                hi = mid
        return date_keys[lo]

    for _ in range(n):
        item = pick_item()
        cust = pick_cust()
        store = random.randint(1, n_stores)
        dkey = pick_date()
        qty = random.randint(1, 10)
        unit = round(random.uniform(5, 2000), 2)
        disc = round(unit * random.choice([0, 0, 0, 0.05, 0.1, 0.2]), 2)
        net = round(qty * (unit - disc), 2)
        yield [item, cust, store, dkey, qty, unit, disc, net]


def main():
    conn = connect(autocommit=False)
    try:
        print(f"Generating dimensions "
              f"(items={SCALE['n_items']}, customers={SCALE['n_customers']}, "
              f"stores={SCALE['n_stores']})...")
        _copy(conn, "dim_item",
              ["item_key", "item_name", "category", "subcategory", "brand", "base_price"],
              gen_dim_item(SCALE["n_items"]))
        _copy(conn, "dim_customer",
              ["customer_key", "first_name", "last_name", "email", "segment",
               "country", "region", "city", "registration_date"],
              gen_dim_customer(SCALE["n_customers"]))
        _copy(conn, "dim_store",
              ["store_key", "store_name", "store_type", "country", "region", "city"],
              gen_dim_store(SCALE["n_stores"]))

        date_rows = gen_dim_date(SCALE["year_start"], SCALE["year_end"])
        _copy(conn, "dim_date",
              ["date_key", "full_date", "day", "month", "quarter", "year",
               "day_of_week", "is_weekend", "season"],
              date_rows)
        conn.commit()
        print(f"  dim_date rows: {len(date_rows)}")

        date_keys = [r[0] for r in date_rows]
        print(f"Generating {SCALE['n_facts']} fact rows (skewed)...")
        # stream in chunks to keep memory flat
        chunk, CHUNK = [], 50000
        loaded = 0
        for row in gen_fact_sales(SCALE["n_facts"], date_keys,
                                  SCALE["n_items"], SCALE["n_customers"],
                                  SCALE["n_stores"]):
            chunk.append(row)
            if len(chunk) >= CHUNK:
                _copy(conn, "fact_sales",
                      ["item_key", "customer_key", "store_key", "date_key",
                       "quantity", "unit_price", "discount", "net_amount"],
                      chunk)
                loaded += len(chunk)
                chunk = []
                print(f"  ...{loaded} fact rows")
        if chunk:
            _copy(conn, "fact_sales",
                  ["item_key", "customer_key", "store_key", "date_key",
                   "quantity", "unit_price", "discount", "net_amount"],
                  chunk)
            loaded += len(chunk)
        conn.commit()
        print(f"Done. Total fact rows loaded: {loaded}")

        with conn.cursor() as cur:
            cur.execute("ANALYZE;")
        conn.commit()
        print("ANALYZE complete. Data generation finished.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
