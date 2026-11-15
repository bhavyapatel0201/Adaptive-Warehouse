"""Analytical query pools for the three workload phases.

Each pool represents one "workload focus type" from Table II of the proposal:

    product   -> product-centric analyses (promotional / merchandising)
    customer  -> customer-centric profiling (segmentation, loyalty)
    temporal  -> temporal trend reporting (quarter-end, seasonality)

Design notes
------------
* Queries are PARAMETERIZED templates. The shape of the query stays constant
  within a pool (so the access path is stable and learnable by the adaptive
  engine) while the literal values vary per execution (so we aren't just
  measuring a single cached plan).

* Crucially, each pool filters and groups on DIFFERENT columns. The baseline
  index set deliberately does NOT cover all of them, which is what gives the
  adaptive engine something to discover. For example `dim_item.brand`,
  `dim_customer.country` and `dim_date.quarter` are unindexed at baseline.

* `params()` returns a fresh randomized parameter tuple for each run.

Broad rollups vs selective drill-downs
--------------------------------------
Each pool deliberately mixes two query kinds, mirroring real analytical use:

  * **Broad rollups** aggregate across a large fraction of the warehouse
    (e.g. "revenue by category"). A sequential scan is genuinely the optimal
    plan for these, and PostgreSQL is right to ignore indexes. They are kept
    because a realistic workload contains them -- and because they show that
    the adaptive engine does *not* waste effort indexing for them.

  * **Selective drill-downs** target a small slice (high-value transactions,
    premium price points, deep discounts) matching roughly 3-5% of rows.
    These are where an index changes the plan from a full scan to a bitmap
    index scan, and where adaptive optimization earns its cost.

Each workload drills down on a DIFFERENT fact column -- product on
`unit_price`, customer on `net_amount`, temporal on `discount` -- so the
correct physical design genuinely differs per workload. An engine that simply
built every possible index would not be distinguishable from one that
actually tracks the workload; this design makes the difference measurable.
"""
import random

CATEGORIES = ["Electronics", "Home", "Apparel", "Grocery", "Sports"]
BRANDS = ["Acme", "Globex", "Initech", "Umbrella", "Soylent", "Stark", "Wayne", "Wonka"]
SEGMENTS = ["consumer", "business", "premium"]
COUNTRIES = ["Canada", "USA", "UK", "India"]
STORE_TYPES = ["flagship", "outlet", "online", "popup"]
SEASONS = ["Winter", "Spring", "Summer", "Fall"]
YEARS = [2022, 2023, 2024, 2025]


# =====================================================================
# PRODUCT-CENTRIC POOL
# Hot columns: dim_item.category, dim_item.brand, dim_item.subcategory
# =====================================================================
PRODUCT_POOL = [
    {
        "name": "p1_revenue_by_category",
        "sql": """
            SELECT i.category, sum(f.net_amount) AS revenue, count(*) AS n
            FROM fact_sales f
            JOIN dim_item i ON i.item_key = f.item_key
            WHERE i.category = %s
            GROUP BY i.category
        """,
        "params": lambda: (random.choice(CATEGORIES),),
    },
    {
        "name": "p2_top_items_by_brand",
        "sql": """
            SELECT i.item_key, i.item_name, sum(f.net_amount) AS revenue
            FROM fact_sales f
            JOIN dim_item i ON i.item_key = f.item_key
            WHERE i.brand = %s
            GROUP BY i.item_key, i.item_name
            ORDER BY revenue DESC
            LIMIT 20
        """,
        "params": lambda: (random.choice(BRANDS),),
    },
    {
        "name": "p3_subcategory_margin",
        "sql": """
            SELECT i.subcategory,
                   sum(f.net_amount) AS revenue,
                   avg(f.discount)   AS avg_discount
            FROM fact_sales f
            JOIN dim_item i ON i.item_key = f.item_key
            WHERE i.category = %s AND f.quantity >= %s
            GROUP BY i.subcategory
            ORDER BY revenue DESC
        """,
        "params": lambda: (random.choice(CATEGORIES), random.randint(1, 5)),
    },
    {
        "name": "p4_brand_performance_by_store_type",
        "sql": """
            SELECT i.brand, s.store_type, sum(f.net_amount) AS revenue
            FROM fact_sales f
            JOIN dim_item i  ON i.item_key  = f.item_key
            JOIN dim_store s ON s.store_key = f.store_key
            WHERE i.brand = %s
            GROUP BY i.brand, s.store_type
            ORDER BY revenue DESC
        """,
        "params": lambda: (random.choice(BRANDS),),
    },
    {
        "name": "p6_premium_price_drilldown",   # SELECTIVE (~5% of rows)
        "sql": """
            SELECT i.category, count(*) AS n, sum(f.net_amount) AS revenue
            FROM fact_sales f
            JOIN dim_item i ON i.item_key = f.item_key
            WHERE f.unit_price > %s
            GROUP BY i.category
            ORDER BY revenue DESC
        """,
        "params": lambda: (random.choice([1850, 1900, 1925, 1950]),),
    },
    {
        "name": "p7_premium_brand_mix",          # SELECTIVE (~5% of rows)
        "sql": """
            SELECT i.brand, count(*) AS units, avg(f.unit_price) AS avg_price
            FROM fact_sales f
            JOIN dim_item i ON i.item_key = f.item_key
            WHERE f.unit_price > %s
            GROUP BY i.brand
            ORDER BY units DESC
            LIMIT 10
        """,
        "params": lambda: (random.choice([1880, 1900, 1930]),),
    },
    {
        "name": "p5_discount_impact",
        "sql": """
            SELECT i.category, count(*) AS discounted_sales, sum(f.discount) AS total_discount
            FROM fact_sales f
            JOIN dim_item i ON i.item_key = f.item_key
            WHERE f.discount > %s AND i.category = %s
            GROUP BY i.category
        """,
        "params": lambda: (round(random.uniform(0, 50), 2), random.choice(CATEGORIES)),
    },
]


# =====================================================================
# CUSTOMER-CENTRIC POOL
# Hot columns: dim_customer.segment, .country, .region, .city
# =====================================================================
CUSTOMER_POOL = [
    {
        "name": "c1_revenue_by_segment",
        "sql": """
            SELECT c.segment, sum(f.net_amount) AS revenue, count(DISTINCT c.customer_key) AS buyers
            FROM fact_sales f
            JOIN dim_customer c ON c.customer_key = f.customer_key
            WHERE c.segment = %s
            GROUP BY c.segment
        """,
        "params": lambda: (random.choice(SEGMENTS),),
    },
    {
        "name": "c2_top_customers_by_country",
        "sql": """
            SELECT c.customer_key, c.first_name, c.last_name, sum(f.net_amount) AS spend
            FROM fact_sales f
            JOIN dim_customer c ON c.customer_key = f.customer_key
            WHERE c.country = %s
            GROUP BY c.customer_key, c.first_name, c.last_name
            ORDER BY spend DESC
            LIMIT 25
        """,
        "params": lambda: (random.choice(COUNTRIES),),
    },
    {
        "name": "c3_regional_basket_size",
        "sql": """
            SELECT c.region, avg(f.quantity) AS avg_qty, avg(f.net_amount) AS avg_ticket
            FROM fact_sales f
            JOIN dim_customer c ON c.customer_key = f.customer_key
            WHERE c.country = %s
            GROUP BY c.region
            ORDER BY avg_ticket DESC
        """,
        "params": lambda: (random.choice(COUNTRIES),),
    },
    {
        "name": "c4_segment_category_affinity",
        "sql": """
            SELECT c.segment, i.category, sum(f.net_amount) AS revenue
            FROM fact_sales f
            JOIN dim_customer c ON c.customer_key = f.customer_key
            JOIN dim_item i     ON i.item_key     = f.item_key
            WHERE c.segment = %s
            GROUP BY c.segment, i.category
            ORDER BY revenue DESC
        """,
        "params": lambda: (random.choice(SEGMENTS),),
    },
    {
        "name": "c6_high_value_transactions",    # SELECTIVE (~3.5% of rows)
        "sql": """
            SELECT c.segment, count(*) AS big_orders, sum(f.net_amount) AS revenue
            FROM fact_sales f
            JOIN dim_customer c ON c.customer_key = f.customer_key
            WHERE f.net_amount > %s
            GROUP BY c.segment
            ORDER BY revenue DESC
        """,
        "params": lambda: (random.choice([14000, 15000, 16000]),),
    },
    {
        "name": "c7_whale_customers",            # SELECTIVE (~3.5% of rows)
        "sql": """
            SELECT c.customer_key, c.first_name, c.last_name,
                   count(*) AS orders, sum(f.net_amount) AS spend
            FROM fact_sales f
            JOIN dim_customer c ON c.customer_key = f.customer_key
            WHERE f.net_amount > %s
            GROUP BY c.customer_key, c.first_name, c.last_name
            ORDER BY spend DESC
            LIMIT 20
        """,
        "params": lambda: (random.choice([15000, 16000, 17000]),),
    },
    {
        "name": "c5_city_customer_count",
        "sql": """
            SELECT c.city, count(DISTINCT c.customer_key) AS customers, sum(f.net_amount) AS revenue
            FROM fact_sales f
            JOIN dim_customer c ON c.customer_key = f.customer_key
            WHERE c.segment = %s AND f.net_amount > %s
            GROUP BY c.city
            ORDER BY revenue DESC
            LIMIT 15
        """,
        "params": lambda: (random.choice(SEGMENTS), round(random.uniform(50, 500), 2)),
    },
]


# =====================================================================
# TEMPORAL-CENTRIC POOL
# Hot columns: dim_date.year, .quarter, .month, .season, .is_weekend
# =====================================================================
TEMPORAL_POOL = [
    {
        "name": "t1_quarterly_revenue",
        "sql": """
            SELECT d.year, d.quarter, sum(f.net_amount) AS revenue
            FROM fact_sales f
            JOIN dim_date d ON d.date_key = f.date_key
            WHERE d.year = %s
            GROUP BY d.year, d.quarter
            ORDER BY d.quarter
        """,
        "params": lambda: (random.choice(YEARS),),
    },
    {
        "name": "t2_monthly_trend",
        "sql": """
            SELECT d.year, d.month, sum(f.net_amount) AS revenue, count(*) AS orders
            FROM fact_sales f
            JOIN dim_date d ON d.date_key = f.date_key
            WHERE d.year = %s AND d.quarter = %s
            GROUP BY d.year, d.month
            ORDER BY d.month
        """,
        "params": lambda: (random.choice(YEARS), random.randint(1, 4)),
    },
    {
        "name": "t3_seasonal_comparison",
        "sql": """
            SELECT d.season, sum(f.net_amount) AS revenue, avg(f.quantity) AS avg_qty
            FROM fact_sales f
            JOIN dim_date d ON d.date_key = f.date_key
            WHERE d.season = %s
            GROUP BY d.season
        """,
        "params": lambda: (random.choice(SEASONS),),
    },
    {
        "name": "t4_weekend_vs_weekday",
        "sql": """
            SELECT d.is_weekend, count(*) AS orders, sum(f.net_amount) AS revenue
            FROM fact_sales f
            JOIN dim_date d ON d.date_key = f.date_key
            WHERE d.year = %s
            GROUP BY d.is_weekend
        """,
        "params": lambda: (random.choice(YEARS),),
    },
    {
        "name": "t6_deep_discount_periods",      # SELECTIVE (~4% of rows)
        "sql": """
            SELECT d.year, d.quarter, count(*) AS promo_sales,
                   sum(f.discount) AS discount_given
            FROM fact_sales f
            JOIN dim_date d ON d.date_key = f.date_key
            WHERE f.discount > %s
            GROUP BY d.year, d.quarter
            ORDER BY discount_given DESC
        """,
        "params": lambda: (random.choice([290, 300, 310]),),
    },
    {
        "name": "t7_promo_seasonality",          # SELECTIVE (~4% of rows)
        "sql": """
            SELECT d.season, d.month, count(*) AS promo_sales
            FROM fact_sales f
            JOIN dim_date d ON d.date_key = f.date_key
            WHERE f.discount > %s
            GROUP BY d.season, d.month
            ORDER BY promo_sales DESC
            LIMIT 12
        """,
        "params": lambda: (random.choice([295, 305, 315]),),
    },
    {
        # Filters directly on the PARTITION KEY, so the planner can prune to
        # a single partition. Realistic ("last N months") and the only query
        # shape that exercises partition pruning -- the others filter on the
        # date dimension, which forces every partition to be scanned.
        "name": "t8_recent_window_rollup",
        "sql": """
            SELECT f.date_key / 100 AS yyyymm,
                   count(*) AS orders, sum(f.net_amount) AS revenue
            FROM fact_sales f
            WHERE f.date_key >= %s AND f.date_key < %s
            GROUP BY 1
            ORDER BY 1
        """,
        "params": lambda: (
            (y := random.choice(YEARS)) * 10000 + 101,
            y * 10000 + 401,
        ),
    },
    {
        "name": "t5_daily_peak_detection",
        "sql": """
            SELECT d.full_date, sum(f.net_amount) AS revenue
            FROM fact_sales f
            JOIN dim_date d ON d.date_key = f.date_key
            WHERE d.year = %s AND d.month = %s
            GROUP BY d.full_date
            ORDER BY revenue DESC
            LIMIT 10
        """,
        "params": lambda: (random.choice(YEARS), random.randint(1, 12)),
    },
]


POOLS = {
    "product":  PRODUCT_POOL,
    "customer": CUSTOMER_POOL,
    "temporal": TEMPORAL_POOL,
}


def get_pool(workload: str):
    """Return the query pool for a workload name, raising a clear error if unknown."""
    if workload not in POOLS:
        raise ValueError(
            f"Unknown workload '{workload}'. Expected one of: {list(POOLS)}"
        )
    return POOLS[workload]
