#!/usr/bin/env python3
"""Example: Distributed VastDB read/write with Ray — large random tables.

Generates random customer and order data, writes to VastDB, reads back,
joins, computes aggregates, and cleans up — all distributed across a Ray
cluster.

Catalog configuration mode used here:
  **Mode 1** — both bucket and schema fixed in VastDBConfig.
  Table identifiers are just the table name: ``catalog.get_table("my_table")``.
  See example.py for a full demonstration of all three modes.

Run via:
    ray job submit \
      --address "http://127.0.0.1:8265" \
      --working-dir . \
      --runtime-env-json '{"py_modules": ["./src/vast_daft"]}' \
      -- python examples/combined_example.py
"""

from __future__ import annotations

import logging
import time

import daft
import pyarrow as pa

from vast_daft import (
    VastDBCatalog,
    VastDBConfig,
    VastDBDataSink,
    VastDBDataSource,
    and_,
    where_between,
    where_equal,
    where_in,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ENDPOINT = "http://vippool.ie-dev-pipeline.svc.cluster.local"
BUCKET = "collections-bucket"
SCHEMA = "collections-schema"
ACCESS_KEY = "7P2486YDRB97497707R2"
SECRET_KEY = "JGAD1JyssLJ3KQ1G2MQp06m/BsefdeZequVb008u"

# Data sizes — adjust to stress the cluster
NUM_CUSTOMERS = 10_000
NUM_ORDERS = 100_000

# Table names
CUSTOMERS_TABLE = "__ray_combined_customers__"
ORDERS_TABLE = "__ray_combined_orders__"
JOINED_TABLE = "__ray_combined_joined__"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
log = logging.getLogger("combined_example")

# ---------------------------------------------------------------------------
# Schemas (pa.string() for VastDB compatibility — not pa.utf8())
# ---------------------------------------------------------------------------
CUSTOMERS_SCHEMA = pa.schema(
    [
        ("customer_id", pa.int64()),
        ("name", pa.string()),
        ("email", pa.string()),
        ("tier", pa.string()),
    ]
)

ORDERS_SCHEMA = pa.schema(
    [
        ("order_id", pa.int64()),
        ("customer_id", pa.int64()),
        ("product", pa.string()),
        ("amount", pa.float64()),
        ("order_date", pa.string()),
    ]
)

JOINED_SCHEMA = pa.schema(
    [
        ("customer_id", pa.int64()),
        ("name", pa.string()),
        ("email", pa.string()),
        ("tier", pa.string()),
        ("order_id", pa.int64()),
        ("product", pa.string()),
        ("amount", pa.float64()),
        ("order_date", pa.string()),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_timings: list[tuple[str, str, float]] = []


def timed(label: str, backend: str = "VastDB"):
    """Context-manager-like timer for benchmarking steps."""

    class _Timer:
        def __init__(self) -> None:
            self._t0: float = 0.0

        def __enter__(self):
            self._t0 = time.perf_counter()
            print(f"\n>>> {label}")
            return self

        def __exit__(self, *_):
            elapsed = time.perf_counter() - self._t0
            print(f"<<< {label} — {elapsed:.2f}s")
            _timings.append((label, backend, elapsed))

    return _Timer()


def print_summary_table() -> None:
    """Print a formatted summary table of all timed operations."""
    if not _timings:
        return
    max_label = max(len(label) for label, _, _ in _timings)
    col_w = max(max_label, len("Operation"))
    bk_w = max(max(len(bk) for _, bk, _ in _timings), len("Backend"))
    hdr = f"| {'Operation':<{col_w}} | {'Backend':<{bk_w}} | {'Time':>8} |"
    sep = f"|{'-' * (col_w + 2)}|{'-' * (bk_w + 2)}|{'-' * 10}|"
    print(f"\n{'=' * len(hdr)}")
    print("  TIMING SUMMARY")
    print(f"{'=' * len(hdr)}")
    print(hdr)
    print(sep)
    total = 0.0
    for label, backend, elapsed in _timings:
        print(f"| {label:<{col_w}} | {backend:<{bk_w}} | {elapsed:>7.2f}s |")
        total += elapsed
    print(sep)
    print(f"| {'TOTAL':<{col_w}} | {'':<{bk_w}} | {total:>7.2f}s |")
    print(f"{'=' * len(hdr)}")


def make_config() -> VastDBConfig:
    return VastDBConfig(
        endpoint=ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------
def generate_customers(n: int):
    """Generate n random customer records using Daft expressions."""
    import random

    random.seed(42)

    tiers = ["bronze", "silver", "gold", "platinum"]
    return {
        "customer_id": list(range(1, n + 1)),
        "name": [f"customer_{i}" for i in range(1, n + 1)],
        "email": [f"user_{i}@example.com" for i in range(1, n + 1)],
        "tier": [random.choice(tiers) for _ in range(n)],
    }


def generate_orders(n: int, num_customers: int):
    """Generate n random order records."""
    import random

    random.seed(123)

    products = [
        "Widget A",
        "Widget B",
        "Gadget X",
        "Gadget Y",
        "Thingamajig",
        "Doohickey",
        "Contraption Z",
        "Module Pro",
        "Sensor Lite",
        "Adapter Max",
    ]
    return {
        "order_id": list(range(1001, 1001 + n)),
        "customer_id": [random.randint(1, num_customers) for _ in range(n)],
        "product": [random.choice(products) for _ in range(n)],
        "amount": [round(random.uniform(5.0, 500.0), 2) for _ in range(n)],
        "order_date": [f"2025-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}" for _ in range(n)],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    daft.set_runner_ray()

    config = make_config()
    catalog = VastDBCatalog(config)

    print(f"\nConfig: {NUM_CUSTOMERS:,} customers, {NUM_ORDERS:,} orders")
    print(f"Ray cluster: distributed execution enabled\n")

    # Clean up stale tables from previous runs
    for t in [CUSTOMERS_TABLE, ORDERS_TABLE, JOINED_TABLE]:
        catalog.drop_table(t)

    # ------------------------------------------------------------------
    # Step 1: Generate and write customers
    # ------------------------------------------------------------------
    with timed(f"Generate {NUM_CUSTOMERS:,} customers", "Daft/Ray"):
        customers_data = generate_customers(NUM_CUSTOMERS)
        df_customers = daft.from_pydict(customers_data)

    with timed(f"Write customers to VastDB ({CUSTOMERS_TABLE})"):
        sink = VastDBDataSink(
            config=config,
            table_name=CUSTOMERS_TABLE,
            table_schema=CUSTOMERS_SCHEMA,
            create_if_missing=True,
        )
        result = df_customers.write_sink(sink)
        result.show()

    # ------------------------------------------------------------------
    # Step 2: Generate and write orders
    # ------------------------------------------------------------------
    with timed(f"Generate {NUM_ORDERS:,} orders", "Daft/Ray"):
        orders_data = generate_orders(NUM_ORDERS, NUM_CUSTOMERS)
        df_orders = daft.from_pydict(orders_data)

    with timed(f"Write orders to VastDB ({ORDERS_TABLE})"):
        sink = VastDBDataSink(
            config=config,
            table_name=ORDERS_TABLE,
            table_schema=ORDERS_SCHEMA,
            create_if_missing=True,
        )
        result = df_orders.write_sink(sink)
        result.show()

    # ------------------------------------------------------------------
    # Step 3: Read back from VastDB
    # ------------------------------------------------------------------
    with timed("Read customers from VastDB (split across workers)"):
        src = VastDBDataSource(
            config=config,
            table_name=CUSTOMERS_TABLE,
            table_schema=CUSTOMERS_SCHEMA,
            num_splits=4,
        )
        df_customers_read = src.read()
        print(f"  (lazy — 4 splits will distribute across Ray workers)")

    with timed("Read orders from VastDB (split across workers)"):
        src = VastDBDataSource(
            config=config,
            table_name=ORDERS_TABLE,
            table_schema=ORDERS_SCHEMA,
            num_splits=4,
        )
        df_orders_read = src.read()
        print(f"  (lazy — 4 splits will distribute across Ray workers)")

    # ------------------------------------------------------------------
    # Step 4: Join customers x orders
    # ------------------------------------------------------------------
    with timed("Join customers x orders on customer_id", "Daft/Ray"):
        df_joined = df_customers_read.join(
            df_orders_read,
            on="customer_id",
            how="inner",
        ).select(
            "customer_id",
            "name",
            "email",
            "tier",
            "order_id",
            "product",
            "amount",
            "order_date",
        )
        print("  Sample:")
        df_joined.limit(5).show()

    # ------------------------------------------------------------------
    # Step 5: Aggregations
    # ------------------------------------------------------------------
    with timed("Aggregate: revenue by tier", "Daft/Ray"):
        df_by_tier = (
            df_joined.groupby("tier")
            .agg(
                daft.col("amount").sum().alias("total_revenue"),
                daft.col("amount").mean().alias("avg_order"),
                daft.col("order_id").count().alias("order_count"),
            )
            .sort("total_revenue", desc=True)
        )
        df_by_tier.show()

    with timed("Aggregate: top 10 customers by spend", "Daft/Ray"):
        df_top = (
            df_joined.groupby("name")
            .agg(
                daft.col("amount").sum().alias("total_spent"),
                daft.col("order_id").count().alias("num_orders"),
            )
            .sort("total_spent", desc=True)
            .limit(10)
        )
        df_top.show()

    # ------------------------------------------------------------------
    # Step 6: Write joined result back to VastDB
    # ------------------------------------------------------------------
    with timed(f"Write joined result to VastDB ({JOINED_TABLE})"):
        sink = VastDBDataSink(
            config=config,
            table_name=JOINED_TABLE,
            table_schema=JOINED_SCHEMA,
            create_if_missing=True,
        )
        result = df_joined.write_sink(sink)
        result.show()

    with timed("Verify: read joined result back"):
        src = VastDBDataSource(
            config=config,
            table_name=JOINED_TABLE,
            table_schema=JOINED_SCHEMA,
            limit=10,
        )
        src.read().show()

    # ------------------------------------------------------------------
    # Step 6b: Predicate pushdown queries
    # ------------------------------------------------------------------
    # Query 1: Filter customers by tier (server-side predicate)
    with timed("Predicate: customers WHERE tier = 'platinum'"):
        src = VastDBDataSource(
            config=config,
            table_name=CUSTOMERS_TABLE,
            table_schema=CUSTOMERS_SCHEMA,
            predicate=where_equal("tier", "platinum"),
            num_splits=4,
        )
        df_plat = src.read().collect()
        print(f"  Platinum customers: {len(df_plat):,} rows")

    # Query 2: Filter orders by amount range (server-side predicate)
    with timed("Predicate: orders WHERE amount BETWEEN 200 AND 500"):
        src = VastDBDataSource(
            config=config,
            table_name=ORDERS_TABLE,
            table_schema=ORDERS_SCHEMA,
            predicate=where_between("amount", 200.0, 500.0),
            num_splits=4,
        )
        df_high = src.read().collect()
        print(f"  High-value orders: {len(df_high):,} rows")

    # Query 3: Filter joined table — compound predicate
    with timed("Predicate: joined WHERE tier IN ('gold','platinum') AND amount >= 300"):
        src = VastDBDataSource(
            config=config,
            table_name=JOINED_TABLE,
            table_schema=JOINED_SCHEMA,
            predicate=and_(
                where_in("tier", ["gold", "platinum"]),
                where_between("amount", 300.0, 500.0),
            ),
            num_splits=4,
        )
        df_vip = src.read().collect()
        print(f"  VIP high-spend rows: {len(df_vip):,} rows")

    # Query 4: Column projection — read only 2 columns from joined table
    with timed("Projection: joined — only (customer_id, amount)"):
        src = VastDBDataSource(
            config=config,
            table_name=JOINED_TABLE,
            table_schema=JOINED_SCHEMA,
            columns=["customer_id", "amount"],
            num_splits=4,
        )
        df_proj = src.read().collect()
        print(f"  Projected rows: {len(df_proj):,} rows")

    # Query 5: Predicate + projection combined
    with timed("Predicate+Projection: joined WHERE tier='gold', cols=(name, amount)"):
        src = VastDBDataSource(
            config=config,
            table_name=JOINED_TABLE,
            table_schema=JOINED_SCHEMA,
            predicate=where_equal("tier", "gold"),
            columns=["name", "amount"],
            num_splits=4,
        )
        df_combo = src.read().collect()
        print(f"  Gold tier (name, amount): {len(df_combo):,} rows")

    # ------------------------------------------------------------------
    # Step 7: Cleanup
    # ------------------------------------------------------------------
    with timed("Cleanup"):
        for table in [CUSTOMERS_TABLE, ORDERS_TABLE, JOINED_TABLE]:
            catalog.drop_table(table)
            print(f"  Dropped {table}")

    print_summary_table()
    print("\nDone — Ray distributed execution complete.")


if __name__ == "__main__":
    main()
