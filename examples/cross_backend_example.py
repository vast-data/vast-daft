#!/usr/bin/env python3
"""Example: Cross-backend operations — VastDB (native) + Iceberg (S3-backed).

Demonstrates reading from VastDB and Iceberg simultaneously, joining across
backends, aggregating, and writing results back — all distributed via Ray.

Catalog configuration mode used here:
  **Mode 1** — both bucket and schema fixed in VastDBConfig.
  Table identifiers are just the table name: ``catalog.get_table("my_table")``.
  See example.py for a full demonstration of all three modes.

Run via:
    ray job submit \
      --address "http://127.0.0.1:8265" \
      --working-dir . \
      --runtime-env-json '{"py_modules": ["./src/vast_daft"]}' \
      -- python examples/cross_backend_example.py
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import daft
import pyarrow as pa
from daft.io import IOConfig, S3Config
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

import vast_daft
from vast_daft import (
    VastDBCatalog,
    VastDBConfig,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ENDPOINT = "http://127.0.0.1:9998"
S3_ENDPOINT = ENDPOINT
BUCKET = "collections-bucket"
SCHEMA = "collections-schema"
ACCESS_KEY = "7P2486YDRB97497707R2"
SECRET_KEY = "JGAD1JyssLJ3KQ1G2MQp06m/BsefdeZequVb008u"

# Iceberg
ICEBERG_WAREHOUSE = f"s3://{BUCKET}/iceberg-warehouse"
ICEBERG_CATALOG_DB = "/tmp/vast_daft_cross_backend_catalog.db"
ICEBERG_NAMESPACE = "cross_backend_demo"

# Data sizes
NUM_ORDERS = 100_000
NUM_CUSTOMERS = 10_000

# Table names
VASTDB_ORDERS_TABLE = "__xbackend_orders__"
VASTDB_CUSTOMERS_TABLE = "__xbackend_customers__"
ICEBERG_PRODUCTS_TABLE = "product_catalog"
ICEBERG_ENRICHED_TABLE = "enriched_orders"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
log = logging.getLogger("cross_backend_example")

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
ORDERS_SCHEMA = pa.schema(
    [
        ("order_id", pa.int64()),
        ("customer_id", pa.int64()),
        ("product", pa.string()),
        ("amount", pa.float64()),
        ("order_date", pa.string()),
    ]
)

CUSTOMERS_SCHEMA = pa.schema(
    [
        ("customer_id", pa.int64()),
        ("name", pa.string()),
        ("tier", pa.string()),
    ]
)

ICEBERG_PRODUCTS_SCHEMA = IcebergSchema(
    NestedField(field_id=1, name="product", field_type=StringType(), required=True),
    NestedField(field_id=2, name="category", field_type=StringType(), required=True),
    NestedField(field_id=3, name="weight_kg", field_type=DoubleType(), required=False),
    NestedField(field_id=4, name="cost_price", field_type=DoubleType(), required=False),
)

ICEBERG_ENRICHED_SCHEMA = IcebergSchema(
    NestedField(field_id=1, name="order_id", field_type=LongType(), required=True),
    NestedField(field_id=2, name="customer_id", field_type=LongType(), required=True),
    NestedField(field_id=3, name="product", field_type=StringType(), required=True),
    NestedField(field_id=4, name="category", field_type=StringType(), required=True),
    NestedField(field_id=5, name="amount", field_type=DoubleType(), required=False),
    NestedField(field_id=6, name="cost_price", field_type=DoubleType(), required=False),
    NestedField(field_id=7, name="order_date", field_type=StringType(), required=False),
    NestedField(field_id=8, name="name", field_type=StringType(), required=False),
    NestedField(field_id=9, name="tier", field_type=StringType(), required=False),
)


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------
_timings: list[tuple[str, str, float]] = []


def timed(label: str, backend: str = "VastDB"):
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


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------
def make_config() -> VastDBConfig:
    return VastDBConfig(
        endpoint=ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )


def make_iceberg_catalog() -> SqlCatalog:
    return SqlCatalog(
        "vast_s3",
        **{
            "uri": f"sqlite:///{ICEBERG_CATALOG_DB}",
            "warehouse": ICEBERG_WAREHOUSE,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": ACCESS_KEY,
            "s3.secret-access-key": SECRET_KEY,
            "s3.region": "us-east-1",
            "s3.no-sign-request": "false",
            "s3.client.verify": "false",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3fs.client_kwargs": '{"verify": false}',
        },
    )


def make_io_config() -> IOConfig:
    return IOConfig(
        s3=S3Config(
            endpoint_url=S3_ENDPOINT,
            key_id=ACCESS_KEY,
            access_key=SECRET_KEY,
            region_name="us-east-1",
            use_ssl=False,
        ),
    )


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------
def generate_orders(n: int, num_customers: int) -> dict[str, Any]:
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


def generate_customers(n: int) -> dict[str, Any]:
    import random

    random.seed(42)

    tiers = ["bronze", "silver", "gold", "platinum"]
    return {
        "customer_id": list(range(1, n + 1)),
        "name": [f"customer_{i}" for i in range(1, n + 1)],
        "tier": [random.choice(tiers) for _ in range(n)],
    }


def generate_product_catalog() -> dict[str, Any]:
    return {
        "product": [
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
        ],
        "category": [
            "Widgets",
            "Widgets",
            "Gadgets",
            "Gadgets",
            "Misc",
            "Misc",
            "Contraptions",
            "Modules",
            "Sensors",
            "Adapters",
        ],
        "weight_kg": [0.5, 0.7, 1.2, 1.5, 0.3, 0.2, 2.1, 0.8, 0.1, 0.4],
        "cost_price": [15.0, 20.0, 45.0, 55.0, 8.0, 5.0, 80.0, 35.0, 12.0, 18.0],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    daft.set_runner_ray()

    config = make_config()
    vastdb_catalog = VastDBCatalog(config)
    iceberg_catalog = make_iceberg_catalog()
    io_config = make_io_config()

    print(f"\nCross-backend example: {NUM_CUSTOMERS:,} customers, {NUM_ORDERS:,} orders")
    print("Backends: VastDB (native) + Iceberg (S3-backed)")
    print("Ray cluster: distributed execution enabled\n")

    # -- Cleanup stale state from previous runs ----------------------------
    for t in [VASTDB_ORDERS_TABLE, VASTDB_CUSTOMERS_TABLE]:
        vastdb_catalog.drop_table(t)

    iceberg_catalog.create_namespace_if_not_exists(ICEBERG_NAMESPACE)
    for t in [ICEBERG_PRODUCTS_TABLE, ICEBERG_ENRICHED_TABLE]:
        fqn = f"{ICEBERG_NAMESPACE}.{t}"
        if iceberg_catalog.table_exists(fqn):
            iceberg_catalog.drop_table(fqn)

    # ==================================================================
    # STEP 1: Write orders to VastDB
    # ==================================================================
    with timed(f"Generate {NUM_ORDERS:,} orders", "Daft/Ray"):
        df_orders = daft.from_pydict(generate_orders(NUM_ORDERS, NUM_CUSTOMERS))

    with timed("Write orders to VastDB"):
        df_orders.write_vastdb(
            config=config,
            table_name=VASTDB_ORDERS_TABLE,
            table_schema=ORDERS_SCHEMA,
            create_if_missing=True,
        ).show()

    # ==================================================================
    # STEP 2: Write customers to VastDB
    # ==================================================================
    with timed(f"Generate {NUM_CUSTOMERS:,} customers", "Daft/Ray"):
        df_customers = daft.from_pydict(generate_customers(NUM_CUSTOMERS))

    with timed("Write customers to VastDB"):
        df_customers.write_vastdb(
            config=config,
            table_name=VASTDB_CUSTOMERS_TABLE,
            table_schema=CUSTOMERS_SCHEMA,
            create_if_missing=True,
        ).show()

    # ==================================================================
    # STEP 3: Write product catalog to Iceberg
    # ==================================================================
    with timed("Write product catalog to Iceberg", "Iceberg"):
        iceberg_table = iceberg_catalog.create_table(
            f"{ICEBERG_NAMESPACE}.{ICEBERG_PRODUCTS_TABLE}",
            schema=ICEBERG_PRODUCTS_SCHEMA,
        )
        df_products = daft.from_pydict(generate_product_catalog())
        df_products.write_iceberg(iceberg_table, mode="append", io_config=io_config)
        print("  Wrote 10 products to Iceberg")

    with timed("Read product catalog from Iceberg", "Iceberg"):
        iceberg_table = iceberg_catalog.load_table(f"{ICEBERG_NAMESPACE}.{ICEBERG_PRODUCTS_TABLE}")
        df_products_read = daft.read_iceberg(iceberg_table, io_config=io_config)
        df_products_read.show()

    # ==================================================================
    # STEP 4: Read orders + customers from VastDB
    # ==================================================================
    with timed("Read orders from VastDB (4 splits)"):
        df_orders_read = vast_daft.read_vastdb(
            config=config,
            table_name=VASTDB_ORDERS_TABLE,
            num_splits=4,
        )
        print("  (lazy)")

    with timed("Read customers from VastDB (4 splits)"):
        df_customers_read = vast_daft.read_vastdb(
            config=config,
            table_name=VASTDB_CUSTOMERS_TABLE,
            num_splits=4,
        )
        print("  (lazy)")

    # ==================================================================
    # STEP 5: Cross-backend join — VastDB orders x Iceberg products
    # ==================================================================
    with timed("Cross-backend join: VastDB orders x Iceberg products", "VastDB+Iceberg"):
        iceberg_table = iceberg_catalog.load_table(f"{ICEBERG_NAMESPACE}.{ICEBERG_PRODUCTS_TABLE}")
        df_products_fresh = daft.read_iceberg(iceberg_table, io_config=io_config)

        df_enriched = df_orders_read.join(
            df_products_fresh,
            on="product",
            how="inner",
        ).select(
            "order_id",
            "customer_id",
            "product",
            "category",
            "amount",
            "cost_price",
            "order_date",
        )
        print("  Sample (orders enriched with Iceberg product data):")
        df_enriched.limit(5).show()

    # ==================================================================
    # STEP 6: Three-way join — enriched orders x VastDB customers
    # ==================================================================
    with timed("Three-way join: enriched orders x VastDB customers", "VastDB+Iceberg"):
        df_full = df_enriched.join(
            df_customers_read,
            on="customer_id",
            how="inner",
        ).select(
            "order_id",
            "customer_id",
            "product",
            "category",
            "amount",
            "cost_price",
            "order_date",
            "name",
            "tier",
        )
        print("  Sample (full enrichment: order + product + customer):")
        df_full.limit(5).show()

    # ==================================================================
    # STEP 7: Aggregations on cross-backend data
    # ==================================================================
    with timed("Aggregate: margin by product category", "Daft/Ray"):
        df_margin = (
            df_full.with_column("margin", daft.col("amount") - daft.col("cost_price"))
            .groupby("category")
            .agg(
                daft.col("margin").sum().alias("total_margin"),
                daft.col("margin").mean().alias("avg_margin"),
                daft.col("order_id").count().alias("order_count"),
            )
            .sort("total_margin", desc=True)
        )
        df_margin.show()

    with timed("Aggregate: margin by tier x category", "Daft/Ray"):
        df_tier_cat = (
            df_full.with_column("margin", daft.col("amount") - daft.col("cost_price"))
            .groupby("tier", "category")
            .agg(
                daft.col("margin").sum().alias("total_margin"),
                daft.col("order_id").count().alias("order_count"),
            )
            .sort("total_margin", desc=True)
            .limit(10)
        )
        df_tier_cat.show()

    # ==================================================================
    # STEP 8: Write enriched result to Iceberg
    # ==================================================================
    with timed("Write enriched orders to Iceberg", "Iceberg"):
        iceberg_enriched = iceberg_catalog.create_table(
            f"{ICEBERG_NAMESPACE}.{ICEBERG_ENRICHED_TABLE}",
            schema=ICEBERG_ENRICHED_SCHEMA,
        )
        result = df_full.write_iceberg(iceberg_enriched, mode="append", io_config=io_config)
        result.show()

    with timed("Verify: read enriched orders from Iceberg", "Iceberg"):
        iceberg_enriched = iceberg_catalog.load_table(f"{ICEBERG_NAMESPACE}.{ICEBERG_ENRICHED_TABLE}")
        df_verify = daft.read_iceberg(iceberg_enriched, io_config=io_config)
        row_count = df_verify.count().collect().to_pydict()["count"][0]
        print(f"  Total enriched rows: {row_count:,}")
        df_verify.limit(5).show()

    # ==================================================================
    # STEP 9: Cleanup
    # ==================================================================
    with timed("Cleanup VastDB tables"):
        for t in [VASTDB_ORDERS_TABLE, VASTDB_CUSTOMERS_TABLE]:
            vastdb_catalog.drop_table(t)
            print(f"  Dropped VastDB: {t}")

    with timed("Cleanup Iceberg tables", "Iceberg"):
        for t in [ICEBERG_ENRICHED_TABLE, ICEBERG_PRODUCTS_TABLE]:
            fqn = f"{ICEBERG_NAMESPACE}.{t}"
            if iceberg_catalog.table_exists(fqn):
                iceberg_catalog.drop_table(fqn)
                print(f"  Dropped Iceberg: {fqn}")
        db_path = Path(ICEBERG_CATALOG_DB)
        if db_path.exists():
            db_path.unlink()
            print(f"  Removed catalog DB: {ICEBERG_CATALOG_DB}")

    # ==================================================================
    # Summary
    # ==================================================================
    print_summary_table()
    print("\nDone — cross-backend execution complete.")


if __name__ == "__main__":
    main()
