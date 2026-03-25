#!/usr/bin/env python3
"""Example: Cross-engine join — VastDB table + Iceberg table via Daft.

This script demonstrates a real-world pattern where data lives in two
different storage engines and needs to be joined and persisted back to
both:

  1. Create table **customers** on VastDB (via vast-daft connector)
  2. Create table **orders** on S3-backed Iceberg (via PyIceberg + Daft)
  3. Read both tables into Daft DataFrames
  4. Join them (customers ⨝ orders on customer_id)
  5. Save the joined result to **both** VastDB and Iceberg

Prerequisites:
  - A `.env` file in the project root with S3_ACCESS_KEY and S3_SECRET_KEY
  - VastDB accessible at the configured endpoint (127.0.0.1:9998)
  - `uv run python examples/combined_example.py` from the project root
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import daft
import pyarrow as pa
from daft.io import IOConfig, S3Config
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
)

from vast_daft import (
    TableManager,
    VastDBConfig,
    VastDBDataSink,
    VastDBDataSource,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ENDPOINT = "127.0.0.1:9998"
S3_ENDPOINT = "http://127.0.0.1:9998"
BUCKET = "collections-bucket"
SCHEMA = "collections-schema"

WAREHOUSE_PATH = f"s3://{BUCKET}/iceberg-warehouse"
CATALOG_DB = "/tmp/vast_daft_combined_catalog.db"
ICE_NAMESPACE = "combined_example"

# Table names
VAST_CUSTOMERS_TABLE = "__combined_customers__"
ICE_ORDERS_TABLE = "orders"
VAST_JOINED_TABLE = "__combined_joined__"
ICE_JOINED_TABLE = "joined_customer_orders"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
log = logging.getLogger("combined_example")


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
# PyArrow schemas (used by VastDB connector)
CUSTOMERS_SCHEMA = pa.schema(
    [
        ("customer_id", pa.int64()),
        ("name", pa.utf8()),
        ("email", pa.utf8()),
        ("tier", pa.utf8()),
    ]
)

ORDERS_SCHEMA = pa.schema(
    [
        ("order_id", pa.int64()),
        ("customer_id", pa.int64()),
        ("product", pa.utf8()),
        ("amount", pa.float64()),
        ("order_date", pa.utf8()),
    ]
)

JOINED_SCHEMA = pa.schema(
    [
        ("customer_id", pa.int64()),
        ("name", pa.utf8()),
        ("email", pa.utf8()),
        ("tier", pa.utf8()),
        ("order_id", pa.int64()),
        ("product", pa.utf8()),
        ("amount", pa.float64()),
        ("order_date", pa.utf8()),
    ]
)

# Iceberg schemas
ICE_ORDERS_SCHEMA = IcebergSchema(
    NestedField(field_id=1, name="order_id", field_type=LongType(), required=True),
    NestedField(field_id=2, name="customer_id", field_type=LongType(), required=True),
    NestedField(field_id=3, name="product", field_type=StringType(), required=True),
    NestedField(field_id=4, name="amount", field_type=DoubleType(), required=False),
    NestedField(field_id=5, name="order_date", field_type=StringType(), required=False),
)

ICE_JOINED_SCHEMA = IcebergSchema(
    NestedField(field_id=1, name="customer_id", field_type=LongType(), required=True),
    NestedField(field_id=2, name="name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="email", field_type=StringType(), required=False),
    NestedField(field_id=4, name="tier", field_type=StringType(), required=False),
    NestedField(field_id=5, name="order_id", field_type=LongType(), required=True),
    NestedField(field_id=6, name="product", field_type=StringType(), required=True),
    NestedField(field_id=7, name="amount", field_type=DoubleType(), required=False),
    NestedField(field_id=8, name="order_date", field_type=StringType(), required=False),
)


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------
CUSTOMERS_DATA = {
    "customer_id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "email": [
        "alice@example.com",
        "bob@example.com",
        "charlie@example.com",
        "diana@example.com",
        "eve@example.com",
    ],
    "tier": ["gold", "silver", "gold", "bronze", "gold"],
}

ORDERS_DATA = {
    "order_id": [101, 102, 103, 104, 105, 106, 107],
    "customer_id": [1, 2, 1, 3, 5, 2, 4],
    "product": [
        "Widget A",
        "Widget B",
        "Gadget X",
        "Widget A",
        "Gadget Y",
        "Widget A",
        "Gadget X",
    ],
    "amount": [29.99, 49.99, 149.99, 29.99, 199.99, 29.99, 149.99],
    "order_date": [
        "2025-01-15",
        "2025-01-16",
        "2025-01-17",
        "2025-02-01",
        "2025-02-10",
        "2025-02-15",
        "2025-03-01",
    ],
}


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------
def load_credentials() -> tuple[str, str]:
    """Load S3 credentials from .env."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        sys.exit(f"ERROR: .env file not found at {env_path}")

    load_dotenv(env_path)

    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    if not access_key or not secret_key:
        sys.exit("ERROR: S3_ACCESS_KEY and S3_SECRET_KEY must be set in .env")

    return access_key, secret_key


def make_vast_config(access_key: str, secret_key: str) -> VastDBConfig:
    """Build VastDB connector config."""
    return VastDBConfig(
        endpoint=ENDPOINT,
        access_key=access_key,
        secret_key=secret_key,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )


def make_iceberg_catalog(access_key: str, secret_key: str) -> SqlCatalog:
    """Create a PyIceberg SQL catalog backed by local SQLite + S3 storage."""
    return SqlCatalog(
        "vast_s3",
        **{
            "uri": f"sqlite:///{CATALOG_DB}",
            "warehouse": WAREHOUSE_PATH,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
            "s3.region": "us-east-1",
            "s3.no-sign-request": "false",
            "s3.client.verify": "false",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3fs.client_kwargs": '{"verify": false}',
        },
    )


def make_io_config(access_key: str, secret_key: str) -> IOConfig:
    """Build a Daft IOConfig for S3 access."""
    return IOConfig(
        s3=S3Config(
            endpoint_url=S3_ENDPOINT,
            key_id=access_key,
            access_key=secret_key,
            region_name="us-east-1",
            use_ssl=False,
        ),
    )


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
def main() -> None:
    access_key, secret_key = load_credentials()

    vast_config = make_vast_config(access_key, secret_key)
    tm = TableManager(vast_config)
    catalog = make_iceberg_catalog(access_key, secret_key)
    io_config = make_io_config(access_key, secret_key)

    ice_orders_fqn = f"{ICE_NAMESPACE}.{ICE_ORDERS_TABLE}"
    ice_joined_fqn = f"{ICE_NAMESPACE}.{ICE_JOINED_TABLE}"

    # ==================================================================
    # Step 1: Create "customers" table on VastDB and insert data
    # ==================================================================
    print("\n" + "=" * 60)
    print("Step 1: Create CUSTOMERS table on VastDB")
    print("=" * 60)

    # Clean up from previous runs
    tm.drop_table(VAST_CUSTOMERS_TABLE)
    tm.drop_table(VAST_JOINED_TABLE)

    # Write customers to VastDB via the DataSink
    df_customers = daft.from_pydict(CUSTOMERS_DATA)
    print("\n  Customer data:")
    df_customers.show()

    sink = VastDBDataSink(
        config=vast_config,
        table_name=VAST_CUSTOMERS_TABLE,
        table_schema=CUSTOMERS_SCHEMA,
        create_if_missing=True,
    )
    result = df_customers.write_sink(sink)
    print("  Write result:")
    result.show()

    # ==================================================================
    # Step 2: Create "orders" table on Iceberg and insert data
    # ==================================================================
    print("\n" + "=" * 60)
    print("Step 2: Create ORDERS table on Iceberg (S3)")
    print("=" * 60)

    catalog.create_namespace_if_not_exists(ICE_NAMESPACE)

    # Drop if leftover
    if catalog.table_exists(ice_orders_fqn):
        catalog.drop_table(ice_orders_fqn)
    if catalog.table_exists(ice_joined_fqn):
        catalog.drop_table(ice_joined_fqn)

    iceberg_orders = catalog.create_table(ice_orders_fqn, schema=ICE_ORDERS_SCHEMA)
    print(f"\n  Created Iceberg table: {ice_orders_fqn}")
    print(f"  Location: {iceberg_orders.location()}")

    df_orders = daft.from_pydict(ORDERS_DATA)
    print("\n  Order data:")
    df_orders.show()

    write_result = df_orders.write_iceberg(iceberg_orders, mode="append", io_config=io_config)
    print("  Write result:")
    write_result.show()

    # ==================================================================
    # Step 3: Read both tables back into Daft DataFrames
    # ==================================================================
    print("\n" + "=" * 60)
    print("Step 3: Read both tables into Daft")
    print("=" * 60)

    # Read customers from VastDB
    print("\n  --- Customers (from VastDB) ---")
    source = VastDBDataSource(
        config=vast_config,
        table_name=VAST_CUSTOMERS_TABLE,
        table_schema=CUSTOMERS_SCHEMA,
    )
    df_vast_customers = source.read()
    df_vast_customers.show()

    # Read orders from Iceberg
    print("\n  --- Orders (from Iceberg) ---")
    iceberg_orders = catalog.load_table(ice_orders_fqn)
    df_ice_orders = daft.read_iceberg(iceberg_orders, io_config=io_config)
    df_ice_orders.show()

    # ==================================================================
    # Step 4: Join customers (VastDB) with orders (Iceberg)
    # ==================================================================
    print("\n" + "=" * 60)
    print("Step 4: Join customers x orders on customer_id")
    print("=" * 60)

    # Collect the VastDB source so both sides are materialised
    df_vast_customers = df_vast_customers.collect()

    df_joined = df_vast_customers.join(
        df_ice_orders,
        on="customer_id",
        how="inner",
    )

    # Select columns in a deterministic order
    df_joined = df_joined.select(
        "customer_id",
        "name",
        "email",
        "tier",
        "order_id",
        "product",
        "amount",
        "order_date",
    )

    print("\n  Joined result (customers x orders):")
    df_joined.show()

    # Quick analytics on the joined data
    print("\n  Revenue by tier:")
    df_joined.groupby("tier").agg(
        daft.col("amount").sum().alias("total_revenue"),
        daft.col("order_id").count().alias("order_count"),
    ).sort("total_revenue", desc=True).show()

    print("\n  Revenue by customer:")
    df_joined.groupby("name").agg(
        daft.col("amount").sum().alias("total_spent"),
        daft.col("order_id").count().alias("num_orders"),
    ).sort("total_spent", desc=True).show()

    # ==================================================================
    # Step 5a: Save joined result to VastDB
    # ==================================================================
    print("\n" + "=" * 60)
    print("Step 5a: Save joined result to VastDB")
    print("=" * 60)

    joined_sink = VastDBDataSink(
        config=vast_config,
        table_name=VAST_JOINED_TABLE,
        table_schema=JOINED_SCHEMA,
        create_if_missing=True,
    )
    result = df_joined.write_sink(joined_sink)
    print("  Write result:")
    result.show()

    # Verify by reading back
    print("\n  Verification — read back from VastDB:")
    verify_source = VastDBDataSource(
        config=vast_config,
        table_name=VAST_JOINED_TABLE,
        table_schema=JOINED_SCHEMA,
    )
    verify_source.read().show()

    # ==================================================================
    # Step 5b: Save joined result to Iceberg
    # ==================================================================
    print("\n" + "=" * 60)
    print("Step 5b: Save joined result to Iceberg")
    print("=" * 60)

    iceberg_joined = catalog.create_table(ice_joined_fqn, schema=ICE_JOINED_SCHEMA)
    print(f"  Created Iceberg table: {ice_joined_fqn}")

    write_result = df_joined.write_iceberg(iceberg_joined, mode="append", io_config=io_config)
    print("  Write result:")
    write_result.show()

    # Verify by reading back
    print("\n  Verification — read back from Iceberg:")
    iceberg_joined = catalog.load_table(ice_joined_fqn)
    daft.read_iceberg(iceberg_joined, io_config=io_config).show()

    # ==================================================================
    # Step 6: Cleanup
    # ==================================================================
    print("\n" + "=" * 60)
    print("Step 6: Cleanup")
    print("=" * 60)

    # VastDB tables
    tm.drop_table(VAST_CUSTOMERS_TABLE)
    print(f"  Dropped VastDB table: {VAST_CUSTOMERS_TABLE}")
    tm.drop_table(VAST_JOINED_TABLE)
    print(f"  Dropped VastDB table: {VAST_JOINED_TABLE}")

    # Iceberg tables
    catalog.drop_table(ice_orders_fqn)
    print(f"  Dropped Iceberg table: {ice_orders_fqn}")
    catalog.drop_table(ice_joined_fqn)
    print(f"  Dropped Iceberg table: {ice_joined_fqn}")

    # Local catalog DB
    db_path = Path(CATALOG_DB)
    if db_path.exists():
        db_path.unlink()
        print(f"  Removed catalog DB: {CATALOG_DB}")

    print("\nDone.")


if __name__ == "__main__":
    main()
