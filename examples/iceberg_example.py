#!/usr/bin/env python3
"""Example: Iceberg table CRUD via Daft on VastDB's S3-compatible storage.

This script demonstrates pure Daft + PyIceberg operations against VastDB's
S3 endpoint (no VastDB SDK — just standard S3):

  1. Create an Iceberg table backed by S3
  2. Insert data using Daft's write_iceberg
  3. Read / query the table using Daft's read_iceberg
  4. Update (overwrite) the table with modified data
  5. Verify the update by reading again
  6. Cleanup

Prerequisites:
  - A `.env` file in the project root with S3_ACCESS_KEY and S3_SECRET_KEY
  - VastDB S3-compatible endpoint at 127.0.0.1:9998
  - `uv run python examples/iceberg_example.py` from the project root
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import daft
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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
S3_ENDPOINT = "http://127.0.0.1:9998"
BUCKET = "collections-bucket"
WAREHOUSE_PATH = f"s3://{BUCKET}/iceberg-warehouse"
CATALOG_DB = "/tmp/vast_daft_iceberg_catalog.db"
NAMESPACE = "examples"
TABLE_NAME = "users"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
log = logging.getLogger("iceberg_example")


# ---------------------------------------------------------------------------
# Setup
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


def make_catalog(access_key: str, secret_key: str) -> SqlCatalog:
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
            # Disable SSL verification for local endpoint
            "s3.client.verify": "false",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3fs.client_kwargs": '{"verify": false}',
        },
    )


def make_io_config(access_key: str, secret_key: str) -> IOConfig:
    """Build a Daft IOConfig pointing at the same S3 endpoint."""
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
# Iceberg schema
# ---------------------------------------------------------------------------
ICEBERG_SCHEMA = IcebergSchema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=True),
    NestedField(field_id=2, name="name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="email", field_type=StringType(), required=False),
    NestedField(field_id=4, name="score", field_type=DoubleType(), required=False),
)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
def main() -> None:
    access_key, secret_key = load_credentials()
    catalog = make_catalog(access_key, secret_key)
    io_config = make_io_config(access_key, secret_key)

    full_table_name = f"{NAMESPACE}.{TABLE_NAME}"

    # ---- 0. Setup namespace ------------------------------------------------
    print("\n=== Step 0: Create Namespace ===")
    catalog.create_namespace_if_not_exists(NAMESPACE)
    print(f"  Namespace {NAMESPACE!r} ready")
    print(f"  Namespaces: {catalog.list_namespaces()}")

    # ---- 1. Create Iceberg table -------------------------------------------
    print("\n=== Step 1: Create Iceberg Table ===")

    # Drop if leftover from a previous run
    if catalog.table_exists(full_table_name):
        catalog.drop_table(full_table_name)
        log.info("Dropped leftover table %s", full_table_name)

    iceberg_table = catalog.create_table(
        full_table_name,
        schema=ICEBERG_SCHEMA,
    )
    print(f"  Created Iceberg table: {full_table_name}")
    print(f"  Location: {iceberg_table.location()}")
    print(f"  Schema:\n{iceberg_table.schema()}")

    # ---- 2. Insert data ----------------------------------------------------
    print("\n=== Step 2: Insert Data ===")

    df_insert = daft.from_pydict(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "email": [
                "alice@example.com",
                "bob@example.com",
                "charlie@example.com",
                "diana@example.com",
                "eve@example.com",
            ],
            "score": [92.5, 87.3, 95.1, 78.9, 91.0],
        }
    )

    print("  Data to insert:")
    df_insert.show()

    result = df_insert.write_iceberg(iceberg_table, mode="append", io_config=io_config)
    print("  Write result:")
    result.show()

    # ---- 3. Read / Select --------------------------------------------------
    print("\n=== Step 3: Read Table ===")

    # Reload table to pick up new snapshot
    iceberg_table = catalog.load_table(full_table_name)

    df_read = daft.read_iceberg(iceberg_table, io_config=io_config)
    print("  Full table:")
    df_read.show()

    # Filtered read — only high scorers
    print("  Filtered (score > 90):")
    df_read.where(daft.col("score") > daft.lit(90.0)).show()

    # Projection
    print("  Projection (name, score):")
    df_read.select("name", "score").show()

    # ---- 4. Update (overwrite with modified data) --------------------------
    print("\n=== Step 4: Update (Overwrite) ===")

    # Read current data, bump all scores by 5
    df_current = daft.read_iceberg(iceberg_table, io_config=io_config).collect()
    df_updated = df_current.with_column(
        "score",
        daft.col("score") + 5.0,
    )
    print("  Updated data (scores +5):")
    df_updated.show()

    # Overwrite the table
    iceberg_table = catalog.load_table(full_table_name)
    result = df_updated.write_iceberg(iceberg_table, mode="overwrite", io_config=io_config)
    print("  Overwrite result:")
    result.show()

    # ---- 5. Verify update --------------------------------------------------
    print("\n=== Step 5: Verify Update ===")

    iceberg_table = catalog.load_table(full_table_name)
    df_verify = daft.read_iceberg(iceberg_table, io_config=io_config)
    print("  Table after update:")
    df_verify.show()

    # ---- 6. Cleanup --------------------------------------------------------
    print("\n=== Step 6: Cleanup ===")
    catalog.drop_table(full_table_name)
    print(f"  Dropped table {full_table_name}")

    # Remove local catalog DB
    db_path = Path(CATALOG_DB)
    if db_path.exists():
        db_path.unlink()
        print(f"  Removed catalog DB {CATALOG_DB}")

    print("\nDone.")


if __name__ == "__main__":
    main()
