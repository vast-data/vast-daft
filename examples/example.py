#!/usr/bin/env python3
"""Example: Read and write VastDB tables using vast-daft.

This script demonstrates three catalog configuration modes that mirror the
Unity Catalog / Iceberg identifier convention:

  Mode 1 — both bucket and schema fixed in config (backward-compatible):
      config = VastDBConfig(..., bucket="b", schema="s")
      catalog.get_table("my_table")          # just the table name
      catalog.get_table("ns.my_table")       # sub-schema + table

  Mode 2 — bucket fixed, schema supplied in the identifier:
      config = VastDBConfig(..., bucket="b")
      catalog.get_table("my_schema.my_table")

  Mode 3 — neither bucket nor schema fixed (fully-qualified identifiers):
      config = VastDBConfig(...)
      catalog.get_table("my_bucket.my_schema.my_table")

The demo below uses Mode 1 (both fixed) for the main workflow, then shows
short Mode 2 and Mode 3 examples using the same VastDB endpoint.

Prerequisites:
  - A `.env` file in the project root with S3_ACCESS_KEY and S3_SECRET_KEY
  - VastDB accessible at the configured endpoint
  - `uv run python examples/example.py` from the project root
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import daft
import pyarrow as pa
from dotenv import load_dotenv

from vast_daft import (
    VastDBCatalog,
    VastDBConfig,
    VastDBDataSink,
    VastDBDataSource,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ENDPOINT = "127.0.0.1:9998"
BUCKET = "collections-bucket"
SCHEMA = "collections-schema"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
log = logging.getLogger("example")


def load_credentials() -> tuple[str, str]:
    """Load S3_ACCESS_KEY and S3_SECRET_KEY from .env."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        sys.exit(f"ERROR: .env file not found at {env_path}")
    load_dotenv(env_path)
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    if not access_key or not secret_key:
        sys.exit("ERROR: S3_ACCESS_KEY and S3_SECRET_KEY must be set in .env")
    return access_key, secret_key


def print_schema(schema: pa.Schema) -> None:
    """Pretty-print a PyArrow schema."""
    print(f"\n  Columns ({len(schema)}):")
    for field in schema:
        print(f"    - {field.name}: {field.type}")


# ---------------------------------------------------------------------------
# Demo functions
# ---------------------------------------------------------------------------


def demo_table_management(catalog: VastDBCatalog) -> None:
    """Show table management: create, exists, row count, drop."""
    from daft.schema import Schema

    demo_table = "__vast_daft_example__"
    demo_schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("score", pa.float64()),
        ]
    )

    print("\n--- Table Management Demo ---")

    catalog.create_table_if_not_exists(demo_table, Schema.from_pyarrow_schema(demo_schema))
    print(f"  Created table {demo_table!r}")

    exists = catalog.has_table(demo_table)
    print(f"  Exists? {exists}")

    count = catalog.read_table(demo_table).collect().count_rows()
    print(f"  Row count: {count}")

    catalog.drop_table(demo_table)
    print(f"  Dropped {demo_table!r}")
    print(f"  Exists after drop? {catalog.has_table(demo_table)}")


def demo_read(catalog: VastDBCatalog, table_name: str) -> None:
    """Read from VastDB via the catalog (works in all 3 config modes)."""
    print(f"\n--- Read Demo (table={table_name!r}) ---")
    catalog.read_table(table_name).limit(20).show()


def demo_write(config: VastDBConfig, catalog: VastDBCatalog) -> None:
    """Write to VastDB using the Daft DataSink, then read back."""
    demo_table = "__vast_daft_write_example__"
    demo_schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("score", pa.float64()),
        ]
    )

    print(f"\n--- Write Demo (table={demo_table!r}) ---")

    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["alice", "bob", "charlie"],
            "score": [0.95, 0.87, 0.72],
        }
    )
    print("  Data to write:")
    df.show()

    sink = VastDBDataSink(
        config=config,
        table_name=demo_table,
        table_schema=demo_schema,
        create_if_missing=True,
    )
    result = df.write_sink(sink)
    print("  Write result:")
    result.show()

    print("  Reading back:")
    VastDBDataSource(
        config=config,
        table_name=demo_table,
        table_schema=demo_schema,
    ).read().show()

    catalog.drop_table(demo_table)
    print(f"  Cleaned up {demo_table!r}")


# ---------------------------------------------------------------------------
# Mode 2 demo: bucket fixed, schema from identifier
# ---------------------------------------------------------------------------


def demo_mode2(access_key: str, secret_key: str) -> None:
    """Mode 2: config has bucket only; schema is part of the identifier.

    Table identifiers must be "schema.table" (or "schema.ns.table").
    """
    print("\n=== Mode 2: bucket fixed, schema in identifier ===")

    # Config — no schema set
    config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=access_key,
        secret_key=secret_key,
        bucket=BUCKET,
        ssl_verify=False,
    )
    catalog = VastDBCatalog(config, alias="vastdb-bucket")

    demo_table = "__mode2_example__"
    demo_schema = pa.schema([("id", pa.int64()), ("label", pa.string())])

    from daft.schema import Schema

    # Create via "schema.table" identifier
    full_ident = f"{SCHEMA}.{demo_table}"
    catalog.create_table_if_not_exists(full_ident, Schema.from_pyarrow_schema(demo_schema))
    print(f"  Created: {full_ident!r}")

    # Write via DataSink with explicit schema kwarg
    df = daft.from_pydict({"id": [10, 20], "label": ["x", "y"]})
    sink = VastDBDataSink(
        config=config,
        table_name=demo_table,
        table_schema=demo_schema,
        schema=SCHEMA,  # schema supplied explicitly
        create_if_missing=False,
    )
    df.write_sink(sink).show()

    # Read via catalog using "schema.table"
    print(f"  Reading {full_ident!r}:")
    catalog.read_table(full_ident).show()

    # List tables — returns "schema.table" identifiers
    tables = catalog.list_tables()
    print(f"  Tables (schema-qualified): {[str(t) for t in tables]}")

    catalog.drop_table(full_ident)
    print(f"  Dropped: {full_ident!r}")


# ---------------------------------------------------------------------------
# Mode 3 demo: no bucket or schema fixed — fully-qualified identifiers
# ---------------------------------------------------------------------------


def demo_mode3(access_key: str, secret_key: str) -> None:
    """Mode 3: config has neither bucket nor schema.

    Table identifiers must be "bucket.schema.table".
    """
    print("\n=== Mode 3: no bucket/schema in config — fully-qualified identifiers ===")

    # Config — no bucket, no schema
    config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=access_key,
        secret_key=secret_key,
        ssl_verify=False,
    )
    catalog = VastDBCatalog(config, alias="vastdb")

    demo_table = "__mode3_example__"
    demo_schema = pa.schema([("id", pa.int64()), ("value", pa.float64())])

    from daft.schema import Schema

    full_ident = f"{BUCKET}.{SCHEMA}.{demo_table}"
    catalog.create_table_if_not_exists(full_ident, Schema.from_pyarrow_schema(demo_schema))
    print(f"  Created: {full_ident!r}")

    # Write via DataSink with both bucket and schema kwarg
    df = daft.from_pydict({"id": [1], "value": [3.14]})
    sink = VastDBDataSink(
        config=config,
        table_name=demo_table,
        table_schema=demo_schema,
        bucket=BUCKET,
        schema=SCHEMA,
        create_if_missing=False,
    )
    df.write_sink(sink).show()

    print(f"  Reading {full_ident!r}:")
    catalog.read_table(full_ident).show()

    catalog.drop_table(full_ident)
    print(f"  Dropped: {full_ident!r}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    access_key, secret_key = load_credentials()

    # ------------------------------------------------------------------
    # Mode 1: both bucket and schema fixed (backward-compatible)
    # ------------------------------------------------------------------
    print("\n=== Mode 1: bucket + schema fixed in config (backward-compatible) ===")
    config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=access_key,
        secret_key=secret_key,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )
    catalog = VastDBCatalog(config)

    # 1. List tables
    print("\n--- Step 1: Table Discovery ---")
    tables = catalog.list_tables()
    if not tables:
        print("No tables found — running write + table-management demos only.")
        demo_table_management(catalog)
        demo_write(config, catalog)
    else:
        print(f"Found {len(tables)} table(s) in {BUCKET}/{SCHEMA}")

        table_name = str(tables[0])
        print(f"\n--- Step 2: Schema Discovery ({table_name!r}) ---")
        schema = catalog.get_table(table_name).schema().to_pyarrow_schema()
        print_schema(schema)

        print("\n--- Step 3: Read Demo ---")
        demo_read(catalog, table_name)

        print("\n--- Step 4: Write Demo ---")
        demo_write(config, catalog)

        print("\n--- Step 5: Table Management Demo ---")
        demo_table_management(catalog)

    # ------------------------------------------------------------------
    # Mode 2: bucket fixed, schema in identifier
    # ------------------------------------------------------------------
    demo_mode2(access_key, secret_key)

    # ------------------------------------------------------------------
    # Mode 3: no bucket/schema fixed — fully-qualified identifiers
    # ------------------------------------------------------------------
    demo_mode3(access_key, secret_key)

    print("\nDone.")


if __name__ == "__main__":
    main()
