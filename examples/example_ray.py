#!/usr/bin/env python3
"""Example: Read and write VastDB tables using vast-daft.

This script demonstrates:
  1. Table discovery  — list tables in a VastDB schema
  2. Schema discovery — detect column types at runtime (no hardcoded schema)
  3. Reading          — use VastDBDataSource to read into a Daft DataFrame
  4. Writing          — use VastDBDataSink to write a Daft DataFrame back
  5. Table management — create, check existence, row count, and drop tables

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

# $ ray job submit \
#   --address "http://127.0.0.1:8265" \
#   --working-dir . \
#   --runtime-env-json '{"py_modules": ["./src/vast_daft"]}' \
#   -- python examples/example_ray.py 2>&1

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ENDPOINT = "http://vippool.ie-dev-pipeline.svc.cluster.local"
BUCKET = "collections-bucket"
SCHEMA = "collections-schema"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
log = logging.getLogger("example")


def load_config() -> VastDBConfig:
    """Load credentials from .env and build a VastDBConfig."""
    S3_ACCESS_KEY="7P2486YDRB97497707R2"
    S3_SECRET_KEY="JGAD1JyssLJ3KQ1G2MQp06m/BsefdeZequVb008u"

    return VastDBConfig(
        endpoint=ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )


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

    # Create
    catalog.create_table_if_not_exists(demo_table, Schema.from_pyarrow_schema(demo_schema))
    print(f"  Created table {demo_table!r}")

    # Exists
    exists = catalog.has_table(demo_table)
    print(f"  Exists? {exists}")
    # Row count
    count = catalog.read_table(demo_table).collect().count_rows()
    print(f"  Row count: {count}")

    # Drop
    catalog.drop_table(demo_table)
    print(f"  Dropped {demo_table!r}")
    print(f"  Exists after drop? {catalog.has_table(demo_table)}")


def demo_read(config: VastDBConfig, table_name: str, schema: pa.Schema) -> None:
    """Read from VastDB using the Daft DataSource."""
    print(f"\n--- Read Demo (table={table_name!r}) ---")

    source = VastDBDataSource(
        config=config,
        table_name=table_name,
        table_schema=schema,
        limit=20,
    )
    df = source.read()
    df.show()


def demo_write(config: VastDBConfig, catalog: VastDBCatalog) -> None:
    """Write to VastDB using the Daft DataSink."""
    demo_table = "__vast_daft_write_example__"
    demo_schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("score", pa.float64()),
        ]
    )

    print(f"\n--- Write Demo (table={demo_table!r}) ---")

    # Build a small Daft DataFrame
    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["alice", "bob", "charlie"],
            "score": [0.95, 0.87, 0.72],
        }
    )
    print("  Data to write:")
    df.show()

    # Write via the sink
    sink = VastDBDataSink(
        config=config,
        table_name=demo_table,
        table_schema=demo_schema,
        create_if_missing=True,
    )
    result = df.write_sink(sink)
    print("  Write result:")
    result.show()

    # Read back to verify
    print("  Reading back:")
    read_source = VastDBDataSource(
        config=config,
        table_name=demo_table,
        table_schema=demo_schema,
    )
    read_source.read().show()

    # Cleanup
    catalog.drop_table(demo_table)
    print(f"  Cleaned up {demo_table!r}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    config = load_config()
    catalog = VastDBCatalog(config)

    # 1. List tables
    print("\n=== Step 1: Table Discovery ===")
    tables = catalog.list_tables()
    if not tables:
        print("No tables found — running write + table-management demos only.\n")
        demo_table_management(catalog)
        demo_write(config, catalog)
        return

    print(f"Found {len(tables)} table(s) in {BUCKET}/{SCHEMA}")

    # 2. Pick a table & discover its schema
    table_name = "chunks"

    print(f"\n=== Step 2: Schema Discovery ({table_name!r}) ===")
    schema = catalog.get_table(table_name).schema().to_pyarrow_schema()
    print_schema(schema)

    # 3. Read from the selected table
    print("\n=== Step 3: Read Demo ===")
    demo_read(config, table_name, schema)

    # 4. Write demo (separate table)
    print("\n=== Step 4: Write Demo ===")
    demo_write(config, catalog)

    # 5. Table management demo
    print("\n=== Step 5: Table Management Demo ===")
    demo_table_management(catalog)

    print("\nDone.")


if __name__ == "__main__":
    main()
