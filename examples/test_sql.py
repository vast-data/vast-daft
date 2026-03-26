#!/usr/bin/env python3
"""Local test for daft.sql() with VastDB via attached catalog.

Demonstrates all three catalog configuration modes:

  Mode 1 — both bucket and schema fixed (backward-compatible):
      config = VastDBConfig(..., bucket="b", schema="s")
      FROM vastdb.<table>

  Mode 2 — bucket fixed, schema supplied in the identifier:
      config = VastDBConfig(..., bucket="b")
      FROM vastdb."schema.table"   (or use a catalog alias + schema prefix)

  Mode 3 — neither bucket nor schema fixed:
      config = VastDBConfig(...)
      FROM vastdb."bucket.schema.table"

Run from the project root:
    uv run python examples/test_sql.py
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
)

ENDPOINT = "127.0.0.1:9998"
BUCKET = "collections-bucket"
SCHEMA = "collections-schema"
DEMO_TABLE = "__sql_daft_test__"
CATALOG_ALIAS = "vastdb"


def sql_id(name: str) -> str:
    """Quote a SQL identifier with double-quotes if it contains special chars.

    Hyphens and other non-alphanumeric/underscore characters are not valid in
    bare SQL identifiers, so names like 'collections-schema' must be written as
    '"collections-schema"' in SQL.
    """
    if name.replace("_", "").isalnum():
        return name
    return f'"{name}"'


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
log = logging.getLogger("test_sql")


def load_credentials() -> tuple[str, str]:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        sys.exit(f"ERROR: .env not found at {env_path}")
    load_dotenv(env_path)
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    if not access_key or not secret_key:
        sys.exit("ERROR: S3_ACCESS_KEY and S3_SECRET_KEY must be set in .env")
    return access_key, secret_key


ORDERS_SCHEMA = pa.schema(
    [
        ("order_id", pa.int64()),
        ("customer_name", pa.string()),
        ("tier", pa.string()),
        ("product", pa.string()),
        ("amount", pa.float64()),
        ("order_date", pa.string()),
    ]
)


def seed_table(config: VastDBConfig, table_name: str, n: int = 1_000) -> None:
    """Write n random order rows into *table_name* (drops first if exists)."""
    import random

    random.seed(42)
    products = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Thingamajig"]
    tiers = ["bronze", "silver", "gold", "platinum"]

    catalog = VastDBCatalog(config)
    catalog.drop_table_if_exists(table_name)

    df = daft.from_pydict(
        {
            "order_id": list(range(1, n + 1)),
            "customer_name": [f"customer_{random.randint(1, 100)}" for _ in range(n)],
            "tier": [random.choice(tiers) for _ in range(n)],
            "product": [random.choice(products) for _ in range(n)],
            "amount": [round(random.uniform(5.0, 500.0), 2) for _ in range(n)],
            "order_date": [f"2025-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}" for _ in range(n)],
        }
    )
    sink = VastDBDataSink(
        config=config,
        table_name=table_name,
        table_schema=ORDERS_SCHEMA,
        create_if_missing=True,
    )
    df.write_sink(sink).show()
    print(f"Seeded {n:,} rows into {table_name!r}")


# ---------------------------------------------------------------------------
# Mode 1: both bucket and schema fixed
# ---------------------------------------------------------------------------


def demo_mode1(access_key: str, secret_key: str) -> None:
    """Mode 1 — bucket + schema fixed in config (backward-compatible).

    Table identifiers are just the table name; SQL FROM uses ``alias.<table>``.
    """
    print("\n" + "=" * 60)
    print("Mode 1: bucket + schema fixed in config")
    print("=" * 60)

    config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=access_key,
        secret_key=secret_key,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )

    print("\n=== Seeding demo table ===")
    seed_table(config, DEMO_TABLE)

    # -- Option A: no alias — catalog.name becomes the SQL prefix ------------
    catalog = VastDBCatalog(config)
    print(f"\n=== Attach without alias (catalog.name = {catalog.name!r}) ===")
    daft.attach_catalog(catalog)
    prefix = sql_id(catalog.name)

    print("\n--- Revenue by tier ---")
    daft.sql(f"""
        SELECT tier, COUNT(*) AS order_count,
               ROUND(SUM(amount), 2) AS total_revenue
        FROM {prefix}.{DEMO_TABLE}
        GROUP BY tier ORDER BY total_revenue DESC
    """).show()

    daft.detach_catalog(catalog.name)

    # -- Option B: short alias -----------------------------------------------
    catalog_aliased = VastDBCatalog(config, alias=CATALOG_ALIAS)
    print(f"\n=== Attach with alias {CATALOG_ALIAS!r} ===")
    daft.attach_catalog(catalog_aliased, CATALOG_ALIAS)

    print("\n--- Top products by revenue ---")
    daft.sql(f"""
        SELECT product, COUNT(*) AS num_orders,
               ROUND(SUM(amount), 2) AS total_revenue
        FROM {CATALOG_ALIAS}.{DEMO_TABLE}
        GROUP BY product ORDER BY total_revenue DESC
    """).show()

    print("\n--- Top 5 gold/platinum customers ---")
    daft.sql(f"""
        SELECT customer_name, tier, ROUND(SUM(amount), 2) AS total_spent
        FROM {CATALOG_ALIAS}.{DEMO_TABLE}
        WHERE tier IN ('gold', 'platinum')
        GROUP BY customer_name, tier
        ORDER BY total_spent DESC LIMIT 5
    """).show()

    daft.detach_catalog(CATALOG_ALIAS)

    # Cleanup
    VastDBCatalog(config).drop_table_if_exists(DEMO_TABLE)
    print(f"\nDropped {DEMO_TABLE!r}")


# ---------------------------------------------------------------------------
# Mode 2: bucket fixed, schema from identifier
# ---------------------------------------------------------------------------


def demo_mode2(access_key: str, secret_key: str) -> None:
    """Mode 2 — bucket fixed, schema part of the identifier.

    Table identifiers are "schema.table"; SQL FROM uses
    ``alias."schema".<table>`` (Daft passes the schema component through the
    catalog's _resolve_bucket_schema to pick up the VastDB schema name).
    """
    print("\n" + "=" * 60)
    print("Mode 2: bucket fixed, schema in identifier")
    print("=" * 60)

    # Config: bucket set, no schema
    config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=access_key,
        secret_key=secret_key,
        bucket=BUCKET,
        ssl_verify=False,
    )

    # Seed using a Mode-1 config so we can reuse seed_table()
    seed_config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=access_key,
        secret_key=secret_key,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )
    print("\n=== Seeding demo table via mode-1 config ===")
    seed_table(seed_config, DEMO_TABLE)

    catalog = VastDBCatalog(config, alias=CATALOG_ALIAS)
    print(f"\n=== Attach with alias {CATALOG_ALIAS!r} (bucket={BUCKET!r}, no schema) ===")
    daft.attach_catalog(catalog, CATALOG_ALIAS)

    # Identifier format for SQL: alias + schema component + table
    # Daft sends (alias, schema, table) → _strip_catalog_prefix drops alias,
    # _resolve_bucket_schema consumes schema from the identifier parts.
    # Names with hyphens must be double-quoted in SQL.
    print("\n--- Revenue by tier (schema-qualified identifier) ---")
    daft.sql(f"""
        SELECT tier, COUNT(*) AS order_count,
               ROUND(SUM(amount), 2) AS total_revenue
        FROM {CATALOG_ALIAS}.{sql_id(SCHEMA)}.{DEMO_TABLE}
        GROUP BY tier ORDER BY total_revenue DESC
    """).show()

    daft.detach_catalog(CATALOG_ALIAS)

    # Programmatic API: use "schema.table" identifier directly
    print(f"\n--- Programmatic: catalog.list_tables() with bucket-only config ---")
    tables = catalog.list_tables()
    print(f"  Tables (schema-qualified): {[str(t) for t in tables]}")

    # Cleanup via mode-1 config
    VastDBCatalog(seed_config).drop_table_if_exists(DEMO_TABLE)
    print(f"\nDropped {DEMO_TABLE!r}")


# ---------------------------------------------------------------------------
# Mode 3: no bucket or schema fixed — fully-qualified identifiers
# ---------------------------------------------------------------------------


def demo_mode3(access_key: str, secret_key: str) -> None:
    """Mode 3 — no bucket or schema in config; fully-qualified identifiers.

    Identifiers are "bucket.schema.table"; SQL FROM uses
    ``alias.<bucket>.<schema>.<table>``.
    """
    print("\n" + "=" * 60)
    print("Mode 3: no bucket/schema in config — fully-qualified identifiers")
    print("=" * 60)

    # Config: no bucket, no schema
    config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=access_key,
        secret_key=secret_key,
        ssl_verify=False,
    )

    # Seed via mode-1 config
    seed_config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=access_key,
        secret_key=secret_key,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )
    print("\n=== Seeding demo table via mode-1 config ===")
    seed_table(seed_config, DEMO_TABLE)

    catalog = VastDBCatalog(config, alias=CATALOG_ALIAS)
    print(f"\n=== Attach with alias {CATALOG_ALIAS!r} (no bucket, no schema) ===")
    daft.attach_catalog(catalog, CATALOG_ALIAS)

    print("\n--- Revenue by tier (fully-qualified identifier) ---")
    daft.sql(f"""
        SELECT tier, COUNT(*) AS order_count,
               ROUND(SUM(amount), 2) AS total_revenue
        FROM {CATALOG_ALIAS}.{sql_id(BUCKET)}.{sql_id(SCHEMA)}.{DEMO_TABLE}
        GROUP BY tier ORDER BY total_revenue DESC
    """).show()

    daft.detach_catalog(CATALOG_ALIAS)

    # Programmatic: load_table with "bucket.schema.table"
    full_ident = f"{BUCKET}.{SCHEMA}.{DEMO_TABLE}"
    print(f"\n--- Programmatic: catalog.load_table({full_ident!r}) ---")
    tbl = catalog.get_table(full_ident)
    print(f"  Table schema: {tbl.schema()}")

    # Cleanup
    VastDBCatalog(seed_config).drop_table_if_exists(DEMO_TABLE)
    print(f"\nDropped {DEMO_TABLE!r}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    access_key, secret_key = load_credentials()

    demo_mode1(access_key, secret_key)
    demo_mode2(access_key, secret_key)
    demo_mode3(access_key, secret_key)

    print("\nDone.")


if __name__ == "__main__":
    main()
