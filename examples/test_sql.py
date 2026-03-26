#!/usr/bin/env python3
"""Local test for daft.sql() with VastDB via attached catalog.

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
log = logging.getLogger("test_sql")


def load_config() -> VastDBConfig:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        sys.exit(f"ERROR: .env not found at {env_path}")
    load_dotenv(env_path)
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    if not access_key or not secret_key:
        sys.exit("ERROR: S3_ACCESS_KEY and S3_SECRET_KEY must be set in .env")
    return VastDBConfig(
        endpoint=ENDPOINT,
        access_key=access_key,
        secret_key=secret_key,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )


def main() -> None:
    config = load_config()

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

    # --- Seed data ---
    print("\n=== Seeding demo table ===")
    import random

    random.seed(42)
    n = 1_000
    products = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Thingamajig"]
    tiers = ["bronze", "silver", "gold", "platinum"]

    catalog = VastDBCatalog(config)
    catalog.drop_table_if_exists(DEMO_TABLE)
    df_seed = daft.from_pydict(
        {
            "order_id": list(range(1, n + 1)),
            "customer_name": [f"customer_{random.randint(1, 100)}" for _ in range(n)],
            "tier": [random.choice(tiers) for _ in range(n)],
            "product": [random.choice(products) for _ in range(n)],
            "amount": [round(random.uniform(5.0, 500.0), 2) for _ in range(n)],
            "order_date": [f"2025-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}" for _ in range(n)],
        }
    )
    sink = VastDBDataSink(config=config, table_name=DEMO_TABLE, table_schema=ORDERS_SCHEMA, create_if_missing=True)
    df_seed.write_sink(sink).show()
    print(f"Seeded {n:,} rows")

    # -------------------------------------------------------------------------
    # Option A: no alias — use catalog.name as the SQL prefix
    #   FROM "collections-bucket/collections-schema".<table>
    # -------------------------------------------------------------------------
    print(f"\n=== Attach without alias (catalog.name = {catalog.name!r}) ===")
    daft.attach_catalog(catalog)
    CATALOG_PREFIX = f'"{catalog.name}"'
    print(CATALOG_PREFIX)

    print("\n--- Revenue by tier ---")
    daft.sql(f"""
        SELECT tier, COUNT(*) AS order_count,
               ROUND(SUM(amount), 2) AS total_revenue
        FROM {CATALOG_PREFIX}.{DEMO_TABLE}
        GROUP BY tier ORDER BY total_revenue DESC
    """).show()

    daft.detach_catalog(catalog.name)

    # -------------------------------------------------------------------------
    # Option B: with alias — short name in SQL
    #   FROM vastdb.<table>
    # -------------------------------------------------------------------------
    print(f"\n=== Attach with alias '{CATALOG_ALIAS}' ===")
    catalog_aliased = VastDBCatalog(config, alias=CATALOG_ALIAS)
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

    # --- Cleanup ---
    print("\n=== Cleanup ===")
    catalog.drop_table_if_exists(DEMO_TABLE)
    print(f"Dropped {DEMO_TABLE}")
    print("\nDone.")


if __name__ == "__main__":
    main()
