"""Validate VastDB column pushdown with Daft join/select plans."""

from __future__ import annotations

import os
import time

import daft
import pyarrow as pa
import ray
from daft.schema import Schema
from dotenv import load_dotenv

from vast_daft import VastDBCatalog, VastDBConfig, VastDBDataSink

EXPECTED_DAFT_VERSION = "0.7.10-dev58+g9c99919f9"
DAFT_PIP_VERSION = "0.7.10.dev58+g9c99919f9"
DAFT_NIGHTLY_FIND_LINKS_URL = "https://ds0gqyebztuyf.cloudfront.net/builds/nightly/daft/index.html"
RAY_RUNTIME_DEPS = [
    "--pre",
    f"--find-links={DAFT_NIGHTLY_FIND_LINKS_URL}",
    f"daft=={DAFT_PIP_VERSION}",
    "numpy<2",
    "ray==2.53.0",
    "pylance>=0.39.0",
    "vastdb>=1.2",
    "pyiceberg[s3fs,sql-sqlite]>=0.11.1",
    "pyarrow>=15.0",
    "ibis-framework>=9.0",
    "sqlalchemy",
]


def _config() -> VastDBConfig:
    load_dotenv()
    return VastDBConfig(
        endpoint=os.environ["VASTDB_ENDPOINT"],
        access_key=os.environ.get("VASTDB_ACCESS_KEY") or os.environ["S3_ACCESS_KEY"],
        secret_key=os.environ.get("VASTDB_SECRET_KEY") or os.environ["S3_SECRET_KEY"],
        bucket=os.environ.get("VASTDB_BUCKET", "collections-bucket"),
        schema=os.environ.get("VASTDB_SCHEMA", "collections-schema"),
        ssl_verify=os.environ.get("VASTDB_SSL_VERIFY", "false").lower() == "true",
    )


def _write_table(config: VastDBConfig, catalog: VastDBCatalog, table_name: str, data: dict[str, list]) -> None:
    table = pa.table(data)
    catalog.drop_table_if_exists(table_name)
    catalog.create_table_if_not_exists(table_name, Schema.from_pyarrow_schema(table.schema))
    daft.from_arrow(table).write_sink(
        VastDBDataSink(
            config,
            table_name,
            table.schema,
        )
    )


def main() -> None:
    if daft.__version__ != EXPECTED_DAFT_VERSION:
        raise RuntimeError(f"Expected Daft {EXPECTED_DAFT_VERSION}, got {daft.__version__}")

    if os.environ.get("VAST_DAFT_TEST_USE_RAY", "0") == "1":
        runtime_env = None
        if os.environ.get("VAST_DAFT_TEST_RUNTIME_ENV", "1") == "1":
            runtime_env = {"pip": RAY_RUNTIME_DEPS}
            wheel_path = os.environ.get("VAST_DAFT_WHEEL")
            if wheel_path and os.path.isfile(wheel_path):
                runtime_env["py_modules"] = [wheel_path]
        ray.init(
            address=os.environ.get("RAY_ADDRESS") or "auto",
            runtime_env=runtime_env,
            ignore_reinit_error=True,
        )
        daft.set_runner_ray(noop_if_initialized=True)

    config = _config()
    catalog = VastDBCatalog(config)
    suffix = int(time.time())
    orders_table = f"__column_pushdown_orders_{suffix}__"
    customers_table = f"__column_pushdown_customers_{suffix}__"

    try:
        _write_table(
            config,
            catalog,
            orders_table,
            {
                "order_id": [1, 2, 3, 4],
                "customer_id": [10, 20, 10, 30],
                "amount": [12.5, 25.0, 7.5, 40.0],
            },
        )
        _write_table(
            config,
            catalog,
            customers_table,
            {
                "customer_id": [10, 20, 30],
                "name": ["alice", "bob", "carol"],
                "tier": ["gold", "silver", "platinum"],
            },
        )

        orders = catalog.get_table(orders_table).read()
        customers = catalog.get_table(customers_table).read()

        projected = orders.select("order_id", "amount").collect()
        if projected.schema().to_pyarrow_schema().names != ["order_id", "amount"]:
            raise AssertionError(f"Unexpected projected schema: {projected.schema()}")

        joined = (
            orders.join(customers, on="customer_id", how="inner")
            .select("order_id", "name", "amount")
            .sort("order_id")
            .collect()
            .to_pydict()
        )
        if joined != {
            "order_id": [1, 2, 3, 4],
            "name": ["alice", "bob", "alice", "carol"],
            "amount": [12.5, 25.0, 7.5, 40.0],
        }:
            raise AssertionError(f"Unexpected join result: {joined}")

        grouped = (
            orders.join(customers, on="customer_id", how="inner")
            .groupby("tier")
            .agg(daft.col("amount").sum().alias("total_amount"))
            .sort("tier")
            .collect()
            .to_pydict()
        )
        if grouped != {
            "tier": ["gold", "platinum", "silver"],
            "total_amount": [20.0, 40.0, 25.0],
        }:
            raise AssertionError(f"Unexpected grouped result: {grouped}")

        print(f"OK daft={daft.__version__} tables={orders_table},{customers_table}")
    finally:
        catalog.drop_table_if_exists(orders_table)
        catalog.drop_table_if_exists(customers_table)


if __name__ == "__main__":
    main()
