from __future__ import annotations

import os
import time
from collections.abc import Callable
from pathlib import Path

import daft
import numpy as np
import pyarrow as pa
from daft.io import IOConfig, S3Config
from daft.schema import Schema
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from vast_daft import VastDBCatalog, VastDBConfig, VastDBDataSink


ORDER_ROWS = int(os.environ.get("BLOG_ORDER_ROWS", "2000000"))
CUSTOMER_ROWS = int(os.environ.get("BLOG_CUSTOMER_ROWS", "10000"))
PRODUCT_ROWS = int(os.environ.get("BLOG_PRODUCT_ROWS", "200"))
ORDERS_TABLE = os.environ.get("BLOG_ORDERS_TABLE", "__blog_orders__")
PRODUCTS_TABLE = os.environ.get("BLOG_PRODUCTS_TABLE", "__blog_products__")
ICEBERG_NAMESPACE = "blog_bench"
ICEBERG_CUSTOMERS_TABLE = "customers"


def _config() -> VastDBConfig:
    return VastDBConfig(
        endpoint=os.environ["VASTDB_ENDPOINT"],
        access_key=os.environ.get("VASTDB_ACCESS_KEY") or os.environ["S3_ACCESS_KEY"],
        secret_key=os.environ.get("VASTDB_SECRET_KEY") or os.environ["S3_SECRET_KEY"],
        bucket=os.environ.get("VASTDB_BUCKET", "collections-bucket"),
        schema=os.environ.get("VASTDB_SCHEMA", "collections-schema"),
        ssl_verify=os.environ.get("VASTDB_SSL_VERIFY", "false").lower() == "true",
    )


def _time(label: str, fn: Callable[[], object]) -> object:
    t0 = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - t0
    print(f"{label}: {elapsed:.3f}s | {result}", flush=True)
    return result


def _rows_cols(df: daft.DataFrame) -> str:
    table = df.collect().to_arrow()
    return f"{table.num_rows} rows x {table.num_columns} cols"


def _count(df: daft.DataFrame) -> int:
    return df.count().collect().to_arrow().column("count")[0].as_py()


def _generate_products() -> pa.Table:
    product_ids = np.arange(1, PRODUCT_ROWS + 1, dtype=np.int64)
    categories = np.array(["compute", "storage", "network", "security", "observability"])
    return pa.table(
        {
            "product_id": product_ids,
            "product": [f"product_{i:04d}" for i in product_ids],
            "category": categories[(product_ids - 1) % len(categories)],
            "retail_price": np.round(10 + (product_ids % 97) * 3.17, 2),
        }
    )


def _generate_orders() -> pa.Table:
    rng = np.random.default_rng(141)
    order_ids = np.arange(1, ORDER_ROWS + 1, dtype=np.int64)
    product_ids = rng.integers(1, PRODUCT_ROWS + 1, size=ORDER_ROWS)
    customer_ids = rng.integers(1, CUSTOMER_ROWS + 1, size=ORDER_ROWS)
    return pa.table(
        {
            "order_id": order_ids,
            "customer_id": customer_ids,
            "product": [f"product_{i:04d}" for i in product_ids],
            "amount": np.round(rng.uniform(5.0, 500.0, size=ORDER_ROWS), 2),
        }
    )


def _generate_customers() -> pa.Table:
    customer_ids = np.arange(1, CUSTOMER_ROWS + 1, dtype=np.int64)
    tiers = np.array(["bronze", "silver", "gold", "platinum"])
    return pa.table(
        {
            "customer_id": customer_ids,
            "name": [f"customer_{i:05d}" for i in customer_ids],
            "tier": tiers[(customer_ids - 1) % len(tiers)],
            "score": np.round(50 + (customer_ids % 50) * 0.9, 2),
        }
    )


def _write_vast_table(config: VastDBConfig, catalog: VastDBCatalog, name: str, table: pa.Table) -> None:
    catalog.drop_table_if_exists(name)
    catalog.create_table_if_not_exists(name, Schema.from_pyarrow_schema(table.schema))
    daft.from_arrow(table).write_sink(
        VastDBDataSink(
            config=config,
            table_name=name,
            table_schema=table.schema,
        )
    )


def main() -> None:
    import ray

    ray.init(address=os.environ.get("RAY_ADDRESS") or "auto", ignore_reinit_error=True)
    daft.set_runner_ray(noop_if_initialized=True)

    config = _config()
    catalog = VastDBCatalog(config, alias="vast")

    print(f"daft={daft.__version__}", flush=True)
    print(f"vastdb={config.endpoint} bucket={config.bucket} schema={config.schema}", flush=True)
    print(f"dataset={ORDER_ROWS} orders, {PRODUCT_ROWS} products, {CUSTOMER_ROWS} customers", flush=True)

    products_pa = _generate_products()
    orders_pa = _generate_orders()
    _time("vastdb.products write", lambda: _write_vast_table(config, catalog, PRODUCTS_TABLE, products_pa) or "ok")
    _time("vastdb.orders write", lambda: _write_vast_table(config, catalog, ORDERS_TABLE, orders_pa) or "ok")

    orders = catalog.get_table(ORDERS_TABLE)
    products = catalog.get_table(PRODUCTS_TABLE)

    _time("vastdb.orders count pushdown", lambda: _count(orders.read()))
    _time("vastdb.products count pushdown", lambda: _count(products.read()))
    _time(
        "vastdb.orders narrow limit 1000",
        lambda: _rows_cols(orders.read(columns=["order_id", "customer_id", "amount"]).limit(1000)),
    )
    _time(
        "vastdb.orders filter amount > 490 limit 1000",
        lambda: _rows_cols(
            orders.read(columns=["order_id", "customer_id", "amount"])
            .where(daft.col("amount") > 490)
            .limit(1000)
        ),
    )
    _time(
        "vastdb join orders->products aggregate",
        lambda: _rows_cols(
            orders.read(columns=["product", "amount"])
            .join(products.read(columns=["product", "category"]), on="product", how="inner")
            .groupby("category")
            .agg(daft.col("amount").sum().alias("revenue"))
        ),
    )

    warehouse = os.environ.get("ICEBERG_WAREHOUSE") or f"s3://{config.bucket}/iceberg-blog-bench"
    db_path = Path("/tmp/blog_iceberg_catalog.db")
    iceberg = SqlCatalog(
        "blog_bench",
        **{
            "uri": f"sqlite:///{db_path}",
            "warehouse": warehouse,
            "s3.endpoint": config.endpoint,
            "s3.access-key-id": config.access_key,
            "s3.secret-access-key": config.secret_key,
            "s3.region": "us-east-1",
            "s3.no-sign-request": "false",
            "s3.client.verify": "false",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3fs.client_kwargs": '{"verify": false}',
            "s3.path-style-access": "true",
        },
    )
    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=config.endpoint,
            key_id=config.access_key,
            access_key=config.secret_key,
            region_name="us-east-1",
            use_ssl=False,
        )
    )

    iceberg_schema = IcebergSchema(
        NestedField(field_id=1, name="customer_id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=True),
        NestedField(field_id=3, name="tier", field_type=StringType(), required=True),
        NestedField(field_id=4, name="score", field_type=DoubleType(), required=False),
    )
    full_table_name = f"{ICEBERG_NAMESPACE}.{ICEBERG_CUSTOMERS_TABLE}"

    def touch_namespace() -> str:
        namespace = (ICEBERG_NAMESPACE,)
        try:
            iceberg.create_namespace(namespace)
        except Exception:
            pass
        return f"{len(list(iceberg.list_namespaces()))} namespaces at {warehouse}"

    _time("iceberg catalog on VAST S3 namespace check", touch_namespace)

    def write_iceberg() -> str:
        if iceberg.table_exists(full_table_name):
            iceberg.drop_table(full_table_name)
        table = iceberg.create_table(full_table_name, schema=iceberg_schema)
        daft.from_arrow(_generate_customers()).write_iceberg(table, mode="append", io_config=io_config).collect()
        return f"{CUSTOMER_ROWS} rows"

    _time("iceberg customers write to VAST S3", write_iceberg)
    iceberg_table = iceberg.load_table(full_table_name)
    _time(
        "iceberg customers read from VAST S3",
        lambda: _rows_cols(daft.read_iceberg(iceberg_table, io_config=io_config).where(daft.col("score") > 90)),
    )


if __name__ == "__main__":
    main()
