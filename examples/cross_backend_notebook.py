import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Cross-Backend: VastDB + Iceberg + Ray

        Read from VastDB and Iceberg simultaneously, join across backends,
        aggregate, and write results back — all distributed via Ray.
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import os
    import time

    import daft
    import pyarrow as pa
    from daft.io import IOConfig, S3Config
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType

    from vast_daft import (
        VastDBCatalog,
        VastDBConfig,
        VastDBDataSink,
        VastDBDataSource,
    )

    return (
        IOConfig,
        IcebergSchema,
        S3Config,
        SqlCatalog,
        VastDBCatalog,
        VastDBConfig,
        VastDBDataSink,
        VastDBDataSource,
        DoubleType,
        LongType,
        NestedField,
        StringType,
        daft,
        os,
        pa,
        time,
    )


@app.cell
def _(daft, os):
    os.environ["RAY_TQDM_DISABLE"] = "1"
    os.environ["RAY_LOG_TO_DRIVER"] = "0"
    os.environ["PYTHONWARNINGS"] = "ignore::DeprecationWarning"
    daft.set_runner_ray()
    print(f"Connected to Ray (RAY_ADDRESS={os.environ.get('RAY_ADDRESS', 'not set')})")
    return


@app.cell
def _(IOConfig, S3Config, SqlCatalog, VastDBCatalog, VastDBConfig):
    ENDPOINT = "http://vippool.ie-dev-pipeline.svc.cluster.local"
    BUCKET = "collections-bucket"
    SCHEMA = "collections-schema"
    ACCESS_KEY = "7P2486YDRB97497707R2"
    SECRET_KEY = "JGAD1JyssLJ3KQ1G2MQp06m/BsefdeZequVb008u"

    ICEBERG_WAREHOUSE = f"s3://{BUCKET}/iceberg-warehouse"
    ICEBERG_CATALOG_DB = "/home/ray/vast_daft_cross_backend_catalog.db"
    ICEBERG_NAMESPACE = "cross_backend_demo"

    config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )
    vastdb_catalog = VastDBCatalog(config)

    iceberg_catalog = SqlCatalog(
        "vast_s3",
        **{
            "uri": f"sqlite:///{ICEBERG_CATALOG_DB}",
            "warehouse": ICEBERG_WAREHOUSE,
            "s3.endpoint": ENDPOINT,
            "s3.access-key-id": ACCESS_KEY,
            "s3.secret-access-key": SECRET_KEY,
            "s3.region": "us-east-1",
            "s3.no-sign-request": "false",
            "s3.client.verify": "false",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3fs.client_kwargs": '{"verify": false}',
        },
    )

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=ENDPOINT,
            key_id=ACCESS_KEY,
            access_key=SECRET_KEY,
            region_name="us-east-1",
            use_ssl=False,
        ),
    )
    return (
        ACCESS_KEY,
        BUCKET,
        ENDPOINT,
        ICEBERG_CATALOG_DB,
        ICEBERG_NAMESPACE,
        ICEBERG_WAREHOUSE,
        SCHEMA,
        SECRET_KEY,
        config,
        iceberg_catalog,
        io_config,
        vastdb_catalog,
    )


@app.cell
def _(mo):
    NUM_ORDERS = mo.ui.slider(10_000, 500_000, value=100_000, step=10_000, label="Number of orders")
    NUM_CUSTOMERS = mo.ui.slider(1000, 50_000, value=10_000, step=1000, label="Number of customers")
    mo.vstack([NUM_ORDERS, NUM_CUSTOMERS])
    return NUM_CUSTOMERS, NUM_ORDERS


@app.cell
def _(DoubleType, IcebergSchema, LongType, NestedField, StringType, pa):
    VASTDB_ORDERS_TABLE = "__xbackend_orders__"
    VASTDB_CUSTOMERS_TABLE = "__xbackend_customers__"
    ICEBERG_PRODUCTS_TABLE = "product_catalog"
    ICEBERG_ENRICHED_TABLE = "enriched_orders"

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
    return (
        CUSTOMERS_SCHEMA,
        ICEBERG_ENRICHED_SCHEMA,
        ICEBERG_ENRICHED_TABLE,
        ICEBERG_PRODUCTS_SCHEMA,
        ICEBERG_PRODUCTS_TABLE,
        ORDERS_SCHEMA,
        VASTDB_CUSTOMERS_TABLE,
        VASTDB_ORDERS_TABLE,
    )


@app.cell
def _(mo):
    mo.md("## Step 1 — Generate & Write Orders to VastDB")
    return


@app.cell
def _(
    NUM_CUSTOMERS, NUM_ORDERS, ORDERS_SCHEMA, VASTDB_ORDERS_TABLE, VastDBDataSink, config, daft, time, vastdb_catalog
):
    import random as _random

    _random.seed(123)

    _n = NUM_ORDERS.value
    _nc = NUM_CUSTOMERS.value
    _products = [
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

    vastdb_catalog.drop_table(VASTDB_ORDERS_TABLE)

    _t0 = time.perf_counter()
    df_orders = daft.from_pydict(
        {
            "order_id": list(range(1001, 1001 + _n)),
            "customer_id": [_random.randint(1, _nc) for _ in range(_n)],
            "product": [_random.choice(_products) for _ in range(_n)],
            "amount": [round(_random.uniform(5.0, 500.0), 2) for _ in range(_n)],
            "order_date": [f"2025-{_random.randint(1, 12):02d}-{_random.randint(1, 28):02d}" for _ in range(_n)],
        }
    )
    _sink = VastDBDataSink(
        config=config, table_name=VASTDB_ORDERS_TABLE, table_schema=ORDERS_SCHEMA, create_if_missing=True
    )
    df_orders.write_sink(_sink).show()
    print(f"Wrote {_n:,} orders in {time.perf_counter() - _t0:.2f}s")
    return (df_orders,)


@app.cell
def _(mo):
    mo.md("## Step 2 — Generate & Write Customers to VastDB")
    return


@app.cell
def _(CUSTOMERS_SCHEMA, NUM_CUSTOMERS, VASTDB_CUSTOMERS_TABLE, VastDBDataSink, config, daft, time, vastdb_catalog):
    import random as _random2

    _random2.seed(42)

    _n = NUM_CUSTOMERS.value
    _tiers = ["bronze", "silver", "gold", "platinum"]

    vastdb_catalog.drop_table(VASTDB_CUSTOMERS_TABLE)

    _t0 = time.perf_counter()
    df_customers = daft.from_pydict(
        {
            "customer_id": list(range(1, _n + 1)),
            "name": [f"customer_{i}" for i in range(1, _n + 1)],
            "tier": [_random2.choice(_tiers) for _ in range(_n)],
        }
    )
    _sink = VastDBDataSink(
        config=config, table_name=VASTDB_CUSTOMERS_TABLE, table_schema=CUSTOMERS_SCHEMA, create_if_missing=True
    )
    df_customers.write_sink(_sink).show()
    print(f"Wrote {_n:,} customers in {time.perf_counter() - _t0:.2f}s")
    return (df_customers,)


@app.cell
def _(mo):
    mo.md("## Step 3 — Write Product Catalog to Iceberg")
    return


@app.cell
def _(ICEBERG_NAMESPACE, ICEBERG_PRODUCTS_SCHEMA, ICEBERG_PRODUCTS_TABLE, daft, iceberg_catalog, io_config, time):
    iceberg_catalog.create_namespace_if_not_exists(ICEBERG_NAMESPACE)
    _fqn = f"{ICEBERG_NAMESPACE}.{ICEBERG_PRODUCTS_TABLE}"
    if iceberg_catalog.table_exists(_fqn):
        iceberg_catalog.drop_table(_fqn)

    _t0 = time.perf_counter()
    _iceberg_table = iceberg_catalog.create_table(_fqn, schema=ICEBERG_PRODUCTS_SCHEMA)
    df_products = daft.from_pydict(
        {
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
    )
    df_products.write_iceberg(_iceberg_table, mode="append", io_config=io_config).show()
    print(f"Wrote 10 products to Iceberg in {time.perf_counter() - _t0:.2f}s")
    return (df_products,)


@app.cell
def _(mo):
    mo.md("## Step 4 — Read Back from Both Backends")
    return


@app.cell
def _(CUSTOMERS_SCHEMA, ORDERS_SCHEMA, VASTDB_CUSTOMERS_TABLE, VASTDB_ORDERS_TABLE, VastDBDataSource, config):
    df_orders_read = VastDBDataSource(
        config=config, table_name=VASTDB_ORDERS_TABLE, table_schema=ORDERS_SCHEMA, num_splits=4
    ).read()
    df_customers_read = VastDBDataSource(
        config=config, table_name=VASTDB_CUSTOMERS_TABLE, table_schema=CUSTOMERS_SCHEMA, num_splits=4
    ).read()
    print("Orders and customers read (lazy, 4 splits each)")
    return df_customers_read, df_orders_read


@app.cell
def _(ICEBERG_NAMESPACE, ICEBERG_PRODUCTS_TABLE, daft, iceberg_catalog, io_config):
    _iceberg_table = iceberg_catalog.load_table(f"{ICEBERG_NAMESPACE}.{ICEBERG_PRODUCTS_TABLE}")
    df_products_read = daft.read_iceberg(_iceberg_table, io_config=io_config)
    df_products_read.show()
    return (df_products_read,)


@app.cell
def _(mo):
    mo.md("## Step 5 — Cross-Backend Join: VastDB Orders x Iceberg Products")
    return


@app.cell
def _(df_orders_read, df_products_read, time):
    _t0 = time.perf_counter()
    df_enriched = df_orders_read.join(df_products_read, on="product", how="inner").select(
        "order_id", "customer_id", "product", "category", "amount", "cost_price", "order_date"
    )
    df_enriched.limit(5).show()
    print(f"Cross-backend join in {time.perf_counter() - _t0:.2f}s")
    return (df_enriched,)


@app.cell
def _(mo):
    mo.md("## Step 6 — Three-Way Join: + VastDB Customers")
    return


@app.cell
def _(df_customers_read, df_enriched, time):
    _t0 = time.perf_counter()
    df_full = df_enriched.join(df_customers_read, on="customer_id", how="inner").select(
        "order_id", "customer_id", "product", "category", "amount", "cost_price", "order_date", "name", "tier"
    )
    df_full.limit(5).show()
    print(f"Three-way join in {time.perf_counter() - _t0:.2f}s")
    return (df_full,)


@app.cell
def _(mo):
    mo.md("## Step 7 — Aggregations on Cross-Backend Data")
    return


@app.cell
def _(daft, df_full, time):
    _t0 = time.perf_counter()
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
    print(f"Margin by category in {time.perf_counter() - _t0:.2f}s")
    return (df_margin,)


@app.cell
def _(daft, df_full, time):
    _t0 = time.perf_counter()
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
    print(f"Margin by tier x category in {time.perf_counter() - _t0:.2f}s")
    return (df_tier_cat,)


@app.cell
def _(mo):
    mo.md("## Step 8 — Write Enriched Result to Iceberg")
    return


@app.cell
def _(
    ICEBERG_ENRICHED_SCHEMA, ICEBERG_ENRICHED_TABLE, ICEBERG_NAMESPACE, daft, df_full, iceberg_catalog, io_config, time
):
    _fqn = f"{ICEBERG_NAMESPACE}.{ICEBERG_ENRICHED_TABLE}"
    if iceberg_catalog.table_exists(_fqn):
        iceberg_catalog.drop_table(_fqn)

    _t0 = time.perf_counter()
    _iceberg_enriched = iceberg_catalog.create_table(_fqn, schema=ICEBERG_ENRICHED_SCHEMA)
    df_full.write_iceberg(_iceberg_enriched, mode="append", io_config=io_config).show()

    _iceberg_enriched = iceberg_catalog.load_table(_fqn)
    _df_verify = daft.read_iceberg(_iceberg_enriched, io_config=io_config)
    _count = _df_verify.count().collect().to_pydict()["count"][0]
    print(f"Wrote {_count:,} enriched rows to Iceberg in {time.perf_counter() - _t0:.2f}s")
    _df_verify.limit(5).show()
    return


@app.cell
def _(mo):
    mo.md("## Cleanup")
    return


@app.cell
def _(
    ICEBERG_ENRICHED_TABLE,
    ICEBERG_NAMESPACE,
    ICEBERG_PRODUCTS_TABLE,
    VASTDB_CUSTOMERS_TABLE,
    VASTDB_ORDERS_TABLE,
    iceberg_catalog,
    vastdb_catalog,
):
    from pathlib import Path as _Path

    for _t in [VASTDB_ORDERS_TABLE, VASTDB_CUSTOMERS_TABLE]:
        vastdb_catalog.drop_table(_t)
        print(f"Dropped VastDB: {_t}")

    for _t in [ICEBERG_ENRICHED_TABLE, ICEBERG_PRODUCTS_TABLE]:
        _fqn = f"{ICEBERG_NAMESPACE}.{_t}"
        if iceberg_catalog.table_exists(_fqn):
            iceberg_catalog.drop_table(_fqn)
            print(f"Dropped Iceberg: {_fqn}")

    _db = _Path("/home/ray/vast_daft_cross_backend_catalog.db")
    if _db.exists():
        _db.unlink()
        print("Removed catalog DB")

    print("Done.")
    return


if __name__ == "__main__":
    app.run()
