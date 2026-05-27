import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md("""
    # VastDB + Sail Basic Example

    Distributed read/write through Sail Spark Connect.

    This notebook generates customer and order data, writes to VastDB through
    the `vast_sail` Python data source, reads back with split parallelism,
    joins, computes aggregates, writes a joined table, and cleans up.
    """)
    return


@app.cell
def _():
    import marimo as mo  # type: ignore

    return (mo,)


@app.cell
def _():
    import os
    import time

    import pyarrow as pa
    from helpers import generate_customers, generate_orders, get_s3_credentials  # type: ignore
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from vast_sail import VastDBConfig, VastDBConnection, register_vastdb

    return (
        F,
        SparkSession,
        VastDBConfig,
        VastDBConnection,
        generate_customers,
        generate_orders,
        get_s3_credentials,
        os,
        pa,
        register_vastdb,
        time,
    )


@app.cell
def _(SparkSession, os, register_vastdb):
    SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "")

    spark = SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
    register_vastdb(spark)

    print(f"Connected to Sail Spark Connect: {SPARK_REMOTE}")
    return (spark,)


@app.cell
def _(VastDBConfig, get_s3_credentials, os):
    ENDPOINT = os.environ.get("VASTDB_ENDPOINT", "")
    BUCKET = os.environ.get("VASTDB_BUCKET", "collections-bucket")
    SCHEMA = os.environ.get("VASTDB_SCHEMA", "collections-schema")
    ACCESS_KEY, SECRET_KEY = get_s3_credentials()

    config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )
    return (config,)


@app.cell
def _(mo):
    NUM_CUSTOMERS = mo.ui.slider(1000, 50_000, value=10_000, step=1000, label="Number of customers")
    NUM_ORDERS = mo.ui.slider(10_000, 500_000, value=100_000, step=10_000, label="Number of orders")
    mo.vstack([NUM_CUSTOMERS, NUM_ORDERS])
    return NUM_CUSTOMERS, NUM_ORDERS


@app.cell
def _():
    CUSTOMERS_TABLE = "__sail_basic_customers__"
    ORDERS_TABLE = "__sail_basic_orders__"
    JOINED_TABLE = "__sail_basic_joined__"
    return CUSTOMERS_TABLE, JOINED_TABLE, ORDERS_TABLE


@app.cell
def _(mo):
    mo.md("""
    ## Step 1 — Generate Data
    """)
    return


@app.cell
def _(NUM_CUSTOMERS, generate_customers):
    _n = NUM_CUSTOMERS.value
    customers_data = generate_customers(_n, include_email=True)
    print(f"Generated {_n:,} customers")
    return (customers_data,)


@app.cell
def _(NUM_CUSTOMERS, NUM_ORDERS, generate_orders):
    _n = NUM_ORDERS.value
    orders_data = generate_orders(_n, NUM_CUSTOMERS.value)
    print(f"Generated {_n:,} orders")
    return (orders_data,)


@app.cell
def _(mo):
    mo.md("""
    ## Step 2 — Write to VastDB
    """)
    return


@app.cell
def _(CUSTOMERS_TABLE, VastDBConnection, config, customers_data, time):
    _t0 = time.perf_counter()
    _connection = VastDBConnection(config)
    with _connection.session.transaction() as _tx:
        _bucket = _tx.bucket(config.bucket)
        _schema = _bucket.schema(config.schema, fail_if_missing=False)
        if _schema is not None:
            _table = _schema.table(CUSTOMERS_TABLE, fail_if_missing=False)
            if _table is not None:
                _table.drop()
    with _connection.get_table(
        CUSTOMERS_TABLE,
        customers_data.schema,
        bucket=config.bucket,
        schema=config.schema,
        create_if_missing=True,
    ) as _table:
        _table.insert(customers_data)
    _elapsed = time.perf_counter() - _t0
    print(f"Wrote customers in {_elapsed:.2f}s")
    return


@app.cell
def _(ORDERS_TABLE, VastDBConnection, config, orders_data, time):
    _t0 = time.perf_counter()
    _connection = VastDBConnection(config)
    with _connection.session.transaction() as _tx:
        _bucket = _tx.bucket(config.bucket)
        _schema = _bucket.schema(config.schema, fail_if_missing=False)
        if _schema is not None:
            _table = _schema.table(ORDERS_TABLE, fail_if_missing=False)
            if _table is not None:
                _table.drop()
    with _connection.get_table(
        ORDERS_TABLE,
        orders_data.schema,
        bucket=config.bucket,
        schema=config.schema,
        create_if_missing=True,
    ) as _table:
        _table.insert(orders_data)
    _elapsed = time.perf_counter() - _t0
    print(f"Wrote orders in {_elapsed:.2f}s")
    return


@app.cell
def _(mo):
    mo.md("""
    ## Step 3 — Read Back
    """)
    return


@app.cell
def _(CUSTOMERS_TABLE, spark, time):
    _t0 = time.perf_counter()
    df_customers_read = spark.read.format("vastdb").option("table", CUSTOMERS_TABLE).option("num_splits", "4").load()
    df_customers_read.show(5)
    _elapsed = time.perf_counter() - _t0
    print(f"Read customers in {_elapsed:.2f}s")
    return (df_customers_read,)


@app.cell
def _(ORDERS_TABLE, spark, time):
    _t0 = time.perf_counter()
    df_orders_read = spark.read.format("vastdb").option("table", ORDERS_TABLE).option("num_splits", "4").load()
    df_orders_read.show(5)
    _elapsed = time.perf_counter() - _t0
    print(f"Read orders in {_elapsed:.2f}s")
    return (df_orders_read,)


@app.cell
def _(mo):
    mo.md("""
    ## Step 4 — Join Customers x Orders
    """)
    return


@app.cell
def _(df_customers_read, df_orders_read, time):
    _t0 = time.perf_counter()
    df_joined = df_customers_read.join(df_orders_read, on="customer_id", how="inner").select(
        "customer_id", "name", "email", "tier", "order_id", "product", "amount", "order_date"
    )
    df_joined.show(5)
    _elapsed = time.perf_counter() - _t0
    print(f"Joined in {_elapsed:.2f}s")
    return (df_joined,)


@app.cell
def _(mo):
    mo.md("""
    ## Step 5 — Aggregations
    """)
    return


@app.cell
def _(F, df_joined, time):
    _t0 = time.perf_counter()
    _df = (
        df_joined.groupBy("tier")
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_order"),
            F.count("order_id").alias("order_count"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    _df.show()
    _elapsed = time.perf_counter() - _t0
    print(f"Revenue by tier in {_elapsed:.2f}s")
    return


@app.cell
def _(F, df_joined, time):
    _t0 = time.perf_counter()
    _df = (
        df_joined.groupBy("name")
        .agg(F.sum("amount").alias("total_spent"), F.count("order_id").alias("num_orders"))
        .orderBy(F.desc("total_spent"))
        .limit(10)
    )
    _df.show()
    _elapsed = time.perf_counter() - _t0
    print(f"Top 10 customers in {_elapsed:.2f}s")
    return


@app.cell
def _(mo):
    mo.md("""
    ## Step 6 — Write Joined Result Back
    """)
    return


@app.cell
def _(JOINED_TABLE, VastDBConnection, config, df_joined, pa, spark, time):
    _t0 = time.perf_counter()
    _joined_arrow = pa.Table.from_pandas(df_joined.toPandas(), preserve_index=False)
    _joined_schema = pa.schema(
        [
            pa.field(_field.name, pa.string(), nullable=_field.nullable)
            if pa.types.is_large_string(_field.type)
            else _field
            for _field in _joined_arrow.schema
        ]
    )
    _joined_arrow = _joined_arrow.cast(_joined_schema)
    _connection = VastDBConnection(config)
    with _connection.session.transaction() as _tx:
        _bucket = _tx.bucket(config.bucket)
        _schema = _bucket.schema(config.schema, fail_if_missing=False)
        if _schema is not None:
            _table = _schema.table(JOINED_TABLE, fail_if_missing=False)
            if _table is not None:
                _table.drop()
    with _connection.get_table(
        JOINED_TABLE,
        _joined_arrow.schema,
        bucket=config.bucket,
        schema=config.schema,
        create_if_missing=True,
    ) as _table:
        _table.insert(_joined_arrow)
    _elapsed = time.perf_counter() - _t0
    print(f"Wrote joined table in {_elapsed:.2f}s")

    print("\nVerify — read back:")
    _df = spark.read.format("vastdb").option("table", JOINED_TABLE).option("num_splits", "4").load()
    _df.show(10)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Step 7 — Predicate Pushdown Queries
    """)
    return


@app.cell
def _(CUSTOMERS_TABLE, spark, time):
    _t0 = time.perf_counter()
    _count = (
        spark.read.format("vastdb")
        .option("table", CUSTOMERS_TABLE)
        .option("num_splits", "4")
        .load()
        .filter("tier = 'platinum'")
        .count()
    )
    _elapsed = time.perf_counter() - _t0
    print(f"Platinum customers: {_count:,} rows ({_elapsed:.2f}s)")
    return


@app.cell
def _(ORDERS_TABLE, spark, time):
    _t0 = time.perf_counter()
    _count = (
        spark.read.format("vastdb")
        .option("table", ORDERS_TABLE)
        .option("num_splits", "4")
        .load()
        .filter("amount >= 200.0 AND amount <= 500.0")
        .count()
    )
    _elapsed = time.perf_counter() - _t0
    print(f"High-value orders (200-500): {_count:,} rows ({_elapsed:.2f}s)")
    return


@app.cell
def _(mo):
    mo.md("""
    ## Cleanup
    """)
    return


@app.cell
def _(CUSTOMERS_TABLE, JOINED_TABLE, ORDERS_TABLE, VastDBConnection, config):
    _connection = VastDBConnection(config)
    with _connection.session.transaction() as _tx:
        _bucket = _tx.bucket(config.bucket)
        _schema = _bucket.schema(config.schema, fail_if_missing=False)
        if _schema is not None:
            for _table_name in [CUSTOMERS_TABLE, ORDERS_TABLE, JOINED_TABLE]:
                _table = _schema.table(_table_name, fail_if_missing=False)
                if _table is not None:
                    _table.drop()
                    print(f"Dropped {_table_name}")
    print("Done.")
    return


if __name__ == "__main__":
    app.run()
