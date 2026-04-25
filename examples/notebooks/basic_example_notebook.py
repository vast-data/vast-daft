import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md("""
    # VastDB + Daft Basic Example

    Distributed read/write on a Ray cluster via Marimo.

    This notebook generates random customer and order data, writes to VastDB,
    reads back, joins, computes aggregates, and cleans up — all distributed
    across the Ray cluster.
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

    import daft
    import pyarrow as pa
    from helpers import configure_daft_runner, generate_customers, generate_orders, get_s3_credentials  # type: ignore

    from vast_daft import (
        VastDBCatalog,
        VastDBConfig,
        VastDBDataSink,
        VastDBDataSource,
    )

    return (
        VastDBCatalog,
        VastDBConfig,
        VastDBDataSink,
        VastDBDataSource,
        configure_daft_runner,
        daft,
        generate_customers,
        generate_orders,
        get_s3_credentials,
        os,
        pa,
        time,
    )


@app.cell
def _(configure_daft_runner):
    print(configure_daft_runner(allow_local_fallback=False))
    return


@app.cell
def _(VastDBCatalog, VastDBConfig, get_s3_credentials, os):
    ENDPOINT = os.environ.get("VASTDB_ENDPOINT", "http://vippool.ie-dev-pipeline.svc.cluster.local")
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
    catalog = VastDBCatalog(config)
    return catalog, config


@app.cell
def _(mo):
    NUM_CUSTOMERS = mo.ui.slider(1000, 50_000, value=10_000, step=1000, label="Number of customers")
    NUM_ORDERS = mo.ui.slider(10_000, 500_000, value=100_000, step=10_000, label="Number of orders")
    mo.vstack([NUM_CUSTOMERS, NUM_ORDERS])
    return NUM_CUSTOMERS, NUM_ORDERS


@app.cell
def _(pa):
    CUSTOMERS_TABLE = "__ray_combined_customers__"
    ORDERS_TABLE = "__ray_combined_orders__"
    JOINED_TABLE = "__ray_combined_joined__"

    CUSTOMERS_SCHEMA = pa.schema(
        [
            ("customer_id", pa.int64()),
            ("name", pa.string()),
            ("email", pa.string()),
            ("tier", pa.string()),
        ]
    )

    ORDERS_SCHEMA = pa.schema(
        [
            ("order_id", pa.int64()),
            ("customer_id", pa.int64()),
            ("product", pa.string()),
            ("amount", pa.float64()),
            ("order_date", pa.date32()),
        ]
    )

    JOINED_SCHEMA = pa.schema(
        [
            ("customer_id", pa.int64()),
            ("name", pa.string()),
            ("email", pa.string()),
            ("tier", pa.string()),
            ("order_id", pa.int64()),
            ("product", pa.string()),
            ("amount", pa.float64()),
            ("order_date", pa.date32()),
        ]
    )
    return (
        CUSTOMERS_SCHEMA,
        CUSTOMERS_TABLE,
        JOINED_SCHEMA,
        JOINED_TABLE,
        ORDERS_SCHEMA,
        ORDERS_TABLE,
    )


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
def _(
    CUSTOMERS_SCHEMA,
    CUSTOMERS_TABLE,
    VastDBDataSink,
    catalog,
    config,
    customers_data,
    daft,
    time,
):
    catalog.drop_table(CUSTOMERS_TABLE)

    _t0 = time.perf_counter()
    df_customers = daft.from_arrow(customers_data)
    _sink = VastDBDataSink(
        config=config,
        table_name=CUSTOMERS_TABLE,
        table_schema=CUSTOMERS_SCHEMA,
        create_if_missing=True,
    )
    df_customers.write_sink(_sink).show()
    _elapsed = time.perf_counter() - _t0
    print(f"Wrote customers in {_elapsed:.2f}s")
    return


@app.cell
def _(
    ORDERS_SCHEMA,
    ORDERS_TABLE,
    VastDBDataSink,
    catalog,
    config,
    daft,
    orders_data,
    time,
):
    catalog.drop_table(ORDERS_TABLE)

    _t0 = time.perf_counter()
    df_orders = daft.from_arrow(orders_data)
    _sink = VastDBDataSink(
        config=config,
        table_name=ORDERS_TABLE,
        table_schema=ORDERS_SCHEMA,
        create_if_missing=True,
    )
    df_orders.write_sink(_sink).show()
    _elapsed = time.perf_counter() - _t0
    print(f"Wrote orders in {_elapsed:.2f}s")
    return


@app.cell
def _(mo):
    mo.md("""
    ## Step 3 — Read Back (Split Across Workers)
    """)
    return


@app.cell
def _(CUSTOMERS_SCHEMA, CUSTOMERS_TABLE, VastDBDataSource, config, time):
    _t0 = time.perf_counter()
    _src = VastDBDataSource(
        config=config,
        table_name=CUSTOMERS_TABLE,
        table_schema=CUSTOMERS_SCHEMA,
        num_splits=4,
    )
    df_customers_read = _src.read()
    df_customers_read.limit(5).show()
    _elapsed = time.perf_counter() - _t0
    print(f"Read customers (4 splits) in {_elapsed:.2f}s")
    return (df_customers_read,)


@app.cell
def _(ORDERS_SCHEMA, ORDERS_TABLE, VastDBDataSource, config, time):
    _t0 = time.perf_counter()
    _src = VastDBDataSource(
        config=config,
        table_name=ORDERS_TABLE,
        table_schema=ORDERS_SCHEMA,
        num_splits=4,
    )
    df_orders_read = _src.read()
    df_orders_read.limit(5).show()
    _elapsed = time.perf_counter() - _t0
    print(f"Read orders (4 splits) in {_elapsed:.2f}s")
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
    df_joined.limit(5).show()
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
def _(daft, df_joined, time):
    _t0 = time.perf_counter()
    df_by_tier = (
        df_joined.groupby("tier")
        .agg(
            daft.col("amount").sum().alias("total_revenue"),
            daft.col("amount").mean().alias("avg_order"),
            daft.col("order_id").count().alias("order_count"),
        )
        .sort("total_revenue", desc=True)
    )
    df_by_tier.show()
    _elapsed = time.perf_counter() - _t0
    print(f"Revenue by tier in {_elapsed:.2f}s")
    return


@app.cell
def _(daft, df_joined, time):
    _t0 = time.perf_counter()
    df_top_customers = (
        df_joined.groupby("name")
        .agg(
            daft.col("amount").sum().alias("total_spent"),
            daft.col("order_id").count().alias("num_orders"),
        )
        .sort("total_spent", desc=True)
        .limit(10)
    )
    df_top_customers.show()
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
def _(
    JOINED_SCHEMA,
    JOINED_TABLE,
    VastDBDataSink,
    VastDBDataSource,
    catalog,
    config,
    df_joined,
    time,
):
    catalog.drop_table(JOINED_TABLE)

    _t0 = time.perf_counter()
    _sink = VastDBDataSink(
        config=config,
        table_name=JOINED_TABLE,
        table_schema=JOINED_SCHEMA,
        create_if_missing=True,
    )
    df_joined.write_sink(_sink).show()
    _elapsed = time.perf_counter() - _t0
    print(f"Wrote joined table in {_elapsed:.2f}s")

    print("\nVerify — read back:")
    _src = VastDBDataSource(config=config, table_name=JOINED_TABLE, table_schema=JOINED_SCHEMA)
    _src.read().limit(10).show()
    return


@app.cell
def _(mo):
    mo.md("""
    ## Step 7 — Predicate Pushdown Queries
    """)
    return


@app.cell
def _(CUSTOMERS_SCHEMA, CUSTOMERS_TABLE, VastDBDataSource, config, daft, time):
    _t0 = time.perf_counter()
    _src = VastDBDataSource(
        config=config,
        table_name=CUSTOMERS_TABLE,
        table_schema=CUSTOMERS_SCHEMA,
        num_splits=4,
    )
    _df = _src.read().filter(daft.col("tier") == daft.lit("platinum")).collect()
    _elapsed = time.perf_counter() - _t0
    print(f"Platinum customers: {len(_df):,} rows ({_elapsed:.2f}s)")
    return


@app.cell
def _(ORDERS_SCHEMA, ORDERS_TABLE, VastDBDataSource, config, daft, time):
    _t0 = time.perf_counter()
    _src = VastDBDataSource(
        config=config,
        table_name=ORDERS_TABLE,
        table_schema=ORDERS_SCHEMA,
        num_splits=4,
    )
    _df = (
        _src.read().filter((daft.col("amount") >= daft.lit(200.0)) & (daft.col("amount") <= daft.lit(500.0))).collect()
    )
    _elapsed = time.perf_counter() - _t0
    print(f"High-value orders (200-500): {len(_df):,} rows ({_elapsed:.2f}s)")
    return


@app.cell
def _(JOINED_SCHEMA, JOINED_TABLE, VastDBDataSource, config, daft, time):
    _t0 = time.perf_counter()
    _src = VastDBDataSource(
        config=config,
        table_name=JOINED_TABLE,
        table_schema=JOINED_SCHEMA,
        num_splits=4,
    )
    _df = (
        _src.read()
        .filter(daft.col("tier").is_in(["gold", "platinum"]) & (daft.col("amount") >= daft.lit(300.0)))
        .collect()
    )
    _elapsed = time.perf_counter() - _t0
    print(f"VIP high-spend rows: {len(_df):,} rows ({_elapsed:.2f}s)")
    return


@app.cell
def _(JOINED_SCHEMA, JOINED_TABLE, VastDBDataSource, config, daft, time):
    _t0 = time.perf_counter()
    _src = VastDBDataSource(
        config=config,
        table_name=JOINED_TABLE,
        table_schema=JOINED_SCHEMA,
        num_splits=4,
    )
    _df = (
        _src.read()
        .select(daft.col("name"), daft.col("amount"), daft.col("tier"))
        .filter(daft.col("tier") == daft.lit("gold"))
        .collect()
    )
    _elapsed = time.perf_counter() - _t0
    print(f"Gold tier (name, amount): {len(_df):,} rows ({_elapsed:.2f}s)")
    return


@app.cell
def _(mo):
    mo.md("""
    ## Cleanup
    """)
    return


@app.cell
def _(CUSTOMERS_TABLE, JOINED_TABLE, ORDERS_TABLE, catalog):
    for _t in [CUSTOMERS_TABLE, ORDERS_TABLE, JOINED_TABLE]:
        catalog.drop_table(_t)
        print(f"Dropped {_t}")
    print("Done.")
    return


if __name__ == "__main__":
    app.run()
