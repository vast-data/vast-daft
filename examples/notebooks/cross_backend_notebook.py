import marimo  # type: ignore

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Cross-Backend: VastDB + Iceberg + Ray

        Write a product catalog (200 rows) and a large orders table (10M rows)
        to **VastDB**, write customers (10K rows) to **Iceberg** (on VastDB S3),
        then join across both backends and aggregate — all distributed via Ray
        through a unified session API.
        """
    )
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
    from daft.io import IOConfig, S3Config

    from helpers import (  # type: ignore
        configure_daft_runner,
        generate_customers,
        generate_orders,
        generate_products,
        get_s3_credentials,
        make_shared_iceberg_catalog,
    )
    from vast_daft import VastDBCatalog, VastDBConfig, VastDBDataSink

    return (
        IOConfig,
        S3Config,
        VastDBCatalog,
        VastDBConfig,
        VastDBDataSink,
        configure_daft_runner,
        daft,
        generate_customers,
        generate_orders,
        generate_products,
        get_s3_credentials,
        make_shared_iceberg_catalog,
        os,
        pa,
        time,
    )


@app.cell
def _(configure_daft_runner):
    print(configure_daft_runner(allow_local_fallback=False))
    return


@app.cell
def _(
    IOConfig,
    S3Config,
    VastDBCatalog,
    VastDBConfig,
    daft,
    generate_products,
    get_s3_credentials,
    make_shared_iceberg_catalog,
    os,
):
    # ---------- constants ----------
    NUM_PRODUCTS = 200
    NUM_CUSTOMERS = 10_000
    NUM_ORDERS = 50_000_000
    ORDERS_BATCH_SIZE = 5_000_000

    # Pre-generate unique product names so orders reference the same set.
    PRODUCT_NAMES = generate_products(NUM_PRODUCTS, unique_names=True).column("product").to_pylist()

    VASTDB_ORDERS_TABLE = "__xbackend_orders__"
    VASTDB_PRODUCTS_TABLE = "__xbackend_products__"
    ICEBERG_NAMESPACE = "cross_backend_demo"
    ICEBERG_CUSTOMERS_TABLE = "customers"

    # ---------- credentials ----------
    ACCESS_KEY, SECRET_KEY = get_s3_credentials()
    ENDPOINT = os.environ.get("VASTDB_ENDPOINT", "http://vippool.ie-dev-pipeline.svc.cluster.local")
    BUCKET = os.environ.get("VASTDB_BUCKET", "collections-bucket")
    SCHEMA = os.environ.get("VASTDB_SCHEMA", "collections-schema")

    # ---------- VastDB catalog ----------
    vastdb_config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )
    vastdb_catalog = VastDBCatalog(vastdb_config, alias="vast")

    # ---------- Iceberg catalog ----------
    iceberg_catalog = make_shared_iceberg_catalog(name="s3_iceberg")

    # ---------- IO config for Iceberg writes ----------
    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=ENDPOINT,
            key_id=ACCESS_KEY,
            access_key=SECRET_KEY,
            region_name="us-east-1",
            use_ssl=False,
        ),
    )

    # ---------- unified session ----------
    sess = daft.session()
    sess.attach_catalog(vastdb_catalog)
    sess.attach_catalog(iceberg_catalog)
    sess.set_catalog(vastdb_catalog.name)

    return (
        ICEBERG_CUSTOMERS_TABLE,
        ICEBERG_NAMESPACE,
        NUM_CUSTOMERS,
        NUM_ORDERS,
        NUM_PRODUCTS,
        ORDERS_BATCH_SIZE,
        PRODUCT_NAMES,
        VASTDB_ORDERS_TABLE,
        VASTDB_PRODUCTS_TABLE,
        iceberg_catalog,
        io_config,
        sess,
        vastdb_config,
    )


# ---------------------------------------------------------------------------
# Step 1 — Write product catalog to VastDB (200 rows, 17 columns)
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 1 — Write Product Catalog to VastDB (200 rows, 17 cols)")
    return


@app.cell
def _(VASTDB_PRODUCTS_TABLE, VastDBDataSink, daft, generate_products, pa, sess, time, vastdb_config):
    sess.set_catalog("vast")
    if sess.has_table(VASTDB_PRODUCTS_TABLE):
        sess.current_catalog().drop_table(VASTDB_PRODUCTS_TABLE)

    _t0 = time.perf_counter()
    _products_schema = pa.schema(
        [
            ("product", pa.string()),
            ("sku", pa.string()),
            ("category", pa.string()),
            ("sub_category", pa.string()),
            ("supplier", pa.string()),
            ("warehouse", pa.string()),
            ("color", pa.string()),
            ("weight_kg", pa.float64()),
            ("cost_price", pa.float64()),
            ("retail_price", pa.float64()),
            ("margin_pct", pa.float64()),
            ("stock_qty", pa.int64()),
            ("reorder_level", pa.int64()),
            ("lead_time_days", pa.int64()),
            ("rating", pa.float64()),
            ("review_count", pa.int64()),
            ("description", pa.string()),
        ]
    )
    df_products = daft.from_arrow(generate_products(200, unique_names=True))
    _sink = VastDBDataSink(
        config=vastdb_config,
        table_name=VASTDB_PRODUCTS_TABLE,
        table_schema=_products_schema,
        create_if_missing=True,
    )
    df_products.write_sink(_sink).show()
    print(f"Wrote 200 products (17 cols) in {time.perf_counter() - _t0:.2f}s")
    return


# ---------------------------------------------------------------------------
# Step 2 — Write customers to Iceberg (10K rows)
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 2 — Write Customers to Iceberg (10K rows)")
    return


@app.cell
def _(
    ICEBERG_CUSTOMERS_TABLE,
    ICEBERG_NAMESPACE,
    NUM_CUSTOMERS,
    daft,
    generate_customers,
    iceberg_catalog,
    io_config,
    time,
):
    iceberg_catalog.create_namespace_if_not_exists(ICEBERG_NAMESPACE)
    _fqn = f"{ICEBERG_NAMESPACE}.{ICEBERG_CUSTOMERS_TABLE}"
    if iceberg_catalog.table_exists(_fqn):
        iceberg_catalog.drop_table(_fqn)

    _t0 = time.perf_counter()
    _df = daft.from_arrow(generate_customers(NUM_CUSTOMERS))
    _iceberg_table = iceberg_catalog.create_table(_fqn, schema=_df.to_arrow().schema)
    _df.write_iceberg(_iceberg_table, mode="append", io_config=io_config).show()
    print(f"Wrote {NUM_CUSTOMERS:,} customers to Iceberg in {time.perf_counter() - _t0:.2f}s")
    return


# ---------------------------------------------------------------------------
# Step 3 — Write orders to VastDB (10M rows, batched)
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 3 — Write Orders to VastDB (10M rows, batched)")
    return


@app.cell
def _(
    NUM_CUSTOMERS,
    NUM_ORDERS,
    ORDERS_BATCH_SIZE,
    PRODUCT_NAMES,
    VASTDB_ORDERS_TABLE,
    VastDBDataSink,
    daft,
    generate_orders,
    pa,
    sess,
    time,
    vastdb_config,
):
    sess.set_catalog("vast")
    if sess.has_table(VASTDB_ORDERS_TABLE):
        sess.current_catalog().drop_table(VASTDB_ORDERS_TABLE)

    _orders_schema = pa.schema(
        [
            ("order_id", pa.int64()),
            ("customer_id", pa.int64()),
            ("product", pa.string()),
            ("amount", pa.float64()),
            ("order_date", pa.string()),
        ]
    )

    _t0 = time.perf_counter()
    _total_written = 0
    for _batch_start in range(0, NUM_ORDERS, ORDERS_BATCH_SIZE):
        _batch_size = min(ORDERS_BATCH_SIZE, NUM_ORDERS - _batch_start)
        _batch = generate_orders(
            _batch_size,
            NUM_CUSTOMERS,
            seed=123 + _batch_start,
            start_id=_batch_start + 1,
            products=PRODUCT_NAMES,
        )
        _df = daft.from_arrow(_batch)
        _sink = VastDBDataSink(
            config=vastdb_config,
            table_name=VASTDB_ORDERS_TABLE,
            table_schema=_orders_schema,
            create_if_missing=True,
        )
        _df.write_sink(_sink).show()
        _total_written += _batch_size
        print(f"  batch {_total_written:,}/{NUM_ORDERS:,}")

    print(f"Wrote {NUM_ORDERS:,} orders in {time.perf_counter() - _t0:.2f}s")
    return


# ---------------------------------------------------------------------------
# Step 4 — Read back from both backends via the session
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 4 — Read Back via Session API")
    return


@app.cell
def _(ICEBERG_CUSTOMERS_TABLE, ICEBERG_NAMESPACE, VASTDB_ORDERS_TABLE, VASTDB_PRODUCTS_TABLE, sess):
    sess.set_catalog("vast")
    df_orders_read = sess.read_table(VASTDB_ORDERS_TABLE)
    df_products_read = sess.read_table(VASTDB_PRODUCTS_TABLE)

    sess.set_catalog("s3_iceberg")
    df_customers_read = sess.read_table(f"{ICEBERG_NAMESPACE}.{ICEBERG_CUSTOMERS_TABLE}")

    print("Loaded: orders (VastDB, 10M), products (VastDB, 200), customers (Iceberg, 10K)")
    df_products_read.limit(5).show()
    return df_customers_read, df_orders_read, df_products_read


@app.cell
def _(daft, df_customers_read, df_orders_read, df_products_read, mo):
    _dims = {
        "orders (VastDB)": (
            df_orders_read.count().collect().to_pydict()["count"][0],
            len(df_orders_read.schema().column_names()),
        ),
        "products (VastDB)": (
            df_products_read.count().collect().to_pydict()["count"][0],
            len(df_products_read.schema().column_names()),
        ),
        "customers (Iceberg)": (
            df_customers_read.count().collect().to_pydict()["count"][0],
            len(df_customers_read.schema().column_names()),
        ),
    }
    _rows = [{"Table": k, "Rows": f"{v[0]:,}", "Columns": v[1]} for k, v in _dims.items()]
    mo.vstack([mo.md("### Table Dimensions"), mo.ui.table(_rows, selection=None)])


# ---------------------------------------------------------------------------
# Step 5 — Cross-backend join: VastDB orders × VastDB products
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 5 — Join: Orders × Products (both VastDB)")
    return


@app.cell
def _(df_orders_read, df_products_read, time):
    _t0 = time.perf_counter()
    df_enriched = df_orders_read.join(df_products_read, on="product", how="inner").select(
        "order_id",
        "customer_id",
        "product",
        "category",
        "amount",
        "cost_price",
        "retail_price",
        "margin_pct",
        "warehouse",
        "order_date",
    )
    df_enriched.limit(5).show()
    print(f"Orders × Products join in {time.perf_counter() - _t0:.2f}s")
    return (df_enriched,)


# ---------------------------------------------------------------------------
# Step 6 — Three-way join: + Iceberg customers
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 6 — Three-Way Join: + Customers (Iceberg)")
    return


@app.cell
def _(df_customers_read, df_enriched, time):
    _t0 = time.perf_counter()
    df_full = df_enriched.join(df_customers_read, on="customer_id", how="inner").select(
        "order_id",
        "customer_id",
        "name",
        "tier",
        "product",
        "category",
        "amount",
        "cost_price",
        "margin_pct",
        "warehouse",
        "order_date",
    )
    df_full.limit(5).show()
    print(f"Three-way join in {time.perf_counter() - _t0:.2f}s")
    return (df_full,)


# ---------------------------------------------------------------------------
# Step 7 — Aggregations
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 7 — Aggregations")
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
    return


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
    print(f"Margin by tier × category in {time.perf_counter() - _t0:.2f}s")
    return


# ---------------------------------------------------------------------------
# Cleanup (commented out — tables persist for catalog explorer)
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md(
        """
        ## Cleanup

        Tables are intentionally **kept** so the catalog explorer notebook
        can browse them.  Uncomment the cell below to drop everything.
        """
    )
    return


@app.cell
def _():
    # Uncomment to clean up:
    #
    # sess.set_catalog("vast")
    # for t in [VASTDB_ORDERS_TABLE, VASTDB_PRODUCTS_TABLE]:
    #     if sess.has_table(t):
    #         sess.current_catalog().drop_table(t)
    #         print(f"Dropped VastDB: {t}")
    #
    # _fqn = f"{ICEBERG_NAMESPACE}.{ICEBERG_CUSTOMERS_TABLE}"
    # if iceberg_catalog.table_exists(_fqn):
    #     iceberg_catalog.drop_table(_fqn)
    #     print(f"Dropped Iceberg: {_fqn}")
    #
    # print("Done.")
    return


if __name__ == "__main__":
    app.run()
