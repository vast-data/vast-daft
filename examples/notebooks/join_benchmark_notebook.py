import marimo  # type: ignore

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Join & Aggregation Benchmark (350M rows)

        Reads **pre-existing** tables — orders and products from **VastDB**,
        customers from **Iceberg** — joins across backends, and runs
        aggregations, all distributed via Ray.

        No data generation or table creation — this notebook assumes the
        tables already exist and focuses on read / join / agg performance.

        Schema is **auto-discovered** from catalog metadata (no hardcoded schemas).
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
    from helpers import configure_daft_runner, generate_orders, get_s3_credentials, make_shared_iceberg_catalog  # type: ignore

    from vast_daft import VastDBCatalog, VastDBConfig
    import vast_daft

    return (
        VastDBCatalog,
        VastDBConfig,
        configure_daft_runner,
        daft,
        generate_orders,
        get_s3_credentials,
        make_shared_iceberg_catalog,
        os,
        time,
        vast_daft,
    )


@app.cell
def _(configure_daft_runner):
    print(configure_daft_runner(allow_local_fallback=False))
    return


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@app.cell
def _(VastDBCatalog, VastDBConfig, daft, get_s3_credentials, make_shared_iceberg_catalog, os):
    # ---------- table names ----------
    ORDERS_TABLE = "backend_orders__325m"
    PRODUCTS_TABLE = "__xbackend_products__"
    ICEBERG_CUSTOMERS_TABLE = "cross_backend_demo.customers"

    # ---------- credentials ----------
    ACCESS_KEY, SECRET_KEY = get_s3_credentials()
    ENDPOINT = os.environ.get("VASTDB_ENDPOINT", "")
    BUCKET = os.environ.get("VASTDB_BUCKET", "collections-bucket")
    SCHEMA = os.environ.get("VASTDB_SCHEMA", "collections-schema")

    vastdb_config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )
    vastdb_catalog = VastDBCatalog(vastdb_config, alias="vast")
    iceberg_catalog = make_shared_iceberg_catalog(name="s3_iceberg")

    # ---------- unified session ----------
    sess = daft.session()
    sess.attach_catalog(vastdb_catalog)
    sess.attach_catalog(iceberg_catalog)
    sess.set_catalog(vastdb_catalog.name)

    return (
        ICEBERG_CUSTOMERS_TABLE,
        ORDERS_TABLE,
        PRODUCTS_TABLE,
        sess,
        vastdb_catalog,
        vastdb_config,
    )


# ---------------------------------------------------------------------------
# Data generation — create orders table if it doesn't exist
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    FORCE_RECREATE = mo.ui.checkbox(label="Force recreate orders table (drop + regenerate)")
    FORCE_RECREATE
    return (FORCE_RECREATE,)


@app.cell
def _(FORCE_RECREATE, ORDERS_TABLE, daft, generate_orders, mo, vast_daft, vastdb_catalog, vastdb_config):
    import time as _time
    import pyarrow as pa

    _TOTAL_ROWS = 350_000_000
    _BATCH_SIZE = 5_000_000
    _NUM_CUSTOMERS = 100_000
    _NUM_BATCHES = _TOTAL_ROWS // _BATCH_SIZE
    # Split each batch into N partitions so the write fans out across Ray workers
    # instead of bottlenecking on one. Tune to ~total cluster CPUs.
    _NUM_PARTITIONS = 8

    _ORDERS_SCHEMA = pa.schema([
        ("order_id", pa.int64()),
        ("customer_id", pa.int64()),
        ("product", pa.string()),
        ("amount", pa.float64()),
        ("order_date", pa.date32()),
    ])

    try:
        vastdb_catalog.get_table(ORDERS_TABLE)
        _exists = True
    except Exception:
        _exists = False

    if _exists and not FORCE_RECREATE.value:
        mo.stop(True, mo.md(f"Orders table **{ORDERS_TABLE}** already exists — skipping generation. Check *Force recreate* to drop and regenerate."))

    if _exists and FORCE_RECREATE.value:
        vastdb_catalog.drop_table(ORDERS_TABLE)
        print(f"Dropped existing table '{ORDERS_TABLE}'")

    print(f"Generating {_TOTAL_ROWS:,} orders in {_NUM_BATCHES} batches of {_BATCH_SIZE:,} rows × {_NUM_PARTITIONS} partitions each...")
    print(f"{'Batch':>6}  {'gen_ms':>8}  {'arrow_ms':>9}  {'write_ms':>9}  {'total_ms':>9}  {'rows_written':>14}")
    print("-" * 66)

    _t_run = _time.perf_counter()
    for _i in range(_NUM_BATCHES):
        _t0 = _time.perf_counter()
        _batch = generate_orders(
            _BATCH_SIZE,
            _NUM_CUSTOMERS,
            seed=_i,
            start_id=_i * _BATCH_SIZE + 1,
        )
        _t_gen = _time.perf_counter()

        _df = daft.from_arrow(_batch).into_partitions(_NUM_PARTITIONS)
        _t_arrow = _time.perf_counter()

        _df.write_vastdb(
            config=vastdb_config,
            table_name=ORDERS_TABLE,
            table_schema=_ORDERS_SCHEMA,
            create_if_missing=True,
        )
        _t_write = _time.perf_counter()

        _gen_ms   = (_t_gen   - _t0)     * 1000
        _arrow_ms = (_t_arrow - _t_gen)  * 1000
        _write_ms = (_t_write - _t_arrow) * 1000
        _total_ms = (_t_write - _t0)     * 1000
        _rows_so_far = (_i + 1) * _BATCH_SIZE
        print(f"{_i + 1:>6}  {_gen_ms:>8.0f}  {_arrow_ms:>9.0f}  {_write_ms:>9.0f}  {_total_ms:>9.0f}  {_rows_so_far:>14,}")

    _elapsed = _time.perf_counter() - _t_run
    print(f"\nDone — {_TOTAL_ROWS:,} rows in {_elapsed:.1f}s ({_TOTAL_ROWS / _elapsed / 1e6:.2f}M rows/s)")
    return


# ---------------------------------------------------------------------------
# Step 1 — Read all three tables (schema auto-discovered)
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md(
        """
        ## Step 1 — Read Tables

        Uses the session API which **auto-discovers schemas** from VastDB
        metadata — no need to declare PyArrow schemas manually.

        Split count is auto-detected from table stats and cluster CPUs.
        For 325M orders this yields ~64 parallel read tasks.
        """
    )
    return


@app.cell
def _(ICEBERG_CUSTOMERS_TABLE, ORDERS_TABLE, PRODUCTS_TABLE, sess, time):
    _t0 = time.perf_counter()

    sess.set_catalog("vast")
    df_orders = sess.read_table(ORDERS_TABLE)
    df_products = sess.read_table(PRODUCTS_TABLE)

    sess.set_catalog("s3_iceberg")
    df_customers = sess.read_table(ICEBERG_CUSTOMERS_TABLE)

    print(f"Created lazy DataFrames in {time.perf_counter() - _t0:.2f}s (schemas auto-discovered)")
    return df_customers, df_orders, df_products


# ---------------------------------------------------------------------------
# Step 2 — Verify table sizes
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 2 — Table Dimensions")
    return


@app.cell
def _(df_customers, df_orders, df_products, mo, time):
    _t0 = time.perf_counter()
    _dims = {
        "orders": (
            df_orders.count().collect().to_pydict()["count"][0],
            len(df_orders.schema().column_names()),
        ),
        "products": (
            df_products.count().collect().to_pydict()["count"][0],
            len(df_products.schema().column_names()),
        ),
        "customers": (
            df_customers.count().collect().to_pydict()["count"][0],
            len(df_customers.schema().column_names()),
        ),
    }
    _rows = [{"Table": k, "Rows": f"{v[0]:,}", "Columns": v[1]} for k, v in _dims.items()]
    mo.vstack([
        mo.md(f"Row counts fetched in **{time.perf_counter() - _t0:.2f}s** (metadata-only, no scan)"),
        mo.ui.table(_rows, selection=None),
    ])


# ---------------------------------------------------------------------------
# Step 3 — Join: Orders x Products
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md(
        """
        ## Step 3 — Join: Orders x Products

        Broadcast join on `product`. Products table is tiny (~200 rows) —
        broadcast to all workers so the 325M orders stay in place (no shuffle).
        """
    )
    return


@app.cell
def _(df_orders, df_products, time):
    _t0 = time.perf_counter()
    df_enriched = df_orders.join(df_products, on="product", how="inner", strategy="broadcast").select(
        "order_id",
        "customer_id",
        "product",
        "category",
        "amount",
        "cost_price",
        "order_date",
    )
    print(f"Defined lazy Orders x Products join in {time.perf_counter() - _t0:.2f}s")
    print(f"  Schema: {df_enriched.schema().column_names()}")
    return (df_enriched,)


# ---------------------------------------------------------------------------
# Step 4 — Three-way join: + Customers
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md(
        """
        ## Step 4 — Three-Way Join: + Customers

        Broadcast join enriched orders with customers on `customer_id`.
        Customers is small (~10K rows) — broadcast avoids shuffling 325M rows.
        """
    )
    return


@app.cell
def _(df_customers, df_enriched, time):
    _t0 = time.perf_counter()
    df_full = df_enriched.join(df_customers, on="customer_id", how="inner", strategy="broadcast").select(
        "order_id",
        "customer_id",
        "name",
        "tier",
        "product",
        "category",
        "amount",
        "cost_price",
        "order_date",
    )
    print(f"Defined lazy three-way join in {time.perf_counter() - _t0:.2f}s")
    print(f"  Schema: {df_full.schema().column_names()}")
    return (df_full,)


# ---------------------------------------------------------------------------
# Step 5 — Aggregation: margin by category
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 5 — Aggregation: Margin by Category")
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


# ---------------------------------------------------------------------------
# Step 6 — Aggregation: margin by tier x category
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 6 — Aggregation: Margin by Tier x Category")
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
        .limit(15)
    )
    df_tier_cat.show()
    print(f"Margin by tier x category in {time.perf_counter() - _t0:.2f}s")
    return


# ---------------------------------------------------------------------------
# Step 7 — Aggregation: top customers by spend
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 7 — Top 20 Customers by Total Spend")
    return


@app.cell
def _(daft, df_full, time):
    _t0 = time.perf_counter()
    df_top_customers = (
        df_full.groupby("customer_id", "name", "tier")
        .agg(
            daft.col("amount").sum().alias("total_spend"),
            daft.col("order_id").count().alias("order_count"),
            daft.col("amount").mean().alias("avg_order"),
        )
        .sort("total_spend", desc=True)
        .limit(20)
    )
    df_top_customers.show()
    print(f"Top customers in {time.perf_counter() - _t0:.2f}s")
    return


if __name__ == "__main__":
    app.run()
