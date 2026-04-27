import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Join & Aggregation Benchmark — Sail/PySpark (350M rows)

        Mirrors `join_benchmark_notebook.py` (Daft/Ray) exactly, using
        **Sail Spark Connect** instead.  Same tables, same joins, same
        aggregations — different engine.

        Tables (pre-existing, read-only):
        - **orders** (350 M rows) — VastDB via `vastdb` Spark data source
        - **products** (10 rows) — VastDB
        - **customers** (~10 K rows) — Iceberg on S3 via PySpark

        Parallelism: `num_splits=6` matches the 6 Ray CPUs available so
        that read concurrency is comparable to the Daft benchmark.
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

    from helpers import get_s3_credentials  # type: ignore
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from vast_sail import VastDBConfig, register_vastdb

    return F, SparkSession, VastDBConfig, get_s3_credentials, os, register_vastdb, time


@app.cell
def _(SparkSession, VastDBConfig, get_s3_credentials, os, register_vastdb):
    SPARK_REMOTE = os.environ.get(
        "SPARK_REMOTE",
        "sc://sail-spark-server.ray-system.svc.cluster.local:50051",
    )
    ENDPOINT = os.environ.get("VASTDB_ENDPOINT", "http://vippool.ie-collections.svc.cluster.local")
    BUCKET = os.environ.get("VASTDB_BUCKET", "collections-bucket")
    SCHEMA = os.environ.get("VASTDB_SCHEMA", "collections-schema")
    ACCESS_KEY, SECRET_KEY = get_s3_credentials()

    spark = SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
    register_vastdb(spark)

    vastdb_config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )

    print(f"Connected to Sail Spark Connect: {SPARK_REMOTE}")
    return ACCESS_KEY, BUCKET, ENDPOINT, SCHEMA, SECRET_KEY, SPARK_REMOTE, spark, vastdb_config


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@app.cell
def _():
    ORDERS_TABLE = "backend_orders__325m"
    PRODUCTS_TABLE = "__xbackend_products__"

    # num_splits omitted — auto-detected from table stats:
    # num_rows // 4M rows_per_split, capped at 64.

    return ORDERS_TABLE, PRODUCTS_TABLE


# ---------------------------------------------------------------------------
# Step 1 — Read all three tables
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md(
        """
        ## Step 1 — Read Tables

        `num_splits` is auto-detected from table stats (num_rows / 4M,
        capped at 64). No manual tuning needed.
        Products and customers are tiny — no split needed.
        """
    )
    return


@app.cell
def _(ORDERS_TABLE, PRODUCTS_TABLE, spark, time, vastdb_config):
    # CUSTOMERS_TABLE reuses the pre-existing __sail_basic_customers__ which has
    # customer_id, name, email, tier — same 10K customers written by sail_basic_example.
    # If absent, fall back to __ray_combined_customers__ (same seed/schema minus email).
    CUSTOMERS_TABLE = "__sail_basic_customers__"

    _t0 = time.perf_counter()

    # -- orders (350M rows from VastDB, auto-split) --
    df_orders = (
        spark.read.format("vastdb")
        .option("table", ORDERS_TABLE)
        .load()
    )

    # -- products (10 rows from VastDB) --
    df_products = (
        spark.read.format("vastdb")
        .option("table", PRODUCTS_TABLE)
        .load()
    )

    # -- customers (10K rows from VastDB, same schema as Daft benchmark) --
    df_customers = (
        spark.read.format("vastdb")
        .option("table", CUSTOMERS_TABLE)
        .load()
        .select("customer_id", "name", "tier")
    )

    print(f"Created lazy DataFrames in {time.perf_counter() - _t0:.3f}s")
    return CUSTOMERS_TABLE, df_customers, df_orders, df_products


# ---------------------------------------------------------------------------
# Step 2 — Verify table sizes
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 2 — Table Dimensions")
    return


@app.cell
def _(df_customers, df_orders, df_products, time):
    _t0 = time.perf_counter()
    _counts = {
        "orders": df_orders.count(),
        "products": df_products.count(),
        "customers": df_customers.count(),
    }
    _elapsed = time.perf_counter() - _t0
    print(f"Row counts in {_elapsed:.2f}s")
    for _name, _n in _counts.items():
        print(f"  {_name}: {_n:,}")


# ---------------------------------------------------------------------------
# Step 3 — Join: Orders x Products (broadcast)
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md(
        """
        ## Step 3 — Join: Orders x Products

        Broadcast join on `product`.  Products (10 rows) is broadcast to all
        workers so the 350M orders stay in place.
        """
    )
    return


@app.cell
def _(F, df_orders, df_products, time):
    from pyspark.sql.functions import broadcast

    _t0 = time.perf_counter()
    df_enriched = df_orders.join(broadcast(df_products), on="product", how="inner").select(
        "order_id",
        "customer_id",
        "product",
        "category",
        "amount",
        "cost_price",
        "order_date",
    )
    print(f"Defined lazy Orders x Products join in {time.perf_counter() - _t0:.3f}s")
    print(f"  Schema: {df_enriched.columns}")
    return (broadcast, df_enriched)


# ---------------------------------------------------------------------------
# Step 4 — Three-way join: + Customers (broadcast)
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md(
        """
        ## Step 4 — Three-Way Join: + Customers

        Broadcast join on `customer_id`. Customers (~10K rows) is broadcast.
        """
    )
    return


@app.cell
def _(broadcast, df_customers, df_enriched, time):
    _t0 = time.perf_counter()
    df_full = df_enriched.join(broadcast(df_customers), on="customer_id", how="inner").select(
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
    print(f"Defined lazy three-way join in {time.perf_counter() - _t0:.3f}s")
    print(f"  Schema: {df_full.columns}")
    return (df_full,)


# ---------------------------------------------------------------------------
# Step 5 — Aggregation: margin by category
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 5 — Aggregation: Margin by Category")
    return


@app.cell
def _(F, df_full, time):
    _t0 = time.perf_counter()
    _df = (
        df_full.withColumn("margin", F.col("amount") - F.col("cost_price"))
        .groupBy("category")
        .agg(
            F.sum("margin").alias("total_margin"),
            F.avg("margin").alias("avg_margin"),
            F.count("order_id").alias("order_count"),
        )
        .orderBy(F.desc("total_margin"))
    )
    _df.show()
    print(f"Margin by category in {time.perf_counter() - _t0:.2f}s")


# ---------------------------------------------------------------------------
# Step 6 — Aggregation: margin by tier x category
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 6 — Aggregation: Margin by Tier x Category")
    return


@app.cell
def _(F, df_full, time):
    _t0 = time.perf_counter()
    _df = (
        df_full.withColumn("margin", F.col("amount") - F.col("cost_price"))
        .groupBy("tier", "category")
        .agg(
            F.sum("margin").alias("total_margin"),
            F.count("order_id").alias("order_count"),
        )
        .orderBy(F.desc("total_margin"))
        .limit(15)
    )
    _df.show()
    print(f"Margin by tier x category in {time.perf_counter() - _t0:.2f}s")


# ---------------------------------------------------------------------------
# Step 7 — Top 20 customers by spend
# ---------------------------------------------------------------------------


@app.cell
def _(mo):
    mo.md("## Step 7 — Top 20 Customers by Total Spend")
    return


@app.cell
def _(F, df_full, time):
    _t0 = time.perf_counter()
    _df = (
        df_full.groupBy("customer_id", "name", "tier")
        .agg(
            F.sum("amount").alias("total_spend"),
            F.count("order_id").alias("order_count"),
            F.avg("amount").alias("avg_order"),
        )
        .orderBy(F.desc("total_spend"))
        .limit(20)
    )
    _df.show()
    print(f"Top customers in {time.perf_counter() - _t0:.2f}s")


if __name__ == "__main__":
    app.run()
