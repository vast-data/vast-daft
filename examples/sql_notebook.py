import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # VastDB SQL Queries via Daft SQL + Ray

        Run SQL queries against VastDB using native Daft SQL, with results
        returned as Daft DataFrames distributed across Ray.

        ## Catalog reference styles

        **Without alias** — use the catalog name as the SQL prefix (quoted if it contains `/`):
        ```sql
        SELECT * FROM "collections-bucket/collections-schema".my_table
        ```

        **With alias** — attach with a short name for cleaner SQL:
        ```python
        daft.attach_catalog(catalog, "vastdb")
        ```
        ```sql
        SELECT * FROM vastdb.my_table
        ```
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

    from vast_daft import (
        VastDBCatalog,
        VastDBConfig,
        VastDBDataSink,
    )

    return VastDBCatalog, VastDBConfig, VastDBDataSink, daft, os, pa, time


@app.cell
def _(daft, os):
    os.environ["RAY_TQDM_DISABLE"] = "1"
    os.environ["RAY_LOG_TO_DRIVER"] = "0"
    daft.set_runner_ray()
    print(f"Connected to Ray (RAY_ADDRESS={os.environ.get('RAY_ADDRESS', 'not set')})")
    return


@app.cell
def _(VastDBCatalog, VastDBConfig, daft):
    ENDPOINT = "http://vippool.ie-dev-pipeline.svc.cluster.local"
    BUCKET = "collections-bucket"
    SCHEMA = "collections-schema"
    ACCESS_KEY = "7P2486YDRB97497707R2"
    SECRET_KEY = "JGAD1JyssLJ3KQ1G2MQp06m/BsefdeZequVb008u"
    CATALOG_ALIAS = "vastdb"

    config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )
    catalog = VastDBCatalog(config, alias=CATALOG_ALIAS)
    daft.attach_catalog(catalog, CATALOG_ALIAS)
    return ACCESS_KEY, BUCKET, CATALOG_ALIAS, ENDPOINT, SCHEMA, SECRET_KEY, catalog, config


@app.cell
def _(mo):
    mo.md("## Setup — Create Sample Table")
    return


@app.cell
def _(CATALOG_ALIAS, VastDBDataSink, catalog, config, daft, pa, time):
    DEMO_TABLE = "__sql_demo_orders__"

    _t0 = time.perf_counter()

    import random as _random

    _random.seed(42)

    _n = 10_000
    _products = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Thingamajig"]
    _tiers = ["bronze", "silver", "gold", "platinum"]

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

    catalog.drop_table_if_exists(DEMO_TABLE)

    df_seed = daft.from_pydict(
        {
            "order_id": list(range(1, _n + 1)),
            "customer_name": [f"customer_{_random.randint(1, 500)}" for _ in range(_n)],
            "tier": [_random.choice(_tiers) for _ in range(_n)],
            "product": [_random.choice(_products) for _ in range(_n)],
            "amount": [round(_random.uniform(5.0, 500.0), 2) for _ in range(_n)],
            "order_date": [f"2025-{_random.randint(1, 12):02d}-{_random.randint(1, 28):02d}" for _ in range(_n)],
        }
    )

    _sink = VastDBDataSink(config=config, table_name=DEMO_TABLE, table_schema=ORDERS_SCHEMA, create_if_missing=True)
    df_seed.write_sink(_sink).show()
    print(f"Seeded {_n:,} rows in {time.perf_counter() - _t0:.2f}s")
    print(f"Query tables as: {CATALOG_ALIAS}.{DEMO_TABLE}")
    return CATALOG_ALIAS, DEMO_TABLE, ORDERS_SCHEMA, df_seed


@app.cell
def _(mo):
    mo.md(
        """
        ## SQL Queries

        Edit the SQL below and run to see results.
        Reference the table as `vastdb.<table_name>`.
        """
    )
    return


@app.cell
def _(CATALOG_ALIAS, DEMO_TABLE, mo):
    sql_input = mo.ui.code_editor(
        value=f"""SELECT
    tier,
    COUNT(*) AS order_count,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_order
FROM {CATALOG_ALIAS}.{DEMO_TABLE}
GROUP BY tier
ORDER BY total_revenue DESC""",
        language="sql",
        min_height=200,
    )
    sql_input
    return (sql_input,)


@app.cell
def _(daft, sql_input, time):
    _query = sql_input.value.strip()
    if _query:
        _t0 = time.perf_counter()
        df_sql_result = daft.sql(_query)
        df_sql_result.show()
        print(f"Query completed in {time.perf_counter() - _t0:.2f}s")
    else:
        df_sql_result = None
        print("Enter a SQL query above")
    return (df_sql_result,)


@app.cell
def _(mo):
    mo.md("## More Query Examples")
    return


@app.cell
def _(mo):
    mo.md("### Top Products by Revenue")
    return


@app.cell
def _(CATALOG_ALIAS, DEMO_TABLE, daft, time):
    _t0 = time.perf_counter()
    df_products = daft.sql(f"""SELECT
    product,
    COUNT(*) AS num_orders,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_price
FROM {CATALOG_ALIAS}.{DEMO_TABLE}
GROUP BY product
ORDER BY total_revenue DESC""")
    df_products.show()
    print(f"Query completed in {time.perf_counter() - _t0:.2f}s")
    return (df_products,)


@app.cell
def _(mo):
    mo.md("### Top Spending Customers (Gold/Platinum)")
    return


@app.cell
def _(CATALOG_ALIAS, DEMO_TABLE, daft, time):
    _t0 = time.perf_counter()
    df_top_customers = daft.sql(f"""SELECT
    customer_name,
    tier,
    COUNT(*) AS num_orders,
    ROUND(SUM(amount), 2) AS total_spent
FROM {CATALOG_ALIAS}.{DEMO_TABLE}
WHERE tier IN ('gold', 'platinum')
GROUP BY customer_name, tier
ORDER BY total_spent DESC
LIMIT 10""")
    df_top_customers.show()
    print(f"Query completed in {time.perf_counter() - _t0:.2f}s")
    return (df_top_customers,)


@app.cell
def _(mo):
    mo.md("### Monthly Revenue Trend")
    return


@app.cell
def _(CATALOG_ALIAS, DEMO_TABLE, daft, time):
    _t0 = time.perf_counter()
    df_monthly = daft.sql(f"""SELECT
    SUBSTRING(order_date, 1, 7) AS month,
    COUNT(*) AS num_orders,
    ROUND(SUM(amount), 2) AS revenue
FROM {CATALOG_ALIAS}.{DEMO_TABLE}
GROUP BY SUBSTRING(order_date, 1, 7)
ORDER BY month""")
    df_monthly.show()
    print(f"Query completed in {time.perf_counter() - _t0:.2f}s")
    return (df_monthly,)


@app.cell
def _(mo):
    mo.md(
        """
        ## No-Alias Style

        Attach the same catalog **without** an alias.  The catalog name becomes
        `bucket/schema`, which contains a `/` and must be quoted in SQL with
        double-quotes:

        ```sql
        SELECT * FROM "collections-bucket/collections-schema".my_table
        ```
        """
    )
    return


@app.cell
def _(CATALOG_ALIAS, VastDBCatalog, config, daft):
    # Detach the aliased catalog first to avoid name collisions
    daft.detach_catalog(CATALOG_ALIAS)

    catalog_no_alias = VastDBCatalog(config)  # no alias → name = "bucket/schema"
    daft.attach_catalog(catalog_no_alias)
    NO_ALIAS_PREFIX = f'"{catalog_no_alias.name}"'  # e.g. "collections-bucket/collections-schema"
    print(f"Attached catalog without alias. SQL prefix: {NO_ALIAS_PREFIX}")
    return catalog_no_alias, NO_ALIAS_PREFIX


@app.cell
def _(DEMO_TABLE, NO_ALIAS_PREFIX, daft, time):
    _t0 = time.perf_counter()
    df_no_alias = daft.sql(f"""SELECT
    tier,
    COUNT(*) AS order_count,
    ROUND(SUM(amount), 2) AS total_revenue
FROM {NO_ALIAS_PREFIX}.{DEMO_TABLE}
GROUP BY tier
ORDER BY total_revenue DESC""")
    df_no_alias.show()
    print(f"Query (no-alias style) completed in {time.perf_counter() - _t0:.2f}s")
    return (df_no_alias,)


@app.cell
def _(CATALOG_ALIAS, VastDBCatalog, catalog_no_alias, config, daft):
    # Restore the aliased catalog for any subsequent cells
    daft.detach_catalog(catalog_no_alias.name)
    catalog_restored = VastDBCatalog(config, alias=CATALOG_ALIAS)
    daft.attach_catalog(catalog_restored, CATALOG_ALIAS)
    print(f"Restored aliased catalog '{CATALOG_ALIAS}'")
    return (catalog_restored,)


@app.cell
def _(mo):
    mo.md("## Cleanup")
    return


@app.cell
def _(DEMO_TABLE, catalog_restored):
    catalog_restored.drop_table_if_exists(DEMO_TABLE)
    print(f"Dropped {DEMO_TABLE}")
    print("Done.")
    return


if __name__ == "__main__":
    app.run()
