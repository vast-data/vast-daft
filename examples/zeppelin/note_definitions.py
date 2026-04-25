from __future__ import annotations

from textwrap import dedent


def _paragraph(*, title: str, text: str) -> dict[str, str]:
    return {
        "title": title,
        "text": dedent(text).strip(),
    }


def build_basic_example_note() -> dict[str, object]:
    return {
        "name": "Validation/Basic Example",
        "paragraphs": [
            _paragraph(
                title="Overview",
                text="""
                %md
                # VastDB + Daft Basic Example

                Zeppelin validation note for `examples/notebooks/basic_example_notebook.py`.
                """,
            ),
            _paragraph(
                title="Setup",
                text="""
                %python
                import os
                import time
                import uuid

                import daft
                import pyarrow as pa
                from helpers import configure_daft_runner, generate_customers, generate_orders, get_s3_credentials
                from vast_daft import VastDBCatalog, VastDBConfig, VastDBDataSink, VastDBDataSource

                print(configure_daft_runner(allow_local_fallback=False))

                RUN_ID = uuid.uuid4().hex[:8]
                NUM_CUSTOMERS = 2_000
                NUM_ORDERS = 10_000
                CUSTOMERS_TABLE = f"__zeppelin_basic_customers_{RUN_ID}__"
                ORDERS_TABLE = f"__zeppelin_basic_orders_{RUN_ID}__"
                JOINED_TABLE = f"__zeppelin_basic_joined_{RUN_ID}__"

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

                access_key, secret_key = get_s3_credentials()
                config = VastDBConfig(
                    endpoint=os.environ.get("VASTDB_ENDPOINT", "http://vippool.ie-dev-pipeline.svc.cluster.local"),
                    access_key=access_key,
                    secret_key=secret_key,
                    bucket=os.environ.get("VASTDB_BUCKET", "collections-bucket"),
                    schema=os.environ.get("VASTDB_SCHEMA", "collections-schema"),
                    ssl_verify=False,
                )
                catalog = VastDBCatalog(config)

                def drop_if_exists(*, table_name: str) -> None:
                    try:
                        catalog.drop_table(table_name)
                        print(f"Dropped existing table: {table_name}")
                    except Exception:
                        print(f"Table already absent: {table_name}")

                def row_count(*, df) -> int:
                    return int(df.count().collect().to_pydict()["count"][0])

                print(f"Run id: {RUN_ID}")
                """,
            ),
            _paragraph(
                title="Write Source Tables",
                text="""
                %python
                drop_if_exists(table_name=CUSTOMERS_TABLE)
                drop_if_exists(table_name=ORDERS_TABLE)
                drop_if_exists(table_name=JOINED_TABLE)

                customers_data = generate_customers(NUM_CUSTOMERS, include_email=True)
                orders_data = generate_orders(NUM_ORDERS, NUM_CUSTOMERS)

                daft.from_arrow(customers_data).write_sink(
                    VastDBDataSink(
                        config=config,
                        table_name=CUSTOMERS_TABLE,
                        table_schema=CUSTOMERS_SCHEMA,
                        create_if_missing=True,
                    )
                ).show()
                daft.from_arrow(orders_data).write_sink(
                    VastDBDataSink(
                        config=config,
                        table_name=ORDERS_TABLE,
                        table_schema=ORDERS_SCHEMA,
                        create_if_missing=True,
                    )
                ).show()

                print(f"Wrote {NUM_CUSTOMERS:,} customers and {NUM_ORDERS:,} orders")
                """,
            ),
            _paragraph(
                title="Read Join Aggregate",
                text="""
                %python
                customers_count = 0
                orders_count = 0
                for attempt in range(10):
                    customers_count = VastDBDataSource(
                        config=config,
                        table_name=CUSTOMERS_TABLE,
                        table_schema=CUSTOMERS_SCHEMA,
                        num_splits=4,
                    ).read()
                    customers_count = row_count(df=customers_count)
                    orders_count = VastDBDataSource(
                        config=config,
                        table_name=ORDERS_TABLE,
                        table_schema=ORDERS_SCHEMA,
                        num_splits=4,
                    ).read()
                    orders_count = row_count(df=orders_count)
                    if customers_count > 0 and orders_count > 0:
                        break
                    print(
                        f"Read attempt {attempt + 1} saw customers={customers_count}, "
                        f"orders={orders_count}; retrying"
                    )
                    time.sleep(2)

                assert customers_count == NUM_CUSTOMERS, f"Expected {NUM_CUSTOMERS}, got {customers_count}"
                assert orders_count == NUM_ORDERS, f"Expected {NUM_ORDERS}, got {orders_count}"

                customers_df = VastDBDataSource(
                    config=config,
                    table_name=CUSTOMERS_TABLE,
                    table_schema=CUSTOMERS_SCHEMA,
                    num_splits=4,
                ).read()
                orders_df = VastDBDataSource(
                    config=config,
                    table_name=ORDERS_TABLE,
                    table_schema=ORDERS_SCHEMA,
                    num_splits=4,
                ).read()

                joined_df = customers_df.join(orders_df, on="customer_id", how="inner").select(
                    "customer_id",
                    "name",
                    "email",
                    "tier",
                    "order_id",
                    "product",
                    "amount",
                    "order_date",
                )
                joined_count = row_count(df=joined_df)
                assert joined_count > 0, "Expected joined rows"

                revenue_df = (
                    joined_df.groupby("tier")
                    .agg(
                        daft.col("amount").sum().alias("total_revenue"),
                        daft.col("order_id").count().alias("order_count"),
                    )
                    .sort("total_revenue", desc=True)
                )
                revenue_count = row_count(df=revenue_df)
                assert revenue_count > 0, "Expected aggregated rows"

                gold_count = row_count(df=joined_df.filter(daft.col("tier") == daft.lit("gold")))
                assert gold_count > 0, "Expected at least one gold row"

                revenue_df.show()

                drop_if_exists(table_name=JOINED_TABLE)
                joined_df.write_sink(
                    VastDBDataSink(
                        config=config,
                        table_name=JOINED_TABLE,
                        table_schema=JOINED_SCHEMA,
                        create_if_missing=True,
                    )
                ).show()

                verified_count = VastDBDataSource(
                    config=config,
                    table_name=JOINED_TABLE,
                    table_schema=JOINED_SCHEMA,
                    num_splits=4,
                ).read()
                verified_count = row_count(df=verified_count)
                assert verified_count == joined_count, f"Expected {joined_count}, got {verified_count}"

                print(
                    f"Source rows: customers={customers_count:,}, orders={orders_count:,}; "
                    f"joined={joined_count:,}; gold rows={gold_count:,}; verified rows={verified_count:,}"
                )
                """,
            ),
            _paragraph(
                title="Cleanup",
                text="""
                %python
                drop_if_exists(table_name=CUSTOMERS_TABLE)
                drop_if_exists(table_name=ORDERS_TABLE)
                drop_if_exists(table_name=JOINED_TABLE)
                print("Basic example Zeppelin validation passed")
                """,
            ),
        ],
    }


def build_sql_console_note() -> dict[str, object]:
    return {
        "name": "Validation/SQL Console",
        "paragraphs": [
            _paragraph(
                title="Overview",
                text="""
                %md
                # SQL Console

                Zeppelin validation note for `examples/notebooks/sql_console_notebook.py`.
                """,
            ),
            _paragraph(
                title="Setup Catalog",
                text="""
                %python
                import html
                import os
                import uuid

                import daft
                import pyarrow as pa
                from helpers import configure_daft_runner, generate_customers, get_s3_credentials
                from vast_daft import VastDBCatalog, VastDBConfig, VastDBDataSink

                print(configure_daft_runner(allow_local_fallback=False))

                RUN_ID = uuid.uuid4().hex[:8]
                CATALOG_ALIAS = f"vastdb_sql_{RUN_ID}"
                TABLE_NAME = f"__zeppelin_sql_console_{RUN_ID}__"
                TABLE_SCHEMA = pa.schema(
                    [
                        ("customer_id", pa.int64()),
                        ("name", pa.string()),
                        ("email", pa.string()),
                        ("tier", pa.string()),
                    ]
                )

                access_key, secret_key = get_s3_credentials()
                config = VastDBConfig(
                    endpoint=os.environ.get("VASTDB_ENDPOINT", "http://vippool.ie-dev-pipeline.svc.cluster.local"),
                    access_key=access_key,
                    secret_key=secret_key,
                    bucket=os.environ.get("VASTDB_BUCKET", "collections-bucket"),
                    schema=os.environ.get("VASTDB_SCHEMA", "collections-schema"),
                    ssl_verify=False,
                )
                catalog = VastDBCatalog(config, alias=CATALOG_ALIAS)
                daft.attach_catalog(catalog, CATALOG_ALIAS)

                def row_count(*, df) -> int:
                    return int(df.count().collect().to_pydict()["count"][0])

                def print_html_result(*, title: str, query: str, df) -> None:
                    pdf = df.to_pandas()
                    headers = "".join(f"<th>{html.escape(str(column))}</th>" for column in pdf.columns)
                    rows = []
                    container_style = (
                        "font-family: -apple-system, BlinkMacSystemFont, sans-serif; "
                        "padding: 8px 0;"
                    )
                    query_style = (
                        "margin-bottom: 12px; padding: 10px 12px; background: #0f172a; "
                        "color: #e2e8f0; border-radius: 8px;"
                    )
                    meta_style = "margin-bottom: 12px; font-size: 13px; color: #475569;"
                    for row in pdf.itertuples(index=False, name=None):
                        cells = "".join(f"<td>{html.escape(str(value))}</td>" for value in row)
                        rows.append(f"<tr>{cells}</tr>")

                    table_rows = "".join(rows) or '<tr><td colspan="99">No rows</td></tr>'
                    print(
                        "%html\\n"
                        f"<div style='{container_style}'>"
                        f"<h3 style='margin: 0 0 8px;'>{html.escape(title)}</h3>"
                        f"<div style='{query_style}'>"
                        f"<div style='font-size: 12px; opacity: 0.8; margin-bottom: 6px;'>SQL</div>"
                        f"<pre style='margin: 0; white-space: pre-wrap;'>{html.escape(query)}</pre>"
                        "</div>"
                        f"<div style='{meta_style}'>Rows returned: <strong>{len(pdf):,}</strong></div>"
                        "<div style='overflow-x: auto;'>"
                        "<table style='border-collapse: collapse; width: 100%; font-size: 13px;'>"
                        f"<thead><tr style='background: #e2e8f0;'>{headers}</tr></thead>"
                        f"<tbody>{table_rows}</tbody>"
                        "</table>"
                        "</div>"
                        "</div>"
                    )

                source_data = generate_customers(1_000, include_email=True)
                daft.from_arrow(source_data).write_sink(
                    VastDBDataSink(
                        config=config,
                        table_name=TABLE_NAME,
                        table_schema=TABLE_SCHEMA,
                        create_if_missing=True,
                    )
                ).show()

                print(f"Attached catalog {CATALOG_ALIAS} and created table {TABLE_NAME}")
                """,
            ),
            _paragraph(
                title="Grouped SQL Query",
                text='''
                %python
                grouped_query = f"""
                SELECT tier, COUNT(*) AS customer_count
                FROM {CATALOG_ALIAS}.{TABLE_NAME}
                GROUP BY tier
                ORDER BY customer_count DESC
                """.strip()

                grouped_df = daft.sql(grouped_query)
                grouped_count = row_count(df=grouped_df)
                assert grouped_count > 0, "Expected grouped SQL rows"
                print_html_result(title="Grouped SQL Result", query=grouped_query, df=grouped_df)
                ''',
            ),
            _paragraph(
                title="Filtered SQL Query",
                text='''
                %python
                filtered_query = f"""
                SELECT customer_id, name, email
                FROM {CATALOG_ALIAS}.{TABLE_NAME}
                WHERE tier = 'gold'
                ORDER BY customer_id
                LIMIT 5
                """.strip()

                filtered_df = daft.sql(filtered_query)
                filtered_count = row_count(df=filtered_df)
                assert filtered_count > 0, "Expected filtered SQL rows"
                print_html_result(title="Filtered SQL Result", query=filtered_query, df=filtered_df)
                ''',
            ),
            _paragraph(
                title="Cleanup",
                text="""
                %python
                catalog.drop_table(TABLE_NAME)
                print("SQL console Zeppelin validation passed")
                """,
            ),
        ],
    }


def build_validation_notes() -> list[dict[str, object]]:
    return [build_basic_example_note(), build_sql_console_note()]
