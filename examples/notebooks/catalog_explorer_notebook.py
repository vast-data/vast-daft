import marimo  # type: ignore

__generated_with = "0.21.1"
app = marimo.App(
    width="medium",
    layout_file="layouts/catalog_explorer_notebook.grid.json",
)


@app.cell
def _(mo):
    mo.md("""
    # Cross-Backend Catalog Explorer

    Browse the same **VastDB** and **Iceberg** catalogs configured in
    `cross_backend_example.py`, then preview table contents side-by-side
    in a small explorer app.
    """)
    return


@app.cell
def _():
    import marimo as mo  # type: ignore

    return (mo,)


@app.cell
def _():
    import os

    import daft
    import pandas as pd
    from daft.io import IOConfig, S3Config

    from helpers import configure_daft_runner, get_s3_credentials, make_shared_iceberg_catalog  # type: ignore
    from vast_daft import VastDBCatalog, VastDBConfig

    return (
        IOConfig,
        S3Config,
        VastDBCatalog,
        VastDBConfig,
        configure_daft_runner,
        daft,
        get_s3_credentials,
        make_shared_iceberg_catalog,
        os,
        pd,
    )


@app.cell
def _(configure_daft_runner):
    runner_status = configure_daft_runner()
    return


@app.cell
def _(
    IOConfig,
    S3Config,
    VastDBCatalog,
    VastDBConfig,
    daft,
    get_s3_credentials,
    make_shared_iceberg_catalog,
    os,
):
    ENDPOINT = "http://vippool.ie-dev-pipeline.svc.cluster.local"
    S3_ENDPOINT = ENDPOINT
    BUCKET = "collections-bucket"
    SCHEMA = "collections-schema"
    ACCESS_KEY, SECRET_KEY = get_s3_credentials()

    vastdb_config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )

    vastdb_catalog = VastDBCatalog(vastdb_config, alias="vast")

    iceberg_catalog = make_shared_iceberg_catalog()

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=S3_ENDPOINT,
            key_id=ACCESS_KEY,
            access_key=SECRET_KEY,
            region_name="us-east-1",
            use_ssl=False,
        ),
    )
    sess = daft.session()
    sess.attach_catalog(vastdb_catalog)
    sess.attach_catalog(iceberg_catalog)

    sess.set_catalog(vastdb_catalog.name)

    return (sess,)


@app.cell
def _(mo, sess):
    catalog_selector = mo.ui.radio(options=sess.list_catalogs())

    mo.vstack(
        [
            mo.md("### Catalogs"),
            catalog_selector,
        ]
    )

    return (catalog_selector,)


@app.cell
def _(catalog_selector, mo, pd, sess):
    table_rows = []
    table_df = pd.DataFrame()
    if catalog_selector.value:
        sess.set_catalog(catalog_selector.value)
        table_rows = sess.current_catalog().list_tables()
        table_df = pd.DataFrame(table_rows)

    tables_table = mo.ui.radio(options=list(map(str, table_rows)))
    mo.vstack([mo.md(f"### **{sess.current_catalog().name}** Tables"), tables_table])
    return (tables_table,)


@app.cell
def _(sess, tables_table):
    schema = None
    if tables_table.value:
        schema = sess.current_catalog().read_table(identifier=tables_table.value).schema()
    schema
    return


@app.cell
def _(sess, tables_table):
    snippet = None
    if tables_table.value:
        snippet = sess.current_catalog().read_table(identifier=tables_table.value).limit(25).to_pandas()
    snippet
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
