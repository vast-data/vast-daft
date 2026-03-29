import marimo

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
    BUCKET = os.environ.get("VASTDB_BUCKET", "collections-bucket")
    SCHEMA = os.environ.get("VASTDB_SCHEMA", "collections-schema")
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

    kafka_config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        bucket="event-broker",
        schema="kafka_topics",
        ssl_verify=False,
    )
    kafka_catalog = VastDBCatalog(kafka_config, alias="vast_kafka")

    iceberg_catalog = make_shared_iceberg_catalog(name="s3_iceberg")

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
    sess.attach_catalog(kafka_catalog)


    sess.set_catalog(vastdb_catalog.name)
    return (sess,)


@app.cell
def _(mo, sess):
    catalog_selector = mo.ui.dropdown(options=sess.list_catalogs())

    mo.vstack(
        [
            mo.md("### Catalogs"),
            catalog_selector,
        ]
    )
    return (catalog_selector,)


@app.cell
def _():
    return


@app.cell
def _(catalog_selector, mo, sess):
    table_rows = []
    if catalog_selector.value:
        sess.set_catalog(catalog_selector.value)
        table_rows = sess.current_catalog().list_tables()

    tables_table = mo.ui.radio(options=list(map(str, table_rows)))

    _output = (
        mo.vstack([mo.md(f"### **{sess.current_catalog().name}** Tables"), tables_table])
        if table_rows
        else mo.md("_Select a catalog above to list its tables._")
    )
    _output
    return (tables_table,)


@app.cell
def _(mo, sess, tables_table):
    _output = mo.md("_Select a table above to view its schema._")
    if tables_table.value:
        _tbl = sess.current_catalog().read_table(identifier=tables_table.value)
        _schema = _tbl.schema()
        _schema_rows = [
            {"Column": name, "Type": str(dtype)}
            for name, dtype in zip(_schema.column_names(), _schema.to_pyarrow_schema().types)
        ]
        _data = _tbl.limit(25).to_pandas()
        _output = mo.ui.tabs(
            {
                "Data": _data,
                "Schema": mo.ui.table(_schema_rows, selection=None, page_size=len(_schema_rows)),
            }
        )
    _output
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
