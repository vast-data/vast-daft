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
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import os
    import time

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
        time,
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
    ENDPOINT = os.environ.get("VASTDB_ENDPOINT", "http://vippool.ie-dev-pipeline.svc.cluster.local")
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
        bucket="event-broker-eagle",
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
def _(catalog_selector, mo, tables_table):
    _sql_default = ""
    _py_default = ""
    if catalog_selector.value and tables_table.value:
        _sql_default = f"""SELECT *\nFROM {catalog_selector.value}."{tables_table.value}"\nLIMIT 100"""
        _py_default = (
            f'df = sess.sql(\'SELECT * FROM {catalog_selector.value}."{tables_table.value}" LIMIT 100\')\n'
        )
    sql_editor = mo.ui.code_editor(value=_sql_default, language="sql", min_height=150)
    sql_run_btn = mo.ui.button(label="▶ Execute (Shift+Enter)")
    py_editor = mo.ui.code_editor(value=_py_default, language="python", min_height=150)
    py_run_btn = mo.ui.button(label="▶ Execute (Shift+Enter)")
    return py_editor, py_run_btn, sql_editor, sql_run_btn


@app.cell
def _(daft, mo, py_editor, py_run_btn, sess, time):
    py_run_btn
    _code = py_editor.value.strip()
    if _code:
        try:
            _t0 = time.perf_counter()
            _ns = {"daft": daft, "mo": mo, "sess": sess}
            exec(_code, _ns)
            _elapsed = time.perf_counter() - _t0
            _df = _ns.get("df")
            if _df is not None:
                _data = _df.limit(1000).to_pydict() if hasattr(_df, "limit") else _df.to_pydict()
                py_output = mo.vstack([
                    mo.md(f"_Executed in {_elapsed:.2f}s_"),
                    mo.ui.table(_data, selection=None).style({"max-height": "400px", "overflow": "auto"}),
                ])
            else:
                py_output = mo.md(f"_Executed in {_elapsed:.2f}s (assign result to `df` to display it)_")
        except Exception as e:
            py_output = mo.callout(mo.md(f"**Error:** {e}"), kind="danger")
    else:
        py_output = mo.md("_Write Python code and press Shift+Enter or click Execute. Use `daft` and assign result to `df`._")
    return (py_output,)


@app.cell
def _(mo, sess, sql_editor, sql_run_btn, time):
    sql_run_btn
    _query = sql_editor.value.strip()
    if _query:
        try:
            _t0 = time.perf_counter()
            _result = sess.sql(_query)
            _data = _result.limit(25).to_pydict()
            _elapsed = time.perf_counter() - _t0
            sql_output = mo.vstack([
                mo.md(f"_Query completed in {_elapsed:.2f}s — {len(_data):,} rows_"),
                mo.ui.table(_data, selection=None).style({"max-height": "400px", "overflow": "auto"})
            ])
        except Exception as e:
            sql_output = mo.callout(mo.md(f"**Error:** {e}"), kind="danger")
    else:
        sql_output = mo.md("_Write a query and press Shift+Enter or click Execute._")
    return (sql_output,)


@app.cell
def _(
    mo,
    py_editor,
    py_output,
    py_run_btn,
    sess,
    sql_editor,
    sql_output,
    sql_run_btn,
    tables_table,
):
    _output = mo.md("_Select a table above to view its schema._")
    if tables_table.value:
        _tbl = sess.current_catalog().read_table(identifier=tables_table.value)
        _schema = _tbl.schema()
        _schema_rows = [
            {"Column": name, "Type": str(dtype)}
            for name, dtype in zip(_schema.column_names(), _schema.to_pyarrow_schema().types)
        ]
        # _data = _tbl.limit(25).to_pydict()
        _sql_tab = mo.vstack([sql_editor, sql_run_btn, sql_output])
        _py_tab = mo.vstack([py_editor, py_run_btn, py_output])
        _output = mo.ui.tabs(
            {
                "SQL": _sql_tab,
                "Schema": mo.ui.table(_schema_rows, selection=None, page_size=len(_schema_rows)).style({"max-height": "400px", "overflow": "auto"}),
                "DataFrame": _py_tab,
            }
        )
    _output
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
