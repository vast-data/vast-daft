import marimo  # type: ignore

__generated_with = "0.13.0"
app = marimo.App(width="full")  # type: ignore


@app.cell
def _(mo):
    mo.md(
        """
        # SQL Console

        Interactive SQL workbench backed by **Daft SQL + Ray**.

        - Pick a **catalog** to browse its tables
        - Use fully-qualified names in SQL: `catalog.table_name`
        - Switch between up to 4 query tabs
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
    from helpers import configure_daft_runner, get_s3_credentials  # type: ignore

    from vast_daft import VastDBCatalog, VastDBConfig

    return (
        VastDBCatalog,
        VastDBConfig,
        configure_daft_runner,
        daft,
        get_s3_credentials,
        os,
        time,
    )


@app.cell
def _(configure_daft_runner):
    print(configure_daft_runner(allow_local_fallback=True))
    return


@app.cell
def _(VastDBCatalog, VastDBConfig, daft, get_s3_credentials, os):
    _ENDPOINT = os.environ.get("VASTDB_ENDPOINT", "http://vippool.ie-dev-pipeline.svc.cluster.local")
    _BUCKET = os.environ.get("VASTDB_BUCKET", "collections-bucket")
    _SCHEMA = os.environ.get("VASTDB_SCHEMA", "collections-schema")
    _AK, _SK = get_s3_credentials()

    _cfg = VastDBConfig(
        endpoint=_ENDPOINT,
        access_key=_AK,
        secret_key=_SK,
        bucket=_BUCKET,
        schema=_SCHEMA,
        ssl_verify=False,
    )

    available_catalogs: dict = {}

    _cat = VastDBCatalog(_cfg, alias="vastdb")
    daft.attach_catalog(_cat, "vastdb")
    available_catalogs["vastdb"] = _cat

    print(f"Attached catalogs: {list(available_catalogs)}")
    return (available_catalogs,)


@app.cell
def _(available_catalogs, mo):
    catalog_picker = mo.ui.dropdown(
        options=list(available_catalogs),
        value=list(available_catalogs)[0] if available_catalogs else "vastdb",
        label="Catalog",
    )
    catalog_picker
    return (catalog_picker,)


@app.cell
def _(available_catalogs, catalog_picker, mo):
    """Schema browser — reactive to the catalog dropdown."""
    _selected = catalog_picker.value
    _cat = available_catalogs.get(_selected)

    if _cat is None:
        _body = mo.md(f"*Catalog `{_selected}` not available.*")
    else:
        try:
            _table_list = sorted(str(t) for t in _cat.list_tables())
        except Exception as _e:
            _table_list = []
            print(f"Could not list tables for {_selected}: {_e}")

        _count = len(_table_list)
        print(f"Tables in {_selected}: {_count}")
        for _t in _table_list[:20]:
            print(f"  {_selected}.{_t}")
        if _count > 20:
            print(f"  ... and {_count - 20} more")

        if _table_list:
            _lines = "\n".join(f"- `{_selected}.{t}`" for t in _table_list[:80])
            _more = f"\n\n*... and {_count - 80} more*" if _count > 80 else ""
            _body = mo.md(f"**{_count} table(s)**\n\n{_lines}{_more}")
        else:
            _body = mo.md(f"*No tables found in `{_selected}`.*")

    mo.accordion({f"Tables ({_selected})": _body})
    return


@app.cell
def _(mo):
    editor_1 = mo.ui.code_editor(
        value="SELECT * FROM vastdb.collection LIMIT 10",
        language="sql",
        min_height=150,
    )
    editor_2 = mo.ui.code_editor(value="", language="sql", min_height=150)
    editor_3 = mo.ui.code_editor(value="", language="sql", min_height=150)
    editor_4 = mo.ui.code_editor(value="", language="sql", min_height=150)

    query_tabs = mo.ui.tabs(
        {
            "Query 1": editor_1,
            "Query 2": editor_2,
            "Query 3": editor_3,
            "Query 4": editor_4,
        }
    )
    query_tabs
    return (editor_1, editor_2, editor_3, editor_4, query_tabs)


@app.cell
def _(daft, editor_1, editor_2, editor_3, editor_4, mo, query_tabs, time):
    """Execute only the active tab's query and show results."""
    _editors = {
        "Query 1": editor_1,
        "Query 2": editor_2,
        "Query 3": editor_3,
        "Query 4": editor_4,
    }
    _active_label = query_tabs.value or "Query 1"
    _active_editor = _editors.get(_active_label, editor_1)
    _query = (_active_editor.value or "").strip()

    if not _query:
        _output = mo.md("*Write a SQL query in the editor above.*")
    else:
        try:
            _t0 = time.perf_counter()
            _pdf = daft.sql(_query).to_pandas()
            _elapsed = time.perf_counter() - _t0
            _n, _c = len(_pdf), len(_pdf.columns)
            print(f"{_active_label}: {_n:,} rows x {_c} cols in {_elapsed:.2f}s")

            _header = mo.md(f"**{_n:,}** rows x **{_c}** cols in **{_elapsed:.2f}s**")
            try:
                _fname = _active_label.lower().replace(" ", "_")
                _csv = _pdf.to_csv(index=False).encode()
                _dl = mo.download(data=_csv, filename=f"{_fname}_result.csv", label="Download CSV")
            except Exception:
                _dl = mo.md("")
            _table = mo.ui.table(_pdf, page_size=20)
            _output = mo.vstack([_header, _dl, _table])
        except Exception as _e:
            print(f"{_active_label} error: {type(_e).__name__}: {_e}")
            _output = mo.callout(
                mo.md(f"**Error**\n\n```\n{type(_e).__name__}: {_e}\n```"),
                kind="danger",
            )

    _output
    return


if __name__ == "__main__":
    app.run()
