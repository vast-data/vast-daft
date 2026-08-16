import marimo  # type: ignore

__generated_with = "0.13.0"
app = marimo.App(width="medium")  # type: ignore


@app.cell
def _(mo):
    mo.md(
        """
        # Daft SQL endpoint (Flight SQL)

        This notebook is a **client** of `daft-sql-ep`. It does not import Daft or
        join the Ray cluster. SQL goes over Arrow Flight SQL; the coordinator runs
        `daft.sql()` on Ray and streams Arrow batches back.

        ```python
        import base64
        from adbc_driver_flightsql import dbapi
        token = base64.b64encode(f"{access_key}:{secret_key}".encode()).decode()
        conn = dbapi.connect(
            os.environ["DAFT_SQL_EP_FLIGHT_URI"],
            db_kwargs={
                "adbc.flight.sql.authorization_header": f"Basic {token}",
                "adbc.flight.sql.rpc.call_header.x-daft-catalog": "vastdb",
                "adbc.flight.sql.rpc.call_header.x-daft-vastdb-endpoint": os.environ["VASTDB_ENDPOINT"],
            },
        )
        ```
        """
    )
    return


@app.cell
def _():
    import marimo as mo  # type: ignore

    return (mo,)


@app.cell
def _():
    import json
    import os
    import time
    import urllib.request

    from adbc_driver_flightsql import dbapi

    return dbapi, json, os, time, urllib


@app.cell
def _(mo, os):
    flight_uri = os.environ.get(
        "DAFT_SQL_EP_FLIGHT_URI",
        "grpc://daft-sql-ep-svc.ray-system.svc.cluster.local:8815",
    )
    http_uri = os.environ.get(
        "DAFT_SQL_EP_HTTP_URI",
        "http://daft-sql-ep-svc.ray-system.svc.cluster.local:8080",
    )
    mo.md(f"**Flight SQL:** `{flight_uri}`  \n**HTTP ops:** `{http_uri}`")
    return flight_uri, http_uri


@app.cell
def _(mo):
    mo.md("## Health")
    return


@app.cell
def _(http_uri, json, mo, urllib):
    _health = json.loads(urllib.request.urlopen(f"{http_uri}/health", timeout=10).read())
    _info = json.loads(urllib.request.urlopen(f"{http_uri}/v1/info", timeout=10).read())
    print(f"runner={_health.get('runner')} catalogs={_health.get('catalogs')}")
    mo.vstack(
        [
            mo.md(f"**status:** `{_health.get('status')}`  **runner:** `{_health.get('runner')}`"),
            mo.ui.table([_health], page_size=1),
            mo.ui.table([_info], page_size=1),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md("## Connect (ADBC)")
    return


@app.cell
def _(dbapi, flight_uri, os):
    import base64

    _access = os.environ.get("S3_ACCESS_KEY") or os.environ.get("VASTDB_ACCESS_KEY", "")
    _secret = os.environ.get("S3_SECRET_KEY") or os.environ.get("VASTDB_SECRET_KEY", "")
    _token = base64.b64encode(f"{_access}:{_secret}".encode()).decode()
    _endpoint = os.environ.get("VASTDB_ENDPOINT", "")
    _kwargs = {
        "adbc.flight.sql.authorization_header": f"Basic {_token}",
        "adbc.flight.sql.rpc.call_header.x-daft-catalog": os.environ.get("DAFT_SQL_EP_CATALOG", "vastdb"),
    }
    if _endpoint:
        _kwargs["adbc.flight.sql.rpc.call_header.x-daft-vastdb-endpoint"] = _endpoint
    if os.environ.get("VASTDB_SCHEMA"):
        _kwargs["adbc.flight.sql.rpc.call_header.x-daft-schema"] = os.environ["VASTDB_SCHEMA"]
    if os.environ.get("VASTDB_BUCKET"):
        _kwargs["adbc.flight.sql.rpc.call_header.x-daft-bucket"] = os.environ["VASTDB_BUCKET"]
    conn = dbapi.connect(flight_uri, db_kwargs=_kwargs)
    print(f"connected {flight_uri} endpoint={_endpoint or '(server default)'}")
    return (conn,)


@app.cell
def _(mo):
    mo.md("## Catalogs via SQL intercept")
    return


@app.cell
def _(conn, mo):
    def run_sql(sql: str):
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetch_arrow_table()

    _catalogs = run_sql("SHOW CATALOGS")
    print(_catalogs)
    mo.ui.table(_catalogs.to_pylist(), page_size=20)
    return (run_sql,)


@app.cell
def _(mo):
    mo.md("## `SELECT 1` — Arrow end-to-end")
    return


@app.cell
def _(mo, run_sql, time):
    _t0 = time.perf_counter()
    _ones = run_sql("SELECT 1 AS n")
    _elapsed = time.perf_counter() - _t0
    print(_ones)
    print(f"{_elapsed:.3f}s  schema={_ones.schema}")
    mo.md(f"**SELECT 1** → `{_ones.column('n').to_pylist()}` in **{_elapsed:.3f}s** (Arrow `{_ones.schema}`)")
    return


@app.cell
def _(mo, run_sql):
    try:
        _tables = run_sql("SHOW TABLES")
        _n = _tables.num_rows
        print(_tables)
        _body = mo.ui.table(_tables.to_pylist(), page_size=20) if _n else mo.md("*No tables (catalog not attached on the endpoint).*")
    except Exception as _exc:
        print(f"SHOW TABLES: {_exc}")
        _body = mo.md(f"*SHOW TABLES failed:* `{_exc}`")
    mo.accordion({"SHOW TABLES": _body})
    return


@app.cell
def _(mo):
    mo.md("## Interactive query")
    return


@app.cell
def _(mo):
    sql_editor = mo.ui.code_editor(
        value="SELECT 1 AS n",
        language="sql",
        min_height=120,
    )
    run_query = mo.ui.run_button(label="Run via Flight SQL")
    mo.vstack([sql_editor, run_query])
    return run_query, sql_editor


@app.cell
def _(mo, run_query, run_sql, sql_editor, time):
    _sql = (sql_editor.value or "").strip()
    if not run_query.value:
        _out = mo.md("*Click **Run via Flight SQL** to execute.*")
    elif not _sql:
        _out = mo.md("*Write a SQL query.*")
    else:
        try:
            _t0 = time.perf_counter()
            _table = run_sql(_sql)
            _elapsed = time.perf_counter() - _t0
            _pdf = _table.to_pandas()
            print(f"{len(_pdf):,} rows x {len(_pdf.columns)} cols in {_elapsed:.2f}s")
            _out = mo.vstack(
                [
                    mo.md(f"**{len(_pdf):,}** rows × **{len(_pdf.columns)}** cols in **{_elapsed:.2f}s**"),
                    mo.ui.table(_pdf, page_size=20),
                ]
            )
        except Exception as _exc:
            print(f"{type(_exc).__name__}: {_exc}")
            _out = mo.callout(mo.md(f"**Error**\n\n```\n{type(_exc).__name__}: {_exc}\n```"), kind="danger")
    _out
    return


if __name__ == "__main__":
    app.run()
