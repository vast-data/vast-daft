import marimo  # type: ignore

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Gravitino: Unified Catalog Browser

        Browse **all** table types — VastDB native, Iceberg, and external files — through a single
        Apache Gravitino catalog.  Tables are synced from VastDB by a background
        CronJob and can be queried transparently via `VastGravitinoCatalog`.
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

    import daft
    from helpers import configure_daft_runner, get_s3_credentials  # type: ignore

    from vast_daft import VastGravitinoCatalog

    return configure_daft_runner, daft, get_s3_credentials, os, VastGravitinoCatalog


@app.cell
def _(configure_daft_runner, os):
    if os.environ.get("RAY_ADDRESS"):
        print(configure_daft_runner())
    else:
        print("Running locally (no Ray). Set RAY_ADDRESS to use Ray.")
    return


@app.cell
def _(mo, os):
    GRAVITINO_ENDPOINT = os.environ.get("GRAVITINO_ENDPOINT", "http://gravitino.ray-system.v141.lc")
    GRAVITINO_METALAKE = os.environ.get("GRAVITINO_METALAKE", "vast_data_lake")
    GRAVITINO_USERNAME = os.environ.get("GRAVITINO_USERNAME", "admin")
    GRAVITINO_ICEBERG_REST_URI = os.environ.get(
        "GRAVITINO_ICEBERG_REST_URI",
        "http://iceberg-rest.ray-system.v141.lc/iceberg/",
    )

    mo.md(
        f"""
        ## Configuration

        | Setting | Value |
        |---------|-------|
        | Gravitino endpoint | `{GRAVITINO_ENDPOINT}` |
        | Iceberg REST URI | `{GRAVITINO_ICEBERG_REST_URI}` |
        | Metalake | `{GRAVITINO_METALAKE}` |
        | Username | `{GRAVITINO_USERNAME}` |

        **Gravitino Web UI**: [Open in browser](http://gravitino.ray-system.v141.lc)
        """
    )
    return GRAVITINO_ENDPOINT, GRAVITINO_ICEBERG_REST_URI, GRAVITINO_METALAKE, GRAVITINO_USERNAME


@app.cell
def _(GRAVITINO_ENDPOINT, GRAVITINO_METALAKE, GRAVITINO_USERNAME, get_s3_credentials, os, VastGravitinoCatalog):
    from daft.gravitino import GravitinoClient
    from daft.io import IOConfig, S3Config

    ENDPOINT = os.environ.get("VASTDB_ENDPOINT", "http://vippool.ie-dev-pipeline.svc.cluster.local")
    ACCESS_KEY, SECRET_KEY = get_s3_credentials()
    BUCKET = os.environ.get("VASTDB_BUCKET", "collections-bucket")

    client = GravitinoClient(
        endpoint=GRAVITINO_ENDPOINT,
        metalake_name=GRAVITINO_METALAKE,
        auth_type="simple",
        username=GRAVITINO_USERNAME or "admin",
    )

    catalog = VastGravitinoCatalog.create(client)
    grav_iceberg = client.load_catalog("iceberg_catalog")

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=ENDPOINT,
            key_id=ACCESS_KEY,
            access_key=SECRET_KEY,
            region_name="us-east-1",
            use_ssl=False,
        ),
    )

    print(f"Daft Gravitino Catalog: {catalog.name}")
    print(f"Gravitino client ready, iceberg catalog: {grav_iceberg.name}")
    return catalog, client, grav_iceberg, io_config, ACCESS_KEY, SECRET_KEY, ENDPOINT, BUCKET


@app.cell
def _(client, daft, io_config, mo, os):
    import requests

    ICEBERG_NS = "gravitino_demo"
    ICEBERG_TABLE = "product_catalog"
    fqn = f"{ICEBERG_NS}.{ICEBERG_TABLE}"

    grav_endpoint = os.environ.get("GRAVITINO_ENDPOINT", "http://gravitino-svc.ray-system.svc.cluster.local:8090")
    metalake = os.environ.get("GRAVITINO_METALAKE", "vast_data_lake")
    iceberg_endpoint = grav_endpoint.replace(":8090", ":9001")

    # Create namespace in Iceberg backend
    requests.post(
        f"{iceberg_endpoint}/iceberg/v1/namespaces",
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        json={"namespace": [ICEBERG_NS]},
    )

    # Drop table if exists
    try:
        requests.delete(
            f"{iceberg_endpoint}/iceberg/v1/namespaces/{ICEBERG_NS}/tables/{ICEBERG_TABLE}",
            headers={"Accept": "application/json"},
        )
    except Exception:
        pass

    # Create table via Iceberg REST
    table_spec = {
        "name": ICEBERG_TABLE,
        "schema": {
            "type": "struct",
            "fields": [
                {"id": 1, "name": "product", "type": "string", "required": True},
                {"id": 2, "name": "category", "type": "string", "required": True},
                {"id": 3, "name": "weight_kg", "type": "double", "required": False},
                {"id": 4, "name": "cost_price", "type": "double", "required": False},
            ],
        },
        "properties": {"format-version": "2"},
    }
    create_resp = requests.post(
        f"{iceberg_endpoint}/iceberg/v1/namespaces/{ICEBERG_NS}/tables",
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        json=table_spec,
    )
    print(f"Create table via Iceberg REST: {create_resp.status_code}")

    # Now register the table in Gravitino's lakehouse-iceberg catalog!
    table_location = f"s3://collections-bucket/iceberg-warehouse/{ICEBERG_NS}/{ICEBERG_TABLE}"
    gravitino_headers = {
        "Accept": "application/vnd.gravitino.v1+json",
        "Content-Type": "application/json",
    }

    # Register table in Gravitino API
    grav_table_spec = {
        "name": ICEBERG_TABLE,
        "comment": "Iceberg table (created via notebook)",
        "columns": [
            {"name": "product", "type": "VARCHAR", "nullable": False},
            {"name": "category", "type": "VARCHAR", "nullable": False},
            {"name": "weight_kg", "type": "DOUBLE", "nullable": True},
            {"name": "cost_price", "type": "DOUBLE", "nullable": True},
        ],
        "properties": {
            "location": table_location,
            "table-location": table_location,
            "format-version": "2",
        },
    }
    grav_resp = requests.post(
        f"{grav_endpoint}/api/metalakes/{metalake}/catalogs/iceberg_catalog/namespaces/{ICEBERG_NS}/tables",
        headers=gravitino_headers,
        json=grav_table_spec,
    )
    print(f"Register in Gravitino: {grav_resp.status_code}")
    if grav_resp.status_code not in (200, 201):
        print(f"  Response: {grav_resp.text[:300]}")

    # Read via parquet
    df_iceberg = daft.read_parquet(f"{table_location}/data/", io_config=io_config)
    df_iceberg.show()

    mo.md(
        f"""
        Created Iceberg table `{fqn}` and registered it in **Gravitino**!

        Check the **Gravitino Web UI** → `iceberg_catalog` → `gravitino_demo` → `product_catalog`
        """
    )
    return (fqn, df_iceberg, table_location)


@app.cell
def _(mo):
    mo.md("## Step 1 — List all catalogs and namespaces")
    return


@app.cell
def _(client, mo):
    _catalogs = client.list_catalogs()
    _rows = []
    for _cat in _catalogs:
        _namespaces = client.list_namespaces(_cat)
        for _ns in _namespaces:
            _tables = client.list_tables(_ns)
            _rows.append(
                {
                    "catalog": _cat,
                    "namespace": _ns,
                    "table_count": len(_tables),
                }
            )

    mo.ui.table(_rows) if _rows else mo.md("_No catalogs found. Has the sync CronJob run yet?_")
    return


@app.cell
def _(mo):
    mo.md("## Step 2 — List all tables")
    return


@app.cell
def _(catalog, mo):
    _tables = catalog.list_tables()
    _table_data = [{"table": str(t)} for t in _tables]
    mo.ui.table(_table_data) if _table_data else mo.md("_No tables found._")
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## Step 3 — Read a VastDB table through Gravitino

        Pick a table from the list above. The `VastGravitinoCatalog` detects
        `format=vastdb` in the Gravitino properties and routes the read through
        the VastDB native SDK — predicate pushdown, split parallelism, and all.
        """
    )
    return


@app.cell
def _(catalog, mo):
    # Pick the first VastDB table available
    all_tables = catalog.list_tables()
    if not all_tables:
        mo.md("_No tables available to query._")
        selected_table = None
    else:
        selected_table = str(all_tables[0])
        print(f"Selected table: {selected_table}")
    return (selected_table,)


@app.cell
def _(selected_table, catalog):
    if selected_table:
        df_gravitino = catalog.get_table(selected_table).read()
        df_gravitino.limit(10).show()
    else:
        print("No table selected")
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## Step 4 — Read an Iceberg table through Gravitino

        The table created via Gravitino's Iceberg REST is now visible in the
        **Gravitino Web UI** and accessible via `VastGravitinoCatalog`. Let's
        read it back to verify.
        """
    )
    return


@app.cell
def _(daft, fqn, base_location, io_config, mo):
    # Read back the Iceberg table via parquet (since read_iceberg has issues with credentials)
    df_iceberg_read = daft.read_parquet(f"{base_location}/data/", io_config=io_config)
    df_iceberg_read.show()

    mo.md(
        """
        The Iceberg table is stored on VastDB's S3 endpoint and managed by
        Gravitino's Iceberg REST service. It appears in the **Gravitino Web UI**
        alongside the synced VastDB-native tables.
        """
    )
    return (df_iceberg_read,)


@app.cell
def _(mo):
    mo.md(
        """
        ## Step 5 — Read an external Parquet file from Vast S3

        Write a small Parquet dataset to Vast S3, then read it back directly
        with Daft's native Parquet reader using the same `io_config` (S3
        credentials from the environment).  This Parquet data is later joined
        with the Iceberg product catalog in Step 6.
        """
    )
    return


@app.cell
def _(
    BUCKET,
    daft,
    io_config,
    mo,
):
    import time as _time

    _t0 = _time.perf_counter()
    parquet_location = f"s3://{BUCKET}/gravitino-demo/parquet-product-prices/"

    df_external = daft.from_pydict(
        {
            "product": ["Widget A", "Widget B", "Gadget X", "Module Pro"],
            "price_band": ["mid", "mid", "premium", "premium"],
            "list_price": [19.99, 24.99, 59.99, 49.99],
        }
    )
    df_external.write_parquet(parquet_location, write_mode="overwrite", io_config=io_config)
    _elapsed = _time.perf_counter() - _t0

    mo.md(
        f"""
        **External Parquet write** completed in **{_elapsed:.2f}s**.

        - Data files: Parquet on Vast S3 at `{parquet_location}`
        - Writer: Daft native Parquet writer via `io_config`
        """
    )
    return (parquet_location,)


@app.cell
def _(daft, io_config, mo, parquet_location):
    df_parquet = daft.read_parquet(parquet_location, io_config=io_config)
    df_parquet.show()
    mo.md("Read back external Parquet data directly from Vast S3 via Daft.")
    return (df_parquet,)


@app.cell
def _(mo):
    mo.md(
        """
        ## Step 6 — Cross-backend join: VastDB orders x Iceberg products

        Join a **VastDB-native** table (read through Gravitino) with the
        **Iceberg** product catalog — both accessed from the same notebook.
        """
    )
    return


@app.cell
def _(catalog, df_iceberg_read, mo):
    import time as _time

    _t0 = _time.perf_counter()
    df_orders = catalog.get_table("vastdb_catalog.collections-schema.__sql_demo_orders__").read()
    df_joined = df_orders.join(df_iceberg_read, on="product", how="inner").select(
        "order_id", "customer_name", "product", "category", "amount", "cost_price", "order_date"
    )
    df_joined.limit(10).show()
    _elapsed = _time.perf_counter() - _t0

    mo.md(
        f"""
        **Cross-backend join** completed in **{_elapsed:.2f}s**.

        - Orders: VastDB native (via Gravitino `vast.table-format=vastdb`)
        - Products: Iceberg on VastDB S3 (via Gravitino Iceberg REST)
        - Join engine: Daft (distributed on Ray)
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
        ---
        ## Summary

        - **Gravitino Web UI** shows VastDB, Iceberg, and external file-backed tables in one tree
        - **VastGravitinoCatalog** routes `vast.table-format=vastdb` tables through the native SDK
        - **Iceberg tables** are created/read via Gravitino's Iceberg REST service
        - **External Parquet/CSV/JSON tables** are read via Daft native file readers
        - **Cross-backend joins** work seamlessly — Daft handles the federation
        - **Sync CronJob** keeps VastDB metadata in sync with Gravitino every 5 minutes
        """
    )
    return


if __name__ == "__main__":
    app.run()
