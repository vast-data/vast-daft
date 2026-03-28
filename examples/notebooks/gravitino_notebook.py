import marimo  # type: ignore

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Gravitino: Unified Catalog Browser

        Browse **all** table types — VastDB native and Iceberg — through a single
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
def _(GRAVITINO_ENDPOINT, GRAVITINO_METALAKE, GRAVITINO_USERNAME, VastGravitinoCatalog):
    from daft.gravitino import GravitinoClient

    client = GravitinoClient(
        endpoint=GRAVITINO_ENDPOINT,
        metalake_name=GRAVITINO_METALAKE,
        auth_type="simple",
        username=GRAVITINO_USERNAME,
    )
    catalog = VastGravitinoCatalog.create(client)
    print(f"Daft Gravitino Catalog: {catalog.name}")
    return catalog, client


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
        ## Step 4 — Create and read an Iceberg table through Gravitino

        Write a small product catalog as an **Iceberg table** on VastDB's S3
        storage, then read it back through the same `VastGravitinoCatalog`.
        This proves both VastDB-native and Iceberg tables are accessible via
        one unified catalog.
        """
    )
    return


@app.cell
def _(GRAVITINO_ICEBERG_REST_URI, GRAVITINO_USERNAME, daft, get_s3_credentials, os):
    from daft.io import IOConfig, S3Config
    from pyiceberg.catalog.rest import RestCatalog

    ENDPOINT = os.environ.get("VASTDB_ENDPOINT", "http://vippool.ie-dev-pipeline.svc.cluster.local")
    ACCESS_KEY, SECRET_KEY = get_s3_credentials()
    BUCKET = os.environ.get("VASTDB_BUCKET", "collections-bucket")

    iceberg_catalog = RestCatalog(
        name="gravitino_iceberg",
        uri=GRAVITINO_ICEBERG_REST_URI,
        warehouse="vastdb_catalog",
        **{"header.X-Gravitino-User": GRAVITINO_USERNAME},
    )

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=ENDPOINT,
            key_id=ACCESS_KEY,
            access_key=SECRET_KEY,
            region_name="us-east-1",
            use_ssl=False,
        ),
    )

    print(f"Iceberg REST catalog at {GRAVITINO_ICEBERG_REST_URI}")
    return iceberg_catalog, io_config, ACCESS_KEY, SECRET_KEY, ENDPOINT, BUCKET


@app.cell
def _(daft, iceberg_catalog, io_config, mo):
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.types import DoubleType, NestedField, StringType

    ICEBERG_NS = "gravitino_demo"
    ICEBERG_TABLE = "product_catalog"

    # Create namespace + table
    iceberg_catalog.create_namespace_if_not_exists(ICEBERG_NS)
    fqn = f"{ICEBERG_NS}.{ICEBERG_TABLE}"
    if iceberg_catalog.table_exists(fqn):
        iceberg_catalog.drop_table(fqn)

    iceberg_schema = IcebergSchema(
        NestedField(field_id=1, name="product", field_type=StringType(), required=True),
        NestedField(field_id=2, name="category", field_type=StringType(), required=True),
        NestedField(field_id=3, name="weight_kg", field_type=DoubleType(), required=False),
        NestedField(field_id=4, name="cost_price", field_type=DoubleType(), required=False),
    )
    iceberg_table = iceberg_catalog.create_table(fqn, schema=iceberg_schema)

    df_products = daft.from_pydict(
        {
            "product": [
                "Widget A",
                "Widget B",
                "Gadget X",
                "Gadget Y",
                "Thingamajig",
                "Doohickey",
                "Contraption Z",
                "Module Pro",
                "Sensor Lite",
                "Adapter Max",
            ],
            "category": [
                "Widgets",
                "Widgets",
                "Gadgets",
                "Gadgets",
                "Misc",
                "Misc",
                "Contraptions",
                "Modules",
                "Sensors",
                "Adapters",
            ],
            "weight_kg": [0.5, 0.7, 1.2, 1.5, 0.3, 0.2, 2.1, 0.8, 0.1, 0.4],
            "cost_price": [15.0, 20.0, 45.0, 55.0, 8.0, 5.0, 80.0, 35.0, 12.0, 18.0],
        }
    )
    df_products.write_iceberg(iceberg_table, mode="append", io_config=io_config).show()

    mo.md(f"Wrote **10 products** to Iceberg table `{fqn}` via Gravitino REST catalog.")
    return iceberg_table, fqn, ICEBERG_NS, ICEBERG_TABLE


@app.cell
def _(daft, fqn, iceberg_catalog, io_config, mo):
    # Read back the Iceberg table
    loaded_iceberg = iceberg_catalog.load_table(fqn)
    df_iceberg_read = daft.read_iceberg(loaded_iceberg, io_config=io_config)
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
        ## Step 5 — Cross-backend join: VastDB orders x Iceberg products

        Join a **VastDB-native** table (read through Gravitino) with the
        **Iceberg** product catalog — both accessed from the same notebook.
        """
    )
    return


@app.cell
def _(catalog, df_iceberg_read, mo):
    import time as _time

    # Read VastDB orders through Gravitino
    t0 = _time.perf_counter()
    df_orders = catalog.get_table("vastdb_catalog.collections-schema.__xbackend_orders__").read()
    df_joined = df_orders.join(df_iceberg_read, on="product", how="inner").select(
        "order_id", "customer_id", "product", "category", "amount", "cost_price", "order_date"
    )
    df_joined.limit(10).show()
    elapsed = _time.perf_counter() - t0

    mo.md(
        f"""
        **Cross-backend join** completed in **{elapsed:.2f}s**.

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

        - **Gravitino Web UI** shows all VastDB + Iceberg tables in one tree
        - **VastGravitinoCatalog** routes `vast.table-format=vastdb` tables through the native SDK
        - **Iceberg tables** are created/read via Gravitino's Iceberg REST service
        - **Cross-backend joins** work seamlessly — Daft handles the federation
        - **Sync CronJob** keeps VastDB metadata in sync with Gravitino every 5 minutes
        """
    )
    return


if __name__ == "__main__":
    app.run()
