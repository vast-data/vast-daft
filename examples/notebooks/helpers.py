"""Shared helpers for Marimo example notebooks."""

from __future__ import annotations

import os

import daft
import numpy as np
import pyarrow as pa
import ray
from pyiceberg.catalog.sql import SqlCatalog

DEFAULT_PRODUCTS: list[str] = [
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
]
DEFAULT_TIERS: list[str] = ["bronze", "silver", "gold", "platinum"]
DAFT_NIGHTLY_FIND_LINKS_URL: str = "https://ds0gqyebztuyf.cloudfront.net/builds/nightly/daft/index.html"
DAFT_VERSION: str = "0.7.10.dev58+g9c99919f9"

VAST_DAFT_DEPS: list[str] = [
    "--pre",
    f"--find-links={DAFT_NIGHTLY_FIND_LINKS_URL}",
    f"daft=={DAFT_VERSION}",
    "numpy<2",
    "ray==2.53.0",
    "pylance>=0.39.0",
    "vastdb>=1.2",
    "pyiceberg[s3fs,sql-sqlite]>=0.11.1",
    "pyarrow>=15.0",
]

SHARED_STORAGE_PATH: str = "/shared"
SHARED_ICEBERG_CATALOG_DB: str = "iceberg_catalog.db"


def configure_daft_runner(*, allow_local_fallback: bool = True) -> str:
    """Configure Daft for marimo and connect to Ray when available."""
    import glob

    os.environ["RAY_TQDM_DISABLE"] = "1"
    os.environ["RAY_LOG_TO_DRIVER"] = "0"
    os.environ["PYTHONWARNINGS"] = "ignore::DeprecationWarning"

    try:
        # Build runtime_env: wheel via py_modules + deps via pip
        runtime_env = {}
        wheel_path = os.environ.get("VAST_DAFT_WHEEL")
        if wheel_path and not os.path.isfile(wheel_path):
            wheel_matches = glob.glob(os.path.join(wheel_path, "vast_daft-*.whl"))
            wheel_path = wheel_matches[0] if wheel_matches else wheel_path
        if not wheel_path:
            wheel_matches = glob.glob("/mnt/wheel/vast_daft-*.whl")
            wheel_path = wheel_matches[0] if wheel_matches else None
        if wheel_path and os.path.isfile(wheel_path):
            runtime_env["py_modules"] = [wheel_path]
            runtime_env["pip"] = VAST_DAFT_DEPS

        ray_address = os.environ.get("RAY_ADDRESS")
        ray.init(address=ray_address, runtime_env=runtime_env or None, ignore_reinit_error=True)
        daft.set_runner_ray(noop_if_initialized=True)
        return f"Connected to Ray (address={ray_address or 'auto'}, wheel={'yes' if wheel_path else 'no'})"
    except Exception as exc:
        if not allow_local_fallback:
            raise
        return f"Using default runner ({type(exc).__name__}: {exc})"


def get_s3_credentials() -> tuple[str, str]:
    """Load notebook S3 credentials from environment variables."""
    access_key = os.environ.get("S3_ACCESS_KEY") or os.environ.get("VASTDB_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY") or os.environ.get("VASTDB_SECRET_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("Missing S3_ACCESS_KEY or S3_SECRET_KEY in the notebook environment.")
    return access_key, secret_key


def generate_customers(
    count: int,
    *,
    seed: int = 42,
    include_email: bool = False,
    tiers: list[str] | None = None,
) -> pa.Table:
    """Generate deterministic customer rows as a PyArrow table."""
    rng = np.random.default_rng(seed)
    tier_values = tiers or DEFAULT_TIERS

    ids = np.arange(1, count + 1, dtype=np.int64)
    names = [f"customer_{i}" for i in range(1, count + 1)]
    tier_idx = rng.integers(0, len(tier_values), size=count)

    columns: dict[str, pa.Array] = {
        "customer_id": pa.array(ids),
        "name": pa.array(names, type=pa.string()),
    }
    if include_email:
        columns["email"] = pa.array([f"user_{i}@example.com" for i in range(1, count + 1)], type=pa.string())
    columns["tier"] = pa.array([tier_values[i] for i in tier_idx], type=pa.string())
    return pa.table(columns)


def generate_orders(
    count: int,
    customer_count: int,
    *,
    seed: int = 123,
    products: list[str] | None = None,
    start_id: int = 1001,
) -> pa.Table:
    """Generate deterministic order rows as a PyArrow table.

    Uses NumPy for vectorized generation — significantly faster than
    pure-Python loops for large row counts (e.g. 1M+ per batch).
    """
    rng = np.random.default_rng(seed)
    product_values = products or DEFAULT_PRODUCTS

    order_ids = np.arange(start_id, start_id + count, dtype=np.int64)
    customer_ids = rng.integers(1, customer_count + 1, size=count)
    product_idx = rng.integers(0, len(product_values), size=count)
    amounts = np.round(rng.uniform(5.0, 500.0, size=count), 2)
    months = rng.integers(1, 13, size=count)
    days = rng.integers(1, 29, size=count)

    product_arr = np.array(product_values)
    product_names = product_arr[product_idx]

    # date32 via numpy datetime64 arithmetic — ~180× faster than np.char string concat
    # for 5M rows. cum_md is the day-of-year offset for the first of each month (non-leap).
    epoch = np.datetime64("2025-01-01", "D")
    cum_md = np.array([0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334], dtype=np.int32)
    order_dates = epoch + (cum_md[months - 1] + (days - 1)).astype("timedelta64[D]")

    return pa.table(
        {
            "order_id": pa.array(order_ids),
            "customer_id": pa.array(customer_ids),
            "product": pa.array(product_names, type=pa.string()),
            "amount": pa.array(amounts),
            "order_date": pa.array(order_dates, type=pa.date32()),
        }
    )


DEFAULT_CATEGORIES: dict[str, str] = {
    "Widget A": "Widgets",
    "Widget B": "Widgets",
    "Gadget X": "Gadgets",
    "Gadget Y": "Gadgets",
    "Thingamajig": "Misc",
    "Doohickey": "Misc",
    "Contraption Z": "Contraptions",
    "Module Pro": "Modules",
    "Sensor Lite": "Sensors",
    "Adapter Max": "Adapters",
}

DEFAULT_WAREHOUSES: list[str] = [
    "US-East",
    "US-West",
    "EU-Central",
    "EU-North",
    "APAC-Tokyo",
    "APAC-Sydney",
]

DEFAULT_SUPPLIERS: list[str] = [
    "Acme Corp",
    "GlobalParts Inc",
    "MegaSupply Co",
    "PrimeSources Ltd",
    "Atlas Manufacturing",
    "Vertex Components",
    "CoreTech Supply",
]

DEFAULT_COLORS: list[str] = [
    "Red",
    "Blue",
    "Green",
    "Black",
    "White",
    "Silver",
    "Gold",
]


def generate_products(
    count: int,
    *,
    seed: int = 777,
    products: list[str] | None = None,
    categories: dict[str, str] | None = None,
    unique_names: bool = False,
) -> pa.Table:
    """Generate a wide product-catalog table as a PyArrow table.

    When *unique_names* is ``True`` each row gets a distinct product name
    (e.g. ``Widget A-001``).  The generated names still start with a base
    product so that ``generate_orders`` output can reference the same names
    when *products* is passed explicitly.

    Columns: product, sku, category, sub_category, supplier, warehouse,
    color, weight_kg, cost_price, retail_price, margin_pct,
    stock_qty, reorder_level, lead_time_days, rating, review_count,
    description
    """
    rng = np.random.default_rng(seed)
    base_products = products or DEFAULT_PRODUCTS
    cat_map = categories or DEFAULT_CATEGORIES
    cat_list = list(set(cat_map.values()))

    if unique_names:
        product_names = [f"{base_products[i % len(base_products)]}-{i + 1:04d}" for i in range(count)]
    else:
        prod_idx = rng.integers(0, len(base_products), size=count)
        product_names = [base_products[i] for i in prod_idx]

    sku_nums = rng.integers(100000, 1000000, size=count)
    sub_cat_nums = rng.integers(1, 21, size=count)
    supplier_idx = rng.integers(0, len(DEFAULT_SUPPLIERS), size=count)
    warehouse_idx = rng.integers(0, len(DEFAULT_WAREHOUSES), size=count)
    color_idx = rng.integers(0, len(DEFAULT_COLORS), size=count)
    weight_kg = np.round(rng.uniform(0.05, 25.0, size=count), 2)
    cost_price = np.round(rng.uniform(1.0, 200.0, size=count), 2)
    retail_price = np.round(rng.uniform(5.0, 500.0, size=count), 2)
    margin_pct = np.round(rng.uniform(0.05, 0.65, size=count), 4)
    stock_qty = rng.integers(0, 10001, size=count)
    reorder_level = rng.integers(10, 501, size=count)
    lead_time_days = rng.integers(1, 91, size=count)
    rating = np.round(rng.uniform(1.0, 5.0, size=count), 1)
    review_count = rng.integers(0, 5001, size=count)
    desc_color_idx = rng.integers(0, len(DEFAULT_COLORS), size=count)
    desc_supplier_idx = rng.integers(0, len(DEFAULT_SUPPLIERS), size=count)

    return pa.table(
        {
            "product": pa.array(product_names, type=pa.string()),
            "sku": pa.array([f"SKU-{n}" for n in sku_nums], type=pa.string()),
            "category": pa.array(
                [cat_map.get(p.split("-")[0], cat_list[0]) for p in product_names],
                type=pa.string(),
            ),
            "sub_category": pa.array([f"sub_{n:02d}" for n in sub_cat_nums], type=pa.string()),
            "supplier": pa.array([DEFAULT_SUPPLIERS[i] for i in supplier_idx], type=pa.string()),
            "warehouse": pa.array([DEFAULT_WAREHOUSES[i] for i in warehouse_idx], type=pa.string()),
            "color": pa.array([DEFAULT_COLORS[i] for i in color_idx], type=pa.string()),
            "weight_kg": pa.array(weight_kg),
            "cost_price": pa.array(cost_price),
            "retail_price": pa.array(retail_price),
            "margin_pct": pa.array(margin_pct),
            "stock_qty": pa.array(stock_qty),
            "reorder_level": pa.array(reorder_level),
            "lead_time_days": pa.array(lead_time_days),
            "rating": pa.array(rating),
            "review_count": pa.array(review_count),
            "description": pa.array(
                [
                    f"Product {p} — high quality {DEFAULT_COLORS[ci].lower()} unit from {DEFAULT_SUPPLIERS[si]}"
                    for p, ci, si in zip(product_names, desc_color_idx, desc_supplier_idx)
                ],
                type=pa.string(),
            ),
        }
    )


def get_shared_catalog_db_path() -> str:
    """Return the path for the shared SQLite Iceberg catalog DB.

    Uses ``SHARED_STORAGE_PATH`` env-var when available (set by the Helm
    chart when the VAST-CSI PVC is mounted).  Falls back to the default
    ``/shared`` so notebooks still work in environments without the env-var.
    """
    base: str = os.environ.get("SHARED_STORAGE_PATH", SHARED_STORAGE_PATH)
    return os.path.join(base, SHARED_ICEBERG_CATALOG_DB)


def make_shared_iceberg_catalog(*, name: str = "iceberg") -> SqlCatalog:
    """Create a PyIceberg ``SqlCatalog`` backed by a shared SQLite DB.

    The catalog stores its metadata on the VAST-CSI shared volume so that
    tables registered in one notebook are visible from any other notebook
    running in the same pod.
    """
    access_key, secret_key = get_s3_credentials()
    endpoint: str = os.environ.get("VASTDB_ENDPOINT", "http://vippool.ie-dev-pipeline.svc.cluster.local")
    bucket: str = os.environ.get("VASTDB_BUCKET", "collections-bucket")
    db_path: str = get_shared_catalog_db_path()
    warehouse: str = f"s3://{bucket}/iceberg-warehouse"

    return SqlCatalog(
        name,
        **{
            "uri": f"sqlite:///{db_path}",
            "warehouse": warehouse,
            "s3.endpoint": endpoint,
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
            "s3.region": "us-east-1",
            "s3.no-sign-request": "false",
            "s3.client.verify": "false",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3fs.client_kwargs": '{"verify": false}',
        },
    )
