"""Shared helpers for Marimo example notebooks."""

from __future__ import annotations

import os
import random

import daft
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

SHARED_STORAGE_PATH: str = "/shared"
SHARED_ICEBERG_CATALOG_DB: str = "iceberg_catalog.db"


def configure_daft_runner(*, allow_local_fallback: bool = True) -> str:
    """Configure Daft for marimo and connect to Ray when available."""
    os.environ["RAY_TQDM_DISABLE"] = "1"
    os.environ["RAY_LOG_TO_DRIVER"] = "0"
    os.environ["PYTHONWARNINGS"] = "ignore::DeprecationWarning"

    try:
        daft.set_runner_ray()
        return f"Connected to Ray (RAY_ADDRESS={os.environ.get('RAY_ADDRESS', 'not set')})"
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
) -> dict[str, list[int] | list[str]]:
    """Generate deterministic customer rows for notebook demos."""
    rng = random.Random(seed)
    tier_values = tiers or DEFAULT_TIERS

    customers: dict[str, list[int] | list[str]] = {
        "customer_id": list(range(1, count + 1)),
        "name": [f"customer_{i}" for i in range(1, count + 1)],
    }
    if include_email:
        customers["email"] = [f"user_{i}@example.com" for i in range(1, count + 1)]
    customers["tier"] = [rng.choice(tier_values) for _ in range(count)]
    return customers


def generate_orders(
    count: int,
    customer_count: int,
    *,
    seed: int = 123,
    products: list[str] | None = None,
    start_id: int = 1001,
) -> dict[str, list[int] | list[float] | list[str]]:
    """Generate deterministic order rows for notebook demos."""
    rng = random.Random(seed)
    product_values = products or DEFAULT_PRODUCTS

    return {
        "order_id": list(range(start_id, start_id + count)),
        "customer_id": [rng.randint(1, customer_count) for _ in range(count)],
        "product": [rng.choice(product_values) for _ in range(count)],
        "amount": [round(rng.uniform(5.0, 500.0), 2) for _ in range(count)],
        "order_date": [f"2025-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}" for _ in range(count)],
    }


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
) -> dict[str, list[int] | list[float] | list[str]]:
    """Generate a wide product-catalog table with many columns.

    When *unique_names* is ``True`` each row gets a distinct product name
    (e.g. ``Widget A-001``).  The generated names still start with a base
    product so that ``generate_orders`` output can reference the same names
    when *products* is passed explicitly.

    Returns a dict suitable for ``daft.from_pydict()`` with columns:
    product, sku, category, sub_category, supplier, warehouse,
    color, weight_kg, cost_price, retail_price, margin_pct,
    stock_qty, reorder_level, lead_time_days, rating, review_count,
    description
    """
    rng = random.Random(seed)
    base_products = products or DEFAULT_PRODUCTS
    cat_map = categories or DEFAULT_CATEGORIES
    cat_list = list(set(cat_map.values()))

    if unique_names:
        product_names = [f"{base_products[i % len(base_products)]}-{i + 1:04d}" for i in range(count)]
    else:
        product_names = [rng.choice(base_products) for _ in range(count)]

    return {
        "product": product_names,
        "sku": [f"SKU-{rng.randint(100000, 999999)}" for _ in range(count)],
        "category": [cat_map.get(p.split("-")[0], rng.choice(cat_list)) for p in product_names],
        "sub_category": [f"sub_{rng.randint(1, 20):02d}" for _ in range(count)],
        "supplier": [rng.choice(DEFAULT_SUPPLIERS) for _ in range(count)],
        "warehouse": [rng.choice(DEFAULT_WAREHOUSES) for _ in range(count)],
        "color": [rng.choice(DEFAULT_COLORS) for _ in range(count)],
        "weight_kg": [round(rng.uniform(0.05, 25.0), 2) for _ in range(count)],
        "cost_price": [round(rng.uniform(1.0, 200.0), 2) for _ in range(count)],
        "retail_price": [round(rng.uniform(5.0, 500.0), 2) for _ in range(count)],
        "margin_pct": [round(rng.uniform(0.05, 0.65), 4) for _ in range(count)],
        "stock_qty": [rng.randint(0, 10000) for _ in range(count)],
        "reorder_level": [rng.randint(10, 500) for _ in range(count)],
        "lead_time_days": [rng.randint(1, 90) for _ in range(count)],
        "rating": [round(rng.uniform(1.0, 5.0), 1) for _ in range(count)],
        "review_count": [rng.randint(0, 5000) for _ in range(count)],
        "description": [
            f"Product {p} — high quality {rng.choice(DEFAULT_COLORS).lower()} unit from {rng.choice(DEFAULT_SUPPLIERS)}"
            for p in product_names
        ],
    }


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
