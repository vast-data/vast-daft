"""vast-daft: Daft custom connector for VastDB.

Provides ``DataSource`` / ``DataSink`` implementations for reading from
and writing to VastDB, a Daft catalog integration for native SQL queries,
and a Gravitino-aware catalog that routes VastDB-format tables through
the native SDK.

Importing this module also registers two convenience methods on
``daft.DataFrame``:

* ``df.read_vastdb(config, table_name, ...)`` — read a VastDB table
* ``df.write_vastdb(config, table_name, schema, ...)`` — write to a VastDB table
"""

from __future__ import annotations

from typing import Any

import daft
import pyarrow as pa

from vast_daft.config import VastDBConfig
from vast_daft.connection import VastDBConnection
from vast_daft.scan import VastDBScanOperator
from vast_daft.sink import VastDBDataSink
from vast_daft.source import VastDBDataSource
from vast_daft.table import VastDBCatalog, VastDBTable

try:
    from vast_daft.gravitino import VastGravitinoCatalog, VastGravitinoTable
except ModuleNotFoundError:
    VastGravitinoCatalog = None
    VastGravitinoTable = None


# ---------------------------------------------------------------------------
# df.read_vastdb()
# ---------------------------------------------------------------------------

def _read_vastdb(
    config: VastDBConfig,
    table_name: str,
    *,
    bucket: str | None = None,
    schema: str | None = None,
    columns: list[str] | None = None,
    num_splits: int | None = None,
) -> daft.DataFrame:
    """Read a VastDB table into a Daft DataFrame.

    This is a module-level factory (not a DataFrame method) that mirrors the
    ergonomics of ``daft.read_parquet()``, ``daft.read_iceberg()``, etc.

    Parameters
    ----------
    config:
        VastDB connection configuration.
    table_name:
        Name of the VastDB table to read.
    bucket:
        VastDB bucket.  Overrides ``config.bucket`` when provided.
    schema:
        VastDB schema.  Overrides ``config.schema`` when provided.
    columns:
        Optional list of column names to project.
    num_splits:
        Number of parallel read splits.  Auto-estimated when omitted.

    Returns
    -------
    daft.DataFrame

    Examples
    --------
    >>> import vast_daft
    >>> from vast_daft import VastDBConfig
    >>> config = VastDBConfig(endpoint="http://vastdb:9090", access_key="...", secret_key="...")
    >>> df = vast_daft.read_vastdb(config, "orders")
    >>> df.show()
    """
    from daft.daft import ScanOperatorHandle
    from daft.logical.builder import LogicalPlanBuilder

    _bucket = bucket or config.bucket
    _schema = schema or config.schema

    # Discover the PyArrow schema from the live table
    conn = VastDBConnection(config)
    with conn.session.transaction() as tx:
        db_table = tx.bucket(_bucket).schema(_schema).table(table_name)
        pa_schema = db_table.columns()

    if columns is not None:
        pa_schema = pa.schema([pa_schema.field(c) for c in columns])

    kwargs: dict[str, Any] = {}
    if num_splits is not None:
        kwargs["num_splits"] = num_splits

    scan_op = VastDBScanOperator(
        config,
        table_name,
        pa_schema,
        bucket=_bucket,
        db_schema=_schema,
        columns=columns,
        **kwargs,
    )
    handle = ScanOperatorHandle.from_python_scan_operator(scan_op)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return daft.DataFrame(builder)


# ---------------------------------------------------------------------------
# df.write_vastdb()
# ---------------------------------------------------------------------------

def _write_vastdb(
    self: daft.DataFrame,
    config: VastDBConfig,
    table_name: str,
    table_schema: pa.Schema,
    *,
    bucket: str | None = None,
    schema: str | None = None,
    create_if_missing: bool = True,
) -> daft.DataFrame:
    """Write the DataFrame to a VastDB table.

    Registered on ``daft.DataFrame`` when ``vast_daft`` is imported.

    Parameters
    ----------
    config:
        VastDB connection configuration.
    table_name:
        Name of the target VastDB table.
    table_schema:
        PyArrow schema of the target table.
    bucket:
        VastDB bucket.  Overrides ``config.bucket`` when provided.
    schema:
        VastDB schema.  Overrides ``config.schema`` when provided.
    create_if_missing:
        Auto-create the table if it does not exist (default: ``True``).

    Returns
    -------
    daft.DataFrame
        A DataFrame with ``rows_written`` and ``bytes_written`` columns,
        one row per partition written.

    Examples
    --------
    >>> import daft, pyarrow as pa, vast_daft
    >>> from vast_daft import VastDBConfig
    >>> config = VastDBConfig(endpoint="http://vastdb:9090", access_key="...", secret_key="...")
    >>> schema = pa.schema([("id", pa.int64()), ("value", pa.float64())])
    >>> df = daft.from_pydict({"id": [1, 2], "value": [0.1, 0.2]})
    >>> df.write_vastdb(config, "my_table", schema)
    """
    sink = VastDBDataSink(
        config,
        table_name,
        table_schema,
        bucket=bucket,
        schema=schema,
        create_if_missing=create_if_missing,
    )
    return self.write_sink(sink)


# Register df.write_vastdb on daft.DataFrame
daft.DataFrame.write_vastdb = _write_vastdb  # type: ignore[attr-defined]


__all__ = [
    # Core
    "VastDBConfig",
    "VastDBConnection",
    # Daft connectors
    "VastDBDataSource",
    "VastDBDataSink",
    "VastDBScanOperator",
    # Catalog / Table (Daft interfaces — use with daft.attach_catalog + daft.sql)
    "VastDBCatalog",
    "VastDBTable",
    # Gravitino-aware catalog (routes format=vastdb to VastDBCatalog)
    "VastGravitinoCatalog",
    "VastGravitinoTable",
    # Convenience top-level functions (mirrors daft.read_parquet etc.)
    "read_vastdb",
]

# Public alias — lets users call vast_daft.read_vastdb(...)
read_vastdb = _read_vastdb
