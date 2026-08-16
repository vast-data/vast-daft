"""VastDB connection manager."""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import vastdb
from vastdb.table_metadata import TableMetadata, TableRef

from vast_daft.config import VastDBConfig

if TYPE_CHECKING:
    from vastdb.table import Table

logger = logging.getLogger(__name__)

# Query planning re-resolves every table reference, and each resolution costs a
# columns() (~140ms) plus load_stats() (~55ms) round trip. Those dominate latency
# for multi-table queries, so memoise them briefly. Set to 0 to disable.
METADATA_TTL_S = float(os.environ.get("VAST_DAFT_METADATA_TTL", "60"))

_meta_lock = threading.Lock()
_schema_cache: dict[tuple[Any, ...], tuple[float, pa.Schema]] = {}
_stats_cache: dict[tuple[Any, ...], tuple[float, int]] = {}
_exists_cache: dict[tuple[Any, ...], tuple[float, bool]] = {}


def _meta_key(config: VastDBConfig, bucket: str, schema: str, table: str) -> tuple[Any, ...]:
    return (config.endpoint, config.access_key, bucket, schema, table)


def _cached(store: dict[tuple[Any, ...], tuple[float, Any]], key: tuple[Any, ...]) -> Any | None:
    if METADATA_TTL_S <= 0:
        return None
    with _meta_lock:
        hit = store.get(key)
    if hit is None or time.monotonic() - hit[0] > METADATA_TTL_S:
        return None
    return hit[1]


def _store(store: dict[tuple[Any, ...], tuple[float, Any]], key: tuple[Any, ...], value: Any) -> Any:
    if METADATA_TTL_S > 0:
        with _meta_lock:
            store[key] = (time.monotonic(), value)
    return value


def clear_metadata_cache() -> None:
    """Drop cached schemas/stats — call after DDL that this process did not make."""
    with _meta_lock:
        _schema_cache.clear()
        _stats_cache.clear()
        _exists_cache.clear()


def cached_exists(key_parts: tuple[Any, ...], loader: Any) -> bool:
    """Memoise a table-existence check for METADATA_TTL_S.

    The SDK resolves ``schema.table(name)`` by listing every table in the
    schema, and Daft's SQL resolver probes existence several times per table
    reference, so this is the single hottest planning RPC.

    Only positives are cached: a miss may be a table about to be created.
    """
    hit = _cached(_exists_cache, key_parts)
    if hit is not None:
        return True
    found = bool(loader())
    if found:
        _store(_exists_cache, key_parts, True)
    return found


def cached_table_schema(key_parts: tuple[Any, ...], loader: Any) -> pa.Schema:
    """Memoise a table's Arrow schema for METADATA_TTL_S.

    ``loader`` is only called on a miss, so callers keep whatever RPC path they
    already use (interactive or TableMetadata).
    """
    hit = _cached(_schema_cache, key_parts)
    if hit is not None:
        return hit
    return _store(_schema_cache, key_parts, loader())


def cached_columns(config: VastDBConfig, bucket: str, schema: str, table: str) -> pa.Schema:
    """Arrow schema for a table, memoised for METADATA_TTL_S."""

    def _load() -> pa.Schema:
        connection = VastDBConnection(config)
        with connection.session.transaction() as tx:
            return tx.bucket(bucket).schema(schema).table(table).columns()

    return cached_table_schema(_meta_key(config, bucket, schema, table), _load)


def cached_row_count(config: VastDBConfig, bucket: str, schema: str, table: str, table_schema: pa.Schema) -> int:
    """Row count from table stats, memoised for METADATA_TTL_S."""
    key = _meta_key(config, bucket, schema, table)
    hit = _cached(_stats_cache, key)
    if hit is not None:
        return hit
    connection = VastDBConnection(config)
    table_md = TableMetadata(TableRef(bucket, schema, table), arrow_schema=table_schema)
    with connection.session.transaction() as tx:
        table_md.load_stats(tx)
        stats = table_md.stats
        rows = stats.num_rows if stats is not None else 0
    return _store(_stats_cache, key, rows)


class VastDBConnection:
    """Manages connections to VastDB via the Python SDK.

    Parameters
    ----------
    config : VastDBConfig
        Connection configuration.
    """

    def __init__(self, config: VastDBConfig) -> None:
        self._config = config
        self._session = vastdb.connect(
            endpoint=config.endpoint,
            access=config.access_key,
            secret=config.secret_key,
            ssl_verify=config.ssl_verify,
        )

    @property
    def config(self) -> VastDBConfig:
        return self._config

    @property
    def session(self) -> vastdb.session.Session:
        return self._session

    # ------------------------------------------------------------------
    # SDK helpers
    # ------------------------------------------------------------------
    @contextmanager
    def get_table(
        self,
        table_name: str,
        table_schema: pa.Schema,
        *,
        bucket: str,
        schema: str,
        create_if_missing: bool = True,
    ) -> Generator[Table, None, None]:
        """Yield a VastDB :class:`Table` inside a transaction.

        Parameters
        ----------
        table_name : str
            Name of the table (leaf name only).
        table_schema : pa.Schema
            PyArrow schema used when auto-creating the table.
        bucket : str
            VastDB bucket name.
        schema : str
            VastDB schema name within the bucket.
        create_if_missing : bool
            If ``True`` (default), the schema and table are created
            automatically when they don't exist yet.
        """
        with self._session.transaction() as tx:
            db_bucket = tx.bucket(bucket)
            db_schema = db_bucket.schema(schema, fail_if_missing=False)
            if db_schema is None:
                if not create_if_missing:
                    raise ValueError(f"Schema {schema!r} does not exist")
                db_schema = db_bucket.create_schema(schema, fail_if_exists=False)
                logger.info("Created schema %r", schema)

            table = db_schema.table(table_name, fail_if_missing=False)
            if table is None:
                if not create_if_missing:
                    raise ValueError(f"Table {table_name!r} does not exist in schema {schema!r}")
                table = db_schema.create_table(
                    table_name,
                    columns=table_schema,
                    fail_if_exists=False,
                )
                logger.info("Created table %r", table_name)

            yield table

    @contextmanager
    def get_table_from_metadata(
        self,
        table_name: str,
        table_schema: pa.Schema,
        *,
        bucket: str,
        schema: str,
    ) -> Generator[Any, None, None]:
        """Yield a VastDB table using the non-interactive ``TableMetadata`` path.

        This skips the bucket HEAD, schema listing, and table listing RPCs
        that the interactive ``get_table()`` path performs.  Use this when
        the table is **known to exist** and the Arrow schema is already
        available (read paths, insert paths after initial ``start()``).

        The returned table object is valid only within the yielded
        transaction context.
        """
        table_md = TableMetadata(
            TableRef(bucket, schema, table_name),
            arrow_schema=table_schema,
        )
        with self._session.transaction() as tx:
            # load_stats sets table_type which is required by insert()
            # (checks _is_sorted_table) and other operations.
            table_md.load_stats(tx)
            try:
                yield tx.table_from_metadata(table_md)  # type: ignore[misc]
            except GeneratorExit:
                # Daft may stop consuming partitions early (e.g. limit,
                # broadcast join).  The transaction is read-only so a
                # silent rollback is fine — no need to log a traceback.
                pass
