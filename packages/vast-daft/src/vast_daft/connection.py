"""VastDB connection manager."""

from __future__ import annotations

import logging
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
