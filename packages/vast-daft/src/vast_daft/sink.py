"""Daft DataSink for writing to VastDB."""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pyarrow as pa
from daft.datatype import DataType
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

from vast_daft.config import VastDBConfig
from vast_daft.connection import VastDBConnection

logger = logging.getLogger(__name__)


class VastDBDataSink(DataSink[dict]):
    """Daft DataSink for writing data to VastDB tables.

    Parameters
    ----------
    config : VastDBConfig
        VastDB connection configuration.
    table_name : str
        Name of the table to write to.
    table_schema : pa.Schema
        PyArrow schema of the target table.
    bucket : str | None
        VastDB bucket name.  Overrides ``config.bucket`` when provided.
    schema : str | None
        VastDB schema name.  Overrides ``config.schema`` when provided.
    create_if_missing : bool
        Whether to auto-create the table/schema if they don't exist.

    Examples
    --------
    >>> import daft, pyarrow as pa
    >>> from vast_daft import VastDBConfig, VastDBDataSink
    >>>
    >>> config = VastDBConfig(...)
    >>> schema = pa.schema([("id", pa.string()), ("value", pa.float64())])
    >>> sink = VastDBDataSink(config, "my_table", schema)
    >>> daft.from_pydict({"id": ["a"], "value": [1.0]}).write_sink(sink).show()
    """

    def __init__(
        self,
        config: VastDBConfig,
        table_name: str,
        table_schema: pa.Schema,
        *,
        bucket: str | None = None,
        schema: str | None = None,
        create_if_missing: bool = True,
    ) -> None:
        self._config = config
        self._connection = VastDBConnection(config)
        self._table_name = table_name
        self._table_schema = table_schema
        _bucket = bucket or config.bucket
        _schema = schema or config.schema
        if not _bucket or not _schema:
            raise ValueError(
                "bucket and schema must be provided either via VastDBConfig or as explicit arguments to VastDBDataSink."
            )
        self._bucket: str = _bucket
        self._schema: str = _schema
        self._create_if_missing = create_if_missing

        self._result_schema = Schema._from_field_name_and_types(
            [
                ("rows_written", DataType.int64()),
                ("bytes_written", DataType.int64()),
            ]
        )

    def name(self) -> str:
        return f"VastDB({self._bucket}/{self._schema}/{self._table_name})"

    def schema(self) -> Schema:
        return self._result_schema

    def start(self) -> None:
        """Called once before any writes. Ensures the table exists."""
        if self._create_if_missing:
            with self._connection.get_table(
                self._table_name,
                self._table_schema,
                bucket=self._bucket,
                schema=self._schema,
                create_if_missing=True,
            ) as _table:
                logger.info(
                    "VastDB sink ready: %s/%s/%s",
                    self._bucket,
                    self._schema,
                    self._table_name,
                )

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[dict]]:
        """Write micro-partitions to VastDB.

        Each micro-partition is converted to a PyArrow table and inserted
        via the VastDB SDK within a transaction.

        Uses the non-interactive ``TableMetadata`` path to avoid the
        bucket HEAD, schema listing, and table listing RPCs on every
        write (3 round-trips saved per micro-partition).
        """
        for mp in micropartitions:
            arrow_table = mp.to_arrow()

            # Ensure schema alignment
            if arrow_table.schema != self._table_schema:
                arrow_table = arrow_table.cast(self._table_schema)

            num_rows = arrow_table.num_rows
            nbytes = arrow_table.nbytes

            with self._connection.get_table_from_metadata(
                self._table_name,
                self._table_schema,
                bucket=self._bucket,
                schema=self._schema,
            ) as table:
                table.insert(arrow_table)

            logger.debug(
                "Wrote %d rows (%d bytes) to VastDB table %r",
                num_rows,
                nbytes,
                self._table_name,
            )

            yield WriteResult(
                result={"rows_written": num_rows, "bytes_written": nbytes},
                bytes_written=nbytes,
                rows_written=num_rows,
            )

    def finalize(self, write_results: list[WriteResult[dict]]) -> MicroPartition:
        """Aggregate write results into a summary MicroPartition."""
        if not write_results:
            return MicroPartition.empty(self._result_schema)

        total_rows = sum(wr.rows_written for wr in write_results)
        total_bytes = sum(wr.bytes_written for wr in write_results)

        logger.info(
            "VastDB write complete: %d rows, %d bytes to %r",
            total_rows,
            total_bytes,
            self._table_name,
        )

        return MicroPartition.from_pydict(
            {
                "rows_written": [total_rows],
                "bytes_written": [total_bytes],
            }
        )
