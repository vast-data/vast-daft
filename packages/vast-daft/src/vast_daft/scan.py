"""ScanOperator implementation for VastDB.

Implements the ``ScanOperator`` interface directly (instead of going
through ``DataSource`` → ``_DataSourceShim``).  This gives access to
optimisation hooks that the shim does not expose, notably:

* **count pushdown** — ``df.count()`` returns ``table.stats.num_rows``
  without scanning any data.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pyarrow as pa
from daft.daft import (
    CountMode,
    PyPartitionField,
    PyPushdowns,
    PyRecordBatch,
    ScanTask,
)
from daft.io.scan import ScanOperator
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

from vast_daft._pushdown import pushdowns_to_predicate_and_columns
from vast_daft.config import VastDBConfig
from vast_daft.connection import VastDBConnection
from vast_daft.source import (
    VastDBDataSourceTask,
    _begin_shared_read_txid,
    _project_pyarrow_schema,
    _reorder_columns,
    _resolve_num_splits,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from vastdb.config import QueryConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Count-result factory (must be a module-level function for pickling)
# ---------------------------------------------------------------------------


def _vastdb_count_result(total_count: int, field_name: str) -> Iterator[PyRecordBatch]:
    """Produce a single-row count result without reading table data."""
    arrow_schema = pa.schema([pa.field(field_name, pa.uint64())])
    arrow_batch = pa.RecordBatch.from_arrays(
        [pa.array([total_count], type=pa.uint64())],
        [field_name],
    )
    yield RecordBatch.from_arrow_record_batches([arrow_batch], arrow_schema)._recordbatch


# ---------------------------------------------------------------------------
# Split-read factory (must be a module-level function for pickling)
# ---------------------------------------------------------------------------


def _read_vastdb_split(task: VastDBDataSourceTask) -> Iterator[PyRecordBatch]:
    """Execute a VastDBDataSourceTask and yield PyRecordBatch objects."""
    for mp in task.get_micro_partitions():
        yield from (rb._recordbatch for rb in mp.get_record_batches())


# ---------------------------------------------------------------------------
# ScanOperator
# ---------------------------------------------------------------------------


class VastDBScanOperator(ScanOperator):
    """Daft :class:`ScanOperator` for VastDB tables.

    Provides the same split-based parallel read as :class:`VastDBDataSource`
    but exposes optimisation hooks that the ``DataSource`` API lacks:

    * ``supports_count_pushdown()`` → ``True``: ``df.count()`` returns
      ``table.stats.num_rows`` instantly.
    * ``can_absorb_filter()`` → ``True``: filter predicates are translated
      to VastDB server-side predicates.
    * ``can_absorb_limit()`` → ``True``: row limits are pushed to VastDB.
    """

    def __init__(
        self,
        config: VastDBConfig,
        table_name: str,
        table_schema: pa.Schema,
        *,
        bucket: str,
        db_schema: str,
        columns: list[str] | None = None,
        query_config: QueryConfig | None = None,
        num_splits: int | None = None,
    ) -> None:
        super().__init__()
        self._config = config
        self._table_name = table_name
        self._table_schema = table_schema
        self._bucket = bucket
        self._db_schema = db_schema
        self._columns = columns
        self._query_config = query_config
        self._schema = Schema.from_pyarrow_schema(table_schema)
        self._explicit_num_splits = num_splits
        self._shared_txid: int | None = None

    # -- ScanOperator abstract interface -------------------------------------

    def schema(self) -> Schema:
        return self._schema

    def name(self) -> str:
        return "VastDBScanOperator"

    def display_name(self) -> str:
        return f"VastDBScan({self._bucket}/{self._db_schema}/{self._table_name})"

    def partitioning_keys(self) -> list[PyPartitionField]:
        return []

    def can_absorb_filter(self) -> bool:
        return True

    def can_absorb_limit(self) -> bool:
        return True

    def can_absorb_select(self) -> bool:
        return True

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
            "Splits = dynamic",
        ]

    def supports_count_pushdown(self) -> bool:
        return True

    def supported_count_modes(self) -> list[CountMode]:
        return [CountMode.All]

    # -- scan tasks ----------------------------------------------------------

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        # --- count pushdown ---
        if (
            pushdowns.aggregation is not None
            and pushdowns.aggregation_count_mode() is not None
            and pushdowns.aggregation_required_column_names()
        ):
            count_mode = pushdowns.aggregation_count_mode()
            if count_mode in self.supported_count_modes():
                field_name = pushdowns.aggregation_required_column_names()[0]
                yield from self._create_count_task(pushdowns, field_name)
                return

        # --- regular split-based scan ---
        yield from self._create_split_tasks(pushdowns)

    # -- internals -----------------------------------------------------------

    def _fetch_row_count(self) -> int:
        """Fetch the row count from VastDB table stats (cheap metadata RPC).

        Uses ``TableMetadata.load_stats()`` directly to avoid the bucket
        HEAD, schema listing, and table listing RPCs that the interactive
        path would perform.

        Retries up to 3 times with exponential backoff for transient
        failures.  Raises on persistent failure — callers should not
        silently degrade to unknown size estimates.
        """
        from tenacity import retry, stop_after_attempt, wait_exponential
        from vastdb.table_metadata import TableMetadata, TableRef

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.5, max=4),
            reraise=True,
        )
        def _fetch() -> int:
            connection = VastDBConnection(self._config)
            table_md = TableMetadata(
                TableRef(self._bucket, self._db_schema, self._table_name),
                arrow_schema=self._table_schema,
            )
            with connection.session.transaction() as tx:
                table_md.load_stats(tx)
                stats = table_md.stats
                return stats.num_rows if stats is not None else 0

        return _fetch()

    def _create_count_task(
        self,
        pushdowns: PyPushdowns,
        field_name: str,
    ) -> Iterator[ScanTask]:
        """Yield a single task that returns the row count from metadata."""
        try:
            total_count = self._fetch_row_count()
            result_schema = Schema.from_pyarrow_schema(pa.schema([pa.field(field_name, pa.uint64())]))
            logger.info(
                "VastDB count pushdown for %r: %d rows",
                self._table_name,
                total_count,
            )
            yield ScanTask.python_factory_func_scan_task(
                module=_vastdb_count_result.__module__,
                func_name=_vastdb_count_result.__name__,
                func_args=(total_count, field_name),
                schema=result_schema._schema,
                num_rows=1,
                size_bytes=8,
                pushdowns=pushdowns,
                stats=None,
                source_name=self.display_name(),
            )
        except Exception:
            logger.warning(
                "VastDB count pushdown failed for %r, falling back to full scan",
                self._table_name,
                exc_info=True,
            )
            yield from self._create_split_tasks(pushdowns)

    def _create_split_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        """Yield one ScanTask per VastDB split."""
        from daft.io.pushdowns import Pushdowns

        pds = Pushdowns._from_pypushdowns(pushdowns)

        predicate = None
        limit: int | None = None
        columns = self._columns
        predicate_columns: set[str] = set()

        if pds.filters is not None:
            predicate, predicate_columns = pushdowns_to_predicate_and_columns(pds)
            if predicate is not None:
                logger.debug(
                    "Applying filter pushdown to VastDB for %r: %s",
                    self._table_name,
                    repr(predicate),
                )

        if pds.columns:
            requested = list(pds.columns)
            # VastDB requires predicate-referenced columns in the projection,
            # otherwise select_splits raises FieldNotFound. Daft's planner will
            # prune the extras above the scan if they are not needed downstream.
            missing = [c for c in predicate_columns if c not in requested]
            if missing:
                logger.debug(
                    "Adding predicate-only columns to VastDB projection for %r: %s",
                    self._table_name,
                    missing,
                )
                requested = requested + missing
            columns = _reorder_columns(self._table_schema, requested)
            logger.debug(
                "Applying column pushdown to VastDB for %r: %s",
                self._table_name,
                columns,
            )
        task_schema = _project_pyarrow_schema(self._table_schema, columns)
        daft_task_schema = Schema.from_pyarrow_schema(task_schema)

        if pds.limit is not None:
            limit = pds.limit
            logger.debug(
                "Applying limit pushdown to VastDB for %r: %d",
                self._table_name,
                limit,
            )

        effective_num_splits, total_rows = self._scan_shape(
            limit=limit,
            predicate=predicate,
        )

        # Estimate per-split size so Daft's executor can parallelise.
        rows_per_split: int | None = None
        bytes_per_split: int | None = None
        if total_rows is not None and total_rows > 0:
            rows_per_split = max(1, total_rows // effective_num_splits)

            # Rough estimate: sum of fixed-size field widths + 64 bytes per string column.
            def _field_bytes(t: pa.DataType) -> int:
                try:
                    return t.bit_width // 8
                except ValueError:
                    return 64  # variable-width (string, binary, etc.)

            row_width = sum(_field_bytes(t) for t in task_schema.types)
            bytes_per_split = rows_per_split * max(row_width, 8)

        logger.info(
            "Creating %d split tasks for %r (est. %s rows/split, %s bytes/split)",
            effective_num_splits,
            self._table_name,
            f"{rows_per_split:,}" if rows_per_split else "unknown",
            f"{bytes_per_split:,}" if bytes_per_split else "unknown",
        )

        if self._shared_txid is None:
            self._shared_txid = _begin_shared_read_txid(self._config)

        for i in range(effective_num_splits):
            task = VastDBDataSourceTask(
                vastdb_config=self._config,
                table_name=self._table_name,
                table_schema=self._table_schema,
                bucket=self._bucket,
                schema=self._db_schema,
                split_index=i,
                total_splits=effective_num_splits,
                columns=columns,
                predicate=predicate,
                query_config=self._query_config,
                limit=limit,
                txid=self._shared_txid,
            )
            yield ScanTask.python_factory_func_scan_task(
                module=_read_vastdb_split.__module__,
                func_name=_read_vastdb_split.__name__,
                func_args=(task,),
                schema=daft_task_schema._schema,
                num_rows=rows_per_split,
                size_bytes=bytes_per_split,
                pushdowns=pushdowns,
                stats=None,
                source_name=self.display_name(),
            )

    def _scan_shape(
        self,
        *,
        limit: int | None,
        predicate: object | None,
    ) -> tuple[int, int | None]:
        """Resolve scan split count and optional row count lazily.

        Even with column pushdown, ``limit(1)`` queries only need one
        unordered split. Running that query across every split can create
        several readers in parallel and add avoidable Ray overhead before
        the global limit short-circuits.

        For unfiltered limits, returning any ``N`` rows is semantically
        acceptable because the scan is unordered. In that case, a single
        split is sufficient and materially reduces memory pressure. This
        path also skips the metadata RPCs for row count and split sizing.
        """
        if limit is not None and limit > 0 and predicate is None:
            return 1, None

        total_rows = self._fetch_row_count()
        num_splits = _resolve_num_splits(
            explicit=self._explicit_num_splits,
            query_config=self._query_config,
            config=self._config,
            table_name=self._table_name,
            table_schema=self._table_schema,
            bucket=self._bucket,
            schema=self._db_schema,
            row_count=total_rows,
        )
        return num_splits, total_rows
