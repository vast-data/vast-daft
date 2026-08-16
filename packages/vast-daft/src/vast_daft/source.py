"""Daft DataSource and DataSourceTask for reading from VastDB.

Uses VastDB's ``select_splits()`` API to partition reads across multiple
Daft tasks. Each task independently connects to VastDB, requests all
splits, and streams only its assigned split. A scan-level transaction ID
can be shared across tasks so every split reads from the same snapshot.
"""

from __future__ import annotations

import atexit
import logging
import os
import threading
import time
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from daft.io import DataSource, DataSourceTask
from daft.recordbatch import MicroPartition
from daft.schema import Schema
from ibis.common.deferred import Deferred
from ibis.expr.types import Column as BooleanColumn
from vastdb.config import QueryConfig
from vastdb.transaction import Transaction

from vast_daft._pushdown import pushdowns_to_predicate_and_columns
from vast_daft.config import VastDBConfig
from vast_daft.connection import VastDBConnection

if TYPE_CHECKING:
    from vastdb.table import TableInTransaction

logger = logging.getLogger(__name__)

# Snapshot pinning did become a problem: daft-sql-ep is a long-lived driver, and
# holding one open read tx per scan operator per query leaked sockets and pinned
# VastDB snapshots indefinitely (measured: +39 fds over ~30 queries). There is no
# close-on-last-task hook — the splits are consumed in Ray workers — so bound the
# set by age and count instead. A snapshot outlives its query by at most the TTL.
_READ_TXN_TTL_S = float(os.environ.get("VAST_DAFT_READ_TXN_TTL", "300"))
_MAX_ACTIVE_READ_TXNS = int(os.environ.get("VAST_DAFT_MAX_READ_TXNS", "64"))

_ACTIVE_READ_TRANSACTIONS: dict[int, tuple[float, Transaction]] = {}
_ACTIVE_READ_TRANSACTIONS_LOCK = threading.Lock()


def _close_shared_read_transaction(txid: int) -> None:
    """Commit and release a driver-owned shared read transaction."""
    with _ACTIVE_READ_TRANSACTIONS_LOCK:
        entry = _ACTIVE_READ_TRANSACTIONS.pop(txid, None)
    tx = entry[1] if entry is not None else None
    if tx is not None and tx.is_active:
        tx.__exit__(None, None, None)


def _expire_shared_read_transactions() -> None:
    """Close read snapshots whose query finished long ago, oldest first."""
    now = time.monotonic()
    with _ACTIVE_READ_TRANSACTIONS_LOCK:
        by_age = sorted((started, txid) for txid, (started, _) in _ACTIVE_READ_TRANSACTIONS.items())
        stale = {txid for started, txid in by_age if now - started > _READ_TXN_TTL_S}
        overflow = len(by_age) - len(stale) - _MAX_ACTIVE_READ_TXNS
        if overflow > 0:
            live = [txid for _, txid in by_age if txid not in stale]
            stale.update(live[:overflow])
    for txid in stale:
        try:
            _close_shared_read_transaction(txid)
        except Exception:  # noqa: BLE001 — teardown must not fail a new query
            logger.warning("Failed to close stale VastDB read transaction %s", txid, exc_info=True)
    if stale:
        logger.info("Closed %d stale VastDB read snapshot(s)", len(stale))


def _close_all_shared_read_transactions() -> None:
    """Best-effort process teardown for shared read snapshots."""
    with _ACTIVE_READ_TRANSACTIONS_LOCK:
        txids = list(_ACTIVE_READ_TRANSACTIONS)
    for txid in txids:
        try:
            _close_shared_read_transaction(txid)
        except Exception:  # noqa: BLE001
            logger.warning("Failed to close shared VastDB read transaction %s", txid, exc_info=True)


atexit.register(_close_all_shared_read_transactions)

# Default number of splits when not specified by the caller.
_DEFAULT_NUM_SPLITS = 4

# Upper bound so we never create an unreasonable number of tasks.
_MAX_AUTO_SPLITS = 64

# Rows per split when the caller gives no QueryConfig. This is a parallelism knob,
# not a VastDB page size: at 4M a 3-7M row table produced a single scan task and
# never engaged Ray at all. Keep it well under typical table sizes.
_DEFAULT_ROWS_PER_SPLIT = int(os.environ.get("VAST_DAFT_ROWS_PER_SPLIT", "500000"))


def _project_pyarrow_schema(table_schema: pa.Schema, columns: list[str] | None) -> pa.Schema:
    """Return ``table_schema`` projected to ``columns``, preserving **table schema order**.

    Column ordering must follow the original table schema so that Daft's
    engine, which references columns by positional index, finds the
    expected field at each position.  VastDB returns data in the order
    of the ``columns`` list passed to ``select_splits()``, so callers
    must also reorder the column list before passing it to VastDB.
    """
    if not columns:
        return table_schema
    col_set = set(columns)
    return pa.schema([f for f in table_schema if f.name in col_set])


def _reorder_columns(table_schema: pa.Schema, columns: list[str] | None) -> list[str] | None:
    """Reorder ``columns`` to match the field order of ``table_schema``.

    VastDB returns batches in the order of the requested column list.
    Daft's engine references columns by positional index within the
    scan task schema.  Both must agree, so we normalise to table-schema
    order everywhere.
    """
    if not columns:
        return columns
    col_set = set(columns)
    return [f.name for f in table_schema if f.name in col_set]


def _detect_cluster_cpus() -> int | None:
    """Return the total CPU count visible to the Ray cluster, or ``None``.

    Returns ``None`` when Ray is not initialised or not available so the
    caller can fall back to a static default.
    """
    try:
        import ray

        if not ray.is_initialized():
            return None
        cpus = ray.cluster_resources().get("CPU", 0)
        return int(cpus) if cpus > 0 else None
    except Exception:  # noqa: BLE001
        return None


def _estimate_splits_from_stats(
    config: VastDBConfig,
    table_name: str,
    table_schema: pa.Schema,
    *,
    bucket: str,
    schema: str,
    rows_per_split: int,
) -> int | None:
    """Query VastDB table stats and estimate the split count.

    Uses the same formula VastDB uses internally:
    ``max(1, num_rows // rows_per_split)``.

    Returns ``None`` if stats cannot be fetched so the caller can fall
    back to other heuristics.

    Uses the non-interactive ``TableMetadata`` path to avoid the bucket
    HEAD, schema listing, and table listing RPCs.
    """
    try:
        from vastdb.table_metadata import TableMetadata, TableRef

        from vast_daft.connection import VastDBConnection

        connection = VastDBConnection(config)
        table_md = TableMetadata(
            TableRef(bucket, schema, table_name),
            arrow_schema=table_schema,
        )
        with connection.session.transaction() as tx:
            table_md.load_stats(tx)
            stats = table_md.stats
            if stats is None or stats.num_rows == 0:
                return 1
            return _split_count(stats.num_rows, rows_per_split)
    except Exception:  # noqa: BLE001
        logger.debug("Failed to fetch table stats for split estimation", exc_info=True)
        return None


def _split_count(row_count: int, rows_per_split: int) -> int:
    """Splits needed to cover ``row_count``.

    Rounds up: floor division silently collapses to a single task for any table
    smaller than ``rows_per_split``, which is exactly when the cliff hurts most.
    """
    if row_count <= 0 or rows_per_split <= 0:
        return 1
    return max(1, -(-row_count // rows_per_split))


def _estimate_splits_from_row_count(
    row_count: int,
    *,
    rows_per_split: int,
) -> int:
    """Estimate split count from a known row count."""
    return _split_count(row_count, rows_per_split)


def _resolve_num_splits(
    *,
    explicit: int | None,
    query_config: QueryConfig | None,
    config: VastDBConfig | None = None,
    table_name: str | None = None,
    table_schema: pa.Schema | None = None,
    bucket: str | None = None,
    schema: str | None = None,
    row_count: int | None = None,
) -> int:
    """Determine the effective number of read splits.

    Resolution order:

    1. *explicit* ``num_splits`` passed by the caller.
    2. ``query_config.num_splits`` if a ``QueryConfig`` was provided.
    3. Estimate from VastDB table stats using the same formula the server
       uses (``num_rows // rows_per_split``), capped at the Ray cluster's
       CPU count (no point having more splits than available cores) and at
       :data:`_MAX_AUTO_SPLITS`.
    4. Static fallback :data:`_DEFAULT_NUM_SPLITS` (``4``).
    """
    if explicit is not None:
        return explicit
    if query_config is not None and query_config.num_splits is not None:
        return query_config.num_splits

    rows_per_split = (query_config.rows_per_split if query_config else None) or _DEFAULT_ROWS_PER_SPLIT

    # Try to estimate from table stats (cheap metadata RPC).
    if row_count is not None:
        estimated = _estimate_splits_from_row_count(
            row_count,
            rows_per_split=rows_per_split,
        )
        cluster_cpus = _detect_cluster_cpus()
        cap = min(cluster_cpus or _MAX_AUTO_SPLITS, _MAX_AUTO_SPLITS)
        n = min(estimated, cap)
        logger.info(
            "Auto-split from known row count for %r: %d rows → %d estimated splits (capped at %d)",
            table_name,
            row_count,
            estimated,
            n,
        )
        return max(n, 1)

    if config is not None and table_name is not None and table_schema is not None and bucket and schema:
        estimated = _estimate_splits_from_stats(
            config,
            table_name,
            table_schema,
            bucket=bucket,
            schema=schema,
            rows_per_split=rows_per_split,
        )
        if estimated is not None:
            # Cap by cluster CPUs — no benefit from more splits than cores.
            cluster_cpus = _detect_cluster_cpus()
            cap = min(cluster_cpus or _MAX_AUTO_SPLITS, _MAX_AUTO_SPLITS)
            n = min(estimated, cap)
            logger.info(
                "Auto-split for %r: %d rows → %d estimated splits (capped at %d)",
                table_name,
                estimated * rows_per_split,
                estimated,
                n,
            )
            return max(n, 1)

    # No table info available — try cluster CPUs alone.
    cluster_cpus = _detect_cluster_cpus()
    if cluster_cpus is not None:
        n = min(cluster_cpus, _MAX_AUTO_SPLITS)
        logger.info("Auto-detected %d cluster CPUs → using %d splits", cluster_cpus, n)
        return max(n, 1)

    return _DEFAULT_NUM_SPLITS


class VastDBDataSourceTask(DataSourceTask):
    """A task that reads a single split of data from VastDB.

    Each task opens its own connection, reuses the scan's transaction ID,
    calls ``select_splits()`` with a fixed ``num_splits``, and streams
    only the split at ``split_index``. This keeps per-task memory bounded
    to one split's worth of data while preserving a consistent snapshot
    across all splits in the scan.
    """

    def __init__(
        self,
        vastdb_config: VastDBConfig,
        table_name: str,
        table_schema: pa.Schema,
        *,
        bucket: str,
        schema: str,
        split_index: int,
        total_splits: int,
        columns: list[str] | None = None,
        predicate: BooleanColumn | Deferred | None = None,
        query_config: QueryConfig | None = None,
        limit: int | None = None,
        txid: int | None = None,
    ) -> None:
        self._vastdb_config = vastdb_config
        self._table_name = table_name
        self._table_schema = table_schema
        self._bucket = bucket
        self._schema = schema
        self._split_index = split_index
        self._total_splits = total_splits
        self._columns = columns
        self._predicate = predicate
        self._query_config = query_config
        self._limit = limit
        self._txid = txid

    @property
    def schema(self) -> Schema:
        """Return the task schema, including any projected column set."""
        return Schema.from_pyarrow_schema(_project_pyarrow_schema(self._table_schema, self._columns))

    def get_micro_partitions(self) -> Iterator[MicroPartition]:
        """Stream data from one VastDB split, yielding MicroPartitions."""
        # Each task creates its own connection so it can run on any Ray worker.
        connection = VastDBConnection(self._vastdb_config)

        # Build a QueryConfig that requests exactly total_splits splits.
        base = self._query_config or QueryConfig()
        config = QueryConfig(
            num_splits=self._total_splits,
            num_sub_splits=base.num_sub_splits,
            limit_rows_per_sub_split=base.limit_rows_per_sub_split,
            data_endpoints=base.data_endpoints,
            num_row_groups_per_sub_split=base.num_row_groups_per_sub_split,
            rows_per_split=base.rows_per_split,
        )

        rows_emitted = 0
        table = _table_from_existing_txid(
            connection=connection,
            table_name=self._table_name,
            table_schema=self._table_schema,
            bucket=self._bucket,
            schema=self._schema,
            txid=self._txid,
        )
        if table is None:
            raise ValueError("Read tasks require a shared transaction ID")
        split_readers = table.select_splits(
            columns=self._columns,
            predicate=self._predicate,
            config=config,
            internal_row_id=False,
        )

        if self._split_index >= len(split_readers):
            logger.debug(
                "Split %d/%d: no reader returned (table may have fewer rows than splits)",
                self._split_index,
                self._total_splits,
            )
            return

        # Keep only our assigned reader; close the rest to free
        # server-side cursors and memory held by unused splits.
        reader = split_readers[self._split_index]
        for i, r in enumerate(split_readers):
            if i != self._split_index:
                try:
                    r.close()
                except Exception:
                    pass
        del split_readers

        for batch in reader:
            if batch.num_rows == 0:
                continue

            if self._limit is not None:
                remaining = self._limit - rows_emitted
                if remaining <= 0:
                    break
                if batch.num_rows > remaining:
                    batch = batch.slice(0, remaining)

            rows_emitted += batch.num_rows
            # RecordBatch → Table: MicroPartition.from_arrow expects pa.Table
            yield MicroPartition.from_arrow(pa.Table.from_batches([batch]))

        logger.debug(
            "Split %d/%d: read %d rows from table %r",
            self._split_index,
            self._total_splits,
            rows_emitted,
            self._table_name,
        )


def _table_from_existing_txid(
    *,
    connection: VastDBConnection,
    table_name: str,
    table_schema: pa.Schema,
    bucket: str,
    schema: str,
    txid: int | None,
) -> TableInTransaction | None:
    """Build a non-interactive VastDB table bound to an existing txid."""
    if txid is None:
        return None

    from vastdb.table_metadata import TableMetadata, TableRef

    table_md = TableMetadata(
        TableRef(bucket, schema, table_name),
        arrow_schema=table_schema,
    )
    tx = Transaction(connection.session, txid=txid)
    table_md.load_stats(tx)
    return cast("TableInTransaction", tx.table_from_metadata(table_md))


class VastDBDataSource(DataSource):
    """Daft DataSource for reading from VastDB tables.

    Uses VastDB's split-based parallelism: ``get_tasks()`` creates one
    :class:`VastDBDataSourceTask` per split, each of which independently
    connects and streams its assigned portion of the data.

    Parameters
    ----------
    config : VastDBConfig
        VastDB connection configuration.
    table_name : str
        Name of the table to read from.
    table_schema : pa.Schema
        PyArrow schema of the table.
    bucket : str
        VastDB bucket name.  Overrides ``config.bucket`` when provided.
    schema : str
        VastDB schema name.  Overrides ``config.schema`` when provided.
    columns : list[str] | None
        Optional list of columns to project (read only these columns).
    predicate : BooleanColumn | Deferred | None
        Optional ibis predicate for server-side filtering.
    query_config : QueryConfig | None
        Optional VastDB query configuration.  If ``num_splits`` is set,
        it controls how many parallel tasks are created.
    limit : int | None
        Optional limit on the number of rows returned.
    num_splits : int | None
        Number of parallel read splits.  Overrides ``query_config.num_splits``.
        When ``None`` (the default), auto-detects from the Ray cluster's total
        CPU count so reads scale with the cluster.  Falls back to ``4`` if Ray
        is not available.

    Examples
    --------
    >>> import pyarrow as pa
    >>> from vast_daft import VastDBConfig, VastDBDataSource
    >>>
    >>> config = VastDBConfig(
    ...     endpoint="http://vastdb:9090",
    ...     access_key="...",
    ...     secret_key="...",
    ...     bucket="my-bucket",
    ...     schema="my-schema",
    ... )
    >>> schema = pa.schema([("id", pa.string()), ("value", pa.float64())])
    >>> source = VastDBDataSource(config, "my_table", schema, num_splits=8)
    >>> df = source.read()
    >>> df.show()
    """

    def __init__(
        self,
        config: VastDBConfig,
        table_name: str,
        table_schema: pa.Schema,
        *,
        bucket: str | None = None,
        schema: str | None = None,
        query_config: QueryConfig | None = None,
        num_splits: int | None = None,
    ) -> None:
        self._config = config
        self._table_name = table_name
        self._table_schema = table_schema
        _bucket = bucket or config.bucket
        _schema = schema or config.schema
        if not _bucket or not _schema:
            raise ValueError(
                "bucket and schema must be provided either via VastDBConfig "
                "or as explicit arguments to VastDBDataSource."
            )
        self._bucket: str = _bucket
        self._schema: str = _schema
        self._query_config = query_config
        self._shared_txid = _begin_shared_read_txid(config)

        # Resolve the effective number of splits.
        self._num_splits = _resolve_num_splits(
            explicit=num_splits,
            query_config=query_config,
            config=config,
            table_name=table_name,
            table_schema=table_schema,
            bucket=self._bucket,
            schema=self._schema,
        )

    @property
    def name(self) -> str:
        return f"VastDB({self._bucket}/{self._schema}/{self._table_name})"

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._table_schema)

    async def get_tasks(self, pushdowns: object) -> AsyncIterator[VastDBDataSourceTask]:
        """Create one read task per VastDB split.

        Each task independently connects to VastDB and reads only its
        assigned split, allowing Daft / Ray to parallelise the read
        across workers.

        Daft may supply a :class:`~daft.io.pushdowns.Pushdowns` object with
        ``filters`` (a filter predicate), ``columns`` (column projection),
        and ``limit`` (a row limit).  These are merged with any predicate /
        columns / limit already set on this source, with the *explicit* values
        taking precedence over the pushed-down ones.
        """
        from daft.io.pushdowns import Pushdowns

        predicate = None
        columns: list[str] | None = None
        limit: int | None = None
        predicate_columns: set[str] = set()

        if isinstance(pushdowns, Pushdowns):
            # --- filter ---
            if pushdowns.filters is not None:
                predicate, predicate_columns = pushdowns_to_predicate_and_columns(pushdowns)
                if predicate is None:
                    logger.error(
                        "pushdowns.filters is not None but translation returned None: %s",
                        pushdowns.filters,
                    )
                logger.debug(
                    "Applying Daft filter push-down to VastDB for table %r: %s",
                    self._table_name,
                    repr(predicate),
                )

            # --- column projection ---
            if pushdowns.columns:
                requested = list(pushdowns.columns)
                # VastDB requires predicate-referenced columns in the projection.
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
                    "Applying Daft column push-down to VastDB for table %r: %s",
                    self._table_name,
                    columns,
                )

            # --- row limit ---
            if pushdowns.limit is not None:
                limit = pushdowns.limit
                logger.debug(
                    "Applying Daft limit push-down to VastDB for table %r: %d",
                    self._table_name,
                    limit,
                )

        logger.info(
            "Creating %d split tasks for table %r",
            self._num_splits,
            self._table_name,
        )
        for i in range(self._num_splits):
            yield VastDBDataSourceTask(
                vastdb_config=self._config,
                table_name=self._table_name,
                table_schema=self._table_schema,
                bucket=self._bucket,
                schema=self._schema,
                split_index=i,
                total_splits=self._num_splits,
                columns=columns,
                predicate=predicate,
                query_config=self._query_config,
                limit=limit,
                txid=self._shared_txid,
            )


def _begin_shared_read_txid(config: VastDBConfig) -> int:
    """Open and retain one read transaction for all split tasks.

    The owner transaction is retained in the driver process so its session is
    not discarded while workers consume the shared snapshot. Registered
    transactions are committed during process teardown, and callers may use
    :func:`_close_shared_read_transaction` for explicit lifecycle control.
    """
    _expire_shared_read_transactions()
    connection = VastDBConnection(config)
    tx = connection.session.transaction()
    tx.__enter__()
    if tx.txid is None:
        raise ValueError("Failed to start VastDB read transaction")
    txid = tx.txid
    with _ACTIVE_READ_TRANSACTIONS_LOCK:
        _ACTIVE_READ_TRANSACTIONS[txid] = (time.monotonic(), tx)
    return txid
