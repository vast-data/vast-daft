"""Unit tests for VastDB scan behavior."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa
from daft.io.pushdowns import Pushdowns

from vast_daft.config import VastDBConfig
from vast_daft.scan import VastDBScanOperator
from vast_daft.source import (
    _ACTIVE_READ_TRANSACTIONS,
    VastDBDataSourceTask,
    _begin_shared_read_txid,
    _close_shared_read_transaction,
)


def _make_scan_operator() -> VastDBScanOperator:
    config = VastDBConfig(
        endpoint="http://vastdb.example.com",
        access_key="access",
        secret_key="secret",
        bucket="bucket",
        schema="schema",
        ssl_verify=False,
    )
    table_schema = pa.schema(
        [
            ("pk", pa.string()),
            ("source", pa.string()),
            ("vector", pa.list_(pa.float32(), 2048)),
        ]
    )
    return VastDBScanOperator(
        config=config,
        table_name="chunks",
        table_schema=table_schema,
        bucket="bucket",
        db_schema="schema",
    )


def test_scan_shape_skips_stats_for_unfiltered_limit() -> None:
    op = _make_scan_operator()

    with (
        patch.object(VastDBScanOperator, "_fetch_row_count", side_effect=AssertionError("should not fetch stats")),
        patch("vast_daft.scan._resolve_num_splits", side_effect=AssertionError("should not resolve splits")),
    ):
        assert op._scan_shape(limit=1, predicate=None) == (1, None)
        assert op._scan_shape(limit=10, predicate=None) == (1, None)


def test_scan_shape_fetches_stats_for_filtered_or_unbounded_scan() -> None:
    op = _make_scan_operator()

    with (
        patch.object(VastDBScanOperator, "_fetch_row_count", return_value=1_000),
        patch("vast_daft.scan._resolve_num_splits", return_value=4),
    ):
        assert op._scan_shape(limit=None, predicate=None) == (4, 1_000)
        assert op._scan_shape(limit=1, predicate=object()) == (4, 1_000)


def test_create_split_tasks_shares_one_txid_across_all_tasks() -> None:
    op = _make_scan_operator()
    fake_pushdowns = SimpleNamespace(filters=None, limit=None)

    with (
        patch.object(VastDBScanOperator, "_scan_shape", return_value=(3, 900)),
        patch("vast_daft.scan._begin_shared_read_txid", return_value=4242),
        patch("daft.io.pushdowns.Pushdowns._from_pypushdowns", return_value=Pushdowns()),
        patch.object(
            __import__("vast_daft.scan", fromlist=["ScanTask"]).ScanTask,
            "python_factory_func_scan_task",
            side_effect=lambda **kwargs: kwargs["func_args"][0],
        ),
    ):
        tasks = list(op._create_split_tasks(cast(Any, fake_pushdowns)))

    assert [cast(Any, task)._txid for task in tasks] == [4242, 4242, 4242]
    assert [cast(Any, task)._split_index for task in tasks] == [0, 1, 2]


def test_create_split_tasks_applies_column_pushdown() -> None:
    op = _make_scan_operator()
    fake_pushdowns = SimpleNamespace(filters=None, limit=None)

    captured_schemas = []
    with (
        patch.object(VastDBScanOperator, "_scan_shape", return_value=(1, 100)),
        patch("vast_daft.scan._begin_shared_read_txid", return_value=4242),
        patch("daft.io.pushdowns.Pushdowns._from_pypushdowns", return_value=Pushdowns(columns=["pk", "source"])),
        patch.object(
            __import__("vast_daft.scan", fromlist=["ScanTask"]).ScanTask,
            "python_factory_func_scan_task",
            side_effect=lambda **kwargs: (captured_schemas.append(kwargs["schema"]) or kwargs["func_args"][0]),
        ),
    ):
        tasks = list(op._create_split_tasks(cast(Any, fake_pushdowns)))

    assert op.can_absorb_select()
    assert cast(Any, tasks[0])._columns == ["pk", "source"]
    assert cast(Any, VastDBDataSourceTask.schema).fget(tasks[0]).to_pyarrow_schema().names == ["pk", "source"]


def test_filtered_count_uses_split_scan_instead_of_unfiltered_metadata_count() -> None:
    op = _make_scan_operator()
    fake_pushdowns = SimpleNamespace(
        aggregation=object(),
        filters=object(),
        limit=None,
        aggregation_count_mode=lambda: op.supported_count_modes()[0],
        aggregation_required_column_names=lambda: ["count"],
    )

    with (
        patch.object(op, "_create_count_task", side_effect=AssertionError("filtered count must not use table stats")),
        patch.object(op, "_create_split_tasks", return_value=iter(["split"])),
    ):
        assert list(op.to_scan_tasks(cast(Any, fake_pushdowns))) == ["split"]


def test_unfiltered_count_uses_metadata_count_task() -> None:
    op = _make_scan_operator()
    fake_pushdowns = SimpleNamespace(
        aggregation=object(),
        filters=None,
        limit=None,
        aggregation_count_mode=lambda: op.supported_count_modes()[0],
        aggregation_required_column_names=lambda: ["count"],
    )

    with (
        patch.object(op, "_create_count_task", return_value=iter(["count"])),
        patch.object(op, "_create_split_tasks", side_effect=AssertionError("unfiltered count should use table stats")),
    ):
        assert list(op.to_scan_tasks(cast(Any, fake_pushdowns))) == ["count"]


def test_shared_read_transaction_owner_is_retained_and_can_be_closed() -> None:
    tx = MagicMock()
    tx.txid = 9191
    tx.is_active = True
    tx.__enter__.return_value = tx
    connection = SimpleNamespace(session=SimpleNamespace(transaction=Mock(return_value=tx)))

    with patch("vast_daft.source.VastDBConnection", return_value=connection):
        txid = _begin_shared_read_txid(_make_scan_operator()._config)

    try:
        assert txid == 9191
        assert _ACTIVE_READ_TRANSACTIONS[txid][1] is tx
        _close_shared_read_transaction(txid)
        tx.__exit__.assert_called_once_with(None, None, None)
        assert txid not in _ACTIVE_READ_TRANSACTIONS
    finally:
        _ACTIVE_READ_TRANSACTIONS.pop(txid, None)


def test_get_micro_partitions_uses_shared_txid_without_opening_new_transaction() -> None:
    task = VastDBDataSourceTask(
        vastdb_config=VastDBConfig(
            endpoint="http://vastdb.example.com",
            access_key="access",
            secret_key="secret",
            bucket="bucket",
            schema="schema",
            ssl_verify=False,
        ),
        table_name="chunks",
        table_schema=pa.schema([("pk", pa.int64())]),
        bucket="bucket",
        schema="schema",
        split_index=0,
        total_splits=2,
        txid=9191,
    )

    batch = pa.record_batch([pa.array([1, 2, 3])], names=["pk"])
    reader0 = MagicMock()
    reader0.__iter__ = Mock(return_value=iter([batch]))
    reader1 = MagicMock()
    reader1.__iter__ = Mock(return_value=iter([]))
    fake_table = Mock()
    fake_table.select_splits.return_value = [reader0, reader1]
    fake_connection = SimpleNamespace(
        session=SimpleNamespace(
            transaction=Mock(side_effect=AssertionError("task should not open a new transaction"))
        )
    )

    with (
        patch("vast_daft.source.VastDBConnection", return_value=fake_connection),
        patch("vast_daft.source._table_from_existing_txid", return_value=fake_table) as table_from_txid,
    ):
        parts = list(task.get_micro_partitions())

    table_from_txid.assert_called_once()
    assert table_from_txid.call_args.kwargs["txid"] == 9191
    assert len(parts) == 1
    reader1.close.assert_called_once()


def test_stale_read_transactions_are_expired() -> None:
    """A long-lived driver must not pin one VastDB snapshot per query forever."""
    from vast_daft import source as src

    def _tx(txid: int) -> MagicMock:
        tx = MagicMock()
        tx.txid = txid
        tx.is_active = True
        tx.__enter__.return_value = tx
        return tx

    old, new = _tx(1), _tx(2)
    config = _make_scan_operator()._config
    src._ACTIVE_READ_TRANSACTIONS.clear()
    try:
        with patch.object(src, "_READ_TXN_TTL_S", 0.0):
            for tx in (old, new):
                connection = SimpleNamespace(session=SimpleNamespace(transaction=Mock(return_value=tx)))
                with patch("vast_daft.source.VastDBConnection", return_value=connection):
                    _begin_shared_read_txid(config)
        # Opening the second snapshot expires the first, which is already past TTL.
        old.__exit__.assert_called_once_with(None, None, None)
        assert 1 not in src._ACTIVE_READ_TRANSACTIONS
        assert 2 in src._ACTIVE_READ_TRANSACTIONS
    finally:
        src._ACTIVE_READ_TRANSACTIONS.clear()
