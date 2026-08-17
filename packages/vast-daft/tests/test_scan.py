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
    _bind_existing_txid,
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
        patch("vast_daft.scan.cached_table_type", return_value=2),
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
    assert [cast(Any, task)._table_type for task in tasks] == [2, 2, 2]


def test_create_split_tasks_applies_column_pushdown() -> None:
    op = _make_scan_operator()
    fake_pushdowns = SimpleNamespace(filters=None, limit=None)

    captured_schemas = []
    with (
        patch.object(VastDBScanOperator, "_scan_shape", return_value=(1, 100)),
        patch("vast_daft.scan._begin_shared_read_txid", return_value=4242),
        patch("vast_daft.scan.cached_table_type", return_value=2),
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
    assert table_from_txid.call_args.kwargs["table_type"] is None
    assert len(parts) == 1
    reader1.close.assert_called_once()


def test_get_micro_partitions_forwards_driver_table_type() -> None:
    task = VastDBDataSourceTask(
        vastdb_config=VastDBConfig(
            endpoint="http://vastdb.example.com",
            access_key="access",
            secret_key="secret",
            bucket="bucket",
            schema="schema",
            ssl_verify=False,
        ),
        table_name="postings",
        table_schema=pa.schema([("term", pa.string())]),
        bucket="bucket",
        schema="schema",
        split_index=0,
        total_splits=1,
        columns=["term"],
        txid=9191,
        table_type=2,
    )
    reader = MagicMock()
    reader.__iter__ = Mock(return_value=iter([]))
    fake_table = Mock()
    fake_table.select_splits.return_value = [reader]
    fake_connection = SimpleNamespace(session=SimpleNamespace())

    with (
        patch("vast_daft.source.VastDBConnection", return_value=fake_connection),
        patch("vast_daft.source._table_from_existing_txid", return_value=fake_table) as table_from_txid,
    ):
        list(task.get_micro_partitions())

    assert table_from_txid.call_args.kwargs["table_type"] == 2


def test_table_from_existing_txid_constructs_metadata_with_table_type() -> None:
    from vast_daft.source import _table_from_existing_txid

    captured: dict[str, Any] = {}
    table_md = SimpleNamespace(_table_type=2, stats=None, load_stats=Mock())
    fake_tx = SimpleNamespace(table_from_metadata=Mock(return_value="table"))
    fake_connection = SimpleNamespace(session=object())

    def fake_metadata(*_args: Any, **kwargs: Any) -> Any:
        captured.update(kwargs)
        return table_md

    with (
        patch("vastdb.table_metadata.TableMetadata", side_effect=fake_metadata),
        patch("vastdb.table_metadata.TableRef", side_effect=lambda *a: a),
        patch("vast_daft.source._bind_existing_txid", return_value=fake_tx),
        patch("vast_daft.source._coerce_table_type", return_value="Elysium"),
        patch("vast_daft.connection.ensure_table_metadata_loaded") as ensure_loaded,
    ):
        result = _table_from_existing_txid(
            connection=cast(Any, fake_connection),
            table_name="inverted_index_postings",
            table_schema=pa.schema([("term", pa.string())]),
            bucket="bucket",
            schema="schema",
            txid=7,
            table_type=2,
        )

    assert result == "table"
    assert captured["table_type"] == "Elysium"
    table_md.load_stats.assert_called_once_with(fake_tx)
    ensure_loaded.assert_not_called()


def test_table_from_existing_txid_loads_when_type_missing() -> None:
    from vast_daft.source import _table_from_existing_txid

    table_md = SimpleNamespace(_table_type=None, stats=None, load_stats=Mock())
    fake_tx = SimpleNamespace(table_from_metadata=Mock(return_value="table"))
    fake_connection = SimpleNamespace(session=object())

    with (
        patch("vastdb.table_metadata.TableMetadata", return_value=table_md),
        patch("vastdb.table_metadata.TableRef", side_effect=lambda *a: a),
        patch("vast_daft.source._bind_existing_txid", return_value=fake_tx),
        patch("vast_daft.connection.ensure_table_metadata_loaded") as ensure_loaded,
    ):
        _table_from_existing_txid(
            connection=cast(Any, fake_connection),
            table_name="inverted_index_postings",
            table_schema=pa.schema([("term", pa.string())]),
            bucket="bucket",
            schema="schema",
            txid=7,
            table_type=None,
        )

    ensure_loaded.assert_called_once_with(table_md, fake_tx)
    table_md.load_stats.assert_not_called()


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


def test_bind_existing_txid_passes_vastdb2_session_context() -> None:
    session = SimpleNamespace(_context="ctx", adbc_driver_path="/drv", _end_user="u")
    with patch("vast_daft.source.Transaction") as txn:
        _bind_existing_txid(session, 42)
    txn.assert_called_once_with(
        _rpc=session,
        _session_context="ctx",
        txid=42,
        _adbc_driver_path="/drv",
        _end_user="u",
    )


def test_bind_existing_txid_falls_back_without_context() -> None:
    session = SimpleNamespace()
    with patch("vast_daft.source.Transaction") as txn:
        _bind_existing_txid(session, 42)
    txn.assert_called_once_with(session, txid=42)


def test_ensure_table_metadata_prefers_load_over_stats() -> None:
    from vast_daft.connection import ensure_table_metadata_loaded

    md = SimpleNamespace(load=Mock(), load_stats=Mock())
    ensure_table_metadata_loaded(md, tx="tx")
    md.load.assert_called_once_with("tx")
    md.load_stats.assert_not_called()


def test_ensure_table_metadata_falls_back_to_stats() -> None:
    from vast_daft.connection import ensure_table_metadata_loaded

    md = SimpleNamespace(load_stats=Mock())
    ensure_table_metadata_loaded(md, tx="tx")
    md.load_stats.assert_called_once_with("tx")


def test_cached_table_type_stores_int_and_survives_miss() -> None:
    from vast_daft.connection import cached_table_type, clear_metadata_cache

    clear_metadata_cache()
    md = SimpleNamespace(_table_type=2)
    tx = MagicMock()
    tx.__enter__.return_value = tx
    tx.__exit__.return_value = None
    connection = SimpleNamespace(session=SimpleNamespace(transaction=Mock(return_value=tx)))
    config = _make_scan_operator()._config
    schema = pa.schema([("term", pa.string())])

    with (
        patch("vast_daft.connection.VastDBConnection", return_value=connection),
        patch("vast_daft.connection.TableMetadata", return_value=md),
        patch("vast_daft.connection.ensure_table_metadata_loaded"),
    ):
        assert cached_table_type(config, "b", "s", "postings", schema) == 2
        assert cached_table_type(config, "b", "s", "postings", schema) == 2
    connection.session.transaction.assert_called_once()

    clear_metadata_cache()
    with patch("vast_daft.connection.VastDBConnection", side_effect=RuntimeError("down")):
        assert cached_table_type(config, "b", "s", "postings", schema) is None


def test_cached_table_type_accepts_enum_without_int_cast() -> None:
    from enum import Enum

    from vast_daft.connection import cached_table_type, clear_metadata_cache

    class TableType(Enum):
        Elysium = 2

    clear_metadata_cache()
    md = SimpleNamespace(_table_type=TableType.Elysium)
    tx = MagicMock()
    tx.__enter__.return_value = tx
    tx.__exit__.return_value = None
    connection = SimpleNamespace(session=SimpleNamespace(transaction=Mock(return_value=tx)))
    config = _make_scan_operator()._config
    schema = pa.schema([("term", pa.string())])

    with (
        patch("vast_daft.connection.VastDBConnection", return_value=connection),
        patch("vast_daft.connection.TableMetadata", return_value=md),
        patch("vast_daft.connection.ensure_table_metadata_loaded"),
    ):
        assert cached_table_type(config, "b", "s", "postings", schema) == 2
