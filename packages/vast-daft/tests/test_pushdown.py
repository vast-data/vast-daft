"""Unit tests for the Daft → ibis predicate translator (_pushdown.py)
and the column/limit push-down paths in VastDBDataSource.get_tasks().
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import patch

import pyarrow as pa
from daft import DataType
from daft.expressions import col, lit
from daft.io.pushdowns import Pushdowns

from vast_daft._pushdown import pushdowns_to_predicate, pushdowns_to_predicate_and_columns
from vast_daft.config import VastDBConfig
from vast_daft.source import VastDBDataSource, VastDBDataSourceTask


def _translate(daft_expr):
    """Helper: wrap a Daft Expression in Pushdowns and translate."""
    return pushdowns_to_predicate(Pushdowns(filters=daft_expr))


class TestNoPredicate:
    def test_none_filters_returns_none(self):
        assert pushdowns_to_predicate(Pushdowns()) is None

    def test_empty_pushdowns_returns_none(self):
        assert pushdowns_to_predicate(Pushdowns(filters=None)) is None


class TestReferencedColumns:
    """The column set returned alongside the predicate must cover every column
    the predicate touches; otherwise the scan layer cannot ensure VastDB's
    select_splits has the columns it needs to evaluate the filter.
    """

    def test_simple_comparison(self):
        pred, cols = pushdowns_to_predicate_and_columns(Pushdowns(filters=col("amount") > lit(400)))
        assert pred is not None
        assert cols == {"amount"}

    def test_logical_and_collects_both_sides(self):
        expr = (col("amount") > lit(100)) & (col("status") == lit("paid"))
        pred, cols = pushdowns_to_predicate_and_columns(Pushdowns(filters=expr))
        assert pred is not None
        assert cols == {"amount", "status"}

    def test_between_and_is_in(self):
        expr = col("amount").between(cast(Any, lit(10)), cast(Any, lit(20))) | col("region").is_in(["us", "eu"])
        pred, cols = pushdowns_to_predicate_and_columns(Pushdowns(filters=expr))
        assert pred is not None
        assert cols == {"amount", "region"}

    def test_empty_when_no_filter(self):
        pred, cols = pushdowns_to_predicate_and_columns(Pushdowns(filters=None))
        assert pred is None
        assert cols == set()


class TestComparisons:
    def test_eq_int(self):
        pred = _translate(col("id") == 3)
        assert pred is not None
        assert repr(pred) == "(_['id'] == 3)"

    def test_ne_int(self):
        pred = _translate(col("id") != 3)
        assert pred is not None
        assert repr(pred) == "(_['id'] != 3)"

    def test_gt(self):
        pred = _translate(col("id") > lit(2))
        assert pred is not None
        assert repr(pred) == "(_['id'] > 2)"

    def test_ge(self):
        pred = _translate(col("id") >= lit(2))
        assert pred is not None
        assert repr(pred) == "(_['id'] >= 2)"

    def test_lt(self):
        pred = _translate(col("id") < lit(4))
        assert pred is not None
        assert repr(pred) == "(_['id'] < 4)"

    def test_le(self):
        pred = _translate(col("id") <= lit(4))
        assert pred is not None
        assert repr(pred) == "(_['id'] <= 4)"

    def test_eq_string(self):
        pred = _translate(col("name") == "alice")
        assert pred is not None

    def test_eq_float(self):
        pred = _translate(col("score") == 0.5)
        assert pred is not None


class TestBetween:
    def test_between_float(self):
        pred = _translate(col("score").between(0.2, 0.4))
        assert pred is not None
        assert repr(pred) == "_['score'].between(0.2, 0.4)"

    def test_between_int(self):
        pred = _translate(col("id").between(1, 5))
        assert pred is not None


class TestIn:
    def test_in_strings(self):
        pred = _translate(col("name").is_in(["alice", "eve"]))
        assert pred is not None
        # ibis isin repr
        assert "isin" in repr(pred)

    def test_in_ints(self):
        pred = _translate(col("id").is_in([1, 2, 3]))
        assert pred is not None
        assert "isin" in repr(pred)

    def test_in_single_value(self):
        pred = _translate(col("id").is_in([42]))
        assert pred is not None


class TestNullChecks:
    def test_is_null(self):
        pred = _translate(col("id").is_null())
        assert pred is not None
        assert "isnull" in repr(pred)

    def test_not_null(self):
        pred = _translate(col("id").not_null())
        assert pred is not None
        assert "notnull" in repr(pred)


class TestLogical:
    def test_and(self):
        pred = _translate((col("id") == lit(1)) & (col("score") > lit(0.1)))
        assert pred is not None
        r = repr(pred)
        assert "_['id']" in r
        assert "_['score']" in r

    def test_or(self):
        pred = _translate((col("id") == 1) | (col("id") == 2))
        assert pred is not None
        r = repr(pred)
        assert "|" in r

    def test_not(self):
        pred = _translate(~(col("id") == 1))
        assert pred is not None
        assert "~" in repr(pred)

    def test_nested_and_or(self):
        expr = ((col("id") >= lit(1)) & (col("id") <= lit(10))) | (col("name") == lit("admin"))
        pred = _translate(expr)
        assert pred is not None


class TestStringOps:
    def test_startswith(self):
        pred = _translate(col("name").startswith("ali"))
        assert pred is not None
        assert "startswith" in repr(pred)

    def test_endswith(self):
        pred = _translate(col("name").endswith("ce"))
        assert pred is not None
        assert "endswith" in repr(pred)

    def test_contains(self):
        pred = _translate(col("name").contains("li"))
        assert pred is not None
        assert "contains" in repr(pred)


class TestUnsupported:
    def test_unsupported_returns_none(self):
        """Expressions we cannot translate should return None, not raise."""
        # abs() is not a supported predicate node
        pred = _translate(col("id").abs() > lit(0))
        assert pred is None


# ---------------------------------------------------------------------------
# Helpers for VastDBDataSource.get_tasks() column push-down tests
# ---------------------------------------------------------------------------

_TABLE_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("name", pa.string()),
        ("score", pa.float64()),
    ]
)

_DUMMY_CONFIG = VastDBConfig(
    endpoint="http://localhost:9999",
    access_key="ak",
    secret_key="sk",
    bucket="b",
    schema="s",
)


def _make_source(**kwargs) -> VastDBDataSource:
    with patch("vast_daft.source._begin_shared_read_txid", return_value=777):
        return VastDBDataSource(
            config=_DUMMY_CONFIG,
            table_name="t",
            table_schema=_TABLE_SCHEMA,
            **kwargs,
        )


def _tasks(source: VastDBDataSource, pushdowns: Pushdowns) -> list[VastDBDataSourceTask]:
    """Collect tasks without executing them (no VastDB connection needed)."""
    return list(cast(Any, source.get_tasks(pushdowns)))


class TestColumnPushdown:
    """Tests for the column-projection path in VastDBDataSource.get_tasks().

    Column projection is enabled now that Daft supplies complete column sets
    for joins followed by selects or aggregations.
    """

    def test_no_columns_no_pushdown_yields_full_schema(self):
        src = _make_source(num_splits=1)
        tasks = _tasks(src, Pushdowns())
        assert tasks[0]._columns is None

    def test_pushdown_columns_applied(self):
        src = _make_source(num_splits=1)
        tasks = _tasks(src, Pushdowns(columns=["id", "name"]))
        assert tasks[0]._columns == ["id", "name"]

    def test_pushdown_single_column_applied(self):
        src = _make_source(num_splits=1)
        tasks = _tasks(src, Pushdowns(columns=["name"]))
        assert tasks[0]._columns == ["name"]

    def test_pushdown_all_columns_applied(self):
        all_cols = ["id", "name", "score"]
        src = _make_source(num_splits=1)
        tasks = _tasks(src, Pushdowns(columns=all_cols))
        assert tasks[0]._columns == all_cols

    def test_columns_set_for_all_splits(self):
        src = _make_source(num_splits=3)
        tasks = _tasks(src, Pushdowns(columns=["id"]))
        assert len(tasks) == 3
        assert all(t._columns == ["id"] for t in tasks)

    def test_task_schema_matches_projection(self):
        src = _make_source(num_splits=1)
        tasks = _tasks(src, Pushdowns(columns=["id", "name"]))
        task_schema = tasks[0].schema.to_pyarrow_schema()
        assert task_schema.names == ["id", "name"]

    def test_task_schema_full_when_no_columns(self):
        src = _make_source(num_splits=1)
        tasks = _tasks(src, Pushdowns())
        task_schema = tasks[0].schema.to_pyarrow_schema()
        assert task_schema.names == ["id", "name", "score"]

    def test_source_schema_is_always_full_table_schema(self):
        src = _make_source(num_splits=1)
        assert src.schema.to_pyarrow_schema().names == ["id", "name", "score"]

    def test_source_schema_full_when_no_columns(self):
        src = _make_source(num_splits=1)
        assert src.schema.to_pyarrow_schema().names == ["id", "name", "score"]

    def test_all_tasks_share_the_same_txid(self):
        src = _make_source(num_splits=3)
        tasks = _tasks(src, Pushdowns())
        assert [task._txid for task in tasks] == [777, 777, 777]


class TestUnsupportedPredicateStillReportsColumns:
    """An untranslatable filter is applied by Daft above the scan, so its
    columns must survive the pushed-down projection. Dropping them made the
    scan yield a schema without them and the filter raised FieldNotFound.
    """

    def test_cast_predicate_reports_its_column(self):
        pred, cols = pushdowns_to_predicate_and_columns(
            Pushdowns(filters=col("term_hash").cast(DataType.uint64()) == lit(7))
        )
        assert pred is None, "cast is not translatable to a VastDB predicate"
        assert "term_hash" in cols

    def test_function_predicate_reports_its_column(self):
        pred, cols = pushdowns_to_predicate_and_columns(
            Pushdowns(filters=col("term").length() == lit(3))
        )
        assert pred is None
        assert "term" in cols

    def test_supported_side_of_and_still_reports_both(self):
        pred, cols = pushdowns_to_predicate_and_columns(
            Pushdowns(filters=(col("term") == lit("x")) & (col("term_hash").cast(DataType.uint64()) == lit(7)))
        )
        assert {"term", "term_hash"} <= cols
