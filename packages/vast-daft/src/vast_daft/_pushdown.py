"""Translate Daft ``Pushdowns`` to ibis predicates and projection columns.

Daft calls ``get_tasks(pushdowns)`` with a :class:`~daft.io.pushdowns.Pushdowns`
object that may carry:

* ``filters``  — a ``daft.expressions.Expression`` (the combined filter predicate)
* ``columns``  — a ``list[str]`` of column names to project
* ``limit``    — an ``int`` row limit

This module converts the ``filters`` expression into an ibis predicate that can
be passed to :func:`vastdb.table.Table.select_splits`, achieving genuine
server-side push-down for filter, column projection, and row limit.

Supported Daft expression nodes
---------------------------------
Binary comparisons: ``==``, ``!=``, ``>``, ``>=``, ``<``, ``<=``
Membership:         ``col.is_in([...])``
Between:            ``col.between(lo, hi)``
Null checks:        ``col.is_null()``, ``col.not_null()``
Logical:            ``expr & expr``, ``expr | expr``, ``~expr``
String:             ``col.contains(s)``, ``col.startswith(s)``, ``col.endswith(s)``

Any unsupported node causes the entire filter to be returned un-translated
(``None``), leaving Daft to apply it client-side as a post-filter.
"""

from __future__ import annotations

import logging
from typing import Any

import ibis
from daft.expressions import Expression, ExpressionVisitor
from daft.io.pushdowns import Pushdowns
from ibis.common.deferred import Deferred
from ibis.expr.types import Column as BooleanColumn

logger = logging.getLogger(__name__)

Predicate = BooleanColumn | Deferred

# Sentinel to signal "unsupported — fall back to client-side"
_UNSUPPORTED = object()


# ---------------------------------------------------------------------------
# Visitor
# ---------------------------------------------------------------------------


class _DaftToIbisVisitor(ExpressionVisitor):
    """Walk a Daft expression tree and produce an ibis predicate.

    Returns ``_UNSUPPORTED`` (the module-level sentinel) for any node that
    cannot be translated, so callers can detect a partial failure without
    raising.
    """

    def __init__(self) -> None:
        super().__init__()
        self.referenced_columns: set[str] = set()

    # -- Leaves --------------------------------------------------------------

    def visit_col(self, name: str) -> Any:
        self.referenced_columns.add(name)
        return ibis._[name]

    def visit_lit(self, value: Any) -> Any:
        # Daft passes the Python value directly (int, float, str, bool, None)
        return value

    # -- Structural nodes that we don't push down ----------------------------

    def visit_alias(self, expr: Expression, alias: str) -> Any:
        # Aliases don't affect predicate semantics — visit the inner expr
        return self.visit(expr)

    def _walk_unsupported(self, *exprs: Any) -> Any:
        """Record columns under an untranslatable node, then decline it.

        Declining still requires the column names: Daft keeps the filter above
        the scan, so those columns must survive the pushed-down projection or
        the scan yields a schema without them and the filter raises
        ``FieldNotFound``.
        """
        for expr in exprs:
            try:
                self.visit(expr)
            except Exception:  # noqa: BLE001 — collecting names must not fail the scan
                logger.debug("Could not walk unsupported sub-expression %r", expr, exc_info=True)
        return _UNSUPPORTED

    def visit_cast(self, expr: Expression, dtype: Any) -> Any:
        # Casts are not translatable to ibis predicates.
        return self._walk_unsupported(expr)

    def visit_coalesce(self, *args: Expression) -> Any:
        # Forward-compatibility with newer Daft visitor APIs.
        return self._walk_unsupported(*args)

    # -- Function dispatch ---------------------------------------------------

    def visit_function(self, name: str, args: list) -> Any:
        """Fallback for unrecognised function names."""
        return self._walk_unsupported(*args)

    # visit_() in the base class dispatches to visit_<name> if it exists,
    # otherwise calls visit_function().  We register individual functions
    # below as visit_<daft_function_name> methods.

    def _binop(self, args: list, op: str) -> Any:
        """Helper: visit two expression args and apply a binary ibis operator."""
        lhs = self.visit(args[0])
        rhs = self.visit(args[1])
        if lhs is _UNSUPPORTED or rhs is _UNSUPPORTED:
            return _UNSUPPORTED
        ops = {
            "equal": lambda a, b: a == b,
            "not_equal": lambda a, b: a != b,
            "greater_than": lambda a, b: a > b,
            "greater_than_or_equal": lambda a, b: a >= b,
            "less_than": lambda a, b: a < b,
            "less_than_or_equal": lambda a, b: a <= b,
            "and": lambda a, b: a & b,
            "or": lambda a, b: a | b,
        }
        return ops[op](lhs, rhs)

    # Comparison operators
    def visit_equal(self, lhs: Expression, rhs: Expression) -> Any:
        return self._binop([lhs, rhs], "equal")

    def visit_not_equal(self, lhs: Expression, rhs: Expression) -> Any:
        return self._binop([lhs, rhs], "not_equal")

    def visit_greater_than(self, lhs: Expression, rhs: Expression) -> Any:
        return self._binop([lhs, rhs], "greater_than")

    def visit_greater_than_or_equal(self, lhs: Expression, rhs: Expression) -> Any:
        return self._binop([lhs, rhs], "greater_than_or_equal")

    def visit_less_than(self, lhs: Expression, rhs: Expression) -> Any:
        return self._binop([lhs, rhs], "less_than")

    def visit_less_than_or_equal(self, lhs: Expression, rhs: Expression) -> Any:
        return self._binop([lhs, rhs], "less_than_or_equal")

    # Logical operators
    def visit_and(self, lhs: Expression, rhs: Expression) -> Any:
        return self._binop([lhs, rhs], "and")

    def visit_or(self, lhs: Expression, rhs: Expression) -> Any:
        return self._binop([lhs, rhs], "or")

    def visit_not(self, expr: Expression) -> Any:
        inner = self.visit(expr)
        if inner is _UNSUPPORTED:
            return _UNSUPPORTED
        return ~inner

    # Null checks
    def visit_is_null(self, expr: Expression) -> Any:
        inner = self.visit(expr)
        if inner is _UNSUPPORTED:
            return _UNSUPPORTED
        return inner.isnull()

    def visit_not_null(self, expr: Expression) -> Any:
        inner = self.visit(expr)
        if inner is _UNSUPPORTED:
            return _UNSUPPORTED
        return inner.notnull()

    # Membership: is_in(expr, [lit, lit, ...])
    # NOTE: args[1] is a raw list of Expression objects, not pre-visited.
    def visit_is_in(self, expr: Expression, values: list) -> Any:
        col_ibis = self.visit(expr)
        if col_ibis is _UNSUPPORTED:
            return _UNSUPPORTED
        visited_vals = [self.visit(v) for v in values]
        if any(v is _UNSUPPORTED for v in visited_vals):
            return _UNSUPPORTED
        return col_ibis.isin(visited_vals)

    # Between: between(expr, lo, hi)
    def visit_between(self, expr: Expression, lo: Expression, hi: Expression) -> Any:
        col_ibis = self.visit(expr)
        lo_val = self.visit(lo)
        hi_val = self.visit(hi)
        if any(v is _UNSUPPORTED for v in [col_ibis, lo_val, hi_val]):
            return _UNSUPPORTED
        return col_ibis.between(lo_val, hi_val)

    # String functions
    def visit_starts_with(self, expr: Expression, prefix: Expression) -> Any:
        col_ibis = self.visit(expr)
        val = self.visit(prefix)
        if col_ibis is _UNSUPPORTED or val is _UNSUPPORTED:
            return _UNSUPPORTED
        return col_ibis.startswith(val)

    def visit_ends_with(self, expr: Expression, suffix: Expression) -> Any:
        col_ibis = self.visit(expr)
        val = self.visit(suffix)
        if col_ibis is _UNSUPPORTED or val is _UNSUPPORTED:
            return _UNSUPPORTED
        return col_ibis.endswith(val)

    def visit_contains(self, expr: Expression, pattern: Expression) -> Any:
        col_ibis = self.visit(expr)
        val = self.visit(pattern)
        if col_ibis is _UNSUPPORTED or val is _UNSUPPORTED:
            return _UNSUPPORTED
        return col_ibis.contains(val)


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------

def pushdowns_to_predicate(pushdowns: Pushdowns) -> Predicate | None:
    """Convert *pushdowns.filters* to an ibis predicate, or ``None``.

    Returns ``None`` when no filter is present or when the expression contains
    an unsupported node.  In that case the caller should not pass a predicate
    to VastDB, and Daft will apply the filter client-side.
    """
    predicate, _ = pushdowns_to_predicate_and_columns(pushdowns)
    return predicate


def pushdowns_to_predicate_and_columns(
    pushdowns: Pushdowns,
) -> tuple[Predicate | None, set[str]]:
    """Translate the filter and return the columns it references.

    The referenced column set is needed by callers that also push down a
    column projection. VastDB's ``select_splits`` requires every column the
    predicate touches to appear in ``columns``; otherwise the server raises
    ``FieldNotFound``. Callers must union this set with their projection
    before handing it to VastDB.

    The set is empty only when there is no filter. An *unsupported* filter still
    reports its columns: Daft applies it above the scan, so those columns must
    stay in the projection even though no predicate is pushed to VastDB.
    """
    if pushdowns.filters is None:
        return None, set()
    visitor = _DaftToIbisVisitor()
    try:
        result = visitor.visit(pushdowns.filters)
    except Exception as exc:
        logger.debug("Daft filter not pushed down to VastDB — visitor error: %s", exc)
        return None, visitor.referenced_columns
    if result is _UNSUPPORTED:
        logger.debug(
            "Daft filter not pushed down to VastDB — unsupported expression: %s (columns kept: %s)",
            pushdowns.filters,
            sorted(visitor.referenced_columns),
        )
        return None, visitor.referenced_columns
    return result, visitor.referenced_columns
