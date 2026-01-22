"""Selector patterns and deferred expression builders for Ibis IR acceleration.

This module provides Ibis selectors, deferred expressions, and Table.bind patterns
to replace bespoke compute with Ibis IR primitives.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from ibis import Deferred
    from ibis.expr.types import Table, Value


@dataclass(frozen=True)
class SelectorPattern:
    """Column selector pattern specification.

    Attributes
    ----------
    name:
        Pattern name for debugging.
    column_type:
        Optional column data type filter.
    regex_pattern:
        Optional regex pattern for column name matching.
    predicate:
        Optional predicate function for custom selection logic.
    """

    name: str
    column_type: str | None = None
    regex_pattern: str | None = None
    predicate: Callable[[Value], bool] | None = None


def across_columns(
    table: Table,
    selector: SelectorPattern,
    fn: Callable[[Value], Value],
) -> list[Value]:
    """Apply a function across columns selected by a pattern.

    Parameters
    ----------
    table:
        Table to select columns from.
    selector:
        Selector pattern defining which columns to transform.
    fn:
        Function to apply to each selected column.

    Returns
    -------
    list[Value]
        List of transformed column expressions.

    Examples
    --------
    Apply normalization across all numeric columns:

    >>> selector = SelectorPattern(name="numeric", column_type="numeric")
    >>> normalized = across_columns(table, selector, lambda col: (col - col.mean()) / col.std())
    """
    import ibis.selectors as s

    # Build selector from pattern
    if selector.column_type == "numeric":
        sel = s.numeric()
    elif selector.column_type == "string":
        sel = s.of_type("string")
    elif selector.column_type == "temporal":
        # Use individual type selectors combined with OR
        sel = s.of_type("timestamp") | s.of_type("date") | s.of_type("time")
    elif selector.regex_pattern:
        sel = s.matches(selector.regex_pattern)
    else:
        # Default to all columns if no specific pattern
        sel = s.all()

    # Bind selector to table to get column expressions
    bound_cols = table.bind(sel)

    # Apply function to each selected column
    results = []
    for col_expr in bound_cols:
        try:
            transformed = fn(col_expr)
            results.append(transformed)
        except (AttributeError, TypeError, ValueError):
            # Skip columns that fail transformation
            continue

    return results


def bind_deferred(table: Table, *exprs: Deferred) -> tuple[Value, ...]:
    """Bind deferred expressions to a table.

    Parameters
    ----------
    table:
        Table to bind expressions to.
    *exprs:
        Deferred expressions to bind.

    Returns
    -------
    tuple[Value, ...]
        Tuple of bound Value expressions.

    Examples
    --------
    >>> from ibis import _
    >>> deferred_expr = _.x + _.y
    >>> bound = bind_deferred(table, deferred_expr)
    """
    return table.bind(*exprs)


def deferred_hash_column(column_name: str) -> Deferred:
    """Create a deferred hash expression for a column.

    Parameters
    ----------
    column_name:
        Name of the column to hash.

    Returns
    -------
    Deferred
        Deferred expression that computes hash of the column.

    Examples
    --------
    >>> hash_expr = deferred_hash_column("id")
    >>> table.mutate(id_hash=hash_expr)
    """
    from ibis import _

    # Use hash() function on the column
    return _[column_name].hash()


def deferred_concat_columns(
    *column_names: str,
    separator: str = "_",
) -> Deferred:
    """Create a deferred concatenation expression for multiple columns.

    Parameters
    ----------
    *column_names:
        Names of columns to concatenate.
    separator:
        Separator string between concatenated values.

    Returns
    -------
    Deferred
        Deferred expression that concatenates columns.

    Raises
    ------
    ValueError
        Raised when no column names are provided.

    Examples
    --------
    >>> concat_expr = deferred_concat_columns("first_name", "last_name", separator=" ")
    >>> table.mutate(full_name=concat_expr)
    """
    from ibis import _

    if not column_names:
        msg = "At least one column name is required."
        raise ValueError(msg)

    # Start with the first column as string
    result = _[column_names[0]].cast("string")

    # Add remaining columns with separator
    for col_name in column_names[1:]:
        import ibis

        result = result + ibis.literal(separator) + _[col_name].cast("string")

    return result


def deferred_coalesce(
    *column_names: str,
    default: Any = None,
) -> Value:
    """Create a deferred coalesce expression for multiple columns.

    Returns the first non-null value across the specified columns.

    Parameters
    ----------
    *column_names:
        Names of columns to coalesce.
    default:
        Default value if all columns are null.

    Returns
    -------
    Deferred
        Deferred expression that returns first non-null value.

    Raises
    ------
    ValueError
        Raised when no column names are provided.

    Examples
    --------
    >>> coalesce_expr = deferred_coalesce("primary_id", "secondary_id", default="unknown")
    >>> table.mutate(id=coalesce_expr)
    """
    import ibis
    from ibis import _

    if not column_names:
        msg = "At least one column name is required."
        raise ValueError(msg)

    # Build coalesce from columns
    columns: list[Value] = [cast("Value", _[col_name]) for col_name in column_names]

    # Add default value if provided
    if default is not None:
        columns.append(cast("Value", ibis.literal(default)))

    return ibis.coalesce(*columns)


class ColumnSelector:
    """Fluent API for column selection patterns.

    This class wraps Ibis selectors to provide a consistent interface
    for column selection in rule templates and transformations.
    """

    def __init__(self) -> None:
        """Initialize column selector."""
        import ibis.selectors as s

        self._s = s

    def numeric(self) -> Any:
        """Select all numeric columns.

        Returns
        -------
        Selector
            Ibis selector for numeric columns.
        """
        return self._s.numeric()

    def string(self) -> Any:
        """Select all string columns.

        Returns
        -------
        Selector
            Ibis selector for string columns.
        """
        return self._s.of_type("string")

    def temporal(self) -> Any:
        """Select all temporal columns (timestamp, date, time).

        Returns
        -------
        Selector
            Ibis selector for temporal columns.
        """
        # Use individual type selectors combined with OR
        return self._s.of_type("timestamp") | self._s.of_type("date") | self._s.of_type("time")

    def by_name(self, *patterns: str) -> Any:
        """Select columns by name pattern.

        Parameters
        ----------
        *patterns:
            String patterns to match column names (supports regex).

        Returns
        -------
        Selector
            Ibis selector for matching column names.
        """
        if len(patterns) == 1:
            return self._s.matches(patterns[0])
        # Combine multiple patterns with OR
        selectors = [self._s.matches(pattern) for pattern in patterns]
        result = selectors[0]
        for sel in selectors[1:]:
            result |= sel
        return result

    def by_type(self, *types: str) -> Any:
        """Select columns by data type.

        Parameters
        ----------
        *types:
            Data type names to select.

        Returns
        -------
        Selector
            Ibis selector for specified types.
        """
        if len(types) == 1:
            return self._s.of_type(types[0])
        selectors = [self._s.of_type(type_name) for type_name in types]
        result = selectors[0]
        for selector in selectors[1:]:
            result |= selector
        return result


__all__ = [
    "ColumnSelector",
    "SelectorPattern",
    "across_columns",
    "bind_deferred",
    "deferred_coalesce",
    "deferred_concat_columns",
    "deferred_hash_column",
]
