"""DataFusion Expr wrappers backed by the native extension."""

from __future__ import annotations

from datafusion import Expr

from datafusion_ext import (
    arrow_metadata as _arrow_metadata,
)
from datafusion_ext import (
    col_to_byte as _col_to_byte,
)
from datafusion_ext import (
    count_distinct_agg as _count_distinct_agg,
)
from datafusion_ext import (
    first_value_agg as _first_value_agg,
)
from datafusion_ext import (
    lag_window as _lag_window,
)
from datafusion_ext import (
    last_value_agg as _last_value_agg,
)
from datafusion_ext import (
    lead_window as _lead_window,
)
from datafusion_ext import (
    list_extract as _list_extract,
)
from datafusion_ext import (
    list_unique as _list_unique,
)
from datafusion_ext import (
    map_entries as _map_entries,
)
from datafusion_ext import (
    map_extract as _map_extract,
)
from datafusion_ext import (
    map_keys as _map_keys,
)
from datafusion_ext import (
    map_values as _map_values,
)
from datafusion_ext import (
    prefixed_hash64 as _prefixed_hash64,
)
from datafusion_ext import (
    row_index as _row_index,
)
from datafusion_ext import (
    row_number_window as _row_number_window,
)
from datafusion_ext import (
    running_count as _running_count,
)
from datafusion_ext import (
    running_total as _running_total,
)
from datafusion_ext import (
    stable_hash64 as _stable_hash64,
)
from datafusion_ext import (
    stable_hash128 as _stable_hash128,
)
from datafusion_ext import (
    stable_id as _stable_id,
)
from datafusion_ext import (
    string_agg as _string_agg,
)
from datafusion_ext import (
    union_extract as _union_extract,
)
from datafusion_ext import (
    union_tag as _union_tag,
)


def map_entries(expr: Expr) -> Expr:
    """Build a map_entries expression.

    Parameters
    ----------
    expr
        Map expression.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _map_entries(expr)


def map_keys(expr: Expr) -> Expr:
    """Build a map_keys expression.

    Parameters
    ----------
    expr
        Map expression.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _map_keys(expr)


def map_values(expr: Expr) -> Expr:
    """Build a map_values expression.

    Parameters
    ----------
    expr
        Map expression.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _map_values(expr)


def map_extract(expr: Expr, key: str) -> Expr:
    """Build a map_extract expression.

    Parameters
    ----------
    expr
        Map expression.
    key
        Key to extract.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _map_extract(expr, key)


def list_extract(expr: Expr, index: int) -> Expr:
    """Build a list_extract expression.

    Parameters
    ----------
    expr
        List expression.
    index
        1-based index to extract.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _list_extract(expr, index)


def list_unique(expr: Expr) -> Expr:
    """Build a list_unique aggregate expression.

    Parameters
    ----------
    expr
        Value expression to aggregate.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _list_unique(expr)


def first_value_agg(expr: Expr) -> Expr:
    """Build a first_value_agg aggregate expression.

    Parameters
    ----------
    expr
        Value expression to aggregate.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _first_value_agg(expr)


def last_value_agg(expr: Expr) -> Expr:
    """Build a last_value_agg aggregate expression.

    Parameters
    ----------
    expr
        Value expression to aggregate.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _last_value_agg(expr)


def count_distinct_agg(expr: Expr) -> Expr:
    """Build a count_distinct_agg aggregate expression.

    Parameters
    ----------
    expr
        Value expression to aggregate.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _count_distinct_agg(expr)


def string_agg(value: Expr, delimiter: Expr) -> Expr:
    """Build a string_agg aggregate expression.

    Parameters
    ----------
    value
        Value expression to aggregate.
    delimiter
        Delimiter expression.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _string_agg(value, delimiter)


def row_index(expr: Expr) -> Expr:
    """Build a row_index window expression.

    Parameters
    ----------
    expr
        Dummy expression (unused by the window function).

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _row_index(expr)


def running_count(expr: Expr) -> Expr:
    """Build a running_count window expression.

    Parameters
    ----------
    expr
        Dummy expression (unused by the window function).

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _running_count(expr)


def running_total(expr: Expr) -> Expr:
    """Build a running_total window expression.

    Parameters
    ----------
    expr
        Value expression to aggregate.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _running_total(expr)


def row_number_window(expr: Expr) -> Expr:
    """Build a row_number_window expression.

    Parameters
    ----------
    expr
        Dummy expression (unused by the window function).

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _row_number_window(expr)


def lag_window(expr: Expr) -> Expr:
    """Build a lag_window expression.

    Parameters
    ----------
    expr
        Value expression to lag.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _lag_window(expr)


def lead_window(expr: Expr) -> Expr:
    """Build a lead_window expression.

    Parameters
    ----------
    expr
        Value expression to lead.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _lead_window(expr)


def arrow_metadata(expr: Expr, key: str | None = None) -> Expr:
    """Build an arrow_metadata expression.

    Parameters
    ----------
    expr
        Expression with Arrow metadata.
    key
        Optional metadata key to extract.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _arrow_metadata(expr, key)


def union_tag(expr: Expr) -> Expr:
    """Build a union_tag expression.

    Parameters
    ----------
    expr
        Union expression.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _union_tag(expr)


def union_extract(expr: Expr, tag: str) -> Expr:
    """Build a union_extract expression.

    Parameters
    ----------
    expr
        Union expression.
    tag
        Tag name to extract.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _union_extract(expr, tag)


def stable_hash64(value: Expr) -> Expr:
    """Build a stable_hash64 expression.

    Parameters
    ----------
    value
        Value expression to hash.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _stable_hash64(value)


def stable_hash128(value: Expr) -> Expr:
    """Build a stable_hash128 expression.

    Parameters
    ----------
    value
        Value expression to hash.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _stable_hash128(value)


def prefixed_hash64(prefix: str, value: Expr) -> Expr:
    """Build a prefixed_hash64 expression.

    Parameters
    ----------
    prefix
        Prefix string.
    value
        Value expression to hash.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _prefixed_hash64(prefix, value)


def stable_id(prefix: str, value: Expr) -> Expr:
    """Build a stable_id expression.

    Parameters
    ----------
    prefix
        Prefix string.
    value
        Value expression to hash.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _stable_id(prefix, value)


def col_to_byte(line_text: Expr, col_index: Expr, col_unit: Expr) -> Expr:
    """Build a col_to_byte expression.

    Parameters
    ----------
    line_text
        Line text expression.
    col_index
        Column offset expression.
    col_unit
        Column unit expression.

    Returns
    -------
    Expr
        DataFusion expression.
    """
    return _col_to_byte(line_text, col_index, col_unit)


__all__ = [
    "arrow_metadata",
    "col_to_byte",
    "count_distinct_agg",
    "first_value_agg",
    "lag_window",
    "last_value_agg",
    "lead_window",
    "list_extract",
    "list_unique",
    "map_entries",
    "map_extract",
    "map_keys",
    "map_values",
    "prefixed_hash64",
    "row_index",
    "row_number_window",
    "running_count",
    "running_total",
    "stable_hash64",
    "stable_hash128",
    "stable_id",
    "string_agg",
    "union_extract",
    "union_tag",
]
