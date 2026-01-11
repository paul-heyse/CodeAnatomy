"""Arrow-native iteration helpers."""

from __future__ import annotations

from collections.abc import Iterator, Sequence

from arrowdsl.pyarrow_protocols import ArrayLike, ScalarLike, TableLike


def iter_array_values(array: ArrayLike) -> Iterator[object | None]:
    """Yield native Python values for an Arrow array.

    Parameters
    ----------
    array
        Arrow array or chunked array to iterate.

    Yields
    ------
    object | None
        Native Python values for each element.
    """
    for value in array:
        if isinstance(value, ScalarLike):
            yield value.as_py()
        else:
            yield value


def iter_arrays(arrays: Sequence[ArrayLike]) -> Iterator[tuple[object | None, ...]]:
    """Iterate arrays row-wise in lockstep.

    Parameters
    ----------
    arrays
        Arrays to iterate together.

    Yields
    ------
    tuple[object | None, ...]
        Row-wise tuples of values.
    """
    iters = [iter_array_values(array) for array in arrays]
    yield from zip(*iters, strict=True)


def iter_table_rows(table: TableLike) -> Iterator[dict[str, object]]:
    """Iterate rows as dicts without materializing a full list.

    Parameters
    ----------
    table
        Table to iterate row-wise.

    Yields
    ------
    dict[str, object]
        Mapping of column name to row value.
    """
    columns = list(table.column_names)
    arrays = [table[col] for col in columns]
    iters = [iter_array_values(array) for array in arrays]
    for values in zip(*iters, strict=True):
        yield dict(zip(columns, values, strict=True))
