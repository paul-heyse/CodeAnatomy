"""Shared span normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.arrays import ColumnDefaultsSpec, ConstExpr, FieldExpr, set_or_append_column
from normalize.schemas import SPAN_ERROR_SCHEMA


@dataclass(frozen=True)
class SpanOutputColumns:
    """Column names for span outputs."""

    bstart: str = "bstart"
    bend: str = "bend"
    ok: str = "span_ok"


def append_span_columns(
    table: TableLike,
    *,
    bstarts: list[int | None],
    bends: list[int | None],
    oks: list[bool],
    columns: SpanOutputColumns | None = None,
) -> TableLike:
    """Append span columns to a table.

    Returns
    -------
    TableLike
        Table with span columns appended.
    """
    cols = columns or SpanOutputColumns()
    out = set_or_append_column(table, cols.bstart, pa.array(bstarts, type=pa.int64()))
    out = set_or_append_column(out, cols.bend, pa.array(bends, type=pa.int64()))
    return set_or_append_column(out, cols.ok, pa.array(oks, type=pa.bool_()))


def append_alias_cols(table: TableLike, aliases: dict[str, str]) -> TableLike:
    """Append alias columns mapped to existing columns.

    Returns
    -------
    TableLike
        Table with alias columns appended.
    """
    out = table
    for newc, oldc in aliases.items():
        if newc in out.column_names:
            continue
        if oldc not in out.column_names:
            defaults = ColumnDefaultsSpec(
                defaults=((newc, ConstExpr(None, dtype=pa.int64())),),
            )
            out = defaults.apply(out)
            continue
        defaults = ColumnDefaultsSpec(
            defaults=((newc, FieldExpr(oldc)),),
        )
        out = defaults.apply(out)
    return out


def span_error_table(rows: list[dict[str, str]]) -> TableLike:
    """Build a span error table from error rows.

    Returns
    -------
    TableLike
        Span error table.
    """
    if rows:
        return pa.Table.from_pylist(rows, schema=SPAN_ERROR_SCHEMA)
    return pa.Table.from_arrays(
        [
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
        ],
        names=SPAN_ERROR_SCHEMA.names,
    )


__all__ = [
    "SpanOutputColumns",
    "append_alias_cols",
    "append_span_columns",
    "span_error_table",
]
