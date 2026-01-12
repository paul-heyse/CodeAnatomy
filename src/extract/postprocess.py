"""Post-processing helpers for extractor tables."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.interop import ComputeExpression, TableLike, ensure_expression, pc
from arrowdsl.schema.schema import EncodingSpec, encode_columns, encode_expression


def apply_encoding(table: TableLike, *, columns: Sequence[str]) -> TableLike:
    """Dictionary-encode specified string columns.

    Returns
    -------
    TableLike
        Table with encoded columns.
    """
    if not columns:
        return table
    specs = tuple(EncodingSpec(column=col) for col in columns)
    return encode_columns(table, specs=specs)


def encoding_projection(
    columns: Sequence[str],
    *,
    available: Sequence[str],
) -> tuple[list[ComputeExpression], list[str]]:
    """Return projection expressions to apply dictionary encoding.

    Returns
    -------
    tuple[list[ComputeExpression], list[str]]
        Expressions and column names for encoding projection.
    """
    encode_set = set(columns)
    expressions: list[ComputeExpression] = []
    names: list[str] = []
    for name in available:
        expr = encode_expression(name) if name in encode_set else pc.field(name)
        expressions.append(ensure_expression(expr))
        names.append(name)
    return expressions, names
