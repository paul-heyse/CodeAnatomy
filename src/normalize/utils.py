"""Shared utilities for normalization pipelines."""

from __future__ import annotations

from arrowdsl.core.ids import SpanIdSpec, add_span_id_column, span_id
from arrowdsl.core.interop import TableLike
from arrowdsl.core.joins import code_unit_meta_config, left_join
from arrowdsl.schema.metadata import (
    DICT_INDEX_META,
    DICT_ORDERED_META,
    ENCODING_DICTIONARY,
    ENCODING_META,
    encoding_policy_from_schema,
)
from normalize.registry_ids import (
    DEF_USE_EVENT_ID_SPEC,
    DIAG_ID_SPEC,
    REACH_EDGE_ID_SPEC,
    TYPE_EXPR_ID_SPEC,
    TYPE_ID_SPEC,
)


def join_code_unit_meta(table: TableLike, code_units: TableLike) -> TableLike:
    """Left-join code unit metadata (file_id/path) onto a table.

    Returns
    -------
    TableLike
        Table with code unit metadata columns.
    """
    config = code_unit_meta_config(table.column_names, code_units.column_names)
    if config is None:
        return table
    return left_join(table, code_units, config=config, use_threads=True)


__all__ = [
    "DEF_USE_EVENT_ID_SPEC",
    "DIAG_ID_SPEC",
    "DICT_INDEX_META",
    "DICT_ORDERED_META",
    "ENCODING_DICTIONARY",
    "ENCODING_META",
    "REACH_EDGE_ID_SPEC",
    "TYPE_EXPR_ID_SPEC",
    "TYPE_ID_SPEC",
    "SpanIdSpec",
    "add_span_id_column",
    "encoding_policy_from_schema",
    "join_code_unit_meta",
    "span_id",
]
