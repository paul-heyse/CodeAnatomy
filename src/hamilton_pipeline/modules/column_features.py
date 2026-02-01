"""Column-level feature subDAGs for CPG quality outputs."""

from __future__ import annotations

import polars as pl
from hamilton.function_modifiers import check_output_custom, schema
from hamilton.plugins.h_polars import with_columns

from cpg.emit_specs import _NODE_OUTPUT_COLUMNS, _PROP_OUTPUT_COLUMNS
from datafusion_engine.arrow.interop import TableLike
from hamilton_pipeline.tag_policy import TagPolicy, apply_tag, semantic_tag_policy
from hamilton_pipeline.validators import (
    NonEmptyTableValidator,
    SchemaContractValidator,
    TableSchemaValidator,
)


def _to_polars(table: TableLike) -> pl.DataFrame:
    if isinstance(table, pl.DataFrame):
        return table
    frame = pl.from_arrow(table)
    if isinstance(frame, pl.Series):
        return frame.to_frame()
    return frame


def _from_polars(frame: pl.DataFrame) -> TableLike:
    return frame.to_arrow()


@apply_tag(TagPolicy(layer="quality", kind="column", artifact="cpg_nodes_path_depth"))
def cpg_nodes_path_depth(path: pl.Series) -> pl.Series:
    return path.fill_null("").str.count_matches("/")


@apply_tag(TagPolicy(layer="quality", kind="column", artifact="cpg_nodes_span_length"))
def cpg_nodes_span_length(bstart: pl.Series, bend: pl.Series) -> pl.Series:
    return (bend - bstart).fill_null(0)


@apply_tag(TagPolicy(layer="quality", kind="column", artifact="cpg_nodes_has_file_id"))
def cpg_nodes_has_file_id(file_id: pl.Series) -> pl.Series:
    return file_id.is_not_null()


@apply_tag(TagPolicy(layer="quality", kind="column", artifact="cpg_nodes_has_task_name"))
def cpg_nodes_has_task_name(task_name: pl.Series) -> pl.Series:
    return task_name.is_not_null()


@apply_tag(TagPolicy(layer="execution", kind="table", artifact="cpg_nodes_polars"))
def cpg_nodes_polars(cpg_nodes: TableLike) -> pl.DataFrame:
    """Return CPG nodes as a Polars DataFrame.

    Returns
    -------
    polars.DataFrame
        Polars DataFrame with node rows.
    """
    return _to_polars(cpg_nodes)


@with_columns(
    cpg_nodes_path_depth,
    cpg_nodes_span_length,
    cpg_nodes_has_file_id,
    cpg_nodes_has_task_name,
    columns_to_pass=["path", "bstart", "bend", "file_id", "task_name"],
    select=[
        "cpg_nodes_path_depth",
        "cpg_nodes_span_length",
        "cpg_nodes_has_file_id",
        "cpg_nodes_has_task_name",
    ],
)
@apply_tag(TagPolicy(layer="execution", kind="table", artifact="cpg_nodes_quality_polars"))
def cpg_nodes_quality_polars(cpg_nodes_polars: pl.DataFrame) -> pl.DataFrame:
    """Return CPG nodes with quality columns appended.

    Returns
    -------
    polars.DataFrame
        Polars DataFrame with quality columns.
    """
    return cpg_nodes_polars


_CPG_NODES_QUALITY_COLUMNS = (
    *_NODE_OUTPUT_COLUMNS,
    "cpg_nodes_path_depth",
    "cpg_nodes_span_length",
    "cpg_nodes_has_file_id",
    "cpg_nodes_has_task_name",
)


@check_output_custom(
    SchemaContractValidator(dataset_name="cpg_nodes_quality_v1", importance="warn"),
    TableSchemaValidator(expected_columns=_CPG_NODES_QUALITY_COLUMNS, importance="fail"),
    NonEmptyTableValidator(),
)
@schema.output(*tuple((col, "string") for col in _CPG_NODES_QUALITY_COLUMNS))
@apply_tag(semantic_tag_policy("cpg_nodes_quality"))
def cpg_nodes_quality(cpg_nodes_quality_polars: pl.DataFrame) -> TableLike:
    """Return CPG nodes quality output as Arrow.

    Returns
    -------
    TableLike
        Arrow-backed table with node quality rows.
    """
    return _from_polars(cpg_nodes_quality_polars)


@apply_tag(TagPolicy(layer="quality", kind="column", artifact="cpg_props_value_present"))
def cpg_props_value_present(
    value_string: pl.Series,
    value_int: pl.Series,
    value_float: pl.Series,
    value_bool: pl.Series,
    value_json: pl.Series,
) -> pl.Series:
    return (
        value_string.is_not_null()
        | value_int.is_not_null()
        | value_float.is_not_null()
        | value_bool.is_not_null()
        | value_json.is_not_null()
    )


@apply_tag(TagPolicy(layer="quality", kind="column", artifact="cpg_props_value_is_numeric"))
def cpg_props_value_is_numeric(value_int: pl.Series, value_float: pl.Series) -> pl.Series:
    return value_int.is_not_null() | value_float.is_not_null()


@apply_tag(TagPolicy(layer="quality", kind="column", artifact="cpg_props_key_length"))
def cpg_props_key_length(prop_key: pl.Series) -> pl.Series:
    return prop_key.fill_null("").str.len_chars()


@apply_tag(TagPolicy(layer="execution", kind="table", artifact="cpg_props_polars"))
def cpg_props_polars(cpg_props: TableLike) -> pl.DataFrame:
    """Return CPG properties as a Polars DataFrame.

    Returns
    -------
    polars.DataFrame
        Polars DataFrame with property rows.
    """
    return _to_polars(cpg_props)


@with_columns(
    cpg_props_value_present,
    cpg_props_value_is_numeric,
    cpg_props_key_length,
    columns_to_pass=[
        "prop_key",
        "value_string",
        "value_int",
        "value_float",
        "value_bool",
        "value_json",
    ],
    select=["cpg_props_value_present", "cpg_props_value_is_numeric", "cpg_props_key_length"],
)
@apply_tag(TagPolicy(layer="execution", kind="table", artifact="cpg_props_quality_polars"))
def cpg_props_quality_polars(cpg_props_polars: pl.DataFrame) -> pl.DataFrame:
    """Return CPG properties with quality columns appended.

    Returns
    -------
    polars.DataFrame
        Polars DataFrame with quality columns.
    """
    return cpg_props_polars


_CPG_PROPS_QUALITY_COLUMNS = (
    *_PROP_OUTPUT_COLUMNS,
    "cpg_props_value_present",
    "cpg_props_value_is_numeric",
    "cpg_props_key_length",
)


@check_output_custom(
    SchemaContractValidator(dataset_name="cpg_props_quality_v1", importance="warn"),
    TableSchemaValidator(expected_columns=_CPG_PROPS_QUALITY_COLUMNS, importance="fail"),
    NonEmptyTableValidator(),
)
@schema.output(*tuple((col, "string") for col in _CPG_PROPS_QUALITY_COLUMNS))
@apply_tag(semantic_tag_policy("cpg_props_quality"))
def cpg_props_quality(cpg_props_quality_polars: pl.DataFrame) -> TableLike:
    """Return CPG properties quality output as Arrow.

    Returns
    -------
    TableLike
        Arrow-backed table with property quality rows.
    """
    return _from_polars(cpg_props_quality_polars)


__all__ = [
    "cpg_nodes_polars",
    "cpg_nodes_quality",
    "cpg_nodes_quality_polars",
    "cpg_props_polars",
    "cpg_props_quality",
    "cpg_props_quality_polars",
]
