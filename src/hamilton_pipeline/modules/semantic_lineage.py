"""Semantic column lineage nodes for key columns."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl
from hamilton.function_modifiers import extract_columns, tag_outputs

from datafusion_engine.arrow.interop import TableLike
from hamilton_pipeline.tag_policy import semantic_column_payloads
from semantics.catalog.tags import column_name_prefix, prefixed_column_names


def _to_polars(table: TableLike) -> pl.DataFrame:
    """Convert Arrow-compatible inputs to a Polars DataFrame.

    Returns
    -------
    pl.DataFrame
        Polars DataFrame representation of the input.
    """
    if isinstance(table, pl.DataFrame):
        return table
    frame = pl.from_arrow(table)
    if isinstance(frame, pl.Series):
        return frame.to_frame()
    return frame


def _prefixed_key_frame(
    table: TableLike,
    *,
    dataset_alias: str,
    columns: Sequence[str],
) -> pl.DataFrame:
    """Return key columns with a semantic dataset prefix.

    Returns
    -------
    pl.DataFrame
        DataFrame containing prefixed key columns.
    """
    frame = _to_polars(table)
    prefix = column_name_prefix(dataset_alias)
    selectors = [pl.col(col).alias(f"{prefix}{col}") for col in columns]
    return frame.select(selectors)


_CPG_NODES_KEYS = ("node_id",)
_CPG_EDGES_KEYS = ("edge_id", "src_node_id", "dst_node_id")
_CPG_PROPS_KEYS = ("entity_kind", "entity_id", "prop_key")
_CPG_NODES_QUALITY_KEYS = ("node_id",)
_CPG_PROPS_QUALITY_KEYS = ("entity_kind", "entity_id", "prop_key")
_CPG_PROPS_MAP_KEYS = ("entity_kind", "entity_id")
_CPG_EDGES_BY_SRC_KEYS = ("src_node_id",)
_CPG_EDGES_BY_DST_KEYS = ("dst_node_id",)


@extract_columns(*prefixed_column_names("cpg_nodes", _CPG_NODES_KEYS))
@tag_outputs(
    **semantic_column_payloads(
        "cpg_nodes",
        _CPG_NODES_KEYS,
        node_prefix=column_name_prefix("cpg_nodes"),
        kind="key",
    )
)
def cpg_nodes_key_columns(cpg_nodes: TableLike) -> pl.DataFrame:
    """Expose key columns for CPG nodes.

    Returns
    -------
    pl.DataFrame
        Key columns for CPG nodes with prefixed names.
    """
    return _prefixed_key_frame(cpg_nodes, dataset_alias="cpg_nodes", columns=_CPG_NODES_KEYS)


@extract_columns(*prefixed_column_names("cpg_edges", _CPG_EDGES_KEYS))
@tag_outputs(
    **semantic_column_payloads(
        "cpg_edges",
        _CPG_EDGES_KEYS,
        node_prefix=column_name_prefix("cpg_edges"),
        kind="key",
    )
)
def cpg_edges_key_columns(cpg_edges: TableLike) -> pl.DataFrame:
    """Expose key columns for CPG edges.

    Returns
    -------
    pl.DataFrame
        Key columns for CPG edges with prefixed names.
    """
    return _prefixed_key_frame(cpg_edges, dataset_alias="cpg_edges", columns=_CPG_EDGES_KEYS)


@extract_columns(*prefixed_column_names("cpg_props", _CPG_PROPS_KEYS))
@tag_outputs(
    **semantic_column_payloads(
        "cpg_props",
        _CPG_PROPS_KEYS,
        node_prefix=column_name_prefix("cpg_props"),
        kind="key",
    )
)
def cpg_props_key_columns(cpg_props: TableLike) -> pl.DataFrame:
    """Expose key columns for CPG properties.

    Returns
    -------
    pl.DataFrame
        Key columns for CPG properties with prefixed names.
    """
    return _prefixed_key_frame(cpg_props, dataset_alias="cpg_props", columns=_CPG_PROPS_KEYS)


@extract_columns(*prefixed_column_names("cpg_nodes_quality", _CPG_NODES_QUALITY_KEYS))
@tag_outputs(
    **semantic_column_payloads(
        "cpg_nodes_quality",
        _CPG_NODES_QUALITY_KEYS,
        node_prefix=column_name_prefix("cpg_nodes_quality"),
        kind="key",
    )
)
def cpg_nodes_quality_key_columns(cpg_nodes_quality: TableLike) -> pl.DataFrame:
    """Expose key columns for CPG node quality outputs.

    Returns
    -------
    pl.DataFrame
        Key columns for CPG node quality outputs with prefixed names.
    """
    return _prefixed_key_frame(
        cpg_nodes_quality,
        dataset_alias="cpg_nodes_quality",
        columns=_CPG_NODES_QUALITY_KEYS,
    )


@extract_columns(*prefixed_column_names("cpg_props_quality", _CPG_PROPS_QUALITY_KEYS))
@tag_outputs(
    **semantic_column_payloads(
        "cpg_props_quality",
        _CPG_PROPS_QUALITY_KEYS,
        node_prefix=column_name_prefix("cpg_props_quality"),
        kind="key",
    )
)
def cpg_props_quality_key_columns(cpg_props_quality: TableLike) -> pl.DataFrame:
    """Expose key columns for CPG property quality outputs.

    Returns
    -------
    pl.DataFrame
        Key columns for CPG property quality outputs with prefixed names.
    """
    return _prefixed_key_frame(
        cpg_props_quality,
        dataset_alias="cpg_props_quality",
        columns=_CPG_PROPS_QUALITY_KEYS,
    )


@extract_columns(*prefixed_column_names("cpg_props_map", _CPG_PROPS_MAP_KEYS))
@tag_outputs(
    **semantic_column_payloads(
        "cpg_props_map",
        _CPG_PROPS_MAP_KEYS,
        node_prefix=column_name_prefix("cpg_props_map"),
        kind="key",
    )
)
def cpg_props_map_key_columns(cpg_props_map: TableLike) -> pl.DataFrame:
    """Expose key columns for CPG property maps.

    Returns
    -------
    pl.DataFrame
        Key columns for CPG property maps with prefixed names.
    """
    return _prefixed_key_frame(
        cpg_props_map,
        dataset_alias="cpg_props_map",
        columns=_CPG_PROPS_MAP_KEYS,
    )


@extract_columns(*prefixed_column_names("cpg_edges_by_src", _CPG_EDGES_BY_SRC_KEYS))
@tag_outputs(
    **semantic_column_payloads(
        "cpg_edges_by_src",
        _CPG_EDGES_BY_SRC_KEYS,
        node_prefix=column_name_prefix("cpg_edges_by_src"),
        kind="key",
    )
)
def cpg_edges_by_src_key_columns(cpg_edges_by_src: TableLike) -> pl.DataFrame:
    """Expose key columns for CPG edges-by-source outputs.

    Returns
    -------
    pl.DataFrame
        Key columns for CPG edges-by-source outputs with prefixed names.
    """
    return _prefixed_key_frame(
        cpg_edges_by_src,
        dataset_alias="cpg_edges_by_src",
        columns=_CPG_EDGES_BY_SRC_KEYS,
    )


@extract_columns(*prefixed_column_names("cpg_edges_by_dst", _CPG_EDGES_BY_DST_KEYS))
@tag_outputs(
    **semantic_column_payloads(
        "cpg_edges_by_dst",
        _CPG_EDGES_BY_DST_KEYS,
        node_prefix=column_name_prefix("cpg_edges_by_dst"),
        kind="key",
    )
)
def cpg_edges_by_dst_key_columns(cpg_edges_by_dst: TableLike) -> pl.DataFrame:
    """Expose key columns for CPG edges-by-destination outputs.

    Returns
    -------
    pl.DataFrame
        Key columns for CPG edges-by-destination outputs with prefixed names.
    """
    return _prefixed_key_frame(
        cpg_edges_by_dst,
        dataset_alias="cpg_edges_by_dst",
        columns=_CPG_EDGES_BY_DST_KEYS,
    )


__all__ = [
    "cpg_edges_by_dst_key_columns",
    "cpg_edges_by_src_key_columns",
    "cpg_edges_key_columns",
    "cpg_nodes_key_columns",
    "cpg_nodes_quality_key_columns",
    "cpg_props_key_columns",
    "cpg_props_map_key_columns",
    "cpg_props_quality_key_columns",
]
