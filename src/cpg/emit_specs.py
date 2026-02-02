"""Shared CPG emission constants for DataFusion builders."""

from __future__ import annotations

from dataclasses import dataclass

_NODE_BUNDLE_COLUMNS: tuple[str, ...] = ("path", "span", "bstart", "bend", "file_id")
_EDGE_BUNDLE_COLUMNS: tuple[str, ...] = ("path", "span", "bstart", "bend")

_NODE_OUTPUT_COLUMNS: tuple[str, ...] = (
    "node_id",
    "node_kind",
    "path",
    "span",
    "bstart",
    "bend",
    "file_id",
    "task_name",
    "task_priority",
)

_EDGE_OUTPUT_COLUMNS: tuple[str, ...] = (
    "edge_id",
    "edge_kind",
    "src_node_id",
    "dst_node_id",
    "path",
    "span",
    "bstart",
    "bend",
    "origin",
    "resolution_method",
    "confidence",
    "score",
    "symbol_roles",
    "qname_source",
    "ambiguity_group_id",
    "task_name",
    "task_priority",
)

_PROP_OUTPUT_COLUMNS: tuple[str, ...] = (
    "entity_kind",
    "entity_id",
    "node_kind",
    "prop_key",
    "value_type",
    "value_string",
    "value_int",
    "value_float",
    "value_bool",
    "value_json",
    "task_name",
    "task_priority",
)

_NODE_OUTPUT_FIELDS: tuple[str, ...] = tuple(
    name for name in _NODE_OUTPUT_COLUMNS if name not in _NODE_BUNDLE_COLUMNS
)
_EDGE_OUTPUT_FIELDS: tuple[str, ...] = tuple(
    name for name in _EDGE_OUTPUT_COLUMNS if name not in _EDGE_BUNDLE_COLUMNS
)


@dataclass(frozen=True)
class CpgOutputSpec:
    """Declarative spec for CPG output datasets."""

    name: str
    fields: tuple[str, ...]
    bundles: tuple[str, ...]
    partition_cols: tuple[str, ...]
    merge_keys: tuple[str, ...]
    join_keys: tuple[str, ...]
    entity: str
    grain: str
    template: str
    view_builder: str
    source_dataset: str | None = None


def cpg_output_specs() -> tuple[CpgOutputSpec, ...]:
    """Return the canonical CPG output specs.

    Returns
    -------
    tuple[CpgOutputSpec, ...]
        CPG output specs for semantic dataset row derivation.
    """
    return (
        CpgOutputSpec(
            name="cpg_nodes",
            fields=_NODE_OUTPUT_FIELDS,
            bundles=("file_identity", "span"),
            partition_cols=(),
            merge_keys=("node_id",),
            join_keys=("node_id",),
            entity="node",
            grain="per_node",
            template="cpg_output",
            view_builder="cpg_nodes_df_builder",
            source_dataset=None,
        ),
        CpgOutputSpec(
            name="cpg_edges",
            fields=_EDGE_OUTPUT_FIELDS,
            bundles=("file_identity", "span"),
            partition_cols=(),
            merge_keys=("edge_id",),
            join_keys=("edge_id",),
            entity="edge",
            grain="per_edge",
            template="cpg_output",
            view_builder="cpg_edges_df_builder",
            source_dataset=None,
        ),
        CpgOutputSpec(
            name="cpg_props",
            fields=_PROP_OUTPUT_COLUMNS,
            bundles=("file_identity",),
            partition_cols=(),
            merge_keys=("entity_kind", "entity_id", "prop_key"),
            join_keys=("entity_kind", "entity_id", "prop_key"),
            entity="prop",
            grain="per_prop",
            template="cpg_output",
            view_builder="cpg_props_df_builder",
            source_dataset=None,
        ),
        CpgOutputSpec(
            name="cpg_props_map",
            fields=("entity_kind", "entity_id", "node_kind", "props"),
            bundles=(),
            partition_cols=(),
            merge_keys=("entity_kind", "entity_id"),
            join_keys=("entity_kind", "entity_id"),
            entity="prop",
            grain="per_entity",
            template="cpg_adjacency",
            view_builder="cpg_props_map_df_builder",
            source_dataset="cpg_props",
        ),
        CpgOutputSpec(
            name="cpg_edges_by_src",
            fields=("src_node_id", "edges"),
            bundles=(),
            partition_cols=(),
            merge_keys=("src_node_id",),
            join_keys=("src_node_id",),
            entity="edge",
            grain="per_node",
            template="cpg_adjacency",
            view_builder="cpg_edges_by_src_df_builder",
            source_dataset="cpg_edges",
        ),
        CpgOutputSpec(
            name="cpg_edges_by_dst",
            fields=("dst_node_id", "edges"),
            bundles=(),
            partition_cols=(),
            merge_keys=("dst_node_id",),
            join_keys=("dst_node_id",),
            entity="edge",
            grain="per_node",
            template="cpg_adjacency",
            view_builder="cpg_edges_by_dst_df_builder",
            source_dataset="cpg_edges",
        ),
    )


@dataclass(frozen=True)
class CpgPropOptions:
    """Default property include options for CPG builders."""

    include_heavy_json_props: bool = False


__all__ = [
    "_EDGE_OUTPUT_COLUMNS",
    "_EDGE_OUTPUT_FIELDS",
    "_NODE_OUTPUT_COLUMNS",
    "_NODE_OUTPUT_FIELDS",
    "_PROP_OUTPUT_COLUMNS",
    "CpgOutputSpec",
    "CpgPropOptions",
    "cpg_output_specs",
]
