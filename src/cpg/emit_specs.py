"""Shared CPG emission constants for DataFusion builders."""

from __future__ import annotations

from dataclasses import dataclass

_NODE_OUTPUT_COLUMNS: tuple[str, ...] = (
    "node_id",
    "node_kind",
    "path",
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


@dataclass(frozen=True)
class CpgPropOptions:
    """Default property include options for CPG builders."""

    include_heavy_json_props: bool = False


__all__ = [
    "_EDGE_OUTPUT_COLUMNS",
    "_NODE_OUTPUT_COLUMNS",
    "_PROP_OUTPUT_COLUMNS",
    "CpgPropOptions",
]
