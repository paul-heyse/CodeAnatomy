"""CPG package for schemas, registries, and derivations."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cpg.view_builders_df import (
        build_cpg_edges_by_dst_df,
        build_cpg_edges_by_src_df,
        build_cpg_edges_df,
        build_cpg_nodes_df,
        build_cpg_props_df,
        build_cpg_props_map_df,
    )

__all__ = [
    "build_cpg_edges_by_dst_df",
    "build_cpg_edges_by_src_df",
    "build_cpg_edges_df",
    "build_cpg_nodes_df",
    "build_cpg_props_df",
    "build_cpg_props_map_df",
]


def __getattr__(name: str) -> object:
    """Lazy-load DataFusion-native CPG builders.

    Args:
        name: Description.

    Returns:
        object: Result.

    Raises:
        AttributeError: If the operation cannot be completed.
    """
    if name in {
        "build_cpg_nodes_df",
        "build_cpg_edges_df",
        "build_cpg_props_df",
        "build_cpg_props_map_df",
        "build_cpg_edges_by_src_df",
        "build_cpg_edges_by_dst_df",
    }:
        from cpg import view_builders_df

        value = getattr(view_builders_df, name)
        globals()[name] = value
        return value
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
