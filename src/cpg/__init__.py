"""CPG package for schemas, registries, and derivations."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cpg.view_builders_df import (
        build_cpg_edges_df,
        build_cpg_nodes_df,
        build_cpg_props_df,
    )

__all__ = [
    "build_cpg_edges_df",
    "build_cpg_nodes_df",
    "build_cpg_props_df",
]


def __getattr__(name: str) -> object:
    """Lazy-load DataFusion-native CPG builders.

    Returns
    -------
    object
        The requested attribute.

    Raises
    ------
    AttributeError
        If the attribute is not found.
    """
    if name in {"build_cpg_nodes_df", "build_cpg_edges_df", "build_cpg_props_df"}:
        from cpg import view_builders_df

        value = getattr(view_builders_df, name)
        globals()[name] = value
        return value
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
