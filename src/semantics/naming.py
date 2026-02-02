"""Canonical output naming policy for semantic views.

This module provides a single source of truth for mapping internal semantic
view names to their canonical output names with versioning suffixes. All
modules that need semantic output names should use this mapping to ensure
consistency.
"""

from __future__ import annotations

from typing import Final

OUTPUT_VERSION_SUFFIX: Final = "_v1"

# Canonical mapping from internal names to output names.
# The _v1 suffix indicates schema version for forward compatibility.
SEMANTIC_OUTPUT_NAMES: Final[dict[str, str]] = {
    # Normalized extraction tables
    "scip_occurrences_norm": "scip_occurrences_norm_v1",
    "cst_refs_norm": "cst_refs_norm_v1",
    "cst_defs_norm": "cst_defs_norm_v1",
    "cst_imports_norm": "cst_imports_norm_v1",
    "cst_calls_norm": "cst_calls_norm_v1",
    "cst_call_args_norm": "cst_call_args_norm_v1",
    "cst_docstrings_norm": "cst_docstrings_norm_v1",
    "cst_decorators_norm": "cst_decorators_norm_v1",
    # Relationship views (semantic joins)
    "rel_name_symbol": "rel_name_symbol_v1",
    "rel_def_symbol": "rel_def_symbol_v1",
    "rel_import_symbol": "rel_import_symbol_v1",
    "rel_callsite_symbol": "rel_callsite_symbol_v1",
    # Semantic union layer outputs
    "semantic_nodes_union": "semantic_nodes_union_v1",
    "semantic_edges_union": "semantic_edges_union_v1",
    # Final CPG outputs
    "cpg_nodes": "cpg_nodes_v1",
    "cpg_edges": "cpg_edges_v1",
    "cpg_props": "cpg_props_v1",
    "cpg_props_map": "cpg_props_map_v1",
    "cpg_edges_by_src": "cpg_edges_by_src_v1",
    "cpg_edges_by_dst": "cpg_edges_by_dst_v1",
}


def canonical_output_name(internal_name: str) -> str:
    """Get canonical output name for an internal view name.

    Parameters
    ----------
    internal_name
        The internal view name used in semantic pipeline code.

    Returns
    -------
    str
        The canonical output name with appropriate versioning suffix.
        Returns the internal name unchanged if no mapping exists.
    """
    canonical = SEMANTIC_OUTPUT_NAMES.get(internal_name)
    if canonical is not None:
        return canonical
    return internal_name


def internal_name(output_name: str) -> str:
    """Get internal name from a canonical output name.

    Parameters
    ----------
    output_name
        The canonical output name with versioning suffix.

    Returns
    -------
    str
        The internal view name without suffix.
        Returns the output name unchanged if no mapping exists.
    """
    # Build reverse mapping on demand
    reverse = {v: k for k, v in SEMANTIC_OUTPUT_NAMES.items()}
    return reverse.get(output_name, output_name)


def is_semantic_output(name: str) -> bool:
    """Check if a name is a known semantic output.

    Parameters
    ----------
    name
        The view name to check.

    Returns
    -------
    bool
        True if the name is a canonical semantic output name.
    """
    return name in SEMANTIC_OUTPUT_NAMES.values()


__all__ = [
    "OUTPUT_VERSION_SUFFIX",
    "SEMANTIC_OUTPUT_NAMES",
    "canonical_output_name",
    "internal_name",
    "is_semantic_output",
]
