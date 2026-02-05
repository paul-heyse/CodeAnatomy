"""Canonical output naming policy for semantic views.

This module provides a single source of truth for semantic output names.
Canonical names are now identical to internal view names (no version suffixes).
"""

from __future__ import annotations

from typing import Final

# Canonical mapping from internal names to output names (identity).
SEMANTIC_OUTPUT_NAMES: Final[dict[str, str]] = {
    # Normalized extraction tables
    "scip_occurrences_norm": "scip_occurrences_norm",
    "cst_refs_norm": "cst_refs_norm",
    "cst_defs_norm": "cst_defs_norm",
    "cst_imports_norm": "cst_imports_norm",
    "cst_calls_norm": "cst_calls_norm",
    "cst_call_args_norm": "cst_call_args_norm",
    "cst_docstrings_norm": "cst_docstrings_norm",
    "cst_decorators_norm": "cst_decorators_norm",
    # Relationship views (semantic joins)
    "rel_name_symbol": "rel_name_symbol",
    "rel_def_symbol": "rel_def_symbol",
    "rel_import_symbol": "rel_import_symbol",
    "rel_callsite_symbol": "rel_callsite_symbol",
    # Semantic union layer outputs
    "semantic_nodes_union": "semantic_nodes_union",
    "semantic_edges_union": "semantic_edges_union",
    # Final CPG outputs
    "cpg_nodes": "cpg_nodes",
    "cpg_edges": "cpg_edges",
    "cpg_props": "cpg_props",
    "cpg_props_map": "cpg_props_map",
    "cpg_edges_by_src": "cpg_edges_by_src",
    "cpg_edges_by_dst": "cpg_edges_by_dst",
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
        The canonical output name.
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
        The canonical output name.

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
    "SEMANTIC_OUTPUT_NAMES",
    "canonical_output_name",
    "internal_name",
    "is_semantic_output",
]
