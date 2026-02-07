"""Canonical output naming policy for semantic views.

This module provides a single source of truth for semantic output names.
Canonical names are now identical to internal view names (no version suffixes).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Mapping

# ---------------------------------------------------------------------------
# Static output name map (deprecated).
#
# This dict is a legacy bootstrap fallback.  The authoritative source of
# output names is ``SemanticProgramManifest.output_name_map``, which is
# derived from the compiled ``SemanticIR`` views at compile time.  Prefer
# passing a manifest to ``canonical_output_name()`` instead of relying on
# this static dict.
# ---------------------------------------------------------------------------
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


def canonical_output_name(
    internal_name: str,
    *,
    manifest: object | None = None,
) -> str:
    """Get canonical output name for an internal view name.

    When a manifest with a populated ``output_name_map`` is provided, the
    manifest-backed map is authoritative.  Otherwise the static
    ``SEMANTIC_OUTPUT_NAMES`` dict is consulted as a backward-compatible
    fallback.

    Parameters
    ----------
    internal_name
        The internal view name used in semantic pipeline code.
    manifest
        Optional ``SemanticProgramManifest`` whose ``output_name_map``
        takes precedence over the static dict.  Accepted as ``object``
        to keep this module free of circular imports.

    Returns:
    -------
    str
        The canonical output name.
        Returns the internal name unchanged if no mapping exists.
    """
    if manifest is not None:
        output_map: Mapping[str, str] | None = getattr(manifest, "output_name_map", None)
        if output_map is not None:
            return output_map.get(internal_name, internal_name)
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

    Returns:
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

    Returns:
    -------
    bool
        True if the name is a canonical semantic output name.
    """
    return name in SEMANTIC_OUTPUT_NAMES.values()


def output_name_map_from_views(
    views: tuple[object, ...],
) -> dict[str, str]:
    """Build an output name map from compiled SemanticIR views.

    Derive the canonical output naming map from the IR view definitions.
    Each view's ``name`` maps to itself (identity mapping), matching the
    current static convention.  The static ``SEMANTIC_OUTPUT_NAMES`` dict
    is merged as a baseline to capture any names not represented in the IR
    (e.g. CPG output names that are added after IR compilation).

    Parameters
    ----------
    views
        Tuple of ``SemanticIRView`` objects from a compiled ``SemanticIR``.

    Returns:
    -------
    dict[str, str]
        Mapping from internal view names to canonical output names.
    """
    name_map: dict[str, str] = dict(SEMANTIC_OUTPUT_NAMES)
    for view in views:
        view_name: str = getattr(view, "name", "")
        if view_name:
            name_map[view_name] = view_name
    return name_map


__all__ = [
    "SEMANTIC_OUTPUT_NAMES",
    "canonical_output_name",
    "internal_name",
    "is_semantic_output",
    "output_name_map_from_views",
]
