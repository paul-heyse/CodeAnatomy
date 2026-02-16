"""Canonical output naming policy for semantic views.

This module provides a single source of truth for semantic output names.
Canonical names are now identical to internal view names (no version suffixes).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


class HasOutputNameMap(Protocol):
    """Protocol for manifest-like objects exposing output name mappings."""

    @property
    def output_name_map(self) -> Mapping[str, str] | None:
        """Return mapping from internal names to canonical output names."""
        ...


class HasName(Protocol):
    """Protocol for IR-like objects exposing a ``name`` attribute."""

    @property
    def name(self) -> str:
        """Return canonical identifier for a semantic view-like object."""
        ...


def canonical_output_name(
    internal_name: str,
    *,
    manifest: HasOutputNameMap | None = None,
) -> str:
    """Get canonical output name for an internal view name.

    When a manifest with a populated ``output_name_map`` is provided, the
    manifest-backed map is authoritative.  Otherwise the internal name is
    returned unchanged.

    Parameters
    ----------
    internal_name
        The internal view name used in semantic pipeline code.
    manifest
        Optional ``SemanticProgramManifest`` whose ``output_name_map`` is
        used when present. Accepted as ``object`` to keep this module free
        of circular imports.

    Returns:
    -------
    str
        The canonical output name.
        Returns the internal name unchanged if no mapping exists.
    """
    if manifest is not None and manifest.output_name_map is not None:
        return manifest.output_name_map.get(internal_name, internal_name)
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
    return output_name


def output_name_map_from_views(
    views: Sequence[HasName],
) -> dict[str, str]:
    """Build an output name map from compiled SemanticIR views.

    Derive the canonical output naming map from IR view definitions.
    Each view's ``name`` maps to itself (identity mapping).

    Parameters
    ----------
    views
        Tuple of ``SemanticIRView`` objects from a compiled ``SemanticIR``.

    Returns:
    -------
    dict[str, str]
        Mapping from internal view names to canonical output names.
    """
    name_map: dict[str, str] = {}
    for view in views:
        if view.name:
            name_map[view.name] = view.name
    return name_map


__all__ = [
    "HasName",
    "HasOutputNameMap",
    "canonical_output_name",
    "internal_name",
    "output_name_map_from_views",
]
