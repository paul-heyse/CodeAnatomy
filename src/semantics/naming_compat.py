"""Ingress-only semantic naming compatibility helpers."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Final

_OUTPUT_ALIAS_TO_CANONICAL: Final[dict[str, str]] = {
    "cpg_nodes_v1": "cpg_nodes",
    "cpg_edges_v1": "cpg_edges",
    "cpg_props_v1": "cpg_props",
    "cpg_props_map_v1": "cpg_props_map",
    "cpg_edges_by_src_v1": "cpg_edges_by_src",
    "cpg_edges_by_dst_v1": "cpg_edges_by_dst",
}


def canonicalize_output_alias(name: str) -> str:
    """Return canonical semantic output name for a potentially legacy alias."""
    return _OUTPUT_ALIAS_TO_CANONICAL.get(name, name)


def canonicalize_output_aliases(names: Iterable[str]) -> tuple[str, ...]:
    """Return canonicalized output names preserving the input order."""
    return tuple(canonicalize_output_alias(name) for name in names)


def output_alias_mapping() -> dict[str, str]:
    """Return a copy of the compatibility alias mapping."""
    return dict(_OUTPUT_ALIAS_TO_CANONICAL)


__all__ = [
    "canonicalize_output_alias",
    "canonicalize_output_aliases",
    "output_alias_mapping",
]
