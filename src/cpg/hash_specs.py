"""Hash spec registry for CPG ID generation."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from arrowdsl.core.ids_registry import hash_spec

if TYPE_CHECKING:
    from arrowdsl.core.ids import HashSpec

EDGE_ID_BASE = hash_spec(
    prefix="edge",
    cols=("src", "dst"),
)
EDGE_ID_SPAN = hash_spec(
    prefix="edge",
    cols=("src", "dst", "path", "bstart", "bend"),
)


def edge_hash_specs(edge_kind: str) -> tuple[HashSpec, HashSpec]:
    """Return base + span hash specs with the edge kind literal applied.

    Returns
    -------
    tuple[HashSpec, HashSpec]
        Base and span hash specifications for edge IDs.
    """
    extra = (edge_kind,) if edge_kind else ()
    return (
        replace(EDGE_ID_BASE, extra_literals=EDGE_ID_BASE.extra_literals + extra),
        replace(EDGE_ID_SPAN, extra_literals=EDGE_ID_SPAN.extra_literals + extra),
    )


__all__ = ["EDGE_ID_BASE", "EDGE_ID_SPAN", "edge_hash_specs"]
