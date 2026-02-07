"""Single authority for semantic IR view kinds.

Consolidate the triple definition of view kind values (``SemanticIRKind``
in ``ir.py``, ``SpecKind`` in ``spec_registry.py``, ``_KIND_ORDER`` in
``ir_pipeline.py``) into one module.

``ViewKind`` is a ``StrEnum`` so values are usable as plain strings,
maintaining backward compatibility with the previous ``Literal`` types.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final, Literal

from semantics.specs import SpanUnit
from serde_msgspec import StructBaseStrict

ViewKindStr = Literal[
    "normalize",
    "scip_normalize",
    "bytecode_line_index",
    "span_unnest",
    "symtable",
    "diagnostic",
    "export",
    "projection",
    "finalize",
    "artifact",
    "join_group",
    "relate",
    "union_nodes",
    "union_edges",
]
"""Literal type matching all view kind string values.

Use this as the annotation type for dataclass fields that store view
kind values.  It is kept in sync with ``ViewKind`` (the runtime enum).
"""


class ViewKind(StrEnum):
    """Semantic IR view kind.

    Each value corresponds to a pipeline processing stage.  Being a
    ``StrEnum`` guarantees ``ViewKind.NORMALIZE == "normalize"`` is True,
    so all existing string-keyed dispatch tables work without changes.
    """

    NORMALIZE = "normalize"
    SCIP_NORMALIZE = "scip_normalize"
    BYTECODE_LINE_INDEX = "bytecode_line_index"
    SPAN_UNNEST = "span_unnest"
    SYMTABLE = "symtable"
    DIAGNOSTIC = "diagnostic"
    EXPORT = "export"
    PROJECTION = "projection"
    FINALIZE = "finalize"
    ARTIFACT = "artifact"
    JOIN_GROUP = "join_group"
    RELATE = "relate"
    UNION_NODES = "union_nodes"
    UNION_EDGES = "union_edges"


VIEW_KIND_ORDER: Final[dict[str, int]] = {
    ViewKind.NORMALIZE: 0,
    ViewKind.SCIP_NORMALIZE: 1,
    ViewKind.BYTECODE_LINE_INDEX: 2,
    ViewKind.SPAN_UNNEST: 2,
    ViewKind.SYMTABLE: 2,
    ViewKind.JOIN_GROUP: 3,
    ViewKind.RELATE: 4,
    ViewKind.UNION_EDGES: 5,
    ViewKind.UNION_NODES: 6,
    ViewKind.PROJECTION: 7,
    ViewKind.DIAGNOSTIC: 8,
    ViewKind.EXPORT: 9,
    ViewKind.FINALIZE: 10,
    ViewKind.ARTIFACT: 11,
}
"""Execution ordering for IR views.

Lower values run first.  Views sharing the same ordinal execute in an
unspecified order within that tier.
"""


# ---------------------------------------------------------------------------
# Target consolidation mapping (14 -> 6 for future phases)
# ---------------------------------------------------------------------------

ConsolidatedKind = Literal[
    "normalize",
    "derive",
    "relate",
    "union",
    "project",
    "diagnostic",
]
"""Target consolidated kind values for the eventual 14 -> 6 collapse."""

CONSOLIDATED_KIND: Final[dict[str, ConsolidatedKind]] = {
    ViewKind.NORMALIZE: "normalize",
    ViewKind.SCIP_NORMALIZE: "normalize",
    ViewKind.BYTECODE_LINE_INDEX: "normalize",
    ViewKind.SPAN_UNNEST: "normalize",
    ViewKind.SYMTABLE: "derive",
    ViewKind.JOIN_GROUP: "relate",
    ViewKind.RELATE: "relate",
    ViewKind.UNION_EDGES: "union",
    ViewKind.UNION_NODES: "union",
    ViewKind.PROJECTION: "project",
    ViewKind.EXPORT: "project",
    ViewKind.FINALIZE: "project",
    ViewKind.DIAGNOSTIC: "diagnostic",
    ViewKind.ARTIFACT: "project",
}
"""Map each current kind to its eventual consolidated category."""


# ---------------------------------------------------------------------------
# Parameterization struct for future consolidated kinds
# ---------------------------------------------------------------------------


class ViewKindParams(StructBaseStrict, frozen=True):
    """Parameterization for consolidated view kinds.

    Enable distinguishing between view kinds that share a consolidated
    category (e.g. normalize vs scip_normalize) when the consolidation
    eventually collapses 14 kinds to 6.

    Attributes:
    ----------
    span_unit
        Span unit for normalization views.
    structure
        Structural shape hint (flat or nested).
    derivation_spec
        Derivation spec name for derive-category views.
    union_target
        Whether a union view targets nodes or edges.
    output_contract
        Optional output contract identifier.
    """

    span_unit: SpanUnit = "byte"
    structure: Literal["flat", "nested"] = "flat"
    derivation_spec: str | None = None
    union_target: Literal["nodes", "edges"] | None = None
    output_contract: str | None = None


__all__ = [
    "CONSOLIDATED_KIND",
    "VIEW_KIND_ORDER",
    "ConsolidatedKind",
    "ViewKind",
    "ViewKindParams",
    "ViewKindStr",
]
