"""Semantic intermediate representation (IR) for compile/optimize/emit pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from cpg.specs import NodePlanSpec, PropTableSpec
    from semantics.catalog.dataset_rows import SemanticDatasetRow
    from semantics.quality import JoinHow

SemanticIRKind = Literal[
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


@dataclass(frozen=True)
class SemanticIRView:
    """Single IR node representing a semantic view definition."""

    name: str
    kind: SemanticIRKind
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]


@dataclass(frozen=True)
class SemanticIRJoinGroup:
    """Join-fusion group with shared join parameters."""

    name: str
    left_view: str
    right_view: str
    left_on: tuple[str, ...]
    right_on: tuple[str, ...]
    how: JoinHow
    relationship_names: tuple[str, ...]


@dataclass(frozen=True)
class SemanticIR:
    """Semantic IR container."""

    views: tuple[SemanticIRView, ...]
    dataset_rows: tuple[SemanticDatasetRow, ...] = ()
    cpg_node_specs: tuple[NodePlanSpec, ...] = ()
    cpg_prop_specs: tuple[PropTableSpec, ...] = ()
    join_groups: tuple[SemanticIRJoinGroup, ...] = ()
    model_hash: str | None = None
    ir_hash: str | None = None


__all__ = [
    "SemanticIR",
    "SemanticIRJoinGroup",
    "SemanticIRKind",
    "SemanticIRView",
]
