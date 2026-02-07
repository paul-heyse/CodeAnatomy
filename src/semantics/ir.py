"""Semantic intermediate representation (IR) for compile/optimize/emit pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from semantics.view_kinds import ViewKindStr

if TYPE_CHECKING:
    from cpg.specs import NodePlanSpec, PropTableSpec
    from semantics.catalog.dataset_rows import SemanticDatasetRow
    from semantics.quality import JoinHow

# Backward-compatible alias.  ``SemanticIRKind`` is the canonical Literal
# type for view kind annotations; ``ViewKindStr`` is the single source of
# truth defined in ``semantics.view_kinds``.
SemanticIRKind = ViewKindStr

GraphPosition = Literal[
    "source",
    "intermediate",
    "terminal",
    "high_fan_out",
]
"""View position within the IR dependency graph.

source
    No upstream dependencies (input views).
intermediate
    Has both upstream and downstream dependencies.
terminal
    No downstream consumers in the IR.
high_fan_out
    Has three or more downstream consumers.
"""


@dataclass(frozen=True)
class InferredViewProperties:
    """Properties inferred from schemas and graph topology during the infer phase.

    All fields are optional to support graceful degradation: if inference
    fails for any property, it is left as None and the view continues to
    work with its original static declarations.

    Attributes:
    ----------
    inferred_join_strategy
        Join strategy type inferred from upstream schema metadata
        (e.g., ``"span_overlap"``, ``"foreign_key"``).  None when the
        view is not a join/relate kind or when inference is inconclusive.
    inferred_join_keys
        Join key column pairs inferred from schema compatibility groups.
        Each element is a ``(left_col, right_col)`` tuple.  None when no
        compatible keys can be determined.
    inferred_cache_policy
        Cache policy hint derived from the view's position in the
        dependency graph.  None when no heuristic applies.
    graph_position
        Topological position of the view in the IR dependency graph.
        None when position cannot be determined.
    """

    inferred_join_strategy: str | None = None
    inferred_join_keys: tuple[tuple[str, str], ...] | None = None
    inferred_cache_policy: str | None = None
    graph_position: GraphPosition | None = None


@dataclass(frozen=True)
class SemanticIRView:
    """Single IR node representing a semantic view definition."""

    name: str
    kind: SemanticIRKind
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    inferred_properties: InferredViewProperties | None = None


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
    "GraphPosition",
    "InferredViewProperties",
    "SemanticIR",
    "SemanticIRJoinGroup",
    "SemanticIRKind",
    "SemanticIRView",
]
