"""Compile/optimize/emit pipeline for the semantic IR."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from semantics.ir import SemanticIR, SemanticIRJoinGroup, SemanticIRView
from semantics.naming import canonical_output_name
from semantics.registry import SEMANTIC_MODEL, SemanticModel
from utils.hashing import hash64_from_text

if TYPE_CHECKING:
    from semantics.quality import JoinHow, QualityRelationshipSpec

_KIND_ORDER: Mapping[str, int] = {
    "normalize": 0,
    "scip_normalize": 1,
    "join_group": 2,
    "relate": 3,
    "union_edges": 4,
    "union_nodes": 5,
    "projection": 6,
    "diagnostic": 7,
    "export": 8,
    "finalize": 9,
    "artifact": 10,
}
_MIN_JOIN_GROUP_SIZE = 2


def _join_group_name(
    *,
    left_view: str,
    right_view: str,
    left_on: tuple[str, ...],
    right_on: tuple[str, ...],
    how: JoinHow,
) -> str:
    key_parts = (
        left_view,
        right_view,
        how,
        ",".join(left_on),
        ",".join(right_on),
    )
    signature = "|".join(key_parts)
    digest = hash64_from_text(signature)
    return f"join_{left_view}__{right_view}__{digest:x}"


def _build_join_groups(
    views: Sequence[SemanticIRView],
    relationship_specs: Mapping[str, QualityRelationshipSpec],
    *,
    existing_names: set[str],
) -> tuple[tuple[SemanticIRJoinGroup, ...], tuple[SemanticIRView, ...]]:
    grouped: dict[tuple[str, str, tuple[str, ...], tuple[str, ...], JoinHow], list[str]] = {}
    for view in views:
        if view.kind != "relate":
            continue
        rel_spec = relationship_specs.get(view.name)
        if rel_spec is None:
            continue
        left_on = tuple(rel_spec.left_on)
        right_on = tuple(rel_spec.right_on)
        if not left_on or not right_on:
            continue
        key = (
            rel_spec.left_view,
            rel_spec.right_view,
            left_on,
            right_on,
            rel_spec.how,
        )
        grouped.setdefault(key, []).append(view.name)

    join_groups: list[SemanticIRJoinGroup] = []
    join_group_views: list[SemanticIRView] = []
    for (left_view, right_view, left_on, right_on, how), names in grouped.items():
        if len(names) < _MIN_JOIN_GROUP_SIZE:
            continue
        group_name = _join_group_name(
            left_view=left_view,
            right_view=right_view,
            left_on=left_on,
            right_on=right_on,
            how=how,
        )
        if group_name in existing_names:
            msg = f"Join group name collision for {group_name!r}."
            raise ValueError(msg)
        existing_names.add(group_name)
        join_groups.append(
            SemanticIRJoinGroup(
                name=group_name,
                left_view=left_view,
                right_view=right_view,
                left_on=left_on,
                right_on=right_on,
                how=how,
                relationship_names=tuple(names),
            )
        )
        join_group_views.append(
            SemanticIRView(
                name=group_name,
                kind="join_group",
                inputs=(left_view, right_view),
                outputs=(group_name,),
            )
        )

    return tuple(join_groups), tuple(join_group_views)


def compile_semantics(model: SemanticModel) -> SemanticIR:
    """Compile a semantic model into the IR.

    Parameters
    ----------
    model
        Semantic model describing normalization and relationships.

    Returns
    -------
    SemanticIR
        Compiled semantic IR.
    """
    views = [
        SemanticIRView(
            name=spec.output_name,
            kind="normalize",
            inputs=(spec.source_table,),
            outputs=(spec.output_name,),
        )
        for spec in model.normalization_specs
    ]
    scip_norm = canonical_output_name("scip_occurrences_norm")
    views.append(
        SemanticIRView(
            name=scip_norm,
            kind="scip_normalize",
            inputs=("scip_occurrences", "file_line_index_v1"),
            outputs=(scip_norm,),
        )
    )
    views.extend(
        SemanticIRView(
            name=spec.name,
            kind="relate",
            inputs=(spec.left_view, spec.right_view),
            outputs=(spec.name,),
        )
        for spec in model.relationship_specs
    )
    views.append(
        SemanticIRView(
            name=model.semantic_edges_union_name,
            kind="union_edges",
            inputs=tuple(spec.name for spec in model.relationship_specs),
            outputs=(model.semantic_edges_union_name,),
        )
    )
    views.append(
        SemanticIRView(
            name=model.semantic_nodes_union_name,
            kind="union_nodes",
            inputs=model.normalization_output_names(include_nodes_only=True),
            outputs=(model.semantic_nodes_union_name,),
        )
    )
    return SemanticIR(views=tuple(views))


def optimize_semantics(ir: SemanticIR) -> SemanticIR:
    """Optimize the semantic IR with join fusion grouping and deduplication.

    Returns
    -------
    SemanticIR
        Optimized semantic IR with join fusion group metadata.

    Raises
    ------
    ValueError
        Raised when duplicate IR views conflict on definition.
    """
    seen: dict[str, SemanticIRView] = {}
    for view in ir.views:
        prior = seen.get(view.name)
        if prior is None:
            seen[view.name] = view
            continue
        if prior != view:
            msg = f"Duplicate semantic IR view with different definitions: {view.name!r}."
            raise ValueError(msg)
    base_views = list(seen.values())
    relationship_specs = {spec.name: spec for spec in SEMANTIC_MODEL.relationship_specs}
    join_groups, join_group_views = _build_join_groups(
        base_views,
        relationship_specs,
        existing_names=set(seen),
    )

    ordered = sorted(
        (*base_views, *join_group_views),
        key=lambda view: (
            _KIND_ORDER.get(view.kind, 99),
            view.inputs if view.kind in {"relate", "join_group"} else (),
            view.name,
        ),
    )
    return SemanticIR(
        views=tuple(ordered),
        dataset_rows=ir.dataset_rows,
        cpg_node_specs=ir.cpg_node_specs,
        cpg_prop_specs=ir.cpg_prop_specs,
        join_groups=join_groups,
    )


def emit_semantics(ir: SemanticIR) -> SemanticIR:
    """Emit semantic IR artifacts (placeholder).

    Returns
    -------
    SemanticIR
        Emitted semantic IR artifacts.
    """
    return SemanticIR(
        views=ir.views,
        dataset_rows=SEMANTIC_MODEL.dataset_rows(),
        cpg_node_specs=SEMANTIC_MODEL.cpg_node_specs(),
        cpg_prop_specs=SEMANTIC_MODEL.cpg_prop_specs(),
        join_groups=ir.join_groups,
    )


def build_semantic_ir() -> SemanticIR:
    """Build the end-to-end semantic IR pipeline.

    Returns
    -------
    SemanticIR
        Semantic IR after compile/optimize/emit.
    """
    compiled = compile_semantics(SEMANTIC_MODEL)
    optimized = optimize_semantics(compiled)
    return emit_semantics(optimized)


__all__ = [
    "build_semantic_ir",
    "compile_semantics",
    "emit_semantics",
    "optimize_semantics",
]
