"""Unified registry for CPG node/prop family specs."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from cpg.constants import ROLE_FLAG_SPECS
from cpg.kind_catalog import EntityKind
from cpg.specs import (
    TRANSFORM_FLAG_TO_BOOL,
    NodePlanSpec,
    PropFieldSpec,
    PropTableSpec,
)

if TYPE_CHECKING:
    from semantics.cpg_entity_specs import CpgEntitySpec
    from semantics.registry import SemanticModel


def _entity_specs(model: SemanticModel) -> tuple[CpgEntitySpec, ...]:
    return model.cpg_entity_specs()


def build_node_plan_specs(model: SemanticModel) -> tuple[NodePlanSpec, ...]:
    """Build node plan specs for all node-emitting entities.

    Returns:
    -------
    tuple[NodePlanSpec, ...]
        Node plan specs for configured entities.
    """
    specs: list[NodePlanSpec] = []
    for spec in _entity_specs(model):
        plan = spec.to_node_plan()
        if plan is not None:
            specs.append(plan)
    return tuple(specs)


def node_plan_specs() -> tuple[NodePlanSpec, ...]:
    """Return node plan specs for all node-emitting entities.

    Returns:
    -------
    tuple[NodePlanSpec, ...]
        Node plan specs for configured entities.
    """
    from semantics.compile_context import semantic_ir_for_outputs

    return semantic_ir_for_outputs().cpg_node_specs


def build_prop_table_specs(
    model: SemanticModel,
    *,
    source_columns_lookup: Callable[[str], Sequence[str] | None] | None = None,
) -> tuple[PropTableSpec, ...]:
    """Build prop table specs for all prop-emitting entities.

    Args:
        model: Semantic model containing entity specs.
        source_columns_lookup: Optional lookup for source columns by table name.

    Returns:
        tuple[PropTableSpec, ...]: Result.

    Raises:
        ValueError: If a prop spec cannot be converted to a prop table.
    """
    specs: list[PropTableSpec] = []
    for spec in _entity_specs(model):
        table_name = spec.prop_table or spec.node_table
        source_columns = None
        if table_name is not None and source_columns_lookup is not None:
            source_columns = source_columns_lookup(table_name)
        try:
            table = spec.to_prop_table(source_columns=source_columns)
        except ValueError as exc:
            msg = f"CPG prop spec {spec.name!r} failed for table {table_name!r}: {exc}"
            raise ValueError(msg) from exc
        if table is not None:
            specs.append(table)
    return tuple(specs)


def prop_table_specs(
    *,
    source_columns_lookup: Callable[[str], Sequence[str] | None] | None = None,
) -> tuple[PropTableSpec, ...]:
    """Return prop table specs for all prop-emitting entities.

    Returns:
    -------
    tuple[PropTableSpec, ...]
        Prop table specs for configured entities.
    """
    if source_columns_lookup is None:
        from semantics.compile_context import semantic_ir_for_outputs

        return semantic_ir_for_outputs().cpg_prop_specs
    from semantics.registry import SEMANTIC_MODEL

    return build_prop_table_specs(SEMANTIC_MODEL, source_columns_lookup=source_columns_lookup)


def scip_role_flag_prop_spec() -> PropTableSpec:
    """Return prop spec for SCIP role flag properties.

    Returns:
    -------
    PropTableSpec
        Prop table spec for role flags.
    """
    return PropTableSpec(
        name="scip_role_flags",
        table_ref="scip_role_flags",
        entity_kind=EntityKind.NODE,
        id_cols=("symbol",),
        fields=tuple(
            PropFieldSpec(
                prop_key=prop_key,
                source_col=flag_name,
                transform_id=TRANSFORM_FLAG_TO_BOOL,
                skip_if_none=True,
                value_type="bool",
            )
            for flag_name, _, prop_key in ROLE_FLAG_SPECS
        ),
    )


def edge_prop_spec() -> PropTableSpec:
    """Return prop spec for edge metadata properties.

    Returns:
    -------
    PropTableSpec
        Prop table spec for edge metadata.
    """
    return PropTableSpec(
        name="edge_props",
        table_ref="cpg_edges",
        entity_kind=EntityKind.EDGE,
        id_cols=("edge_id",),
        fields=(
            PropFieldSpec(prop_key="edge_kind", source_col="edge_kind", value_type="string"),
            PropFieldSpec(prop_key="origin", source_col="origin", value_type="string"),
            PropFieldSpec(
                prop_key="resolution_method", source_col="resolution_method", value_type="string"
            ),
            PropFieldSpec(prop_key="confidence", source_col="confidence", value_type="float"),
            PropFieldSpec(prop_key="score", source_col="score", value_type="float"),
            PropFieldSpec(prop_key="symbol_roles", source_col="symbol_roles", value_type="int"),
            PropFieldSpec(prop_key="qname_source", source_col="qname_source", value_type="string"),
            PropFieldSpec(
                prop_key="ambiguity_group_id", source_col="ambiguity_group_id", value_type="string"
            ),
            PropFieldSpec(prop_key="task_name", source_col="task_name", value_type="string"),
            PropFieldSpec(prop_key="task_priority", source_col="task_priority", value_type="int"),
        ),
    )


__all__ = [
    "ROLE_FLAG_SPECS",
    "build_node_plan_specs",
    "build_prop_table_specs",
    "edge_prop_spec",
    "node_plan_specs",
    "prop_table_specs",
    "scip_role_flag_prop_spec",
]
