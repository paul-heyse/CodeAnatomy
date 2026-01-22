"""Edge plan spec helpers for CPG relationship rules."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from cpg.kind_catalog import EdgeKindId
from cpg.specs import EdgeEmitSpec, EdgePlanSpec
from relspec.rules.definitions import RelationshipPayload, RuleDefinition
from relspec.rules.spec_tables import rule_definitions_from_table


def edge_plan_specs_from_definitions(
    definitions: Sequence[RuleDefinition],
) -> tuple[EdgePlanSpec, ...]:
    """Return edge plan specs from canonical rule definitions.

    Returns
    -------
    tuple[EdgePlanSpec, ...]
        Edge plan specs derived from edge emit payloads.
    """
    specs: list[EdgePlanSpec] = []
    for definition in definitions:
        payload = definition.payload
        if not isinstance(payload, RelationshipPayload):
            continue
        edge = payload.edge_emit
        if edge is None:
            continue
        emit = EdgeEmitSpec(
            edge_kind=EdgeKindId(edge.edge_kind),
            src_cols=edge.src_cols,
            dst_cols=edge.dst_cols,
            path_cols=edge.path_cols,
            bstart_cols=edge.bstart_cols,
            bend_cols=edge.bend_cols,
            origin=edge.origin,
            default_resolution_method=edge.resolution_method,
        )
        specs.append(
            EdgePlanSpec(
                name=definition.name,
                option_flag=edge.option_flag,
                relation_ref=definition.output,
                emit=emit,
            )
        )
    return tuple(specs)


def edge_plan_specs_from_table(table: pa.Table) -> tuple[EdgePlanSpec, ...]:
    """Return edge plan specs decoded from a canonical rule table.

    Returns
    -------
    tuple[EdgePlanSpec, ...]
        Edge plan specs derived from the canonical rule table.
    """
    definitions = rule_definitions_from_table(table)
    return edge_plan_specs_from_definitions(definitions)


__all__ = [
    "edge_plan_specs_from_definitions",
    "edge_plan_specs_from_table",
]
