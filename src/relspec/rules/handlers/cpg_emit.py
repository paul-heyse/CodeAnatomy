"""CPG emission rule handlers for nodes, edges, and props."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from cpg.emit_edges_ibis import emit_edges_ibis
from cpg.specs import EdgePlanSpec, NodePlanSpec, PropFieldSpec, PropTableSpec
from ibis_engine.plan import IbisPlan
from relspec.cpg.emit_nodes_ibis import emit_nodes_ibis
from relspec.cpg.emit_props_ibis import emit_props_fast, emit_props_json


@dataclass(frozen=True)
class NodeEmitRuleHandler:
    """Emit node plans from Ibis sources."""

    @staticmethod
    def compile_ibis(plan: IbisPlan, *, spec: NodePlanSpec) -> IbisPlan:
        """Return an Ibis node emission plan.

        Returns
        -------
        IbisPlan
            Ibis plan emitting node rows for the spec.
        """
        return emit_nodes_ibis(plan, spec=spec.emit)


@dataclass(frozen=True)
class EdgeEmitRuleHandler:
    """Emit edge plans from relationship outputs."""

    @staticmethod
    def compile_ibis(
        plan: IbisPlan,
        *,
        spec: EdgePlanSpec,
        include_keys: bool,
    ) -> IbisPlan:
        """Return an Ibis edge emission plan.

        Returns
        -------
        IbisPlan
            Ibis plan emitting edge rows for the spec.
        """
        return emit_edges_ibis(plan, spec=spec.emit, include_keys=include_keys)


@dataclass(frozen=True)
class PropEmitRuleHandler:
    """Emit property plans from node or edge tables."""

    @staticmethod
    def compile_fast_ibis(
        plan: IbisPlan,
        *,
        spec: PropTableSpec,
        fields: list[PropFieldSpec],
        schema_version: int | None,
    ) -> list[IbisPlan]:
        """Return Ibis plans for fast-lane property fields.

        Returns
        -------
        list[IbisPlan]
            Ibis plans for fast-lane property fields.
        """
        return emit_props_fast(plan, spec=spec, fields=fields, schema_version=schema_version)

    @staticmethod
    def compile_json_ibis(
        plan: IbisPlan,
        *,
        spec: PropTableSpec,
        fields: list[PropFieldSpec],
        schema_version: int | None,
        schema: pa.Schema,
    ) -> list[IbisPlan]:
        """Return Ibis plans for JSON-heavy property fields.

        Returns
        -------
        list[IbisPlan]
            Ibis plans for JSON-heavy property fields.
        """
        return emit_props_json(
            plan,
            spec=spec,
            fields=fields,
            schema_version=schema_version,
            schema=schema,
        )


__all__ = ["EdgeEmitRuleHandler", "NodeEmitRuleHandler", "PropEmitRuleHandler"]
