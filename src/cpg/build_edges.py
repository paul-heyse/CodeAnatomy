"""Build CPG edge tables from relationship outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import Table, TableLike
from arrowdsl.plan.plan import Plan, union_all_plans
from arrowdsl.schema.schema import EncodingSpec
from cpg.artifacts import CpgBuildArtifacts
from cpg.catalog import PlanCatalog
from cpg.emit_edges import emit_edges_plan
from cpg.plan_helpers import align_plan, empty_plan, encode_plan, finalize_plan
from cpg.quality import QualityPlanSpec, quality_plan_from_ids
from cpg.relations import EDGE_RELATION_SPECS
from cpg.schemas import CPG_EDGES_SCHEMA, CPG_EDGES_SPEC

EDGE_ENCODING_SPECS: tuple[EncodingSpec, ...] = (
    EncodingSpec(column="edge_kind"),
    EncodingSpec(column="origin"),
    EncodingSpec(column="resolution_method"),
    EncodingSpec(column="qname_source"),
    EncodingSpec(column="rule_name"),
)


@dataclass(frozen=True)
class EdgeBuildOptions:
    """Configure which edge families are emitted."""

    emit_symbol_role_edges: bool = True
    emit_scip_symbol_relationship_edges: bool = True
    emit_import_edges: bool = True
    emit_call_edges: bool = True
    emit_qname_fallback_call_edges: bool = True
    emit_diagnostic_edges: bool = True
    emit_type_edges: bool = True
    emit_runtime_edges: bool = True


@dataclass(frozen=True)
class EdgeBuildInputs:
    """Input tables for edge construction."""

    relationship_outputs: Mapping[str, TableLike] | None = None
    scip_symbol_relationships: TableLike | None = None
    diagnostics_norm: TableLike | None = None
    repo_files: TableLike | None = None
    type_exprs_norm: TableLike | None = None
    rt_signatures: TableLike | None = None
    rt_signature_params: TableLike | None = None
    rt_members: TableLike | None = None


def _edge_inputs_from_legacy(legacy: Mapping[str, object]) -> EdgeBuildInputs:
    def _maybe_table(value: object) -> TableLike | None:
        if isinstance(value, Table):
            return value
        return None

    relationship_outputs = legacy.get("relationship_outputs")
    scip_symbol_relationships = legacy.get("scip_symbol_relationships")
    diagnostics_norm = legacy.get("diagnostics_norm")
    repo_files = legacy.get("repo_files")
    type_exprs_norm = legacy.get("type_exprs_norm")
    rt_signatures = legacy.get("rt_signatures")
    rt_signature_params = legacy.get("rt_signature_params")
    rt_members = legacy.get("rt_members")
    return EdgeBuildInputs(
        relationship_outputs=relationship_outputs
        if isinstance(relationship_outputs, Mapping)
        else None,
        scip_symbol_relationships=_maybe_table(scip_symbol_relationships),
        diagnostics_norm=_maybe_table(diagnostics_norm),
        repo_files=_maybe_table(repo_files),
        type_exprs_norm=_maybe_table(type_exprs_norm),
        rt_signatures=_maybe_table(rt_signatures),
        rt_signature_params=_maybe_table(rt_signature_params),
        rt_members=_maybe_table(rt_members),
    )


def _edge_catalog(inputs: EdgeBuildInputs) -> PlanCatalog:
    catalog = PlanCatalog()
    if inputs.relationship_outputs:
        for name, table in inputs.relationship_outputs.items():
            if table is not None:
                catalog.add(name, table)
    catalog.extend(
        {
            name: table
            for name, table in {
                "scip_symbol_relationships": inputs.scip_symbol_relationships,
                "diagnostics_norm": inputs.diagnostics_norm,
                "repo_files": inputs.repo_files,
                "type_exprs_norm": inputs.type_exprs_norm,
                "rt_signatures": inputs.rt_signatures,
                "rt_signature_params": inputs.rt_signature_params,
                "rt_members": inputs.rt_members,
            }.items()
            if table is not None
        }
    )
    return catalog


def build_cpg_edges_raw(
    *,
    ctx: ExecutionContext,
    inputs: EdgeBuildInputs | None = None,
    options: EdgeBuildOptions | None = None,
    **legacy: object,
) -> Plan:
    """Emit raw CPG edges as a plan without finalization.

    Returns
    -------
    Plan
        Plan producing the raw edges table.

    Raises
    ------
    ValueError
        Raised when an option flag is missing.
    """
    options = options or EdgeBuildOptions()
    if inputs is None and legacy:
        inputs = _edge_inputs_from_legacy(legacy)
    catalog = _edge_catalog(inputs or EdgeBuildInputs())

    parts: list[Plan] = []
    for spec in EDGE_RELATION_SPECS:
        enabled = getattr(options, spec.option_flag, None)
        if enabled is None:
            msg = f"Unknown option flag: {spec.option_flag}"
            raise ValueError(msg)
        if not enabled:
            continue
        rel = spec.build(catalog, ctx=ctx)
        if rel is None:
            continue
        parts.append(emit_edges_plan(rel, spec=spec.emit, ctx=ctx))

    if not parts:
        return empty_plan(CPG_EDGES_SCHEMA, label="cpg_edges_raw")

    combined = union_all_plans(parts, label="cpg_edges_raw")
    combined = encode_plan(combined, specs=EDGE_ENCODING_SPECS, ctx=ctx)
    return align_plan(combined, schema=CPG_EDGES_SCHEMA, ctx=ctx)


def build_cpg_edges(
    *,
    ctx: ExecutionContext,
    inputs: EdgeBuildInputs | None = None,
    options: EdgeBuildOptions | None = None,
) -> CpgBuildArtifacts:
    """Build and finalize CPG edges with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalize result plus quality table.
    """
    raw_plan = build_cpg_edges_raw(ctx=ctx, inputs=inputs, options=options)
    quality_plan = quality_plan_from_ids(
        raw_plan,
        spec=QualityPlanSpec(
            id_col="edge_id",
            entity_kind="edge",
            issue="invalid_edge_id",
            source_table="cpg_edges_raw",
        ),
        ctx=ctx,
    )
    raw = finalize_plan(raw_plan, ctx=ctx)
    quality = finalize_plan(quality_plan, ctx=ctx)
    finalize = CPG_EDGES_SPEC.finalize_context(ctx).run(raw, ctx=ctx)
    return CpgBuildArtifacts(finalize=finalize, quality=quality)
