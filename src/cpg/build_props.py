"""Build CPG properties tables from extracted metadata."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, TableLike, ensure_expression, pc
from arrowdsl.plan.plan import Plan, union_all_plans
from arrowdsl.schema.schema import EncodingSpec
from cpg.artifacts import CpgBuildArtifacts
from cpg.catalog import PlanCatalog
from cpg.emit_props import emit_props_plans, filter_fields
from cpg.plan_helpers import (
    align_plan,
    empty_plan,
    ensure_plan,
    finalize_plan,
    set_or_append_column,
)
from cpg.quality import QualityPlanSpec, quality_plan_from_ids
from cpg.schemas import CPG_PROPS_SCHEMA, CPG_PROPS_SPEC, SCHEMA_VERSION
from cpg.spec_registry import (
    ROLE_FLAG_SPECS,
    edge_prop_spec,
    prop_table_specs,
    scip_role_flag_prop_spec,
)
from cpg.specs import PropTableSpec


def _defs_plan(plan: Plan, *, ctx: ExecutionContext) -> Plan:
    schema = plan.schema(ctx=ctx)
    available = set(schema.names)
    exprs = [
        ensure_expression(pc.cast(pc.field(col), pa.string(), safe=False))
        for col in ("def_kind", "kind")
        if col in available
    ]
    if not exprs:
        expr = ensure_expression(pc.cast(pc.scalar(None), pa.string(), safe=False))
    elif len(exprs) == 1:
        expr = exprs[0]
    else:
        expr = ensure_expression(pc.coalesce(*exprs))
    return set_or_append_column(plan, name="def_kind_norm", expr=expr, ctx=ctx)


def _scip_role_flags_plan(scip_occurrences: Plan, *, ctx: ExecutionContext) -> Plan | None:
    schema = scip_occurrences.schema(ctx=ctx)
    available = set(schema.names)
    if "symbol" not in available or "symbol_roles" not in available:
        return None
    role_expr = pc.cast(pc.field("symbol_roles"), pa.int64(), safe=False)
    flag_exprs: list[ComputeExpression] = []
    flag_names: list[str] = []
    for name, mask, _ in ROLE_FLAG_SPECS:
        hit = pc.not_equal(pc.bit_wise_and(role_expr, pa.scalar(mask)), pa.scalar(0))
        flag_exprs.append(ensure_expression(pc.cast(hit, pa.int32(), safe=False)))
        flag_names.append(name)
    project_exprs = [
        ensure_expression(pc.cast(pc.field("symbol"), pa.string(), safe=False)),
        *flag_exprs,
    ]
    project_names = ["symbol", *flag_names]
    projected = scip_occurrences.project(project_exprs, project_names, ctx=ctx)
    aggregated = projected.aggregate(
        group_keys=("symbol",),
        aggs=[(name, "max") for name in flag_names],
        ctx=ctx,
    )
    rename_exprs = [pc.field("symbol")] + [pc.field(f"{name}_max") for name in flag_names]
    rename_names = ["symbol", *flag_names]
    return aggregated.project(rename_exprs, rename_names, ctx=ctx)


PROP_TABLE_SPECS: tuple[PropTableSpec, ...] = (
    *prop_table_specs(),
    scip_role_flag_prop_spec(),
    edge_prop_spec(),
)

PROP_ENCODING_SPECS: tuple[EncodingSpec, ...] = ()


@dataclass(frozen=True)
class PropsBuildOptions:
    """Configure which property families are emitted."""

    include_node_props: bool = True
    include_edge_props: bool = True
    include_heavy_json_props: bool = True


@dataclass(frozen=True)
class PropsInputTables:
    """Bundle of input tables for property extraction."""

    repo_files: TableLike | None = None
    cst_name_refs: TableLike | None = None
    cst_imports: TableLike | None = None
    cst_callsites: TableLike | None = None
    cst_defs: TableLike | None = None
    dim_qualified_names: TableLike | None = None
    scip_symbol_information: TableLike | None = None
    scip_occurrences: TableLike | None = None
    scip_external_symbol_information: TableLike | None = None
    ts_nodes: TableLike | None = None
    ts_errors: TableLike | None = None
    ts_missing: TableLike | None = None
    type_exprs_norm: TableLike | None = None
    types_norm: TableLike | None = None
    diagnostics_norm: TableLike | None = None
    rt_objects: TableLike | None = None
    rt_signatures: TableLike | None = None
    rt_signature_params: TableLike | None = None
    rt_members: TableLike | None = None
    cpg_edges: TableLike | None = None


def _prop_tables(
    inputs: PropsInputTables,
    *,
    ctx: ExecutionContext,
) -> dict[str, TableLike | Plan]:
    catalog = PlanCatalog()
    catalog.extend(
        {
            name: table
            for name, table in {
                "repo_files": inputs.repo_files,
                "cst_name_refs": inputs.cst_name_refs,
                "cst_imports": inputs.cst_imports,
                "cst_callsites": inputs.cst_callsites,
                "dim_qualified_names": inputs.dim_qualified_names,
                "scip_symbol_information": inputs.scip_symbol_information,
                "scip_external_symbol_information": inputs.scip_external_symbol_information,
                "ts_nodes": inputs.ts_nodes,
                "ts_errors": inputs.ts_errors,
                "ts_missing": inputs.ts_missing,
                "type_exprs_norm": inputs.type_exprs_norm,
                "types_norm": inputs.types_norm,
                "diagnostics_norm": inputs.diagnostics_norm,
                "rt_objects": inputs.rt_objects,
                "rt_signatures": inputs.rt_signatures,
                "rt_signature_params": inputs.rt_signature_params,
                "rt_members": inputs.rt_members,
                "cpg_edges": inputs.cpg_edges,
            }.items()
            if table is not None
        }
    )

    if inputs.cst_defs is not None:
        defs_plan = Plan.table_source(inputs.cst_defs, label="cst_defs")
        defs_norm = _defs_plan(defs_plan, ctx=ctx)
        catalog.add("cst_defs", defs_plan)
        catalog.add("cst_defs_norm", defs_norm)

    if inputs.scip_occurrences is not None:
        occ_plan = Plan.table_source(inputs.scip_occurrences, label="scip_occurrences")
        scip_role_flags = _scip_role_flags_plan(occ_plan, ctx=ctx)
        if scip_role_flags is not None:
            catalog.add("scip_role_flags", scip_role_flags)
    return catalog.snapshot()


def build_cpg_props_raw(
    *,
    ctx: ExecutionContext,
    inputs: PropsInputTables | None = None,
    options: PropsBuildOptions | None = None,
) -> Plan:
    """Build CPG properties as a plan without finalization.

    Returns
    -------
    Plan
        Plan producing the raw properties table.

    Raises
    ------
    ValueError
        Raised when an option flag is missing.
    """
    options = options or PropsBuildOptions()
    tables = _prop_tables(inputs or PropsInputTables(), ctx=ctx)
    plans: list[Plan] = []
    include_schema_version = "schema_version" in CPG_PROPS_SCHEMA.names
    schema_version = SCHEMA_VERSION if include_schema_version else None
    for spec in PROP_TABLE_SPECS:
        enabled = getattr(options, spec.option_flag, None)
        if enabled is None:
            msg = f"Unknown option flag: {spec.option_flag}"
            raise ValueError(msg)
        if not enabled:
            continue
        if spec.include_if is not None and not spec.include_if(options):
            continue
        plan_source = spec.table_getter(tables)
        if plan_source is None:
            continue
        plan = ensure_plan(plan_source, label=spec.name)
        filtered_fields = filter_fields(spec.fields, options=options)
        if not filtered_fields and spec.node_kind is None:
            continue
        if filtered_fields != list(spec.fields):
            updated_spec = spec.model_copy(update={"fields": tuple(filtered_fields)})
        else:
            updated_spec = spec
        plans.extend(
            emit_props_plans(
                plan,
                spec=updated_spec,
                schema_version=schema_version,
                ctx=ctx,
            )
        )

    if not plans:
        return empty_plan(CPG_PROPS_SCHEMA, label="cpg_props_raw")

    return align_plan(
        union_all_plans(plans, label="cpg_props_raw"),
        schema=CPG_PROPS_SCHEMA,
        ctx=ctx,
    )


def build_cpg_props(
    *,
    ctx: ExecutionContext,
    inputs: PropsInputTables | None = None,
    options: PropsBuildOptions | None = None,
) -> CpgBuildArtifacts:
    """Build and finalize CPG properties with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalize result plus quality table.
    """
    raw_plan = build_cpg_props_raw(ctx=ctx, inputs=inputs, options=options)
    quality_plan = quality_plan_from_ids(
        raw_plan,
        spec=QualityPlanSpec(
            id_col="entity_id",
            entity_kind="prop",
            issue="invalid_entity_id",
            source_table="cpg_props_raw",
        ),
        ctx=ctx,
    )
    raw = finalize_plan(raw_plan, ctx=ctx)
    quality = finalize_plan(quality_plan, ctx=ctx)
    finalize = CPG_PROPS_SPEC.finalize_context(ctx).run(raw, ctx=ctx)
    return CpgBuildArtifacts(finalize=finalize, quality=quality)
