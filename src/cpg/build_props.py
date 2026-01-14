"""Build CPG properties tables from extracted metadata."""

from __future__ import annotations

from dataclasses import dataclass, replace

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.plan import Plan, union_all_plans
from arrowdsl.plan.scan_io import DatasetSource
from cpg.catalog import PlanCatalog, resolve_plan_source
from cpg.constants import CpgBuildArtifacts, QualityPlanSpec, quality_plan_from_ids
from cpg.emit_props import emit_props_plans, filter_fields
from cpg.plan_specs import (
    align_plan,
    assert_schema_metadata,
    empty_plan,
    ensure_plan,
    finalize_context_for_plan,
    finalize_plan,
)
from cpg.registry import CpgRegistry, default_cpg_registry
from cpg.spec_tables import prop_table_specs_from_table
from cpg.specs import PropTableSpec, resolve_prop_include


def _prop_table_specs(
    prop_spec_table: pa.Table | None,
    *,
    registry: CpgRegistry,
) -> tuple[PropTableSpec, ...]:
    table = prop_spec_table or registry.prop_table_spec_table
    return prop_table_specs_from_table(table)


@dataclass(frozen=True)
class PropsBuildOptions:
    """Configure which property families are emitted."""

    include_node_props: bool = True
    include_edge_props: bool = True
    include_heavy_json_props: bool = True


@dataclass(frozen=True)
class PropsInputTables:
    """Bundle of input tables for property extraction."""

    repo_files: TableLike | DatasetSource | None = None
    cst_name_refs: TableLike | DatasetSource | None = None
    cst_imports: TableLike | DatasetSource | None = None
    cst_callsites: TableLike | DatasetSource | None = None
    cst_defs: TableLike | DatasetSource | None = None
    dim_qualified_names: TableLike | DatasetSource | None = None
    scip_symbol_information: TableLike | DatasetSource | None = None
    scip_occurrences: TableLike | DatasetSource | None = None
    scip_external_symbol_information: TableLike | DatasetSource | None = None
    ts_nodes: TableLike | DatasetSource | None = None
    ts_errors: TableLike | DatasetSource | None = None
    ts_missing: TableLike | DatasetSource | None = None
    type_exprs_norm: TableLike | DatasetSource | None = None
    types_norm: TableLike | DatasetSource | None = None
    diagnostics_norm: TableLike | DatasetSource | None = None
    rt_objects: TableLike | DatasetSource | None = None
    rt_signatures: TableLike | DatasetSource | None = None
    rt_signature_params: TableLike | DatasetSource | None = None
    rt_members: TableLike | DatasetSource | None = None
    cpg_edges: TableLike | DatasetSource | None = None


def _prop_tables(
    inputs: PropsInputTables,
) -> PlanCatalog:
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
        catalog.add("cst_defs", inputs.cst_defs)

    if inputs.scip_occurrences is not None:
        catalog.add("scip_occurrences", inputs.scip_occurrences)
    return catalog


def build_cpg_props_raw(
    *,
    ctx: ExecutionContext,
    inputs: PropsInputTables | None = None,
    options: PropsBuildOptions | None = None,
    prop_spec_table: pa.Table | None = None,
    registry: CpgRegistry | None = None,
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
    registry = registry or default_cpg_registry()
    props_spec = registry.props_spec()
    props_schema = props_spec.schema()
    options = options or PropsBuildOptions()
    catalog = _prop_tables(inputs or PropsInputTables())
    plans: list[Plan] = []
    include_schema_version = "schema_version" in props_schema.names
    schema_version = props_spec.table_spec.version if include_schema_version else None
    for spec in _prop_table_specs(prop_spec_table, registry=registry):
        enabled = getattr(options, spec.option_flag, None)
        if enabled is None:
            msg = f"Unknown option flag: {spec.option_flag}"
            raise ValueError(msg)
        if not enabled:
            continue
        include_if = resolve_prop_include(spec.include_if_id)
        if include_if is not None and not include_if(options):
            continue
        plan_source = resolve_plan_source(catalog, spec.table_ref, ctx=ctx)
        if plan_source is None:
            continue
        plan = ensure_plan(plan_source, label=spec.name, ctx=ctx)
        filtered_fields = filter_fields(spec.fields, options=options)
        if not filtered_fields and spec.node_kind is None:
            continue
        if filtered_fields != list(spec.fields):
            updated_spec = replace(spec, fields=tuple(filtered_fields))
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
        return empty_plan(props_schema, label="cpg_props_raw")

    return align_plan(
        union_all_plans(plans, label="cpg_props_raw"),
        schema=props_schema,
        ctx=ctx,
    )


def build_cpg_props(
    *,
    ctx: ExecutionContext,
    inputs: PropsInputTables | None = None,
    options: PropsBuildOptions | None = None,
    prop_spec_table: pa.Table | None = None,
    registry: CpgRegistry | None = None,
) -> CpgBuildArtifacts:
    """Build and finalize CPG properties with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalize result plus quality table.
    """
    registry = registry or default_cpg_registry()
    props_spec = registry.props_spec()
    raw_plan = build_cpg_props_raw(
        ctx=ctx,
        inputs=inputs,
        options=options,
        prop_spec_table=prop_spec_table,
        registry=registry,
    )
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
    finalize_ctx = finalize_context_for_plan(
        raw_plan,
        contract=props_spec.contract(),
        ctx=ctx,
    )
    finalize = props_spec.finalize_context(ctx).run(raw, ctx=finalize_ctx)
    if ctx.debug:
        assert_schema_metadata(finalize.good, schema=props_spec.schema())
    return CpgBuildArtifacts(
        finalize=finalize,
        quality=quality,
        pipeline_breakers=raw_plan.pipeline_breakers,
    )
