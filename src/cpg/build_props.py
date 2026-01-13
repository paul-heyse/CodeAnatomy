"""Build CPG properties tables from extracted metadata."""

from __future__ import annotations

from dataclasses import dataclass, replace

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.plan import Plan, union_all_plans
from arrowdsl.plan.source import DatasetSource
from arrowdsl.schema.schema import EncodingSpec
from cpg.artifacts import CpgBuildArtifacts
from cpg.catalog import PlanCatalog, resolve_plan_source
from cpg.emit_props import emit_props_plans, filter_fields
from cpg.plan_helpers import (
    align_plan,
    assert_schema_metadata,
    empty_plan,
    encoding_columns_from_metadata,
    ensure_plan,
    finalize_context_for_plan,
    finalize_plan,
)
from cpg.quality import QualityPlanSpec, quality_plan_from_ids
from cpg.schemas import CPG_PROPS_SCHEMA, CPG_PROPS_SPEC, SCHEMA_VERSION
from cpg.spec_registry import edge_prop_spec, prop_table_specs, scip_role_flag_prop_spec
from cpg.spec_tables import prop_table_specs_from_table
from cpg.specs import PropTableSpec, resolve_prop_include

PROP_TABLE_SPECS: tuple[PropTableSpec, ...] = (
    *prop_table_specs(),
    scip_role_flag_prop_spec(),
    edge_prop_spec(),
)

PROP_ENCODING_SPECS: tuple[EncodingSpec, ...] = tuple(
    EncodingSpec(column=col) for col in encoding_columns_from_metadata(CPG_PROPS_SCHEMA)
)


def _prop_table_specs(prop_spec_table: pa.Table | None) -> tuple[PropTableSpec, ...]:
    if prop_spec_table is None:
        return PROP_TABLE_SPECS
    return prop_table_specs_from_table(prop_spec_table)


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
    catalog = _prop_tables(inputs or PropsInputTables())
    plans: list[Plan] = []
    include_schema_version = "schema_version" in CPG_PROPS_SCHEMA.names
    schema_version = SCHEMA_VERSION if include_schema_version else None
    for spec in _prop_table_specs(prop_spec_table):
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
    prop_spec_table: pa.Table | None = None,
) -> CpgBuildArtifacts:
    """Build and finalize CPG properties with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalize result plus quality table.
    """
    raw_plan = build_cpg_props_raw(
        ctx=ctx,
        inputs=inputs,
        options=options,
        prop_spec_table=prop_spec_table,
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
        contract=CPG_PROPS_SPEC.contract(),
        ctx=ctx,
    )
    finalize = CPG_PROPS_SPEC.finalize_context(ctx).run(raw, ctx=finalize_ctx)
    if ctx.debug:
        assert_schema_metadata(finalize.good, schema=CPG_PROPS_SPEC.schema())
    return CpgBuildArtifacts(
        finalize=finalize,
        quality=quality,
        pipeline_breakers=raw_plan.pipeline_breakers,
    )
