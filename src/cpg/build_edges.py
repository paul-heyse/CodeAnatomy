"""Build CPG edge tables from relationship outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike, Table, TableLike
from arrowdsl.plan.plan import Plan, union_all_plans
from arrowdsl.plan.scan_io import DatasetSource
from arrowdsl.schema.schema import EncodingSpec
from cpg.catalog import PlanCatalog
from cpg.constants import CpgBuildArtifacts, QualityPlanSpec, quality_plan_from_ids
from cpg.emit_edges import emit_edges_plan
from cpg.plan_specs import (
    align_plan,
    assert_schema_metadata,
    empty_plan,
    encode_plan,
    encoding_columns_from_metadata,
    finalize_context_for_plan,
    finalize_plan,
)
from cpg.registry import CpgRegistry, default_cpg_registry
from cpg.relation_registry import compile_relation_plans, edge_plan_specs_from_table
from cpg.specs import EdgePlanSpec


def _encoding_specs(schema: SchemaLike) -> tuple[EncodingSpec, ...]:
    return tuple(EncodingSpec(column=col) for col in encoding_columns_from_metadata(schema))


def _edge_plan_specs(
    relation_rule_table: pa.Table | None,
    *,
    registry: CpgRegistry,
) -> tuple[EdgePlanSpec, ...]:
    table = relation_rule_table or registry.relation_rule_table
    return edge_plan_specs_from_table(table)


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

    relationship_outputs: Mapping[str, TableLike | DatasetSource] | None = None
    scip_symbol_relationships: TableLike | DatasetSource | None = None
    diagnostics_norm: TableLike | DatasetSource | None = None
    repo_files: TableLike | DatasetSource | None = None
    type_exprs_norm: TableLike | DatasetSource | None = None
    rt_signatures: TableLike | DatasetSource | None = None
    rt_signature_params: TableLike | DatasetSource | None = None
    rt_members: TableLike | DatasetSource | None = None


@dataclass(frozen=True)
class EdgeSpecOverrides:
    """Optional spec table overrides for edge construction."""

    relation_rule_table: pa.Table | None = None


@dataclass(frozen=True)
class EdgeBuildConfig:
    """Configuration bundle for edge construction."""

    inputs: EdgeBuildInputs | None = None
    options: EdgeBuildOptions | None = None
    spec_tables: EdgeSpecOverrides | None = None
    registry: CpgRegistry | None = None
    legacy: Mapping[str, object] | None = None
    materialize_relation_outputs: bool | None = None
    required_relation_sources: tuple[str, ...] | None = None


def _resolve_edge_config(config: EdgeBuildConfig | None) -> EdgeBuildConfig:
    resolved = config or EdgeBuildConfig()
    if resolved.registry is not None:
        return resolved
    return EdgeBuildConfig(
        inputs=resolved.inputs,
        options=resolved.options,
        spec_tables=resolved.spec_tables,
        registry=default_cpg_registry(),
        legacy=resolved.legacy,
        materialize_relation_outputs=resolved.materialize_relation_outputs,
        required_relation_sources=resolved.required_relation_sources,
    )


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
    config: EdgeBuildConfig | None = None,
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
    config = _resolve_edge_config(config)
    registry = config.registry or default_cpg_registry()
    edges_spec = registry.edges_spec()
    edges_schema = edges_spec.schema()
    options = config.options or EdgeBuildOptions()
    inputs = config.inputs
    if inputs is None and config.legacy:
        inputs = _edge_inputs_from_legacy(config.legacy)
    catalog = _edge_catalog(inputs or EdgeBuildInputs())
    spec_tables = config.spec_tables or EdgeSpecOverrides()
    relation_rule_table = spec_tables.relation_rule_table or registry.relation_rule_table
    relation_plans = compile_relation_plans(
        catalog,
        ctx=ctx,
        rule_table=relation_rule_table,
        materialize_debug=config.materialize_relation_outputs,
        required_sources=config.required_relation_sources,
    )

    parts: list[Plan] = []
    for spec in _edge_plan_specs(relation_rule_table, registry=registry):
        enabled = getattr(options, spec.option_flag, None)
        if enabled is None:
            msg = f"Unknown option flag: {spec.option_flag}"
            raise ValueError(msg)
        if not enabled:
            continue
        rel = relation_plans.get(spec.relation_ref)
        if rel is None:
            continue
        parts.append(emit_edges_plan(rel, spec=spec.emit, ctx=ctx))

    if not parts:
        return empty_plan(edges_schema, label="cpg_edges_raw")

    combined = union_all_plans(parts, label="cpg_edges_raw")
    combined = encode_plan(combined, specs=_encoding_specs(edges_schema), ctx=ctx)
    return align_plan(combined, schema=edges_schema, ctx=ctx)


def build_cpg_edges(
    *,
    ctx: ExecutionContext,
    config: EdgeBuildConfig | None = None,
) -> CpgBuildArtifacts:
    """Build and finalize CPG edges with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalize result plus quality table.
    """
    config = _resolve_edge_config(config)
    registry = config.registry or default_cpg_registry()
    edges_spec = registry.edges_spec()
    raw_plan = build_cpg_edges_raw(
        ctx=ctx,
        config=config,
    )
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
    finalize_ctx = finalize_context_for_plan(
        raw_plan,
        contract=edges_spec.contract(),
        ctx=ctx,
    )
    finalize = edges_spec.finalize_context(ctx).run(raw, ctx=finalize_ctx)
    if ctx.debug:
        assert_schema_metadata(finalize.good, schema=edges_spec.schema())
    return CpgBuildArtifacts(
        finalize=finalize,
        quality=quality,
        pipeline_breakers=raw_plan.pipeline_breakers,
    )
