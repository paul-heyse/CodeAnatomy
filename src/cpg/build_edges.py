"""Build CPG edge tables from relationship outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

import pyarrow as pa
from ibis.backends import BaseBackend

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike, Table, TableLike
from arrowdsl.plan.plan import Plan, union_all_plans
from arrowdsl.plan.query import ScanTelemetry
from arrowdsl.plan.runner import AdapterRunOptions, run_plan_bundle_adapter
from arrowdsl.plan.scan_io import DatasetSource
from arrowdsl.schema.schema import EncodingSpec
from config import AdapterMode
from cpg.catalog import PlanCatalog
from cpg.constants import CpgBuildArtifacts, QualityPlanSpec, quality_plan_from_ids
from cpg.edge_specs import edge_plan_specs_from_table
from cpg.emit_edges import emit_edges_plan
from cpg.ibis_bridge import plan_bundle_to_ibis
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
from cpg.relationship_plans import (
    RelationPlanBundle,
    RelationPlanCompileOptions,
    compile_relation_plans,
)
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
class EdgeRelationContext:
    """Resolved relation plan context for edge compilation."""

    relation_bundle: RelationPlanBundle
    options: EdgeBuildOptions
    edges_schema: SchemaLike
    relation_rule_table: pa.Table
    registry: CpgRegistry


@dataclass(frozen=True)
class EdgePlanBundle:
    """Raw edge plan plus relspec scan telemetry."""

    plan: Plan
    telemetry: Mapping[str, ScanTelemetry] = field(default_factory=dict)


def _edge_relation_context(
    config: EdgeBuildConfig | None,
    *,
    ctx: ExecutionContext,
) -> EdgeRelationContext:
    config = _resolve_edge_config(config)
    registry = config.registry or default_cpg_registry()
    edges_schema = registry.edges_spec().schema()
    options = config.options or EdgeBuildOptions()
    inputs = config.inputs
    if inputs is None and config.legacy:
        inputs = _edge_inputs_from_legacy(config.legacy)
    catalog = _edge_catalog(inputs or EdgeBuildInputs())
    spec_tables = config.spec_tables or EdgeSpecOverrides()
    relation_rule_table = spec_tables.relation_rule_table or registry.relation_rule_table
    relation_bundle = compile_relation_plans(
        catalog,
        ctx=ctx,
        options=RelationPlanCompileOptions(
            rule_table=relation_rule_table,
            materialize_debug=config.materialize_relation_outputs,
            required_sources=config.required_relation_sources,
        ),
    )
    if config.adapter_mode is not None and config.adapter_mode.use_ibis_bridge:
        if config.ibis_backend is None:
            msg = "Ibis backend is required when AdapterMode.use_ibis_bridge is enabled."
            raise ValueError(msg)
        ibis_plans = plan_bundle_to_ibis(
            relation_bundle.plans,
            ctx=ctx,
            backend=config.ibis_backend,
            name_prefix="cpg_rel",
        )
        tables = run_plan_bundle_adapter(
            ibis_plans,
            ctx=ctx,
            options=AdapterRunOptions(
                adapter_mode=config.adapter_mode,
                prefer_reader=False,
                attach_ordering_metadata=True,
            ),
        )
        relation_bundle = RelationPlanBundle(
            plans={name: Plan.table_source(table, label=name) for name, table in tables.items()},
            telemetry=relation_bundle.telemetry,
        )
    return EdgeRelationContext(
        relation_bundle=relation_bundle,
        options=options,
        edges_schema=edges_schema,
        relation_rule_table=relation_rule_table,
        registry=registry,
    )


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
    adapter_mode: AdapterMode | None = None
    ibis_backend: BaseBackend | None = None


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
        adapter_mode=resolved.adapter_mode,
        ibis_backend=resolved.ibis_backend,
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
) -> EdgePlanBundle:
    """Emit raw CPG edges as a plan without finalization.

    Returns
    -------
    EdgePlanBundle
        Raw plan plus scan telemetry.

    Raises
    ------
    ValueError
        Raised when an option flag is missing.
    """
    edge_context = _edge_relation_context(config, ctx=ctx)
    relation_plans = edge_context.relation_bundle.plans

    parts: list[Plan] = []
    for spec in _edge_plan_specs(edge_context.relation_rule_table, registry=edge_context.registry):
        enabled = getattr(edge_context.options, spec.option_flag, None)
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
        return EdgePlanBundle(
            plan=empty_plan(edge_context.edges_schema, label="cpg_edges_raw"),
            telemetry=edge_context.relation_bundle.telemetry,
        )

    combined = union_all_plans(parts, label="cpg_edges_raw")
    combined = encode_plan(combined, specs=_encoding_specs(edge_context.edges_schema), ctx=ctx)
    return EdgePlanBundle(
        plan=align_plan(combined, schema=edge_context.edges_schema, ctx=ctx),
        telemetry=edge_context.relation_bundle.telemetry,
    )


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
    raw_bundle = build_cpg_edges_raw(
        ctx=ctx,
        config=config,
    )
    raw_plan = raw_bundle.plan
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
        relspec_scan_telemetry=raw_bundle.telemetry,
    )
