"""Build CPG edge tables from relationship outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.core.ordering import Ordering
from arrowdsl.core.scan_telemetry import ScanTelemetry
from arrowdsl.schema.schema import EncodingSpec, empty_table, encode_table
from cpg.constants import CpgBuildArtifacts, quality_from_ids
from cpg.edge_specs import edge_plan_specs_from_table
from cpg.registry import CpgRegistry, default_cpg_registry
from cpg.relationship_plans import (
    IbisPlanCatalog,
    IbisPlanSource,
    RelationPlanBundle,
    RelationPlanCompileOptions,
    compile_relation_plans_ibis,
)
from cpg.specs import EdgePlanSpec
from cpg.table_utils import (
    align_table_to_schema,
    assert_schema_metadata,
    encoding_columns_from_metadata,
)
from datafusion_engine.runtime import AdapterExecutionPolicy
from engine.materialize import resolve_prefer_reader
from engine.plan_policy import ExecutionSurfacePolicy
from engine.session import EngineSession
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan, stream_ibis_plan
from ibis_engine.plan import IbisPlan
from ibis_engine.scan_io import DatasetSource
from ibis_engine.sources import SourceToIbisOptions, register_ibis_view
from relspec.rules.handlers.cpg_emit import EdgeEmitRuleHandler
from schema_spec.system import DatasetSpec


def _encoding_specs(schema: SchemaLike) -> tuple[EncodingSpec, ...]:
    return tuple(EncodingSpec(column=col) for col in encoding_columns_from_metadata(schema))


def _edge_plan_specs(
    relation_rule_table: pa.Table | None,
    *,
    registry: CpgRegistry,
) -> tuple[EdgePlanSpec, ...]:
    table = relation_rule_table or registry.relation_rule_table
    return edge_plan_specs_from_table(table)


def _materialize_table(table: TableLike | RecordBatchReaderLike) -> TableLike:
    if isinstance(table, RecordBatchReaderLike):
        return table.read_all()
    return table


def _empty_edges_ibis(schema: SchemaLike) -> IbisPlan:
    table = empty_table(schema)
    return IbisPlan(expr=ibis.memtable(table), ordering=Ordering.unordered())


def _union_edges_ibis(parts: list[IbisPlan]) -> IbisPlan:
    combined = parts[0].expr
    for part in parts[1:]:
        combined = combined.union(part.expr)
    return IbisPlan(expr=combined, ordering=Ordering.unordered())


def _build_edges_raw_ibis(edge_context: EdgeRelationContext) -> EdgePlanBundle:
    relation_plans = edge_context.relation_bundle.plans
    parts: list[IbisPlan] = []
    handler = EdgeEmitRuleHandler()
    include_keys = edge_context.ctx.debug
    for spec in _edge_plan_specs(
        edge_context.relation_rule_table,
        registry=edge_context.registry,
    ):
        enabled = getattr(edge_context.options, spec.option_flag, None)
        if enabled is None:
            msg = f"Unknown option flag: {spec.option_flag}"
            raise ValueError(msg)
        if not enabled:
            continue
        rel = relation_plans.get(spec.relation_ref)
        if rel is None:
            continue
        if not isinstance(rel, IbisPlan):
            msg = f"Expected Ibis plan for relation {spec.relation_ref!r}."
            raise TypeError(msg)
        parts.append(handler.compile_ibis(rel, spec=spec, include_keys=include_keys))
    if not parts:
        return EdgePlanBundle(
            plan=_empty_edges_ibis(edge_context.edges_schema),
            telemetry=edge_context.relation_bundle.telemetry,
        )
    return EdgePlanBundle(
        plan=_union_edges_ibis(parts),
        telemetry=edge_context.relation_bundle.telemetry,
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

    ctx: ExecutionContext
    relation_bundle: RelationPlanBundle
    options: EdgeBuildOptions
    edges_schema: SchemaLike
    relation_rule_table: pa.Table
    registry: CpgRegistry


@dataclass(frozen=True)
class EdgePlanBundle:
    """Raw edge plan plus relspec scan telemetry."""

    plan: IbisPlan
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
    if inputs is None:
        msg = "EdgeBuildConfig.inputs is required for CPG edge compilation."
        raise ValueError(msg)
    backend = config.ibis_backend
    if backend is None:
        msg = "Ibis backend is required for CPG edge compilation."
        raise ValueError(msg)
    catalog = _edge_catalog(inputs, backend=backend)
    spec_tables = config.spec_tables or EdgeSpecOverrides()
    relation_rule_table = spec_tables.relation_rule_table or registry.relation_rule_table
    relation_bundle = compile_relation_plans_ibis(
        catalog,
        ctx=ctx,
        backend=backend,
        options=RelationPlanCompileOptions(
            rule_table=relation_rule_table,
            materialize_debug=config.materialize_relation_outputs,
            required_sources=config.required_relation_sources,
            backend=backend,
            execution_policy=config.execution_policy,
        ),
    )
    return EdgeRelationContext(
        ctx=ctx,
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
    materialize_relation_outputs: bool | None = None
    required_relation_sources: tuple[str, ...] | None = None
    execution_policy: AdapterExecutionPolicy | None = None
    ibis_backend: BaseBackend | None = None
    surface_policy: ExecutionSurfacePolicy | None = None


def _resolve_edge_config(config: EdgeBuildConfig | None) -> EdgeBuildConfig:
    resolved = config or EdgeBuildConfig()
    if resolved.registry is not None:
        return resolved
    return EdgeBuildConfig(
        inputs=resolved.inputs,
        options=resolved.options,
        spec_tables=resolved.spec_tables,
        registry=default_cpg_registry(),
        materialize_relation_outputs=resolved.materialize_relation_outputs,
        required_relation_sources=resolved.required_relation_sources,
        execution_policy=resolved.execution_policy,
        ibis_backend=resolved.ibis_backend,
        surface_policy=resolved.surface_policy,
    )


def _resolve_ctx_for_session(
    ctx: ExecutionContext | None,
    session: EngineSession | None,
) -> ExecutionContext:
    if session is None:
        if ctx is None:
            msg = "Either ctx or session must be provided for CPG edge builds."
            raise ValueError(msg)
        return ctx
    if ctx is not None and ctx is not session.ctx:
        msg = "Provided ctx must match session.ctx when session is supplied."
        raise ValueError(msg)
    return session.ctx


def _merge_edge_config(
    config: EdgeBuildConfig | None,
    session: EngineSession | None,
) -> EdgeBuildConfig | None:
    if session is None:
        return config
    resolved = config or EdgeBuildConfig()
    if resolved.ibis_backend is None:
        resolved = replace(resolved, ibis_backend=session.ibis_backend)
    if resolved.surface_policy is None:
        resolved = replace(resolved, surface_policy=session.surface_policy)
    return resolved


def _edge_catalog(
    inputs: EdgeBuildInputs,
    *,
    backend: BaseBackend,
) -> IbisPlanCatalog:
    tables: dict[str, IbisPlanSource] = {}
    if inputs.relationship_outputs:
        tables.update(
            {
                name: table
                for name, table in inputs.relationship_outputs.items()
                if table is not None
            }
        )
    tables.update(
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
    return IbisPlanCatalog(backend=backend, tables=tables)


def build_cpg_edges_raw(
    *,
    ctx: ExecutionContext | None = None,
    session: EngineSession | None = None,
    config: EdgeBuildConfig | None = None,
) -> EdgePlanBundle:
    """Emit raw CPG edges as a plan without finalization.

    Returns
    -------
    EdgePlanBundle
        Raw plan plus scan telemetry.

    """
    exec_ctx = _resolve_ctx_for_session(ctx, session)
    config = _merge_edge_config(config, session)
    edge_context = _edge_relation_context(config, ctx=exec_ctx)
    return _build_edges_raw_ibis(edge_context)


def _finalize_edges_ibis(
    *,
    exec_ctx: ExecutionContext,
    config: EdgeBuildConfig,
    edges_spec: DatasetSpec,
    raw_bundle: EdgePlanBundle,
) -> CpgBuildArtifacts:
    raw_plan = raw_bundle.plan
    if not isinstance(raw_plan, IbisPlan):
        msg = "Expected an IbisPlan for Ibis edge materialization."
        raise TypeError(msg)
    if config.ibis_backend is None:
        msg = "Ibis backend is required for Ibis edge materialization."
        raise ValueError(msg)
    policy = config.surface_policy or ExecutionSurfacePolicy()
    prefer_reader = resolve_prefer_reader(ctx=exec_ctx, policy=policy)
    view_name = f"{edges_spec.name}_raw"
    registered = register_ibis_view(
        raw_plan.expr,
        options=SourceToIbisOptions(
            backend=config.ibis_backend,
            name=view_name,
            ordering=raw_plan.ordering,
        ),
    )
    execution = IbisExecutionContext(
        ctx=exec_ctx,
        execution_policy=config.execution_policy,
        ibis_backend=config.ibis_backend,
    )
    raw = (
        _materialize_table(stream_ibis_plan(registered, execution=execution))
        if prefer_reader
        else materialize_ibis_plan(registered, execution=execution)
    )
    raw = encode_table(
        raw,
        columns=encoding_columns_from_metadata(edges_spec.schema()),
    )
    raw = align_table_to_schema(
        raw,
        schema=edges_spec.schema(),
        safe_cast=exec_ctx.safe_cast,
        keep_extra_columns=exec_ctx.debug,
    )
    quality = quality_from_ids(
        raw,
        id_col="edge_id",
        entity_kind="edge",
        issue="invalid_edge_id",
        source_table="cpg_edges_raw",
    )
    finalize = edges_spec.finalize_context(exec_ctx).run(raw, ctx=exec_ctx)
    if exec_ctx.debug:
        assert_schema_metadata(finalize.good, schema=edges_spec.schema())
    return CpgBuildArtifacts(
        finalize=finalize,
        quality=quality,
        pipeline_breakers=(),
        relspec_scan_telemetry=raw_bundle.telemetry,
    )


def build_cpg_edges(
    *,
    ctx: ExecutionContext | None = None,
    session: EngineSession | None = None,
    config: EdgeBuildConfig | None = None,
) -> CpgBuildArtifacts:
    """Build and finalize CPG edges with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalize result plus quality table.
    """
    exec_ctx = _resolve_ctx_for_session(ctx, session)
    config = _merge_edge_config(config, session)
    config = _resolve_edge_config(config)
    registry = config.registry or default_cpg_registry()
    edges_spec = registry.edges_spec()
    raw_bundle = build_cpg_edges_raw(
        ctx=exec_ctx,
        session=session,
        config=config,
    )
    return _finalize_edges_ibis(
        exec_ctx=exec_ctx,
        config=config,
        edges_spec=edges_spec,
        raw_bundle=raw_bundle,
    )
