"""Ibis-first runner utilities for normalize pipelines."""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from functools import cache
from typing import TYPE_CHECKING, cast

import msgspec
from ibis.backends import BaseBackend
from ibis.expr.types import Value

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.core.schema_constants import PROVENANCE_COLS
from arrowdsl.finalize.finalize import Contract, FinalizeOptions, FinalizeResult, finalize
from arrowdsl.schema.metadata import encoding_policy_from_schema, merge_metadata_specs
from arrowdsl.schema.policy import SchemaPolicy, SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import SchemaMetadataSpec
from datafusion_engine.nested_tables import ViewReference, materialize_view_reference
from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.sql_options import sql_options_for_profile
from ibis_engine.catalog import IbisPlanCatalog
from ibis_engine.execution import materialize_ibis_plan
from ibis_engine.execution_factory import ibis_execution_from_ctx
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import IbisQuerySpec, apply_query_spec
from ibis_engine.sources import (
    DatasetSource,
    SourceToIbisOptions,
    namespace_recorder_from_ctx,
    register_ibis_view,
    source_to_ibis,
    table_to_ibis,
)
from normalize.ibis_bridge import resolve_plan_builder_ibis
from normalize.registry_runtime import (
    NORMALIZE_ALIAS_META,
    NORMALIZE_STAGE_META,
    dataset_alias,
    dataset_schema,
)
from normalize.rule_defaults import resolve_rule_defaults
from normalize.rule_factories import build_rule_definitions_from_specs
from normalize.runtime import NormalizeRuntime
from normalize.runtime_validation import validate_rule_specs
from relspec.model import AmbiguityPolicy, ConfidencePolicy
from relspec.normalize.rule_registry_specs import rule_family_specs
from relspec.policies import PolicyRegistry
from relspec.rustworkx_graph import build_rule_graph_from_normalize_rules
from relspec.rustworkx_schedule import schedule_rules
from relspec.rules.definitions import (
    EvidenceOutput,
    EvidenceSpec,
    ExecutionMode,
    NormalizePayload,
    RuleDefinition,
)
from relspec.rules.evidence import EvidenceCatalog
from relspec.rules.rel_ops import query_spec_from_rel_ops
from schema_spec.system import ContractSpec
from sqlglot_tools.bridge import SqlGlotDiagnosticsOptions, sqlglot_diagnostics
from sqlglot_tools.lineage import LineageExtractionOptions, extract_lineage_payload
from sqlglot_tools.optimizer import (
    ast_to_artifact,
    plan_fingerprint,
    resolve_sqlglot_policy,
    serialize_ast_artifact,
    sqlglot_policy_snapshot_for,
)

PostFn = Callable[[TableLike, ExecutionContext], TableLike]

if TYPE_CHECKING:
    from sqlglot.expressions import Expression

    from ibis_engine.execution import IbisExecutionContext
    from relspec.rules.rel_ops import RelOpT
    from sqlglot_tools.bridge import IbisCompilerBackend
    from sqlglot_tools.lineage import LineagePayload
    from sqlglot_tools.optimizer import SqlGlotPolicy


@dataclass(frozen=True)
class NormalizeFinalizeSpec:
    """Finalize overrides for normalize pipelines."""

    metadata_spec: SchemaMetadataSpec | None = None
    schema_policy: SchemaPolicy | None = None


@dataclass(frozen=True)
class NormalizeRunOptions:
    """Execution options for normalize plans."""

    finalize_spec: NormalizeFinalizeSpec | None = None
    execution_policy: AdapterExecutionPolicy | None = None
    execution_label: ExecutionLabel | None = None
    runtime: NormalizeRuntime | None = None
    params: Mapping[Value, object] | None = None


@dataclass(frozen=True)
class ResolvedNormalizeRule:
    """Normalize rule with resolved policies and evidence defaults."""

    name: str
    output: str
    inputs: tuple[str, ...]
    ibis_builder: str | None
    query: IbisQuerySpec | None
    evidence: EvidenceSpec | None
    evidence_output: EvidenceOutput | None
    confidence_policy: ConfidencePolicy | None
    ambiguity_policy: AmbiguityPolicy | None
    priority: int
    emit_rule_meta: bool
    execution_mode: ExecutionMode


@dataclass(frozen=True)
class NormalizeRuleCompilation:
    """Compiled normalize rules and derived Ibis plans."""

    rules: tuple[RuleDefinition, ...]
    resolved_rules: tuple[ResolvedNormalizeRule, ...]
    plans: Mapping[str, IbisPlan]
    ibis_catalog: IbisPlanCatalog
    output_storage: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class NormalizeIbisPlanOptions:
    """Options for compiling normalize plans into Ibis plans."""

    backend: BaseBackend
    rules: Sequence[RuleDefinition] | None = None
    required_outputs: Sequence[str] | None = None
    name_prefix: str = "normalize"
    materialize_outputs: Sequence[str] | None = None
    execution_policy: AdapterExecutionPolicy | None = None
    policy_registry: PolicyRegistry = field(default_factory=PolicyRegistry)
    scan_provenance_columns: Sequence[str] = ()


@dataclass(frozen=True)
class _NormalizeIbisCompilationContext:
    ordered: tuple[ResolvedNormalizeRule, ...]
    ibis_catalog: IbisPlanCatalog
    execution: IbisExecutionContext
    evidence: EvidenceCatalog


@dataclass(frozen=True)
class _SqlGlotPlanContext:
    policy: SqlGlotPolicy
    policy_hash: str
    policy_rules_hash: str
    schema_map: Mapping[str, Mapping[str, str]] | None
    schema_map_hash: str | None


def _normalize_ibis_context(
    catalog: IbisPlanCatalog,
    *,
    ctx: ExecutionContext,
    options: NormalizeIbisPlanOptions,
) -> _NormalizeIbisCompilationContext:
    if catalog.backend is not options.backend:
        msg = "Normalize Ibis catalog backend does not match compile options backend."
        raise ValueError(msg)
    rule_set = _default_normalize_rules() if options.rules is None else tuple(options.rules)
    if options.required_outputs:
        required_set = _expand_required_outputs(rule_set, options.required_outputs)
        rule_set = tuple(rule for rule in rule_set if rule.output in required_set)
    resolved_rules = resolve_normalize_rules(
        rule_set,
        policy_registry=options.policy_registry,
        scan_provenance_columns=options.scan_provenance_columns,
    )
    evidence = EvidenceCatalog.from_sources(catalog.tables)
    graph = build_rule_graph_from_normalize_rules(resolved_rules)
    output_schemas = _normalize_output_schema_map(resolved_rules)
    schedule = schedule_rules(
        graph,
        evidence=evidence,
        output_schema_for=output_schemas.get,
    )
    ordered = _order_normalize_rules(resolved_rules, schedule.ordered_rules)
    execution = ibis_execution_from_ctx(
        ctx,
        backend=options.backend,
        execution_policy=options.execution_policy,
    )
    return _NormalizeIbisCompilationContext(
        ordered=ordered,
        ibis_catalog=catalog,
        execution=execution,
        evidence=evidence,
    )


def _sqlglot_context(runtime: NormalizeRuntime) -> _SqlGlotPlanContext:
    policy = resolve_sqlglot_policy()
    snapshot = sqlglot_policy_snapshot_for(policy)
    schema_map, schema_map_hash = _sqlglot_schema_map(runtime)
    return _SqlGlotPlanContext(
        policy=policy,
        policy_hash=snapshot.policy_hash,
        policy_rules_hash=snapshot.rules_hash,
        schema_map=schema_map,
        schema_map_hash=schema_map_hash,
    )


def _sqlglot_schema_map(
    runtime: NormalizeRuntime,
) -> tuple[Mapping[str, Mapping[str, str]] | None, str | None]:
    try:
        introspector = SchemaIntrospector(
            runtime.ctx,
            sql_options=sql_options_for_profile(runtime.runtime_profile),
        )
        schema_map = introspector.schema_map()
        return schema_map, introspector.schema_map_fingerprint()
    except (RuntimeError, TypeError, ValueError):
        return None, None


def _sqlglot_lineage_payload(
    expr: Expression,
    *,
    context: _SqlGlotPlanContext,
) -> LineagePayload | None:
    try:
        return extract_lineage_payload(
            expr,
            options=LineageExtractionOptions(
                schema=context.schema_map,
                dialect=context.policy.write_dialect,
            ),
        )
    except (RuntimeError, TypeError, ValueError):
        return None


def _sqlglot_ast_payload(expr: Expression, *, sql: str, policy: SqlGlotPolicy) -> bytes | None:
    with contextlib.suppress(RuntimeError, TypeError, ValueError, msgspec.EncodeError):
        return serialize_ast_artifact(ast_to_artifact(expr, sql=sql, policy=policy))
    return None


def _record_sqlglot_plan(
    plan: IbisPlan,
    *,
    runtime: NormalizeRuntime,
    execution_label: ExecutionLabel | None,
) -> None:
    sink = runtime.diagnostics
    if sink is None:
        return
    backend = runtime.ibis_backend
    if not hasattr(backend, "compiler"):
        return
    context = _sqlglot_context(runtime)
    diagnostics = sqlglot_diagnostics(
        plan.expr,
        backend=cast("IbisCompilerBackend", backend),
        options=SqlGlotDiagnosticsOptions(
            schema_map=context.schema_map,
            policy=context.policy,
        ),
    )
    plan_hash = plan_fingerprint(
        diagnostics.optimized,
        dialect=context.policy.write_dialect,
        policy_hash=context.policy_hash,
        schema_map_hash=context.schema_map_hash,
    )
    lineage = _sqlglot_lineage_payload(
        diagnostics.optimized,
        context=context,
    )
    ast_payload = _sqlglot_ast_payload(
        diagnostics.optimized,
        sql=diagnostics.sql_text_optimized,
        policy=context.policy,
    )
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "rule": execution_label.rule_name if execution_label else None,
        "output": execution_label.output_dataset if execution_label else None,
        "plan_hash": plan_hash,
        "sql_dialect": diagnostics.sql_dialect,
        "sql_text_raw": diagnostics.sql_text_raw,
        "sql_text_optimized": diagnostics.sql_text_optimized,
        "tables": list(diagnostics.tables),
        "columns": list(diagnostics.columns),
        "identifiers": list(diagnostics.identifiers),
        "ast_repr": diagnostics.ast_repr,
        "canonical_fingerprint": (lineage.canonical_fingerprint if lineage is not None else None),
        "lineage_tables": list(lineage.tables) if lineage is not None else None,
        "lineage_columns": list(lineage.columns) if lineage is not None else None,
        "lineage_scopes": list(lineage.scopes) if lineage is not None else None,
        "sqlglot_ast": ast_payload,
        "policy_hash": context.policy_hash,
        "policy_rules_hash": context.policy_rules_hash,
        "schema_map_hash": context.schema_map_hash,
    }
    sink.record_artifact("normalize_sqlglot_plan_v1", payload)


def ensure_execution_context(
    ctx: ExecutionContext | None,
    *,
    profile: str = "default",
) -> ExecutionContext:
    """Return a normalized execution context.

    Returns
    -------
    ExecutionContext
        Provided context or a default context when missing.
    """
    if ctx is not None:
        return ctx
    return execution_context_factory(profile)


def ensure_canonical(ctx: ExecutionContext) -> ExecutionContext:
    """Return an execution context upgraded to canonical determinism.

    Returns
    -------
    ExecutionContext
        Execution context using canonical determinism.
    """
    return ctx.with_determinism(DeterminismTier.CANONICAL)


@cache
def _normalize_rule_definitions() -> tuple[RuleDefinition, ...]:
    return build_rule_definitions_from_specs(rule_family_specs())


def _default_normalize_rules() -> tuple[RuleDefinition, ...]:
    return _normalize_rule_definitions()


def resolve_normalize_rules(
    rules: Sequence[RuleDefinition],
    *,
    policy_registry: PolicyRegistry,
    scan_provenance_columns: Sequence[str] = (),
) -> tuple[ResolvedNormalizeRule, ...]:
    """Resolve normalize rules with defaults applied.

    Returns
    -------
    tuple[ResolvedNormalizeRule, ...]
        Rules with defaults applied.
    """
    validate_rule_specs(
        rules,
        registry=policy_registry,
        scan_provenance_columns=scan_provenance_columns,
    )
    return tuple(
        _resolved_rule_from_definition(
            rule,
            policy_registry=policy_registry,
            scan_provenance_columns=scan_provenance_columns,
        )
        for rule in rules
    )


def _resolved_rule_from_definition(
    rule: RuleDefinition,
    *,
    policy_registry: PolicyRegistry,
    scan_provenance_columns: Sequence[str],
) -> ResolvedNormalizeRule:
    defaults = resolve_rule_defaults(
        rule,
        registry=policy_registry,
        scan_provenance_columns=scan_provenance_columns,
    )
    payload = rule.payload if isinstance(rule.payload, NormalizePayload) else None
    query = _normalize_query(payload, rel_ops=rule.rel_ops)
    return ResolvedNormalizeRule(
        name=rule.name,
        output=rule.output,
        inputs=rule.inputs,
        ibis_builder=payload.plan_builder if payload is not None else None,
        query=query,
        evidence=defaults.evidence,
        evidence_output=defaults.evidence_output,
        confidence_policy=defaults.confidence_policy,
        ambiguity_policy=defaults.ambiguity_policy,
        priority=rule.priority,
        emit_rule_meta=rule.emit_rule_meta,
        execution_mode=rule.execution_mode,
    )


def _normalize_query(
    payload: NormalizePayload | None,
    *,
    rel_ops: tuple[RelOpT, ...],
) -> IbisQuerySpec | None:
    if payload is not None and payload.query is not None:
        return payload.query
    if rel_ops:
        return query_spec_from_rel_ops(rel_ops)
    return None


def _should_skip_canonical_sort(
    plan: IbisPlan,
    *,
    contract: Contract,
    ctx: ExecutionContext,
) -> bool:
    if ctx.determinism != DeterminismTier.CANONICAL:
        return False
    if not contract.canonical_sort:
        return False
    if plan.ordering.level != OrderingLevel.EXPLICIT:
        return False
    expected = tuple((key.column, key.order) for key in contract.canonical_sort)
    if ctx.provenance:
        schema = plan.expr.schema().to_pyarrow()
        for col in PROVENANCE_COLS:
            if col in schema.names:
                expected = (*expected, (col, "ascending"))
    return plan.ordering.keys == expected


def run_normalize(
    *,
    plan: IbisPlan,
    post: Iterable[PostFn],
    contract: ContractSpec,
    ctx: ExecutionContext,
    options: NormalizeRunOptions | None = None,
) -> FinalizeResult:
    """Execute a normalize plan with post steps and finalize gate.

    Returns
    -------
    FinalizeResult
        Finalize bundle with good/errors/stats/alignment outputs.

    Raises
    ------
    ValueError
        Raised when the normalize runtime is missing.
    """
    options = options or NormalizeRunOptions()
    runtime = options.runtime
    if runtime is None:
        msg = "Normalize execution requires a NormalizeRuntime."
        raise ValueError(msg)
    _record_sqlglot_plan(
        plan,
        runtime=runtime,
        execution_label=options.execution_label,
    )
    execution = ibis_execution_from_ctx(
        ctx,
        backend=runtime.ibis_backend,
        params=options.params,
        execution_policy=options.execution_policy,
        execution_label=options.execution_label,
    )
    table = materialize_ibis_plan(plan, execution=execution)
    for fn in post:
        table = fn(table, ctx)
    contract_obj = contract.to_contract()
    finalize_spec = options.finalize_spec or NormalizeFinalizeSpec()
    metadata = None
    if finalize_spec.metadata_spec is not None:
        schema_meta = dict(finalize_spec.metadata_spec.schema_metadata)
        schema_meta[b"determinism_tier"] = ctx.determinism.value.encode("utf-8")
        metadata = SchemaMetadataSpec(
            schema_metadata=schema_meta,
            field_metadata=finalize_spec.metadata_spec.field_metadata,
        )
    if finalize_spec.schema_policy is None:
        schema_policy = schema_policy_factory(
            contract.table_schema,
            ctx=ctx,
            options=SchemaPolicyOptions(
                schema=contract_obj.with_versioned_schema(),
                encoding=encoding_policy_from_schema(contract_obj.schema),
                metadata=metadata,
                validation=contract_obj.validation,
            ),
        )
    else:
        schema_policy = _merge_policy_metadata(finalize_spec.schema_policy, metadata)
    finalize_options = FinalizeOptions(
        schema_policy=schema_policy,
        skip_canonical_sort=_should_skip_canonical_sort(plan, contract=contract_obj, ctx=ctx),
    )
    return finalize(table, contract=contract_obj, ctx=ctx, options=finalize_options)


def _merge_policy_metadata(
    policy: SchemaPolicy,
    metadata: SchemaMetadataSpec | None,
) -> SchemaPolicy:
    if metadata is None:
        return policy
    merged = merge_metadata_specs(policy.metadata, metadata)
    return replace(policy, metadata=merged)


def _expand_required_outputs(
    rules: Sequence[RuleDefinition],
    outputs: Sequence[str],
) -> set[str]:
    required = set(outputs)
    available_outputs = {rule.output for rule in rules}
    changed = True
    while changed:
        changed = False
        for rule in rules:
            if rule.output not in required:
                continue
            for input_name in rule.inputs:
                if input_name in available_outputs and input_name not in required:
                    required.add(input_name)
                    changed = True
    return required


def _normalize_output_schema(rule: ResolvedNormalizeRule) -> SchemaLike | None:
    try:
        return dataset_schema(rule.output)
    except KeyError:
        return None


def _normalize_output_schema_map(
    rules: Sequence[ResolvedNormalizeRule],
) -> dict[str, SchemaLike | None]:
    schemas: dict[str, SchemaLike | None] = {}
    for rule in rules:
        output = rule.output
        if output in schemas and schemas[output] is not None:
            continue
        schemas[output] = _normalize_output_schema(rule)
    return schemas


def _order_normalize_rules(
    rules: Sequence[ResolvedNormalizeRule],
    ordered_names: Sequence[str],
) -> tuple[ResolvedNormalizeRule, ...]:
    by_name = {rule.name: rule for rule in rules}
    missing = [name for name in ordered_names if name not in by_name]
    if missing:
        msg = f"Normalize rule schedule missing rules: {missing}."
        raise ValueError(msg)
    return tuple(by_name[name] for name in ordered_names)


def _normalize_table_metadata(output: str) -> Mapping[str, str]:
    return {
        NORMALIZE_STAGE_META: "normalize",
        NORMALIZE_ALIAS_META: dataset_alias(output),
        "normalize_dataset": output,
    }


def compile_normalize_plans_ibis(
    catalog: IbisPlanCatalog,
    *,
    ctx: ExecutionContext,
    options: NormalizeIbisPlanOptions,
) -> dict[str, IbisPlan]:
    """Compile normalize rules into Ibis plans keyed by output dataset.

    Returns
    -------
    dict[str, IbisPlan]
        Mapping of output dataset names to Ibis plans.

    Raises
    ------
    ValueError
        Raised when a rule requires plan execution but no plan is available.
    """
    context = _normalize_ibis_context(catalog, ctx=ctx, options=options)
    plans: dict[str, IbisPlan] = {}
    materialize = set(options.materialize_outputs or ())
    for rule in context.ordered:
        plan = _resolve_rule_plan_ibis(
            rule,
            context.ibis_catalog,
            ctx=ctx,
            backend=options.backend,
        )
        if plan is None:
            if rule.execution_mode == "plan":
                msg = f"Normalize rule {rule.name!r} requires plan execution."
                raise ValueError(msg)
            continue
        if rule.query is not None:
            expr = apply_query_spec(plan.expr, spec=rule.query)
            plan = IbisPlan(expr=expr, ordering=plan.ordering)
        view_name = f"{options.name_prefix}_{rule.output}" if options.name_prefix else rule.output
        plan = register_ibis_view(
            plan.expr,
            options=SourceToIbisOptions(
                backend=options.backend,
                name=view_name,
                ordering=plan.ordering,
                table_metadata=_normalize_table_metadata(rule.output),
            ),
        )
        if rule.output in materialize:
            materialized = materialize_ibis_plan(plan, execution=context.execution)
            plan = table_to_ibis(
                materialized,
                options=SourceToIbisOptions(
                    backend=options.backend,
                    name=view_name,
                    ordering=plan.ordering,
                    overwrite=True,
                    table_metadata=_normalize_table_metadata(rule.output),
                ),
            )
        plans[rule.output] = plan
        context.ibis_catalog.add(rule.output, plan)
        try:
            context.evidence.register(rule.output, dataset_schema(rule.output))
        except KeyError:
            continue
    return plans


def _resolve_rule_plan_ibis(
    rule: ResolvedNormalizeRule,
    catalog: IbisPlanCatalog,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
) -> IbisPlan | None:
    if rule.ibis_builder is None:
        if not rule.inputs:
            return None
        source = catalog.tables.get(rule.inputs[0])
        if source is None:
            return None
        if isinstance(source, DatasetSource):
            msg = f"DatasetSource {rule.inputs[0]!r} must be materialized for normalize."
            raise TypeError(msg)
        if isinstance(source, ViewReference):
            source = materialize_view_reference(backend, source)
        return source_to_ibis(
            source,
            options=SourceToIbisOptions(
                backend=backend,
                name=rule.inputs[0],
                namespace_recorder=namespace_recorder_from_ctx(ctx),
            ),
        )
    builder = resolve_plan_builder_ibis(rule.ibis_builder)
    return builder(catalog, ctx, backend)


__all__ = [
    "NormalizeFinalizeSpec",
    "NormalizeIbisPlanOptions",
    "NormalizeRuleCompilation",
    "NormalizeRunOptions",
    "PostFn",
    "ResolvedNormalizeRule",
    "compile_normalize_plans_ibis",
    "ensure_canonical",
    "ensure_execution_context",
    "resolve_normalize_rules",
    "run_normalize",
]
