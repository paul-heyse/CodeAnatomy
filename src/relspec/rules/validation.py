"""Validation helpers for centralized rule definitions."""

from __future__ import annotations

import base64
import importlib
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile

import msgspec
import pyarrow as pa
from datafusion import SessionContext
from datafusion.substrait import Serde
from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from arrowdsl.schema.build import iter_rows_from_table
from datafusion_engine.bridge import sqlglot_to_datafusion
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.extract_bundles import bundle
from datafusion_engine.extract_pipelines import post_kernels_for_postprocess
from datafusion_engine.kernel_registry import kernel_capability
from datafusion_engine.schema_introspection import SchemaIntrospector, table_names_snapshot
from engine.session import EngineSession
from ibis_engine.compiler_checkpoint import compile_checkpoint
from ibis_engine.execution_factory import ibis_backend_from_profile
from ibis_engine.expr_compiler import OperationSupportBackend, unsupported_operations
from ibis_engine.param_tables import ParamTablePolicy, ParamTableSpec
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import IbisQuerySpec, apply_query_spec
from ibis_engine.sources import SourceToIbisOptions, table_to_ibis
from ibis_engine.sql_bridge import ibis_plan_artifacts
from ibis_engine.substrait_bridge import try_ibis_to_substrait_bytes
from obs.diagnostics import DiagnosticsCollector
from relspec.errors import RelspecValidationError
from relspec.execution_bundle import sqlglot_plan_signature
from relspec.list_filter_gate import (
    ListFilterGateError,
    ListFilterGatePolicy,
    validate_no_inline_inlists,
)
from relspec.model import DatasetRef, DedupeKernelSpec, ExplodeListSpec, RelationshipRule
from relspec.param_deps import RuleDependencyReport, dataset_table_names, infer_param_deps
from relspec.pipeline_policy import KernelLanePolicy
from relspec.rules.definitions import (
    ExtractPayload,
    NormalizePayload,
    RelationshipPayload,
    RuleDefinition,
)
from relspec.rules.diagnostics import (
    KernelLaneDiagnosticOptions,
    RuleDiagnostic,
    RuleDiagnosticSeverity,
    SqlGlotMetadataDiagnosticOptions,
    kernel_lane_diagnostic,
    sqlglot_metadata_diagnostic,
)
from relspec.rules.handlers.cpg import relationship_rule_from_definition
from relspec.schema_context import RelspecSchemaContext
from sqlglot_tools.bridge import (
    IbisCompilerBackend,
    SqlGlotDiagnostics,
    SqlGlotDiagnosticsOptions,
    relation_diff,
    sqlglot_diagnostics,
)
from sqlglot_tools.compat import ErrorLevel, Expression
from sqlglot_tools.lineage import (
    LineageExtractionOptions,
    LineagePayload,
    TableRef,
    extract_lineage_payload,
    extract_table_refs,
    lineage_graph_by_output,
    required_columns_by_table,
)
from sqlglot_tools.optimizer import (
    PreflightOptions,
    SchemaMapping,
    SqlGlotPolicy,
    SqlGlotQualificationError,
    ast_to_artifact,
    default_sqlglot_policy,
    emit_preflight_diagnostics,
    parse_sql_strict,
    plan_fingerprint,
    preflight_sql,
    sanitize_templated_sql,
    serialize_ast_artifact,
    sqlglot_policy_snapshot_for,
    sqlglot_sql,
)


def _rel_plan_for_rule(rule: RelationshipRule) -> object:
    module = importlib.import_module("relspec.compiler")
    rel_plan_fn = cast("Callable[..., object]", module.rel_plan_for_rule)
    return rel_plan_fn(rule)


class _IbisRelPlanCompiler(Protocol):
    def compile(self, plan: object, *, ctx: ExecutionContext, resolver: object) -> IbisPlan: ...


def _ibis_rel_plan_compiler() -> _IbisRelPlanCompiler:
    module = importlib.import_module("relspec.engine")
    compiler_cls = cast("type[_IbisRelPlanCompiler]", module.IbisRelPlanCompiler)
    return compiler_cls()


def validate_rule_definitions(
    rules: Sequence[RuleDefinition],
    *,
    extra_checks: Callable[[RuleDefinition], None] | None = None,
) -> None:
    """Validate centralized rule definitions.

    Raises
    ------
    RelspecValidationError
        Raised when definitions are invalid or inconsistent.
    """
    seen: set[str] = set()
    for rule in rules:
        if rule.name in seen:
            msg = f"Duplicate rule definition name: {rule.name!r}."
            raise RelspecValidationError(msg)
        seen.add(rule.name)
        _validate_payload(rule)
        _validate_stages(rule)
        if extra_checks is not None:
            extra_checks(rule)


def _validate_payload(rule: RuleDefinition) -> None:
    """Validate rule payload type by domain.

    Parameters
    ----------
    rule
        Rule definition to validate.

    Raises
    ------
    RelspecValidationError
        Raised when the rule payload is missing or invalid.
    """
    payload = rule.payload
    if rule.domain == "cpg":
        if not isinstance(payload, RelationshipPayload):
            msg = f"RuleDefinition {rule.name!r} missing relationship payload."
            raise RelspecValidationError(msg)
        return
    if rule.domain == "normalize":
        if payload is not None and not isinstance(payload, NormalizePayload):
            msg = f"RuleDefinition {rule.name!r} has invalid normalize payload."
            raise RelspecValidationError(msg)
        return
    if rule.domain == "extract":
        if not isinstance(payload, ExtractPayload):
            msg = f"RuleDefinition {rule.name!r} missing extract payload."
            raise RelspecValidationError(msg)
        _validate_extract_payload(rule.name, payload)
        return


def _validate_extract_payload(name: str, payload: ExtractPayload) -> None:
    """Validate extract payload references against available fields.

    Parameters
    ----------
    name
        Rule name for diagnostics.
    payload
        Extract payload to validate.

    Raises
    ------
    RelspecValidationError
        Raised when referenced fields are missing.
    """
    available = _extract_available_fields(payload)
    for key in payload.join_keys:
        if key not in available:
            msg = f"Extract rule {name!r} join_keys references missing field: {key!r}"
            raise RelspecValidationError(msg)
    for derived in payload.derived_ids:
        for key in derived.required:
            if key not in available:
                msg = (
                    f"Extract rule {name!r} derived id {derived.name!r} "
                    f"references missing field: {key!r}"
                )
                raise RelspecValidationError(msg)
    if payload.postprocess is not None:
        _validate_postprocess_kernel(name, payload.postprocess)


def _validate_postprocess_kernel(name: str, kernel_name: str) -> None:
    """Validate that a postprocess kernel name is registered.

    Parameters
    ----------
    name
        Rule name for diagnostics.
    kernel_name
        Postprocess kernel name to validate.

    Raises
    ------
    RelspecValidationError
        Raised when the postprocess kernel name is unknown.
    """
    try:
        post_kernels_for_postprocess(kernel_name)
    except KeyError as exc:
        msg = f"Extract rule {name!r} has unknown postprocess kernel: {kernel_name!r}"
        raise RelspecValidationError(msg) from exc


def _extract_available_fields(payload: ExtractPayload) -> set[str]:
    """Return available field names for an extract payload.

    Parameters
    ----------
    payload
        Extract payload to inspect.

    Returns
    -------
    set[str]
        Field names available to the payload.
    """
    available = set(payload.fields) | set(payload.row_fields) | set(payload.row_extras)
    if not payload.bundles:
        return available
    for bundle_name in payload.bundles:
        available.update(field.name for field in bundle(bundle_name).fields)
    return available


def _validate_stages(rule: RuleDefinition) -> None:
    """Validate stage names for uniqueness in a rule.

    Parameters
    ----------
    rule
        Rule definition to validate.

    Raises
    ------
    RelspecValidationError
        Raised when stage names are duplicated.
    """
    names = [stage.name for stage in rule.stages]
    if len(set(names)) != len(names):
        msg = f"RuleDefinition {rule.name!r} contains duplicate stages."
        raise RelspecValidationError(msg)


def validate_sqlglot_columns(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    ctx: ExecutionContext,
) -> SqlGlotDiagnostics:
    """Validate SQLGlot-derived columns with DataFusion DESCRIBE.

    Parameters
    ----------
    expr
        Ibis expression to validate.
    backend
        SQLGlot compiler backend.
    ctx
        Execution context with a DataFusion runtime profile.

    Returns
    -------
    SqlGlotDiagnostics
        SQLGlot metadata for the expression.

    Raises
    ------
    RelspecValidationError
        Raised when DataFusion cannot compute the query schema.
    """
    from datafusion_engine.runtime import sql_options_for_profile

    diagnostics = sqlglot_diagnostics(
        expr,
        backend=backend,
        options=SqlGlotDiagnosticsOptions(),
    )
    profile = ctx.runtime.datafusion
    if profile is None:
        msg = "SQLGlot validation requires a DataFusion runtime profile."
        raise RelspecValidationError(msg)
    session = profile.session_context()
    sql_text = sanitize_templated_sql(diagnostics.sql_text_optimized)
    try:
        describe_rows = SchemaIntrospector(
            session,
            sql_options=sql_options_for_profile(profile),
        ).describe_query(sql_text)
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"DataFusion DESCRIBE failed for SQLGlot validation: {exc}"
        raise RelspecValidationError(msg) from exc
    describe_names = _describe_column_names(describe_rows)
    duplicates = _find_duplicate_names(describe_names)
    if duplicates:
        msg = (
            "DataFusion schema contains ambiguous columns for SQLGlot validation: "
            f"{sorted(duplicates)}."
        )
        raise RelspecValidationError(msg)
    return diagnostics


@dataclass(frozen=True)
class _RuleSqlGlotSource:
    """Compiled SQLGlot input context for a rule."""

    expr: IbisTable
    input_names: tuple[str, ...]


RuleSqlGlotSource = _RuleSqlGlotSource


@dataclass(frozen=True)
class SqlGlotDiagnosticsConfig:
    """Optional inputs for SQLGlot diagnostics."""

    backend: IbisCompilerBackend | None = None
    schema_context: RelspecSchemaContext | None = None
    ctx: ExecutionContext | None = None
    engine_session: EngineSession | None = None
    param_table_specs: Sequence[ParamTableSpec] = ()
    param_table_policy: ParamTablePolicy | None = None
    list_filter_gate_policy: ListFilterGatePolicy | None = None
    kernel_lane_policy: KernelLanePolicy | None = None


@dataclass(frozen=True)
class SqlGlotDiagnosticsContext:
    """Resolved SQLGlot diagnostics context."""

    backend: IbisCompilerBackend
    schema_context: RelspecSchemaContext
    ctx: ExecutionContext
    schema_map: SchemaMapping | None
    schema_map_hash: str | None
    policy: SqlGlotPolicy
    param_specs: Mapping[str, ParamTableSpec]
    param_policy: ParamTablePolicy
    list_filter_gate_policy: ListFilterGatePolicy | None
    kernel_lane_policy: KernelLanePolicy | None


@dataclass(frozen=True)
class SqlGlotRuleContext:
    """Prepared context for SQLGlot diagnostics on a single rule."""

    rule: RuleDefinition
    source: _RuleSqlGlotSource
    plan_signature: str | None


@dataclass(frozen=True)
class _SchemaPlanResolver:
    """Plan resolver that returns empty tables from DataFusion schemas."""

    backend: IbisCompilerBackend
    schema_context: RelspecSchemaContext

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> IbisPlan:
        """Resolve a dataset reference to an empty-schema Ibis plan.

        Parameters
        ----------
        ref
            Dataset reference to resolve.
        ctx
            Execution context (unused).

        Returns
        -------
        IbisPlan
            Plan built from the DataFusion schema context.

        Raises
        ------
        KeyError
            Raised when the dataset schema is missing from the context.
        """
        _ = ctx
        schema = _schema_for_dataset(ref.name, schema_context=self.schema_context)
        if schema is None:
            msg = f"SQLGlot validation missing schema for dataset {ref.name!r}."
            raise KeyError(msg)
        plan = _ibis_plan_from_schema(
            ref.name,
            schema=schema,
            backend=self.backend,
        )
        if ref.query is not None:
            expr = apply_query_spec(plan.expr, spec=ref.query)
            return IbisPlan(expr=expr, ordering=plan.ordering)
        return plan

    @staticmethod
    def telemetry(ref: DatasetRef, *, ctx: ExecutionContext) -> None:
        """Return telemetry for schema-backed plans (none)."""
        _ = ref
        _ = ctx


def build_sqlglot_context(
    config: SqlGlotDiagnosticsConfig | None = None,
) -> SqlGlotDiagnosticsContext:
    """Resolve SQLGlot diagnostics context defaults.

    Returns
    -------
    SqlGlotDiagnosticsContext
        Resolved diagnostics context.

    Raises
    ------
    RelspecValidationError
        Raised when no engine session or execution context is available.
    """
    config = config or SqlGlotDiagnosticsConfig()
    ctx = config.ctx or (config.engine_session.ctx if config.engine_session is not None else None)
    if ctx is None:
        msg = "SQLGlot diagnostics require an execution context."
        raise RelspecValidationError(msg)
    if config.backend is not None:
        backend = config.backend
    elif config.engine_session is not None:
        backend = cast("IbisCompilerBackend", config.engine_session.ibis_backend)
    else:
        profile = ctx.runtime.datafusion
        if profile is None:
            from datafusion_engine.runtime import DataFusionRuntimeProfile

            profile = DataFusionRuntimeProfile()
        backend = cast("IbisCompilerBackend", ibis_backend_from_profile(profile))
    schema_context = _resolve_schema_context(config, ctx=ctx)
    schema_map = schema_context.schema_map()
    schema_map_hash = schema_context.schema_map_fingerprint()
    spec_map = {spec.logical_name: spec for spec in config.param_table_specs}
    param_policy = config.param_table_policy or ParamTablePolicy()
    return SqlGlotDiagnosticsContext(
        backend=backend,
        schema_context=schema_context,
        ctx=ctx,
        schema_map=schema_map,
        schema_map_hash=schema_map_hash,
        policy=default_sqlglot_policy(),
        param_specs=spec_map,
        param_policy=param_policy,
        list_filter_gate_policy=config.list_filter_gate_policy,
        kernel_lane_policy=config.kernel_lane_policy,
    )


def _resolve_schema_context(
    config: SqlGlotDiagnosticsConfig,
    *,
    ctx: ExecutionContext,
) -> RelspecSchemaContext:
    """Resolve the schema context for SQLGlot diagnostics.

    Parameters
    ----------
    config
        SQLGlot diagnostics configuration.
    ctx
        Execution context providing runtime profile access.

    Returns
    -------
    RelspecSchemaContext
        Resolved DataFusion schema context.
    """
    from datafusion_engine.runtime import DataFusionRuntimeProfile

    if config.schema_context is not None:
        return config.schema_context
    if config.engine_session is not None:
        return RelspecSchemaContext.from_engine_session(config.engine_session)
    profile = ctx.runtime.datafusion
    if profile is not None:
        return RelspecSchemaContext.from_session(profile.session_context())
    runtime_profile = DataFusionRuntimeProfile()
    return RelspecSchemaContext.from_session(runtime_profile.session_context())


def rule_sqlglot_diagnostics(
    rules: Sequence[RuleDefinition],
    *,
    config: SqlGlotDiagnosticsConfig | None = None,
) -> tuple[RuleDiagnostic, ...]:
    """Return SQLGlot diagnostics for rule definitions.

    Returns
    -------
    tuple[RuleDiagnostic, ...]
        SQLGlot diagnostics for rules that can compile to SQLGlot.
    """
    try:
        context = build_sqlglot_context(config)
    except (ImportError, RelspecValidationError, ValueError):
        return ()
    diagnostics: list[RuleDiagnostic] = []
    for rule in rules:
        diagnostics.extend(
            _sqlglot_diagnostics_for_rule(
                rule,
                context=context,
            )
        )
        diagnostics.extend(
            _kernel_lane_diagnostics_for_rule(
                rule,
                context=context,
            )
        )
    return tuple(diagnostics)


def _sqlglot_diagnostics_for_rule(
    rule: RuleDefinition,
    *,
    context: SqlGlotDiagnosticsContext,
) -> tuple[RuleDiagnostic, ...]:
    """Generate SQLGlot diagnostics for a single rule.

    Parameters
    ----------
    rule
        Rule definition to analyze.
    context
        Diagnostics context bundle.

    Returns
    -------
    tuple[RuleDiagnostic, ...]
        Diagnostics emitted for the rule.
    """
    rule_ctx, prep_diag = _prepare_sqlglot_rule_context(rule, context=context)
    if prep_diag:
        return prep_diag
    if rule_ctx is None:
        return ()
    raw = _compile_rule_sqlglot(rule_ctx, context=context, normalize=False)
    if isinstance(raw, RuleDiagnostic):
        return (raw,)
    optimized = _compile_rule_sqlglot(rule_ctx, context=context, normalize=True)
    if isinstance(optimized, RuleDiagnostic):
        return (optimized,)
    plan_signature = _sqlglot_plan_signature(
        optimized.optimized,
        context=context,
    )
    updated_ctx = replace(rule_ctx, plan_signature=plan_signature)
    return _build_rule_diagnostics(updated_ctx, context=context, raw=raw, optimized=optimized)


def _prepare_sqlglot_rule_context(
    rule: RuleDefinition,
    *,
    context: SqlGlotDiagnosticsContext,
) -> tuple[SqlGlotRuleContext | None, tuple[RuleDiagnostic, ...]]:
    """Prepare SQLGlot compilation context for a rule.

    Parameters
    ----------
    rule
        Rule definition to analyze.
    context
        Diagnostics context bundle.

    Returns
    -------
    tuple[SqlGlotRuleContext | None, tuple[RuleDiagnostic, ...]]
        Prepared context and any diagnostics emitted during preparation.
    """
    input_names = _rule_input_names(rule)
    if not input_names:
        return None, ()
    missing_inputs = _missing_inputs_for_inputs(
        input_names,
        schema_context=context.schema_context,
    )
    if missing_inputs:
        diagnostic = _schema_missing_diagnostic(
            rule,
            plan_signature=None,
            missing_inputs=missing_inputs,
        )
        return None, (diagnostic,)
    source = _rule_sqlglot_source(
        rule,
        backend=context.backend,
        schema_context=context.schema_context,
        ctx=context.ctx,
    )
    if source is None:
        return None, ()
    missing_ops = unsupported_operations(
        source.expr,
        backend=cast("OperationSupportBackend", context.backend),
    )
    if missing_ops:
        diagnostic = _unsupported_ops_diagnostic(
            rule,
            plan_signature=None,
            missing_ops=missing_ops,
        )
        return None, (diagnostic,)
    return (
        SqlGlotRuleContext(
            rule=rule,
            source=source,
            plan_signature=None,
        ),
        (),
    )


def _compile_rule_sqlglot(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
    normalize: bool,
) -> SqlGlotDiagnostics | RuleDiagnostic:
    """Compile a rule expression to SQLGlot diagnostics.

    Parameters
    ----------
    rule_ctx
        Prepared SQLGlot rule context.
    context
        Diagnostics context bundle.
    normalize
        Whether to normalize the SQLGlot expression.

    Returns
    -------
    SqlGlotDiagnostics | RuleDiagnostic
        Diagnostics or a failure diagnostic.
    """
    options = SqlGlotDiagnosticsOptions(
        normalize=normalize,
        policy=context.policy,
        schema_map=context.schema_map,
    )
    try:
        return sqlglot_diagnostics(
            rule_ctx.source.expr,
            backend=context.backend,
            options=options,
        )
    except (TypeError, ValueError) as exc:
        return _sqlglot_failure_diagnostic(
            rule_ctx.rule,
            plan_signature=rule_ctx.plan_signature,
            error=exc,
        )


def _sqlglot_plan_signature(
    expr: Expression,
    *,
    context: SqlGlotDiagnosticsContext,
) -> str | None:
    """Return a canonical SQLGlot plan signature for an expression.

    Returns
    -------
    str | None
        Plan signature when computed, otherwise ``None``.
    """
    try:
        return sqlglot_plan_signature(
            expr,
            policy=context.policy,
            schema_map_hash=context.schema_map_hash,
        )
    except (TypeError, ValueError):
        return None


def _build_rule_diagnostics(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
    raw: SqlGlotDiagnostics,
    optimized: SqlGlotDiagnostics,
) -> tuple[RuleDiagnostic, ...]:
    """Build diagnostics for a rule from SQLGlot outputs.

    Parameters
    ----------
    rule_ctx
        Prepared SQLGlot rule context.
    context
        Diagnostics context bundle.
    raw
        Raw SQLGlot diagnostics.
    optimized
        Optimized SQLGlot diagnostics.

    Returns
    -------
    tuple[RuleDiagnostic, ...]
        Diagnostics produced for the rule.
    """
    diff = relation_diff(raw, optimized)
    diagnostics: list[RuleDiagnostic] = []
    diagnostics.extend(
        _list_filter_gate_diagnostics(
            rule_ctx,
            context=context,
            optimized=optimized,
        )
    )
    param_names, table_refs = _param_metadata(
        rule_ctx,
        optimized=optimized,
        context=context,
        diagnostics=diagnostics,
    )
    extra_metadata = _datafusion_diagnostics_metadata(
        sql=optimized.optimized.sql(unsupported_level=ErrorLevel.RAISE),
        ctx=context.ctx,
    )
    describe_error = extra_metadata.get("df_describe_error")
    preflight_diagnostics, preflight_payload = _preflight_rule_diagnostics(
        rule_ctx,
        context=context,
        sql=optimized.optimized.sql(unsupported_level=ErrorLevel.RAISE),
    )
    if preflight_payload is not None:
        extra_metadata["preflight_payload"] = preflight_payload
    if context.schema_map_hash is not None:
        extra_metadata["schema_map_hash"] = context.schema_map_hash
    extra_metadata.update(_rule_ir_metadata(rule_ctx, context=context))
    metadata = _final_sqlglot_metadata(
        extra_metadata,
        param_names=param_names,
        table_refs=table_refs,
        param_policy=context.param_policy,
    )
    metadata.update(_provider_metadata_payload(rule_ctx, context=context))
    metadata.update(_required_columns_metadata(rule_ctx, context=context))
    if (lineage_payload := _lineage_graph_metadata(rule_ctx, context=context)) is not None:
        metadata["lineage_graph"] = lineage_payload
    if (plan_hash := _plan_hash_metadata(rule_ctx, context=context)) is not None:
        metadata["plan_hash"] = plan_hash
    substrait_b64 = _substrait_payload(rule_ctx, context=context)
    if substrait_b64 is not None:
        metadata["substrait_plan_b64"] = substrait_b64
    diagnostics.append(
        sqlglot_metadata_diagnostic(
            optimized,
            domain=rule_ctx.rule.domain,
            diff=diff,
            options=SqlGlotMetadataDiagnosticOptions(
                rule_name=rule_ctx.rule.name,
                plan_signature=rule_ctx.plan_signature,
                schema_ddl=None,
                extra_metadata=metadata,
            ),
        )
    )
    diagnostics.extend(preflight_diagnostics)
    _record_sqlglot_artifact(
        rule_ctx,
        context=context,
        raw=raw,
        optimized=optimized,
        substrait_b64=substrait_b64,
    )
    drift = _contract_schema_drift_diagnostic(
        rule_ctx,
        context=context,
        sql=optimized.optimized.sql(unsupported_level=ErrorLevel.RAISE),
    )
    if drift is not None:
        diagnostics.append(drift)
    if describe_error is not None:
        diagnostics.append(
            RuleDiagnostic(
                domain=rule_ctx.rule.domain,
                template=None,
                rule_name=rule_ctx.rule.name,
                severity="warning",
                message="DataFusion DESCRIBE failed for rule SQL.",
                plan_signature=rule_ctx.plan_signature,
                metadata={"df_describe_error": str(describe_error)},
            )
        )
    return tuple(diagnostics)


def _sqlglot_lineage_payload(
    expr: Expression,
    *,
    context: SqlGlotDiagnosticsContext,
) -> LineagePayload | None:
    try:
        return extract_lineage_payload(
            expr,
            options=LineageExtractionOptions(
                schema=cast("Mapping[str, Mapping[str, str]] | None", context.schema_map),
                dialect=context.policy.write_dialect,
            ),
        )
    except (RuntimeError, TypeError, ValueError):
        return None


def _sqlglot_ast_payload(
    expr: Expression,
    *,
    sql: str,
    policy: SqlGlotPolicy,
) -> bytes | None:
    try:
        return serialize_ast_artifact(ast_to_artifact(expr, sql=sql, policy=policy))
    except (RuntimeError, TypeError, ValueError, msgspec.EncodeError):
        return None


def _substrait_payload(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
) -> str | None:
    runtime_profile = context.ctx.runtime.datafusion
    sink = None
    if runtime_profile is not None:
        diagnostics_sink = runtime_profile.diagnostics_sink
        if isinstance(diagnostics_sink, DiagnosticsCollector):
            sink = diagnostics_sink
    try:
        plan_bytes = try_ibis_to_substrait_bytes(rule_ctx.source.expr, diagnostics_sink=sink)
    except (RuntimeError, TypeError, ValueError):
        return None
    if plan_bytes is None:
        return None
    return base64.b64encode(plan_bytes).decode("ascii")


def _record_sqlglot_artifact(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
    raw: SqlGlotDiagnostics,
    optimized: SqlGlotDiagnostics,
    substrait_b64: str | None,
) -> None:
    runtime_profile = context.ctx.runtime.datafusion
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    policy_snapshot = sqlglot_policy_snapshot_for(context.policy)
    plan_hash = rule_ctx.plan_signature
    if plan_hash is None:
        try:
            plan_hash = plan_fingerprint(
                optimized.optimized,
                dialect=context.policy.write_dialect,
                policy_hash=policy_snapshot.policy_hash,
                schema_map_hash=context.schema_map_hash,
            )
        except (RuntimeError, TypeError, ValueError):
            plan_hash = None
    lineage = _sqlglot_lineage_payload(optimized.optimized, context=context)
    ast_payload = _sqlglot_ast_payload(
        optimized.optimized,
        sql=optimized.sql_text_optimized,
        policy=context.policy,
    )
    rule_ir = _rule_ir_metadata(rule_ctx, context=context)
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "domain": rule_ctx.rule.domain,
        "rule_name": rule_ctx.rule.name,
        "output_dataset": rule_ctx.rule.output,
        "plan_signature": rule_ctx.plan_signature,
        "plan_hash": plan_hash,
        "sql_dialect": optimized.sql_dialect,
        "sql_text_raw": raw.sql_text_raw,
        "sql_text_optimized": optimized.sql_text_optimized,
        "tables": list(optimized.tables),
        "columns": list(optimized.columns),
        "identifiers": list(optimized.identifiers),
        "ast_repr": optimized.ast_repr,
        "sqlglot_ast": ast_payload,
        "policy_hash": policy_snapshot.policy_hash,
        "policy_rules_hash": policy_snapshot.rules_hash,
        "schema_map_hash": context.schema_map_hash,
        "canonical_fingerprint": (lineage.canonical_fingerprint if lineage is not None else None),
        "lineage_tables": list(lineage.tables) if lineage is not None else None,
        "lineage_columns": list(lineage.columns) if lineage is not None else None,
        "lineage_scopes": list(lineage.scopes) if lineage is not None else None,
        "lineage_qualified_sql": lineage.qualified_sql if lineage is not None else None,
        "substrait_plan_b64": substrait_b64,
        "ibis_decompile": rule_ir.get("ibis_decompile"),
        "ibis_sql": rule_ir.get("ibis_sql"),
        "ibis_sql_pretty": rule_ir.get("ibis_sql_pretty"),
    }
    runtime_profile.diagnostics_sink.record_artifact("relspec_sqlglot_plan_v1", payload)


def _required_columns_metadata(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
) -> dict[str, str]:
    metadata: dict[str, str] = {}
    try:
        required = required_columns_by_table(
            rule_ctx.source.expr,
            backend=context.backend,
        )
    except (TypeError, ValueError):
        return metadata
    for table, columns in sorted(required.items()):
        if columns:
            metadata[f"required_columns:{table}"] = ",".join(columns)
    return metadata


def _preflight_rule_diagnostics(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
    sql: str,
) -> tuple[tuple[RuleDiagnostic, ...], str | None]:
    result = preflight_sql(
        sql,
        options=PreflightOptions(
            schema=context.schema_map,
            dialect=context.policy.write_dialect,
            strict=True,
            policy=context.policy,
        ),
    )
    payload_text = _stable_payload_text(emit_preflight_diagnostics(result))
    metadata: dict[str, str] = {
        "preflight_payload": payload_text,
    }
    if result.errors:
        metadata["preflight_errors"] = "; ".join(result.errors)
        return (
            (
                RuleDiagnostic(
                    domain=rule_ctx.rule.domain,
                    template=None,
                    rule_name=rule_ctx.rule.name,
                    severity="error",
                    message="SQL preflight validation failed.",
                    plan_signature=rule_ctx.plan_signature,
                    metadata=metadata,
                ),
            ),
            payload_text,
        )
    if result.warnings:
        metadata["preflight_warnings"] = "; ".join(result.warnings)
        return (
            (
                RuleDiagnostic(
                    domain=rule_ctx.rule.domain,
                    template=None,
                    rule_name=rule_ctx.rule.name,
                    severity="warning",
                    message="SQL preflight validation warnings.",
                    plan_signature=rule_ctx.plan_signature,
                    metadata=metadata,
                ),
            ),
            payload_text,
        )
    return (), payload_text


def _provider_metadata_payload(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for name in rule_ctx.source.input_names:
        provider = context.schema_context.table_provider_metadata(name)
        if provider is None:
            continue
        if provider.file_format:
            metadata[f"provider:{name}:format"] = provider.file_format
        if provider.storage_location:
            metadata[f"provider:{name}:location"] = provider.storage_location
        if provider.ddl_fingerprint:
            metadata[f"provider:{name}:ddl_fingerprint"] = provider.ddl_fingerprint
        if provider.unbounded:
            metadata[f"provider:{name}:unbounded"] = "true"
    return metadata


def _contract_schema_drift_diagnostic(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
    sql: str,
) -> RuleDiagnostic | None:
    """Return a diagnostic when output schema drifts from the contract.

    Parameters
    ----------
    rule_ctx
        Prepared SQLGlot rule context.
    context
        Diagnostics context bundle.
    sql
        SQL string to describe via DataFusion.

    Returns
    -------
    RuleDiagnostic | None
        Diagnostic describing schema drift when detected.
    """
    from datafusion_engine.runtime import sql_options_for_profile

    payload = rule_ctx.rule.payload
    if not isinstance(payload, RelationshipPayload) or payload.contract_name is None:
        return None
    expected_schema = context.schema_context.dataset_schema(payload.contract_name)
    if expected_schema is None:
        return None
    profile = context.ctx.runtime.datafusion
    if profile is None:
        return None
    sql_text = sanitize_templated_sql(sql)
    try:
        rows = SchemaIntrospector(
            profile.session_context(),
            sql_options=sql_options_for_profile(profile),
        ).describe_query(sql_text)
    except (RuntimeError, TypeError, ValueError) as exc:
        return RuleDiagnostic(
            domain=rule_ctx.rule.domain,
            template=None,
            rule_name=rule_ctx.rule.name,
            severity="warning",
            message="DataFusion DESCRIBE failed while validating contract schema.",
            plan_signature=rule_ctx.plan_signature,
            metadata={"df_describe_error": str(exc)},
        )
    actual_columns = _describe_column_names(rows)
    expected_columns = tuple(expected_schema.names)
    missing = sorted(name for name in expected_columns if name not in actual_columns)
    extra = sorted(name for name in actual_columns if name not in expected_columns)
    if not missing and not extra:
        return None
    metadata = {
        "contract": payload.contract_name,
        "expected_columns": ",".join(expected_columns),
        "actual_columns": ",".join(actual_columns),
    }
    if missing:
        metadata["missing_columns"] = ",".join(missing)
    if extra:
        metadata["extra_columns"] = ",".join(extra)
    return RuleDiagnostic(
        domain=rule_ctx.rule.domain,
        template=None,
        rule_name=rule_ctx.rule.name,
        severity="warning",
        message="Rule output schema does not match the contract.",
        plan_signature=rule_ctx.plan_signature,
        metadata=metadata,
    )


def _describe_column_names(rows: Sequence[Mapping[str, object]]) -> tuple[str, ...]:
    names: list[str] = []
    for row in rows:
        for key in ("column_name", "name", "column"):
            value = row.get(key)
            if value is not None:
                names.append(str(value))
                break
    return tuple(names)


def _find_duplicate_names(names: Sequence[str]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for name in names:
        if name in seen:
            duplicates.add(name)
        seen.add(name)
    return duplicates


def _plan_hash_metadata(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
) -> str | None:
    try:
        checkpoint = compile_checkpoint(
            rule_ctx.source.expr,
            backend=context.backend,
        )
    except (TypeError, ValueError):
        return None
    return checkpoint.plan_hash


def _lineage_graph_metadata(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
) -> str | None:
    try:
        lineage = lineage_graph_by_output(
            rule_ctx.source.expr,
            backend=context.backend,
        )
    except (TypeError, ValueError):
        return None
    if not lineage:
        return None
    return _stable_repr(lineage)


def _list_filter_gate_diagnostics(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
    optimized: SqlGlotDiagnostics,
) -> tuple[RuleDiagnostic, ...]:
    """Run list-filter gate checks and return diagnostics.

    Parameters
    ----------
    rule_ctx
        Prepared SQLGlot rule context.
    context
        Diagnostics context bundle.
    optimized
        Optimized SQLGlot diagnostics.

    Returns
    -------
    tuple[RuleDiagnostic, ...]
        Diagnostics emitted for list-filter gate failures.
    """
    try:
        validate_no_inline_inlists(
            rule_name=rule_ctx.rule.name,
            sg_ast=optimized.optimized,
            param_policy=context.param_policy,
            policy=context.list_filter_gate_policy,
        )
    except ListFilterGateError as exc:
        return (
            RuleDiagnostic(
                domain=rule_ctx.rule.domain,
                template=None,
                rule_name=rule_ctx.rule.name,
                severity="error",
                message=str(exc),
                plan_signature=rule_ctx.plan_signature,
            ),
        )
    return ()


def _param_metadata(
    rule_ctx: SqlGlotRuleContext,
    *,
    optimized: SqlGlotDiagnostics,
    context: SqlGlotDiagnosticsContext,
    diagnostics: list[RuleDiagnostic],
) -> tuple[set[str], tuple[TableRef, ...]]:
    """Collect parameter metadata and append diagnostics as needed.

    Parameters
    ----------
    rule_ctx
        Prepared SQLGlot rule context.
    optimized
        Optimized SQLGlot diagnostics.
    context
        Diagnostics context bundle.
    diagnostics
        Diagnostics list to append to.

    Returns
    -------
    tuple[set[str], tuple[TableRef, ...]]
        Parameter table names and table references.
    """
    table_refs = tuple(extract_table_refs(optimized.optimized))
    param_deps = infer_param_deps(table_refs, policy=context.param_policy)
    param_names = {dep.logical_name for dep in param_deps}
    if context.param_specs:
        missing = sorted(name for name in param_names if name not in context.param_specs)
        if missing:
            diagnostics.append(
                RuleDiagnostic(
                    domain=rule_ctx.rule.domain,
                    template=None,
                    rule_name=rule_ctx.rule.name,
                    severity="error",
                    message="Rule references undeclared param tables.",
                    plan_signature=rule_ctx.plan_signature,
                    metadata={"missing_params": ",".join(missing)},
                )
            )
    collisions = _param_table_collisions(context, param_names=param_names)
    if collisions:
        diagnostics.append(
            RuleDiagnostic(
                domain=rule_ctx.rule.domain,
                template=None,
                rule_name=rule_ctx.rule.name,
                severity="error",
                message="Param table names collide with registered datasets.",
                plan_signature=rule_ctx.plan_signature,
                metadata={"param_table_collisions": ",".join(collisions)},
            )
        )
    return param_names, table_refs


def _param_table_collisions(
    context: SqlGlotDiagnosticsContext,
    *,
    param_names: set[str],
) -> tuple[str, ...]:
    profile = context.ctx.runtime.datafusion
    if profile is None or not param_names:
        return ()
    from datafusion_engine.runtime import sql_options_for_profile

    sql_options = sql_options_for_profile(profile)
    session = profile.session_context()
    table_names = table_names_snapshot(session, sql_options=sql_options)
    collisions = [
        f"{context.param_policy.prefix}{name}"
        for name in sorted(param_names)
        if f"{context.param_policy.prefix}{name}" in table_names
    ]
    return tuple(collisions)


def _final_sqlglot_metadata(
    base_metadata: Mapping[str, str],
    *,
    param_names: set[str],
    table_refs: Sequence[TableRef],
    param_policy: ParamTablePolicy,
) -> dict[str, str]:
    """Build final SQLGlot metadata including param table annotations.

    Parameters
    ----------
    base_metadata
        Base metadata from SQLGlot diagnostics.
    param_names
        Param table logical names referenced.
    table_refs
        Table references extracted from SQLGlot.
    param_policy
        Parameter table policy for filtering.

    Returns
    -------
    dict[str, str]
        Final metadata mapping.
    """
    metadata = dict(base_metadata)
    if param_names:
        metadata["param_tables"] = ",".join(sorted(param_names))
    non_param_tables = dataset_table_names(table_refs, policy=param_policy)
    if non_param_tables:
        metadata["dataset_tables"] = ",".join(non_param_tables)
    return metadata


def _rule_ir_metadata(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
) -> dict[str, str]:
    metadata: dict[str, str] = {}
    ibis_metadata = ibis_plan_artifacts(rule_ctx.source.expr, dialect="datafusion")
    metadata.update(
        {key: str(value) for key, value in ibis_metadata.items() if value is not None}
    )
    try:
        checkpoint = compile_checkpoint(
            rule_ctx.source.expr,
            backend=context.backend,
            dialect="datafusion",
        )
    except (TypeError, ValueError):
        return metadata
    policy = default_sqlglot_policy()
    policy = replace(policy, read_dialect="datafusion", write_dialect="datafusion")
    metadata["sqlglot_plan_hash"] = checkpoint.plan_hash
    metadata["sqlglot_policy_hash"] = checkpoint.policy_hash
    metadata["ibis_sql_pretty"] = sqlglot_sql(
        checkpoint.normalized,
        policy=policy,
        pretty=True,
    )
    return metadata


def rule_ir_metadata(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
) -> dict[str, str]:
    """Return IR metadata for a SQLGlot rule context.

    Returns
    -------
    dict[str, str]
        Rule IR metadata payload.
    """
    return _rule_ir_metadata(rule_ctx, context=context)


def rule_dependency_reports(
    rules: Sequence[RuleDefinition],
    *,
    config: SqlGlotDiagnosticsConfig | None = None,
) -> tuple[RuleDependencyReport, ...]:
    """Return dependency reports for rule definitions.

    Returns
    -------
    tuple[RuleDependencyReport, ...]
        Rule dependency reports with param and dataset tables.
    """
    try:
        context = build_sqlglot_context(config)
    except (ImportError, ValueError):
        return ()
    reports: list[RuleDependencyReport] = []
    for rule in rules:
        input_names = _rule_input_names(rule)
        if not input_names:
            continue
        missing_inputs = _missing_inputs_for_inputs(
            input_names,
            schema_context=context.schema_context,
        )
        if missing_inputs:
            continue
        source = _rule_sqlglot_source(
            rule,
            backend=context.backend,
            schema_context=context.schema_context,
            ctx=context.ctx,
        )
        if source is None:
            continue
        try:
            optimized = sqlglot_diagnostics(
                source.expr,
                backend=context.backend,
                options=SqlGlotDiagnosticsOptions(
                    normalize=True,
                    policy=context.policy,
                    schema_map=context.schema_map,
                ),
            )
        except (TypeError, ValueError):
            continue
        table_refs = extract_table_refs(optimized.optimized)
        param_deps = infer_param_deps(table_refs, policy=context.param_policy)
        param_names = {dep.logical_name for dep in param_deps}
        if context.param_specs:
            missing = sorted(name for name in param_names if name not in context.param_specs)
            if missing:
                continue
        reports.append(
            RuleDependencyReport(
                rule_name=rule.name,
                param_tables=tuple(sorted(param_names)),
                dataset_tables=dataset_table_names(table_refs, policy=context.param_policy),
            )
        )
    return tuple(reports)


def _kernel_lane_diagnostics_for_rule(
    rule: RuleDefinition,
    *,
    context: SqlGlotDiagnosticsContext,
) -> tuple[RuleDiagnostic, ...]:
    """Return kernel lane diagnostics for a relationship rule.

    Parameters
    ----------
    rule
        Rule definition to analyze.
    context
        Diagnostics context used for kernel capability checks and signatures.

    Returns
    -------
    tuple[RuleDiagnostic, ...]
        Kernel lane diagnostics.
    """
    if rule.domain != "cpg":
        return ()
    rel_rule = relationship_rule_from_definition(rule)
    kernel_names = _kernel_names_for_rule(rel_rule)
    if not kernel_names:
        return ()
    plan_signature = rule_sqlglot_signature(rule, context=context)
    base_metadata: dict[str, str] = {}
    kernel_lane_policy = context.kernel_lane_policy
    if kernel_lane_policy is not None:
        allowed = ",".join(lane.value for lane in kernel_lane_policy.allowed)
        base_metadata["policy_allowed_lanes"] = allowed
        base_metadata["policy_action"] = kernel_lane_policy.on_violation
    diagnostics: list[RuleDiagnostic] = []
    for name in kernel_names:
        capability = kernel_capability(name, ctx=context.ctx)
        violation = False
        severity: RuleDiagnosticSeverity = "warning"
        if not capability.available:
            violation = True
            severity = "error"
        elif kernel_lane_policy is not None:
            violation = capability.lane not in kernel_lane_policy.allowed
            if violation and kernel_lane_policy.on_violation == "error":
                severity = "error"
        extra_metadata = dict(base_metadata)
        if kernel_lane_policy is not None:
            extra_metadata["policy_violation"] = str(violation).lower()
        extra_metadata["available"] = str(capability.available).lower()
        options = KernelLaneDiagnosticOptions(
            rule_name=rule.name,
            plan_signature=plan_signature,
            severity=severity,
            extra_metadata=extra_metadata,
        )
        diagnostics.append(
            kernel_lane_diagnostic(
                capability,
                domain=rule.domain,
                options=options,
            )
        )
    return tuple(diagnostics)


def _kernel_names_for_rule(rule: RelationshipRule) -> tuple[str, ...]:
    """Collect kernel names referenced by a relationship rule.

    Parameters
    ----------
    rule
        Relationship rule to inspect.

    Returns
    -------
    tuple[str, ...]
        Kernel names referenced by the rule.
    """
    names: list[str] = []
    if rule.interval_align is not None:
        names.append("interval_align")
    if rule.winner_select is not None:
        names.append("winner_select")
    for spec in rule.post_kernels:
        if isinstance(spec, ExplodeListSpec):
            names.append("explode_list")
        elif isinstance(spec, DedupeKernelSpec):
            names.append("dedupe")
    return tuple(dict.fromkeys(names))


def _datafusion_diagnostics_metadata(
    *,
    sql: str,
    ctx: ExecutionContext,
) -> dict[str, str]:
    """Collect DataFusion diagnostics metadata for SQL.

    Parameters
    ----------
    sql
        SQL string to compile.
    ctx
        Execution context containing DataFusion runtime profile.

    Returns
    -------
    dict[str, str]
        Diagnostics metadata for DataFusion compilation.
    """
    profile = ctx.runtime.datafusion
    if profile is None:
        return {}
    sanitized_sql = sanitize_templated_sql(sql)
    metadata: dict[str, str] = {
        "datafusion_profile": _json_payload(profile.telemetry_payload_v1()),
    }
    if sanitized_sql != sql:
        metadata["sql_sanitized"] = "true"
    if not ctx.debug:
        return metadata
    session = profile.session_context()
    metadata.update(
        _datafusion_debug_metadata(
            session=session,
            sanitized_sql=sanitized_sql,
            profile=profile,
        )
    )
    return metadata


def _datafusion_debug_metadata(
    *,
    session: SessionContext,
    sanitized_sql: str,
    profile: DataFusionRuntimeProfile,
) -> dict[str, str]:
    from datafusion_engine.runtime import (
        settings_snapshot_for_profile,
        snapshot_plans,
        sql_options_for_profile,
    )

    metadata: dict[str, str] = {}
    try:
        options = profile.compile_options(options=DataFusionCompileOptions())
        expr = parse_sql_strict(sanitized_sql, dialect=options.dialect)
        df = sqlglot_to_datafusion(expr, ctx=session, options=options)
        plans = snapshot_plans(df)
        metadata["df_logical_plan"] = str(plans["logical"])
        metadata["df_optimized_plan"] = str(plans["optimized"])
        metadata["df_physical_plan"] = str(plans["physical"])
        metadata["dfschema_tree"] = str(df.schema())
    except (RuntimeError, TypeError, ValueError) as exc:
        metadata["df_plan_error"] = str(exc)
    sql_options = sql_options_for_profile(profile)
    _try_add_metadata(
        metadata,
        key="df_explain",
        build=lambda: _stable_repr(
            session.sql_with_options(
                f"EXPLAIN {sanitized_sql}",
                sql_options,
            )
            .to_arrow_table()
            .to_pylist()
        ),
    )
    _try_add_metadata(
        metadata,
        key="df_describe",
        build=lambda: _describe_snapshot_payload(
            SchemaIntrospector(session, sql_options=sql_options).describe_query(sanitized_sql)
        ),
    )
    _try_add_metadata(
        metadata,
        key="df_parameters",
        build=lambda: _stable_repr(
            SchemaIntrospector(session, sql_options=sql_options).parameters_snapshot()
        ),
    )
    _try_add_metadata(
        metadata,
        key="df_settings",
        build=lambda: _settings_snapshot_payload(settings_snapshot_for_profile(profile, session)),
    )
    _try_add_metadata(
        metadata,
        key="substrait_plan_b64",
        build=lambda: base64.b64encode(Serde.serialize_bytes(sanitized_sql, session)).decode(
            "ascii"
        ),
    )
    return metadata


def _try_add_metadata(
    metadata: dict[str, str],
    *,
    key: str,
    build: Callable[[], str],
) -> None:
    try:
        metadata[key] = build()
    except (RuntimeError, TypeError, ValueError) as exc:
        metadata[f"{key}_error"] = str(exc)


def _json_payload(payload: Mapping[str, object]) -> str:
    """Serialize a payload mapping to stable text.

    Parameters
    ----------
    payload
        Mapping to serialize.

    Returns
    -------
    str
        Stable string payload.
    """
    return _stable_repr(payload)


def _settings_snapshot_payload(table: pa.Table) -> str:
    """Serialize DataFusion settings snapshot table to text.

    Parameters
    ----------
    table
        Settings snapshot table.

    Returns
    -------
    str
        Stable string payload.
    """
    rows: list[dict[str, str]] = []
    for row in iter_rows_from_table(table):
        name = row.get("name")
        value = row.get("value")
        if name is None or value is None:
            continue
        rows.append({"name": str(name), "value": str(value)})
    return _stable_repr(rows)


def _describe_snapshot_payload(rows: Sequence[Mapping[str, object]]) -> str:
    """Serialize DataFusion DESCRIBE rows to text.

    Parameters
    ----------
    rows
        Rows returned by ``DESCRIBE`` for a SQL query.

    Returns
    -------
    str
        Stable string representation of the describe rows.
    """
    payload = [{str(key): str(value) for key, value in row.items()} for row in rows]
    return _stable_repr(payload)


def _stable_repr(value: object) -> str:
    if isinstance(value, Mapping):
        items = ", ".join(
            f"{_stable_repr(key)}:{_stable_repr(val)}"
            for key, val in sorted(value.items(), key=lambda item: str(item[0]))
        )
        return f"{{{items}}}"
    if isinstance(value, (list, tuple, set)):
        rendered = [_stable_repr(item) for item in value]
        if isinstance(value, set):
            rendered = sorted(rendered)
        items = ", ".join(rendered)
        bracket = "()" if isinstance(value, tuple) else "[]"
        return f"{bracket[0]}{items}{bracket[1]}"
    return repr(value)


def _rule_sqlglot_source(
    rule: RuleDefinition,
    *,
    backend: IbisCompilerBackend,
    schema_context: RelspecSchemaContext,
    ctx: ExecutionContext,
) -> _RuleSqlGlotSource | None:
    """Build a SQLGlot source expression for a rule when possible.

    Parameters
    ----------
    rule
        Rule definition to compile.
    backend
        Compiler backend for Ibis.
    schema_context
        DataFusion schema context for dataset lookups.
    ctx
        Execution context.

    Returns
    -------
    _RuleSqlGlotSource | None
        SQLGlot source for the rule when available.

    Raises
    ------
    KeyError
        Raised when a schema is missing for a referenced dataset.
    """
    if rule.domain == "cpg":
        rel_rule = relationship_rule_from_definition(rule)
        rel_plan = _rel_plan_for_rule(rel_rule)
        if rel_plan is None:
            return None
        resolver = _SchemaPlanResolver(backend=backend, schema_context=schema_context)
        compiler = _ibis_rel_plan_compiler()
        ibis_plan = compiler.compile(rel_plan, ctx=ctx, resolver=resolver)
        inputs = tuple(ref.name for ref in rel_rule.inputs)
        return _RuleSqlGlotSource(expr=ibis_plan.expr, input_names=inputs)
    query_source = _query_spec_source(rule)
    if query_source is None:
        return None
    query_spec, source = query_source
    schema = _schema_for_dataset(source, schema_context=schema_context)
    if schema is None:
        msg = f"SQLGlot validation missing schema for dataset {source!r}."
        raise KeyError(msg)
    plan = _ibis_plan_from_schema(source, schema=schema, backend=backend)
    expr = apply_query_spec(plan.expr, spec=query_spec)
    return _RuleSqlGlotSource(expr=expr, input_names=(source,))


def _query_spec_source(rule: RuleDefinition) -> tuple[IbisQuerySpec, str] | None:
    """Resolve a query spec and source dataset for a rule.

    Parameters
    ----------
    rule
        Rule definition to inspect.

    Returns
    -------
    tuple[IbisQuerySpec, str] | None
        Query spec and source dataset when available.
    """
    payload = rule.payload
    if (
        isinstance(payload, NormalizePayload)
        and payload.query is not None
        and len(rule.inputs) == 1
    ):
        return payload.query, rule.inputs[0]
    return None


def _rule_input_names(rule: RuleDefinition) -> tuple[str, ...]:
    """Return input dataset names for a rule.

    Parameters
    ----------
    rule
        Rule definition to inspect.

    Returns
    -------
    tuple[str, ...]
        Input dataset names for the rule.
    """
    return rule.inputs


def _missing_inputs_for_inputs(
    inputs: Sequence[str],
    *,
    schema_context: RelspecSchemaContext,
) -> tuple[str, ...]:
    """Return missing dataset names for SQLGlot diagnostics.

    Parameters
    ----------
    inputs
        Input dataset names.
    schema_context
        DataFusion schema context for dataset lookups.

    Returns
    -------
    tuple[str, ...]
        Missing input dataset names.
    """
    return tuple(
        name for name in dict.fromkeys(inputs) if schema_context.dataset_schema(name) is None
    )


def _schema_for_dataset(
    name: str,
    *,
    schema_context: RelspecSchemaContext,
) -> SchemaLike | None:
    """Resolve a dataset schema from the DataFusion context.

    Parameters
    ----------
    name
        Dataset name.
    schema_context
        DataFusion schema context for dataset lookups.

    Returns
    -------
    SchemaLike | None
        Schema for the dataset when available.
    """
    return schema_context.dataset_schema(name)


def _ibis_plan_from_schema(
    name: str,
    *,
    schema: SchemaLike,
    backend: IbisCompilerBackend,
) -> IbisPlan:
    """Build an Ibis plan backed by an empty table for a schema.

    Parameters
    ----------
    name
        Dataset name.
    schema
        Schema to materialize.
    backend
        Ibis backend to register the table with.

    Returns
    -------
    IbisPlan
        Plan wrapping an empty table of the schema.
    """
    arrow_schema = cast("pa.Schema", schema)
    arrays = [pa.array([], type=field.type) for field in arrow_schema]
    table = pa.Table.from_arrays(arrays, schema=arrow_schema)
    return table_to_ibis(
        table,
        options=SourceToIbisOptions(
            backend=cast("BaseBackend", backend),
            name=name,
        ),
    )


def rule_sqlglot_signature(
    rule: RuleDefinition,
    *,
    context: SqlGlotDiagnosticsContext,
) -> str | None:
    """Return a canonical SQLGlot plan signature for a rule when available.

    Returns
    -------
    str | None
        Plan signature when available.
    """
    try:
        source = _rule_sqlglot_source(
            rule,
            backend=context.backend,
            schema_context=context.schema_context,
            ctx=context.ctx,
        )
    except (KeyError, TypeError, ValueError):
        return None
    if source is None:
        return None
    try:
        optimized = sqlglot_diagnostics(
            source.expr,
            backend=context.backend,
            options=SqlGlotDiagnosticsOptions(
                normalize=True,
                policy=context.policy,
                schema_map=context.schema_map,
            ),
        )
    except (TypeError, ValueError):
        return None
    return _sqlglot_plan_signature(optimized.optimized, context=context)


def _schema_missing_diagnostic(
    rule: RuleDefinition,
    *,
    plan_signature: str | None,
    missing_inputs: Sequence[str],
) -> RuleDiagnostic:
    """Build a diagnostic for missing input schemas.

    Parameters
    ----------
    rule
        Rule definition being validated.
    plan_signature
        Optional plan signature for the rule.
    missing_inputs
        Missing input dataset names.

    Returns
    -------
    RuleDiagnostic
        Diagnostic describing missing schemas.
    """
    return RuleDiagnostic(
        domain=rule.domain,
        template=None,
        rule_name=rule.name,
        severity="warning",
        message="SQLGlot validation skipped; missing input schemas.",
        plan_signature=plan_signature,
        metadata={"missing_inputs": ",".join(missing_inputs)},
    )


def _unsupported_ops_diagnostic(
    rule: RuleDefinition,
    *,
    plan_signature: str | None,
    missing_ops: Sequence[str],
) -> RuleDiagnostic:
    return RuleDiagnostic(
        domain=rule.domain,
        template=None,
        rule_name=rule.name,
        severity="error",
        message="Ibis backend missing operations required by this rule.",
        plan_signature=plan_signature,
        metadata={"missing_ops": ",".join(missing_ops)},
    )


def _sqlglot_failure_diagnostic(
    rule: RuleDefinition,
    *,
    plan_signature: str | None,
    error: Exception,
) -> RuleDiagnostic:
    """Build a diagnostic for SQLGlot compilation failures.

    Parameters
    ----------
    rule
        Rule definition being validated.
    plan_signature
        Optional plan signature for the rule.
    error
        Compilation error captured.

    Returns
    -------
    RuleDiagnostic
        Diagnostic describing the failure.
    """
    metadata: dict[str, str] = {"error": str(error)}
    if isinstance(error, SqlGlotQualificationError):
        metadata["qualification_payload"] = _stable_payload_text(error.payload)
    return RuleDiagnostic(
        domain=rule.domain,
        template=None,
        rule_name=rule.name,
        severity="error",
        message="SQLGlot validation failed to compile the rule expression.",
        plan_signature=plan_signature,
        metadata=metadata,
    )


def _stable_payload_text(payload: object) -> str:
    if isinstance(payload, Mapping):
        items = ", ".join(
            f"{_stable_payload_text(key)}:{_stable_payload_text(value)}"
            for key, value in sorted(payload.items(), key=lambda item: str(item[0]))
        )
        return f"{{{items}}}"
    if isinstance(payload, (list, tuple, set)):
        rendered = [_stable_payload_text(item) for item in payload]
        if isinstance(payload, set):
            rendered = sorted(rendered)
        items = ", ".join(rendered)
        bracket = "()" if isinstance(payload, tuple) else "[]"
        return f"{bracket[0]}{items}{bracket[1]}"
    return str(payload)


__all__ = [
    "RuleSqlGlotSource",
    "rule_dependency_reports",
    "rule_ir_metadata",
    "rule_sqlglot_diagnostics",
    "rule_sqlglot_signature",
    "validate_rule_definitions",
    "validate_sqlglot_columns",
]
