"""Validation helpers for centralized rule definitions."""

from __future__ import annotations

import base64
import importlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, cast

import pyarrow as pa
from datafusion import SessionContext
from datafusion.substrait import Serde
from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable
from sqlglot import ErrorLevel

from arrowdsl.compute.kernels import kernel_capability
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from datafusion_engine.runtime import snapshot_plans
from engine.session import EngineSession
from extract.registry_bundles import bundle
from extract.registry_pipelines import post_kernels_for_postprocess
from ibis_engine.compiler_checkpoint import compile_checkpoint
from ibis_engine.lineage import lineage_graph_by_output, required_columns_by_table
from ibis_engine.param_tables import (
    ParamTablePolicy,
    ParamTableSpec,
    qualified_param_table_name,
)
from ibis_engine.params_bridge import list_param_names_from_rel_ops
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import IbisQuerySpec, apply_query_spec
from ibis_engine.sources import table_to_ibis
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
    MissingColumnsDiagnosticOptions,
    RuleDiagnostic,
    RuleDiagnosticSeverity,
    SqlGlotMetadataDiagnosticOptions,
    kernel_lane_diagnostic,
    sqlglot_metadata_diagnostic,
    sqlglot_missing_columns_diagnostic,
)
from relspec.rules.handlers.cpg import relationship_rule_from_definition
from relspec.rules.rel_ops import (
    RelOpT,
    ScanOp,
    query_spec_from_rel_ops,
    rel_ops_signature,
)
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY, SchemaRegistry, table_spec_from_schema
from sqlglot_tools.bridge import (
    IbisCompilerBackend,
    SqlGlotDiagnostics,
    SqlGlotDiagnosticsOptions,
    missing_schema_columns,
    relation_diff,
    sqlglot_diagnostics,
)
from sqlglot_tools.lineage import TableRef, extract_table_refs


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


def _rel_plan_signature(plan: object) -> str:
    module = importlib.import_module("relspec.plan")
    sig_fn = cast("Callable[..., str]", module.rel_plan_signature)
    return sig_fn(plan)


def validate_rule_definitions(
    rules: Sequence[RuleDefinition],
    *,
    extra_checks: Callable[[RuleDefinition], None] | None = None,
) -> None:
    """Validate centralized rule definitions.

    Raises
    ------
    ValueError
        Raised when definitions are invalid or inconsistent.
    """
    seen: set[str] = set()
    for rule in rules:
        if rule.name in seen:
            msg = f"Duplicate rule definition name: {rule.name!r}."
            raise ValueError(msg)
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
    ValueError
        Raised when the rule payload is missing or invalid.
    """
    payload = rule.payload
    if rule.domain == "cpg":
        if not isinstance(payload, RelationshipPayload):
            msg = f"RuleDefinition {rule.name!r} missing relationship payload."
            raise ValueError(msg)
        return
    if rule.domain == "normalize":
        if payload is not None and not isinstance(payload, NormalizePayload):
            msg = f"RuleDefinition {rule.name!r} has invalid normalize payload."
            raise ValueError(msg)
        return
    if rule.domain == "extract":
        if not isinstance(payload, ExtractPayload):
            msg = f"RuleDefinition {rule.name!r} missing extract payload."
            raise ValueError(msg)
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
    ValueError
        Raised when referenced fields are missing.
    """
    available = _extract_available_fields(payload)
    for key in payload.join_keys:
        if key not in available:
            msg = f"Extract rule {name!r} join_keys references missing field: {key!r}"
            raise ValueError(msg)
    for derived in payload.derived_ids:
        for key in derived.required:
            if key not in available:
                msg = (
                    f"Extract rule {name!r} derived id {derived.name!r} "
                    f"references missing field: {key!r}"
                )
                raise ValueError(msg)
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
    ValueError
        Raised when the postprocess kernel name is unknown.
    """
    try:
        post_kernels_for_postprocess(kernel_name)
    except KeyError as exc:
        msg = f"Extract rule {name!r} has unknown postprocess kernel: {kernel_name!r}"
        raise ValueError(msg) from exc


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
    ValueError
        Raised when stage names are duplicated.
    """
    names = [stage.name for stage in rule.stages]
    if len(set(names)) != len(names):
        msg = f"RuleDefinition {rule.name!r} contains duplicate stages."
        raise ValueError(msg)


def validate_sqlglot_columns(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    schema: SchemaLike,
) -> SqlGlotDiagnostics:
    """Validate SQLGlot-derived columns against a schema.

    Returns
    -------
    SqlGlotDiagnostics
        SQLGlot metadata for the expression.

    Raises
    ------
    ValueError
        Raised when SQLGlot references missing columns.
    """
    diagnostics = sqlglot_diagnostics(
        expr,
        backend=backend,
        options=SqlGlotDiagnosticsOptions(),
    )
    missing = missing_schema_columns(diagnostics.columns, schema=schema.names)
    if missing:
        msg = f"SQLGlot validation missing columns: {missing}."
        raise ValueError(msg)
    return diagnostics


@dataclass(frozen=True)
class _RuleSqlGlotSource:
    """Compiled SQLGlot input context for a rule."""

    expr: IbisTable
    input_names: tuple[str, ...]
    plan_signature: str | None


@dataclass(frozen=True)
class SqlGlotDiagnosticsConfig:
    """Optional inputs for SQLGlot diagnostics."""

    backend: IbisCompilerBackend | None = None
    registry: SchemaRegistry | None = None
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
    registry: SchemaRegistry
    ctx: ExecutionContext
    param_specs: Mapping[str, ParamTableSpec]
    param_policy: ParamTablePolicy
    list_filter_gate_policy: ListFilterGatePolicy | None
    kernel_lane_policy: KernelLanePolicy | None


@dataclass(frozen=True)
class SqlGlotRuleContext:
    """Prepared context for SQLGlot diagnostics on a single rule."""

    rule: RuleDefinition
    source: _RuleSqlGlotSource
    schema_map: Mapping[str, Mapping[str, str]] | None
    union_schema: pa.Schema | None
    schema_ddl: str | None
    plan_signature: str | None


@dataclass(frozen=True)
class _SchemaPlanResolver:
    """Plan resolver that returns empty tables from schema registry."""

    backend: IbisCompilerBackend
    registry: SchemaRegistry

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
            Plan built from the schema registry.

        Raises
        ------
        KeyError
            Raised when the dataset schema is missing from the registry.
        """
        _ = ctx
        schema = _schema_for_dataset(ref.name, registry=self.registry)
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
    ValueError
        Raised when no engine session or execution context is available.
    """
    config = config or SqlGlotDiagnosticsConfig()
    if config.backend is not None:
        backend = config.backend
    elif config.engine_session is not None:
        backend = cast("IbisCompilerBackend", config.engine_session.ibis_backend)
    else:
        msg = "SQLGlot diagnostics require an EngineSession or compiler backend."
        raise ValueError(msg)
    registry = config.registry or GLOBAL_SCHEMA_REGISTRY
    ctx = config.ctx or (config.engine_session.ctx if config.engine_session is not None else None)
    if ctx is None:
        msg = "SQLGlot diagnostics require an execution context."
        raise ValueError(msg)
    spec_map = {spec.logical_name: spec for spec in config.param_table_specs}
    param_policy = config.param_table_policy or ParamTablePolicy()
    return SqlGlotDiagnosticsContext(
        backend=backend,
        registry=registry,
        ctx=ctx,
        param_specs=spec_map,
        param_policy=param_policy,
        list_filter_gate_policy=config.list_filter_gate_policy,
        kernel_lane_policy=config.kernel_lane_policy,
    )


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
    except (ImportError, ValueError):
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
                ctx=context.ctx,
                kernel_lane_policy=context.kernel_lane_policy,
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
    return _build_rule_diagnostics(rule_ctx, context=context, raw=raw, optimized=optimized)


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
    schema_map, union_schema, missing_inputs = _schema_map_for_inputs(
        input_names,
        registry=context.registry,
    )
    plan_signature = _plan_signature_for_rule(rule)
    if missing_inputs:
        diagnostic = _schema_missing_diagnostic(
            rule,
            plan_signature=plan_signature,
            missing_inputs=missing_inputs,
        )
        return None, (diagnostic,)
    schema_ddl = None
    if union_schema is not None:
        schema_ddl = _schema_ddl(union_schema, name=f"{rule.name}_inputs")
    source = _rule_sqlglot_source(
        rule,
        backend=context.backend,
        registry=context.registry,
        ctx=context.ctx,
        plan_signature=plan_signature,
    )
    if source is None:
        return None, ()
    return (
        SqlGlotRuleContext(
            rule=rule,
            source=source,
            schema_map=schema_map or None,
            union_schema=union_schema,
            schema_ddl=schema_ddl,
            plan_signature=plan_signature,
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
        schema_map=rule_ctx.schema_map,
        normalize=normalize,
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
            plan_signature=rule_ctx.source.plan_signature,
            error=exc,
        )


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
        schema_map=rule_ctx.schema_map,
        ctx=context.ctx,
    )
    metadata = _final_sqlglot_metadata(
        extra_metadata,
        param_names=param_names,
        table_refs=table_refs,
        param_policy=context.param_policy,
    )
    metadata.update(_required_columns_metadata(rule_ctx, context=context))
    if (lineage_payload := _lineage_graph_metadata(rule_ctx, context=context)) is not None:
        metadata["lineage_graph"] = lineage_payload
    if (plan_hash := _plan_hash_metadata(rule_ctx, context=context)) is not None:
        metadata["plan_hash"] = plan_hash
    diagnostics.append(
        sqlglot_metadata_diagnostic(
            optimized,
            domain=rule_ctx.rule.domain,
            diff=diff,
            options=SqlGlotMetadataDiagnosticOptions(
                rule_name=rule_ctx.rule.name,
                plan_signature=rule_ctx.source.plan_signature,
                schema_ddl=rule_ctx.schema_ddl,
                extra_metadata=metadata,
            ),
        )
    )
    if rule_ctx.union_schema is not None:
        missing = sqlglot_missing_columns_diagnostic(
            optimized,
            domain=rule_ctx.rule.domain,
            schema=rule_ctx.union_schema,
            options=MissingColumnsDiagnosticOptions(
                rule_name=rule_ctx.rule.name,
                plan_signature=rule_ctx.source.plan_signature,
                schema_ddl=rule_ctx.schema_ddl,
            ),
        )
        if missing is not None:
            diagnostics.append(missing)
    return tuple(diagnostics)


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
            schema_map=rule_ctx.schema_map,
        )
    except (TypeError, ValueError):
        return metadata
    for table, columns in sorted(required.items()):
        if columns:
            metadata[f"required_columns:{table}"] = ",".join(columns)
    return metadata


def _plan_hash_metadata(
    rule_ctx: SqlGlotRuleContext,
    *,
    context: SqlGlotDiagnosticsContext,
) -> str | None:
    try:
        checkpoint = compile_checkpoint(
            rule_ctx.source.expr,
            backend=context.backend,
            schema_map=rule_ctx.schema_map,
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
            schema_map=rule_ctx.schema_map,
        )
    except (TypeError, ValueError):
        return None
    if not lineage:
        return None
    return json.dumps(lineage, ensure_ascii=True, sort_keys=True)


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
                plan_signature=rule_ctx.source.plan_signature,
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
    param_names.update(list_param_names_from_rel_ops(rule_ctx.rule.rel_ops))
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
                    plan_signature=rule_ctx.source.plan_signature,
                    metadata={"missing_params": ",".join(missing)},
                )
            )
    return param_names, table_refs


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
        qualified = [
            qualified_param_table_name(param_policy, name, scope_key=None)
            for name in sorted(param_names)
        ]
        metadata["param_tables_qualified"] = ",".join(qualified)
    non_param_tables = dataset_table_names(table_refs, policy=param_policy)
    if non_param_tables:
        metadata["dataset_tables"] = ",".join(non_param_tables)
    return metadata


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
        schema_map, _union_schema, missing_inputs = _schema_map_for_inputs(
            input_names,
            registry=context.registry,
        )
        if missing_inputs:
            continue
        plan_signature = _plan_signature_for_rule(rule)
        source = _rule_sqlglot_source(
            rule,
            backend=context.backend,
            registry=context.registry,
            ctx=context.ctx,
            plan_signature=plan_signature,
        )
        if source is None:
            continue
        try:
            optimized = sqlglot_diagnostics(
                source.expr,
                backend=context.backend,
                options=SqlGlotDiagnosticsOptions(
                    schema_map=schema_map or None,
                    normalize=True,
                ),
            )
        except (TypeError, ValueError):
            continue
        table_refs = extract_table_refs(optimized.optimized)
        param_deps = infer_param_deps(table_refs, policy=context.param_policy)
        param_names = {dep.logical_name for dep in param_deps}
        param_names.update(list_param_names_from_rel_ops(rule.rel_ops))
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
    ctx: ExecutionContext,
    kernel_lane_policy: KernelLanePolicy | None,
) -> tuple[RuleDiagnostic, ...]:
    """Return kernel lane diagnostics for a relationship rule.

    Parameters
    ----------
    rule
        Rule definition to analyze.
    ctx
        Execution context used for kernel capability checks.
    kernel_lane_policy
        Optional kernel lane policy to enforce.

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
    plan_signature = _plan_signature_for_rule(rule)
    base_metadata: dict[str, str] = {}
    if kernel_lane_policy is not None:
        allowed = ",".join(lane.value for lane in kernel_lane_policy.allowed)
        base_metadata["policy_allowed_lanes"] = allowed
        base_metadata["policy_action"] = kernel_lane_policy.on_violation
    diagnostics: list[RuleDiagnostic] = []
    for name in kernel_names:
        capability = kernel_capability(name, ctx=ctx)
        violation = False
        severity: RuleDiagnosticSeverity = "warning"
        if kernel_lane_policy is not None:
            violation = capability.lane not in kernel_lane_policy.allowed
            if violation and kernel_lane_policy.on_violation == "error":
                severity = "error"
        extra_metadata = dict(base_metadata)
        if kernel_lane_policy is not None:
            extra_metadata["policy_violation"] = str(violation).lower()
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
    schema_map: Mapping[str, Mapping[str, str]] | None,
    ctx: ExecutionContext,
) -> dict[str, str]:
    """Collect DataFusion diagnostics metadata for SQL.

    Parameters
    ----------
    sql
        SQL string to compile.
    schema_map
        Optional schema map for table registration.
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
    metadata: dict[str, str] = {
        "datafusion_profile": _json_payload(profile.telemetry_payload()),
    }
    if not ctx.debug:
        return metadata
    session = profile.session_context()
    if schema_map:
        try:
            _register_schema_tables(session, schema_map)
            df = session.sql(sql)
            plans = snapshot_plans(df)
            metadata["df_logical_plan"] = str(plans["logical"])
            metadata["df_optimized_plan"] = str(plans["optimized"])
            metadata["df_physical_plan"] = str(plans["physical"])
        except (RuntimeError, TypeError, ValueError) as exc:
            metadata["df_plan_error"] = str(exc)
    try:
        settings = profile.settings_snapshot(session)
        metadata["df_settings"] = _settings_snapshot_payload(settings)
    except (RuntimeError, TypeError, ValueError) as exc:
        metadata["df_settings_error"] = str(exc)
    try:
        substrait = Serde.serialize_bytes(sql, session)
        metadata["substrait_plan_b64"] = base64.b64encode(substrait).decode("ascii")
    except (RuntimeError, TypeError, ValueError) as exc:
        metadata["substrait_error"] = str(exc)
    return metadata


def _register_schema_tables(
    session: SessionContext,
    schema_map: Mapping[str, Mapping[str, str]],
) -> None:
    """Register empty tables in a DataFusion session for schemas.

    Parameters
    ----------
    session
        DataFusion session context.
    schema_map
        Mapping of table name to column name/type strings.
    """
    for name, columns in schema_map.items():
        if not columns:
            continue
        arrays = {
            col: pa.array([], type=pa.type_for_alias(dtype)) for col, dtype in columns.items()
        }
        session.register_table(name, pa.table(arrays))


def _json_payload(payload: Mapping[str, object]) -> str:
    """Serialize a payload mapping to JSON.

    Parameters
    ----------
    payload
        Mapping to serialize.

    Returns
    -------
    str
        JSON string payload.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _settings_snapshot_payload(table: pa.Table) -> str:
    """Serialize DataFusion settings snapshot table to JSON.

    Parameters
    ----------
    table
        Settings snapshot table.

    Returns
    -------
    str
        JSON string payload.
    """
    rows: list[dict[str, str]] = []
    for row in table.to_pylist():
        name = row.get("name")
        value = row.get("value")
        if name is None or value is None:
            continue
        rows.append({"name": str(name), "value": str(value)})
    return json.dumps(rows, sort_keys=True, separators=(",", ":"))


def _rule_sqlglot_source(
    rule: RuleDefinition,
    *,
    backend: IbisCompilerBackend,
    registry: SchemaRegistry,
    ctx: ExecutionContext,
    plan_signature: str | None,
) -> _RuleSqlGlotSource | None:
    """Build a SQLGlot source expression for a rule when possible.

    Parameters
    ----------
    rule
        Rule definition to compile.
    backend
        Compiler backend for Ibis.
    registry
        Schema registry for dataset lookups.
    ctx
        Execution context.
    plan_signature
        Optional plan signature for diagnostics.

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
        resolver = _SchemaPlanResolver(backend=backend, registry=registry)
        compiler = _ibis_rel_plan_compiler()
        ibis_plan = compiler.compile(rel_plan, ctx=ctx, resolver=resolver)
        inputs = tuple(ref.name for ref in rel_rule.inputs)
        return _RuleSqlGlotSource(
            expr=ibis_plan.expr,
            input_names=inputs,
            plan_signature=plan_signature,
        )
    query_source = _query_spec_source(rule)
    if query_source is None:
        return None
    query_spec, source = query_source
    schema = _schema_for_dataset(source, registry=registry)
    if schema is None:
        msg = f"SQLGlot validation missing schema for dataset {source!r}."
        raise KeyError(msg)
    plan = _ibis_plan_from_schema(source, schema=schema, backend=backend)
    expr = apply_query_spec(plan.expr, spec=query_spec)
    return _RuleSqlGlotSource(
        expr=expr,
        input_names=(source,),
        plan_signature=plan_signature,
    )


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
    if rule.rel_ops:
        source = _scan_source(rule.rel_ops)
        if source is None:
            return None
        try:
            spec = query_spec_from_rel_ops(rule.rel_ops)
        except ValueError:
            return None
        if spec is None:
            return None
        return spec, source
    payload = rule.payload
    if (
        isinstance(payload, NormalizePayload)
        and payload.query is not None
        and len(rule.inputs) == 1
    ):
        return payload.query, rule.inputs[0]
    return None


def _scan_source(ops: Sequence[RelOpT]) -> str | None:
    """Return the scan source name from rel ops.

    Parameters
    ----------
    ops
        Relational operations to inspect.

    Returns
    -------
    str | None
        Source name when a scan op is present.
    """
    for op in ops:
        if isinstance(op, ScanOp):
            return op.source
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
    if rule.inputs:
        return rule.inputs
    source = _scan_source(rule.rel_ops)
    if source is not None:
        return (source,)
    return ()


def _schema_map_for_inputs(
    inputs: Sequence[str],
    *,
    registry: SchemaRegistry,
) -> tuple[dict[str, dict[str, str]], pa.Schema | None, tuple[str, ...]]:
    """Build a schema map and union schema for input datasets.

    Parameters
    ----------
    inputs
        Input dataset names.
    registry
        Schema registry for dataset lookups.

    Returns
    -------
    tuple[dict[str, dict[str, str]], pa.Schema | None, tuple[str, ...]]
        Schema map, union schema, and missing input names.
    """
    fields: list[pa.Field] = []
    seen: set[str] = set()
    schema_map: dict[str, dict[str, str]] = {}
    missing: list[str] = []
    for name in dict.fromkeys(inputs):
        spec = registry.dataset_specs.get(name)
        if spec is None:
            missing.append(name)
            continue
        schema = cast("pa.Schema", spec.schema())
        schema_map[name] = {field.name: str(field.type) for field in schema}
        for field in schema:
            if field.name in seen:
                continue
            seen.add(field.name)
            fields.append(field)
    union_schema = pa.schema(fields) if fields else None
    return schema_map, union_schema, tuple(missing)


def _schema_ddl(schema: SchemaLike, *, name: str) -> str:
    """Render a CREATE TABLE DDL statement for a schema.

    Parameters
    ----------
    schema
        Schema to render.
    name
        Table name for the DDL.

    Returns
    -------
    str
        CREATE TABLE statement.
    """
    spec = table_spec_from_schema(name, schema)
    return spec.to_create_table_sql(dialect="datafusion")


def _schema_for_dataset(
    name: str,
    *,
    registry: SchemaRegistry,
) -> SchemaLike | None:
    """Resolve a dataset schema from the registry.

    Parameters
    ----------
    name
        Dataset name.
    registry
        Schema registry for dataset lookups.

    Returns
    -------
    SchemaLike | None
        Schema for the dataset when available.
    """
    spec = registry.dataset_specs.get(name)
    if spec is None:
        return None
    return spec.schema()


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
    return table_to_ibis(table, backend=cast("BaseBackend", backend), name=name)


def _plan_signature_for_rule(rule: RuleDefinition) -> str | None:
    """Return a plan signature for a rule when available.

    Parameters
    ----------
    rule
        Rule definition to inspect.

    Returns
    -------
    str | None
        Plan signature when available.
    """
    if rule.domain == "cpg":
        rel_rule = relationship_rule_from_definition(rule)
        rel_plan = _rel_plan_for_rule(rel_rule)
        if rel_plan is None:
            return None
        return _rel_plan_signature(rel_plan)
    if rule.rel_ops:
        return rel_ops_signature(rule.rel_ops)
    return None


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
    return RuleDiagnostic(
        domain=rule.domain,
        template=None,
        rule_name=rule.name,
        severity="error",
        message="SQLGlot validation failed to compile the rule expression.",
        plan_signature=plan_signature,
        metadata={"error": str(error)},
    )


__all__ = [
    "rule_dependency_reports",
    "rule_sqlglot_diagnostics",
    "validate_rule_definitions",
    "validate_sqlglot_columns",
]
