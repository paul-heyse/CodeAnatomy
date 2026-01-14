"""Validation helpers for centralized rule definitions."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from typing import cast

import pyarrow as pa
from datafusion import SessionContext
from datafusion.substrait import Serde
from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable
from sqlglot import ErrorLevel

from arrowdsl.compute.kernels import kernel_capability
from arrowdsl.core.context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import SchemaLike
from datafusion_engine.runtime import snapshot_plans
from extract.registry_bundles import bundle
from extract.registry_pipelines import post_kernels_for_postprocess
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.plan import IbisPlan
from ibis_engine.plan_bridge import table_to_ibis
from ibis_engine.query_compiler import IbisQuerySpec, apply_query_spec
from relspec.compiler import rel_plan_for_rule
from relspec.engine import IbisRelPlanCompiler, PlanResolver
from relspec.model import DatasetRef, DedupeKernelSpec, ExplodeListSpec, RelationshipRule
from relspec.plan import rel_plan_signature
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
    missing_schema_columns,
    relation_diff,
    sqlglot_diagnostics,
)


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
    try:
        post_kernels_for_postprocess(kernel_name)
    except KeyError as exc:
        msg = f"Extract rule {name!r} has unknown postprocess kernel: {kernel_name!r}"
        raise ValueError(msg) from exc


def _extract_available_fields(payload: ExtractPayload) -> set[str]:
    available = set(payload.fields) | set(payload.row_fields) | set(payload.row_extras)
    if not payload.bundles:
        return available
    for bundle_name in payload.bundles:
        available.update(field.name for field in bundle(bundle_name).fields)
    return available


def _validate_stages(rule: RuleDefinition) -> None:
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
    diagnostics = sqlglot_diagnostics(expr, backend=backend)
    missing = missing_schema_columns(diagnostics.columns, schema=schema.names)
    if missing:
        msg = f"SQLGlot validation missing columns: {missing}."
        raise ValueError(msg)
    return diagnostics


@dataclass(frozen=True)
class _RuleSqlGlotSource:
    expr: IbisTable
    input_names: tuple[str, ...]
    plan_signature: str | None


@dataclass(frozen=True)
class _SchemaPlanResolver(PlanResolver[IbisPlan]):
    backend: IbisCompilerBackend
    registry: SchemaRegistry

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> IbisPlan:
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
        _ = ref
        _ = ctx


@cache
def _default_sqlglot_backend() -> IbisCompilerBackend:
    return cast("IbisCompilerBackend", build_backend(IbisBackendConfig()))


def rule_sqlglot_diagnostics(
    rules: Sequence[RuleDefinition],
    *,
    backend: IbisCompilerBackend | None = None,
    registry: SchemaRegistry | None = None,
    ctx: ExecutionContext | None = None,
) -> tuple[RuleDiagnostic, ...]:
    """Return SQLGlot diagnostics for rule definitions.

    Returns
    -------
    tuple[RuleDiagnostic, ...]
        SQLGlot diagnostics for rules that can compile to SQLGlot.
    """
    backend = backend or _default_sqlglot_backend()
    registry = registry or GLOBAL_SCHEMA_REGISTRY
    ctx = ctx or execution_context_factory("default")
    diagnostics: list[RuleDiagnostic] = []
    for rule in rules:
        diagnostics.extend(
            _sqlglot_diagnostics_for_rule(
                rule,
                backend=backend,
                registry=registry,
                ctx=ctx,
            )
        )
        diagnostics.extend(_kernel_lane_diagnostics_for_rule(rule, ctx=ctx))
    return tuple(diagnostics)


def _sqlglot_diagnostics_for_rule(
    rule: RuleDefinition,
    *,
    backend: IbisCompilerBackend,
    registry: SchemaRegistry,
    ctx: ExecutionContext,
) -> tuple[RuleDiagnostic, ...]:
    input_names = _rule_input_names(rule)
    if not input_names:
        return ()
    schema_map, union_schema, missing_inputs = _schema_map_for_inputs(
        input_names,
        registry=registry,
    )
    plan_signature = _plan_signature_for_rule(rule)
    if missing_inputs:
        return (
            _schema_missing_diagnostic(
                rule,
                plan_signature=plan_signature,
                missing_inputs=missing_inputs,
            ),
        )
    schema_ddl = None
    if union_schema is not None:
        schema_ddl = _schema_ddl(union_schema, name=f"{rule.name}_inputs")
    source = _rule_sqlglot_source(
        rule,
        backend=backend,
        registry=registry,
        ctx=ctx,
        plan_signature=plan_signature,
    )
    if source is None:
        return ()
    try:
        raw = sqlglot_diagnostics(
            source.expr,
            backend=backend,
            schema_map=schema_map or None,
            normalize=False,
        )
        optimized = sqlglot_diagnostics(
            source.expr,
            backend=backend,
            schema_map=schema_map or None,
            normalize=True,
        )
    except (TypeError, ValueError) as exc:
        return (
            _sqlglot_failure_diagnostic(
                rule,
                plan_signature=source.plan_signature,
                error=exc,
            ),
        )
    diff = relation_diff(raw, optimized)
    extra_metadata = _datafusion_diagnostics_metadata(
        sql=optimized.optimized.sql(unsupported_level=ErrorLevel.RAISE),
        schema_map=schema_map or None,
        ctx=ctx,
    )
    diagnostics: list[RuleDiagnostic] = [
        sqlglot_metadata_diagnostic(
            optimized,
            domain=rule.domain,
            diff=diff,
            options=SqlGlotMetadataDiagnosticOptions(
                rule_name=rule.name,
                plan_signature=source.plan_signature,
                schema_ddl=schema_ddl,
                extra_metadata=extra_metadata,
            ),
        )
    ]
    if union_schema is not None:
        missing = sqlglot_missing_columns_diagnostic(
            optimized,
            domain=rule.domain,
            schema=union_schema,
            options=MissingColumnsDiagnosticOptions(
                rule_name=rule.name,
                plan_signature=source.plan_signature,
                schema_ddl=schema_ddl,
            ),
        )
        if missing is not None:
            diagnostics.append(missing)
    return tuple(diagnostics)


def _kernel_lane_diagnostics_for_rule(
    rule: RuleDefinition,
    *,
    ctx: ExecutionContext,
) -> tuple[RuleDiagnostic, ...]:
    if rule.domain != "cpg":
        return ()
    rel_rule = relationship_rule_from_definition(rule)
    kernel_names = _kernel_names_for_rule(rel_rule)
    if not kernel_names:
        return ()
    plan_signature = _plan_signature_for_rule(rule)
    options = KernelLaneDiagnosticOptions(
        rule_name=rule.name,
        plan_signature=plan_signature,
    )
    diagnostics: list[RuleDiagnostic] = []
    for name in kernel_names:
        capability = kernel_capability(name, ctx=ctx)
        diagnostics.append(
            kernel_lane_diagnostic(
                capability,
                domain=rule.domain,
                options=options,
            )
        )
    return tuple(diagnostics)


def _kernel_names_for_rule(rule: RelationshipRule) -> tuple[str, ...]:
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
    profile = ctx.runtime.datafusion
    if profile is None:
        return {}
    metadata: dict[str, str] = {
        "datafusion_profile": _json_payload(profile.telemetry_payload()),
    }
    if not ctx.debug or not schema_map:
        return metadata
    session = profile.session_context()
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
        substrait = Serde.serialize_bytes(sql, session)
        metadata["substrait_plan_b64"] = base64.b64encode(substrait).decode("ascii")
    except (RuntimeError, TypeError, ValueError) as exc:
        metadata["substrait_error"] = str(exc)
    return metadata


def _register_schema_tables(
    session: SessionContext,
    schema_map: Mapping[str, Mapping[str, str]],
) -> None:
    for name, columns in schema_map.items():
        if not columns:
            continue
        arrays = {
            col: pa.array([], type=pa.type_for_alias(dtype))
            for col, dtype in columns.items()
        }
        session.register_table(name, pa.table(arrays))


def _json_payload(payload: Mapping[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _rule_sqlglot_source(
    rule: RuleDefinition,
    *,
    backend: IbisCompilerBackend,
    registry: SchemaRegistry,
    ctx: ExecutionContext,
    plan_signature: str | None,
) -> _RuleSqlGlotSource | None:
    if rule.domain == "cpg":
        rel_rule = relationship_rule_from_definition(rule)
        rel_plan = rel_plan_for_rule(rel_rule)
        if rel_plan is None:
            return None
        resolver = _SchemaPlanResolver(backend=backend, registry=registry)
        compiler = IbisRelPlanCompiler()
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
    for op in ops:
        if isinstance(op, ScanOp):
            return op.source
    return None


def _rule_input_names(rule: RuleDefinition) -> tuple[str, ...]:
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
        schema_map[name] = {field.name: field.type.to_string() for field in schema}
        for field in schema:
            if field.name in seen:
                continue
            seen.add(field.name)
            fields.append(field)
    union_schema = pa.schema(fields) if fields else None
    return schema_map, union_schema, tuple(missing)


def _schema_ddl(schema: SchemaLike, *, name: str) -> str:
    spec = table_spec_from_schema(name, schema)
    return spec.to_create_table_sql(dialect="ansi")


def _schema_for_dataset(
    name: str,
    *,
    registry: SchemaRegistry,
) -> SchemaLike | None:
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
    arrow_schema = cast("pa.Schema", schema)
    arrays = [pa.array([], type=field.type) for field in arrow_schema]
    table = pa.Table.from_arrays(arrays, schema=arrow_schema)
    return table_to_ibis(table, backend=cast("BaseBackend", backend), name=name)


def _plan_signature_for_rule(rule: RuleDefinition) -> str | None:
    if rule.domain == "cpg":
        rel_rule = relationship_rule_from_definition(rule)
        rel_plan = rel_plan_for_rule(rel_rule)
        if rel_plan is None:
            return None
        return rel_plan_signature(rel_plan)
    if rule.rel_ops:
        return rel_ops_signature(rule.rel_ops)
    return None


def _schema_missing_diagnostic(
    rule: RuleDefinition,
    *,
    plan_signature: str | None,
    missing_inputs: Sequence[str],
) -> RuleDiagnostic:
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
    "rule_sqlglot_diagnostics",
    "validate_rule_definitions",
    "validate_sqlglot_columns",
]
