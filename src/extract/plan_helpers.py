"""Shared helpers for building extract plans."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, is_dataclass

from ibis.backends import BaseBackend

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, SchemaLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.plan.scan_io import rows_to_table
from arrowdsl.plan.schema_utils import plan_output_columns
from arrowdsl.plan_helpers import column_or_null_expr
from arrowdsl.schema.schema import empty_table
from arrowdsl.spec.codec import parse_string_tuple
from arrowdsl.spec.expr_ir import ExprIR, ExprRegistry, ExprSpec, expr_spec_from_json
from config import AdapterMode
from extract.evidence_plan import EvidencePlan
from extract.registry_pipelines import pipeline_spec
from extract.registry_specs import dataset_query, dataset_schema, query_expr_registry
from extract.registry_specs import dataset_row as registry_row
from extract.schema_ops import ExtractNormalizeOptions, normalize_extract_plan
from extract.spec_helpers import plan_requires_row, rule_execution_options
from ibis_engine.plan import IbisPlan
from ibis_engine.plan_bridge import plan_to_ibis
from ibis_engine.query_bridge import QueryBridgeResult, queryspec_to_ibis
from relspec.rules.definitions import RuleStage, stage_enabled


def empty_plan_for_dataset(name: str) -> Plan:
    """Return an empty plan for a dataset name.

    Returns
    -------
    Plan
        Plan sourcing an empty table for the dataset.
    """
    return Plan.table_source(empty_table(dataset_schema(name)))


def plan_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike,
    label: str = "",
) -> Plan:
    """Build a plan from row data by materializing a table source.

    Returns
    -------
    Plan
        Plan for the row data.
    """
    row_sequence = rows if isinstance(rows, Sequence) else list(rows)
    table = rows_to_table(row_sequence, schema)
    return Plan.table_source(table, label=label)


def apply_query_and_normalize(
    name: str,
    plan: Plan,
    *,
    ctx: ExecutionContext,
    normalize: ExtractNormalizeOptions | None = None,
    evidence_plan: EvidencePlan | None = None,
) -> Plan:
    """Apply the dataset query and normalize plan alignment.

    Returns
    -------
    Plan
        Query-filtered and normalized plan.
    """
    row = registry_row(name)
    if evidence_plan is not None and not plan_requires_row(evidence_plan, row):
        return empty_plan_for_dataset(name)
    overrides = _options_overrides(normalize.options if normalize else None)
    execution = rule_execution_options(row.template or name, evidence_plan, overrides=overrides)
    if row.enabled_when is not None:
        stage = RuleStage(name=name, mode="source", enabled_when=row.enabled_when)
        if not stage_enabled(stage, execution.as_mapping()):
            return empty_plan_for_dataset(name)
    plan = dataset_query(name).apply_to_plan(plan, ctx=ctx)
    plan = _apply_pipeline_query_ops(name, plan, ctx=ctx)
    plan = apply_evidence_projection(name, plan, ctx=ctx, evidence_plan=evidence_plan)
    return normalize_extract_plan(name, plan, ctx=ctx, normalize=normalize)


@dataclass(frozen=True)
class ExtractPlanBuildOptions:
    """Options for building extract plans from row data."""

    normalize: ExtractNormalizeOptions | None = None
    evidence_plan: EvidencePlan | None = None


@dataclass(frozen=True)
class ExtractIbisPlanBuildOptions:
    """Options for building Ibis plans from extract row data."""

    backend: BaseBackend
    plan_options: ExtractPlanBuildOptions | None = None


@dataclass(frozen=True)
class ExtractPlanAdapterOptions:
    """Options for building adapter-routed extract plans."""

    adapter_mode: AdapterMode | None = None
    ibis_backend: BaseBackend | None = None
    plan_options: ExtractPlanBuildOptions | None = None


def plan_from_rows_for_dataset(
    name: str,
    rows: Iterable[Mapping[str, object]],
    *,
    row_schema: SchemaLike,
    ctx: ExecutionContext,
    options: ExtractPlanBuildOptions | None = None,
) -> Plan:
    """Build a plan from row data with dataset query and normalization.

    Returns
    -------
    Plan
        Plan for the dataset rows with query + normalization applied.
    """
    options = options or ExtractPlanBuildOptions()
    plan = plan_from_rows(rows, schema=row_schema, label=name)
    return apply_query_and_normalize(
        name,
        plan,
        ctx=ctx,
        normalize=options.normalize,
        evidence_plan=options.evidence_plan,
    )


def plan_from_rows_for_dataset_ibis(
    name: str,
    rows: Iterable[Mapping[str, object]],
    *,
    row_schema: SchemaLike,
    ctx: ExecutionContext,
    ibis_options: ExtractIbisPlanBuildOptions,
) -> IbisPlan:
    """Build an Ibis plan from row data with dataset query and normalization.

    Returns
    -------
    IbisPlan
        Ibis plan for the dataset rows.
    """
    plan = plan_from_rows_for_dataset(
        name,
        rows,
        row_schema=row_schema,
        ctx=ctx,
        options=ibis_options.plan_options,
    )
    return plan_to_ibis(plan, ctx=ctx, backend=ibis_options.backend, name=name)


def plan_from_rows_for_dataset_adapter(
    name: str,
    rows: Iterable[Mapping[str, object]],
    *,
    row_schema: SchemaLike,
    ctx: ExecutionContext,
    options: ExtractPlanAdapterOptions | None = None,
) -> Plan | IbisPlan:
    """Build a plan (or Ibis plan) from row data with adapter routing.

    Returns
    -------
    Plan | IbisPlan
        Plan or Ibis plan for the dataset rows.

    Raises
    ------
    ValueError
        Raised when the Ibis adapter is enabled without a backend.
    """
    options = options or ExtractPlanAdapterOptions()
    resolved_mode = options.adapter_mode or AdapterMode()
    if resolved_mode.use_ibis_bridge:
        if options.ibis_backend is None:
            msg = "Ibis backend is required when AdapterMode.use_ibis_bridge is enabled."
            raise ValueError(msg)
        return plan_from_rows_for_dataset_ibis(
            name,
            rows,
            row_schema=row_schema,
            ctx=ctx,
            ibis_options=ExtractIbisPlanBuildOptions(
                backend=options.ibis_backend,
                plan_options=options.plan_options,
            ),
        )
    return plan_from_rows_for_dataset(
        name,
        rows,
        row_schema=row_schema,
        ctx=ctx,
        options=options.plan_options,
    )


@dataclass
class _QueryOpState:
    base: tuple[str, ...] = ()
    derived: dict[str, ExprSpec] = field(default_factory=dict)
    predicate: ExprSpec | None = None
    pushdown: ExprSpec | None = None
    registry: ExprRegistry = field(default_factory=query_expr_registry)


def _expr_spec_from_row(
    row: Mapping[str, object],
    registry: ExprRegistry,
    *,
    label: str,
) -> ExprSpec:
    expr_ir_json = row.get("expr_ir_json")
    if expr_ir_json is not None:
        expr_ir = ExprIR.from_json(str(expr_ir_json))
        return expr_ir.to_expr_spec(registry=registry)
    expr_json = row.get("expr_json")
    if expr_json is None:
        msg = f"Query {label} op requires expr_ir_json or expr_json."
        raise ValueError(msg)
    return expr_spec_from_json(str(expr_json), registry=registry)


def _handle_project(row: Mapping[str, object], state: _QueryOpState) -> None:
    columns = parse_string_tuple(row.get("columns"), label="columns")
    if columns:
        state.base = columns


def _handle_derive(row: Mapping[str, object], state: _QueryOpState) -> None:
    name = row.get("name")
    if name is None:
        msg = "Query derive op requires name."
        raise ValueError(msg)
    state.derived[str(name)] = _expr_spec_from_row(row, state.registry, label="derive")


def _handle_filter(row: Mapping[str, object], state: _QueryOpState) -> None:
    state.predicate = _expr_spec_from_row(row, state.registry, label="filter")


def _handle_pushdown_filter(row: Mapping[str, object], state: _QueryOpState) -> None:
    state.pushdown = _expr_spec_from_row(row, state.registry, label="pushdown_filter")


_QUERY_OP_HANDLERS: dict[str, Callable[[Mapping[str, object], _QueryOpState], None]] = {
    "project": _handle_project,
    "derive": _handle_derive,
    "filter": _handle_filter,
    "pushdown_filter": _handle_pushdown_filter,
}


def _query_ops_to_spec(ops: Sequence[Mapping[str, object]]) -> QuerySpec | None:
    if not ops:
        return None
    state = _QueryOpState()
    for row in ops:
        kind = str(row.get("kind", ""))
        handler = _QUERY_OP_HANDLERS.get(kind)
        if handler is None:
            msg = f"Unsupported query op kind: {kind!r}"
            raise ValueError(msg)
        handler(row, state)
    if not state.base:
        state.base = tuple(state.derived)
    return QuerySpec(
        projection=ProjectionSpec(base=state.base, derived=state.derived),
        predicate=state.predicate,
        pushdown_predicate=state.pushdown,
    )


def query_ops_to_ibis(ops: Sequence[Mapping[str, object]]) -> QueryBridgeResult | None:
    """Return an Ibis bridge spec for query ops when possible.

    Returns
    -------
    QueryBridgeResult | None
        Bridge spec when the ops can be represented, otherwise ``None``.
    """
    spec = _query_ops_to_spec(ops)
    if spec is None:
        return None
    return queryspec_to_ibis(spec)


def _apply_pipeline_query_ops(name: str, plan: Plan, *, ctx: ExecutionContext) -> Plan:
    ops = pipeline_spec(name).query_ops
    spec = _query_ops_to_spec(ops)
    if spec is None:
        return plan
    return spec.apply_to_plan(plan, ctx=ctx)


def _projection_columns(name: str, *, evidence_plan: EvidencePlan | None) -> tuple[str, ...]:
    if evidence_plan is None:
        return ()
    required = set(evidence_plan.required_columns_for(name))
    row = registry_row(name)
    required.update(row.join_keys)
    if row.evidence_required_columns:
        required.update(row.evidence_required_columns)
    if not required:
        return ()
    schema = dataset_schema(name)
    return tuple(field.name for field in schema if field.name in required)


def apply_evidence_projection(
    name: str,
    plan: Plan,
    *,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None,
) -> Plan:
    """Apply evidence-minimized projection for a dataset plan.

    Returns
    -------
    Plan
        Plan projected to evidence-required columns when applicable.
    """
    columns = _projection_columns(name, evidence_plan=evidence_plan)
    if not columns:
        return plan
    schema = dataset_schema(name)
    field_types = {field.name: field.type for field in schema}
    inferred = plan_output_columns(plan)
    available = set(dataset_schema(name).names) if inferred is None else set(inferred)
    names: list[str] = []
    exprs: list[ComputeExpression] = []
    for column in columns:
        dtype = field_types.get(column)
        if dtype is None:
            continue
        names.append(column)
        exprs.append(
            column_or_null_expr(
                column,
                dtype,
                available=available,
                cast=True,
                safe=False,
            )
        )
    if not names:
        return plan
    return plan.project(exprs, names, ctx=ctx)


def _options_overrides(options: object | None) -> Mapping[str, object]:
    if options is None:
        return {}
    if is_dataclass(options) and not isinstance(options, type):
        return asdict(options)
    if isinstance(options, Mapping):
        return dict(options)
    return {}


__all__ = [
    "ExtractPlanAdapterOptions",
    "ExtractPlanBuildOptions",
    "apply_evidence_projection",
    "apply_query_and_normalize",
    "empty_plan_for_dataset",
    "plan_from_rows",
    "plan_from_rows_for_dataset",
    "plan_from_rows_for_dataset_adapter",
    "plan_from_rows_for_dataset_ibis",
    "query_ops_to_ibis",
]
