"""Plan-lane runner utilities for normalize pipelines."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import cast

import pyarrow as pa

from arrowdsl.compute.ids import masked_hash_expr
from arrowdsl.compute.macros import ColumnOrNullExpr
from arrowdsl.core.context import (
    DeterminismTier,
    ExecutionContext,
    OrderingLevel,
    execution_context_factory,
)
from arrowdsl.core.ids import HashSpec
from arrowdsl.core.interop import (
    ComputeExpression,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    pc,
)
from arrowdsl.finalize.finalize import Contract, FinalizeOptions, FinalizeResult, finalize
from arrowdsl.plan.catalog import PlanCatalog
from arrowdsl.plan.plan import Plan, PlanSpec
from arrowdsl.plan.runner import PlanRunResult, run_plan
from arrowdsl.plan.scan_io import plan_from_source
from arrowdsl.schema.metadata import merge_metadata_specs
from arrowdsl.schema.ops import align_table
from arrowdsl.schema.policy import SchemaPolicy, SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import SchemaMetadataSpec, empty_table
from normalize.catalog import NormalizePlanCatalog
from normalize.compiler_graph import NormalizeGraphPlan, compile_graph_plan, order_rules
from normalize.contracts import NORMALIZE_EVIDENCE_NAME, normalize_evidence_schema
from normalize.evidence_catalog import EvidenceCatalog
from normalize.evidence_specs import evidence_output_from_schema, evidence_spec_from_schema
from normalize.policies import (
    ambiguity_kernels,
    ambiguity_policy_from_schema,
    confidence_expr,
    confidence_policy_from_schema,
    default_tie_breakers,
)
from normalize.registry_specs import dataset_metadata_spec, dataset_schema, dataset_spec
from normalize.rule_model import AmbiguityPolicy, EvidenceOutput, EvidenceSpec, NormalizeRule
from normalize.rule_registry import normalize_rules
from normalize.utils import (
    PlanSource,
    encoding_policy_from_schema,
    finalize_plan,
    finalize_plan_result,
    plan_source,
)
from schema_spec.specs import PROVENANCE_COLS
from schema_spec.system import ContractSpec

PostFn = Callable[[TableLike, ExecutionContext], TableLike]


@dataclass(frozen=True)
class NormalizeFinalizeSpec:
    """Finalize overrides for normalize pipelines."""

    metadata_spec: SchemaMetadataSpec | None = None
    schema_policy: SchemaPolicy | None = None


@dataclass(frozen=True)
class NormalizeRuleCompilation:
    """Compiled normalize rules and derived plans."""

    rules: tuple[NormalizeRule, ...]
    plans: Mapping[str, Plan]
    catalog: PlanCatalog


def ensure_execution_context(ctx: ExecutionContext | None) -> ExecutionContext:
    """Return a normalized execution context.

    Returns
    -------
    ExecutionContext
        Provided context or a default context when missing.
    """
    if ctx is not None:
        return ctx
    return execution_context_factory("default")


def ensure_canonical(ctx: ExecutionContext) -> ExecutionContext:
    """Return an execution context upgraded to canonical determinism.

    Returns
    -------
    ExecutionContext
        Execution context using canonical determinism.
    """
    return ctx.with_determinism(DeterminismTier.CANONICAL)


def _should_skip_canonical_sort(
    plan: Plan,
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
        schema = plan.schema(ctx=ctx)
        for col in PROVENANCE_COLS:
            if col in schema.names:
                expected = (*expected, (col, "ascending"))
    return plan.ordering.keys == expected


def run_normalize(
    *,
    plan: Plan,
    post: Iterable[PostFn],
    contract: ContractSpec,
    ctx: ExecutionContext,
    finalize_spec: NormalizeFinalizeSpec | None = None,
) -> FinalizeResult:
    """Execute a normalize plan with post steps and finalize gate.

    Returns
    -------
    FinalizeResult
        Finalize bundle with good/errors/stats/alignment outputs.
    """
    result = run_plan(
        plan,
        ctx=ctx,
        prefer_reader=False,
        attach_ordering_metadata=True,
    )
    table = cast("TableLike", result.value)
    for fn in post:
        table = fn(table, ctx)
    contract_obj = contract.to_contract()
    finalize_spec = finalize_spec or NormalizeFinalizeSpec()
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
    options = FinalizeOptions(
        schema_policy=schema_policy,
        skip_canonical_sort=_should_skip_canonical_sort(plan, contract=contract_obj, ctx=ctx),
    )
    return finalize(table, contract=contract_obj, ctx=ctx, options=options)


def _merge_policy_metadata(
    policy: SchemaPolicy,
    metadata: SchemaMetadataSpec | None,
) -> SchemaPolicy:
    if metadata is None:
        return policy
    merged = merge_metadata_specs(policy.metadata, metadata)
    return replace(policy, metadata=merged)


def run_normalize_reader(plan: PlanSource, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
    """Return a streaming reader for a normalize plan.

    Returns
    -------
    RecordBatchReaderLike
        Streaming reader for the plan.
    """
    resolved = plan_source(plan, ctx=ctx)
    return PlanSpec.from_plan(resolved).to_reader(ctx=ctx)


def run_normalize_streamable(
    plan: PlanSource,
    *,
    ctx: ExecutionContext,
    schema: SchemaLike | None = None,
) -> RecordBatchReaderLike | TableLike:
    """Return a reader when no pipeline breakers exist, otherwise a table.

    Notes
    -----
    This path does not apply finalize contracts (alignment, dedupe, or canonical sort).
    Use ``run_normalize`` when contract enforcement is required. When ``schema`` is
    provided, the plan is projected to the schema before materialization.

    Returns
    -------
    RecordBatchReaderLike | TableLike
        Reader when streamable, otherwise a materialized table.
    """
    resolved = plan_source(plan, ctx=ctx)
    return finalize_plan(
        resolved,
        ctx=ctx,
        prefer_reader=True,
        schema=schema,
        keep_extra_columns=ctx.provenance,
    )


def run_normalize_streamable_contract(
    plan: PlanSource,
    *,
    contract: ContractSpec,
    ctx: ExecutionContext | None = None,
) -> RecordBatchReaderLike | TableLike:
    """Return a streamable output aligned to the contract schema.

    Returns
    -------
    RecordBatchReaderLike | TableLike
        Reader when streamable, otherwise a materialized table.
    """
    exec_ctx = ensure_execution_context(ctx)
    schema = contract.to_contract().schema
    return run_normalize_streamable(plan, ctx=exec_ctx, schema=schema)


def run_normalize_streamable_result(
    plan: PlanSource,
    *,
    ctx: ExecutionContext,
    schema: SchemaLike | None = None,
) -> PlanRunResult:
    """Return a reader/table plus materialization metadata.

    Returns
    -------
    PlanRunResult
        Plan output with materialization kind metadata.
    """
    resolved = plan_source(plan, ctx=ctx)
    return finalize_plan_result(
        resolved,
        ctx=ctx,
        prefer_reader=True,
        schema=schema,
        keep_extra_columns=ctx.provenance,
    )


def _expand_required_outputs(
    rules: Sequence[NormalizeRule],
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


def _apply_policy_defaults(rule: NormalizeRule) -> NormalizeRule:
    schema = dataset_schema(rule.output)
    confidence = rule.confidence_policy or confidence_policy_from_schema(schema)
    ambiguity = rule.ambiguity_policy or ambiguity_policy_from_schema(schema)
    if ambiguity is not None:
        ambiguity = _apply_default_tie_breakers(ambiguity, schema=schema)
    if confidence == rule.confidence_policy and ambiguity == rule.ambiguity_policy:
        return rule
    return replace(
        rule,
        confidence_policy=confidence,
        ambiguity_policy=ambiguity,
    )


def _merge_evidence(
    base: EvidenceSpec | None,
    defaults: EvidenceSpec | None,
) -> EvidenceSpec | None:
    if base is None:
        return defaults
    if defaults is None:
        return base
    sources = base.sources or defaults.sources
    required_columns = tuple(sorted(set(base.required_columns).union(defaults.required_columns)))
    required_types = dict(defaults.required_types)
    required_types.update(base.required_types)
    required_metadata = dict(defaults.required_metadata)
    required_metadata.update(base.required_metadata)
    if not sources and not required_columns and not required_types and not required_metadata:
        return None
    return EvidenceSpec(
        sources=sources,
        required_columns=required_columns,
        required_types=required_types,
        required_metadata=required_metadata,
    )


def _merge_evidence_output(
    base: EvidenceOutput | None,
    defaults: EvidenceOutput | None,
) -> EvidenceOutput | None:
    if base is None:
        return defaults
    if defaults is None:
        return base
    column_map = dict(defaults.column_map)
    column_map.update(base.column_map)
    literals = dict(defaults.literals)
    literals.update(base.literals)
    if not column_map and not literals:
        return None
    return EvidenceOutput(column_map=column_map, literals=literals)


def _apply_evidence_defaults(rule: NormalizeRule) -> NormalizeRule:
    schema = dataset_schema(rule.output)
    evidence_defaults = evidence_spec_from_schema(schema)
    output_defaults = evidence_output_from_schema(schema)
    evidence = _merge_evidence(rule.evidence, evidence_defaults)
    evidence_output = _merge_evidence_output(rule.evidence_output, output_defaults)
    if evidence == rule.evidence and evidence_output == rule.evidence_output:
        return rule
    return replace(rule, evidence=evidence, evidence_output=evidence_output)


def apply_policy_defaults(rules: Sequence[NormalizeRule]) -> tuple[NormalizeRule, ...]:
    """Return rules with contract-derived policy defaults applied.

    Returns
    -------
    tuple[NormalizeRule, ...]
        Rules with confidence and ambiguity policies filled from metadata.
    """
    return tuple(_apply_policy_defaults(rule) for rule in rules)


def apply_evidence_defaults(rules: Sequence[NormalizeRule]) -> tuple[NormalizeRule, ...]:
    """Return rules with evidence defaults applied.

    Returns
    -------
    tuple[NormalizeRule, ...]
        Rules with evidence defaults filled from metadata.
    """
    return tuple(_apply_evidence_defaults(rule) for rule in rules)


def _validate_rule_policies(rule: NormalizeRule) -> None:
    policy = rule.ambiguity_policy
    if policy is None or policy.winner_select is None:
        return
    schema = normalize_evidence_schema()
    available = set(schema.names)
    required: set[str] = set(policy.winner_select.keys)
    required.add(policy.winner_select.score_col)
    required.update(key.column for key in policy.winner_select.tie_breakers)
    required.update(key.column for key in policy.tie_breakers)
    missing = sorted(required - available)
    if missing:
        msg = (
            "Normalize ambiguity policy references missing evidence columns "
            f"for rule {rule.name!r}: {missing}"
        )
        raise ValueError(msg)


def _apply_default_tie_breakers(
    policy: AmbiguityPolicy,
    *,
    schema: SchemaLike,
) -> AmbiguityPolicy:
    if policy.winner_select is None:
        return policy
    if policy.tie_breakers or policy.winner_select.tie_breakers:
        return policy
    defaults = default_tie_breakers(schema)
    if not defaults:
        return policy
    return replace(policy, tie_breakers=defaults)


def compile_normalize_rules(
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
    rules: Sequence[NormalizeRule] | None = None,
    required_outputs: Sequence[str] | None = None,
) -> NormalizeRuleCompilation:
    """Compile normalize rules into plans with evidence gating.

    Returns
    -------
    NormalizeRuleCompilation
        Compiled rule set with derived plans.

    Raises
    ------
    ValueError
        Raised when a rule requires plan execution but no plan can be built.
    """
    rule_set = tuple(normalize_rules()) if rules is None else tuple(rules)
    if required_outputs:
        required_set = _expand_required_outputs(rule_set, required_outputs)
        rule_set = tuple(rule for rule in rule_set if rule.output in required_set)
    rule_set = apply_evidence_defaults(apply_policy_defaults(rule_set))
    for rule in rule_set:
        _validate_rule_policies(rule)
    work_catalog = _clone_catalog(catalog)
    evidence = EvidenceCatalog.from_plan_catalog(work_catalog, ctx=ctx)
    ordered = order_rules(rule_set, evidence=evidence)

    plans: dict[str, Plan] = {}
    compiled_rules: list[NormalizeRule] = []
    for rule in ordered:
        plan = _resolve_rule_plan(rule, work_catalog, ctx=ctx)
        if plan is None:
            if rule.execution_mode == "plan":
                msg = f"Normalize rule {rule.name!r} requires plan execution."
                raise ValueError(msg)
            continue
        compiled_rules.append(rule)
        plans[rule.output] = plan
        work_catalog.add(rule.output, plan)
        evidence.register(rule.output, plan.schema(ctx=ctx))

    return NormalizeRuleCompilation(
        rules=tuple(compiled_rules),
        plans=plans,
        catalog=work_catalog,
    )


def compile_normalize_plans(
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
    rules: Sequence[NormalizeRule] | None = None,
    required_outputs: Sequence[str] | None = None,
) -> dict[str, Plan]:
    """Compile normalize rules into plans keyed by output dataset.

    Returns
    -------
    dict[str, Plan]
        Mapping of output dataset names to plans.
    """
    compilation = compile_normalize_rules(
        catalog,
        ctx=ctx,
        rules=rules,
        required_outputs=required_outputs,
    )
    return dict(compilation.plans)


def compile_normalize_graph_plan(
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
    rules: Sequence[NormalizeRule] | None = None,
    required_outputs: Sequence[str] | None = None,
) -> NormalizeGraphPlan:
    """Compile normalize rules into a unified graph plan.

    Returns
    -------
    NormalizeGraphPlan
        Graph-level plan with per-output subplans.
    """
    compilation = compile_normalize_rules(
        catalog,
        ctx=ctx,
        rules=rules,
        required_outputs=required_outputs,
    )
    if not compilation.plans:
        empty = Plan.table_source(empty_table(pa.schema([])), label="normalize_graph_empty")
        return NormalizeGraphPlan(plan=empty, outputs={})
    return compile_graph_plan(compilation.rules, plans=dict(compilation.plans))


def materialize_normalize_evidence(
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
    rules: Sequence[NormalizeRule] | None = None,
    required_outputs: Sequence[str] | None = None,
) -> dict[str, TableLike]:
    """Materialize canonical evidence outputs for normalize rules.

    Returns
    -------
    dict[str, TableLike]
        Evidence outputs keyed by rule output dataset name.
    """
    compilation = compile_normalize_rules(
        catalog,
        ctx=ctx,
        rules=rules,
        required_outputs=required_outputs,
    )
    outputs: dict[str, TableLike] = {}
    for rule in compilation.rules:
        plan = compilation.plans.get(rule.output)
        if plan is None:
            continue
        outputs[rule.output] = _materialize_evidence_output(plan, rule, ctx=ctx)
    return outputs


def normalize_evidence_table(
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
    rules: Sequence[NormalizeRule] | None = None,
) -> TableLike:
    """Return a unified canonical evidence table from normalize rules.

    Returns
    -------
    TableLike
        Unified evidence table aligned to the canonical contract.
    """
    outputs = materialize_normalize_evidence(catalog, ctx=ctx, rules=rules)
    if not outputs:
        return empty_table(normalize_evidence_schema())
    spec = dataset_spec(NORMALIZE_EVIDENCE_NAME)
    unified = spec.unify_tables(tuple(outputs.values()), ctx=ctx)
    return spec.finalize_context(ctx).run(unified, ctx=ctx).good


def _clone_catalog(catalog: PlanCatalog) -> PlanCatalog:
    tables = catalog.snapshot()
    if isinstance(catalog, NormalizePlanCatalog):
        return NormalizePlanCatalog(tables=tables, repo_text_index=catalog.repo_text_index)
    return PlanCatalog(tables=tables)


def _resolve_rule_plan(
    rule: NormalizeRule,
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
) -> Plan | None:
    source: PlanSource | None
    if rule.derive is None:
        if not rule.inputs:
            return None
        source = catalog.tables.get(rule.inputs[0])
    else:
        source = rule.derive(catalog, ctx)
    if source is None:
        return None
    plan = plan_from_source(source, ctx=ctx, label=rule.name)
    if rule.query is not None:
        plan = rule.query.apply_to_plan(plan, ctx=ctx)
    return plan


def _materialize_evidence_output(
    plan: Plan,
    rule: NormalizeRule,
    *,
    ctx: ExecutionContext,
) -> TableLike:
    evidence_plan = _build_evidence_plan(plan, rule, ctx=ctx)
    evidence_plan = _apply_ambiguity_plan_ops(evidence_plan, rule, ctx=ctx)
    result = run_plan(
        evidence_plan,
        ctx=ctx,
        prefer_reader=False,
        attach_ordering_metadata=True,
    )
    table = cast("TableLike", result.value)
    return align_table(table, schema=normalize_evidence_schema(), ctx=ctx)


def _build_evidence_plan(
    plan: Plan,
    rule: NormalizeRule,
    *,
    ctx: ExecutionContext,
) -> Plan:
    schema = normalize_evidence_schema()
    available = frozenset(plan.schema(ctx=ctx).names)
    output = rule.evidence_output or EvidenceOutput()
    col_map: dict[str, str] = dict(output.column_map)
    literals: dict[str, object] = dict(output.literals)
    literals.setdefault("rule_name", rule.name)
    if "source" not in literals and "source" not in col_map:
        literals["source"] = rule.output
    _set_metadata_literal(literals, rule.output, key=b"evidence_family", name="evidence_family")

    source_field = _source_field_for_confidence(col_map, available, literals)

    exprs: list[ComputeExpression] = []
    names: list[str] = []
    for field in schema:
        name = field.name
        dtype = field.type
        if name == "span_id":
            expr = _span_id_expr(col_map, available)
        elif name == "confidence":
            expr = _confidence_expr(
                rule,
                context=_EvidenceExprContext(
                    dtype=dtype,
                    available=available,
                    col_map=col_map,
                    literals=literals,
                    source_field=source_field,
                ),
            )
        else:
            expr = _evidence_expr(
                name,
                dtype=dtype,
                available=available,
                col_map=col_map,
                literals=literals,
            )
        exprs.append(expr)
        names.append(name)
    return plan.project(exprs, names, ctx=ctx)


def _apply_ambiguity_plan_ops(
    plan: Plan,
    rule: NormalizeRule,
    *,
    ctx: ExecutionContext,
) -> Plan:
    current = plan
    for spec in ambiguity_kernels(rule.ambiguity_policy):
        schema = current.schema(ctx=ctx)
        columns = [name for name in schema.names if name not in spec.keys]
        current = current.winner_select(
            spec,
            columns=columns,
            ctx=ctx,
            label=f"{rule.name}_winner_select",
        )
    return current


def _set_metadata_literal(
    literals: dict[str, object],
    output_name: str,
    *,
    key: bytes,
    name: str,
) -> None:
    if name in literals:
        return
    meta = dataset_metadata_spec(output_name).schema_metadata
    value = meta.get(key)
    if isinstance(value, (bytes, bytearray)):
        literals[name] = value.decode("utf-8")


def _evidence_expr(
    name: str,
    *,
    dtype: pa.DataType,
    available: frozenset[str],
    col_map: Mapping[str, str],
    literals: Mapping[str, object],
) -> ComputeExpression:
    if name in literals:
        return _literal_expr(literals[name], dtype=dtype)
    source = col_map.get(name, name)
    return _column_expr(source, dtype=dtype, available=available)


@dataclass(frozen=True)
class _EvidenceExprContext:
    dtype: pa.DataType
    available: frozenset[str]
    col_map: Mapping[str, str]
    literals: Mapping[str, object]
    source_field: str | None


def _confidence_expr(
    rule: NormalizeRule,
    *,
    context: _EvidenceExprContext,
) -> ComputeExpression:
    if "confidence" in context.literals:
        return _literal_expr(context.literals["confidence"], dtype=context.dtype)
    mapped = context.col_map.get("confidence")
    if mapped is not None:
        return _column_expr(mapped, dtype=context.dtype, available=context.available)
    if rule.confidence_policy is not None:
        expr = confidence_expr(rule.confidence_policy, source_field=context.source_field)
        return expr.to_expression()
    return _column_expr("confidence", dtype=context.dtype, available=context.available)


def _source_field_for_confidence(
    col_map: Mapping[str, str],
    available: frozenset[str],
    literals: Mapping[str, object],
) -> str | None:
    if "source" in literals:
        return None
    source = col_map.get("source", "source")
    if source in available:
        return source
    return None


def _span_id_expr(
    col_map: Mapping[str, str],
    available: frozenset[str],
) -> ComputeExpression:
    path_col = col_map.get("path", "path")
    bstart_col = col_map.get("bstart", "bstart")
    bend_col = col_map.get("bend", "bend")
    spec = HashSpec(
        prefix="span",
        cols=(path_col, bstart_col, bend_col),
        null_sentinel="None",
    )
    return masked_hash_expr(
        spec,
        required=(path_col, bstart_col, bend_col),
        available=tuple(available),
    )


def _column_expr(
    name: str,
    *,
    dtype: pa.DataType,
    available: frozenset[str],
) -> ComputeExpression:
    expr = ColumnOrNullExpr(name=name, dtype=dtype, available=available, cast=True)
    return expr.to_expression()


def _literal_expr(value: object, *, dtype: pa.DataType) -> ComputeExpression:
    if value is None:
        return pc.scalar(pa.scalar(None, type=dtype))
    return pc.scalar(value)


__all__ = [
    "NormalizeFinalizeSpec",
    "NormalizeRuleCompilation",
    "PostFn",
    "apply_evidence_defaults",
    "apply_policy_defaults",
    "compile_normalize_graph_plan",
    "compile_normalize_plans",
    "compile_normalize_rules",
    "ensure_canonical",
    "ensure_execution_context",
    "materialize_normalize_evidence",
    "normalize_evidence_table",
    "run_normalize",
    "run_normalize_reader",
    "run_normalize_streamable",
    "run_normalize_streamable_contract",
    "run_normalize_streamable_result",
]
