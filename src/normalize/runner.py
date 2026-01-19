"""Ibis-first runner utilities for normalize pipelines."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from functools import cache
from typing import Protocol, cast

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
from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import apply_query_spec
from ibis_engine.scan_io import DatasetSource
from ibis_engine.sources import (
    SourceToIbisOptions,
    namespace_recorder_from_ctx,
    register_ibis_view,
    source_to_ibis,
    table_to_ibis,
)
from normalize.contracts import normalize_evidence_schema
from normalize.ibis_bridge import resolve_plan_builder_ibis
from normalize.ibis_plan_builders import IbisPlanCatalog
from normalize.registry_specs import dataset_schema
from normalize.registry_validation import validate_rule_specs
from normalize.rule_defaults import apply_evidence_defaults, apply_policy_defaults
from normalize.rule_factories import build_rule_definitions_from_specs
from relspec.normalize.rule_model import EvidenceSpec as NormalizeEvidenceSpec
from relspec.normalize.rule_model import NormalizeRule
from relspec.normalize.rule_registry_specs import rule_family_specs
from relspec.rules.compiler import RuleCompiler, RuleHandler
from relspec.rules.definitions import EvidenceSpec as CentralEvidenceSpec
from relspec.rules.definitions import RuleDefinition
from relspec.rules.evidence import EvidenceCatalog
from relspec.rules.graph import RuleSelectors, order_rules_by_evidence
from schema_spec.system import ContractSpec

PostFn = Callable[[TableLike, ExecutionContext], TableLike]


@dataclass(frozen=True)
class NormalizeFinalizeSpec:
    """Finalize overrides for normalize pipelines."""

    metadata_spec: SchemaMetadataSpec | None = None
    schema_policy: SchemaPolicy | None = None


class _NormalizeHandlerModule(Protocol):
    NormalizeRuleHandler: type[RuleHandler]


def _normalize_rule_handler() -> RuleHandler:
    module = cast(
        "_NormalizeHandlerModule",
        importlib.import_module("relspec.rules.handlers.normalize"),
    )
    return module.NormalizeRuleHandler()


@dataclass(frozen=True)
class NormalizeRunOptions:
    """Execution options for normalize plans."""

    finalize_spec: NormalizeFinalizeSpec | None = None
    execution_policy: AdapterExecutionPolicy | None = None
    execution_label: ExecutionLabel | None = None
    ibis_backend: BaseBackend | None = None
    params: Mapping[Value, object] | None = None


@dataclass(frozen=True)
class NormalizeRuleCompilation:
    """Compiled normalize rules and derived Ibis plans."""

    rules: tuple[NormalizeRule, ...]
    plans: Mapping[str, IbisPlan]
    ibis_catalog: IbisPlanCatalog
    output_storage: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class NormalizeIbisPlanOptions:
    """Options for compiling normalize plans into Ibis plans."""

    backend: BaseBackend
    rules: Sequence[NormalizeRule] | None = None
    required_outputs: Sequence[str] | None = None
    name_prefix: str = "normalize"
    materialize_outputs: Sequence[str] | None = None
    execution_policy: AdapterExecutionPolicy | None = None


@dataclass(frozen=True)
class _NormalizeIbisCompilationContext:
    ordered: tuple[NormalizeRule, ...]
    ibis_catalog: IbisPlanCatalog
    execution: IbisExecutionContext
    evidence: EvidenceCatalog


def _normalize_ibis_context(
    catalog: IbisPlanCatalog,
    *,
    ctx: ExecutionContext,
    options: NormalizeIbisPlanOptions,
) -> _NormalizeIbisCompilationContext:
    if catalog.backend is not options.backend:
        msg = "Normalize Ibis catalog backend does not match compile options backend."
        raise ValueError(msg)
    rule_set = _default_normalize_rules(ctx) if options.rules is None else tuple(options.rules)
    if options.required_outputs:
        required_set = _expand_required_outputs(rule_set, options.required_outputs)
        rule_set = tuple(rule for rule in rule_set if rule.output in required_set)
    for rule in rule_set:
        _validate_rule_policies(rule)
    evidence = EvidenceCatalog.from_sources(catalog.tables)
    selectors = RuleSelectors(
        inputs_for=lambda rule: rule.inputs,
        output_for=lambda rule: rule.output,
        name_for=lambda rule: rule.name,
        priority_for=lambda rule: rule.priority,
        evidence_for=lambda rule: _central_evidence(rule.evidence),
        output_schema_for=_normalize_output_schema,
    )
    ordered = order_rules_by_evidence(
        rule_set,
        evidence=evidence,
        selectors=selectors,
        allow_fallback=True,
        label="Normalize rule",
    )
    execution = IbisExecutionContext(
        ctx=ctx,
        execution_policy=options.execution_policy,
        ibis_backend=options.backend,
    )
    return _NormalizeIbisCompilationContext(
        ordered=tuple(ordered),
        ibis_catalog=catalog,
        execution=execution,
        evidence=evidence,
    )


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


def _default_normalize_rules(ctx: ExecutionContext) -> tuple[NormalizeRule, ...]:
    compiler = RuleCompiler(handlers={"normalize": _normalize_rule_handler()})
    compiled = compiler.compile_rules(_normalize_rule_definitions(), ctx=ctx)
    rules = cast("tuple[NormalizeRule, ...]", compiled)
    validate_rule_specs(rules)
    return rules


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
    """
    options = options or NormalizeRunOptions()
    execution = IbisExecutionContext(
        ctx=ctx,
        execution_policy=options.execution_policy,
        execution_label=options.execution_label,
        ibis_backend=options.ibis_backend,
        params=options.params,
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


def _central_evidence(spec: NormalizeEvidenceSpec | None) -> CentralEvidenceSpec | None:
    if spec is None:
        return None
    return CentralEvidenceSpec(
        sources=spec.sources,
        required_columns=spec.required_columns,
        required_types=spec.required_types,
        required_metadata=spec.required_metadata,
    )


def _normalize_output_schema(rule: NormalizeRule) -> SchemaLike | None:
    try:
        return dataset_schema(rule.output)
    except KeyError:
        return None


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
    rule: NormalizeRule,
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
    "apply_evidence_defaults",
    "apply_policy_defaults",
    "compile_normalize_plans_ibis",
    "ensure_canonical",
    "ensure_execution_context",
    "run_normalize",
]
