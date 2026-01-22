"""Coverage assessment for rule-driven function and kernel demands."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cache
from typing import Protocol, TypeGuard, get_args, runtime_checkable

import ibis

from arrowdsl.kernel.registry import KERNEL_REGISTRY, KernelDef
from datafusion_engine.extract_builders import QueryContext, build_query_spec
from datafusion_engine.extract_metadata import extract_metadata_specs
from datafusion_engine.extract_pipelines import post_kernels_for_postprocess
from engine.function_registry import (
    FunctionRegistry,
    FunctionSpec,
    default_function_registry,
    pyarrow_compute_functions,
)
from ibis_engine.expr_compiler import IbisExprRegistry, default_expr_registry
from ibis_engine.query_compiler import IbisQuerySpec
from normalize.rule_factories import build_rule_definitions_from_specs
from relspec.model import KernelSpecT
from relspec.normalize.rule_registry_specs import rule_family_specs
from relspec.rules.cache import rule_definitions_cached
from relspec.rules.definitions import (
    ExtractPayload,
    NormalizePayload,
    RelationshipPayload,
    RuleDefinition,
)
from relspec.rules.rel_ops import AggregateOp, DeriveOp, FilterOp, PushdownFilterOp, RelOpT
from relspec.rules.relationship_specs import relationship_rule_definitions


@runtime_checkable
class ExprIRLike(Protocol):
    """Protocol for ExprIR-like nodes used in coverage analysis."""

    @property
    def op(self) -> str:
        """Return the operation name."""
        ...

    @property
    def name(self) -> str | None:
        """Return the operation name or field identifier."""
        ...

    @property
    def args(self) -> Sequence[ExprIRLike]:
        """Return child expression nodes."""
        ...


@cache
def _post_kernel_kinds() -> tuple[str, ...]:
    kinds: set[str] = set()
    for spec_type in get_args(KernelSpecT):
        instance = spec_type()
        kinds.add(instance.kind)
    return tuple(sorted(kinds))


@cache
def _normalize_plan_builders() -> tuple[str, ...]:
    rules = build_rule_definitions_from_specs(rule_family_specs())
    builders: set[str] = set()
    for rule in rules:
        payload = rule.payload
        if isinstance(payload, NormalizePayload) and payload.plan_builder:
            builders.add(payload.plan_builder)
    return tuple(sorted(builders))


@dataclass
class RuleDemandIndex:
    """Index of rule-driven demands mapped to rules that reference them."""

    expr_calls: dict[str, set[str]] = field(default_factory=dict)
    aggregate_funcs: dict[str, set[str]] = field(default_factory=dict)
    kernel_names: dict[str, set[str]] = field(default_factory=dict)
    post_kernel_kinds: dict[str, set[str]] = field(default_factory=dict)
    plan_builders: dict[str, set[str]] = field(default_factory=dict)
    extract_postprocess: dict[str, set[str]] = field(default_factory=dict)

    def payload(self) -> dict[str, dict[str, list[str]]]:
        """Return a JSON-ready payload for rule demand indices.

        Returns
        -------
        dict[str, dict[str, list[str]]]
            Mapping of demand categories to name->rules lists.
        """
        return {
            "expr_calls": _payload_mapping(self.expr_calls),
            "aggregate_funcs": _payload_mapping(self.aggregate_funcs),
            "kernel_names": _payload_mapping(self.kernel_names),
            "post_kernel_kinds": _payload_mapping(self.post_kernel_kinds),
            "plan_builders": _payload_mapping(self.plan_builders),
            "extract_postprocess": _payload_mapping(self.extract_postprocess),
        }


@dataclass(frozen=True)
class FunctionCoverage:
    """Coverage metadata for a function demand."""

    ibis_supported: bool
    pyarrow_supported: bool
    registry_supported: bool
    lanes: tuple[str, ...] = ()
    kind: str | None = None

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for function coverage.

        Returns
        -------
        dict[str, object]
            Coverage payload for the function.
        """
        return {
            "ibis_supported": self.ibis_supported,
            "pyarrow_supported": self.pyarrow_supported,
            "registry_supported": self.registry_supported,
            "lanes": list(self.lanes),
            "kind": self.kind,
        }


@dataclass(frozen=True)
class KernelCoverage:
    """Coverage metadata for kernel demands."""

    registered: bool
    lane: str | None = None

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for kernel coverage.

        Returns
        -------
        dict[str, object]
            Coverage payload for the kernel.
        """
        return {
            "registered": self.registered,
            "lane": self.lane,
        }


@dataclass(frozen=True)
class RuleCoverageAssessment:
    """Coverage assessment for rule-driven demands."""

    demands: RuleDemandIndex
    expr_call_coverage: Mapping[str, FunctionCoverage]
    aggregate_coverage: Mapping[str, FunctionCoverage]
    kernel_coverage: Mapping[str, KernelCoverage]
    post_kernel_coverage: Mapping[str, bool]
    plan_builder_coverage: Mapping[str, bool]
    extract_postprocess_coverage: Mapping[str, bool]

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for the assessment.

        Returns
        -------
        dict[str, object]
            Coverage assessment payload.
        """
        return {
            "demands": self.demands.payload(),
            "expr_call_coverage": _payload_coverage(self.expr_call_coverage),
            "aggregate_coverage": _payload_coverage(self.aggregate_coverage),
            "kernel_coverage": {name: cov.payload() for name, cov in self.kernel_coverage.items()},
            "post_kernel_coverage": dict(self.post_kernel_coverage),
            "plan_builder_coverage": dict(self.plan_builder_coverage),
            "extract_postprocess_coverage": dict(self.extract_postprocess_coverage),
            "summary": _coverage_summary(self),
        }


def default_rule_definitions(
    *,
    include_relationship_rules: bool = False,
) -> tuple[RuleDefinition, ...]:
    """Return default rule definitions for coverage assessment.

    Parameters
    ----------
    include_relationship_rules
        Whether to include relspec relationship rule definitions.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Aggregated rule definitions.
    """
    rules = rule_definitions_cached()
    if include_relationship_rules:
        return rules
    excluded = {rule.name for rule in relationship_rule_definitions()}
    return tuple(rule for rule in rules if rule.name not in excluded)


def assess_rule_coverage(
    rules: Sequence[RuleDefinition] | None = None,
    *,
    registry: FunctionRegistry | None = None,
    include_extract_queries: bool = True,
) -> RuleCoverageAssessment:
    """Assess function and kernel coverage for rule-driven demands.

    Parameters
    ----------
    rules
        Rule definitions to assess. Defaults to the central rule sources.
    registry
        Optional function registry override.
    include_extract_queries
        Whether to include extract dataset query specs in ExprIR demands.

    Returns
    -------
    RuleCoverageAssessment
        Coverage assessment for the requested rules.
    """
    registry = registry or default_function_registry()
    rules = default_rule_definitions() if rules is None else rules
    demands = collect_rule_demands(rules, include_extract_queries=include_extract_queries)
    expr_registry = default_expr_registry()
    pyarrow_functions = set(pyarrow_compute_functions())
    expr_call_coverage = _function_coverage(
        demands.expr_calls,
        registry=registry,
        expr_registry=expr_registry,
        pyarrow_functions=pyarrow_functions,
    )
    aggregate_coverage = _aggregate_coverage(
        demands.aggregate_funcs,
        registry=registry,
        pyarrow_functions=pyarrow_functions,
    )
    kernel_coverage = _kernel_coverage(demands.kernel_names)
    supported_post_kernels = set(_post_kernel_kinds())
    post_kernel_coverage = {
        name: name in supported_post_kernels for name in sorted(demands.post_kernel_kinds)
    }
    plan_builder_coverage = _plan_builder_coverage(demands.plan_builders)
    extract_postprocess_coverage = _extract_postprocess_coverage(demands.extract_postprocess)
    return RuleCoverageAssessment(
        demands=demands,
        expr_call_coverage=expr_call_coverage,
        aggregate_coverage=aggregate_coverage,
        kernel_coverage=kernel_coverage,
        post_kernel_coverage=post_kernel_coverage,
        plan_builder_coverage=plan_builder_coverage,
        extract_postprocess_coverage=extract_postprocess_coverage,
    )


def collect_rule_demands(
    rules: Sequence[RuleDefinition],
    *,
    include_extract_queries: bool = True,
) -> RuleDemandIndex:
    """Collect function and kernel demands from rule definitions.

    Parameters
    ----------
    rules
        Rule definitions to inspect.
    include_extract_queries
        Whether to include extract dataset query specs in ExprIR demands.

    Returns
    -------
    RuleDemandIndex
        Demand index of referenced functions and kernels.
    """
    demands = RuleDemandIndex()
    for rule in rules:
        _collect_rel_ops(rule.rel_ops, rule.name, demands)
        _collect_payload(rule, demands)
        _collect_post_kernels(rule, demands)
    if include_extract_queries:
        _collect_extract_queries(demands)
    return demands


def _collect_rel_ops(ops: Sequence[RelOpT], rule_name: str, demands: RuleDemandIndex) -> None:
    for op in ops:
        if isinstance(op, DeriveOp):
            _collect_expr_calls(op.expr, rule_name, demands)
        elif isinstance(op, (FilterOp, PushdownFilterOp)):
            _collect_expr_calls(op.predicate, rule_name, demands)
        elif isinstance(op, AggregateOp):
            for spec in op.aggregates:
                _add_demand(demands.aggregate_funcs, spec.func, rule_name)
                for arg in spec.args:
                    _collect_expr_calls(arg, rule_name, demands)


def _collect_payload(rule: RuleDefinition, demands: RuleDemandIndex) -> None:
    payload = rule.payload
    if isinstance(payload, RelationshipPayload):
        _collect_relationship_payload(rule, payload, demands)
    elif isinstance(payload, NormalizePayload):
        _collect_normalize_payload(rule, payload, demands)
    elif isinstance(payload, ExtractPayload):
        _collect_extract_payload(rule, payload, demands)


def _collect_post_kernels(rule: RuleDefinition, demands: RuleDemandIndex) -> None:
    for spec in rule.post_kernels:
        _add_demand(demands.post_kernel_kinds, spec.kind, rule.name)
        if spec.kind in {"explode_list", "dedupe"}:
            _add_demand(demands.kernel_names, spec.kind, rule.name)


def _collect_extract_queries(demands: RuleDemandIndex) -> None:
    ctx = QueryContext()
    for row in extract_metadata_specs():
        spec = build_query_spec(row, ctx=ctx)
        _collect_query_spec(spec, row.name, demands)


def _collect_query_spec(spec: IbisQuerySpec, rule_name: str, demands: RuleDemandIndex) -> None:
    for expr in spec.projection.derived.values():
        if _is_expr_like(expr):
            _collect_expr_calls(expr, rule_name, demands)
    for expr in (spec.predicate, spec.pushdown_predicate):
        if _is_expr_like(expr):
            _collect_expr_calls(expr, rule_name, demands)


def _collect_expr_calls(expr: ExprIRLike, rule_name: str, demands: RuleDemandIndex) -> None:
    if expr.op == "call" and expr.name:
        _add_demand(demands.expr_calls, expr.name, rule_name)
    for arg in expr.args:
        _collect_expr_calls(arg, rule_name, demands)


def _collect_relationship_payload(
    rule: RuleDefinition,
    payload: RelationshipPayload,
    demands: RuleDemandIndex,
) -> None:
    if payload.predicate is not None:
        _collect_expr_calls(payload.predicate, rule.name, demands)
    project = payload.project
    if project is not None:
        for expr in project.exprs.values():
            _collect_expr_calls(expr, rule.name, demands)
    if payload.interval_align is not None:
        _add_demand(demands.kernel_names, "interval_align", rule.name)
    if payload.winner_select is not None:
        _add_demand(demands.kernel_names, "winner_select", rule.name)


def _collect_normalize_payload(
    rule: RuleDefinition,
    payload: NormalizePayload,
    demands: RuleDemandIndex,
) -> None:
    if payload.plan_builder:
        _add_demand(demands.plan_builders, payload.plan_builder, rule.name)
    if payload.query is not None:
        _collect_query_spec(payload.query, rule.name, demands)


def _collect_extract_payload(
    rule: RuleDefinition,
    payload: ExtractPayload,
    demands: RuleDemandIndex,
) -> None:
    if payload.postprocess:
        _add_demand(demands.extract_postprocess, payload.postprocess, rule.name)


def _function_coverage(
    demands: Mapping[str, set[str]],
    *,
    registry: FunctionRegistry,
    expr_registry: IbisExprRegistry,
    pyarrow_functions: set[str],
) -> dict[str, FunctionCoverage]:
    coverage: dict[str, FunctionCoverage] = {}
    for name in sorted(demands):
        registry_spec = registry.specs.get(name)
        registry_supported = registry_spec is not None
        ibis_supported = _ibis_supports(expr_registry, name)
        pyarrow_supported = name in pyarrow_functions
        lanes, kind = _registry_meta(registry_spec)
        coverage[name] = FunctionCoverage(
            ibis_supported=ibis_supported,
            pyarrow_supported=pyarrow_supported,
            registry_supported=registry_supported,
            lanes=lanes,
            kind=kind,
        )
    return coverage


def _aggregate_coverage(
    demands: Mapping[str, set[str]],
    *,
    registry: FunctionRegistry,
    pyarrow_functions: set[str],
) -> dict[str, FunctionCoverage]:
    coverage: dict[str, FunctionCoverage] = {}
    ibis_supported = _ibis_aggregate_support_map(demands)
    for name in sorted(demands):
        registry_spec = registry.specs.get(name)
        registry_supported = registry_spec is not None
        pyarrow_supported = name in pyarrow_functions
        lanes, kind = _registry_meta(registry_spec)
        coverage[name] = FunctionCoverage(
            ibis_supported=ibis_supported.get(name, False),
            pyarrow_supported=pyarrow_supported,
            registry_supported=registry_supported,
            lanes=lanes,
            kind=kind,
        )
    return coverage


def _kernel_coverage(demands: Mapping[str, set[str]]) -> dict[str, KernelCoverage]:
    coverage: dict[str, KernelCoverage] = {}
    for name in sorted(demands):
        kernel_defn = KERNEL_REGISTRY.get(name)
        coverage[name] = _kernel_metadata(kernel_defn)
    return coverage


def _plan_builder_coverage(demands: Mapping[str, set[str]]) -> dict[str, bool]:
    available = set(_normalize_plan_builders())
    return {name: name in available for name in sorted(demands)}


def _extract_postprocess_coverage(demands: Mapping[str, set[str]]) -> dict[str, bool]:
    coverage: dict[str, bool] = {}
    for name in sorted(demands):
        try:
            _ = post_kernels_for_postprocess(name)
        except KeyError:
            coverage[name] = False
        else:
            coverage[name] = True
    return coverage


def _ibis_supports(registry: IbisExprRegistry, name: str) -> bool:
    try:
        _ = registry.resolve(name)
    except KeyError:
        return False
    return True


def _ibis_aggregate_support_map(demands: Mapping[str, set[str]]) -> dict[str, bool]:
    if not demands:
        return {}
    table = ibis.table([("value", "int64")], name="coverage_table")
    expr = table["value"]
    return {name: callable(getattr(expr, name, None)) for name in demands}


def _registry_meta(spec: FunctionSpec | None) -> tuple[tuple[str, ...], str | None]:
    if spec is None:
        return (), None
    return tuple(spec.lanes), spec.kind


def _kernel_metadata(spec: KernelDef | None) -> KernelCoverage:
    if spec is None:
        return KernelCoverage(registered=False, lane=None)
    return KernelCoverage(registered=True, lane=spec.lane.value)


def _payload_mapping(source: Mapping[str, Iterable[str]]) -> dict[str, list[str]]:
    return {key: sorted(set(values)) for key, values in source.items()}


def _payload_coverage(
    coverage: Mapping[str, FunctionCoverage],
) -> dict[str, dict[str, object]]:
    return {name: cov.payload() for name, cov in coverage.items()}


def _coverage_summary(assessment: RuleCoverageAssessment) -> dict[str, int]:
    return {
        "expr_calls": len(assessment.expr_call_coverage),
        "expr_calls_missing_ibis": sum(
            1 for cov in assessment.expr_call_coverage.values() if not cov.ibis_supported
        ),
        "expr_calls_missing_registry": sum(
            1 for cov in assessment.expr_call_coverage.values() if not cov.registry_supported
        ),
        "expr_calls_missing_pyarrow": sum(
            1 for cov in assessment.expr_call_coverage.values() if not cov.pyarrow_supported
        ),
        "aggregate_funcs": len(assessment.aggregate_coverage),
        "aggregate_funcs_missing_ibis": sum(
            1 for cov in assessment.aggregate_coverage.values() if not cov.ibis_supported
        ),
        "aggregate_funcs_missing_registry": sum(
            1 for cov in assessment.aggregate_coverage.values() if not cov.registry_supported
        ),
        "aggregate_funcs_missing_pyarrow": sum(
            1 for cov in assessment.aggregate_coverage.values() if not cov.pyarrow_supported
        ),
        "kernel_names": len(assessment.kernel_coverage),
        "kernel_names_missing_registry": sum(
            1 for cov in assessment.kernel_coverage.values() if not cov.registered
        ),
        "post_kernel_kinds": len(assessment.post_kernel_coverage),
        "post_kernel_kinds_missing": sum(
            1 for supported in assessment.post_kernel_coverage.values() if not supported
        ),
        "plan_builders": len(assessment.plan_builder_coverage),
        "plan_builders_missing": sum(
            1 for supported in assessment.plan_builder_coverage.values() if not supported
        ),
        "extract_postprocess": len(assessment.extract_postprocess_coverage),
        "extract_postprocess_missing": sum(
            1 for supported in assessment.extract_postprocess_coverage.values() if not supported
        ),
    }


def _add_demand(target: dict[str, set[str]], name: str, rule_name: str) -> None:
    if not name:
        return
    target.setdefault(name, set()).add(rule_name)


def _is_expr_like(value: object) -> TypeGuard[ExprIRLike]:
    return isinstance(value, ExprIRLike)


__all__ = [
    "FunctionCoverage",
    "KernelCoverage",
    "RuleCoverageAssessment",
    "RuleDemandIndex",
    "assess_rule_coverage",
    "collect_rule_demands",
    "default_rule_definitions",
]
