"""Fallback coverage assessment across execution layers."""

from __future__ import annotations

import importlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, cast

import ibis
import pyarrow as pa
from datafusion import SessionContext
from ibis.backends import BaseBackend

from arrowdsl.compute.kernels import kernel_capability
from arrowdsl.core.context import ExecutionContext, RuntimeProfile
from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.kernel.registry import KernelLane
from arrowdsl.ops.catalog import OP_CATALOG, Lane
from cpg.registry_rows import DATASET_ROWS as CPG_DATASET_ROWS
from cpg.registry_specs import dataset_spec as cpg_dataset_spec
from datafusion_engine.bridge import ibis_plan_to_datafusion
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.kernels import datafusion_kernel_registry
from datafusion_engine.runtime import (
    AdapterExecutionPolicy,
    DataFusionRuntimeProfile,
    ExecutionLabel,
    apply_execution_policy,
)
from engine.function_registry import FunctionRegistry, default_function_registry
from extract.registry_builders import QueryContext, build_query_spec
from extract.registry_rows import DATASET_ROWS as EXTRACT_DATASET_ROWS
from extract.registry_specs import dataset_row_schema as extract_dataset_row_schema
from extract.registry_specs import dataset_spec as extract_dataset_spec
from ibis_engine.expr_compiler import (
    IbisExprRegistry,
    OperationSupportBackend,
    default_expr_registry,
    unsupported_operations,
)
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import IbisQuerySpec, apply_query_spec
from ibis_engine.sources import SourceToIbisOptions, table_to_ibis
from incremental.registry_rows import DATASET_ROWS as INCREMENTAL_DATASET_ROWS
from incremental.registry_specs import dataset_spec as incremental_dataset_spec
from normalize.registry_rows import DATASET_ROWS as NORMALIZE_DATASET_ROWS
from normalize.registry_specs import dataset_spec as normalize_dataset_spec
from relspec.model import RuleKind
from relspec.rules.coverage import (
    RuleDemandIndex,
    collect_rule_demands,
    default_rule_definitions,
)
from relspec.rules.definitions import NormalizePayload, RelationshipPayload, RuleDefinition
from relspec.rules.rel_ops import ScanOp, query_spec_from_rel_ops
from schema_spec.system import SchemaRegistry
from sqlglot_tools.bridge import IbisCompilerBackend

LayerName = Literal[
    "datafusion_sql_fallback",
    "datafusion_policy_hooks",
    "arrowdsl_lane_fallback",
    "ibis_backend_fallback",
    "udf_kernel_fallback",
    "incremental_data_fallback",
    "policy_driven_fallback",
]

RULE_KIND_TO_OPS: Mapping[str, tuple[str, ...]] = {
    RuleKind.FILTER_PROJECT.value: ("filter", "project"),
    RuleKind.HASH_JOIN.value: ("hash_join",),
    RuleKind.UNION_ALL.value: ("union_all",),
    RuleKind.INTERVAL_ALIGN.value: (),
    RuleKind.EXPLODE_LIST.value: ("explode_list",),
    RuleKind.WINNER_SELECT.value: ("winner_select",),
}

INCREMENTAL_FALLBACK_SITES: tuple[str, ...] = (
    "incremental.publish.publish_cpg_nodes",
    "incremental.publish.publish_cpg_edges",
    "incremental.publish.publish_cpg_props",
    "incremental.relspec_update.relspec_inputs_from_state",
)


@dataclass(frozen=True)
class DynamicFailure:
    """Dynamic fallback failure capture."""

    layer: LayerName
    rule_name: str | None
    dataset_name: str | None
    source: str | None
    detail: str

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload.

        Returns
        -------
        dict[str, object]
            JSON-ready payload.
        """
        return {
            "layer": self.layer,
            "rule_name": self.rule_name,
            "dataset_name": self.dataset_name,
            "source": self.source,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class LayerReport:
    """Static + dynamic report for a fallback layer."""

    static: Mapping[str, object]
    failures: tuple[DynamicFailure, ...] = ()
    skipped: tuple[str, ...] = ()

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload.

        Returns
        -------
        dict[str, object]
            JSON-ready payload.
        """
        return {
            "static": dict(self.static),
            "dynamic_failures": [failure.payload() for failure in self.failures],
            "dynamic_skipped": list(self.skipped),
        }


@dataclass(frozen=True)
class FallbackCoverageReport:
    """Fallback coverage report with static inventory and dynamic failures."""

    layers: Mapping[LayerName, LayerReport]
    created_at: str

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload.

        Returns
        -------
        dict[str, object]
            JSON-ready payload.
        """
        return {
            "created_at": self.created_at,
            "layers": {name: report.payload() for name, report in self.layers.items()},
            "summary": _summary(self.layers),
        }


@dataclass(frozen=True)
class RuleQueryDemand:
    """Query demand derived from rules or dataset specs."""

    rule_name: str
    dataset_name: str | None
    source: str
    spec: IbisQuerySpec


@dataclass(frozen=True)
class CoverageContext:
    """Shared context for fallback coverage checks."""

    registry: FunctionRegistry
    expr_registry: IbisExprRegistry
    schema_registry: SchemaRegistry
    ctx: ExecutionContext
    backend: BaseBackend
    session_ctx: SessionContext


def build_fallback_coverage_report(
    rules: Sequence[RuleDefinition] | None = None,
    *,
    include_relationship_rules: bool = True,
    include_extract_queries: bool = True,
) -> FallbackCoverageReport:
    """Build the fallback coverage report for configured rules.

    Parameters
    ----------
    rules
        Optional rules to assess.
    include_relationship_rules
        Whether to include relspec relationship rules.
    include_extract_queries
        Whether to include extract dataset query specs.

    Returns
    -------
    FallbackCoverageReport
        Static + dynamic fallback coverage report.
    """
    rules = (
        default_rule_definitions(include_relationship_rules=include_relationship_rules)
        if rules is None
        else rules
    )
    demands = collect_rule_demands(rules, include_extract_queries=include_extract_queries)
    query_demands, query_skipped = _collect_query_demands(
        rules,
        include_extract_queries=include_extract_queries,
    )
    context = _build_context()
    layers = _build_layers(
        rules,
        demands=demands,
        query_demands=query_demands,
        query_skipped=query_skipped,
        context=context,
    )
    return FallbackCoverageReport(layers=layers, created_at=datetime.now(UTC).isoformat())


def _build_layers(
    rules: Sequence[RuleDefinition],
    *,
    demands: RuleDemandIndex,
    query_demands: Sequence[RuleQueryDemand],
    query_skipped: Sequence[str],
    context: CoverageContext,
) -> dict[LayerName, LayerReport]:
    return {
        "datafusion_sql_fallback": _datafusion_sql_layer(
            query_demands,
            query_skipped,
            context=context,
        ),
        "datafusion_policy_hooks": _datafusion_policy_layer(),
        "arrowdsl_lane_fallback": _arrowdsl_lane_layer(rules, ctx=context.ctx),
        "ibis_backend_fallback": _ibis_backend_layer(
            query_demands,
            context=context,
        ),
        "udf_kernel_fallback": _kernel_layer(demands.kernel_names, ctx=context.ctx),
        "incremental_data_fallback": _incremental_layer(),
        "policy_driven_fallback": _policy_layer(),
    }


def _collect_query_demands(
    rules: Sequence[RuleDefinition],
    *,
    include_extract_queries: bool,
) -> tuple[tuple[RuleQueryDemand, ...], tuple[str, ...]]:
    demands: list[RuleQueryDemand] = []
    skipped: list[str] = []
    for rule in rules:
        _append_rel_ops_demands(rule, demands, skipped)
        _append_normalize_demands(rule, demands, skipped)
        _append_relationship_skips(rule, skipped)
    if include_extract_queries:
        demands.extend(_extract_query_demands())
    return tuple(demands), tuple(sorted(set(skipped)))


def _append_rel_ops_demands(
    rule: RuleDefinition,
    demands: list[RuleQueryDemand],
    skipped: list[str],
) -> None:
    if not rule.rel_ops:
        return
    spec = query_spec_from_rel_ops(rule.rel_ops)
    if spec is None:
        return
    dataset_name = _dataset_for_rel_ops(rule.rel_ops, rule.inputs)
    if dataset_name is None:
        skipped.append(f"rel_ops:{rule.name}:missing_dataset")
        return
    demands.append(
        RuleQueryDemand(
            rule_name=rule.name,
            dataset_name=dataset_name,
            source="rel_ops",
            spec=spec,
        )
    )


def _append_normalize_demands(
    rule: RuleDefinition,
    demands: list[RuleQueryDemand],
    skipped: list[str],
) -> None:
    payload = rule.payload
    if not isinstance(payload, NormalizePayload) or payload.query is None:
        return
    dataset_name = rule.inputs[0] if rule.inputs else None
    if dataset_name is None:
        skipped.append(f"normalize_query:{rule.name}:missing_input")
        return
    demands.append(
        RuleQueryDemand(
            rule_name=rule.name,
            dataset_name=dataset_name,
            source="normalize_query",
            spec=payload.query,
        )
    )


def _append_relationship_skips(rule: RuleDefinition, skipped: list[str]) -> None:
    payload = rule.payload
    if (
        isinstance(payload, RelationshipPayload)
        and (payload.predicate is not None or payload.project is not None)
    ):
        skipped.append(f"relationship_expr:{rule.name}:not_query_spec")


def _extract_query_demands() -> list[RuleQueryDemand]:
    ctx = QueryContext()
    return [
        RuleQueryDemand(
            rule_name=row.name,
            dataset_name=row.name,
            source="extract_query",
            spec=build_query_spec(row, ctx=ctx),
        )
        for row in EXTRACT_DATASET_ROWS
    ]


def _dataset_for_rel_ops(ops: Sequence[object], inputs: Sequence[str]) -> str | None:
    for op in ops:
        if isinstance(op, ScanOp) and op.source:
            return op.source
    if len(inputs) == 1:
        return inputs[0]
    return None


def _build_schema_registry() -> SchemaRegistry:
    registry = SchemaRegistry()
    for row in CPG_DATASET_ROWS:
        registry.register_dataset(cpg_dataset_spec(row.name))
    for row in NORMALIZE_DATASET_ROWS:
        registry.register_dataset(normalize_dataset_spec(row.name))
    for row in EXTRACT_DATASET_ROWS:
        registry.register_dataset(extract_dataset_spec(row.name))
    for row in INCREMENTAL_DATASET_ROWS:
        registry.register_dataset(incremental_dataset_spec(row.name))
    return registry


def _build_context() -> CoverageContext:
    df_profile = DataFusionRuntimeProfile()
    runtime = RuntimeProfile(
        name="fallback_coverage",
        determinism=DeterminismTier.BEST_EFFORT,
        datafusion=df_profile,
    )
    ctx = ExecutionContext(runtime=runtime)
    session_ctx = SessionContext()
    backend = _ibis_datafusion_connect(session_ctx)
    return CoverageContext(
        registry=default_function_registry(),
        expr_registry=default_expr_registry(),
        schema_registry=_build_schema_registry(),
        ctx=ctx,
        backend=backend,
        session_ctx=session_ctx,
    )


def _datafusion_sql_layer(
    demands: Sequence[RuleQueryDemand],
    skipped: Sequence[str],
    *,
    context: CoverageContext,
) -> LayerReport:
    failures: list[DynamicFailure] = []
    plan_cache: dict[tuple[str, str], IbisPlan] = {}
    for demand in demands:
        dataset_name = demand.dataset_name
        if dataset_name is None:
            failures.append(
                DynamicFailure(
                    layer="datafusion_sql_fallback",
                    rule_name=demand.rule_name,
                    dataset_name=None,
                    source=demand.source,
                    detail="missing dataset binding for query spec",
                )
            )
            continue
        try:
            schema = _schema_for_demand(demand, schema_registry=context.schema_registry)
            plan = _plan_for_dataset(
                dataset_name,
                schema_registry=context.schema_registry,
                backend=context.backend,
                plan_cache=plan_cache,
                schema_override=schema,
            )
        except KeyError:
            failures.append(
                DynamicFailure(
                    layer="datafusion_sql_fallback",
                    rule_name=demand.rule_name,
                    dataset_name=dataset_name,
                    source=demand.source,
                    detail="missing schema for dataset",
                )
            )
            continue
        try:
            expr = apply_query_spec(plan.expr, spec=demand.spec)
        except (KeyError, TypeError, ValueError) as exc:
            failures.append(
                DynamicFailure(
                    layer="datafusion_sql_fallback",
                    rule_name=demand.rule_name,
                    dataset_name=dataset_name,
                    source=demand.source,
                    detail=f"query_spec_compile: {exc}",
                )
            )
            continue
        query_plan = IbisPlan(expr=expr, ordering=plan.ordering)
        options = DataFusionCompileOptions(
            schema_map=_schema_map_for_table(dataset_name, schema=schema)
        )
        options = apply_execution_policy(
            options,
            execution_policy=AdapterExecutionPolicy(
                allow_fallback=False,
                fail_on_fallback=True,
            ),
            execution_label=ExecutionLabel(
                rule_name=demand.rule_name,
                output_dataset=dataset_name,
            ),
        )
        try:
            ibis_plan_to_datafusion(
                query_plan,
                backend=cast("IbisCompilerBackend", context.backend),
                ctx=context.session_ctx,
                options=options,
            )
        except (RuntimeError, TypeError, ValueError, OSError) as exc:
                failures.append(
                    DynamicFailure(
                        layer="datafusion_sql_fallback",
                        rule_name=demand.rule_name,
                        dataset_name=dataset_name,
                        source=demand.source,
                        detail=str(exc),
                    )
                )
    static = {"demand_count": len(demands)}
    return LayerReport(static=static, failures=tuple(failures), skipped=tuple(skipped))


def _ibis_backend_layer(
    demands: Sequence[RuleQueryDemand],
    *,
    context: CoverageContext,
) -> LayerReport:
    expr_support = _expr_registry_support(
        demands,
        registry=context.registry,
        expr_registry=context.expr_registry,
    )
    missing_schema = _dataset_schema_missing(
        demands,
        schema_registry=context.schema_registry,
    )
    failures: list[DynamicFailure] = []
    plan_cache: dict[tuple[str, str], IbisPlan] = {}
    for demand in demands:
        dataset_name = demand.dataset_name
        if dataset_name is None:
            failures.append(
                DynamicFailure(
                    layer="ibis_backend_fallback",
                    rule_name=demand.rule_name,
                    dataset_name=None,
                    source=demand.source,
                    detail="missing dataset binding for query spec",
                )
            )
            continue
        try:
            schema = _schema_for_demand(demand, schema_registry=context.schema_registry)
            plan = _plan_for_dataset(
                dataset_name,
                schema_registry=context.schema_registry,
                backend=context.backend,
                plan_cache=plan_cache,
                schema_override=schema,
            )
        except KeyError:
            failures.append(
                DynamicFailure(
                    layer="ibis_backend_fallback",
                    rule_name=demand.rule_name,
                    dataset_name=dataset_name,
                    source=demand.source,
                    detail="missing schema for dataset",
                )
            )
            continue
        try:
            expr = apply_query_spec(plan.expr, spec=demand.spec)
        except (KeyError, TypeError, ValueError) as exc:
            failures.append(
                DynamicFailure(
                    layer="ibis_backend_fallback",
                    rule_name=demand.rule_name,
                    dataset_name=dataset_name,
                    source=demand.source,
                    detail=f"query_spec_compile: {exc}",
                )
            )
            continue
        missing_ops = unsupported_operations(
            expr,
            backend=cast("OperationSupportBackend", context.backend),
        )
        if missing_ops:
            failures.append(
                DynamicFailure(
                    layer="ibis_backend_fallback",
                    rule_name=demand.rule_name,
                    dataset_name=dataset_name,
                    source=demand.source,
                    detail=f"unsupported ops: {', '.join(missing_ops)}",
                )
            )
    static = {
        "expr_calls_missing_registry": _payload_mapping(expr_support["missing_registry"]),
        "expr_calls_missing_ibis": _payload_mapping(expr_support["missing_ibis"]),
        "expr_calls_ibis_fallback": _payload_mapping(expr_support["ibis_fallback"]),
        "dataset_schema_missing": _payload_mapping(missing_schema),
    }
    return LayerReport(static=static, failures=tuple(failures))


def _arrowdsl_lane_layer(rules: Sequence[RuleDefinition], *, ctx: ExecutionContext) -> LayerReport:
    failures: list[DynamicFailure] = []
    ops_by_rule: dict[str, set[str]] = {}
    for rule in rules:
        for op_name in RULE_KIND_TO_OPS.get(rule.kind, ()):
            ops_by_rule.setdefault(op_name, set()).add(rule.name)
    for op_name, rule_names in ops_by_rule.items():
        op_def = OP_CATALOG.get(op_name)
        if op_def is None:
            failures.extend(
                DynamicFailure(
                    layer="arrowdsl_lane_fallback",
                    rule_name=rule_name,
                    dataset_name=None,
                    source=op_name,
                    detail="op missing from catalog",
                )
                for rule_name in sorted(rule_names)
            )
            continue
        lane = _select_lane(op_def.lanes, ctx=ctx, op_name=op_name)
        if lane != "datafusion":
            failures.extend(
                DynamicFailure(
                    layer="arrowdsl_lane_fallback",
                    rule_name=rule_name,
                    dataset_name=None,
                    source=op_name,
                    detail=f"selected lane: {lane}",
                )
                for rule_name in sorted(rule_names)
            )
    static = {
        "op_lanes": {
            name: sorted(op_def.lanes) for name, op_def in OP_CATALOG.items() if name in ops_by_rule
        },
        "rules_by_op": _payload_mapping(ops_by_rule),
    }
    return LayerReport(static=static, failures=tuple(failures))


def _kernel_layer(
    demands: Mapping[str, set[str]],
    *,
    ctx: ExecutionContext,
) -> LayerReport:
    datafusion_kernels = set(datafusion_kernel_registry())
    failures: list[DynamicFailure] = []
    for name, rule_names in demands.items():
        capability = kernel_capability(name, ctx=ctx)
        if capability.lane == KernelLane.ARROW_FALLBACK:
            failures.extend(
                DynamicFailure(
                    layer="udf_kernel_fallback",
                    rule_name=rule_name,
                    dataset_name=None,
                    source=name,
                    detail=f"kernel lane: {capability.lane.value}",
                )
                for rule_name in sorted(rule_names)
            )
    static = {
        "kernel_names": _payload_mapping(demands),
        "datafusion_kernel_registry": sorted(datafusion_kernels),
    }
    return LayerReport(static=static, failures=tuple(failures))


def _incremental_layer() -> LayerReport:
    static = {"fallback_sites": list(INCREMENTAL_FALLBACK_SITES)}
    skipped = ("dynamic check requires state store datasets",)
    return LayerReport(static=static, skipped=skipped)


def _policy_layer() -> LayerReport:
    static = {
        "graph_allow_fallback_default": True,
        "ibis_allow_fallback_default": True,
        "datafusion_allow_fallback_default": True,
        "arrowdsl_allow_fallback_default": True,
    }
    return LayerReport(static=static)


def _datafusion_policy_layer() -> LayerReport:
    static = {
        "diagnostics_table": "datafusion_fallbacks_v1",
        "capture_fallbacks_default": True,
        "labeled_fallbacks_default": True,
        "adapter_policy_defaults": {
            "allow_fallback": AdapterExecutionPolicy().allow_fallback,
            "fail_on_fallback": AdapterExecutionPolicy().fail_on_fallback,
            "force_sql": AdapterExecutionPolicy().force_sql,
        },
    }
    return LayerReport(static=static)


def _plan_for_dataset(
    name: str,
    *,
    schema_registry: SchemaRegistry,
    backend: BaseBackend,
    plan_cache: dict[tuple[str, str], IbisPlan],
    schema_override: pa.Schema | None = None,
) -> IbisPlan:
    cache_key = (name, "row" if schema_override is not None else "dataset")
    cached = plan_cache.get(cache_key)
    if cached is not None:
        return cached
    if schema_override is None:
        spec = schema_registry.dataset_specs.get(name)
        if spec is None:
            msg = f"Missing schema for dataset {name!r}."
            raise KeyError(msg)
        arrow_schema = pa.schema(spec.schema())
    else:
        arrow_schema = schema_override
    arrays = [pa.array([], type=field.type) for field in arrow_schema]
    table = pa.Table.from_arrays(arrays, schema=arrow_schema)
    plan = table_to_ibis(
        table,
        options=SourceToIbisOptions(
            backend=backend,
            name=name,
        ),
    )
    plan_cache[cache_key] = plan
    return plan


def _expr_registry_support(
    demands: Sequence[RuleQueryDemand],
    *,
    registry: FunctionRegistry,
    expr_registry: IbisExprRegistry,
) -> dict[str, dict[str, set[str]]]:
    registry_funcs = expr_registry.functions
    missing_registry: dict[str, set[str]] = {}
    missing_ibis: dict[str, set[str]] = {}
    ibis_fallback: dict[str, set[str]] = {}
    for demand in demands:
        for name in _expr_calls_from_query(demand.spec):
            if name not in registry.specs:
                missing_registry.setdefault(name, set()).add(demand.rule_name)
            if name in registry_funcs:
                continue
            ibis_fn = getattr(ibis, name, None)
            if callable(ibis_fn):
                ibis_fallback.setdefault(name, set()).add(demand.rule_name)
            else:
                missing_ibis.setdefault(name, set()).add(demand.rule_name)
    return {
        "missing_registry": missing_registry,
        "missing_ibis": missing_ibis,
        "ibis_fallback": ibis_fallback,
    }


def _dataset_schema_missing(
    demands: Sequence[RuleQueryDemand],
    *,
    schema_registry: SchemaRegistry,
) -> dict[str, set[str]]:
    missing: dict[str, set[str]] = {}
    for demand in demands:
        if demand.dataset_name is None:
            missing.setdefault("unknown", set()).add(demand.rule_name)
            continue
        try:
            _schema_for_demand(demand, schema_registry=schema_registry)
        except KeyError:
            missing.setdefault(demand.dataset_name, set()).add(demand.rule_name)
    return missing


def _schema_for_demand(
    demand: RuleQueryDemand,
    *,
    schema_registry: SchemaRegistry,
) -> pa.Schema:
    dataset_name = demand.dataset_name
    if dataset_name is None:
        msg = "Missing dataset name for schema lookup."
        raise KeyError(msg)
    if demand.source == "extract_query":
        row_schema = pa.schema(extract_dataset_row_schema(dataset_name))
        spec = schema_registry.dataset_specs.get(dataset_name)
        if spec is None:
            msg = f"Missing schema for dataset {dataset_name!r}."
            raise KeyError(msg)
        dataset_schema = pa.schema(spec.schema())
        return _merge_schemas(row_schema, dataset_schema)
    spec = schema_registry.dataset_specs.get(dataset_name)
    if spec is None:
        msg = f"Missing schema for dataset {dataset_name!r}."
        raise KeyError(msg)
    return pa.schema(spec.schema())


def _schema_map_for_table(name: str, *, schema: pa.Schema) -> dict[str, dict[str, str]]:
    return {name: {field.name: str(field.type) for field in schema}}


def _merge_schemas(primary: pa.Schema, secondary: pa.Schema) -> pa.Schema:
    fields = list(primary)
    names = {field.name for field in primary}
    for field in secondary:
        if field.name in names:
            continue
        fields.append(field)
        names.add(field.name)
    return pa.schema(fields)


def _expr_calls_from_query(spec: IbisQuerySpec) -> set[str]:
    names: set[str] = set()
    for expr in spec.projection.derived.values():
        _collect_expr_calls(expr, names)
    if spec.predicate is not None:
        _collect_expr_calls(spec.predicate, names)
    if spec.pushdown_predicate is not None:
        _collect_expr_calls(spec.pushdown_predicate, names)
    return names


def _collect_expr_calls(expr: object, names: set[str]) -> None:
    op = getattr(expr, "op", None)
    name = getattr(expr, "name", None)
    args = getattr(expr, "args", ())
    if op == "call" and isinstance(name, str):
        names.add(name)
    if isinstance(args, Sequence):
        for arg in args:
            _collect_expr_calls(arg, names)


def _select_lane(
    lanes: Iterable[Lane],
    *,
    ctx: ExecutionContext,
    op_name: str,
) -> Lane:
    if (
        ctx.determinism in {DeterminismTier.CANONICAL, DeterminismTier.STABLE_SET}
        and op_name == "order_by"
        and "kernel" in lanes
    ):
        return "kernel"
    if ctx.runtime.datafusion is not None and "datafusion" in lanes:
        return "datafusion"
    if "acero" in lanes:
        return "acero"
    if "kernel" in lanes:
        return "kernel"
    msg = f"No supported lane for op {op_name!r}."
    raise ValueError(msg)


def _ibis_datafusion_connect(ctx: SessionContext) -> BaseBackend:
    msg = "Ibis datafusion backend is unavailable; install ibis-framework[datafusion]."
    module_names = ("ibis.datafusion", "ibis.backends.datafusion")
    for module_name in module_names:
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name == module_name:
                continue
            raise ImportError(msg) from exc
        except ImportError as exc:
            raise ImportError(msg) from exc
        connect = getattr(module, "connect", None)
        if callable(connect):
            return cast("BaseBackend", connect(ctx))
        backend_cls = getattr(module, "Backend", None)
        if callable(backend_cls):
            backend = backend_cls()
            connect = getattr(backend, "connect", None)
            if callable(connect):
                return cast("BaseBackend", connect(ctx))
    raise ImportError(msg)


def _payload_mapping(source: Mapping[str, Iterable[str]]) -> dict[str, list[str]]:
    return {key: sorted(set(values)) for key, values in source.items()}


def _summary(layers: Mapping[LayerName, LayerReport]) -> dict[str, int]:
    summary: dict[str, int] = {}
    total_failures = 0
    for name, report in layers.items():
        count = len(report.failures)
        summary[f"{name}_failures"] = count
        total_failures += count
    summary["total_failures"] = total_failures
    return summary


def render_fallback_coverage_markdown(report: FallbackCoverageReport) -> str:
    """Render a Markdown summary for the fallback coverage report.

    Returns
    -------
    str
        Markdown report contents.
    """
    lines: list[str] = []
    lines.append("# Fallback Coverage Report")
    lines.append("")
    lines.append(f"Generated: {report.created_at}")
    lines.append("")
    lines.append("## Summary")
    for key, value in sorted(_summary(report.layers).items()):
        lines.append(f"- {key}: {value}")
    lines.append("")
    for layer_name, layer in report.layers.items():
        lines.append(f"## {layer_name}")
        lines.append(f"- static_keys: {len(layer.static)}")
        lines.append(f"- dynamic_failures: {len(layer.failures)}")
        if layer.skipped:
            lines.append(f"- dynamic_skipped: {len(layer.skipped)}")
        if layer.failures:
            lines.append("- sample_failures:")
            for failure in layer.failures[:10]:
                rule = failure.rule_name or "unknown_rule"
                dataset = failure.dataset_name or "unknown_dataset"
                source = failure.source or "unknown_source"
                lines.append(
                    f"  - {rule} | {dataset} | {source} | {failure.detail}"
                )
        lines.append("")
    return "\n".join(lines)


__all__ = [
    "DynamicFailure",
    "FallbackCoverageReport",
    "LayerReport",
    "RuleQueryDemand",
    "build_fallback_coverage_report",
    "render_fallback_coverage_markdown",
]
