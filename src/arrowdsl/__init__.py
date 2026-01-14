"""Arrow-based DSL for building inference-driven datasets (Acero + compute + contracts)."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from arrowdsl.compute.expr_core import HashExprSpec, MaskedHashExprSpec, TrimExprSpec
    from arrowdsl.compute.kernels import (
        apply_dedupe,
        canonical_sort,
        canonical_sort_if_canonical,
        explode_list_column,
    )
    from arrowdsl.core.context import (
        DeterminismTier,
        ExecutionContext,
        Ordering,
        OrderingLevel,
        RuntimeProfile,
        ScanProfile,
        execution_context_factory,
        runtime_profile_factory,
        scan_profile_factory,
    )
    from arrowdsl.finalize.finalize import Contract, FinalizeContext, FinalizeResult, finalize
    from arrowdsl.plan.ops import DedupeSpec, JoinSpec, SortKey
    from arrowdsl.plan.plan import Plan, PlanFactory, hash_join, union_all_plans
    from arrowdsl.plan.query import (
        ProjectionSpec,
        QuerySpec,
        ScanContext,
        compile_to_acero_scan,
        open_dataset,
    )
    from arrowdsl.plan.runner import (
        PlanRunResult,
        run_plan,
        run_plan_bundle,
        run_plan_streamable,
    )
    from arrowdsl.plan.scan_io import DatasetSource, PlanSource, plan_from_source
    from arrowdsl.schema.metadata import options_metadata_spec, ordering_metadata_spec

__all__ = [
    "Contract",
    "DatasetSource",
    "DedupeSpec",
    "DeterminismTier",
    "ExecutionContext",
    "FinalizeContext",
    "FinalizeResult",
    "HashExprSpec",
    "JoinSpec",
    "MaskedHashExprSpec",
    "Ordering",
    "OrderingLevel",
    "Plan",
    "PlanFactory",
    "PlanRunResult",
    "PlanSource",
    "ProjectionSpec",
    "QuerySpec",
    "RuntimeProfile",
    "ScanContext",
    "ScanProfile",
    "SortKey",
    "TrimExprSpec",
    "apply_dedupe",
    "canonical_sort",
    "canonical_sort_if_canonical",
    "compile_to_acero_scan",
    "execution_context_factory",
    "explode_list_column",
    "finalize",
    "hash_join",
    "open_dataset",
    "options_metadata_spec",
    "ordering_metadata_spec",
    "plan_from_source",
    "run_plan",
    "run_plan_bundle",
    "run_plan_streamable",
    "runtime_profile_factory",
    "scan_profile_factory",
    "union_all_plans",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    "HashExprSpec": ("arrowdsl.compute.expr_core", "HashExprSpec"),
    "MaskedHashExprSpec": ("arrowdsl.compute.expr_core", "MaskedHashExprSpec"),
    "TrimExprSpec": ("arrowdsl.compute.expr_core", "TrimExprSpec"),
    "apply_dedupe": ("arrowdsl.compute.kernels", "apply_dedupe"),
    "canonical_sort": ("arrowdsl.compute.kernels", "canonical_sort"),
    "canonical_sort_if_canonical": ("arrowdsl.compute.kernels", "canonical_sort_if_canonical"),
    "explode_list_column": ("arrowdsl.compute.kernels", "explode_list_column"),
    "DeterminismTier": ("arrowdsl.core.context", "DeterminismTier"),
    "ExecutionContext": ("arrowdsl.core.context", "ExecutionContext"),
    "Ordering": ("arrowdsl.core.context", "Ordering"),
    "OrderingLevel": ("arrowdsl.core.context", "OrderingLevel"),
    "RuntimeProfile": ("arrowdsl.core.context", "RuntimeProfile"),
    "ScanProfile": ("arrowdsl.core.context", "ScanProfile"),
    "execution_context_factory": ("arrowdsl.core.context", "execution_context_factory"),
    "runtime_profile_factory": ("arrowdsl.core.context", "runtime_profile_factory"),
    "scan_profile_factory": ("arrowdsl.core.context", "scan_profile_factory"),
    "Contract": ("arrowdsl.finalize.finalize", "Contract"),
    "FinalizeContext": ("arrowdsl.finalize.finalize", "FinalizeContext"),
    "FinalizeResult": ("arrowdsl.finalize.finalize", "FinalizeResult"),
    "finalize": ("arrowdsl.finalize.finalize", "finalize"),
    "DedupeSpec": ("arrowdsl.plan.ops", "DedupeSpec"),
    "JoinSpec": ("arrowdsl.plan.ops", "JoinSpec"),
    "SortKey": ("arrowdsl.plan.ops", "SortKey"),
    "Plan": ("arrowdsl.plan.plan", "Plan"),
    "PlanFactory": ("arrowdsl.plan.plan", "PlanFactory"),
    "hash_join": ("arrowdsl.plan.plan", "hash_join"),
    "union_all_plans": ("arrowdsl.plan.plan", "union_all_plans"),
    "ProjectionSpec": ("arrowdsl.plan.query", "ProjectionSpec"),
    "QuerySpec": ("arrowdsl.plan.query", "QuerySpec"),
    "ScanContext": ("arrowdsl.plan.query", "ScanContext"),
    "compile_to_acero_scan": ("arrowdsl.plan.query", "compile_to_acero_scan"),
    "open_dataset": ("arrowdsl.plan.query", "open_dataset"),
    "PlanRunResult": ("arrowdsl.plan.runner", "PlanRunResult"),
    "run_plan": ("arrowdsl.plan.runner", "run_plan"),
    "run_plan_bundle": ("arrowdsl.plan.runner", "run_plan_bundle"),
    "run_plan_streamable": ("arrowdsl.plan.runner", "run_plan_streamable"),
    "DatasetSource": ("arrowdsl.plan.scan_io", "DatasetSource"),
    "PlanSource": ("arrowdsl.plan.scan_io", "PlanSource"),
    "plan_from_source": ("arrowdsl.plan.scan_io", "plan_from_source"),
    "options_metadata_spec": ("arrowdsl.schema.metadata", "options_metadata_spec"),
    "ordering_metadata_spec": ("arrowdsl.schema.metadata", "ordering_metadata_spec"),
}


def __getattr__(name: str) -> object:
    if name not in _EXPORTS:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attr = _EXPORTS[name]
    module = importlib.import_module(module_name)
    value = getattr(module, attr)
    globals()[name] = value
    return value
