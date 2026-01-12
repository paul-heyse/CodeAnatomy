"""Arrow-based DSL for building inference-driven datasets (Acero + compute + contracts)."""

from arrowdsl.compute.expr_specs import HashExprSpec, MaskedHashExprSpec, TrimExprSpec
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
)
from arrowdsl.finalize.finalize import Contract, FinalizeContext, FinalizeResult, finalize
from arrowdsl.plan.ops import DedupeSpec, JoinSpec, SortKey
from arrowdsl.plan.plan import Plan, hash_join, union_all_plans
from arrowdsl.plan.query import (
    ProjectionSpec,
    QuerySpec,
    ScanContext,
    compile_to_acero_scan,
    open_dataset,
)
from arrowdsl.plan.runner import PlanRunResult, run_plan, run_plan_bundle
from arrowdsl.plan.source import DatasetSource, PlanSource, plan_from_source
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
    "explode_list_column",
    "finalize",
    "hash_join",
    "open_dataset",
    "options_metadata_spec",
    "ordering_metadata_spec",
    "plan_from_source",
    "run_plan",
    "run_plan_bundle",
    "union_all_plans",
]
