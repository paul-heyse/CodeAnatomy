"""Arrow-based DSL for building inference-driven datasets (Acero + compute + contracts)."""

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
