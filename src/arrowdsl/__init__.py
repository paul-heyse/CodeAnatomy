"""Arrow-based DSL for building inference-driven datasets (Acero + compute + contracts)."""

from arrowdsl.compute.expr import E
from arrowdsl.compute.kernels import (
    apply_dedupe,
    canonical_sort,
    canonical_sort_if_canonical,
    explode_list_column,
)
from arrowdsl.core.context import (
    DeterminismTier,
    ExecutionContext,
    ExecutionProfile,
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
    make_scanner,
    open_dataset,
    run_pipeline,
    scan_to_table,
)

__all__ = [
    "Contract",
    "DedupeSpec",
    "DeterminismTier",
    "E",
    "ExecutionContext",
    "ExecutionProfile",
    "FinalizeContext",
    "FinalizeResult",
    "JoinSpec",
    "Ordering",
    "OrderingLevel",
    "Plan",
    "ProjectionSpec",
    "QuerySpec",
    "RuntimeProfile",
    "ScanContext",
    "ScanProfile",
    "SortKey",
    "apply_dedupe",
    "canonical_sort",
    "canonical_sort_if_canonical",
    "compile_to_acero_scan",
    "explode_list_column",
    "finalize",
    "hash_join",
    "make_scanner",
    "open_dataset",
    "run_pipeline",
    "scan_to_table",
    "union_all_plans",
]
