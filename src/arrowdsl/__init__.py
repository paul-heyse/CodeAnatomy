"""Arrow-based DSL for building inference-driven datasets (Acero + compute + contracts)."""

from arrowdsl.contracts import Contract, DedupeSpec, SortKey
from arrowdsl.dataset_io import compile_to_acero_scan, make_scanner, open_dataset, scan_to_table
from arrowdsl.expr import E
from arrowdsl.finalize import FinalizeResult, finalize
from arrowdsl.joins import JoinSpec, hash_join
from arrowdsl.kernels import (
    apply_dedupe,
    canonical_sort,
    canonical_sort_if_canonical,
    explode_list_column,
)
from arrowdsl.plan import Plan
from arrowdsl.queryspec import ProjectionSpec, QuerySpec
from arrowdsl.runner import run_pipeline
from arrowdsl.runtime import (
    DeterminismTier,
    ExecutionContext,
    Ordering,
    OrderingLevel,
    RuntimeProfile,
    ScanProfile,
)

__all__ = [
    "Contract",
    "DedupeSpec",
    "DeterminismTier",
    "E",
    "ExecutionContext",
    "FinalizeResult",
    "JoinSpec",
    "Ordering",
    "OrderingLevel",
    "Plan",
    "ProjectionSpec",
    "QuerySpec",
    "RuntimeProfile",
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
]
