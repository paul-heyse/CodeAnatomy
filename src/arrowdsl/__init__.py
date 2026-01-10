"""Arrow-based DSL for building inference-driven datasets (Acero + compute + contracts)."""

from .contracts import Contract, DedupeSpec, SortKey
from .dataset_io import compile_to_acero_scan, make_scanner, open_dataset, scan_to_table
from .expr import E
from .finalize import FinalizeResult, finalize
from .joins import JoinSpec, hash_join
from .kernels import (
    apply_dedupe,
    canonical_sort,
    canonical_sort_if_canonical,
    explode_list_column,
)
from .plan import Plan
from .queryspec import ProjectionSpec, QuerySpec
from .runner import run_pipeline
from .runtime import (
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
