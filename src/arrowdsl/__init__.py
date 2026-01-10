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
from .runtime import DeterminismTier, ExecutionContext, Ordering, OrderingLevel, RuntimeProfile, ScanProfile
from .runner import run_pipeline

__all__ = [
    "Contract",
    "DedupeSpec",
    "SortKey",
    "E",
    "ProjectionSpec",
    "QuerySpec",
    "Plan",
    "JoinSpec",
    "hash_join",
    "compile_to_acero_scan",
    "open_dataset",
    "make_scanner",
    "scan_to_table",
    "finalize",
    "FinalizeResult",
    "run_pipeline",
    "DeterminismTier",
    "ExecutionContext",
    "RuntimeProfile",
    "ScanProfile",
    "Ordering",
    "OrderingLevel",
    "canonical_sort",
    "canonical_sort_if_canonical",
    "apply_dedupe",
    "explode_list_column",
]
