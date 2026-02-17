"""Write-planning primitives for DataFusion write operations."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from core_types import IDENTIFIER_PATTERN
from datafusion_engine.plan.signals import extract_plan_signals
from relspec.table_size_tiers import TableSizeTier, classify_table_size
from utils.hashing import hash_sha256_hex

if TYPE_CHECKING:
    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact


_IDENTIFIER_RE = re.compile(IDENTIFIER_PATTERN)
_MAX_STATS_DATASET_NAME_LEN = 128
_ADAPTIVE_SMALL_FILE_CAP = 32 * 1024 * 1024  # 32 MB
_ADAPTIVE_LARGE_FILE_FLOOR = 128 * 1024 * 1024  # 128 MB


def resolved_partition_by(partition_by: Sequence[str] | None) -> tuple[str, ...]:
    """Return normalized partition columns for write planning."""
    return tuple(str(name) for name in (partition_by or ()))


def normalize_stats_dataset_name(name: str) -> str:
    """Normalize dataset names for stats-policy diagnostics payloads.

    Returns:
        str: Normalized dataset identifier suitable for diagnostics metadata.
    """
    if _IDENTIFIER_RE.fullmatch(name):
        return name
    candidate = Path(name).name if name else ""
    if candidate and _IDENTIFIER_RE.fullmatch(candidate):
        return candidate
    normalized = re.sub(r"[^A-Za-z0-9_.:-]+", "_", candidate).strip("_")
    if not normalized or not normalized[0].isalnum():
        normalized = f"dataset_{hash_sha256_hex(name.encode('utf-8'))[:12]}"
    if len(normalized) > _MAX_STATS_DATASET_NAME_LEN:
        normalized = normalized[:_MAX_STATS_DATASET_NAME_LEN]
    if not _IDENTIFIER_RE.fullmatch(normalized):
        normalized = f"dataset_{hash_sha256_hex(name.encode('utf-8'))[:12]}"
    return normalized


def compute_adaptive_file_size(
    estimated_rows: int,
    base_target: int,
) -> int:
    """Compute adaptive target file size from plan statistics.

    Returns:
        int: Target file size after applying table-size heuristics.
    """
    size_tier = classify_table_size(estimated_rows)
    if size_tier is TableSizeTier.SMALL:
        return min(base_target, _ADAPTIVE_SMALL_FILE_CAP)
    if size_tier is TableSizeTier.LARGE:
        return max(base_target, _ADAPTIVE_LARGE_FILE_FLOOR)
    return base_target


@dataclass(frozen=True)
class AdaptiveFileSizeDecision:
    """Decision record for adaptive file sizing from plan statistics."""

    base_target_file_size: int
    adaptive_target_file_size: int
    estimated_rows: int
    reason: str


def adaptive_file_size_from_bundle(
    plan_bundle: DataFusionPlanArtifact,
    target_file_size: int,
) -> tuple[int, AdaptiveFileSizeDecision | None]:
    """Apply plan-derived adaptive file sizing and return the decision.

    Returns:
        tuple[int, AdaptiveFileSizeDecision | None]: Resolved file size and optional
        decision metadata when adaptation changed the target.
    """
    signals = extract_plan_signals(plan_bundle)
    stats = signals.stats
    row_count = stats.num_rows if stats is not None else None
    if row_count is None:
        return target_file_size, None
    adaptive = compute_adaptive_file_size(row_count, target_file_size)
    if adaptive == target_file_size:
        return target_file_size, None
    size_tier = classify_table_size(row_count)
    return adaptive, AdaptiveFileSizeDecision(
        base_target_file_size=target_file_size,
        adaptive_target_file_size=adaptive,
        estimated_rows=row_count,
        reason=("small_table" if size_tier is TableSizeTier.SMALL else "large_table"),
    )


__all__ = [
    "AdaptiveFileSizeDecision",
    "adaptive_file_size_from_bundle",
    "compute_adaptive_file_size",
    "normalize_stats_dataset_name",
    "resolved_partition_by",
]
