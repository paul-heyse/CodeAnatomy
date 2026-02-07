"""Canonical table size tier classification and threshold constants.

Centralise row-count thresholds that several subsystems use independently
to classify tables as small, medium, or large.  Having a single source
of truth prevents silent drift between scan-policy inference, adaptive
write sizing, and incremental streaming diagnostics.
"""

from __future__ import annotations

from enum import StrEnum

from serde_msgspec import StructBaseStrict


class TableSizeTier(StrEnum):
    """Classification tier based on estimated row count."""

    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


class TableSizeThresholds(StructBaseStrict, frozen=True):
    """Canonical row-count boundaries for table size classification.

    Parameters
    ----------
    small_threshold
        Upper bound (exclusive) for small tables.  Tables with fewer rows
        than this value are classified as :pyattr:`TableSizeTier.SMALL`.
    large_threshold
        Lower bound (exclusive) for large tables.  Tables with more rows
        than this value are classified as :pyattr:`TableSizeTier.LARGE`.
    streaming_threshold
        Row count above which streaming / incremental write diagnostics
        are recorded.  Sits between the small and large boundaries.
    """

    small_threshold: int = 10_000
    large_threshold: int = 1_000_000
    streaming_threshold: int = 100_000


_DEFAULT_THRESHOLDS: TableSizeThresholds = TableSizeThresholds()
"""Module-level default instance for callers that only need the raw values."""


def classify_table_size(
    row_count: int | None,
    thresholds: TableSizeThresholds = _DEFAULT_THRESHOLDS,
) -> TableSizeTier:
    """Classify a table into a size tier from its estimated row count.

    Parameters
    ----------
    row_count
        Estimated number of rows, or ``None`` when statistics are
        unavailable.  ``None`` is treated conservatively as
        :pyattr:`TableSizeTier.MEDIUM`.

    thresholds
        Threshold boundaries to use.  Defaults to the canonical
        project-wide thresholds.

    Returns:
    -------
    TableSizeTier
        The inferred size tier.
    """
    if row_count is None:
        return TableSizeTier.MEDIUM
    if row_count < thresholds.small_threshold:
        return TableSizeTier.SMALL
    if row_count > thresholds.large_threshold:
        return TableSizeTier.LARGE
    return TableSizeTier.MEDIUM


__all__ = [
    "TableSizeThresholds",
    "TableSizeTier",
    "classify_table_size",
]
