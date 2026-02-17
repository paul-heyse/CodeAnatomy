"""Filtering helpers for CQ CLI result output."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.core.findings_table import (
    FindingsTableOptions,
    apply_filters,
    build_frame,
    flatten_result,
    rehydrate_result,
)

if TYPE_CHECKING:
    from tools.cq.cli_app.context import FilterConfig
    from tools.cq.core.schema import CqResult


def apply_result_filters(result: CqResult, filters: FilterConfig) -> CqResult:
    """Apply CLI filter options to a result.

    Returns:
        CqResult: Filtered result payload.
    """
    if not filters.has_filters:
        return result

    records = flatten_result(result)
    if not records:
        return result

    df = build_frame(records)
    options = FindingsTableOptions(
        include=filters.include if filters.include else None,
        exclude=filters.exclude if filters.exclude else None,
        impact=[str(bucket) for bucket in filters.impact] if filters.impact else None,
        confidence=[str(bucket) for bucket in filters.confidence] if filters.confidence else None,
        severity=[str(level) for level in filters.severity] if filters.severity else None,
        limit=filters.limit,
    )
    filtered_df = apply_filters(df, options)
    return rehydrate_result(result, filtered_df)


__all__ = ["apply_result_filters"]
