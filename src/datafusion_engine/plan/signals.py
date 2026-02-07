"""Unified plan signal extraction for typed consumption."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from datafusion_engine.lineage.datafusion import LineageReport
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.plan.bundle import DataFusionPlanBundle


@dataclass(frozen=True)
class NormalizedPlanStats:
    """Normalized statistics payload with source provenance.

    Parameters
    ----------
    num_rows
        Row count from plan statistics (canonical key).
    total_bytes
        Total byte size from plan statistics.
    partition_count
        Number of partitions in the physical plan.
    stats_source
        Provenance label for the statistics origin.
    """

    num_rows: int | None = None
    total_bytes: int | None = None
    partition_count: int | None = None
    stats_source: str = "unavailable"


@dataclass(frozen=True)
class ScanUnitCompatSummary:
    """Per-scan-unit protocol compatibility summary.

    Parameters
    ----------
    dataset_name
        Logical name of the scanned dataset.
    compatible
        Protocol compatibility result (True/False/None for unconfigured).
    reason
        Human-readable compatibility reason when incompatible.
    """

    dataset_name: str
    compatible: bool | None = None
    reason: str | None = None


@dataclass(frozen=True)
class PlanSignals:
    """Typed signal bundle derived from a DataFusion plan bundle.

    Single source of truth for all plan-signal consumers. All proposals
    (10.2, 10.3, 10.9, 10.10, Wave 4B) read from this instead of raw
    ``plan_details`` or ad-hoc bundle accessors.

    Parameters
    ----------
    schema
        Output schema derived from the plan DataFrame.
    lineage
        Lineage report from the optimized logical plan.
    stats
        Normalized statistics with source provenance.
    scan_compat
        Per-scan-unit Delta protocol compatibility summaries.
    plan_fingerprint
        Stable plan fingerprint for deterministic comparison.
    """

    schema: pa.Schema | None = None
    lineage: LineageReport | None = None
    stats: NormalizedPlanStats | None = None
    scan_compat: tuple[ScanUnitCompatSummary, ...] = ()
    plan_fingerprint: str | None = None


def _extract_stats(plan_details: object) -> NormalizedPlanStats:
    """Extract normalized statistics from plan details.

    Parameters
    ----------
    plan_details
        Plan detail mapping from the plan bundle.

    Returns:
    -------
    NormalizedPlanStats
        Normalized statistics with source provenance.
    """
    if not isinstance(plan_details, dict):
        return NormalizedPlanStats()
    raw_stats = plan_details.get("statistics")
    if isinstance(raw_stats, dict):
        num_rows_raw = raw_stats.get("num_rows")
        if num_rows_raw is None:
            num_rows_raw = raw_stats.get("row_count")
        num_rows = int(num_rows_raw) if num_rows_raw is not None else None
        total_bytes_raw = raw_stats.get("total_bytes")
        total_bytes = int(total_bytes_raw) if total_bytes_raw is not None else None
        return NormalizedPlanStats(
            num_rows=num_rows,
            total_bytes=total_bytes,
            partition_count=_int_or_none(plan_details.get("partition_count")),
            stats_source="plan_details",
        )
    return NormalizedPlanStats(
        partition_count=_int_or_none(plan_details.get("partition_count")),
        stats_source="unavailable",
    )


def _int_or_none(value: object) -> int | None:
    """Coerce a value to int or return None.

    Parameters
    ----------
    value
        Value to coerce.

    Returns:
    -------
    int | None
        Integer value or None if not coercible.
    """
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, (float, str)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


def _extract_scan_compat(
    scan_units: Sequence[ScanUnit],
) -> tuple[ScanUnitCompatSummary, ...]:
    """Build scan-unit compatibility summaries.

    Parameters
    ----------
    scan_units
        Scan units from the plan bundle or execution plan.

    Returns:
    -------
    tuple[ScanUnitCompatSummary, ...]
        Per-scan-unit compatibility summaries.
    """
    return tuple(
        ScanUnitCompatSummary(
            dataset_name=unit.dataset_name,
            compatible=(
                unit.protocol_compatibility.compatible
                if unit.protocol_compatibility is not None
                else None
            ),
            reason=(
                unit.protocol_compatibility.reason
                if unit.protocol_compatibility is not None
                else None
            ),
        )
        for unit in scan_units
    )


def extract_plan_signals(
    bundle: DataFusionPlanBundle,
    *,
    scan_units: Sequence[ScanUnit] | None = None,
) -> PlanSignals:
    """Extract typed signals from a plan bundle.

    Reuse existing helpers for schema and lineage extraction. Do not add
    methods to ``DataFusionPlanBundle``; keep signal derivation external.

    Parameters
    ----------
    bundle
        Canonical DataFusion plan bundle.
    scan_units
        Optional scan units for protocol compatibility summaries.
        Falls back to ``bundle.scan_units`` when not provided.

    Returns:
    -------
    PlanSignals
        Typed signal bundle for plan-signal consumers.
    """
    from datafusion_engine.views.bundle_extraction import (
        arrow_schema_from_df,
        extract_lineage_from_bundle,
    )

    # Schema extraction
    try:
        schema: pa.Schema | None = arrow_schema_from_df(bundle.df)
    except (TypeError, AttributeError):
        schema = None

    # Lineage extraction (may fail when optimized_logical_plan is None)
    lineage: LineageReport | None
    try:
        lineage = extract_lineage_from_bundle(bundle)
    except (ValueError, AttributeError):
        lineage = None

    # Stats normalization
    stats = _extract_stats(bundle.plan_details)

    # Scan unit compatibility
    resolved_units = scan_units if scan_units is not None else bundle.scan_units
    scan_compat = _extract_scan_compat(resolved_units)

    return PlanSignals(
        schema=schema,
        lineage=lineage,
        stats=stats,
        scan_compat=scan_compat,
        plan_fingerprint=bundle.plan_fingerprint,
    )


__all__ = [
    "NormalizedPlanStats",
    "PlanSignals",
    "ScanUnitCompatSummary",
    "extract_plan_signals",
]
