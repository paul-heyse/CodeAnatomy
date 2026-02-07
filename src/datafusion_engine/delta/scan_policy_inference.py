"""Scan policy inference from plan signals."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from datafusion_engine.lineage.datafusion import ScanLineage
    from datafusion_engine.plan.signals import NormalizedPlanStats, PlanSignals
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from schema_spec.system import ScanPolicyConfig


# Threshold below which file pruning overhead is not worthwhile.
_SMALL_TABLE_ROW_THRESHOLD = 10_000


@dataclass(frozen=True)
class ScanPolicyOverride:
    """Per-dataset scan policy override derived from plan signals.

    Parameters
    ----------
    dataset_name
        Logical name of the scanned dataset.
    policy
        Inferred scan policy config for this dataset.
    reasons
        Human-readable reasons for policy derivation.
    """

    dataset_name: str
    policy: ScanPolicyConfig
    reasons: tuple[str, ...] = ()


def derive_scan_policy_overrides(
    signals: PlanSignals,
    *,
    base_policy: ScanPolicyConfig | None = None,
) -> tuple[ScanPolicyOverride, ...]:
    """Derive per-dataset scan policy overrides from plan signals.

    Examine lineage scans for filter predicates, projected columns, and
    table size signals. Return overrides only for datasets where the
    inferred policy differs from the base policy.

    Parameters
    ----------
    signals
        Typed plan signals containing lineage and stats.
    base_policy
        Baseline scan policy to derive overrides from.

    Returns:
    -------
    tuple[ScanPolicyOverride, ...]
        Per-dataset scan policy overrides with reasons.
    """
    if signals.lineage is None:
        return ()

    from schema_spec.system import ScanPolicyConfig as ScanPolicyCls

    effective_base = base_policy or ScanPolicyCls()
    overrides: list[ScanPolicyOverride] = []
    for scan in signals.lineage.scans:
        override = _infer_override_for_scan(
            scan,
            base_policy=effective_base,
            stats=signals.stats,
        )
        if override is not None:
            overrides.append(override)
    return tuple(overrides)


def _infer_override_for_scan(
    scan: ScanLineage,
    *,
    base_policy: ScanPolicyConfig,
    stats: NormalizedPlanStats | None,
) -> ScanPolicyOverride | None:
    """Infer scan policy override for a single scan lineage entry.

    Parameters
    ----------
    scan
        Scan lineage entry for a single dataset.
    base_policy
        Baseline scan policy config.
    stats
        Normalized plan statistics.

    Returns:
    -------
    ScanPolicyOverride | None
        Override when policy differs from base, otherwise ``None``.
    """
    reasons: list[str] = []
    delta_scan_overrides: dict[str, object] = {}
    listing_overrides: dict[str, object] = {}

    # Small tables: disable statistics collection (overhead not worthwhile)
    if _is_small_table(stats):
        listing_overrides["collect_statistics"] = False
        reasons.append("small_table")

    # Tables with pushed filters: enable Parquet pushdown
    if scan.pushed_filters:
        delta_scan_overrides["enable_parquet_pushdown"] = True
        reasons.append("has_pushed_filters")

    if not reasons:
        return None

    # Build inferred policy config from base + overrides
    inferred_listing = (
        msgspec.structs.replace(base_policy.listing, **listing_overrides)
        if listing_overrides
        else base_policy.listing
    )
    inferred_delta_listing = (
        msgspec.structs.replace(base_policy.delta_listing, **listing_overrides)
        if listing_overrides
        else base_policy.delta_listing
    )
    inferred_delta_scan = (
        msgspec.structs.replace(base_policy.delta_scan, **delta_scan_overrides)
        if delta_scan_overrides
        else base_policy.delta_scan
    )
    inferred_policy = msgspec.structs.replace(
        base_policy,
        listing=inferred_listing,
        delta_listing=inferred_delta_listing,
        delta_scan=inferred_delta_scan,
    )
    return ScanPolicyOverride(
        dataset_name=scan.dataset_name,
        policy=inferred_policy,
        reasons=tuple(reasons),
    )


def _is_small_table(stats: NormalizedPlanStats | None) -> bool:
    """Return True when plan statistics indicate a small table.

    Parameters
    ----------
    stats
        Normalized plan statistics.

    Returns:
    -------
    bool
        ``True`` when the table has fewer rows than the threshold.
    """
    if stats is None or stats.num_rows is None:
        return False
    return stats.num_rows < _SMALL_TABLE_ROW_THRESHOLD


def scan_policy_overrides_by_dataset(
    overrides: tuple[ScanPolicyOverride, ...],
) -> Mapping[str, ScanPolicyConfig]:
    """Index scan policy overrides by dataset name.

    Parameters
    ----------
    overrides
        Scan policy overrides from ``derive_scan_policy_overrides()``.

    Returns:
    -------
    Mapping[str, ScanPolicyConfig]
        Per-dataset scan policy configs keyed by dataset name.
    """
    return {override.dataset_name: override.policy for override in overrides}


def scan_policy_override_artifact_payload(
    override: ScanPolicyOverride,
) -> dict[str, object]:
    """Build artifact payload for a scan policy override decision.

    Parameters
    ----------
    override
        Scan policy override with reasons.

    Returns:
    -------
    dict[str, object]
        Payload suitable for ``profile.record_artifact()``.
    """
    return {
        "dataset_name": override.dataset_name,
        "reasons": list(override.reasons),
        "override_applied": True,
    }


def record_scan_policy_decisions(
    profile: DataFusionRuntimeProfile | None,
    *,
    overrides: tuple[ScanPolicyOverride, ...],
) -> None:
    """Record scan policy override decisions as observability artifacts.

    Parameters
    ----------
    profile
        Runtime profile for artifact recording, or ``None`` to skip.
    overrides
        Scan policy overrides from ``derive_scan_policy_overrides()``.
    """
    if profile is None:
        return
    for override in overrides:
        payload = scan_policy_override_artifact_payload(override)
        profile.record_artifact("scan_policy_override_v1", payload)


__all__ = [
    "ScanPolicyOverride",
    "derive_scan_policy_overrides",
    "record_scan_policy_decisions",
    "scan_policy_override_artifact_payload",
    "scan_policy_overrides_by_dataset",
]
