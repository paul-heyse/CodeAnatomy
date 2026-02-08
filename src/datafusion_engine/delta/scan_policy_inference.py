"""Scan policy inference from plan signals."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

import msgspec

from relspec.inference_confidence import InferenceConfidence, high_confidence, low_confidence
from relspec.table_size_tiers import TableSizeTier, classify_table_size

if TYPE_CHECKING:
    from datafusion_engine.extensions.runtime_capabilities import RuntimeCapabilitiesSnapshot
    from datafusion_engine.lineage.datafusion import ScanLineage
    from datafusion_engine.plan.signals import NormalizedPlanStats, PlanSignals
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from schema_spec.system import ScanPolicyConfig


# Projection ratio below which column pruning is considered highly beneficial.
# When only a small fraction of columns are projected, enabling parquet
# pushdown is especially valuable because it avoids reading unused columns.
_NARROW_PROJECTION_RATIO_THRESHOLD = 0.5

# Confidence floor for sort-order exploitation signal.  Sort-key matching
# is structural (from plan lineage) and does not depend on statistics,
# so confidence is relatively high.
_SORT_ORDER_CONFIDENCE = 0.85

# Confidence floor for narrow projection signal.
_NARROW_PROJECTION_CONFIDENCE = 0.8


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
    confidence
        Inference confidence score in [0.0, 1.0].  Higher values
        indicate stronger evidence backing the override.
    inference_confidence
        Structured confidence metadata with evidence sources and
        fallback reasoning.  ``None`` when the override was created
        by legacy code paths that do not yet produce structured
        confidence.
    """

    dataset_name: str
    policy: ScanPolicyConfig
    reasons: tuple[str, ...] = ()
    confidence: float = 1.0
    inference_confidence: InferenceConfidence | None = None


def derive_scan_policy_overrides(
    signals: PlanSignals,
    *,
    base_policy: ScanPolicyConfig | None = None,
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None = None,
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
    capability_snapshot
        Optional runtime capability payload used to gate stats-dependent
        heuristics when stats signals are unavailable.

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
            capability_snapshot=capability_snapshot,
            sort_keys=signals.sort_keys,
            projection_ratio=signals.projection_ratio,
        )
        if override is not None:
            overrides.append(override)
    return tuple(overrides)


def _infer_override_for_scan(
    scan: ScanLineage,
    *,
    base_policy: ScanPolicyConfig,
    stats: NormalizedPlanStats | None,
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None,
    sort_keys: tuple[str, ...] = (),
    projection_ratio: float | None = None,
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
    capability_snapshot
        Optional runtime capability payload used to gate stats-dependent
        heuristics when stats signals are unavailable.
    sort_keys
        Column names appearing in plan Sort nodes.  When the scan's
        projected columns already cover the sort keys, a sort-order
        exploitation signal is emitted.
    projection_ratio
        Ratio of projected columns to total schema columns.  When the
        ratio is below the narrow-projection threshold, column pruning
        via parquet pushdown is signaled.

    Returns:
    -------
    ScanPolicyOverride | None
        Override when policy differs from base, otherwise ``None``.
    """
    reasons: list[str] = []
    evidence_sources: list[str] = []
    delta_scan_overrides: dict[str, object] = {}
    listing_overrides: dict[str, object] = {}
    confidence = 1.0
    fallback_reason: str | None = None

    has_stats = stats is not None and stats.num_rows is not None
    has_cap = _stats_heuristics_capable(capability_snapshot)

    # Small tables: disable statistics collection (overhead not worthwhile)
    if _is_small_table(stats, capability_snapshot=capability_snapshot):
        listing_overrides["collect_statistics"] = False
        reasons.append("small_table")
        if has_cap and has_stats:
            confidence = min(confidence, 0.9)
            evidence_sources.extend(("stats", "capabilities"))
        elif has_stats:
            confidence = min(confidence, 0.7)
            evidence_sources.append("stats")
            fallback_reason = "capabilities_unavailable"
        else:
            confidence = min(confidence, 0.5)
            fallback_reason = "no_row_statistics"

    # Tables with pushed filters: enable Parquet pushdown
    if scan.pushed_filters:
        delta_scan_overrides["enable_parquet_pushdown"] = True
        reasons.append("has_pushed_filters")
        confidence = min(confidence, 0.8)
        evidence_sources.append("lineage")
        if fallback_reason is None:
            fallback_reason = "lineage_only_no_stats"

    # Sort-order exploitation: when the scan's projected columns already
    # include all sort keys, downstream sorts may be redundant.  This is a
    # structural signal (no stats dependency) so confidence is relatively high.
    if sort_keys and _scan_covers_sort_keys(scan, sort_keys):
        reasons.append("sort_order_aligned")
        confidence = min(confidence, _SORT_ORDER_CONFIDENCE)
        evidence_sources.append("lineage")

    # Narrow projection: when only a small fraction of columns are projected,
    # enable parquet pushdown to exploit column pruning at the storage layer.
    if _is_narrow_projection(scan, projection_ratio):
        delta_scan_overrides["enable_parquet_pushdown"] = True
        reasons.append("narrow_projection")
        confidence = min(confidence, _NARROW_PROJECTION_CONFIDENCE)
        evidence_sources.append("lineage")

    if not reasons:
        return None

    # Build structured confidence metadata.
    deduped_sources = tuple(dict.fromkeys(evidence_sources))
    structured_confidence = _build_inference_confidence(
        confidence=confidence,
        reasons=tuple(reasons),
        evidence_sources=deduped_sources,
        fallback_reason=fallback_reason,
    )

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
        confidence=confidence,
        inference_confidence=structured_confidence,
    )


_HIGH_CONFIDENCE_THRESHOLD = 0.8


def _build_inference_confidence(
    *,
    confidence: float,
    reasons: tuple[str, ...],
    evidence_sources: tuple[str, ...],
    fallback_reason: str | None,
) -> InferenceConfidence:
    decision_value = ",".join(reasons) if reasons else "none"
    if confidence >= _HIGH_CONFIDENCE_THRESHOLD:
        return high_confidence(
            decision_type="scan_policy",
            decision_value=decision_value,
            evidence_sources=evidence_sources,
            score=confidence,
        )
    return low_confidence(
        decision_type="scan_policy",
        decision_value=decision_value,
        fallback_reason=fallback_reason or "insufficient_evidence",
        evidence_sources=evidence_sources,
        score=confidence,
    )


def _scan_covers_sort_keys(
    scan: ScanLineage,
    sort_keys: tuple[str, ...],
) -> bool:
    """Return True when a scan's projected columns include all sort keys.

    Parameters
    ----------
    scan
        Scan lineage entry with projected columns.
    sort_keys
        Column names from plan Sort nodes.

    Returns:
    -------
    bool
        ``True`` when every sort key is present in the scan's projection.
    """
    if not sort_keys or not scan.projected_columns:
        return False
    projected = set(scan.projected_columns)
    return all(key in projected for key in sort_keys)


def _is_narrow_projection(
    scan: ScanLineage,
    projection_ratio: float | None,
) -> bool:
    """Return True when projection ratio indicates high column-pruning benefit.

    The check requires both a valid projection ratio and the scan to have
    projected columns (avoiding false positives from wildcard projections).

    Parameters
    ----------
    scan
        Scan lineage entry with projected columns.
    projection_ratio
        Ratio of projected columns to total schema columns.

    Returns:
    -------
    bool
        ``True`` when the projection is narrow enough to benefit from
        column pruning.
    """
    if projection_ratio is None:
        return False
    if not scan.projected_columns:
        return False
    return projection_ratio < _NARROW_PROJECTION_RATIO_THRESHOLD


def _is_small_table(
    stats: NormalizedPlanStats | None,
    *,
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None,
) -> bool:
    """Return True when plan statistics indicate a small table.

    Parameters
    ----------
    stats
        Normalized plan statistics.
    capability_snapshot
        Optional runtime capability payload used to gate stats-dependent
        heuristics when stats signals are unavailable.

    Returns:
    -------
    bool
        ``True`` when the table has fewer rows than the threshold.
    """
    if stats is None or stats.num_rows is None:
        if not _stats_heuristics_capable(capability_snapshot):
            return False
        # Conservative default: do not escalate without concrete row statistics.
        return False
    return classify_table_size(int(stats.num_rows)) is TableSizeTier.SMALL


def _stats_heuristics_capable(
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None,
) -> bool:
    payload = _capability_payload(capability_snapshot)
    if not payload:
        return False
    execution_metrics = _mapping_value(payload, "execution_metrics")
    if execution_metrics is None:
        return False
    if _string_value(execution_metrics, "error") is not None:
        return False
    plan_capabilities = _mapping_value(payload, "plan_capabilities")
    if plan_capabilities is None:
        return False
    has_statistics = _bool_value(plan_capabilities, "has_execution_plan_statistics")
    return has_statistics is not False


def _capability_payload(
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None,
) -> Mapping[str, object]:
    if capability_snapshot is None:
        return {}
    if isinstance(capability_snapshot, Mapping):
        return capability_snapshot
    plan_capabilities_payload: dict[str, object] | None = None
    if capability_snapshot.plan_capabilities is not None:
        plan_capabilities_payload = {
            "has_execution_plan_statistics": (
                capability_snapshot.plan_capabilities.has_execution_plan_statistics
            ),
            "has_execution_plan_schema": (
                capability_snapshot.plan_capabilities.has_execution_plan_schema
            ),
            "datafusion_version": capability_snapshot.plan_capabilities.datafusion_version,
            "has_dataframe_execution_plan": (
                capability_snapshot.plan_capabilities.has_dataframe_execution_plan
            ),
        }
    return {
        "execution_metrics": (
            dict(capability_snapshot.execution_metrics)
            if isinstance(capability_snapshot.execution_metrics, Mapping)
            else None
        ),
        "plan_capabilities": plan_capabilities_payload,
    }


def _mapping_value(payload: Mapping[str, object], key: str) -> Mapping[str, object] | None:
    value = payload.get(key)
    if isinstance(value, Mapping):
        return value
    return None


def _bool_value(payload: Mapping[str, object], key: str) -> bool | None:
    value = payload.get(key)
    if isinstance(value, bool):
        return value
    return None


def _string_value(payload: Mapping[str, object], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value:
        return value
    return None


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
    payload: dict[str, object] = {
        "dataset_name": override.dataset_name,
        "reasons": list(override.reasons),
        "override_applied": True,
        "confidence": override.confidence,
    }
    if override.inference_confidence is not None:
        payload["inference_confidence"] = {
            "confidence_score": override.inference_confidence.confidence_score,
            "evidence_sources": list(override.inference_confidence.evidence_sources),
            "fallback_reason": override.inference_confidence.fallback_reason,
            "decision_type": override.inference_confidence.decision_type,
            "decision_value": override.inference_confidence.decision_value,
        }
    return payload


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
    from serde_artifact_specs import SCAN_POLICY_OVERRIDE_SPEC

    for override in overrides:
        payload = scan_policy_override_artifact_payload(override)
        profile.record_artifact(SCAN_POLICY_OVERRIDE_SPEC, payload)


__all__ = [
    "ScanPolicyOverride",
    "derive_scan_policy_overrides",
    "record_scan_policy_decisions",
    "scan_policy_override_artifact_payload",
    "scan_policy_overrides_by_dataset",
]
