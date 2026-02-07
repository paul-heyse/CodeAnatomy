"""Performance policy types for DataFusion plan compilation and execution.

Provide a formal performance policy framework tied to runtime config snapshots
and plan signatures. Ensure performance tuning decisions are traceable and
reproducible.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class CachePolicyTier:
    """Cache policy tier configuration.

    Parameters
    ----------
    listing_cache_ttl_seconds
        Time-to-live for listing cache entries in seconds.
    listing_cache_max_entries
        Maximum number of listing cache entries.
    metadata_cache_max_entries
        Maximum number of metadata cache entries.
    enable_dataframe_cache
        Enable selective DataFrame.cache() for fanout-heavy nodes.
    """

    listing_cache_ttl_seconds: int | None = None
    listing_cache_max_entries: int | None = None
    metadata_cache_max_entries: int | None = None
    enable_dataframe_cache: bool = False


@dataclass(frozen=True)
class StatisticsPolicy:
    """Statistics collection stance for plan compilation.

    Parameters
    ----------
    collect_statistics
        Enable statistics collection during plan compilation.
    meta_fetch_concurrency
        Concurrency level for metadata fetch operations.
    fallback_when_unavailable
        Behavior when statistics are unavailable. One of
        ``"skip"``, ``"estimate"``, or ``"error"``.
    """

    collect_statistics: bool = True
    meta_fetch_concurrency: int = 4
    fallback_when_unavailable: str = "skip"


@dataclass(frozen=True)
class PlanBundleComparisonPolicy:
    """Deterministic plan bundle comparison for regression detection.

    Parameters
    ----------
    retain_p0_artifacts
        Retain plan structure hash artifacts.
    retain_p1_artifacts
        Retain physical plan detail artifacts.
    retain_p2_artifacts
        Retain full plan serialization artifacts.
    enable_diff_gates
        Fail on unexpected plan regressions.
    """

    retain_p0_artifacts: bool = True
    retain_p1_artifacts: bool = True
    retain_p2_artifacts: bool = False
    enable_diff_gates: bool = False


@dataclass(frozen=True)
class PerformancePolicy:
    """Top-level performance policy tied to runtime config snapshot.

    Compose cache, statistics, and comparison policies into a single
    traceable configuration object.

    Parameters
    ----------
    cache
        Cache policy tier for listing, metadata, and DataFrame caching.
    statistics
        Statistics collection stance for plan compilation.
    comparison
        Plan bundle comparison policy for regression detection.
    """

    cache: CachePolicyTier = field(default_factory=CachePolicyTier)
    statistics: StatisticsPolicy = field(default_factory=StatisticsPolicy)
    comparison: PlanBundleComparisonPolicy = field(
        default_factory=PlanBundleComparisonPolicy,
    )


def performance_policy_artifact_payload(
    policy: PerformancePolicy,
) -> dict[str, object]:
    """Return an artifact-ready payload for a performance policy.

    Parameters
    ----------
    policy
        Performance policy to serialize.

    Returns:
    -------
    dict[str, object]
        Artifact-ready payload mapping with cache, statistics, and
        comparison sub-dictionaries.
    """
    return {
        "cache": asdict(policy.cache),
        "statistics": asdict(policy.statistics),
        "comparison": asdict(policy.comparison),
    }


__all__ = [
    "CachePolicyTier",
    "PerformancePolicy",
    "PlanBundleComparisonPolicy",
    "StatisticsPolicy",
    "performance_policy_artifact_payload",
]
