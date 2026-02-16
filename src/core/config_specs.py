"""Typed configuration models for CodeAnatomy."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

import msgspec

from core.config_base import config_fingerprint
from obs.otel import OtelConfigSpec
from planning_engine.config import EngineConfigSpec
from serde_msgspec import StructBaseStrict


class PlanConfigSpec(StructBaseStrict, frozen=True):
    """Plan-related configuration values."""

    allow_partial: bool | None = None
    requested_tasks: tuple[str, ...] | None = None
    impacted_tasks: tuple[str, ...] | None = None
    enable_metric_scheduling: bool | None = None
    enable_plan_diagnostics: bool | None = None
    enable_plan_task_submission_hook: bool | None = None
    enable_plan_task_grouping_hook: bool | None = None
    enforce_plan_task_submission: bool | None = None
    execution_authority_enforcement: Literal["warn", "error"] | None = None


class CacheConfigSpec(StructBaseStrict, frozen=True):
    """Cache-related configuration values."""

    policy_profile: str | None = None
    path: str | None = None
    log_to_file: bool | None = None
    opt_in: bool | None = None
    default_behavior: str | None = None
    default_loader_behavior: str | None = None
    default_saver_behavior: str | None = None
    default_nodes: tuple[str, ...] | None = None


class DataFusionCachePolicySpec(StructBaseStrict, frozen=True):
    """DataFusion cache policy configuration values."""

    listing_cache_size: int | None = None
    metadata_cache_size: int | None = None
    stats_cache_size: int | None = None


class DiskCacheSettingsSpec(StructBaseStrict, frozen=True):
    """DiskCache settings overrides."""

    size_limit_bytes: int | None = None
    cull_limit: int | None = None
    eviction_policy: str | None = None
    statistics: bool | None = None
    tag_index: bool | None = None
    shards: int | None = None
    timeout_seconds: float | None = None
    disk_min_file_size: int | None = None
    sqlite_journal_mode: str | None = None
    sqlite_mmap_size: int | None = None
    sqlite_synchronous: str | None = None


class DiskCacheProfileSpec(StructBaseStrict, frozen=True):
    """DiskCache profile overrides."""

    root: str | None = None
    base_settings: DiskCacheSettingsSpec | None = None
    overrides: Mapping[str, DiskCacheSettingsSpec] | None = None
    ttl_seconds: Mapping[str, float | None] | None = None


class DataFusionCacheConfigSpec(StructBaseStrict, frozen=True):
    """DataFusion cache configuration values."""

    cache_policy: DataFusionCachePolicySpec | None = None
    diskcache_profile: DiskCacheProfileSpec | None = None
    snapshot_pinned_mode: Literal["off", "delta_version"] = "off"
    cache_profile_name: (
        Literal[
            "snapshot_pinned",
            "always_latest_ttl30s",
            "multi_tenant_strict",
        ]
        | None
    ) = None


class IncrementalConfigSpec(StructBaseStrict, frozen=True):
    """Incremental execution configuration values."""

    enabled: bool | None = None
    state_dir: str | None = None
    repo_id: str | None = None
    impact_strategy: str | None = None
    git_base_ref: str | None = None
    git_head_ref: str | None = None
    git_changed_only: bool | None = None


class DeltaRestoreConfigSpec(StructBaseStrict, frozen=True):
    """Delta restore configuration values."""

    version: int | None = None
    timestamp: str | None = None


class DeltaExportConfigSpec(StructBaseStrict, frozen=True):
    """Delta export configuration values."""

    version: int | None = None
    timestamp: str | None = None


class DeltaConfigSpec(StructBaseStrict, frozen=True):
    """Delta-specific configuration values."""

    restore: DeltaRestoreConfigSpec | None = None
    export: DeltaExportConfigSpec | None = None


class DocstringsPolicyConfigSpec(StructBaseStrict, frozen=True):
    """Docstring policy configuration values."""

    coverage_threshold: float | None = msgspec.field(
        default=None,
        name="coverage-threshold",
    )
    coverage_action: str | None = msgspec.field(
        default=None,
        name="coverage-action",
    )
    missing_params_action: str | None = msgspec.field(
        default=None,
        name="missing-params-action",
    )
    missing_returns_action: str | None = msgspec.field(
        default=None,
        name="missing-returns-action",
    )

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for docstring policy settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing docstring policy settings.
        """
        return {
            "coverage_threshold": self.coverage_threshold,
            "coverage_action": self.coverage_action,
            "missing_params_action": self.missing_params_action,
            "missing_returns_action": self.missing_returns_action,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for docstring policy settings.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


class DocstringsConfigSpec(StructBaseStrict, frozen=True):
    """Docstring-related configuration values."""

    policy: DocstringsPolicyConfigSpec | None = None


class RootConfigSpec(StructBaseStrict, frozen=True):
    """Root configuration payload for CodeAnatomy."""

    plan: PlanConfigSpec | None = None
    cache: CacheConfigSpec | None = None
    datafusion_cache: DataFusionCacheConfigSpec | None = None
    incremental: IncrementalConfigSpec | None = None
    delta: DeltaConfigSpec | None = None
    docstrings: DocstringsConfigSpec | None = None
    otel: OtelConfigSpec | None = None
    engine: EngineConfigSpec | None = None


__all__ = [
    "CacheConfigSpec",
    "DataFusionCacheConfigSpec",
    "DataFusionCachePolicySpec",
    "DeltaConfigSpec",
    "DeltaExportConfigSpec",
    "DeltaRestoreConfigSpec",
    "DiskCacheProfileSpec",
    "DiskCacheSettingsSpec",
    "DocstringsConfigSpec",
    "DocstringsPolicyConfigSpec",
    "IncrementalConfigSpec",
    "OtelConfigSpec",
    "PlanConfigSpec",
    "RootConfigSpec",
]
