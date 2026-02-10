"""Runtime validation models for root CLI configuration."""

from __future__ import annotations

from typing import Literal

from pydantic import ConfigDict, Field

from runtime_models.base import RuntimeBase
from runtime_models.otel import OtelConfigRuntime


class PlanConfigRuntime(RuntimeBase):
    """Validated plan configuration."""

    allow_partial: bool | None = None
    requested_tasks: tuple[str, ...] | None = None
    impacted_tasks: tuple[str, ...] | None = None
    enable_metric_scheduling: bool | None = None
    enable_plan_diagnostics: bool | None = None
    enable_plan_task_submission_hook: bool | None = None
    enable_plan_task_grouping_hook: bool | None = None
    enforce_plan_task_submission: bool | None = None
    execution_authority_enforcement: Literal["warn", "error"] | None = None


class CacheConfigRuntime(RuntimeBase):
    """Validated cache configuration."""

    policy_profile: str | None = None
    path: str | None = None
    log_to_file: bool | None = None
    opt_in: bool | None = None
    default_behavior: str | None = None
    default_loader_behavior: str | None = None
    default_saver_behavior: str | None = None
    default_nodes: tuple[str, ...] | None = None


class DataFusionCachePolicyRuntime(RuntimeBase):
    """Validated DataFusion cache policy config."""

    listing_cache_size: int | None = Field(default=None, ge=0)
    metadata_cache_size: int | None = Field(default=None, ge=0)
    stats_cache_size: int | None = Field(default=None, ge=0)


class DiskCacheSettingsRuntime(RuntimeBase):
    """Validated DiskCache settings overrides."""

    size_limit_bytes: int | None = Field(default=None, ge=0)
    cull_limit: int | None = Field(default=None, ge=0)
    eviction_policy: str | None = None
    statistics: bool | None = None
    tag_index: bool | None = None
    shards: int | None = Field(default=None, ge=0)
    timeout_seconds: float | None = Field(default=None, ge=0)
    disk_min_file_size: int | None = Field(default=None, ge=0)
    sqlite_journal_mode: str | None = None
    sqlite_mmap_size: int | None = Field(default=None, ge=0)
    sqlite_synchronous: str | None = None


class DiskCacheProfileRuntime(RuntimeBase):
    """Validated DiskCache profile overrides."""

    root: str | None = None
    base_settings: DiskCacheSettingsRuntime | None = None
    overrides: dict[str, DiskCacheSettingsRuntime] | None = None
    ttl_seconds: dict[str, float | None] | None = None


class DataFusionCacheConfigRuntime(RuntimeBase):
    """Validated DataFusion cache configuration."""

    cache_policy: DataFusionCachePolicyRuntime | None = None
    diskcache_profile: DiskCacheProfileRuntime | None = None
    snapshot_pinned_mode: Literal["off", "delta_version"] = "off"
    cache_profile_name: (
        Literal[
            "snapshot_pinned",
            "always_latest_ttl30s",
            "multi_tenant_strict",
        ]
        | None
    ) = None


class IncrementalConfigRuntime(RuntimeBase):
    """Validated incremental config."""

    enabled: bool | None = None
    state_dir: str | None = None
    repo_id: str | None = None
    impact_strategy: str | None = None
    git_base_ref: str | None = None
    git_head_ref: str | None = None
    git_changed_only: bool | None = None


class DeltaRestoreConfigRuntime(RuntimeBase):
    """Validated Delta restore config."""

    version: int | None = Field(default=None, ge=0)
    timestamp: str | None = None


class DeltaExportConfigRuntime(RuntimeBase):
    """Validated Delta export config."""

    version: int | None = Field(default=None, ge=0)
    timestamp: str | None = None


class DeltaConfigRuntime(RuntimeBase):
    """Validated Delta config."""

    restore: DeltaRestoreConfigRuntime | None = None
    export: DeltaExportConfigRuntime | None = None


class DocstringsPolicyConfigRuntime(RuntimeBase):
    """Validated docstring policy config."""

    coverage_threshold: float | None = Field(default=None, ge=0)
    coverage_action: str | None = None
    missing_params_action: str | None = None
    missing_returns_action: str | None = None


class DocstringsConfigRuntime(RuntimeBase):
    """Validated docstrings config."""

    policy: DocstringsPolicyConfigRuntime | None = None


class RootConfigRuntime(RuntimeBase):
    """Validated root configuration payload."""

    model_config = ConfigDict(
        extra="forbid",
        validate_default=True,
        frozen=True,
        arbitrary_types_allowed=True,
        revalidate_instances="always",
    )

    plan: PlanConfigRuntime | None = None
    cache: CacheConfigRuntime | None = None
    datafusion_cache: DataFusionCacheConfigRuntime | None = None
    incremental: IncrementalConfigRuntime | None = None
    delta: DeltaConfigRuntime | None = None
    docstrings: DocstringsConfigRuntime | None = None
    otel: OtelConfigRuntime | None = None


__all__ = [
    "CacheConfigRuntime",
    "DataFusionCacheConfigRuntime",
    "DataFusionCachePolicyRuntime",
    "DeltaConfigRuntime",
    "DeltaExportConfigRuntime",
    "DeltaRestoreConfigRuntime",
    "DiskCacheProfileRuntime",
    "DiskCacheSettingsRuntime",
    "DocstringsConfigRuntime",
    "DocstringsPolicyConfigRuntime",
    "IncrementalConfigRuntime",
    "PlanConfigRuntime",
    "RootConfigRuntime",
]
