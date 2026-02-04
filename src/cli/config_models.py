"""Typed configuration models for CodeAnatomy."""

from __future__ import annotations

import msgspec

from hamilton_pipeline.types import GraphAdapterConfig
from serde_msgspec import StructBaseStrict


class PlanConfig(StructBaseStrict, frozen=True):
    """Plan-related configuration values."""

    allow_partial: bool | None = None
    requested_tasks: tuple[str, ...] | None = None
    impacted_tasks: tuple[str, ...] | None = None
    enable_metric_scheduling: bool | None = None
    enable_plan_diagnostics: bool | None = None
    enable_plan_task_submission_hook: bool | None = None
    enable_plan_task_grouping_hook: bool | None = None
    enforce_plan_task_submission: bool | None = None


class CacheConfig(StructBaseStrict, frozen=True):
    """Cache-related configuration values."""

    policy_profile: str | None = None
    path: str | None = None
    log_to_file: bool | None = None
    opt_in: bool | None = None


class IncrementalConfig(StructBaseStrict, frozen=True):
    """Incremental execution configuration values."""

    enabled: bool | None = None
    state_dir: str | None = None
    repo_id: str | None = None
    impact_strategy: str | None = None
    git_base_ref: str | None = None
    git_head_ref: str | None = None
    git_changed_only: bool | None = None


class DeltaRestoreConfig(StructBaseStrict, frozen=True):
    """Delta restore configuration values."""

    version: int | None = None
    timestamp: str | None = None


class DeltaExportConfig(StructBaseStrict, frozen=True):
    """Delta export configuration values."""

    version: int | None = None
    timestamp: str | None = None


class DeltaConfig(StructBaseStrict, frozen=True):
    """Delta-specific configuration values."""

    restore: DeltaRestoreConfig | None = None
    export: DeltaExportConfig | None = None


class DocstringsPolicyConfig(StructBaseStrict, frozen=True):
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


class DocstringsConfig(StructBaseStrict, frozen=True):
    """Docstring-related configuration values."""

    policy: DocstringsPolicyConfig | None = None


class OtelConfig(StructBaseStrict, frozen=True):
    """OpenTelemetry configuration values."""

    enable_node_tracing: bool | None = None
    enable_plan_tracing: bool | None = None
    endpoint: str | None = None
    protocol: str | None = None
    sampler: str | None = None
    sampler_arg: float | int | None = None
    log_correlation: bool | None = None
    metric_export_interval_ms: int | None = None
    metric_export_timeout_ms: int | None = None
    bsp_schedule_delay_ms: int | None = None
    bsp_export_timeout_ms: int | None = None
    bsp_max_queue_size: int | None = None
    bsp_max_export_batch_size: int | None = None
    blrp_schedule_delay_ms: int | None = None
    blrp_export_timeout_ms: int | None = None
    blrp_max_queue_size: int | None = None
    blrp_max_export_batch_size: int | None = None


class HamiltonConfig(StructBaseStrict, frozen=True):
    """Hamilton-related configuration values."""

    enable_tracker: bool | None = None
    enable_type_checker: bool | None = None
    enable_node_diagnostics: bool | None = None
    graph_adapter_kind: str | None = None
    graph_adapter_options: dict[str, object] | None = None
    enable_structured_run_logs: bool | None = None
    structured_log_path: str | None = None
    run_log_path: str | None = None
    enable_graph_snapshot: bool | None = None
    graph_snapshot_path: str | None = None
    graph_snapshot_hamilton_path: str | None = None
    enable_cache_lineage: bool | None = None
    cache_lineage_path: str | None = None
    cache_path: str | None = None
    capture_data_statistics: bool | None = None
    max_list_length_capture: int | None = None
    max_dict_length_capture: int | None = None
    tags: dict[str, object] | None = None
    project_id: int | None = None
    username: str | None = None
    dag_name: str | None = None
    api_url: str | None = None
    ui_url: str | None = None
    telemetry_profile: str | None = None


class RootConfig(StructBaseStrict, frozen=True):
    """Root configuration payload for CodeAnatomy."""

    plan: PlanConfig | None = None
    cache: CacheConfig | None = None
    graph_adapter: GraphAdapterConfig | None = None
    incremental: IncrementalConfig | None = None
    delta: DeltaConfig | None = None
    docstrings: DocstringsConfig | None = None
    otel: OtelConfig | None = None
    hamilton: HamiltonConfig | None = None

    runtime_profile_name: str | None = None
    diagnostics_profile: str | None = None
    log_level: str | None = None
    determinism_override: str | None = None
    repo_root: str | None = None
    output_dir: str | None = None
    work_dir: str | None = None
    execution_mode: str | None = None

    runtime_environment: str | None = None
    runtime_team: str | None = None

    disable_scip: bool | None = None
    scip_output_dir: str | None = None


__all__ = [
    "CacheConfig",
    "DeltaConfig",
    "DeltaExportConfig",
    "DeltaRestoreConfig",
    "DocstringsConfig",
    "DocstringsPolicyConfig",
    "GraphAdapterConfig",
    "HamiltonConfig",
    "IncrementalConfig",
    "OtelConfig",
    "PlanConfig",
    "RootConfig",
]
