"""Typed configuration models for CodeAnatomy."""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec

from serde_msgspec import StructBaseStrict

if TYPE_CHECKING:
    from core_types import JsonValue


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


class GraphAdapterConfig(StructBaseStrict, frozen=True):
    """Graph adapter configuration values."""

    kind: str | None = None
    options: dict[str, JsonValue] | None = None


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


class RootConfig(StructBaseStrict, frozen=True):
    """Root configuration payload for CodeAnatomy."""

    plan: PlanConfig | None = None
    cache: CacheConfig | None = None
    graph_adapter: GraphAdapterConfig | None = None
    incremental: IncrementalConfig | None = None
    delta: DeltaConfig | None = None

    plan_allow_partial: bool | None = None
    plan_requested_tasks: tuple[str, ...] | None = None
    plan_impacted_tasks: tuple[str, ...] | None = None
    enable_metric_scheduling: bool | None = None
    enable_plan_diagnostics: bool | None = None
    enable_plan_task_submission_hook: bool | None = None
    enable_plan_task_grouping_hook: bool | None = None
    enforce_plan_task_submission: bool | None = None

    cache_policy_profile: str | None = None
    cache_path: str | None = None
    cache_log_to_file: bool | None = None
    cache_opt_in: bool | None = None

    graph_adapter_kind: str | None = None
    graph_adapter_options: dict[str, JsonValue] | None = None
    hamilton_graph_adapter_kind: str | None = None
    hamilton_graph_adapter_options: dict[str, JsonValue] | None = None

    incremental_enabled: bool | None = None
    incremental_state_dir: str | None = None
    incremental_repo_id: str | None = None
    incremental_impact_strategy: str | None = None
    incremental_git_base_ref: str | None = None
    incremental_git_head_ref: str | None = None
    incremental_git_changed_only: bool | None = None

    runtime_profile_name: str | None = None
    log_level: str | None = None
    determinism_override: str | None = None
    repo_root: str | None = None
    output_dir: str | None = None
    work_dir: str | None = None
    execution_mode: str | None = None

    enable_hamilton_tracker: bool | None = None
    enable_hamilton_type_checker: bool | None = None
    enable_hamilton_node_diagnostics: bool | None = None
    enable_otel_node_tracing: bool | None = None
    enable_otel_plan_tracing: bool | None = None
    enable_structured_run_logs: bool | None = None
    structured_log_path: str | None = None
    hamilton_run_log_path: str | None = None

    enable_graph_snapshot: bool | None = None
    graph_snapshot_path: str | None = None
    hamilton_graph_snapshot_path: str | None = None

    enable_cache_lineage: bool | None = None
    cache_lineage_path: str | None = None

    hamilton_cache_path: str | None = None
    hamilton_capture_data_statistics: bool | None = None
    hamilton_max_list_length_capture: int | None = None
    hamilton_max_dict_length_capture: int | None = None
    hamilton_tags: dict[str, JsonValue] | None = None
    hamilton_project_id: int | None = None
    hamilton_username: str | None = None
    hamilton_dag_name: str | None = None
    hamilton_api_url: str | None = None
    hamilton_ui_url: str | None = None
    hamilton_telemetry_profile: str | None = None

    runtime_environment: str | None = None
    runtime_team: str | None = None

    disable_scip: bool | None = None
    scip_output_dir: str | None = None


class RootConfigPatch(StructBaseStrict, frozen=True):
    """Patch payload for root configuration overrides."""

    output_dir: str | msgspec.UnsetType = msgspec.UNSET
    work_dir: str | msgspec.UnsetType = msgspec.UNSET
    execution_mode: str | msgspec.UnsetType = msgspec.UNSET
    runtime_profile_name: str | msgspec.UnsetType = msgspec.UNSET
    log_level: str | msgspec.UnsetType = msgspec.UNSET
    determinism_override: str | msgspec.UnsetType = msgspec.UNSET
    incremental_state_dir: str | msgspec.UnsetType = msgspec.UNSET
    incremental_repo_id: str | msgspec.UnsetType = msgspec.UNSET
    incremental_impact_strategy: str | msgspec.UnsetType = msgspec.UNSET
    incremental_git_base_ref: str | msgspec.UnsetType = msgspec.UNSET
    incremental_git_head_ref: str | msgspec.UnsetType = msgspec.UNSET
    incremental_git_changed_only: bool | msgspec.UnsetType = msgspec.UNSET
    disable_scip: bool | msgspec.UnsetType = msgspec.UNSET
    scip_output_dir: str | msgspec.UnsetType = msgspec.UNSET


__all__ = [
    "CacheConfig",
    "DeltaConfig",
    "DeltaExportConfig",
    "DeltaRestoreConfig",
    "GraphAdapterConfig",
    "IncrementalConfig",
    "PlanConfig",
    "RootConfig",
    "RootConfigPatch",
]
