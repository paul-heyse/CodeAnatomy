"""Runtime validation models for root CLI configuration."""

from __future__ import annotations

from pydantic import ConfigDict, Field

from hamilton_pipeline.types import GraphAdapterConfig
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


class HamiltonConfigRuntime(RuntimeBase):
    """Validated Hamilton config."""

    enable_tracker: bool | None = None
    enable_type_checker: bool | None = None
    enable_node_diagnostics: bool | None = None
    enable_structured_run_logs: bool | None = None
    structured_log_path: str | None = None
    run_log_path: str | None = None
    enable_graph_snapshot: bool | None = None
    graph_snapshot_path: str | None = None
    graph_snapshot_hamilton_path: str | None = None
    enable_cache_lineage: bool | None = None
    cache_lineage_path: str | None = None
    capture_data_statistics: bool | None = None
    max_list_length_capture: int | None = Field(default=None, ge=0)
    max_dict_length_capture: int | None = Field(default=None, ge=0)
    tags: dict[str, object] | None = None
    project_id: int | None = None
    username: str | None = None
    dag_name: str | None = None
    api_url: str | None = None
    ui_url: str | None = None
    telemetry_profile: str | None = None


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
    graph_adapter: GraphAdapterConfig | None = None
    incremental: IncrementalConfigRuntime | None = None
    delta: DeltaConfigRuntime | None = None
    docstrings: DocstringsConfigRuntime | None = None
    otel: OtelConfigRuntime | None = None
    hamilton: HamiltonConfigRuntime | None = None


__all__ = [
    "CacheConfigRuntime",
    "DeltaConfigRuntime",
    "DeltaExportConfigRuntime",
    "DeltaRestoreConfigRuntime",
    "DocstringsConfigRuntime",
    "DocstringsPolicyConfigRuntime",
    "HamiltonConfigRuntime",
    "IncrementalConfigRuntime",
    "PlanConfigRuntime",
    "RootConfigRuntime",
]
