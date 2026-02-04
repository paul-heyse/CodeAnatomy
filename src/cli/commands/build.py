"""Build command implementation for CodeAnatomy CLI."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, cast

from cyclopts import Parameter, validators

from cli.context import RunContext
from cli.converters import resolve_determinism_alias
from cli.groups import (
    advanced_group,
    execution_group,
    graph_adapter_group,
    incremental_group,
    observability_group,
    output_group,
    repo_scope_group,
    scip_group,
)
from cli.kv_parser import parse_kv_pairs, parse_kv_pairs_json
from cli.path_utils import resolve_path
from hamilton_pipeline.types import (
    ExecutionMode,
    ExecutorConfig,
    GraphAdapterConfig,
    ScipIdentityOverrides,
    ScipIndexConfig,
)
from semantics.incremental import SemanticIncrementalConfig

if TYPE_CHECKING:
    from collections.abc import Mapping

    from core_types import DeterminismTier, JsonValue


@dataclass(frozen=True)
class _ScipPayload:
    """Structured SCIP payload extracted from config."""

    enabled: bool
    output_dir: str
    index_path_override: str | None
    env_json_path: str | None
    scip_python_bin: str
    scip_cli_bin: str
    target_only: str | None
    node_max_old_space_mb: int | None
    timeout_s: int | None
    extra_args: tuple[str, ...]
    generate_env_json: bool
    use_incremental_shards: bool
    shards_dir: str | None
    shards_manifest_path: str | None
    run_scip_print: bool
    scip_print_path: str | None
    run_scip_snapshot: bool
    scip_snapshot_dir: str | None
    scip_snapshot_comment_syntax: str
    run_scip_test: bool
    scip_test_args: tuple[str, ...]


@dataclass(frozen=True)
class _PlanOverrides:
    """Plan-level config overrides derived from CLI."""

    plan_allow_partial: bool | None = None
    plan_requested_tasks: tuple[str, ...] = ()
    plan_impacted_tasks: tuple[str, ...] = ()
    enable_metric_scheduling: bool | None = None
    enable_plan_diagnostics: bool | None = None
    enable_plan_task_submission_hook: bool | None = None
    enable_plan_task_grouping_hook: bool | None = None
    enforce_plan_task_submission: bool | None = None


_DEFAULT_SCIP_PYTHON = "scip-python"


DeterminismTierChoice = Literal[
    "tier2",
    "canonical",
    "tier1",
    "stable",
    "stable_set",
    "tier0",
    "fast",
    "best_effort",
]


@dataclass(frozen=True)
class BuildRequestOptions:
    """CLI options that map directly to GraphProductBuildRequest."""

    output_dir: Annotated[
        Path | None,
        Parameter(
            name=["--output-dir", "-o"],
            help="Directory for CPG outputs. Defaults to <repo_root>/build.",
            env_var="CODEANATOMY_OUTPUT_DIR",
            group=output_group,
        ),
    ] = None
    work_dir: Annotated[
        Path | None,
        Parameter(
            name="--work-dir",
            help="Working directory for intermediate artifacts.",
            env_var="CODEANATOMY_WORK_DIR",
            group=output_group,
        ),
    ] = None
    include_extract_errors: Annotated[
        bool,
        Parameter(
            name="--include-errors",
            help="Include extraction error artifacts.",
            group=output_group,
        ),
    ] = True
    include_manifest: Annotated[
        bool,
        Parameter(
            name="--include-manifest",
            help="Include run manifest in output.",
            group=output_group,
        ),
    ] = True
    include_run_bundle: Annotated[
        bool,
        Parameter(
            name="--include-run-bundle",
            help="Include run bundle directory.",
            group=output_group,
        ),
    ] = True
    execution_mode: Annotated[
        ExecutionMode,
        Parameter(
            name="--execution-mode",
            help=(
                "Pipeline execution strategy: deterministic_serial, plan_parallel, "
                "plan_parallel_remote."
            ),
            env_var="CODEANATOMY_EXECUTION_MODE",
            group=execution_group,
        ),
    ] = ExecutionMode.PLAN_PARALLEL
    determinism_tier: Annotated[
        DeterminismTierChoice | None,
        Parameter(
            name="--determinism-tier",
            help=(
                "Determinism level: canonical/tier2, stable_set/tier1/stable, "
                "best_effort/tier0/fast."
            ),
            env_var="CODEANATOMY_DETERMINISM_TIER",
            group=execution_group,
        ),
    ] = None
    runtime_profile_name: Annotated[
        str | None,
        Parameter(
            name="--runtime-profile",
            help="Named runtime profile from configuration.",
            env_var="CODEANATOMY_RUNTIME_PROFILE",
            group=execution_group,
        ),
    ] = None
    writer_strategy: Annotated[
        Literal["arrow", "datafusion"] | None,
        Parameter(
            name="--writer-strategy",
            help="Writer strategy for materialization.",
            group=execution_group,
        ),
    ] = None


@dataclass(frozen=True)
class BuildOptions:
    """CLI options for build command (non-request fields)."""

    executor_kind: Annotated[
        Literal["threadpool", "multiprocessing", "dask", "ray"] | None,
        Parameter(
            name="--executor-kind",
            help="Primary executor backend for parallel execution.",
            group=execution_group,
        ),
    ] = None
    executor_max_tasks: Annotated[
        int | None,
        Parameter(
            name="--executor-max-tasks",
            help="Maximum concurrent tasks for primary executor.",
            validator=validators.Number(gte=1),
            group=execution_group,
        ),
    ] = None
    executor_remote_kind: Annotated[
        Literal["threadpool", "multiprocessing", "dask", "ray"] | None,
        Parameter(
            name="--executor-remote-kind",
            help="Remote executor backend for scan/high-cost tasks.",
            group=execution_group,
        ),
    ] = None
    executor_remote_max_tasks: Annotated[
        int | None,
        Parameter(
            name="--executor-remote-max-tasks",
            help="Maximum concurrent tasks for remote executor.",
            validator=validators.Number(gte=1),
            group=execution_group,
        ),
    ] = None
    executor_cost_threshold: Annotated[
        float | None,
        Parameter(
            name="--executor-cost-threshold",
            help="Cost threshold for routing tasks to remote executor.",
            validator=validators.Number(gt=0.0),
            group=execution_group,
        ),
    ] = None
    disable_scip: Annotated[
        bool,
        Parameter(
            name="--disable-scip",
            help=(
                "Disable SCIP indexing (not recommended; produces less useful outputs). "
                "When enabled, SCIP-specific options are ignored and a warning is emitted."
            ),
            env_var="CODEANATOMY_DISABLE_SCIP",
            group=scip_group,
        ),
    ] = False
    scip_output_dir: Annotated[
        str | None,
        Parameter(
            name="--scip-output-dir",
            help=(
                "SCIP output directory (relative to repo root unless absolute). "
                "Default: build/scip."
            ),
            env_var="CODEANATOMY_SCIP_OUTPUT_DIR",
            group=scip_group,
        ),
    ] = None
    scip_index_path_override: Annotated[
        str | None,
        Parameter(
            name="--scip-index-path-override",
            help="Override the index.scip path.",
            group=scip_group,
        ),
    ] = None
    scip_python_bin: Annotated[
        str,
        Parameter(
            name="--scip-python-bin",
            help="scip-python executable to use.",
            group=scip_group,
        ),
    ] = _DEFAULT_SCIP_PYTHON
    scip_target_only: Annotated[
        str | None,
        Parameter(
            name="--scip-target-only",
            help="Optional target file/module to index.",
            group=scip_group,
        ),
    ] = None
    scip_timeout_s: Annotated[
        int | None,
        Parameter(
            name="--scip-timeout-s",
            help="Timeout in seconds for scip-python.",
            validator=validators.Number(gte=1),
            group=scip_group,
        ),
    ] = None
    scip_env_json: Annotated[
        str | None,
        Parameter(
            name="--scip-env-json",
            help="Path to SCIP environment JSON.",
            group=scip_group,
        ),
    ] = None
    node_max_old_space_mb: Annotated[
        int | None,
        Parameter(
            name="--node-max-old-space-mb",
            help="Node.js memory cap (MB) for scip-python.",
            validator=validators.Number(gte=256),
            group=scip_group,
        ),
    ] = None
    scip_extra_args: Annotated[
        tuple[str, ...],
        Parameter(
            name="--scip-extra-arg",
            help="Extra arguments passed to scip-python (repeatable).",
            group=scip_group,
        ),
    ] = ()
    scip_project_name: Annotated[
        str | None,
        Parameter(
            name="--scip-project-name",
            help="Override SCIP project name.",
            group=scip_group,
        ),
    ] = None
    scip_project_version: Annotated[
        str | None,
        Parameter(
            name="--scip-project-version",
            help="Override SCIP project version.",
            group=scip_group,
        ),
    ] = None
    scip_project_namespace: Annotated[
        str | None,
        Parameter(
            name="--scip-project-namespace",
            help="Override SCIP project namespace.",
            group=scip_group,
        ),
    ] = None
    incremental: Annotated[
        bool,
        Parameter(
            name="--incremental",
            help="Enable incremental processing using cached state.",
            group=incremental_group,
        ),
    ] = False
    incremental_state_dir: Annotated[
        Path | None,
        Parameter(
            name="--incremental-state-dir",
            help="Directory for incremental processing state (relative to repo_root).",
            env_var="CODEANATOMY_STATE_DIR",
            group=incremental_group,
        ),
    ] = None
    incremental_repo_id: Annotated[
        str | None,
        Parameter(
            name="--incremental-repo-id",
            help="Repository identifier for incremental state.",
            env_var="CODEANATOMY_REPO_ID",
            group=incremental_group,
        ),
    ] = None
    incremental_impact_strategy: Annotated[
        Literal["hybrid", "symbol_closure", "import_closure"] | None,
        Parameter(
            name="--incremental-impact-strategy",
            help="Strategy for computing incremental impact.",
            env_var="CODEANATOMY_INCREMENTAL_IMPACT_STRATEGY",
            group=incremental_group,
        ),
    ] = None
    git_base_ref: Annotated[
        str | None,
        Parameter(
            name="--git-base-ref",
            help="Git base ref for incremental diff.",
            env_var="CODEANATOMY_GIT_BASE_REF",
            group=incremental_group,
        ),
    ] = None
    git_head_ref: Annotated[
        str | None,
        Parameter(
            name="--git-head-ref",
            help="Git head ref for incremental diff.",
            env_var="CODEANATOMY_GIT_HEAD_REF",
            group=incremental_group,
        ),
    ] = None
    git_changed_only: Annotated[
        bool,
        Parameter(
            name="--git-changed-only",
            help="Process only changed files (no closure expansion).",
            env_var="CODEANATOMY_GIT_CHANGED_ONLY",
            group=incremental_group,
        ),
    ] = False
    include_globs: Annotated[
        tuple[str, ...],
        Parameter(
            name="--include-glob",
            help="Glob patterns for files to include (repeatable).",
            env_var="CODEANATOMY_INCLUDE_GLOBS",
            group=repo_scope_group,
        ),
    ] = ()
    exclude_globs: Annotated[
        tuple[str, ...],
        Parameter(
            name="--exclude-glob",
            help="Glob patterns for files to exclude (repeatable).",
            env_var="CODEANATOMY_EXCLUDE_GLOBS",
            group=repo_scope_group,
        ),
    ] = ()
    include_untracked: Annotated[
        bool,
        Parameter(
            name="--include-untracked",
            help="Include untracked (not git-ignored) files.",
            group=repo_scope_group,
        ),
    ] = True
    include_submodules: Annotated[
        bool,
        Parameter(
            name="--include-submodules",
            help="Include files from git submodules.",
            group=repo_scope_group,
        ),
    ] = False
    include_worktrees: Annotated[
        bool,
        Parameter(
            name="--include-worktrees",
            help="Include files from git worktrees.",
            group=repo_scope_group,
        ),
    ] = False
    follow_symlinks: Annotated[
        bool,
        Parameter(
            name="--follow-symlinks",
            help="Follow symbolic links when scanning.",
            group=repo_scope_group,
        ),
    ] = False
    external_interface_depth: Annotated[
        Literal["metadata", "full"],
        Parameter(
            name="--external-interface-depth",
            help="Depth for external interface extraction.",
            group=repo_scope_group,
        ),
    ] = "metadata"
    graph_adapter_kind: Annotated[
        Literal["threadpool", "dask", "ray"] | None,
        Parameter(
            name="--graph-adapter-kind",
            help="Hamilton graph adapter backend (non-dynamic execution).",
            group=graph_adapter_group,
        ),
    ] = None
    graph_adapter_option: Annotated[
        tuple[str, ...],
        Parameter(
            name="--graph-adapter-option",
            help="Graph adapter option key=value (repeatable).",
            group=graph_adapter_group,
        ),
    ] = ()
    graph_adapter_option_json: Annotated[
        tuple[str, ...],
        Parameter(
            name="--graph-adapter-option-json",
            help="Graph adapter option key=json (repeatable; JSON values).",
            group=graph_adapter_group,
        ),
    ] = ()
    enable_tree_sitter: Annotated[
        bool,
        Parameter(
            name="--enable-tree-sitter",
            help="Enable tree-sitter extraction for syntax nodes.",
            group=advanced_group,
        ),
    ] = True
    plan_allow_partial: Annotated[
        bool | None,
        Parameter(
            name="--plan-allow-partial",
            help="Allow partial execution plans (advanced).",
            group=advanced_group,
        ),
    ] = None
    plan_requested_task: Annotated[
        tuple[str, ...],
        Parameter(
            name="--plan-requested-task",
            help="Explicit task names to request in plan compilation (repeatable).",
            group=advanced_group,
        ),
    ] = ()
    plan_impacted_task: Annotated[
        tuple[str, ...],
        Parameter(
            name="--plan-impacted-task",
            help="Explicit task names to mark as impacted in plan compilation (repeatable).",
            group=advanced_group,
        ),
    ] = ()
    enable_metric_scheduling: Annotated[
        bool | None,
        Parameter(
            name="--enable-metric-scheduling",
            help="Enable metric-based scheduling in plan compilation (advanced).",
            group=advanced_group,
        ),
    ] = None
    enable_plan_diagnostics: Annotated[
        bool | None,
        Parameter(
            name="--enable-plan-diagnostics",
            help="Enable plan diagnostics emission (advanced).",
            group=advanced_group,
        ),
    ] = None
    enable_plan_task_submission_hook: Annotated[
        bool | None,
        Parameter(
            name="--enable-plan-task-submission-hook",
            help="Enable plan task submission hook (advanced).",
            group=advanced_group,
        ),
    ] = None
    enable_plan_task_grouping_hook: Annotated[
        bool | None,
        Parameter(
            name="--enable-plan-task-grouping-hook",
            help="Enable plan task grouping hook (advanced).",
            group=advanced_group,
        ),
    ] = None
    enforce_plan_task_submission: Annotated[
        bool | None,
        Parameter(
            name="--enforce-plan-task-submission",
            help="Enforce plan task submission policy (advanced).",
            group=advanced_group,
        ),
    ] = None
    enable_hamilton_tracker: Annotated[
        bool | None,
        Parameter(
            name="--enable-hamilton-tracker",
            help="Enable Hamilton UI tracker integration.",
            group=observability_group,
        ),
    ] = None
    hamilton_project_id: Annotated[
        int | None,
        Parameter(
            name="--hamilton-project-id",
            help="Hamilton tracker project id.",
            validator=validators.Number(gte=1),
            group=observability_group,
        ),
    ] = None
    hamilton_username: Annotated[
        str | None,
        Parameter(
            name="--hamilton-username",
            help="Hamilton tracker username.",
            group=observability_group,
        ),
    ] = None
    hamilton_dag_name: Annotated[
        str | None,
        Parameter(
            name="--hamilton-dag-name",
            help="Hamilton DAG name override.",
            group=observability_group,
        ),
    ] = None
    hamilton_api_url: Annotated[
        str | None,
        Parameter(
            name="--hamilton-api-url",
            help="Hamilton tracker API URL override.",
            group=observability_group,
        ),
    ] = None
    hamilton_ui_url: Annotated[
        str | None,
        Parameter(
            name="--hamilton-ui-url",
            help="Hamilton tracker UI URL override.",
            group=observability_group,
        ),
    ] = None
    hamilton_capture_data_statistics: Annotated[
        bool | None,
        Parameter(
            name="--hamilton-capture-data-statistics",
            help="Enable Hamilton data statistics capture.",
            group=observability_group,
        ),
    ] = None
    hamilton_max_list_length_capture: Annotated[
        int | None,
        Parameter(
            name="--hamilton-max-list-length-capture",
            help="Maximum list length to capture in Hamilton telemetry.",
            validator=validators.Number(gte=0),
            group=observability_group,
        ),
    ] = None
    hamilton_max_dict_length_capture: Annotated[
        int | None,
        Parameter(
            name="--hamilton-max-dict-length-capture",
            help="Maximum dict length to capture in Hamilton telemetry.",
            validator=validators.Number(gte=0),
            group=observability_group,
        ),
    ] = None


_DEFAULT_BUILD_REQUEST = BuildRequestOptions()
_DEFAULT_BUILD_OPTIONS = BuildOptions()


@dataclass(frozen=True)
class _CliConfigOverrides:
    runtime_profile_name: str | None
    determinism_override: DeterminismTier | None
    incremental: bool
    incremental_state_dir: Path | None
    incremental_repo_id: str | None
    incremental_impact_strategy: Literal["hybrid", "symbol_closure", "import_closure"] | None
    git_base_ref: str | None
    git_head_ref: str | None
    git_changed_only: bool
    enable_hamilton_tracker: bool | None
    hamilton_project_id: int | None
    hamilton_username: str | None
    hamilton_dag_name: str | None
    hamilton_api_url: str | None
    hamilton_ui_url: str | None
    hamilton_capture_data_statistics: bool | None
    hamilton_max_list_length_capture: int | None
    hamilton_max_dict_length_capture: int | None


@dataclass(frozen=True)
class _IncrementalOverrides:
    incremental: bool
    incremental_state_dir: Path | None
    incremental_repo_id: str | None
    incremental_impact_strategy: Literal["hybrid", "symbol_closure", "import_closure"] | None
    git_base_ref: str | None
    git_head_ref: str | None
    git_changed_only: bool


@dataclass(frozen=True)
class _ScipOverrides:
    disable_scip: bool
    scip_output_dir: str | None
    scip_index_path_override: str | None
    scip_env_json: str | None
    scip_python_bin: str
    scip_target_only: str | None
    scip_timeout_s: int | None
    node_max_old_space_mb: int | None
    scip_extra_args: tuple[str, ...]


def build_command(
    repo_root: Annotated[
        Path,
        Parameter(
            help="Root directory of the repository to analyze.",
            validator=validators.Path(exists=True, dir_okay=True, file_okay=False),
        ),
    ],
    request: Annotated[BuildRequestOptions, Parameter(name="*")] = _DEFAULT_BUILD_REQUEST,
    options: Annotated[BuildOptions, Parameter(name="*")] = _DEFAULT_BUILD_OPTIONS,
    *,
    run_context: Annotated[RunContext | None, Parameter(parse=False)] = None,
) -> int:
    """Build the Code Property Graph for a repository.

    Returns
    -------
    int
        Exit status code.
    """
    import logging

    from graph import GraphProductBuildRequest, build_graph_product

    logger = logging.getLogger("codeanatomy.pipeline")
    resolved_repo_root = repo_root.resolve()

    if run_context is None:
        from cli.config_loader import load_effective_config

        config_contents = load_effective_config(None)
    else:
        config_contents = dict(run_context.config_contents)
    config_contents["repo_root"] = str(resolved_repo_root)
    otel_options = run_context.otel_options if run_context else None

    resolved_tier = resolve_determinism_alias(request.determinism_tier)

    plan_overrides = _PlanOverrides(
        plan_allow_partial=options.plan_allow_partial,
        plan_requested_tasks=options.plan_requested_task,
        plan_impacted_tasks=options.plan_impacted_task,
        enable_metric_scheduling=options.enable_metric_scheduling,
        enable_plan_diagnostics=options.enable_plan_diagnostics,
        enable_plan_task_submission_hook=options.enable_plan_task_submission_hook,
        enable_plan_task_grouping_hook=options.enable_plan_task_grouping_hook,
        enforce_plan_task_submission=options.enforce_plan_task_submission,
    )

    config_contents = _apply_plan_overrides(config_contents, plan_overrides)
    cli_overrides = _CliConfigOverrides(
        runtime_profile_name=request.runtime_profile_name,
        determinism_override=resolved_tier,
        incremental=options.incremental,
        incremental_state_dir=options.incremental_state_dir,
        incremental_repo_id=options.incremental_repo_id,
        incremental_impact_strategy=options.incremental_impact_strategy,
        git_base_ref=options.git_base_ref,
        git_head_ref=options.git_head_ref,
        git_changed_only=options.git_changed_only,
        enable_hamilton_tracker=options.enable_hamilton_tracker,
        hamilton_project_id=options.hamilton_project_id,
        hamilton_username=options.hamilton_username,
        hamilton_dag_name=options.hamilton_dag_name,
        hamilton_api_url=options.hamilton_api_url,
        hamilton_ui_url=options.hamilton_ui_url,
        hamilton_capture_data_statistics=options.hamilton_capture_data_statistics,
        hamilton_max_list_length_capture=options.hamilton_max_list_length_capture,
        hamilton_max_dict_length_capture=options.hamilton_max_dict_length_capture,
    )
    config_contents = _apply_cli_config_overrides(
        config_contents,
        repo_root=resolved_repo_root,
        overrides=cli_overrides,
    )

    scip_config = _build_scip_config(
        config_contents,
        resolved_repo_root,
        overrides=_ScipOverrides(
            disable_scip=options.disable_scip,
            scip_output_dir=options.scip_output_dir,
            scip_index_path_override=options.scip_index_path_override,
            scip_env_json=options.scip_env_json,
            scip_python_bin=options.scip_python_bin,
            scip_target_only=options.scip_target_only,
            scip_timeout_s=options.scip_timeout_s,
            node_max_old_space_mb=options.node_max_old_space_mb,
            scip_extra_args=options.scip_extra_args,
        ),
    )

    scip_identity = None
    if options.scip_project_name or options.scip_project_version or options.scip_project_namespace:
        scip_identity = ScipIdentityOverrides(
            project_name_override=options.scip_project_name,
            project_version_override=options.scip_project_version,
            project_namespace_override=options.scip_project_namespace,
        )

    executor_config = _build_executor_config(
        executor_kind=options.executor_kind,
        executor_max_tasks=options.executor_max_tasks,
        executor_remote_kind=options.executor_remote_kind,
        executor_remote_max_tasks=options.executor_remote_max_tasks,
        executor_cost_threshold=options.executor_cost_threshold,
    )

    graph_adapter_config = _build_graph_adapter_config(
        graph_adapter_kind=options.graph_adapter_kind,
        graph_adapter_option=options.graph_adapter_option,
        graph_adapter_option_json=options.graph_adapter_option_json,
    )

    incremental_config = _build_incremental_config(
        resolved_repo_root,
        overrides=_IncrementalOverrides(
            incremental=options.incremental,
            incremental_state_dir=options.incremental_state_dir,
            incremental_repo_id=options.incremental_repo_id,
            incremental_impact_strategy=options.incremental_impact_strategy,
            git_base_ref=options.git_base_ref,
            git_head_ref=options.git_head_ref,
            git_changed_only=options.git_changed_only,
        ),
    )

    overrides: dict[str, object] = {}
    if run_context:
        overrides["run_id"] = run_context.run_id
    overrides.update(
        {
            "include_globs": list(options.include_globs),
            "exclude_globs": list(options.exclude_globs),
            "include_untracked": options.include_untracked,
            "include_submodules": options.include_submodules,
            "include_worktrees": options.include_worktrees,
            "follow_symlinks": options.follow_symlinks,
            "external_interface_depth": options.external_interface_depth,
            "enable_tree_sitter": options.enable_tree_sitter,
        }
    )

    build_request = GraphProductBuildRequest(
        repo_root=resolved_repo_root,
        output_dir=request.output_dir,
        work_dir=request.work_dir,
        execution_mode=request.execution_mode,
        executor_config=executor_config,
        graph_adapter_config=graph_adapter_config,
        scip_index_config=scip_config,
        scip_identity_overrides=scip_identity,
        runtime_profile_name=request.runtime_profile_name,
        determinism_override=resolved_tier,
        writer_strategy=request.writer_strategy,
        incremental_config=incremental_config,
        incremental_impact_strategy=options.incremental_impact_strategy,
        include_extract_errors=request.include_extract_errors,
        include_manifest=request.include_manifest,
        include_run_bundle=request.include_run_bundle,
        config=config_contents,
        overrides=overrides or None,
        otel_options=otel_options,
    )

    result = build_graph_product(build_request)

    logger.info(
        "Build complete. Output dir=%s bundle=%s",
        result.output_dir,
        result.run_bundle_dir,
    )
    logger.info("Pipeline outputs: %s", sorted(result.pipeline_outputs))

    return 0


def _apply_plan_overrides(
    config_contents: Mapping[str, JsonValue],
    overrides: _PlanOverrides,
) -> dict[str, JsonValue]:
    payload = dict(config_contents)
    if overrides.plan_allow_partial is not None:
        payload["plan_allow_partial"] = overrides.plan_allow_partial
    if overrides.plan_requested_tasks:
        payload["plan_requested_tasks"] = list(overrides.plan_requested_tasks)
    if overrides.plan_impacted_tasks:
        payload["plan_impacted_tasks"] = list(overrides.plan_impacted_tasks)
    if overrides.enable_metric_scheduling is not None:
        payload["enable_metric_scheduling"] = overrides.enable_metric_scheduling
    if overrides.enable_plan_diagnostics is not None:
        payload["enable_plan_diagnostics"] = overrides.enable_plan_diagnostics
    if overrides.enable_plan_task_submission_hook is not None:
        payload["enable_plan_task_submission_hook"] = overrides.enable_plan_task_submission_hook
    if overrides.enable_plan_task_grouping_hook is not None:
        payload["enable_plan_task_grouping_hook"] = overrides.enable_plan_task_grouping_hook
    if overrides.enforce_plan_task_submission is not None:
        payload["enforce_plan_task_submission"] = overrides.enforce_plan_task_submission
    return payload


def _apply_optional_value(
    payload: dict[str, JsonValue],
    *,
    key: str,
    value: JsonValue | None,
) -> None:
    if value is None:
        return
    payload[key] = value


def _apply_optional_str(
    payload: dict[str, JsonValue],
    *,
    key: str,
    value: str | None,
) -> None:
    if value:
        payload[key] = value


def _apply_cli_config_overrides(
    config_contents: Mapping[str, JsonValue],
    *,
    repo_root: Path,
    overrides: _CliConfigOverrides,
) -> dict[str, JsonValue]:
    payload = dict(config_contents)
    _apply_optional_str(
        payload,
        key="runtime_profile_name",
        value=overrides.runtime_profile_name,
    )
    determinism_value = (
        overrides.determinism_override.value if overrides.determinism_override is not None else None
    )
    _apply_optional_value(
        payload,
        key="determinism_override",
        value=determinism_value,
    )
    payload["incremental_enabled"] = overrides.incremental
    resolved_state_dir = resolve_path(repo_root, overrides.incremental_state_dir)
    state_dir_value = str(resolved_state_dir) if resolved_state_dir is not None else None
    _apply_optional_value(
        payload,
        key="incremental_state_dir",
        value=state_dir_value,
    )
    _apply_optional_str(
        payload,
        key="incremental_repo_id",
        value=overrides.incremental_repo_id,
    )
    _apply_optional_value(
        payload,
        key="incremental_impact_strategy",
        value=overrides.incremental_impact_strategy,
    )
    _apply_optional_str(
        payload,
        key="incremental_git_base_ref",
        value=overrides.git_base_ref,
    )
    _apply_optional_str(
        payload,
        key="incremental_git_head_ref",
        value=overrides.git_head_ref,
    )
    payload["incremental_git_changed_only"] = overrides.git_changed_only
    _apply_optional_value(
        payload,
        key="enable_hamilton_tracker",
        value=overrides.enable_hamilton_tracker,
    )
    _apply_optional_value(
        payload,
        key="hamilton_project_id",
        value=overrides.hamilton_project_id,
    )
    _apply_optional_str(
        payload,
        key="hamilton_username",
        value=overrides.hamilton_username,
    )
    _apply_optional_str(
        payload,
        key="hamilton_dag_name",
        value=overrides.hamilton_dag_name,
    )
    _apply_optional_str(
        payload,
        key="hamilton_api_url",
        value=overrides.hamilton_api_url,
    )
    _apply_optional_str(
        payload,
        key="hamilton_ui_url",
        value=overrides.hamilton_ui_url,
    )
    _apply_optional_value(
        payload,
        key="hamilton_capture_data_statistics",
        value=overrides.hamilton_capture_data_statistics,
    )
    _apply_optional_value(
        payload,
        key="hamilton_max_list_length_capture",
        value=overrides.hamilton_max_list_length_capture,
    )
    _apply_optional_value(
        payload,
        key="hamilton_max_dict_length_capture",
        value=overrides.hamilton_max_dict_length_capture,
    )
    return payload


def _build_executor_config(
    *,
    executor_kind: Literal["threadpool", "multiprocessing", "dask", "ray"] | None,
    executor_max_tasks: int | None,
    executor_remote_kind: Literal["threadpool", "multiprocessing", "dask", "ray"] | None,
    executor_remote_max_tasks: int | None,
    executor_cost_threshold: float | None,
) -> ExecutorConfig | None:
    if (
        executor_kind is None
        and executor_max_tasks is None
        and executor_remote_kind is None
        and executor_remote_max_tasks is None
        and executor_cost_threshold is None
    ):
        return None
    return ExecutorConfig(
        kind=executor_kind or "multiprocessing",
        max_tasks=executor_max_tasks or 4,
        remote_kind=executor_remote_kind,
        remote_max_tasks=executor_remote_max_tasks,
        cost_threshold=executor_cost_threshold,
    )


def _build_graph_adapter_config(
    *,
    graph_adapter_kind: Literal["threadpool", "dask", "ray"] | None,
    graph_adapter_option: tuple[str, ...],
    graph_adapter_option_json: tuple[str, ...],
) -> GraphAdapterConfig | None:
    if graph_adapter_kind is None:
        return None
    adapter_options: dict[str, JsonValue] = (
        {
            key: cast("JsonValue", value)
            for key, value in parse_kv_pairs(graph_adapter_option).items()
        }
        if graph_adapter_option
        else {}
    )
    adapter_options_json: dict[str, JsonValue] = (
        dict(parse_kv_pairs_json(graph_adapter_option_json)) if graph_adapter_option_json else {}
    )
    combined: dict[str, JsonValue] = {**adapter_options, **adapter_options_json}
    return GraphAdapterConfig(
        kind=graph_adapter_kind,
        options=combined or None,
    )


def _build_incremental_config(
    repo_root: Path,
    *,
    overrides: _IncrementalOverrides,
) -> SemanticIncrementalConfig | None:
    if not overrides.incremental:
        return None
    resolved_state_dir = resolve_path(repo_root, overrides.incremental_state_dir)
    return SemanticIncrementalConfig(
        enabled=True,
        state_dir=resolved_state_dir,
        repo_id=overrides.incremental_repo_id,
        impact_strategy=overrides.incremental_impact_strategy or "hybrid",
        git_base_ref=overrides.git_base_ref,
        git_head_ref=overrides.git_head_ref,
        git_changed_only=overrides.git_changed_only,
    )


def _build_scip_config(
    config_contents: Mapping[str, JsonValue],
    repo_root: Path,
    *,
    overrides: _ScipOverrides,
) -> ScipIndexConfig:
    payload = _scip_payload_from_config(config_contents)
    resolved_output = (
        str(resolve_path(repo_root, overrides.scip_output_dir))
        if overrides.scip_output_dir is not None
        else None
    )
    resolved_index = (
        str(resolve_path(repo_root, overrides.scip_index_path_override))
        if overrides.scip_index_path_override is not None
        else None
    )
    resolved_env = (
        str(resolve_path(repo_root, overrides.scip_env_json))
        if overrides.scip_env_json is not None
        else None
    )

    enabled = payload.enabled
    if overrides.disable_scip:
        enabled = False

    output_dir = resolved_output or payload.output_dir
    scip_python = payload.scip_python_bin
    if overrides.scip_python_bin != _DEFAULT_SCIP_PYTHON:
        scip_python = overrides.scip_python_bin

    return ScipIndexConfig(
        enabled=enabled,
        output_dir=output_dir,
        index_path_override=resolved_index or payload.index_path_override,
        env_json_path=resolved_env or payload.env_json_path,
        scip_python_bin=scip_python,
        scip_cli_bin=payload.scip_cli_bin,
        target_only=overrides.scip_target_only or payload.target_only,
        node_max_old_space_mb=(
            overrides.node_max_old_space_mb
            if overrides.node_max_old_space_mb is not None
            else payload.node_max_old_space_mb
        ),
        timeout_s=overrides.scip_timeout_s
        if overrides.scip_timeout_s is not None
        else payload.timeout_s,
        extra_args=overrides.scip_extra_args or payload.extra_args,
        generate_env_json=payload.generate_env_json,
        use_incremental_shards=payload.use_incremental_shards,
        shards_dir=payload.shards_dir,
        shards_manifest_path=payload.shards_manifest_path,
        run_scip_print=payload.run_scip_print,
        scip_print_path=payload.scip_print_path,
        run_scip_snapshot=payload.run_scip_snapshot,
        scip_snapshot_dir=payload.scip_snapshot_dir,
        scip_snapshot_comment_syntax=payload.scip_snapshot_comment_syntax,
        run_scip_test=payload.run_scip_test,
        scip_test_args=payload.scip_test_args,
    )


def _scip_payload_from_config(config_contents: Mapping[str, JsonValue]) -> _ScipPayload:
    scip_config = config_contents.get("scip")
    payload: dict[str, JsonValue] = {}
    if isinstance(scip_config, dict):
        payload = {str(key): value for key, value in scip_config.items()}
    defaults = ScipIndexConfig()
    extra_args = _tuple_from_payload(payload.get("extra_args")) or defaults.extra_args
    scip_test_args = _tuple_from_payload(payload.get("scip_test_args")) or defaults.scip_test_args
    node_max_old_space_mb = _optional_int(payload.get("node_max_old_space_mb"))
    if node_max_old_space_mb is None:
        node_max_old_space_mb = defaults.node_max_old_space_mb
    timeout_s = _optional_int(payload.get("timeout_s"))
    if timeout_s is None:
        timeout_s = defaults.timeout_s
    return _ScipPayload(
        enabled=_bool_from_payload(payload.get("enabled"), default=defaults.enabled),
        output_dir=_str_from_payload(payload.get("output_dir"), default=defaults.output_dir),
        index_path_override=_optional_str(payload.get("index_path_override")),
        env_json_path=_optional_str(payload.get("env_json_path")),
        scip_python_bin=_str_from_payload(
            payload.get("scip_python_bin"),
            default=defaults.scip_python_bin,
        ),
        scip_cli_bin=_str_from_payload(payload.get("scip_cli_bin"), default=defaults.scip_cli_bin),
        target_only=_optional_str(payload.get("target_only")) or defaults.target_only,
        node_max_old_space_mb=node_max_old_space_mb,
        timeout_s=timeout_s,
        extra_args=extra_args,
        generate_env_json=_bool_from_payload(
            payload.get("generate_env_json"),
            default=defaults.generate_env_json,
        ),
        use_incremental_shards=_bool_from_payload(
            payload.get("use_incremental_shards"),
            default=defaults.use_incremental_shards,
        ),
        shards_dir=_optional_str(payload.get("shards_dir")) or defaults.shards_dir,
        shards_manifest_path=(
            _optional_str(payload.get("shards_manifest_path")) or defaults.shards_manifest_path
        ),
        run_scip_print=_bool_from_payload(
            payload.get("run_scip_print"),
            default=defaults.run_scip_print,
        ),
        scip_print_path=_optional_str(payload.get("scip_print_path")) or defaults.scip_print_path,
        run_scip_snapshot=_bool_from_payload(
            payload.get("run_scip_snapshot"),
            default=defaults.run_scip_snapshot,
        ),
        scip_snapshot_dir=(
            _optional_str(payload.get("scip_snapshot_dir")) or defaults.scip_snapshot_dir
        ),
        scip_snapshot_comment_syntax=_str_from_payload(
            payload.get("scip_snapshot_comment_syntax"),
            default=defaults.scip_snapshot_comment_syntax,
        ),
        run_scip_test=_bool_from_payload(
            payload.get("run_scip_test"),
            default=defaults.run_scip_test,
        ),
        scip_test_args=scip_test_args,
    )


def _bool_from_payload(value: JsonValue | None, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    return default


def _optional_int(value: JsonValue | None) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _str_from_payload(value: JsonValue | None, *, default: str) -> str:
    if isinstance(value, str) and value:
        return value
    return default


def _optional_str(value: JsonValue | None) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _tuple_from_payload(value: JsonValue | None) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, tuple):
        return tuple(str(item) for item in value)
    if isinstance(value, list):
        return tuple(str(item) for item in value)
    return ()


__all__ = ["build_command"]
