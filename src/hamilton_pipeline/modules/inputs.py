"""Hamilton input nodes for pipeline configuration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal, TypedDict, Unpack

from hamilton.function_modifiers import cache, tag

from core_types import DeterminismTier, JsonDict
from datafusion_engine.runtime import AdapterExecutionPolicy
from engine.plan_policy import ExecutionSurfacePolicy, WriterStrategy
from engine.runtime_profile import RuntimeProfileSpec, resolve_runtime_profile
from engine.session import EngineSession
from engine.session_factory import build_engine_session
from extract.repo_scan import default_repo_scan_options
from extract.scip_extract import ScipExtractOptions, SCIPParseOptions
from hamilton_pipeline.lifecycle import get_hamilton_diagnostics_collector
from hamilton_pipeline.pipeline_types import (
    CacheRuntimeContext,
    OutputConfig,
    OutputStoragePolicy,
    RelspecConfig,
    RepoScanConfig,
    RepoScopeConfig,
    ScipIdentityOverrides,
    ScipIndexConfig,
    TreeSitterConfig,
)
from incremental.types import IncrementalConfig
from obs.diagnostics import DiagnosticsCollector
from relspec.pipeline_policy import PipelinePolicy
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy
from storage.ipc_utils import IpcWriteConfig
from utils.env_utils import env_bool, env_value


def _incremental_pipeline_enabled(config: IncrementalConfig | None = None) -> bool:
    if config is not None:
        return bool(config.enabled)
    mode = (env_value("CODEANATOMY_PIPELINE_MODE") or "").lower()
    return mode in {"incremental", "streaming"}


def _determinism_from_str(value: str) -> DeterminismTier | None:
    mapping: dict[str, DeterminismTier] = {
        "tier2": DeterminismTier.CANONICAL,
        "canonical": DeterminismTier.CANONICAL,
        "tier1": DeterminismTier.STABLE_SET,
        "stable": DeterminismTier.STABLE_SET,
        "stable_set": DeterminismTier.STABLE_SET,
        "tier0": DeterminismTier.BEST_EFFORT,
        "fast": DeterminismTier.BEST_EFFORT,
        "best_effort": DeterminismTier.BEST_EFFORT,
    }
    return mapping.get(value.strip().lower())


def _cache_path_from_inputs(cache_path: str | None) -> str | None:
    if isinstance(cache_path, str) and cache_path.strip():
        return cache_path.strip()
    for name in ("CODEANATOMY_HAMILTON_CACHE_PATH", "HAMILTON_CACHE_PATH"):
        value = env_value(name) or ""
        if value:
            return value
    return None


def _cache_policy_profile_from_inputs(cache_policy_profile: str | None) -> str | None:
    if isinstance(cache_policy_profile, str) and cache_policy_profile.strip():
        return cache_policy_profile.strip()
    for name in ("CODEANATOMY_CACHE_POLICY_PROFILE", "HAMILTON_CACHE_POLICY_PROFILE"):
        value = env_value(name) or ""
        if value:
            return value
    return None


@tag(layer="inputs", kind="runtime")
def runtime_profile_name(runtime_profile_name_override: str | None = None) -> str:
    """Return the runtime profile name for execution.

    Returns
    -------
    str
        Runtime profile name (defaults to "default").
    """
    if isinstance(runtime_profile_name_override, str) and runtime_profile_name_override.strip():
        return runtime_profile_name_override.strip()
    return env_value("CODEANATOMY_RUNTIME_PROFILE") or "default"


@tag(layer="inputs", kind="runtime")
def determinism_override(
    determinism_override_override: DeterminismTier | str | None = None,
) -> DeterminismTier | None:
    """Return an optional determinism tier override.

    Returns
    -------
    DeterminismTier | None
        Override tier when requested, otherwise ``None``.
    """
    override = determinism_override_override
    if isinstance(override, DeterminismTier):
        return override
    if isinstance(override, str):
        resolved = _determinism_from_str(override)
        if resolved is not None:
            return resolved
    force_flag = env_bool("CODEANATOMY_FORCE_TIER2", default=False, on_invalid="false")
    if force_flag:
        return DeterminismTier.CANONICAL
    tier = env_value("CODEANATOMY_DETERMINISM_TIER")
    return _determinism_from_str(tier or "")


@cache(behavior="ignore")
@tag(layer="inputs", kind="runtime")
def runtime_profile_spec(
    runtime_profile_name: str,
    determinism_override: DeterminismTier | None,
    output_config: OutputConfig,
) -> RuntimeProfileSpec:
    """Return a resolved runtime profile spec.

    Returns
    -------
    RuntimeProfileSpec
        Runtime profile spec with determinism overrides applied.
    """
    resolved = resolve_runtime_profile(runtime_profile_name, determinism=determinism_override)
    normalize_root = _normalize_output_root(output_config)
    if normalize_root is None:
        return resolved
    updated_profile = replace(resolved.datafusion, normalize_output_root=normalize_root)
    return replace(resolved, datafusion=updated_profile)


def _normalize_output_root(output_config: OutputConfig) -> str | None:
    base_dir = output_config.output_dir or output_config.work_dir
    if not base_dir:
        return None
    return str(Path(base_dir) / "normalize")


@cache(behavior="ignore")
@tag(layer="inputs", kind="runtime")
def diagnostics_collector() -> DiagnosticsCollector:
    """Return a diagnostics collector for the run.

    Returns
    -------
    DiagnosticsCollector
        Collector for diagnostics events and artifacts.
    """
    collector = get_hamilton_diagnostics_collector()
    return collector if collector is not None else DiagnosticsCollector()


@cache(behavior="ignore")
@tag(layer="inputs", kind="runtime")
def engine_session(
    runtime_profile_spec: RuntimeProfileSpec,
    diagnostics_collector: DiagnosticsCollector,
    execution_surface_policy: ExecutionSurfacePolicy,
    pipeline_policy: PipelinePolicy,
) -> EngineSession:
    """Build an engine session for downstream execution surfaces.

    Returns
    -------
    EngineSession
        Session containing runtime surfaces and diagnostics wiring.
    """
    return build_engine_session(
        runtime_spec=runtime_profile_spec,
        diagnostics=diagnostics_collector,
        surface_policy=execution_surface_policy,
        diagnostics_policy=pipeline_policy.diagnostics,
    )


@cache(behavior="ignore")
@tag(layer="inputs", kind="runtime")
def streaming_table_provider(incremental_config: IncrementalConfig) -> object | None:
    """Return an optional streaming table provider (placeholder).

    This hook is reserved for Rust-backed StreamingTable providers. It is
    restricted to incremental/streaming pipeline runs only.

    Returns
    -------
    object | None
        Provider instance or None when disabled.
    """
    if not _incremental_pipeline_enabled(incremental_config):
        return None
    flag = env_bool("CODEANATOMY_ENABLE_STREAMING_TABLES", default=False, on_invalid="false")
    if not flag:
        return None
    return None


@tag(layer="inputs", kind="runtime")
def adapter_execution_policy() -> AdapterExecutionPolicy:
    """Return the execution policy for adapter execution behavior.

    Returns
    -------
    AdapterExecutionPolicy
        Adapter execution policy configuration.
    """
    return AdapterExecutionPolicy()


@tag(layer="inputs", kind="object")
def relspec_param_values() -> JsonDict:
    """Return parameter values for relspec execution.

    Returns
    -------
    JsonDict
        Mapping of parameter names to values. Keys may be global ("threshold"),
        task-scoped ("task_name.threshold"), output-scoped ("output.threshold"),
        or nested per-task mappings ({"task_name": {"threshold": 0.5}}).
    """
    return {}


@tag(layer="inputs", kind="object")
def pipeline_policy() -> PipelinePolicy:
    """Return the pipeline policy for rule execution.

    Returns
    -------
    PipelinePolicy
        Policy bundle for rule execution and diagnostics.
    """
    return PipelinePolicy()


@tag(layer="inputs", kind="object")
def param_table_delta_paths() -> Mapping[str, str] | None:
    """Return optional Delta table paths for param table replay.

    Returns
    -------
    Mapping[str, str] | None
        Mapping of logical param names to Delta table paths.
    """
    return {}


@tag(layer="inputs", kind="scalar")
def python_extensions() -> list[str]:
    """Return explicit Python extensions for repo scope rules.

    Override via execute(overrides={"python_extensions": [...]}).

    Returns
    -------
    list[str]
        Explicit Python extensions to add to discovery.
    """
    return []


@tag(layer="inputs", kind="scalar")
def include_untracked() -> bool:
    """Return whether to include untracked files in repo scope.

    Returns
    -------
    bool
        True to include untracked files.
    """
    return True


@tag(layer="inputs", kind="scalar")
def include_submodules() -> bool:
    """Return whether to include submodules in repo scope.

    Returns
    -------
    bool
        True to include submodules.
    """
    return False


@tag(layer="inputs", kind="scalar")
def include_worktrees() -> bool:
    """Return whether to include worktrees in repo scope.

    Returns
    -------
    bool
        True to include worktrees.
    """
    return False


@tag(layer="inputs", kind="scalar")
def follow_symlinks() -> bool:
    """Return whether to follow symlinks in repo scope.

    Returns
    -------
    bool
        True to follow symlinks.
    """
    return False


@tag(layer="inputs", kind="scalar")
def external_interface_depth() -> Literal["metadata", "full"]:
    """Return the external interface extraction depth.

    Returns
    -------
    str
        Depth policy for external interface extraction.
    """
    return "metadata"


@tag(layer="inputs", kind="scalar")
def max_files() -> int:
    """Return the default maximum number of files to scan.

    Returns
    -------
    int
        Maximum file count for repository scanning.
    """
    max_files_opt = default_repo_scan_options().max_files
    if max_files_opt is None:
        return 200_000
    return int(max_files_opt)


@tag(layer="inputs", kind="scalar")
def cache_salt() -> str:
    """Return a manual cache-busting salt for repo-dependent nodes.

    Returns
    -------
    str
        Cache salt string used to invalidate cached outputs when changed.
    """
    return ""


@tag(layer="inputs", kind="scalar")
def output_dir() -> str | None:
    """Return the default output directory for artifacts.

    Override to materialize artifacts.

    Returns
    -------
    str | None
        Output directory path, or None to disable.
    """
    return None


@tag(layer="inputs", kind="scalar")
def work_dir() -> str | None:
    """Return the default working directory for intermediates.

    Override if you want all intermediate datasets written somewhere specific.

    Returns
    -------
    str | None
        Working directory path, or None for automatic selection.
    """
    return None


@tag(layer="inputs", kind="scalar")
def scip_identity_overrides() -> ScipIdentityOverrides:
    """Return default overrides for SCIP identity.

    Override via execute(overrides={"scip_identity_overrides": ScipIdentityOverrides(...)}).

    Returns
    -------
    ScipIdentityOverrides
        Identity overrides for SCIP project metadata.
    """
    return ScipIdentityOverrides(
        project_name_override=None,
        project_version_override=None,
        project_namespace_override=None,
    )


@tag(layer="inputs", kind="scalar")
def scip_index_config() -> ScipIndexConfig:
    """Return default config for scip-python indexing.

    Override via execute(overrides={"scip_index_config": ScipIndexConfig(...)}).

    Returns
    -------
    ScipIndexConfig
        Indexing configuration.
    """
    return ScipIndexConfig()


@tag(layer="inputs", kind="scalar")
def scip_parse_options() -> SCIPParseOptions:
    """Return default parse options for SCIP extraction.

    Override via execute(overrides={"scip_parse_options": SCIPParseOptions(...)}).

    Returns
    -------
    SCIPParseOptions
        Parsing options for SCIP index decoding.
    """
    return SCIPParseOptions()


@tag(layer="inputs", kind="scalar")
def scip_extract_options() -> ScipExtractOptions:
    """Return default extraction options for SCIP tables.

    Override via execute(overrides={"scip_extract_options": ScipExtractOptions(...)}).

    Returns
    -------
    ScipExtractOptions
        Extract options for SCIP table materialization.
    """
    return ScipExtractOptions()


@tag(layer="inputs", kind="scalar")
def enable_tree_sitter() -> bool:
    """Return whether tree-sitter extraction is enabled.

    Returns
    -------
    bool
        True to enable tree-sitter extraction.
    """
    return False


@tag(layer="inputs", kind="scalar")
def relspec_mode() -> Literal["memory", "filesystem"]:
    """Return the relationship spec mode.

    Override via execute(overrides={"relspec_mode": "filesystem"}).

    Returns
    -------
    str
        Relationship spec mode ("memory" or "filesystem").
    """
    return "memory"


@tag(layer="inputs", kind="scalar")
def overwrite_intermediate_datasets() -> bool:
    """Return whether to overwrite intermediate datasets on disk.

    Returns
    -------
    bool
        True to delete and rewrite intermediate datasets.
    """
    return True


@dataclass(frozen=True)
class RepoScopeGlobs:
    """Bundle include/exclude globs for repo scope."""

    include_globs: tuple[str, ...]
    exclude_globs: tuple[str, ...]


@dataclass(frozen=True)
class RepoScopeFlags:
    """Bundle boolean flags for repo scope."""

    include_untracked: bool
    include_submodules: bool
    include_worktrees: bool
    follow_symlinks: bool


@tag(layer="inputs", kind="object")
def repo_scope_globs() -> RepoScopeGlobs:
    """Bundle repository scope globs.

    Returns
    -------
    RepoScopeGlobs
        Include/exclude glob bundle.
    """
    return RepoScopeGlobs(include_globs=(), exclude_globs=())


@tag(layer="inputs", kind="object")
def repo_scope_flags(
    *,
    include_untracked: bool,
    include_submodules: bool,
    include_worktrees: bool,
    follow_symlinks: bool,
) -> RepoScopeFlags:
    """Bundle repository scope flags.

    Returns
    -------
    RepoScopeFlags
        Boolean scope flags.
    """
    return RepoScopeFlags(
        include_untracked=include_untracked,
        include_submodules=include_submodules,
        include_worktrees=include_worktrees,
        follow_symlinks=follow_symlinks,
    )


@tag(layer="inputs", kind="object")
def repo_scope_config(
    python_extensions: list[str],
    repo_scope_globs: RepoScopeGlobs,
    repo_scope_flags: RepoScopeFlags,
    external_interface_depth: Literal["metadata", "full"],
) -> RepoScopeConfig:
    """Bundle repository scope configuration.

    Returns
    -------
    RepoScopeConfig
        Repository scope configuration bundle.
    """
    return RepoScopeConfig(
        include_untracked=repo_scope_flags.include_untracked,
        python_extensions=tuple(python_extensions),
        include_globs=repo_scope_globs.include_globs,
        exclude_globs=repo_scope_globs.exclude_globs,
        include_submodules=repo_scope_flags.include_submodules,
        include_worktrees=repo_scope_flags.include_worktrees,
        follow_symlinks=repo_scope_flags.follow_symlinks,
        external_interface_depth=external_interface_depth,
    )


@tag(layer="inputs", kind="object")
def repo_scan_config(
    repo_root: str,
    repo_scope_config: RepoScopeConfig,
    max_files: int,
    incremental_config: IncrementalConfig | None = None,
) -> RepoScanConfig:
    """Bundle repository scan configuration.

    Returns
    -------
    RepoScanConfig
        Repository scan configuration bundle.
    """
    diff_base_ref = None
    diff_head_ref = None
    changed_only = False
    if incremental_config is not None:
        diff_base_ref = incremental_config.git_base_ref
        diff_head_ref = incremental_config.git_head_ref
        changed_only = incremental_config.git_changed_only
    if not diff_base_ref or not diff_head_ref:
        diff_base_ref = None
        diff_head_ref = None
        changed_only = False
    return RepoScanConfig(
        repo_root=repo_root,
        scope_config=repo_scope_config,
        max_files=int(max_files),
        diff_base_ref=diff_base_ref,
        diff_head_ref=diff_head_ref,
        changed_only=changed_only,
    )


@tag(layer="inputs", kind="object")
def relspec_config(
    relspec_mode: Literal["memory", "filesystem"],
    scip_index_path: str | None,
) -> RelspecConfig:
    """Bundle relationship-spec configuration.

    Returns
    -------
    RelspecConfig
        Relationship-spec configuration bundle.
    """
    return RelspecConfig(relspec_mode=relspec_mode, scip_index_path=scip_index_path)


@tag(layer="inputs", kind="object")
def output_config_overrides(
    *,
    overwrite_intermediate_datasets: bool,
    materialize_param_tables: bool = False,
    writer_strategy: WriterStrategy = "arrow",
    options: OutputConfigOverrideOptions | None = None,
) -> OutputConfigOverrides:
    """Bundle output configuration overrides.

    Returns
    -------
    OutputConfigOverrides
        Output configuration overrides bundle.
    """
    resolved = options or OutputConfigOverrideOptions()
    return OutputConfigOverrides(
        overwrite_intermediate_datasets=overwrite_intermediate_datasets,
        materialize_param_tables=materialize_param_tables,
        writer_strategy=writer_strategy,
        ipc=resolved.ipc,
        delta=resolved.delta,
        output_storage_policy=resolved.output_storage_policy,
    )


@dataclass(frozen=True)
class IpcConfigOverrides:
    """Overrides for IPC output configuration."""

    dump_enabled: bool = False
    write_config: IpcWriteConfig | None = None


@dataclass(frozen=True)
class DeltaOutputOverrides:
    """Overrides for Delta output configuration."""

    write_policy: DeltaWritePolicy | None = None
    schema_policy: DeltaSchemaPolicy | None = None
    storage_options: Mapping[str, str] | None = None


@dataclass(frozen=True)
class OutputConfigOverrideOptions:
    """Grouped overrides for output configuration values."""

    ipc: IpcConfigOverrides | None = None
    delta: DeltaOutputOverrides | None = None
    output_storage_policy: OutputStoragePolicy | None = None


@dataclass(frozen=True)
class OutputConfigOverrides:
    """Overrides for output configuration values."""

    overwrite_intermediate_datasets: bool
    materialize_param_tables: bool = False
    writer_strategy: WriterStrategy = "arrow"
    ipc: IpcConfigOverrides | None = None
    delta: DeltaOutputOverrides | None = None
    output_storage_policy: OutputStoragePolicy | None = None


@cache(behavior="ignore")
@tag(layer="inputs", kind="object")
def output_config(
    work_dir: str | None,
    output_dir: str | None,
    overrides: OutputConfigOverrides,
) -> OutputConfig:
    """Bundle output configuration values.

    Returns
    -------
    OutputConfig
        Output configuration bundle.

    Raises
    ------
    ValueError
        Raised when the output storage policy is not Delta.
    """
    storage_policy = overrides.output_storage_policy or OutputStoragePolicy()
    if storage_policy.format != "delta":
        msg = f"Output storage policy requires Delta, got {storage_policy.format!r}."
        raise ValueError(msg)
    return OutputConfig(
        work_dir=work_dir,
        output_dir=output_dir,
        overwrite_intermediate_datasets=overrides.overwrite_intermediate_datasets,
        materialize_param_tables=overrides.materialize_param_tables,
        writer_strategy=overrides.writer_strategy,
        ipc_dump_enabled=overrides.ipc.dump_enabled if overrides.ipc is not None else False,
        ipc_write_config=overrides.ipc.write_config if overrides.ipc is not None else None,
        output_storage_policy=storage_policy,
        delta_write_policy=overrides.delta.write_policy if overrides.delta is not None else None,
        delta_schema_policy=overrides.delta.schema_policy if overrides.delta is not None else None,
        delta_storage_options=overrides.delta.storage_options
        if overrides.delta is not None
        else None,
    )


@cache(behavior="ignore")
@tag(layer="inputs", kind="object")
def cache_context(
    *,
    cache_path: str | None = None,
    cache_log_to_file: bool | None = None,
    cache_policy_profile: str | None = None,
) -> CacheRuntimeContext:
    """Return cache configuration details for run manifests.

    Returns
    -------
    CacheRuntimeContext
        Cache configuration snapshot for the run.
    """
    resolved_path = _cache_path_from_inputs(cache_path)
    log_enabled = True if cache_log_to_file is None else bool(cache_log_to_file)
    log_dir = resolved_path if log_enabled and resolved_path else None
    log_glob = str(Path(log_dir) / "**" / "*.jsonl") if log_dir is not None else None
    profile = _cache_policy_profile_from_inputs(cache_policy_profile)
    return CacheRuntimeContext(
        cache_path=resolved_path,
        cache_log_dir=log_dir,
        cache_log_glob=log_glob,
        cache_policy_profile=profile,
        cache_log_enabled=log_enabled,
    )


@cache(behavior="ignore")
@tag(layer="inputs", kind="list")
def materialized_outputs(materialized_outputs: Sequence[str] | None = None) -> tuple[str, ...]:
    """Return the ordered list of materialized output nodes.

    Returns
    -------
    tuple[str, ...]
        Ordered output node names targeted for materialization.
    """
    return tuple(str(name) for name in materialized_outputs or ())


@tag(layer="inputs", kind="object")
def execution_surface_policy(
    runtime_profile_spec: RuntimeProfileSpec,
    output_config: OutputConfig,
) -> ExecutionSurfacePolicy:
    """Return the execution surface policy for plan materialization.

    Returns
    -------
    ExecutionSurfacePolicy
        Policy describing streaming and writer strategy preferences.
    """
    return ExecutionSurfacePolicy(
        prefer_streaming=True,
        determinism_tier=runtime_profile_spec.determinism_tier,
        writer_strategy=output_config.writer_strategy,
    )


class IncrementalConfigKwargs(TypedDict, total=False):
    """Keyword arguments supported by incremental_config."""

    incremental_enabled: bool
    incremental_state_dir: str | None
    incremental_repo_id: str | None
    incremental_impact_strategy: str | None
    incremental_git_base_ref: str | None
    incremental_git_head_ref: str | None
    incremental_git_changed_only: bool


@tag(layer="inputs", kind="object")
def incremental_config(
    repo_root: str,
    **kwargs: Unpack[IncrementalConfigKwargs],
) -> IncrementalConfig:
    """Bundle incremental configuration values.

    Returns
    -------
    IncrementalConfig
        Incremental configuration bundle.

    Raises
    ------
    ValueError
        Raised when the impact strategy is unsupported.
    """
    enabled = bool(kwargs.get("incremental_enabled")) or _incremental_pipeline_enabled()
    state_dir = kwargs.get("incremental_state_dir") or env_value("CODEANATOMY_STATE_DIR")
    repo_id = kwargs.get("incremental_repo_id") or env_value("CODEANATOMY_REPO_ID")
    impact_strategy = (
        kwargs.get("incremental_impact_strategy")
        or env_value("CODEANATOMY_INCREMENTAL_IMPACT_STRATEGY")
        or "hybrid"
    )
    git_base_ref = kwargs.get("incremental_git_base_ref") or env_value("CODEANATOMY_GIT_BASE_REF")
    git_head_ref = kwargs.get("incremental_git_head_ref") or env_value("CODEANATOMY_GIT_HEAD_REF")
    git_changed_only_env = env_bool("CODEANATOMY_GIT_CHANGED_ONLY")
    git_changed_only = (
        git_changed_only_env
        if git_changed_only_env is not None
        else bool(kwargs.get("incremental_git_changed_only"))
    )
    impact_strategy = impact_strategy.lower()
    if impact_strategy not in {"hybrid", "symbol_closure", "import_closure"}:
        msg = f"Unsupported incremental impact strategy {impact_strategy!r}"
        raise ValueError(msg)
    impact_strategy_value: Literal["hybrid", "symbol_closure", "import_closure"]
    if impact_strategy == "symbol_closure":
        impact_strategy_value = "symbol_closure"
    elif impact_strategy == "import_closure":
        impact_strategy_value = "import_closure"
    else:
        impact_strategy_value = "hybrid"
    resolved_state_dir: Path | None = None
    if enabled:
        if state_dir:
            resolved_state_dir = Path(repo_root) / Path(state_dir)
        else:
            resolved_state_dir = Path(repo_root) / "build" / "state"
    return IncrementalConfig(
        enabled=enabled,
        state_dir=resolved_state_dir,
        repo_id=repo_id,
        impact_strategy=impact_strategy_value,
        git_base_ref=git_base_ref,
        git_head_ref=git_head_ref,
        git_changed_only=git_changed_only,
    )


@tag(layer="inputs", kind="object")
def tree_sitter_config(*, enable_tree_sitter: bool) -> TreeSitterConfig:
    """Bundle tree-sitter configuration values.

    Returns
    -------
    TreeSitterConfig
        Tree-sitter configuration bundle.
    """
    return TreeSitterConfig(enable_tree_sitter=enable_tree_sitter)
