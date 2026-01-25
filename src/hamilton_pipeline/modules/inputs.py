"""Hamilton input nodes for pipeline configuration."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypedDict, Unpack

from hamilton.function_modifiers import cache, tag
from ibis.backends import BaseBackend

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.execution_context import ExecutionContext
from core_types import JsonDict
from datafusion_engine.runtime import AdapterExecutionPolicy
from engine.plan_policy import ExecutionSurfacePolicy, WriterStrategy
from engine.runtime_profile import RuntimeProfileSpec, resolve_runtime_profile
from engine.session import EngineSession
from engine.session_factory import build_engine_session
from extract.repo_scan import default_repo_scan_options, repo_scan_globs_from_options
from extract.scip_extract import ScipExtractOptions, SCIPParseOptions
from hamilton_pipeline.lifecycle import get_hamilton_diagnostics_collector
from hamilton_pipeline.pipeline_types import (
    OutputConfig,
    OutputStoragePolicy,
    RelspecConfig,
    RepoScanConfig,
    RuntimeInspectConfig,
    ScipIdentityOverrides,
    ScipIndexConfig,
    TreeSitterConfig,
)
from ibis_engine.config import IbisBackendConfig
from ibis_engine.execution_factory import ibis_execution_from_ctx
from incremental.types import IncrementalConfig
from obs.diagnostics import DiagnosticsCollector
from relspec.pipeline_policy import PipelinePolicy
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy
from storage.ipc import IpcWriteConfig

if TYPE_CHECKING:
    from ibis_engine.execution import IbisExecutionContext


def _incremental_pipeline_enabled(config: IncrementalConfig | None = None) -> bool:
    if config is not None:
        return bool(config.enabled)
    mode = os.environ.get("CODEANATOMY_PIPELINE_MODE", "").strip().lower()
    return mode in {"incremental", "streaming"}


def _env_bool(name: str) -> bool | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    return None


@tag(layer="inputs", kind="runtime")
def runtime_profile_name() -> str:
    """Return the runtime profile name for execution.

    Returns
    -------
    str
        Runtime profile name (defaults to "default").
    """
    return os.environ.get("CODEANATOMY_RUNTIME_PROFILE", "").strip() or "default"


@tag(layer="inputs", kind="runtime")
def determinism_override() -> DeterminismTier | None:
    """Return an optional determinism tier override.

    Returns
    -------
    DeterminismTier | None
        Override tier when requested, otherwise ``None``.
    """
    force_flag = os.environ.get("CODEANATOMY_FORCE_TIER2", "").strip().lower()
    if force_flag in {"1", "true", "yes", "y"}:
        return DeterminismTier.CANONICAL
    tier = os.environ.get("CODEANATOMY_DETERMINISM_TIER", "").strip().lower()
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
    return mapping.get(tier)


@tag(layer="inputs", kind="runtime")
def runtime_profile_spec(
    runtime_profile_name: str,
    determinism_override: DeterminismTier | None,
) -> RuntimeProfileSpec:
    """Return a resolved runtime profile spec.

    Returns
    -------
    RuntimeProfileSpec
        Runtime profile spec with determinism overrides applied.
    """
    return resolve_runtime_profile(runtime_profile_name, determinism=determinism_override)


@cache(behavior="ignore")
@tag(layer="inputs", kind="runtime")
def ctx(runtime_profile_spec: RuntimeProfileSpec) -> ExecutionContext:
    """Build the execution context for Arrow DSL execution.

    Returns
    -------
    ExecutionContext
        Default execution context instance.
    """
    runtime = runtime_profile_spec.runtime
    runtime.apply_global_thread_pools()
    return ExecutionContext(runtime=runtime)


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
    ctx: ExecutionContext,
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
        ctx=ctx,
        runtime_spec=runtime_profile_spec,
        diagnostics=diagnostics_collector,
        surface_policy=execution_surface_policy,
        diagnostics_policy=pipeline_policy.diagnostics,
    )


@tag(layer="inputs", kind="runtime")
def ibis_backend_config(engine_session: EngineSession) -> IbisBackendConfig:
    """Return the default Ibis backend configuration.

    Returns
    -------
    IbisBackendConfig
        Backend configuration for Ibis execution.
    """
    return engine_session.engine_runtime.ibis_config


@cache(behavior="ignore")
@tag(layer="inputs", kind="runtime")
def ibis_backend(engine_session: EngineSession) -> BaseBackend:
    """Return a configured Ibis backend for pipeline execution.

    Returns
    -------
    ibis.backends.BaseBackend
        Backend instance for Ibis execution.
    """
    return engine_session.ibis_backend


@cache(behavior="ignore")
@tag(layer="inputs", kind="runtime")
def ibis_execution(
    engine_session: EngineSession,
    adapter_execution_policy: AdapterExecutionPolicy,
) -> IbisExecutionContext:
    """Bundle execution settings for Ibis plans.

    Returns
    -------
    IbisExecutionContext
        Execution context used for Ibis materialization.
    """
    return ibis_execution_from_ctx(
        engine_session.ctx,
        backend=engine_session.ibis_backend,
        execution_policy=adapter_execution_policy,
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
    flag = os.environ.get("CODEANATOMY_ENABLE_STREAMING_TABLES", "").strip().lower()
    if flag not in {"1", "true", "yes", "y"}:
        return None
    return None


@tag(layer="inputs", kind="runtime")
def adapter_execution_policy(ctx: ExecutionContext) -> AdapterExecutionPolicy:
    """Return the execution policy for adapter execution behavior.

    Raises
    ------
    ValueError
        Raised when an unsupported execution mode is configured.

    Returns
    -------
    AdapterExecutionPolicy
        Policy that governs adapter execution behavior.
    """
    if ctx.mode not in {"strict", "tolerant"}:
        msg = f"Unsupported execution mode: {ctx.mode!r}."
        raise ValueError(msg)
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
def include_globs() -> list[str]:
    """Return default include globs for repo scanning.

    Override via execute(overrides={"include_globs": [...]}).

    Returns
    -------
    list[str]
        Glob patterns to include.
    """
    include, _ = repo_scan_globs_from_options(default_repo_scan_options())
    return include


@tag(layer="inputs", kind="scalar")
def exclude_globs() -> list[str]:
    """Return default exclude globs for repo scanning.

    Override via execute(overrides={"exclude_globs": [...]}).

    Returns
    -------
    list[str]
        Glob patterns to exclude.
    """
    _, exclude = repo_scan_globs_from_options(default_repo_scan_options())
    return exclude


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
def enable_runtime_inspect() -> bool:
    """Return whether runtime inspection is enabled.

    Returns
    -------
    bool
        True to enable runtime inspection.
    """
    return False


@tag(layer="inputs", kind="scalar")
def runtime_module_allowlist() -> list[str]:
    """Return the default module allowlist for runtime inspection.

    Returns
    -------
    list[str]
        Module allowlist strings.
    """
    return []


@tag(layer="inputs", kind="scalar")
def runtime_timeout_s() -> int:
    """Return the runtime inspection timeout in seconds.

    Returns
    -------
    int
        Timeout seconds for runtime inspection.
    """
    return 15


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


@tag(layer="inputs", kind="object")
def repo_scan_config(
    repo_root: str,
    include_globs: list[str],
    exclude_globs: list[str],
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
        include_globs=tuple(include_globs),
        exclude_globs=tuple(exclude_globs),
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


@tag(layer="inputs", kind="object")
def execution_surface_policy(
    ctx: ExecutionContext,
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
        determinism_tier=ctx.determinism,
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
    state_dir = kwargs.get("incremental_state_dir") or os.environ.get("CODEANATOMY_STATE_DIR")
    repo_id = kwargs.get("incremental_repo_id") or os.environ.get("CODEANATOMY_REPO_ID")
    impact_strategy = (
        kwargs.get("incremental_impact_strategy")
        or os.environ.get("CODEANATOMY_INCREMENTAL_IMPACT_STRATEGY")
        or "hybrid"
    )
    git_base_ref = kwargs.get("incremental_git_base_ref") or os.environ.get(
        "CODEANATOMY_GIT_BASE_REF"
    )
    git_head_ref = kwargs.get("incremental_git_head_ref") or os.environ.get(
        "CODEANATOMY_GIT_HEAD_REF"
    )
    git_changed_only_env = _env_bool("CODEANATOMY_GIT_CHANGED_ONLY")
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


@tag(layer="inputs", kind="object")
def runtime_inspect_config(
    *,
    enable_runtime_inspect: bool,
    runtime_module_allowlist: list[str],
    runtime_timeout_s: int,
) -> RuntimeInspectConfig:
    """Bundle runtime inspection configuration values.

    Returns
    -------
    RuntimeInspectConfig
        Runtime inspection configuration bundle.
    """
    return RuntimeInspectConfig(
        enable_runtime_inspect=enable_runtime_inspect,
        module_allowlist=tuple(runtime_module_allowlist),
        timeout_s=int(runtime_timeout_s),
    )
