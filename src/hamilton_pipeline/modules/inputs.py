"""Hamilton input nodes for pipeline configuration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypedDict, Unpack

from hamilton.function_modifiers import cache

from core_types import DeterminismTier, JsonDict, parse_determinism_tier
from datafusion_engine.materialize_policy import MaterializationPolicy, WriterStrategy
from datafusion_engine.semantics_runtime import semantic_runtime_from_profile
from datafusion_engine.session.runtime import AdapterExecutionPolicy
from engine.runtime_profile import RuntimeProfileSpec, resolve_runtime_profile
from engine.session import EngineSession
from engine.session_factory import build_engine_session
from extract.extractors.scip.extract import ScipExtractOptions, SCIPParseOptions
from extract.scanning.repo_scan import default_repo_scan_options
from hamilton_pipeline.io_contracts import OutputRuntimeContext
from hamilton_pipeline.lifecycle import get_hamilton_diagnostics_collector
from hamilton_pipeline.tag_policy import TagPolicy, apply_tag
from hamilton_pipeline.types import (
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
from obs.diagnostics import DiagnosticsCollector
from relspec.pipeline_policy import PipelinePolicy
from semantics.incremental import IncrementalConfig
from semantics.runtime import SemanticRuntimeConfig
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy
from storage.ipc_utils import IpcWriteConfig
from utils.env_utils import env_bool, env_value

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetCatalog
    from obs.otel import OtelBootstrapOptions
else:
    try:
        from obs.otel import OtelBootstrapOptions
    except ImportError:
        OtelBootstrapOptions = object
    DatasetCatalog = object


def _incremental_pipeline_enabled(config: IncrementalConfig | None = None) -> bool:
    if config is not None:
        return bool(config.enabled)
    mode = (env_value("CODEANATOMY_PIPELINE_MODE") or "").lower()
    return mode in {"incremental", "streaming"}


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


@apply_tag(TagPolicy(layer="inputs", kind="runtime"))
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


@apply_tag(TagPolicy(layer="inputs", kind="runtime"))
def determinism_override(
    determinism_override_override: DeterminismTier | str | None = None,
) -> DeterminismTier | None:
    """Return an optional determinism tier override.

    Returns
    -------
    DeterminismTier | None
        Override tier when requested, otherwise ``None``.
    """
    resolved = parse_determinism_tier(determinism_override_override)
    if resolved is not None:
        return resolved
    force_flag = env_bool("CODEANATOMY_FORCE_TIER2", default=False, on_invalid="false")
    if force_flag:
        return DeterminismTier.CANONICAL
    tier = env_value("CODEANATOMY_DETERMINISM_TIER")
    return parse_determinism_tier(tier)


@cache(behavior="ignore")
@apply_tag(TagPolicy(layer="inputs", kind="runtime"))
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
    semantic_root = _semantic_output_root(output_config)
    catalog_name, registry_catalogs = _semantic_output_catalog(
        output_config=output_config,
        semantic_root=semantic_root,
        existing_catalogs=resolved.datafusion.registry_catalogs,
    )
    extract_root = _extract_output_root(output_config)
    extract_catalog_name, registry_catalogs = _extract_output_catalog(
        output_config=output_config,
        extract_root=extract_root,
        existing_catalogs=registry_catalogs,
    )
    cache_root = _cache_output_root(
        output_config=output_config,
        existing=resolved.datafusion.cache_output_root,
    )
    if not _requires_profile_update(
        (
            normalize_root,
            semantic_root,
            extract_root,
            cache_root,
            catalog_name,
            extract_catalog_name,
        )
    ):
        return resolved
    resolved_semantic_catalog_name = catalog_name
    resolved_extract_catalog_name = extract_catalog_name
    updated_profile = replace(
        resolved.datafusion,
        normalize_output_root=normalize_root,
        semantic_output_root=semantic_root,
        semantic_output_catalog_name=resolved_semantic_catalog_name,
        extract_output_root=extract_root,
        extract_output_catalog_name=resolved_extract_catalog_name,
        cache_output_root=cache_root,
        registry_catalogs=registry_catalogs,
    )
    return RuntimeProfileSpec(
        name=resolved.name,
        datafusion=updated_profile,
        determinism_tier=resolved.determinism_tier,
        tracker_config=resolved.tracker_config,
        hamilton_telemetry=resolved.hamilton_telemetry,
    )


@cache(behavior="ignore")
@apply_tag(TagPolicy(layer="inputs", kind="runtime"))
def ctx(
    runtime_profile_spec: RuntimeProfileSpec,
    output_config: OutputConfig,
    cache_context: CacheRuntimeContext,
) -> OutputRuntimeContext:
    """Return the output runtime context for caching and execution.

    Returns
    -------
    OutputRuntimeContext
        Runtime context for cache and output operations.
    """
    return OutputRuntimeContext(
        runtime_profile_spec=runtime_profile_spec,
        output_config=output_config,
        cache_context=cache_context,
    )


def _normalize_output_root(output_config: OutputConfig) -> str | None:
    base_dir = output_config.output_dir or output_config.work_dir
    if not base_dir:
        return None
    return str(Path(base_dir) / "normalize")


def _requires_profile_update(values: tuple[object | None, ...]) -> bool:
    return any(value is not None for value in values)


def _semantic_output_root(output_config: OutputConfig) -> str | None:
    base_dir = output_config.output_dir or output_config.work_dir
    if not base_dir:
        return None
    return str(Path(base_dir) / "semantic")


def _extract_output_root(output_config: OutputConfig) -> str | None:
    base_dir = output_config.output_dir or output_config.work_dir
    if not base_dir:
        return None
    return str(Path(base_dir) / "extract")


def _cache_output_root(
    *,
    output_config: OutputConfig,
    existing: str | None,
) -> str | None:
    if existing is not None:
        return existing
    base_dir = output_config.output_dir or output_config.work_dir
    if not base_dir:
        return None
    return str(Path(base_dir) / "cache")


def _semantic_output_catalog(
    *,
    output_config: OutputConfig,
    semantic_root: str | None,
    existing_catalogs: Mapping[str, DatasetCatalog],
) -> tuple[str | None, Mapping[str, DatasetCatalog]]:
    if semantic_root is None:
        return output_config.semantic_output_catalog_name, existing_catalogs
    catalog_name = output_config.semantic_output_catalog_name or "semantic_outputs"
    if catalog_name in existing_catalogs:
        return catalog_name, existing_catalogs

    from datafusion_engine.dataset.registry import DatasetCatalog, DatasetLocation
    from semantics.catalog.dataset_specs import dataset_spec
    from semantics.registry import SEMANTIC_MODEL

    catalog = DatasetCatalog()
    view_names = [spec.name for spec in SEMANTIC_MODEL.outputs]
    for name in view_names:
        spec = None
        try:
            spec = dataset_spec(name)
        except KeyError:
            spec = None
        location = DatasetLocation(
            path=str(Path(semantic_root) / name),
            format="delta",
            dataset_spec=spec,
        )
        catalog.register(name, location)

    updated_catalogs = dict(existing_catalogs)
    updated_catalogs[catalog_name] = catalog
    return catalog_name, updated_catalogs


def _extract_output_catalog(
    *,
    output_config: OutputConfig,
    extract_root: str | None,
    existing_catalogs: Mapping[str, DatasetCatalog],
) -> tuple[str | None, Mapping[str, DatasetCatalog]]:
    if extract_root is None:
        return output_config.extract_output_catalog_name, existing_catalogs
    catalog_name = output_config.extract_output_catalog_name or "extract_outputs"
    if catalog_name in existing_catalogs:
        return catalog_name, existing_catalogs
    from datafusion_engine.extract.output_catalog import build_extract_output_catalog

    catalog = build_extract_output_catalog(output_root=extract_root)
    updated_catalogs = dict(existing_catalogs)
    updated_catalogs[catalog_name] = catalog
    return catalog_name, updated_catalogs


@cache(behavior="ignore")
@apply_tag(TagPolicy(layer="inputs", kind="runtime"))
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
@apply_tag(TagPolicy(layer="inputs", kind="runtime"))
def engine_session_runtime_inputs(
    runtime_profile_spec: RuntimeProfileSpec,
    execution_surface_policy: MaterializationPolicy,
    semantic_runtime_config: SemanticRuntimeConfig,
) -> EngineSessionRuntimeInputs:
    """Build runtime-focused inputs for the engine session.

    Returns
    -------
    EngineSessionRuntimeInputs
        Grouped runtime inputs for EngineSession construction.
    """
    return EngineSessionRuntimeInputs(
        runtime_profile_spec=runtime_profile_spec,
        execution_surface_policy=execution_surface_policy,
        semantic_runtime_config=semantic_runtime_config,
    )


@cache(behavior="ignore")
@apply_tag(TagPolicy(layer="inputs", kind="runtime"))
def engine_session_observability_inputs(
    diagnostics_collector: DiagnosticsCollector,
    pipeline_policy: PipelinePolicy,
    otel_options: OtelBootstrapOptions | None = None,
) -> EngineSessionObservabilityInputs:
    """Build observability-focused inputs for the engine session.

    Returns
    -------
    EngineSessionObservabilityInputs
        Grouped observability inputs for EngineSession construction.
    """
    return EngineSessionObservabilityInputs(
        diagnostics_collector=diagnostics_collector,
        pipeline_policy=pipeline_policy,
        otel_options=otel_options,
    )


@dataclass(frozen=True)
class EngineSessionRuntimeInputs:
    """Runtime inputs for EngineSession construction."""

    runtime_profile_spec: RuntimeProfileSpec
    execution_surface_policy: MaterializationPolicy
    semantic_runtime_config: SemanticRuntimeConfig


@dataclass(frozen=True)
class EngineSessionObservabilityInputs:
    """Observability inputs for EngineSession construction."""

    diagnostics_collector: DiagnosticsCollector
    pipeline_policy: PipelinePolicy
    otel_options: OtelBootstrapOptions | None


@cache(behavior="ignore")
@apply_tag(TagPolicy(layer="inputs", kind="runtime"))
def engine_session(
    runtime_inputs: EngineSessionRuntimeInputs,
    observability_inputs: EngineSessionObservabilityInputs,
) -> EngineSession:
    """Build an engine session for downstream execution surfaces.

    Returns
    -------
    EngineSession
        Session containing runtime surfaces and diagnostics wiring.
    """
    return build_engine_session(
        runtime_spec=runtime_inputs.runtime_profile_spec,
        diagnostics=observability_inputs.diagnostics_collector,
        surface_policy=runtime_inputs.execution_surface_policy,
        diagnostics_policy=observability_inputs.pipeline_policy.diagnostics,
        semantic_config=runtime_inputs.semantic_runtime_config,
        otel_options=observability_inputs.otel_options,
    )


@cache(behavior="ignore")
@apply_tag(TagPolicy(layer="inputs", kind="runtime"))
def semantic_runtime_config(
    runtime_profile_spec: RuntimeProfileSpec,
) -> SemanticRuntimeConfig:
    """Return semantic runtime configuration derived from the runtime profile.

    Returns
    -------
    SemanticRuntimeConfig
        Semantic runtime configuration derived from the profile.
    """
    return semantic_runtime_from_profile(runtime_profile_spec.datafusion)


@cache(behavior="ignore")
@apply_tag(TagPolicy(layer="inputs", kind="runtime"))
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


@apply_tag(TagPolicy(layer="inputs", kind="runtime"))
def adapter_execution_policy() -> AdapterExecutionPolicy:
    """Return the execution policy for adapter execution behavior.

    Returns
    -------
    AdapterExecutionPolicy
        Adapter execution policy configuration.
    """
    return AdapterExecutionPolicy()


@apply_tag(TagPolicy(layer="inputs", kind="object"))
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


@apply_tag(TagPolicy(layer="inputs", kind="object"))
def pipeline_policy() -> PipelinePolicy:
    """Return the pipeline policy for rule execution.

    Returns
    -------
    PipelinePolicy
        Policy bundle for rule execution and diagnostics.
    """
    return PipelinePolicy()


@apply_tag(TagPolicy(layer="inputs", kind="object"))
def param_table_delta_paths() -> Mapping[str, str] | None:
    """Return optional Delta table paths for param table replay.

    Returns
    -------
    Mapping[str, str] | None
        Mapping of logical param names to Delta table paths.
    """
    return {}


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def python_extensions() -> list[str]:
    """Return explicit Python extensions for repo scope rules.

    Override via execute(overrides={"python_extensions": [...]}).

    Returns
    -------
    list[str]
        Explicit Python extensions to add to discovery.
    """
    return []


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def include_untracked() -> bool:
    """Return whether to include untracked files in repo scope.

    Returns
    -------
    bool
        True to include untracked files.
    """
    return True


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def include_submodules() -> bool:
    """Return whether to include submodules in repo scope.

    Returns
    -------
    bool
        True to include submodules.
    """
    return False


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def include_worktrees() -> bool:
    """Return whether to include worktrees in repo scope.

    Returns
    -------
    bool
        True to include worktrees.
    """
    return False


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def follow_symlinks() -> bool:
    """Return whether to follow symlinks in repo scope.

    Returns
    -------
    bool
        True to follow symlinks.
    """
    return False


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def external_interface_depth() -> Literal["metadata", "full"]:
    """Return the external interface extraction depth.

    Returns
    -------
    str
        Depth policy for external interface extraction.
    """
    return "metadata"


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
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


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def cache_salt() -> str:
    """Return a manual cache-busting salt for repo-dependent nodes.

    Returns
    -------
    str
        Cache salt string used to invalidate cached outputs when changed.
    """
    return ""


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def output_dir() -> str | None:
    """Return the default output directory for artifacts.

    Override to materialize artifacts.

    Returns
    -------
    str | None
        Output directory path, or None to disable.
    """
    return None


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def work_dir() -> str | None:
    """Return the default working directory for intermediates.

    Override if you want all intermediate datasets written somewhere specific.

    Returns
    -------
    str | None
        Working directory path, or None for automatic selection.
    """
    return None


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
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


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def scip_index_config() -> ScipIndexConfig:
    """Return default config for scip-python indexing.

    Override via execute(overrides={"scip_index_config": ScipIndexConfig(...)}).

    Returns
    -------
    ScipIndexConfig
        Indexing configuration.
    """
    return ScipIndexConfig()


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def scip_parse_options() -> SCIPParseOptions:
    """Return default parse options for SCIP extraction.

    Override via execute(overrides={"scip_parse_options": SCIPParseOptions(...)}).

    Returns
    -------
    SCIPParseOptions
        Parsing options for SCIP index decoding.
    """
    return SCIPParseOptions()


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def scip_extract_options() -> ScipExtractOptions:
    """Return default extraction options for SCIP tables.

    Override via execute(overrides={"scip_extract_options": ScipExtractOptions(...)}).

    Returns
    -------
    ScipExtractOptions
        Extract options for SCIP table materialization.
    """
    return ScipExtractOptions()


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def enable_tree_sitter() -> bool:
    """Return whether tree-sitter extraction is enabled.

    Returns
    -------
    bool
        True to enable tree-sitter extraction.
    """
    return False


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
def relspec_mode() -> Literal["memory", "filesystem"]:
    """Return the relationship spec mode.

    Override via execute(overrides={"relspec_mode": "filesystem"}).

    Returns
    -------
    str
        Relationship spec mode ("memory" or "filesystem").
    """
    return "memory"


@apply_tag(TagPolicy(layer="inputs", kind="scalar"))
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


@apply_tag(TagPolicy(layer="inputs", kind="object"))
def repo_scope_globs() -> RepoScopeGlobs:
    """Bundle repository scope globs.

    Returns
    -------
    RepoScopeGlobs
        Include/exclude glob bundle.
    """
    return RepoScopeGlobs(include_globs=(), exclude_globs=())


@apply_tag(TagPolicy(layer="inputs", kind="object"))
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


@apply_tag(TagPolicy(layer="inputs", kind="object"))
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


@apply_tag(TagPolicy(layer="inputs", kind="object"))
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


@apply_tag(TagPolicy(layer="inputs", kind="object"))
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


@apply_tag(TagPolicy(layer="inputs", kind="object"))
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
        semantic_output_catalog_name=resolved.semantic_output_catalog_name,
        extract_output_catalog_name=resolved.extract_output_catalog_name,
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
    semantic_output_catalog_name: str | None = None
    extract_output_catalog_name: str | None = None


@dataclass(frozen=True)
class OutputConfigOverrides:
    """Overrides for output configuration values."""

    overwrite_intermediate_datasets: bool
    materialize_param_tables: bool = False
    writer_strategy: WriterStrategy = "arrow"
    ipc: IpcConfigOverrides | None = None
    delta: DeltaOutputOverrides | None = None
    output_storage_policy: OutputStoragePolicy | None = None
    semantic_output_catalog_name: str | None = None
    extract_output_catalog_name: str | None = None


@cache(behavior="ignore")
@apply_tag(TagPolicy(layer="inputs", kind="object"))
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
        semantic_output_catalog_name=overrides.semantic_output_catalog_name,
        extract_output_catalog_name=overrides.extract_output_catalog_name,
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
@apply_tag(TagPolicy(layer="inputs", kind="object"))
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
@apply_tag(TagPolicy(layer="inputs", kind="list"))
def materialized_outputs(requested_outputs: Sequence[str] | None = None) -> tuple[str, ...]:
    """Return the ordered list of materialized output nodes.

    Returns
    -------
    tuple[str, ...]
        Ordered output node names targeted for materialization.
    """
    return tuple(str(name) for name in requested_outputs or ())


@apply_tag(TagPolicy(layer="inputs", kind="object"))
def execution_surface_policy(
    runtime_profile_spec: RuntimeProfileSpec,
    output_config: OutputConfig,
) -> MaterializationPolicy:
    """Return the execution surface policy for plan materialization.

    Returns
    -------
    MaterializationPolicy
        Policy describing streaming and writer strategy preferences.
    """
    return MaterializationPolicy(
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


@apply_tag(TagPolicy(layer="inputs", kind="object"))
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


@apply_tag(TagPolicy(layer="inputs", kind="object"))
def tree_sitter_config(*, enable_tree_sitter: bool) -> TreeSitterConfig:
    """Bundle tree-sitter configuration values.

    Returns
    -------
    TreeSitterConfig
        Tree-sitter configuration bundle.
    """
    return TreeSitterConfig(enable_tree_sitter=enable_tree_sitter)
