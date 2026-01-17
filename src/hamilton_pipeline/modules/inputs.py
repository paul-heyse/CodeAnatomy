"""Hamilton input nodes for pipeline configuration."""

from __future__ import annotations

import importlib
import os
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Literal

from hamilton.function_modifiers import tag
from ibis.backends import BaseBackend

from arrowdsl.core.context import ExecutionContext, execution_context_factory
from config import AdapterMode
from core_types import JsonDict
from datafusion_engine.runtime import AdapterExecutionPolicy
from extract.scip_extract import SCIPParseOptions
from hamilton_pipeline.pipeline_types import (
    OutputConfig,
    RelspecConfig,
    RepoScanConfig,
    RuntimeInspectConfig,
    ScipIdentityOverrides,
    ScipIndexConfig,
    TreeSitterConfig,
)
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.execution import IbisAdapterExecution
from incremental.types import IncrementalConfig
from relspec.pipeline_policy import PipelinePolicy


def _incremental_pipeline_enabled(config: IncrementalConfig | None = None) -> bool:
    if config is not None:
        return bool(config.enabled)
    mode = os.environ.get("CODEANATOMY_PIPELINE_MODE", "").strip().lower()
    return mode in {"incremental", "streaming"}


@tag(layer="inputs", kind="runtime")
def ctx() -> ExecutionContext:
    """Build the execution context for Arrow DSL execution.

    Returns
    -------
    ExecutionContext
        Default execution context instance.
    """
    return execution_context_factory("default")


@tag(layer="inputs", kind="runtime")
def ibis_backend_config(ctx: ExecutionContext) -> IbisBackendConfig:
    """Return the default Ibis backend configuration.

    Returns
    -------
    IbisBackendConfig
        Backend configuration for Ibis execution.
    """
    return IbisBackendConfig(datafusion_profile=ctx.runtime.datafusion)


@tag(layer="inputs", kind="runtime")
def ibis_backend(ibis_backend_config: IbisBackendConfig) -> BaseBackend:
    """Return a configured Ibis backend for pipeline execution.

    Returns
    -------
    ibis.backends.BaseBackend
        Backend instance for Ibis execution.
    """
    return build_backend(ibis_backend_config)


@tag(layer="inputs", kind="runtime")
def ibis_execution(
    ctx: ExecutionContext,
    adapter_mode: AdapterMode,
    adapter_execution_policy: AdapterExecutionPolicy,
    ibis_backend: BaseBackend,
) -> IbisAdapterExecution:
    """Bundle adapter execution settings for Ibis plans.

    Returns
    -------
    IbisAdapterExecution
        Execution context used by adapterized Ibis materialization.
    """
    return IbisAdapterExecution(
        ctx=ctx,
        adapter_mode=adapter_mode,
        execution_policy=adapter_execution_policy,
        ibis_backend=ibis_backend,
    )


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
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError:
        return None
    factory = getattr(module, "streaming_table_provider", None)
    if callable(factory):
        provider = factory()
        if isinstance(provider, bool):
            return None
        return provider
    return None


@tag(layer="inputs", kind="runtime")
def adapter_mode(ctx: ExecutionContext) -> AdapterMode:
    """Return the adapter mode flags for plan execution.

    Returns
    -------
    AdapterMode
        Adapter mode configuration bundle.
    """
    mode = AdapterMode()
    if ctx.debug:
        return replace(mode, use_datafusion_bridge=True)
    flag = os.environ.get("CODEANATOMY_USE_DATAFUSION_BRIDGE", "").strip().lower()
    if flag in {"1", "true", "yes", "y"}:
        return replace(mode, use_datafusion_bridge=True)
    return mode


@tag(layer="inputs", kind="runtime")
def adapter_execution_policy(ctx: ExecutionContext) -> AdapterExecutionPolicy:
    """Return the execution policy for adapter fallback behavior.

    Returns
    -------
    AdapterExecutionPolicy
        Policy that governs fallback and strictness behavior.
    """
    policy = AdapterExecutionPolicy()
    if ctx.mode == "strict":
        policy = replace(policy, fail_on_fallback=True)
    strict_flag = os.environ.get("CODEANATOMY_STRICT_FALLBACK", "").strip().lower()
    if strict_flag in {"1", "true", "yes", "y"}:
        policy = replace(policy, fail_on_fallback=True)
    disable_flag = os.environ.get("CODEANATOMY_DISABLE_FALLBACK", "").strip().lower()
    if disable_flag in {"1", "true", "yes", "y"}:
        policy = replace(policy, allow_fallback=False)
    return policy


@tag(layer="inputs", kind="object")
def relspec_param_values() -> JsonDict:
    """Return parameter values for relspec execution.

    Returns
    -------
    JsonDict
        Mapping of parameter names to values.
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
def param_table_parquet_paths() -> Mapping[str, str] | None:
    """Return optional parquet paths for param table replay.

    Returns
    -------
    Mapping[str, str] | None
        Mapping of logical param names to parquet dataset paths.
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
    return ["**/*.py"]


@tag(layer="inputs", kind="scalar")
def exclude_globs() -> list[str]:
    """Return default exclude globs for repo scanning.

    Override via execute(overrides={"exclude_globs": [...]}).

    Returns
    -------
    list[str]
        Glob patterns to exclude.
    """
    return [
        "**/.git/**",
        "**/.venv/**",
        "**/venv/**",
        "**/__pycache__/**",
        "**/node_modules/**",
        "**/.mypy_cache/**",
        "**/.pytest_cache/**",
        "**/.ruff_cache/**",
        "**/build/**",
        "**/dist/**",
    ]


@tag(layer="inputs", kind="scalar")
def max_files() -> int:
    """Return the default maximum number of files to scan.

    Returns
    -------
    int
        Maximum file count for repository scanning.
    """
    return 200_000


@tag(layer="inputs", kind="scalar")
def repo_include_text() -> bool:
    """Return whether repo scan should include decoded text payloads.

    Returns
    -------
    bool
        True to include text payloads in repo scan output.
    """
    return True


@tag(layer="inputs", kind="scalar")
def repo_include_bytes() -> bool:
    """Return whether repo scan should include raw bytes payloads.

    Returns
    -------
    bool
        True to include bytes payloads in repo scan output.
    """
    return True


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


@tag(layer="inputs", kind="scalar")
def hamilton_tags() -> JsonDict:
    """Return optional metadata tags for introspection.

    Returns
    -------
    JsonDict
        Optional tag metadata.
    """
    tags: JsonDict = {}
    return tags


@tag(layer="inputs", kind="object")
def repo_scan_config(
    repo_root: str,
    include_globs: list[str],
    exclude_globs: list[str],
    max_files: int,
) -> RepoScanConfig:
    """Bundle repository scan configuration.

    Returns
    -------
    RepoScanConfig
        Repository scan configuration bundle.
    """
    return RepoScanConfig(
        repo_root=repo_root,
        include_globs=tuple(include_globs),
        exclude_globs=tuple(exclude_globs),
        max_files=int(max_files),
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
def output_config(
    work_dir: str | None,
    output_dir: str | None,
    *,
    overwrite_intermediate_datasets: bool,
    materialize_param_tables: bool = False,
) -> OutputConfig:
    """Bundle output configuration values.

    Returns
    -------
    OutputConfig
        Output configuration bundle.
    """
    return OutputConfig(
        work_dir=work_dir,
        output_dir=output_dir,
        overwrite_intermediate_datasets=overwrite_intermediate_datasets,
        materialize_param_tables=materialize_param_tables,
    )


@tag(layer="inputs", kind="object")
def incremental_config(
    repo_root: str,
    *,
    incremental_enabled: bool = False,
    incremental_state_dir: str | None = None,
    incremental_repo_id: str | None = None,
) -> IncrementalConfig:
    """Bundle incremental configuration values.

    Returns
    -------
    IncrementalConfig
        Incremental configuration bundle.
    """
    enabled = bool(incremental_enabled) or _incremental_pipeline_enabled()
    state_dir = incremental_state_dir or os.environ.get("CODEANATOMY_STATE_DIR")
    repo_id = incremental_repo_id or os.environ.get("CODEANATOMY_REPO_ID")
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
