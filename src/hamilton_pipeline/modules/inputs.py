"""Hamilton input nodes for pipeline configuration."""

from __future__ import annotations

from typing import Literal

from hamilton.function_modifiers import tag

from arrowdsl.runtime import ExecutionContext, RuntimeProfile
from core_types import JsonDict
from hamilton_pipeline.pipeline_types import (
    OutputConfig,
    RelspecConfig,
    RepoScanConfig,
    RuntimeInspectConfig,
    TreeSitterConfig,
)


@tag(layer="inputs", kind="runtime")
def ctx() -> ExecutionContext:
    """Build the execution context for Arrow DSL execution.

    Returns
    -------
    ExecutionContext
        Default execution context instance.
    """
    runtime = RuntimeProfile(name="DEFAULT")
    return ExecutionContext(runtime=runtime)


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
def scip_index_path() -> str | None:
    """Return the default SCIP index path.

    Override to attach SCIP data.

    Returns
    -------
    str | None
        SCIP index path, or None when not provided.
    """
    return None


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
