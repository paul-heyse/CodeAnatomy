"""Hamilton dataloaders for repo scan and SCIP sources."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path

from hamilton.function_modifiers import dataloader, inject, resolve_from_config, source, tag

from datafusion_engine.arrow_interop import RecordBatchReaderLike, TableLike
from engine.runtime_profile import RuntimeProfileSpec
from extract.helpers import ExtractExecutionContext
from extract.python_scope import PythonScopePolicy
from extract.repo_scan import RepoScanOptions, scan_repo_tables
from extract.repo_scope import RepoScopeOptions
from extract.scip_extract import (
    ScipExtractContext,
    ScipExtractOptions,
    extract_scip_tables,
    run_scip_python_index,
)
from extract.scip_identity import resolve_scip_identity
from extract.scip_indexer import (
    build_scip_index_options,
    resolve_scip_paths,
    write_scip_environment_json,
)
from hamilton_pipeline.types import RepoScanConfig, ScipIdentityOverrides, ScipIndexConfig
from incremental.types import IncrementalConfig


def _rows(table: TableLike | RecordBatchReaderLike) -> int:
    if isinstance(table, RecordBatchReaderLike):
        table = table.read_all()
    value = getattr(table, "num_rows", 0)
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


def _repo_scan_options(
    config: RepoScanConfig,
    *,
    incremental: IncrementalConfig | None,
    cache_salt: str,
) -> RepoScanOptions:
    repo_id = incremental.repo_id if incremental is not None else None
    if cache_salt:
        repo_id = f"{repo_id}:{cache_salt}" if repo_id else cache_salt
    scope_config = config.scope_config
    scope_policy = RepoScopeOptions(
        python_scope=PythonScopePolicy(extra_extensions=scope_config.python_extensions),
        include_globs=scope_config.include_globs,
        exclude_globs=scope_config.exclude_globs,
        include_untracked=scope_config.include_untracked,
        include_submodules=scope_config.include_submodules,
        include_worktrees=scope_config.include_worktrees,
        follow_symlinks=scope_config.follow_symlinks,
    )
    return RepoScanOptions(
        repo_id=repo_id,
        scope_policy=scope_policy,
        max_files=config.max_files,
        diff_base_ref=config.diff_base_ref,
        diff_head_ref=config.diff_head_ref,
        changed_only=config.changed_only,
        record_pathspec_trace=config.record_pathspec_trace,
        pathspec_trace_limit=config.pathspec_trace_limit,
        pathspec_trace_pattern_limit=config.pathspec_trace_pattern_limit,
    )


@tag(layer="inputs", artifact="scip_index_path", kind="path")
def scip_index_path(
    repo_root: str,
    scip_index_config: ScipIndexConfig,
    scip_identity_overrides: ScipIdentityOverrides,
) -> str | None:
    """Return the resolved path to index.scip, running scip-python when needed.

    Returns
    -------
    str | None
        Resolved index.scip path when available.

    Raises
    ------
    ValueError
        Raised when the index output path cannot be resolved.
    """
    repo_root_path = Path(repo_root)
    if not scip_index_config.enabled and not scip_index_config.index_path_override:
        return None
    paths = resolve_scip_paths(
        repo_root_path,
        scip_index_config.output_dir,
        scip_index_config.index_path_override,
    )
    if not scip_index_config.enabled:
        return str(paths.index_path)
    identity = resolve_scip_identity(
        repo_root_path,
        project_name_override=scip_identity_overrides.project_name_override,
        project_version_override=scip_identity_overrides.project_version_override,
        project_namespace_override=scip_identity_overrides.project_namespace_override,
    )
    config = scip_index_config
    if scip_index_config.generate_env_json and scip_index_config.env_json_path:
        env_path = Path(scip_index_config.env_json_path)
        resolved = env_path if env_path.is_absolute() else repo_root_path / env_path
        write_scip_environment_json(resolved)
    elif scip_index_config.generate_env_json and scip_index_config.env_json_path is None:
        env_path = paths.build_dir / "environment.json"
        write_scip_environment_json(env_path)
        config = replace(scip_index_config, env_json_path=str(env_path))
    options = build_scip_index_options(
        repo_root=repo_root_path,
        identity=identity,
        config=config,
    )
    output_path = options.output_path
    if output_path is None:
        msg = "SCIP index output path must be resolved for indexing."
        raise ValueError(msg)
    if not output_path.exists():
        run_scip_python_index(options)
    return str(output_path)


@dataloader()
@tag(layer="inputs", artifact="repo_files", kind="dataloader")
def repo_files(
    repo_scan_config: RepoScanConfig,
    runtime_profile_spec: RuntimeProfileSpec,
    cache_salt: str,
    incremental_config: IncrementalConfig | None = None,
) -> tuple[TableLike, dict[str, object]]:
    """Load repo scan outputs as a TableLike plus metadata.

    Returns
    -------
    tuple[TableLike, dict[str, object]]
        Repo scan table and metadata payload.
    """
    options = _repo_scan_options(
        repo_scan_config,
        incremental=incremental_config,
        cache_salt=cache_salt,
    )
    exec_ctx = ExtractExecutionContext(runtime_spec=runtime_profile_spec)
    tables = scan_repo_tables(repo_scan_config.repo_root, options=options, context=exec_ctx)
    table = tables["repo_files_v1"]
    if isinstance(table, RecordBatchReaderLike):
        table = table.read_all()
    metadata: dict[str, object] = {
        "repo_root": repo_scan_config.repo_root,
        "rows": _rows(table),
        "include_globs": list(repo_scan_config.scope_config.include_globs),
        "exclude_globs": list(repo_scan_config.scope_config.exclude_globs),
        "python_extensions": list(repo_scan_config.scope_config.python_extensions),
        "include_untracked": repo_scan_config.scope_config.include_untracked,
        "include_submodules": repo_scan_config.scope_config.include_submodules,
        "include_worktrees": repo_scan_config.scope_config.include_worktrees,
        "changed_only": options.changed_only,
        "diff_base_ref": options.diff_base_ref,
        "diff_head_ref": options.diff_head_ref,
        "repo_id": options.repo_id,
    }
    return table, metadata


@dataloader()
@tag(layer="inputs", artifact="scip_tables", kind="dataloader")
def scip_tables(
    repo_root: str,
    scip_index_path: str | None,
    scip_extract_options: ScipExtractOptions,
    runtime_profile_spec: RuntimeProfileSpec,
) -> tuple[Mapping[str, TableLike], dict[str, object]]:
    """Load SCIP tables from an index.scip file.

    Returns
    -------
    tuple[Mapping[str, TableLike], dict[str, object]]
        Mapping of table names to tables plus metadata.
    """
    context = ScipExtractContext(
        scip_index_path=scip_index_path,
        repo_root=repo_root,
        runtime_spec=runtime_profile_spec,
    )
    tables = extract_scip_tables(context=context, options=scip_extract_options, prefer_reader=False)
    metadata: dict[str, object] = {
        "scip_index_path": scip_index_path,
        "tables": sorted(tables.keys()),
        "row_counts": {name: _rows(table) for name, table in tables.items()},
    }
    return tables, metadata


@tag(layer="inputs", artifact="scip_tables_empty", kind="catalog")
def empty_scip_tables() -> Mapping[str, TableLike]:
    """Return an empty SCIP table mapping for repo-only runs.

    Returns
    -------
    Mapping[str, TableLike]
        Empty mapping placeholder.
    """
    return {}


@tag(layer="inputs", artifact="source_catalog_inputs", kind="catalog")
@resolve_from_config(
    decorate_with=lambda source_catalog_mode="full": inject(
        scip_tables=source("empty_scip_tables")
        if source_catalog_mode == "repo_only"
        else source("scip_tables")
    )
)
def source_catalog_inputs(
    repo_files: TableLike,
    scip_tables: Mapping[str, TableLike],
) -> Mapping[str, TableLike]:
    """Bundle source tables for plan compilation.

    Returns
    -------
    Mapping[str, TableLike]
        Mapping of source names to table-like inputs.
    """
    sources: dict[str, TableLike] = {"repo_files": repo_files}
    sources.update(scip_tables)
    return sources


__all__ = [
    "empty_scip_tables",
    "repo_files",
    "scip_index_path",
    "scip_tables",
    "source_catalog_inputs",
]
