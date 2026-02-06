"""Hamilton dataloaders for repo scan and SCIP sources."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path

from hamilton.function_modifiers import inject, resolve_from_config, source

from datafusion_engine.arrow.interop import TableLike
from extract.extractors.scip.extract import SCIPIndexOptions, run_scip_python_index
from extract.extractors.scip.identity import ScipIdentity, resolve_scip_identity
from extract.extractors.scip.setup import (
    ScipIndexPaths,
    build_scip_index_options,
    resolve_scip_paths,
    write_scip_environment_json,
)
from hamilton_pipeline.io_contracts import (
    SCIP_INDEX_PATH,
    SCIP_TABLES_EMPTY,
    SOURCE_CATALOG_INPUTS,
)
from hamilton_pipeline.tag_policy import apply_tag
from hamilton_pipeline.types import ScipIdentityOverrides, ScipIndexConfig


def _resolve_scip_paths_with_diagnostics(
    repo_root_path: Path,
    scip_index_config: ScipIndexConfig,
    record_issue: Callable[..., None],
) -> ScipIndexPaths | None:
    try:
        return resolve_scip_paths(
            repo_root_path,
            scip_index_config.output_dir,
            scip_index_config.index_path_override,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        record_issue(
            status="index_path_failed",
            issue=f"{type(exc).__name__}: {exc}",
        )
        return None


def _resolve_scip_identity_with_diagnostics(
    repo_root_path: Path,
    scip_identity_overrides: ScipIdentityOverrides,
    record_issue: Callable[..., None],
) -> ScipIdentity | None:
    try:
        return resolve_scip_identity(
            repo_root_path,
            project_name_override=scip_identity_overrides.project_name_override,
            project_version_override=scip_identity_overrides.project_version_override,
            project_namespace_override=scip_identity_overrides.project_namespace_override,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        record_issue(
            status="identity_failed",
            issue=f"{type(exc).__name__}: {exc}",
        )
        return None


def _maybe_write_env_json(
    repo_root_path: Path,
    scip_index_config: ScipIndexConfig,
    paths: ScipIndexPaths,
    record_issue: Callable[..., None],
) -> ScipIndexConfig:
    config = scip_index_config
    if scip_index_config.generate_env_json and scip_index_config.env_json_path:
        env_path = Path(scip_index_config.env_json_path)
        resolved = env_path if env_path.is_absolute() else repo_root_path / env_path
        try:
            write_scip_environment_json(resolved)
        except (OSError, RuntimeError, ValueError) as exc:
            record_issue(
                status="env_json_failed",
                issue=f"{type(exc).__name__}: {exc}",
                path=resolved,
            )
    elif scip_index_config.generate_env_json and scip_index_config.env_json_path is None:
        env_path = paths.build_dir / "environment.json"
        try:
            write_scip_environment_json(env_path)
        except (OSError, RuntimeError, ValueError) as exc:
            record_issue(
                status="env_json_failed",
                issue=f"{type(exc).__name__}: {exc}",
                path=env_path,
            )
        else:
            config = replace(scip_index_config, env_json_path=str(env_path))
    return config


def _build_scip_index_options_with_diagnostics(
    repo_root_path: Path,
    identity: ScipIdentity,
    scip_index_config: ScipIndexConfig,
    record_issue: Callable[..., None],
) -> SCIPIndexOptions | None:
    try:
        return build_scip_index_options(
            repo_root=repo_root_path,
            identity=identity,
            config=scip_index_config,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        record_issue(
            status="index_config_failed",
            issue=f"{type(exc).__name__}: {exc}",
        )
        return None


def _ensure_scip_output_path(
    options: SCIPIndexOptions,
    record_issue: Callable[..., None],
) -> Path | None:
    output_path = options.output_path
    if output_path is None:
        record_issue(
            status="index_path_failed",
            issue="SCIP index output path must be resolved for indexing.",
        )
        return None
    if not output_path.exists():
        try:
            run_scip_python_index(options)
        except (OSError, RuntimeError, ValueError) as exc:
            record_issue(
                status="index_failed",
                issue=f"{type(exc).__name__}: {exc}",
                path=output_path,
            )
            return None
    if not output_path.exists():
        record_issue(
            status="index_missing",
            issue="SCIP index output path missing after indexing.",
            path=output_path,
        )
        return None
    return output_path


@apply_tag(SCIP_INDEX_PATH.tag_policy())
def scip_index_path(
    repo_root: str,
    scip_index_config: ScipIndexConfig,
    scip_identity_overrides: ScipIdentityOverrides,
) -> str | None:
    """Return the resolved path to index.scip, running scip-python when needed.

    Returns:
    -------
    str | None
        Resolved index.scip path when available.

    Notes:
    -----
    Indexing failures are recorded as diagnostics events and return ``None``
    so builds can proceed with clear extraction quality signals.
    """
    from engine.diagnostics import ExtractQualityEvent
    from hamilton_pipeline.lifecycle import get_hamilton_diagnostics_collector

    collector = get_hamilton_diagnostics_collector()

    def _record_issue(*, status: str, issue: str, path: Path | None = None) -> None:
        if collector is None:
            return
        event = ExtractQualityEvent(
            dataset="scip_index_v1",
            stage="index",
            status=status,
            rows=None,
            location_path=str(path) if path is not None else None,
            location_format="scip",
            issue=issue,
            extractor="scip_tables",
        )
        collector.record_events("extract_quality_v1", [event.to_payload()])

    repo_root_path = Path(repo_root)
    if not scip_index_config.enabled and not scip_index_config.index_path_override:
        _record_issue(status="disabled", issue="SCIP indexing disabled.")
        return None
    paths = _resolve_scip_paths_with_diagnostics(
        repo_root_path,
        scip_index_config,
        _record_issue,
    )
    if paths is None:
        return None
    if not scip_index_config.enabled:
        return str(paths.index_path)
    identity = _resolve_scip_identity_with_diagnostics(
        repo_root_path,
        scip_identity_overrides,
        _record_issue,
    )
    if identity is None:
        return None
    config = _maybe_write_env_json(
        repo_root_path,
        scip_index_config,
        paths,
        _record_issue,
    )
    options = _build_scip_index_options_with_diagnostics(
        repo_root_path,
        identity,
        config,
        _record_issue,
    )
    if options is None:
        return None
    output_path = _ensure_scip_output_path(options, _record_issue)
    return str(output_path) if output_path is not None else None


@apply_tag(SCIP_TABLES_EMPTY.tag_policy())
def empty_scip_tables() -> Mapping[str, TableLike]:
    """Return an empty SCIP table mapping for repo-only runs.

    Returns:
    -------
    Mapping[str, TableLike]
        Empty mapping placeholder.
    """
    return {}


@apply_tag(SOURCE_CATALOG_INPUTS.tag_policy())
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

    Returns:
    -------
    Mapping[str, TableLike]
        Mapping of source names to table-like inputs.
    """
    sources: dict[str, TableLike] = {"repo_files": repo_files}
    sources.update(scip_tables)
    return sources


__all__ = [
    "empty_scip_tables",
    "scip_index_path",
    "source_catalog_inputs",
]
