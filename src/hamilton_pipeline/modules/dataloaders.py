"""Hamilton dataloaders for repo scan and SCIP sources."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

from hamilton.function_modifiers import inject, resolve_from_config, source

from extract.extractors.scip.extract import run_scip_python_index
from extract.extractors.scip.identity import resolve_scip_identity
from extract.extractors.scip.setup import (
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

if TYPE_CHECKING:
    from collections.abc import Mapping

    from datafusion_engine.arrow_interop import TableLike
    from hamilton_pipeline.types import ScipIdentityOverrides, ScipIndexConfig


@apply_tag(SCIP_INDEX_PATH.tag_policy())
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


@apply_tag(SCIP_TABLES_EMPTY.tag_policy())
def empty_scip_tables() -> Mapping[str, TableLike]:
    """Return an empty SCIP table mapping for repo-only runs.

    Returns
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
    "scip_index_path",
    "source_catalog_inputs",
]
