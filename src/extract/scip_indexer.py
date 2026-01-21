"""Helpers for invoking scip-python inside the pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import TYPE_CHECKING

from extract.scip_extract import SCIPIndexOptions
from extract.scip_identity import ScipIdentity

if TYPE_CHECKING:
    from hamilton_pipeline.pipeline_types import ScipIndexConfig

BUILD_SUBDIR = Path("build") / "scip"


@dataclass(frozen=True)
class ScipIndexPaths:
    """Resolved paths for SCIP indexing artifacts."""

    build_dir: Path
    index_path: Path


def ensure_scip_build_dir(repo_root: Path, output_dir: str) -> Path:
    """Ensure the build/scip directory exists.

    Parameters
    ----------
    repo_root:
        Repository root path.
    output_dir:
        Output directory path (relative to repo_root unless absolute).

    Returns
    -------
    pathlib.Path
        Resolved build directory path.
    """
    candidate = Path(output_dir)
    build_dir = candidate if candidate.is_absolute() else repo_root / candidate
    build_dir.mkdir(parents=True, exist_ok=True)
    return build_dir.resolve()


def resolve_scip_paths(
    repo_root: Path, output_dir: str, index_path_override: str | None
) -> ScipIndexPaths:
    """Resolve build/scip paths for indexing.

    Parameters
    ----------
    repo_root:
        Repository root path.
    output_dir:
        Output directory path.
    index_path_override:
        Optional override for index.scip path.

    Returns
    -------
    ScipIndexPaths
        Resolved build directory and index path.
    """
    build_dir = ensure_scip_build_dir(repo_root, output_dir)
    if index_path_override:
        override = Path(index_path_override)
        index_path = override if override.is_absolute() else repo_root / override
    else:
        index_path = build_dir / "index.scip"
    return ScipIndexPaths(build_dir=build_dir, index_path=index_path.resolve())


def scip_environment_payload() -> list[dict[str, object]]:
    """Return a scip-python environment payload for installed distributions.

    Returns
    -------
    list[dict[str, object]]
        Environment payload entries with package names, versions, and files.
    """
    env: list[dict[str, object]] = []
    for dist in metadata.distributions():
        name = dist.metadata.get("Name") or dist.name
        files = sorted({str(path) for path in (dist.files or ())})
        env.append(
            {
                "name": str(name),
                "version": str(dist.version),
                "files": files,
            }
        )
    env.sort(key=lambda entry: str(entry["name"]).lower())
    return env


def write_scip_environment_json(path: Path) -> Path:
    """Write a scip-python environment JSON manifest to disk.

    Parameters
    ----------
    path:
        Output path for the environment JSON file.

    Returns
    -------
    pathlib.Path
        Resolved output path.
    """
    resolved = path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    payload = scip_environment_payload()
    resolved.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return resolved


def build_scip_index_options(
    *,
    repo_root: Path,
    identity: ScipIdentity,
    config: ScipIndexConfig,
) -> SCIPIndexOptions:
    """Build SCIPIndexOptions for scip-python invocation.

    Parameters
    ----------
    repo_root:
        Repository root path.
    identity:
        Resolved SCIP identity values.
    config:
        Indexing configuration.

    Returns
    -------
    SCIPIndexOptions
        Fully populated scip-python invocation options.
    """
    paths = resolve_scip_paths(repo_root, config.output_dir, config.index_path_override)
    env_json = Path(config.env_json_path) if config.env_json_path else None
    extra_args: list[str] = list(config.extra_args)
    if config.use_incremental_shards:
        shards_dir = Path(config.shards_dir) if config.shards_dir else paths.build_dir / "shards"
        extra_args.extend(["--index-shards", str(shards_dir)])
        if config.shards_manifest_path:
            extra_args.extend(["--index-shard-manifest", config.shards_manifest_path])
    return SCIPIndexOptions(
        repo_root=repo_root,
        project_name=identity.project_name,
        project_version=identity.project_version,
        project_namespace=identity.project_namespace,
        output_path=paths.index_path,
        environment_json=env_json,
        target_only=config.target_only,
        scip_python_bin=config.scip_python_bin,
        node_max_old_space_mb=config.node_max_old_space_mb,
        timeout_s=config.timeout_s,
        extra_args=tuple(extra_args),
    )
