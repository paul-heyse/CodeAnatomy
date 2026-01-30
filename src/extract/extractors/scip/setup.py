"""SCIP build and initialization utilities.

This module combines SCIP protobuf loading and indexer setup helpers
for configuring scip-python invocations within the extraction pipeline.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING

from extract.extractors.scip.identity import ScipIdentity

if TYPE_CHECKING:
    from extract.extractors.scip.extract import SCIPIndexOptions
    from hamilton_pipeline.types import ScipIndexConfig


# ---------------------------------------------------------------------------
# Protobuf loader helpers (from scip_proto_loader.py)
# ---------------------------------------------------------------------------


class ScipProtoLoadError(RuntimeError):
    """Raised when scip_pb2 module spec cannot be loaded."""


def load_scip_pb2_from_build(build_dir: Path) -> ModuleType:
    """Load the scip_pb2 module from the build/scip directory.

    Parameters
    ----------
    build_dir:
        Path to the build/scip directory.

    Returns
    -------
    types.ModuleType
        Loaded scip_pb2 module.

    Raises
    ------
    FileNotFoundError
        Raised when scip_pb2.py is missing.
    ScipProtoLoadError
        Raised when the module spec cannot be loaded.
    """
    module_path = build_dir / "scip_pb2.py"
    if not module_path.exists():
        raise FileNotFoundError(module_path)

    spec = importlib.util.spec_from_file_location("scip_pb2", module_path)
    if spec is None or spec.loader is None:
        raise ScipProtoLoadError

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def ensure_scip_pb2(repo_root: Path, build_dir: Path) -> Path:
    """Ensure scip_pb2.py exists under build/scip, generating it if missing.

    Parameters
    ----------
    repo_root:
        Repository root path.
    build_dir:
        Build/scip directory path.

    Returns
    -------
    pathlib.Path
        Path to the scip_pb2.py file.

    Raises
    ------
    FileNotFoundError
        Raised when the codegen script or generated module is missing.
    """
    module_path = build_dir / "scip_pb2.py"
    if module_path.exists():
        return module_path

    script = repo_root / "scripts" / "scip_proto_codegen.py"
    if not script.exists():
        raise FileNotFoundError(script)

    subprocess.run(
        [
            sys.executable,
            str(script),
            "--repo-root",
            str(repo_root),
            "--build-subdir",
            str(build_dir),
        ],
        check=True,
    )
    if not module_path.exists():
        raise FileNotFoundError(module_path)
    return module_path


# ---------------------------------------------------------------------------
# Indexer setup helpers (from scip_indexer.py)
# ---------------------------------------------------------------------------


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
    # Import locally to avoid circular dependency
    from extract.extractors.scip.extract import SCIPIndexOptions

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


__all__ = [
    "BUILD_SUBDIR",
    "ScipIndexPaths",
    "ScipProtoLoadError",
    "build_scip_index_options",
    "ensure_scip_build_dir",
    "ensure_scip_pb2",
    "load_scip_pb2_from_build",
    "resolve_scip_paths",
    "scip_environment_payload",
    "write_scip_environment_json",
]
