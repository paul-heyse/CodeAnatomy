"""SCIP extraction configuration types.

Standalone configuration types for SCIP indexing, decoupled from the Hamilton pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScipIndexSettings:
    """Settings for scip-python indexing."""

    enabled: bool = True
    index_path_override: str | None = None
    output_dir: str = "build/scip"
    env_json_path: str | None = None
    generate_env_json: bool = False
    scip_python_bin: str = "scip-python"
    scip_cli_bin: str = "scip"
    target_only: str | None = None
    node_max_old_space_mb: int | None = 8192
    timeout_s: int | None = None
    extra_args: tuple[str, ...] = ()
    use_incremental_shards: bool = False
    shards_dir: str | None = None
    shards_manifest_path: str | None = None
    run_scip_print: bool = False
    scip_print_path: str | None = None
    run_scip_snapshot: bool = False
    scip_snapshot_dir: str | None = None
    scip_snapshot_comment_syntax: str = "#"
    run_scip_test: bool = False
    scip_test_args: tuple[str, ...] = ("--check-documents",)


ScipIndexConfig = ScipIndexSettings


@dataclass(frozen=True)
class ScipIdentityOverrides:
    """Optional overrides for SCIP project identity."""

    project_name_override: str | None
    project_version_override: str | None
    project_namespace_override: str | None


__all__ = [
    "ScipIdentityOverrides",
    "ScipIndexConfig",
    "ScipIndexSettings",
]
