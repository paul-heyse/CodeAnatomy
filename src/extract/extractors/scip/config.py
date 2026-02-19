"""SCIP extraction configuration types.

Standalone configuration types for SCIP indexing, decoupled from the Hamilton pipeline.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast


def _coerce_bool(value: object, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    return default


def _coerce_optional_str(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _coerce_str(value: object, *, default: str) -> str:
    if isinstance(value, str) and value:
        return value
    return default


def _coerce_optional_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _coerce_tuple_str(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence):
        return tuple(str(item) for item in value)
    return ()


def _resolve_path(repo_root: Path, value: str | Path | None) -> str | None:
    if value is None:
        return None
    path = Path(value)
    if not path.is_absolute():
        path = repo_root / path
    return str(path)


@dataclass(frozen=True)
class ScipCliOverrides:
    """CLI-provided overrides for SCIP index settings."""

    disable_scip: bool
    scip_output_dir: str | Path | None
    scip_index_path_override: str | Path | None
    scip_env_json: str | Path | None
    scip_python_bin: str
    default_scip_python: str = "scip-python"
    scip_target_only: str | None = None
    scip_timeout_s: int | None = None
    node_max_old_space_mb: int | None = None
    scip_extra_args: tuple[str, ...] = ()


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

    @classmethod
    def from_cli_overrides(
        cls,
        config_contents: Mapping[str, object],
        *,
        repo_root: Path,
        overrides: ScipCliOverrides,
    ) -> ScipIndexSettings:
        """Build SCIP settings by merging config payload and CLI overrides.

        Returns:
        -------
        ScipIndexSettings
            Normalized SCIP index settings after applying CLI overrides.
        """
        defaults = cls()
        raw_scip = config_contents.get("scip")
        payload: Mapping[str, object] = (
            cast("Mapping[str, object]", raw_scip) if isinstance(raw_scip, Mapping) else {}
        )
        extra_args = _coerce_tuple_str(payload.get("extra_args")) or defaults.extra_args
        scip_test_args = _coerce_tuple_str(payload.get("scip_test_args")) or defaults.scip_test_args
        payload_node_max_old_space_mb = _coerce_optional_int(payload.get("node_max_old_space_mb"))
        payload_timeout_s = _coerce_optional_int(payload.get("timeout_s"))
        enabled = _coerce_bool(payload.get("enabled"), default=defaults.enabled)
        if overrides.disable_scip:
            enabled = False

        resolved_output = _resolve_path(repo_root, overrides.scip_output_dir)
        resolved_index = _resolve_path(repo_root, overrides.scip_index_path_override)
        resolved_env = _resolve_path(repo_root, overrides.scip_env_json)

        resolved_scip_python = _coerce_str(
            payload.get("scip_python_bin"),
            default=defaults.scip_python_bin,
        )
        if overrides.scip_python_bin != overrides.default_scip_python:
            resolved_scip_python = overrides.scip_python_bin

        return cls(
            enabled=enabled,
            index_path_override=resolved_index
            or _coerce_optional_str(payload.get("index_path_override")),
            output_dir=resolved_output
            or _coerce_str(payload.get("output_dir"), default=defaults.output_dir),
            env_json_path=resolved_env or _coerce_optional_str(payload.get("env_json_path")),
            generate_env_json=_coerce_bool(
                payload.get("generate_env_json"),
                default=defaults.generate_env_json,
            ),
            scip_python_bin=resolved_scip_python,
            scip_cli_bin=_coerce_str(
                payload.get("scip_cli_bin"),
                default=defaults.scip_cli_bin,
            ),
            target_only=overrides.scip_target_only
            or _coerce_optional_str(payload.get("target_only"))
            or defaults.target_only,
            node_max_old_space_mb=(
                overrides.node_max_old_space_mb
                if overrides.node_max_old_space_mb is not None
                else (
                    payload_node_max_old_space_mb
                    if payload_node_max_old_space_mb is not None
                    else defaults.node_max_old_space_mb
                )
            ),
            timeout_s=(
                overrides.scip_timeout_s
                if overrides.scip_timeout_s is not None
                else payload_timeout_s
                if payload_timeout_s is not None
                else defaults.timeout_s
            ),
            extra_args=overrides.scip_extra_args or extra_args,
            use_incremental_shards=_coerce_bool(
                payload.get("use_incremental_shards"),
                default=defaults.use_incremental_shards,
            ),
            shards_dir=_coerce_optional_str(payload.get("shards_dir")) or defaults.shards_dir,
            shards_manifest_path=(
                _coerce_optional_str(payload.get("shards_manifest_path"))
                or defaults.shards_manifest_path
            ),
            run_scip_print=_coerce_bool(
                payload.get("run_scip_print"),
                default=defaults.run_scip_print,
            ),
            scip_print_path=(
                _coerce_optional_str(payload.get("scip_print_path")) or defaults.scip_print_path
            ),
            run_scip_snapshot=_coerce_bool(
                payload.get("run_scip_snapshot"),
                default=defaults.run_scip_snapshot,
            ),
            scip_snapshot_dir=(
                _coerce_optional_str(payload.get("scip_snapshot_dir")) or defaults.scip_snapshot_dir
            ),
            scip_snapshot_comment_syntax=_coerce_str(
                payload.get("scip_snapshot_comment_syntax"),
                default=defaults.scip_snapshot_comment_syntax,
            ),
            run_scip_test=_coerce_bool(
                payload.get("run_scip_test"),
                default=defaults.run_scip_test,
            ),
            scip_test_args=scip_test_args,
        )


ScipIndexConfig = ScipIndexSettings


@dataclass(frozen=True)
class ScipIdentityOverrides:
    """Optional overrides for SCIP project identity."""

    project_name_override: str | None
    project_version_override: str | None
    project_namespace_override: str | None


__all__ = [
    "ScipCliOverrides",
    "ScipIdentityOverrides",
    "ScipIndexConfig",
    "ScipIndexSettings",
]
