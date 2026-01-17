"""Canonical pipeline execution entry points."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import cast

from hamilton import driver as hamilton_driver
from hamilton.graph_types import HamiltonNode

from arrowdsl.core.context import ExecutionContext
from config import AdapterMode
from core_types import JsonDict, JsonValue, PathLike, ensure_path
from hamilton_pipeline.driver_factory import build_driver
from hamilton_pipeline.pipeline_types import ScipIdentityOverrides, ScipIndexConfig
from incremental.types import IncrementalConfig

type PipelineFinalVar = str | HamiltonNode | Callable[..., object]

FULL_PIPELINE_OUTPUTS: tuple[str, ...] = (
    "write_cpg_nodes_parquet",
    "write_cpg_nodes_quality_parquet",
    "write_cpg_edges_parquet",
    "write_cpg_props_parquet",
    "write_cpg_props_quality_parquet",
    "write_extract_error_artifacts_parquet",
    "write_run_manifest_json",
    "write_run_bundle_dir",
)


@dataclass(frozen=True)
class PipelineExecutionOptions:
    """Execution options for the full Hamilton pipeline."""

    output_dir: PathLike | None = None
    work_dir: PathLike | None = None
    scip_index_config: ScipIndexConfig | None = None
    scip_identity_overrides: ScipIdentityOverrides | None = None
    adapter_mode: AdapterMode | None = None
    ctx: ExecutionContext | None = None
    incremental_config: IncrementalConfig | None = None
    outputs: Sequence[str] | None = None
    config: Mapping[str, JsonValue] = field(default_factory=dict)
    pipeline_driver: hamilton_driver.Driver | None = None
    overrides: Mapping[str, object] | None = None


def _resolve_dir(repo_root: Path, value: PathLike | None) -> Path | None:
    if value is None:
        return None
    if isinstance(value, str) and not value:
        return None
    path = ensure_path(value)
    return path if path.is_absolute() else repo_root / path


def _default_output_dir(repo_root: Path, output_dir: PathLike | None) -> Path:
    resolved = _resolve_dir(repo_root, output_dir)
    return resolved if resolved is not None else repo_root / "build"


def _default_state_dir(repo_root: Path, value: PathLike | None) -> Path:
    resolved = _resolve_dir(repo_root, value)
    return resolved if resolved is not None else repo_root / "build" / "state"


def execute_pipeline(
    *,
    repo_root: PathLike,
    options: PipelineExecutionOptions | None = None,
) -> Mapping[str, JsonDict | None]:
    """Execute the full Hamilton pipeline for a repository.

    Returns
    -------
    Mapping[str, JsonDict | None]
        Mapping of output node names to their emitted metadata.
    """
    repo_root_path = ensure_path(repo_root).resolve()
    options = options or PipelineExecutionOptions()
    resolved_output_dir = _default_output_dir(repo_root_path, options.output_dir)
    resolved_work_dir = _resolve_dir(repo_root_path, options.work_dir)
    execute_overrides: dict[str, object] = {
        "output_dir": str(resolved_output_dir),
    }
    if resolved_work_dir is not None:
        execute_overrides["work_dir"] = str(resolved_work_dir)
    if options.scip_index_config is not None:
        execute_overrides["scip_index_config"] = options.scip_index_config
    if options.scip_identity_overrides is not None:
        execute_overrides["scip_identity_overrides"] = options.scip_identity_overrides
    if options.adapter_mode is not None:
        execute_overrides["adapter_mode"] = options.adapter_mode
    if options.ctx is not None:
        execute_overrides["ctx"] = options.ctx
    if options.incremental_config is not None:
        incremental = options.incremental_config
        if incremental.enabled and incremental.state_dir is None:
            incremental = replace(incremental, state_dir=_default_state_dir(repo_root_path, None))
        execute_overrides["incremental_config"] = incremental
    if options.overrides:
        execute_overrides.update(options.overrides)

    driver_instance = (
        options.pipeline_driver
        if options.pipeline_driver is not None
        else build_driver(config=options.config)
    )
    output_nodes = cast(
        "list[PipelineFinalVar]",
        list(options.outputs or FULL_PIPELINE_OUTPUTS),
    )
    results = driver_instance.execute(
        output_nodes,
        inputs={"repo_root": str(repo_root_path)},
        overrides=execute_overrides,
    )
    return cast("Mapping[str, JsonDict | None]", results)


__all__ = ["FULL_PIPELINE_OUTPUTS", "PipelineExecutionOptions", "execute_pipeline"]
