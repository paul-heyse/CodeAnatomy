"""Canonical pipeline execution entry points."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Literal, cast
from uuid import uuid4

from hamilton import async_driver as hamilton_async_driver
from hamilton import driver as hamilton_driver
from hamilton.graph_types import HamiltonNode
from opentelemetry import context as otel_context

from core_types import JsonDict, JsonValue, PathLike, ensure_path
from hamilton_pipeline.driver_factory import (
    DriverBuildRequest,
    build_async_driver,
    build_driver,
)
from hamilton_pipeline.materializers import build_hamilton_materializers
from hamilton_pipeline.pipeline_types import (
    ExecutionMode,
    ExecutorConfig,
    GraphAdapterConfig,
    ScipIdentityOverrides,
    ScipIndexConfig,
)
from incremental.types import IncrementalConfig
from obs.otel.run_context import reset_run_id, set_run_id
from obs.otel.scopes import SCOPE_PIPELINE
from obs.otel.tracing import get_tracer, record_exception, set_span_attributes, span_attributes

type PipelineFinalVar = str | HamiltonNode | Callable[..., object]

FULL_PIPELINE_OUTPUTS: tuple[str, ...] = (
    "write_cpg_nodes_delta",
    "write_cpg_nodes_quality_delta",
    "write_cpg_edges_delta",
    "write_cpg_props_delta",
    "write_cpg_props_map_delta",
    "write_cpg_edges_by_src_delta",
    "write_cpg_edges_by_dst_delta",
    "write_cpg_props_quality_delta",
    "write_normalize_outputs_delta",
    "write_extract_error_artifacts_delta",
    "write_run_manifest_delta",
    "write_run_bundle_dir",
)

_IMPACT_STRATEGIES: frozenset[str] = frozenset({"hybrid", "symbol_closure", "import_closure"})
ImpactStrategy = Literal["hybrid", "symbol_closure", "import_closure"]


@dataclass(frozen=True)
class PipelineExecutionOptions:
    """Execution options for the full Hamilton pipeline."""

    output_dir: PathLike | None = None
    work_dir: PathLike | None = None
    scip_index_config: ScipIndexConfig | None = None
    scip_identity_overrides: ScipIdentityOverrides | None = None
    incremental_config: IncrementalConfig | None = None
    incremental_impact_strategy: str | None = None
    execution_mode: ExecutionMode = ExecutionMode.PLAN_PARALLEL
    executor_config: ExecutorConfig | None = None
    graph_adapter_config: GraphAdapterConfig | None = None
    outputs: Sequence[str] | None = None
    config: Mapping[str, JsonValue] = field(default_factory=dict)
    pipeline_driver: hamilton_driver.Driver | hamilton_async_driver.AsyncDriver | None = None
    overrides: Mapping[str, object] | None = None
    use_materialize: bool = True


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


def _normalize_impact_strategy(impact_strategy: str | None) -> ImpactStrategy | None:
    if impact_strategy is None:
        return None
    normalized = impact_strategy.lower()
    if normalized not in _IMPACT_STRATEGIES:
        msg = f"Unsupported incremental impact strategy {normalized!r}"
        raise ValueError(msg)
    return cast("ImpactStrategy", normalized)


def _apply_incremental_overrides(
    execute_overrides: dict[str, object],
    *,
    options: PipelineExecutionOptions,
    repo_root_path: Path,
) -> None:
    impact_strategy = _normalize_impact_strategy(options.incremental_impact_strategy)
    if options.incremental_config is not None:
        incremental = options.incremental_config
        if impact_strategy is not None:
            incremental = replace(incremental, impact_strategy=impact_strategy)
        if incremental.enabled and incremental.state_dir is None:
            incremental = replace(incremental, state_dir=_default_state_dir(repo_root_path, None))
        execute_overrides["incremental_config"] = incremental
        return
    if impact_strategy is not None:
        execute_overrides["incremental_impact_strategy"] = impact_strategy


def _apply_cache_overrides(
    execute_overrides: dict[str, object],
    *,
    options: PipelineExecutionOptions,
) -> None:
    config = options.config
    cache_path_value = config.get("cache_path")
    if not isinstance(cache_path_value, str) or not cache_path_value.strip():
        cache_path_value = config.get("hamilton_cache_path")
    if isinstance(cache_path_value, str) and cache_path_value.strip():
        execute_overrides.setdefault("cache_path", cache_path_value.strip())
    cache_log_value = config.get("cache_log_to_file")
    if isinstance(cache_log_value, bool):
        execute_overrides.setdefault("cache_log_to_file", cache_log_value)
    cache_policy_value = config.get("cache_policy_profile")
    if isinstance(cache_policy_value, str) and cache_policy_value.strip():
        execute_overrides.setdefault("cache_policy_profile", cache_policy_value.strip())


def _build_execute_overrides(
    *,
    repo_root_path: Path,
    options: PipelineExecutionOptions,
) -> dict[str, object]:
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
    _apply_incremental_overrides(
        execute_overrides,
        options=options,
        repo_root_path=repo_root_path,
    )
    _apply_cache_overrides(execute_overrides, options=options)
    if options.overrides:
        execute_overrides.update(options.overrides)
    return execute_overrides


def _output_names(nodes: Sequence[PipelineFinalVar]) -> tuple[str, ...]:
    names: list[str] = []
    for node in nodes:
        if isinstance(node, str):
            names.append(node)
            continue
        if isinstance(node, HamiltonNode):
            names.append(node.name)
            continue
        name = getattr(node, "__name__", None)
        if isinstance(name, str) and name:
            names.append(name)
    return tuple(names)


def _resolve_run_id(execute_overrides: dict[str, object]) -> str:
    value = execute_overrides.get("run_id")
    if isinstance(value, str) and value:
        return value
    run_id = str(uuid4())
    execute_overrides["run_id"] = run_id
    return run_id


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
    execute_overrides = _build_execute_overrides(
        repo_root_path=repo_root_path,
        options=options,
    )
    run_id = _resolve_run_id(execute_overrides)
    driver_instance = _resolve_driver_instance(options)
    (
        output_names,
        execution_outputs,
        materializer_ids,
        materialized_outputs,
    ) = _resolve_execution_outputs(options)
    execute_overrides.setdefault("materialized_outputs", materialized_outputs)
    tracer = get_tracer(SCOPE_PIPELINE)
    run_token = set_run_id(run_id)
    try:
        with tracer.start_as_current_span(
            "pipeline.execute",
            attributes=span_attributes(
                attrs={
                    "codeanatomy.execution_mode": options.execution_mode.value,
                    "codeanatomy.output_count": len(output_names),
                    "codeanatomy.outputs": list(output_names),
                }
            ),
        ) as span:
            try:
                if options.use_materialize:
                    _materialized, results = driver_instance.materialize(
                        additional_vars=execution_outputs,
                        inputs={"repo_root": str(repo_root_path)},
                        overrides=execute_overrides,
                    )
                elif isinstance(driver_instance, hamilton_async_driver.AsyncDriver):
                    async_output_names = _execution_output_names(
                        output_names,
                        materializer_ids,
                    )
                    results = driver_instance.execute(
                        async_output_names,
                        inputs={"repo_root": str(repo_root_path)},
                        overrides=execute_overrides,
                    )
                else:
                    results = driver_instance.execute(
                        execution_outputs,
                        inputs={"repo_root": str(repo_root_path)},
                        overrides=execute_overrides,
                    )
            except Exception as exc:
                record_exception(span, exc)
                raise
            set_span_attributes(span, {"codeanatomy.repo_root": str(repo_root_path)})
            results_map = cast("Mapping[str, JsonDict | None]", results)
            return {name: results_map.get(name) for name in output_names}
    finally:
        reset_run_id(run_token)


def _resolve_driver_instance(
    options: PipelineExecutionOptions,
) -> hamilton_driver.Driver | hamilton_async_driver.AsyncDriver:
    if options.pipeline_driver is not None:
        return options.pipeline_driver
    return build_driver(
        request=DriverBuildRequest(
            config=options.config,
            execution_mode=options.execution_mode,
            executor_config=options.executor_config,
            graph_adapter_config=options.graph_adapter_config,
        )
    )


def _resolve_execution_outputs(
    options: PipelineExecutionOptions,
) -> tuple[
    list[str],
    list[PipelineFinalVar | str],
    list[str],
    tuple[str, ...],
]:
    output_nodes = cast(
        "list[PipelineFinalVar]",
        list(options.outputs or FULL_PIPELINE_OUTPUTS),
    )
    materializer_ids = [materializer.id for materializer in build_hamilton_materializers()]
    output_names = list(_output_names(output_nodes))
    execution_outputs = list(output_nodes) + materializer_ids
    materialized_outputs = tuple(dict.fromkeys((*output_names, *materializer_ids)))
    return output_names, execution_outputs, materializer_ids, materialized_outputs


def _execution_output_names(
    output_names: list[str],
    materializer_ids: list[str],
) -> list[str]:
    return list(dict.fromkeys((*output_names, *materializer_ids)))


async def _resolve_async_driver_instance(
    options: PipelineExecutionOptions,
) -> hamilton_async_driver.AsyncDriver:
    driver_instance = (
        options.pipeline_driver
        if options.pipeline_driver is not None
        else await build_async_driver(
            request=DriverBuildRequest(
                config=options.config,
                execution_mode=options.execution_mode,
                executor_config=options.executor_config,
                graph_adapter_config=options.graph_adapter_config,
            )
        )
    )
    if not isinstance(driver_instance, hamilton_async_driver.AsyncDriver):
        msg = "Async pipeline execution requires an async Hamilton driver."
        raise TypeError(msg)
    return driver_instance


async def execute_pipeline_async(
    *,
    repo_root: PathLike,
    options: PipelineExecutionOptions | None = None,
) -> Mapping[str, JsonDict | None]:
    """Execute the pipeline using the async Hamilton driver.

    Returns
    -------
    Mapping[str, JsonDict | None]
        Mapping of output node names to payloads.

    Raises
    ------
    ValueError
        Raised when materialize() is requested for async execution.
    """
    repo_root_path = ensure_path(repo_root).resolve()
    options = options or PipelineExecutionOptions()
    if options.use_materialize:
        msg = "Async driver does not support materialize(); use execute() instead."
        raise ValueError(msg)
    execute_overrides = _build_execute_overrides(
        repo_root_path=repo_root_path,
        options=options,
    )
    (
        output_names,
        execution_outputs,
        materializer_ids,
        materialized_outputs,
    ) = _resolve_execution_outputs(options)
    execute_overrides.setdefault("materialized_outputs", materialized_outputs)
    driver_instance = await _resolve_async_driver_instance(options)
    tracer = get_tracer(SCOPE_PIPELINE)
    run_id = _resolve_run_id(execute_overrides)
    run_token = set_run_id(run_id)
    try:
        with tracer.start_as_current_span(
            "pipeline.execute",
            attributes=span_attributes(
                attrs={
                    "codeanatomy.execution_mode": options.execution_mode.value,
                    "codeanatomy.output_count": len(output_names),
                    "codeanatomy.outputs": list(output_names),
                }
            ),
        ) as span:
            try:
                current_context = otel_context.get_current()
                token = otel_context.attach(current_context)
                try:
                    async_output_names = _execution_output_names(
                        output_names,
                        materializer_ids,
                    )
                    result = await driver_instance.execute(
                        async_output_names,
                        inputs={"repo_root": str(repo_root_path)},
                        overrides=execute_overrides,
                    )
                finally:
                    otel_context.detach(token)
            except Exception as exc:
                record_exception(span, exc)
                raise
            set_span_attributes(span, {"codeanatomy.repo_root": str(repo_root_path)})
            results_map = cast("Mapping[str, JsonDict | None]", result)
            return {name: results_map.get(name) for name in output_names}
    finally:
        reset_run_id(run_token)


__all__ = [
    "FULL_PIPELINE_OUTPUTS",
    "PipelineExecutionOptions",
    "execute_pipeline",
    "execute_pipeline_async",
]
