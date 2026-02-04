"""Canonical pipeline execution entry points."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Literal, cast

from hamilton import driver as hamilton_driver
from hamilton.graph_types import HamiltonNode

from core_types import JsonDict, JsonValue, PathLike, ensure_path
from hamilton_pipeline.driver_factory import DriverBuildRequest, build_driver
from hamilton_pipeline.materializers import build_hamilton_materializers
from hamilton_pipeline.types import (
    ExecutionMode,
    ExecutorConfig,
    GraphAdapterConfig,
    ScipIdentityOverrides,
    ScipIndexConfig,
)
from obs.otel.logs import emit_diagnostics_event
from obs.otel.run_context import get_run_id, reset_run_id, set_run_id
from obs.otel.scopes import SCOPE_PIPELINE
from obs.otel.tracing import record_exception, set_span_attributes, stage_span
from semantics.incremental import SemanticIncrementalConfig
from utils.uuid_factory import uuid7_str

type PipelineFinalVar = str | HamiltonNode | Callable[..., object]

logger = logging.getLogger(__name__)

FULL_PIPELINE_OUTPUTS: tuple[str, ...] = (
    "write_cpg_nodes_delta",
    "write_cpg_edges_delta",
    "write_cpg_props_delta",
    "write_cpg_props_map_delta",
    "write_cpg_edges_by_src_delta",
    "write_cpg_edges_by_dst_delta",
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
    incremental_config: SemanticIncrementalConfig | None = None
    incremental_impact_strategy: str | None = None
    execution_mode: ExecutionMode = ExecutionMode.PLAN_PARALLEL
    executor_config: ExecutorConfig | None = None
    graph_adapter_config: GraphAdapterConfig | None = None
    outputs: Sequence[str] | None = None
    config: Mapping[str, JsonValue] = field(default_factory=dict)
    pipeline_driver: hamilton_driver.Driver | None = None
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
    run_id = uuid7_str()
    execute_overrides["run_id"] = run_id
    return run_id


def _cache_output_root_for_options(options: PipelineExecutionOptions) -> str | None:
    base_dir = options.output_dir or options.work_dir
    if not base_dir:
        return None
    return str(ensure_path(base_dir) / "cache")


def _runtime_profile_name_from_inputs(
    options: PipelineExecutionOptions,
    execute_overrides: Mapping[str, object],
) -> str:
    for key in ("runtime_profile_name", "runtime_profile_name_override"):
        override_value = execute_overrides.get(key)
        if isinstance(override_value, str) and override_value.strip():
            return override_value.strip()
    config_value = options.config.get("runtime_profile_name")
    if isinstance(config_value, str) and config_value.strip():
        return config_value.strip()
    from utils.env_utils import env_value

    return env_value("CODEANATOMY_RUNTIME_PROFILE") or "default"


def _record_cache_run_summary(
    *,
    run_id: str,
    options: PipelineExecutionOptions,
    execute_overrides: Mapping[str, object],
) -> None:
    from datafusion_engine.cache.ledger import CacheRunSummary, record_cache_run_summary
    from engine.runtime_profile import resolve_runtime_profile
    from obs.otel.cache import drain_cache_run_stats

    stats = drain_cache_run_stats(run_id)
    if stats is None:
        return
    profile_name = _runtime_profile_name_from_inputs(options, execute_overrides)
    profile_spec = resolve_runtime_profile(profile_name)
    profile = profile_spec.datafusion
    cache_root = _cache_output_root_for_options(options)
    if cache_root is not None and profile.policies.cache_output_root is None:
        profile = replace(
            profile,
            policies=replace(
                profile.policies,
                cache_output_root=cache_root,
            ),
        )
    summary = CacheRunSummary(
        run_id=stats.run_id,
        start_time_unix_ms=stats.start_time_unix_ms,
        end_time_unix_ms=stats.end_time_unix_ms,
        cache_root=profile.cache_root(),
        total_writes=stats.write_count,
        total_reads=stats.read_count,
        error_count=stats.error_count,
    )
    record_cache_run_summary(profile, summary=summary)


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
        _materializer_ids,
        materialized_outputs,
    ) = _resolve_execution_outputs(options)
    execute_overrides.setdefault("materialized_outputs", materialized_outputs)
    execution_inputs: dict[str, object] = {}
    if "repo_root" not in options.config:
        execution_inputs["repo_root"] = str(repo_root_path)
    run_token = set_run_id(run_id)
    try:
        with stage_span(
            "pipeline.execute",
            stage="execution",
            scope_name=SCOPE_PIPELINE,
            attributes={
                "codeanatomy.execution_mode": options.execution_mode.value,
                "codeanatomy.output_count": len(output_names),
                "codeanatomy.outputs": list(output_names),
            },
        ) as span:
            try:
                if options.use_materialize:
                    _materialized, results = driver_instance.materialize(
                        additional_vars=execution_outputs,
                        inputs=execution_inputs,
                        overrides=execute_overrides,
                    )
                else:
                    results = driver_instance.execute(
                        execution_outputs,
                        inputs=execution_inputs,
                        overrides=execute_overrides,
                    )
            except Exception as exc:
                record_exception(span, exc)
                raise
            set_span_attributes(span, {"codeanatomy.repo_root": str(repo_root_path)})
            results_map = cast("Mapping[str, object]", results)
            _emit_plan_execution_diff(results_map)
            results_map = cast("Mapping[str, JsonDict | None]", results_map)
            return {name: results_map.get(name) for name in output_names}
    finally:
        try:
            _record_cache_run_summary(
                run_id=run_id,
                options=options,
                execute_overrides=execute_overrides,
            )
        except (RuntimeError, TypeError, ValueError, OSError):
            logger.exception("Cache run summary recording failed.")
        reset_run_id(run_token)


def _resolve_driver_instance(
    options: PipelineExecutionOptions,
) -> hamilton_driver.Driver:
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
    internal_outputs = ("execution_plan", "runtime_artifacts")
    output_nodes = cast(
        "list[PipelineFinalVar]",
        list(options.outputs or FULL_PIPELINE_OUTPUTS),
    )
    materializer_ids = [materializer.id for materializer in build_hamilton_materializers()]
    output_names = list(_output_names(output_nodes))
    execution_outputs = list(output_nodes) + materializer_ids + list(internal_outputs)
    materialized_outputs = tuple(dict.fromkeys((*output_names, *materializer_ids)))
    return output_names, execution_outputs, materializer_ids, materialized_outputs


def _emit_plan_execution_diff(results: Mapping[str, object]) -> None:
    plan = results.get("execution_plan")
    runtime_artifacts = results.get("runtime_artifacts")
    if plan is None or runtime_artifacts is None:
        return
    active_tasks = getattr(plan, "active_tasks", None)
    execution_order = getattr(runtime_artifacts, "execution_order", None)
    if not isinstance(active_tasks, frozenset) or not isinstance(execution_order, list):
        return
    expected = set(active_tasks)
    executed = {_base_task_name(name) for name in execution_order if isinstance(name, str)}
    missing = sorted(expected - executed)
    unexpected = sorted(executed - expected)
    missing_fingerprints = _extract_task_mapping(plan, "plan_fingerprints", missing)
    missing_signatures = _extract_task_mapping(plan, "plan_task_signatures", missing)
    blocked_datasets, blocked_scan_units = _blocked_scan_units(plan, missing)
    emit_diagnostics_event(
        "plan_execution_diff_v1",
        payload={
            "run_id": get_run_id(),
            "timestamp_ns": time.time_ns(),
            "plan_signature": _plan_signature_value(plan, "plan_signature"),
            "task_dependency_signature": _plan_signature_value(plan, "task_dependency_signature"),
            "reduced_task_dependency_signature": _plan_signature_value(
                plan, "reduced_task_dependency_signature"
            ),
            "expected_task_count": len(expected),
            "executed_task_count": len(executed),
            "missing_task_count": len(missing),
            "unexpected_task_count": len(unexpected),
            "missing_tasks": missing,
            "unexpected_tasks": unexpected,
            "missing_task_fingerprints": missing_fingerprints,
            "missing_task_signatures": missing_signatures,
            "blocked_datasets": blocked_datasets,
            "blocked_scan_units": blocked_scan_units,
        },
        event_kind="artifact",
    )


def _base_task_name(task_name: str) -> str:
    if ":" not in task_name:
        return task_name
    return task_name.split(":", 1)[0]


def _extract_task_mapping(
    plan: object,
    attribute: str,
    tasks: Sequence[str],
) -> dict[str, str]:
    values = getattr(plan, attribute, None)
    if not isinstance(values, Mapping):
        return {}
    result: dict[str, str] = {}
    for task in tasks:
        value = values.get(task)
        if isinstance(value, str):
            result[task] = value
    return result


def _blocked_scan_units(
    plan: object,
    tasks: Sequence[str],
) -> tuple[list[str], list[str]]:
    scan_names_by_task = getattr(plan, "scan_task_names_by_task", None)
    scan_units_by_name = getattr(plan, "scan_task_units_by_name", None)
    if not isinstance(scan_names_by_task, Mapping) or not isinstance(scan_units_by_name, Mapping):
        return [], []
    blocked_datasets: set[str] = set()
    blocked_scan_units: set[str] = set()
    for task in tasks:
        scan_names = scan_names_by_task.get(task)
        if not isinstance(scan_names, Sequence) or isinstance(scan_names, (str, bytes, bytearray)):
            continue
        for scan_name in scan_names:
            if not isinstance(scan_name, str):
                continue
            blocked_scan_units.add(scan_name)
            scan_unit = scan_units_by_name.get(scan_name)
            dataset_name = getattr(scan_unit, "dataset_name", None)
            if isinstance(dataset_name, str):
                blocked_datasets.add(dataset_name)
    return sorted(blocked_datasets), sorted(blocked_scan_units)


def _plan_signature_value(plan: object, attribute: str) -> str | None:
    value = getattr(plan, attribute, None)
    if isinstance(value, str) and value:
        return value
    return None


__all__ = [
    "FULL_PIPELINE_OUTPUTS",
    "PipelineExecutionOptions",
    "execute_pipeline",
]
