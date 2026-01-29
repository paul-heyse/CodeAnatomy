"""Hamilton driver construction helpers for the pipeline."""

from __future__ import annotations

import inspect
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast

import pyarrow as pa
from hamilton import async_driver, driver
from hamilton.execution import executors
from hamilton.lifecycle import FunctionInputOutputTypeChecker
from hamilton.lifecycle import base as lifecycle_base

from core_types import DeterminismTier, JsonValue
from datafusion_engine.arrow_interop import SchemaLike
from engine.runtime_profile import RuntimeProfileSpec, resolve_runtime_profile
from hamilton_pipeline import modules as hamilton_modules
from hamilton_pipeline.execution_manager import PlanExecutionManager
from hamilton_pipeline.lifecycle import (
    DiagnosticsNodeHook,
    PlanDiagnosticsHook,
    set_hamilton_diagnostics_collector,
)
from hamilton_pipeline.modules.execution_plan import build_execution_plan_module
from hamilton_pipeline.pipeline_types import (
    ExecutionMode,
    ExecutorConfig,
    ExecutorKind,
    GraphAdapterConfig,
    GraphAdapterKind,
)
from hamilton_pipeline.task_module_builder import (
    TaskExecutionModuleOptions,
    build_task_execution_module,
)
from obs.diagnostics import DiagnosticsCollector
from obs.otel.hamilton import OtelNodeHook, OtelPlanHook
from relspec.view_defs import RELATION_OUTPUT_NAME

if TYPE_CHECKING:
    from hamilton.base import HamiltonGraphAdapter
    from hamilton.io.materialization import MaterializerFactory

    from datafusion_engine.runtime import DataFusionRuntimeProfile, SessionRuntime
    from datafusion_engine.view_graph_registry import ViewNode
    from hamilton_pipeline.cache_lineage import CacheLineageHook
    from hamilton_pipeline.semantic_registry import SemanticRegistryHook
    from relspec.execution_plan import ExecutionPlan
    from relspec.incremental import IncrementalDiff
from storage.ipc_utils import ipc_hash

try:
    from hamilton_sdk import adapters as hamilton_adapters
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    hamilton_adapters = None

_DEFAULT_DAG_NAME = "codeintel::semantic_v1"
_SEMANTIC_VERSION = "v1"


def default_modules() -> list[ModuleType]:
    """Return the default Hamilton module set for the pipeline.

    Returns
    -------
    list[ModuleType]
        Default module list for the pipeline.
    """
    return hamilton_modules.load_all_modules()


def config_fingerprint(config: Mapping[str, JsonValue]) -> str:
    """Compute a stable config fingerprint for driver caching.

    Hamilton build-time config is immutable after build; if config changes,
    rebuild a new driver. The Hamilton docs recommend a small driver-factory
    that caches by config fingerprint.
    :contentReference[oaicite:2]{index=2}

    Returns
    -------
    str
        SHA-256 fingerprint for the config.
    """
    payload = {"version": 1, "config": dict(config)}
    table = pa.Table.from_pylist([payload])
    return ipc_hash(table)


def driver_cache_key(
    config: Mapping[str, JsonValue],
    *,
    plan_signature: str,
    execution_mode: ExecutionMode,
    executor_config: ExecutorConfig | None,
    graph_adapter_config: GraphAdapterConfig | None,
) -> str:
    """Compute a plan-aware driver cache key.

    Returns
    -------
    str
        SHA-256 fingerprint for the config and plan signature.
    """
    executor_payload = _executor_config_payload(executor_config)
    adapter_payload = _graph_adapter_config_payload(graph_adapter_config)
    payload = {
        "version": 3,
        "plan_signature": plan_signature,
        "execution_mode": execution_mode.value,
        "executor_config": executor_payload,
        "graph_adapter_config": adapter_payload,
        "config": dict(config),
    }
    table = pa.Table.from_pylist([payload])
    return ipc_hash(table)


def _runtime_profile_name(config: Mapping[str, JsonValue]) -> str:
    value = config.get("runtime_profile_name")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return os.environ.get("CODEANATOMY_RUNTIME_PROFILE", "").strip() or "default"


def _determinism_override(config: Mapping[str, JsonValue]) -> DeterminismTier | None:
    value = config.get("determinism_override")
    if isinstance(value, str) and value.strip():
        normalized = value.strip().lower()
        mapping: dict[str, DeterminismTier] = {
            "tier2": DeterminismTier.CANONICAL,
            "canonical": DeterminismTier.CANONICAL,
            "tier1": DeterminismTier.STABLE_SET,
            "stable": DeterminismTier.STABLE_SET,
            "stable_set": DeterminismTier.STABLE_SET,
            "tier0": DeterminismTier.BEST_EFFORT,
            "fast": DeterminismTier.BEST_EFFORT,
            "best_effort": DeterminismTier.BEST_EFFORT,
        }
        return mapping.get(normalized)
    force_flag = os.environ.get("CODEANATOMY_FORCE_TIER2", "").strip().lower()
    if force_flag in {"1", "true", "yes", "y"}:
        return DeterminismTier.CANONICAL
    tier = os.environ.get("CODEANATOMY_DETERMINISM_TIER", "").strip().lower()
    mapping = {
        "tier2": DeterminismTier.CANONICAL,
        "canonical": DeterminismTier.CANONICAL,
        "tier1": DeterminismTier.STABLE_SET,
        "stable": DeterminismTier.STABLE_SET,
        "stable_set": DeterminismTier.STABLE_SET,
        "tier0": DeterminismTier.BEST_EFFORT,
        "fast": DeterminismTier.BEST_EFFORT,
        "best_effort": DeterminismTier.BEST_EFFORT,
    }
    return mapping.get(tier)


def _executor_config_payload(
    executor_config: ExecutorConfig | None,
) -> dict[str, object]:
    if executor_config is None:
        return {}
    return {
        "kind": executor_config.kind,
        "max_tasks": executor_config.max_tasks,
        "remote_kind": executor_config.remote_kind,
        "remote_max_tasks": executor_config.remote_max_tasks,
        "cost_threshold": executor_config.cost_threshold,
        "ray_init_config": (
            dict(executor_config.ray_init_config) if executor_config.ray_init_config else None
        ),
        "dask_scheduler": executor_config.dask_scheduler,
        "dask_client_kwargs": (
            dict(executor_config.dask_client_kwargs) if executor_config.dask_client_kwargs else None
        ),
    }


def _graph_adapter_config_payload(
    adapter_config: GraphAdapterConfig | None,
) -> dict[str, object]:
    if adapter_config is None:
        return {}
    return {
        "kind": adapter_config.kind,
        "options": dict(adapter_config.options) if adapter_config.options else None,
    }


@dataclass(frozen=True)
class ViewGraphContext:
    """Runtime context needed to compile the execution plan."""

    profile: DataFusionRuntimeProfile
    session_runtime: SessionRuntime
    determinism_tier: DeterminismTier
    snapshot: Mapping[str, object]
    view_nodes: tuple[ViewNode, ...]
    runtime_profile_spec: RuntimeProfileSpec


def _view_graph_context(config: Mapping[str, JsonValue]) -> ViewGraphContext:
    runtime_profile_spec = resolve_runtime_profile(
        _runtime_profile_name(config),
        determinism=_determinism_override(config),
    )
    profile = runtime_profile_spec.datafusion
    from cpg.kind_catalog import validate_edge_kind_requirements
    from datafusion_engine.view_registry import ensure_view_graph
    from datafusion_engine.view_registry_specs import view_graph_nodes

    session_runtime = profile.session_runtime()
    snapshot = ensure_view_graph(
        session_runtime.ctx,
        runtime_profile=profile,
        include_registry_views=True,
    )
    validate_edge_kind_requirements(_relation_output_schema(session_runtime))
    nodes = view_graph_nodes(
        session_runtime.ctx,
        snapshot=snapshot,
        runtime_profile=profile,
    )
    return ViewGraphContext(
        profile=profile,
        session_runtime=session_runtime,
        determinism_tier=runtime_profile_spec.determinism_tier,
        snapshot=snapshot,
        view_nodes=tuple(nodes),
        runtime_profile_spec=runtime_profile_spec,
    )


def _task_name_list_from_config(
    config: Mapping[str, JsonValue],
    *,
    key: str,
) -> tuple[str, ...] | None:
    value = config.get(key)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return None
    names = [item.strip() for item in value if isinstance(item, str) and item.strip()]
    if not names:
        return None
    return tuple(sorted(set(names)))


def _compile_plan(
    view_ctx: ViewGraphContext,
    config: Mapping[str, JsonValue],
) -> ExecutionPlan:
    from relspec.execution_plan import ExecutionPlanRequest, compile_execution_plan

    requested = _task_name_list_from_config(config, key="plan_requested_tasks")
    impacted = _task_name_list_from_config(config, key="plan_impacted_tasks")
    allow_partial = bool(config.get("plan_allow_partial", False))
    enable_metric_scheduling = True
    metric_flag = config.get("enable_metric_scheduling")
    if isinstance(metric_flag, bool):
        enable_metric_scheduling = metric_flag
    request = ExecutionPlanRequest(
        view_nodes=view_ctx.view_nodes,
        snapshot=view_ctx.snapshot,
        runtime_profile=view_ctx.profile,
        requested_task_names=requested,
        impacted_task_names=impacted,
        allow_partial=allow_partial,
        enable_metric_scheduling=enable_metric_scheduling,
    )
    return compile_execution_plan(session_runtime=view_ctx.session_runtime, request=request)


def _incremental_enabled(config: Mapping[str, JsonValue]) -> bool:
    if bool(config.get("incremental_enabled")):
        return True
    mode = os.environ.get("CODEANATOMY_PIPELINE_MODE", "").strip().lower()
    return mode in {"incremental", "streaming"}


def _resolve_incremental_state_dir(config: Mapping[str, JsonValue]) -> Path | None:
    if not _incremental_enabled(config):
        return None
    repo_root_value = config.get("repo_root")
    if not isinstance(repo_root_value, str) or not repo_root_value.strip():
        return None
    repo_root = Path(repo_root_value).expanduser()
    state_dir_value = config.get("incremental_state_dir")
    state_dir_env = os.environ.get("CODEANATOMY_STATE_DIR")
    state_dir = state_dir_value if isinstance(state_dir_value, str) else state_dir_env
    if isinstance(state_dir, str) and state_dir.strip():
        return repo_root / Path(state_dir)
    return repo_root / "build" / "state"


def _precompute_incremental_diff(
    *,
    view_ctx: ViewGraphContext,
    plan: ExecutionPlan,
    config: Mapping[str, JsonValue],
) -> tuple[IncrementalDiff | None, str | None]:
    state_dir = _resolve_incremental_state_dir(config)
    if state_dir is None:
        return None, None
    from incremental.delta_context import DeltaAccessContext
    from incremental.plan_fingerprints import read_plan_snapshots
    from incremental.runtime import IncrementalRuntime
    from incremental.state_store import StateStore
    from relspec.incremental import diff_plan_snapshots

    try:
        runtime = IncrementalRuntime.build(
            profile=view_ctx.profile,
            determinism_tier=view_ctx.determinism_tier,
        )
    except ValueError:
        return None, str(state_dir)
    context = DeltaAccessContext(runtime=runtime)
    state_store = StateStore(root=state_dir)
    previous = read_plan_snapshots(state_store, context=context)
    diff = diff_plan_snapshots(previous, plan.plan_snapshots)
    return diff, str(state_dir)


def _active_tasks_from_incremental_diff(
    *,
    plan: ExecutionPlan,
    diff: IncrementalDiff,
) -> set[str]:
    from relspec.execution_plan import downstream_task_closure, upstream_task_closure

    active = set(plan.active_tasks)
    rebuild = diff.tasks_requiring_rebuild()
    rebuild_active = set(rebuild) & active
    if not rebuild_active:
        return active
    impacted = downstream_task_closure(plan.task_graph, rebuild_active)
    impacted &= active
    if not impacted:
        return active
    impacted_with_deps = upstream_task_closure(plan.task_graph, impacted)
    impacted_with_deps &= active
    if not plan.requested_task_names:
        return impacted_with_deps
    requested_tasks = plan.requested_task_names
    requested_anchor = upstream_task_closure(plan.task_graph, requested_tasks)
    requested_anchor &= active
    return impacted_with_deps | requested_anchor


def _plan_with_incremental_pruning(
    *,
    view_ctx: ViewGraphContext,
    plan: ExecutionPlan,
    config: Mapping[str, JsonValue],
) -> ExecutionPlan:
    diff, state_dir = _precompute_incremental_diff(
        view_ctx=view_ctx,
        plan=plan,
        config=config,
    )
    if diff is None and state_dir is None:
        return plan
    plan_with_diff = replace(
        plan,
        incremental_diff=diff,
        incremental_state_dir=state_dir,
    )
    if diff is None:
        return plan_with_diff
    active_from_diff = _active_tasks_from_incremental_diff(
        plan=plan_with_diff,
        diff=diff,
    )
    if active_from_diff == set(plan_with_diff.active_tasks):
        return plan_with_diff
    from relspec.execution_plan import prune_execution_plan

    return prune_execution_plan(
        plan_with_diff,
        active_tasks=active_from_diff,
    )


def _tracker_value(
    config: Mapping[str, JsonValue],
    *,
    config_key: str,
    env_key: str,
) -> object | None:
    """Return a tracker config value from config or environment.

    Returns
    -------
    object | None
        Config value resolved from config or environment.
    """
    value = config.get(config_key)
    if value is None:
        value = os.environ.get(env_key)
    return value


def _tracker_project_id(value: object | None) -> int | None:
    """Coerce the tracker project id from config sources.

    Returns
    -------
    int | None
        Parsed project id when available.
    """
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _tracker_username(value: object | None) -> str | None:
    """Coerce the tracker username from config sources.

    Returns
    -------
    str | None
        Parsed username when available.
    """
    if isinstance(value, str) and value.strip():
        return value
    return None


def _tracker_dag_name(value: object | None) -> str:
    """Return a non-empty DAG name or fall back to the default.

    Returns
    -------
    str
        DAG name for tracker registration.
    """
    if isinstance(value, str) and value.strip():
        return value
    return _DEFAULT_DAG_NAME


def _tracker_tags(value: object | None) -> dict[str, str]:
    """Return normalized tracker tags from config sources.

    Returns
    -------
    dict[str, str]
        Normalized tags payload.
    """
    if isinstance(value, Mapping):
        return {str(key): str(tag_value) for key, tag_value in value.items()}
    return {}


def _maybe_build_tracker_adapter(
    config: Mapping[str, JsonValue],
) -> lifecycle_base.LifecycleAdapter | None:
    """Build an optional Hamilton UI tracker adapter.

    Docs show:
      tracker = adapters.HamiltonTracker(
          project_id=...,
          username=...,
          dag_name=...,
          tags=...,
      )
      Builder().with_modules(...).with_config(...).with_adapters(tracker).build()
      :contentReference[oaicite:3]{index=3}

    Returns
    -------
    object | None
        Tracker adapter when enabled and available.
    """
    if hamilton_adapters is None:
        return None

    if not bool(config.get("enable_hamilton_tracker", False)):
        return None

    project_id = _tracker_project_id(
        _tracker_value(
            config,
            config_key="hamilton_project_id",
            env_key="HAMILTON_PROJECT_ID",
        )
    )
    username = _tracker_username(
        _tracker_value(
            config,
            config_key="hamilton_username",
            env_key="HAMILTON_USERNAME",
        )
    )
    if project_id is None or username is None:
        return None

    dag_name = _tracker_dag_name(
        _tracker_value(
            config,
            config_key="hamilton_dag_name",
            env_key="HAMILTON_DAG_NAME",
        )
    )
    tags = _tracker_tags(config.get("hamilton_tags"))
    api_url_value = _tracker_value(
        config,
        config_key="hamilton_api_url",
        env_key="HAMILTON_API_URL",
    )
    api_url = api_url_value if isinstance(api_url_value, str) else None
    ui_url_value = _tracker_value(
        config,
        config_key="hamilton_ui_url",
        env_key="HAMILTON_UI_URL",
    )
    ui_url = ui_url_value if isinstance(ui_url_value, str) else None

    class _TrackerKwargs(TypedDict, total=False):
        project_id: int
        username: str
        dag_name: str
        tags: dict[str, str]
        hamilton_api_url: str
        hamilton_ui_url: str

    tracker_kwargs: _TrackerKwargs = {
        "project_id": project_id,
        "username": username,
        "dag_name": dag_name,
        "tags": tags,
    }
    if api_url is not None:
        tracker_kwargs["hamilton_api_url"] = api_url
    if ui_url is not None:
        tracker_kwargs["hamilton_ui_url"] = ui_url

    tracker = hamilton_adapters.HamiltonTracker(**tracker_kwargs)
    return cast("lifecycle_base.LifecycleAdapter", tracker)


def _maybe_build_async_tracker_adapter(
    config: Mapping[str, JsonValue],
) -> lifecycle_base.LifecycleAdapter | None:
    """Build an async Hamilton UI tracker adapter when enabled.

    Returns
    -------
    object | None
        Async tracker adapter when enabled and available.
    """
    if hamilton_adapters is None:
        return None
    if not bool(config.get("enable_hamilton_tracker", False)):
        return None
    project_id = _tracker_project_id(
        _tracker_value(
            config,
            config_key="hamilton_project_id",
            env_key="HAMILTON_PROJECT_ID",
        )
    )
    username = _tracker_username(
        _tracker_value(
            config,
            config_key="hamilton_username",
            env_key="HAMILTON_USERNAME",
        )
    )
    if project_id is None or username is None:
        return None
    dag_name = _tracker_dag_name(
        _tracker_value(
            config,
            config_key="hamilton_dag_name",
            env_key="HAMILTON_DAG_NAME",
        )
    )
    tags = _tracker_tags(config.get("hamilton_tags"))
    api_url_value = _tracker_value(
        config,
        config_key="hamilton_api_url",
        env_key="HAMILTON_API_URL",
    )
    api_url = api_url_value if isinstance(api_url_value, str) else None
    ui_url_value = _tracker_value(
        config,
        config_key="hamilton_ui_url",
        env_key="HAMILTON_UI_URL",
    )
    ui_url = ui_url_value if isinstance(ui_url_value, str) else None

    class _AsyncTrackerKwargs(TypedDict, total=False):
        project_id: int
        username: str
        dag_name: str
        tags: dict[str, str]
        hamilton_api_url: str
        hamilton_ui_url: str

    tracker_kwargs: _AsyncTrackerKwargs = {
        "project_id": project_id,
        "username": username,
        "dag_name": dag_name,
        "tags": tags,
    }
    if api_url is not None:
        tracker_kwargs["hamilton_api_url"] = api_url
    if ui_url is not None:
        tracker_kwargs["hamilton_ui_url"] = ui_url

    tracker = hamilton_adapters.AsyncHamiltonTracker(**tracker_kwargs)
    return cast("lifecycle_base.LifecycleAdapter", tracker)


def _with_graph_tags(
    config: Mapping[str, JsonValue],
    *,
    plan: ExecutionPlan,
) -> dict[str, JsonValue]:
    config_payload = dict(config)
    dag_name_value = config_payload.get("hamilton_dag_name")
    dag_name = (
        dag_name_value if isinstance(dag_name_value, str) and dag_name_value else _DEFAULT_DAG_NAME
    )
    config_payload["hamilton_dag_name"] = dag_name
    tags_value = config_payload.get("hamilton_tags")
    merged_tags: dict[str, str] = {}
    if isinstance(tags_value, Mapping):
        merged_tags.update({str(k): str(v) for k, v in tags_value.items()})
    merged_tags["plan_signature"] = plan.plan_signature
    merged_tags["reduced_plan_signature"] = plan.reduced_task_dependency_signature
    merged_tags["task_dependency_signature"] = plan.task_dependency_signature
    merged_tags["plan_task_count"] = str(len(plan.active_tasks))
    merged_tags["plan_task_signature_count"] = str(len(plan.plan_task_signatures))
    merged_tags["plan_generation_count"] = str(len(plan.task_schedule.generations))
    merged_tags["plan_reduction_edge_count"] = str(plan.reduction_edge_count)
    merged_tags["plan_reduction_removed_edge_count"] = str(plan.reduction_removed_edge_count)
    runtime_env = _string_override(config_payload, "runtime_environment")
    if runtime_env is None:
        runtime_env = os.environ.get("CODEANATOMY_ENV", "").strip() or None
    if runtime_env:
        merged_tags.setdefault("environment", runtime_env)
    team_value = _string_override(config_payload, "runtime_team")
    if team_value is None:
        team_value = os.environ.get("CODEANATOMY_TEAM", "").strip() or None
    if team_value:
        merged_tags.setdefault("team", team_value)
    merged_tags.setdefault("runtime_profile", _runtime_profile_name(config_payload))
    determinism = _determinism_override(config_payload)
    if determinism is not None:
        merged_tags.setdefault("determinism_tier", determinism.value)
    if plan.session_runtime_hash is not None:
        merged_tags["session_runtime_hash"] = plan.session_runtime_hash
    merged_tags["semantic_version"] = _SEMANTIC_VERSION
    if plan.critical_path_length_weighted is not None:
        merged_tags["plan_critical_path_length_weighted"] = str(plan.critical_path_length_weighted)
    config_payload["hamilton_tags"] = merged_tags
    return config_payload


def _apply_tracker_config_from_profile(
    config: dict[str, JsonValue],
    *,
    profile_spec: RuntimeProfileSpec,
) -> dict[str, JsonValue]:
    tracker = profile_spec.tracker_config
    if tracker is None:
        return config
    if tracker.project_id is not None:
        config.setdefault("hamilton_project_id", tracker.project_id)
    if tracker.username is not None:
        config.setdefault("hamilton_username", tracker.username)
    if tracker.dag_name is not None:
        config.setdefault("hamilton_dag_name", tracker.dag_name)
    if tracker.api_url is not None:
        config.setdefault("hamilton_api_url", tracker.api_url)
    if tracker.ui_url is not None:
        config.setdefault("hamilton_ui_url", tracker.ui_url)
    if tracker.enabled:
        config.setdefault("enable_hamilton_tracker", True)
    return config


class DaskClientKwargs(TypedDict, total=False):
    """Typed subset of Dask Client keyword arguments supported via config."""

    timeout: float
    set_as_default: bool
    scheduler_file: str
    asynchronous: bool
    name: str
    direct_to_workers: bool
    connection_limit: int


DaskClientValue = bool | int | float | str

_DASK_CLIENT_BOOL_KEYS = {"asynchronous", "direct_to_workers", "set_as_default"}
_DASK_CLIENT_STR_KEYS = {"name", "scheduler_file"}
_DASK_CLIENT_INT_KEYS = {"connection_limit"}
_DASK_CLIENT_FLOAT_KEYS = {"timeout"}


def _require_bool(value: JsonValue, *, key: str) -> bool:
    if isinstance(value, bool):
        return value
    msg = f"Dask client {key} must be a boolean."
    raise ValueError(msg)


def _require_str(value: JsonValue, *, key: str) -> str:
    if isinstance(value, str):
        return value
    msg = f"Dask client {key} must be a string."
    raise ValueError(msg)


def _require_int(value: JsonValue, *, key: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    msg = f"Dask client {key} must be an integer."
    raise ValueError(msg)


def _require_float(value: JsonValue, *, key: str) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    msg = f"Dask client {key} must be a number."
    raise ValueError(msg)


def _parse_dask_client_kwargs(
    raw_kwargs: Mapping[str, JsonValue] | None,
) -> DaskClientKwargs:
    if not raw_kwargs:
        return {}
    parsed: dict[str, DaskClientValue] = {}
    for key, value in raw_kwargs.items():
        if key in _DASK_CLIENT_FLOAT_KEYS:
            parsed[key] = _require_float(value, key=key)
            continue
        if key in _DASK_CLIENT_BOOL_KEYS:
            parsed[key] = _require_bool(value, key=key)
            continue
        if key in _DASK_CLIENT_STR_KEYS:
            parsed[key] = _require_str(value, key=key)
            continue
        if key in _DASK_CLIENT_INT_KEYS:
            parsed[key] = _require_int(value, key=key)
            continue
        msg = f"Unsupported Dask client kwarg: {key!r}."
        raise ValueError(msg)
    return cast("DaskClientKwargs", parsed)


def _executor_from_kind(
    kind: ExecutorKind,
    *,
    max_tasks: int,
    executor_config: ExecutorConfig | None,
) -> executors.TaskExecutor:
    if max_tasks <= 0:
        msg = "Executor max_tasks must be a positive integer."
        raise ValueError(msg)
    if kind == "multiprocessing":
        return executors.MultiProcessingExecutor(max_tasks=max_tasks)
    if kind == "threadpool":
        return executors.MultiThreadingExecutor(max_tasks=max_tasks)
    if kind == "ray":
        try:
            from hamilton.plugins import h_ray
        except ImportError as exc:
            msg = "Ray executor requested but Hamilton ray plugin is not installed."
            raise ValueError(msg) from exc
        ray_init_config: dict[str, Any] = (
            dict(executor_config.ray_init_config)
            if executor_config is not None and executor_config.ray_init_config is not None
            else {}
        )
        return h_ray.RayTaskExecutor(
            num_cpus=max_tasks,
            ray_init_config=ray_init_config,
        )
    if kind == "dask":
        try:
            from distributed import Client
            from hamilton.plugins import h_dask
        except ImportError as exc:
            msg = "Dask executor requested but Hamilton dask plugin is not installed."
            raise ValueError(msg) from exc
        client_kwargs = _parse_dask_client_kwargs(
            executor_config.dask_client_kwargs if executor_config is not None else None
        )
        if executor_config is not None and executor_config.dask_scheduler:
            client = Client(executor_config.dask_scheduler, **client_kwargs)
        else:
            client = Client(n_workers=max_tasks, threads_per_worker=1, **client_kwargs)
        return h_dask.DaskExecutor(client=client)
    msg = f"Executor kind {kind!r} is not supported in dynamic execution."
    raise ValueError(msg)


@dataclass(frozen=True)
class DynamicExecutionOptions:
    """Inputs required to configure dynamic plan execution."""

    config: Mapping[str, JsonValue]
    plan: ExecutionPlan
    diagnostics: DiagnosticsCollector
    execution_mode: ExecutionMode
    executor_config: ExecutorConfig | None = None


def _apply_dynamic_execution(
    builder: driver.Builder,
    *,
    options: DynamicExecutionOptions,
) -> driver.Builder:
    if options.execution_mode == ExecutionMode.DETERMINISTIC_SERIAL:
        return builder
    resolved_config = options.executor_config or ExecutorConfig()
    from hamilton_pipeline.scheduling_hooks import (
        plan_grouping_strategy,
        plan_task_grouping_hook,
        plan_task_submission_hook,
    )

    enable_submission_hook = bool(options.config.get("enable_plan_task_submission_hook", True))
    enable_grouping_hook = bool(options.config.get("enable_plan_task_grouping_hook", True))
    enforce_submission = bool(options.config.get("enforce_plan_task_submission", True))
    local_executor = executors.SynchronousLocalTaskExecutor()
    remote_kind = resolved_config.remote_kind or resolved_config.kind
    remote_max_tasks = resolved_config.remote_max_tasks or resolved_config.max_tasks
    remote_executor = _executor_from_kind(
        remote_kind,
        max_tasks=remote_max_tasks,
        executor_config=resolved_config,
    )
    cost_threshold = (
        resolved_config.cost_threshold
        if options.execution_mode == ExecutionMode.PLAN_PARALLEL_REMOTE
        else None
    )
    execution_manager = PlanExecutionManager(
        local_executor=local_executor,
        remote_executor=remote_executor,
        cost_threshold=cost_threshold,
        diagnostics=options.diagnostics,
    )

    dynamic_builder = (
        builder.enable_dynamic_execution(allow_experimental_mode=True)
        .with_execution_manager(execution_manager)
        .with_grouping_strategy(plan_grouping_strategy(options.plan))
    )
    if enable_submission_hook:
        dynamic_builder = dynamic_builder.with_adapters(
            plan_task_submission_hook(
                options.plan,
                options.diagnostics,
                enforce_active=enforce_submission,
            )
        )
    if enable_grouping_hook:
        dynamic_builder = dynamic_builder.with_adapters(
            plan_task_grouping_hook(options.plan, options.diagnostics)
        )
    return dynamic_builder


def _graph_adapter_config_from_config(
    config: Mapping[str, JsonValue],
) -> GraphAdapterConfig | None:
    value = config.get("hamilton_graph_adapter_kind") or config.get("graph_adapter_kind")
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip().lower()
    if normalized not in {"threadpool", "dask", "ray"}:
        return None
    options_value = config.get("hamilton_graph_adapter_options") or config.get(
        "graph_adapter_options"
    )
    options: dict[str, JsonValue] | None = None
    if isinstance(options_value, Mapping):
        options = {str(key): cast("JsonValue", value) for key, value in options_value.items()}
    return GraphAdapterConfig(kind=cast("GraphAdapterKind", normalized), options=options)


def _graph_adapter_from_config(
    adapter_config: GraphAdapterConfig,
    *,
    executor_config: ExecutorConfig | None,
) -> HamiltonGraphAdapter:
    options = dict(adapter_config.options or {})
    if adapter_config.kind == "threadpool":
        return _threadpool_adapter(options, executor_config=executor_config)
    if adapter_config.kind == "ray":
        return _ray_adapter(options, executor_config=executor_config)
    if adapter_config.kind == "dask":
        return _dask_adapter(options, executor_config=executor_config)
    msg = f"Unsupported graph adapter kind: {adapter_config.kind!r}."
    raise ValueError(msg)


def _threadpool_adapter(
    options: Mapping[str, JsonValue],
    *,
    executor_config: ExecutorConfig | None,
) -> HamiltonGraphAdapter:
    """Build a threadpool graph adapter.

    Returns
    -------
    object
        Threadpool graph adapter instance.
    """
    from hamilton import base as hamilton_base
    from hamilton.plugins import h_threadpool

    max_workers_value = options.get("max_workers")
    max_workers = (
        int(max_workers_value)
        if isinstance(max_workers_value, int) and not isinstance(max_workers_value, bool)
        else None
    )
    if max_workers is None and executor_config is not None:
        max_workers = executor_config.max_tasks
    if max_workers is None:
        max_workers = 4
    thread_name_prefix = options.get("thread_name_prefix")
    prefix = thread_name_prefix if isinstance(thread_name_prefix, str) else ""
    adapter = h_threadpool.FutureAdapter(
        max_workers=max_workers,
        thread_name_prefix=prefix,
        result_builder=hamilton_base.DictResult(),
    )
    return cast("HamiltonGraphAdapter", adapter)


def _ray_adapter(
    options: Mapping[str, JsonValue],
    *,
    executor_config: ExecutorConfig | None,
) -> HamiltonGraphAdapter:
    """Build a Ray graph adapter.

    Returns
    -------
    object
        Ray graph adapter instance.
    """
    from hamilton import base as hamilton_base
    from hamilton.plugins import h_ray

    ray_init_config: dict[str, Any] = {}
    ray_init_value = options.get("ray_init_config")
    if ray_init_value is None and executor_config is not None:
        ray_init_value = executor_config.ray_init_config
    if isinstance(ray_init_value, Mapping):
        ray_init_config = dict(ray_init_value)
    shutdown_flag = bool(options.get("shutdown_ray_on_completion"))
    adapter = h_ray.RayGraphAdapter(
        result_builder=hamilton_base.DictResult(),
        ray_init_config=ray_init_config,
        shutdown_ray_on_completion=shutdown_flag,
    )
    return cast("HamiltonGraphAdapter", adapter)


def _dask_adapter(
    options: Mapping[str, JsonValue],
    *,
    executor_config: ExecutorConfig | None,
) -> HamiltonGraphAdapter:
    """Build a Dask graph adapter.

    Returns
    -------
    object
        Dask graph adapter instance.

    Raises
    ------
    ValueError
        Raised when the Dask dependency is missing.
    """
    from hamilton import base as hamilton_base
    from hamilton.plugins import h_dask

    try:
        from distributed import Client
    except ImportError as exc:
        msg = "Dask graph adapter requested but distributed is not installed."
        raise ValueError(msg) from exc
    scheduler_value = options.get("scheduler_address") or options.get("scheduler")
    client_kwargs_value = options.get("client_kwargs")
    raw_client_kwargs = (
        cast("Mapping[str, JsonValue]", client_kwargs_value)
        if isinstance(client_kwargs_value, Mapping)
        else None
    )
    client_kwargs = _parse_dask_client_kwargs(raw_client_kwargs)
    if scheduler_value is None and executor_config is not None:
        scheduler_value = executor_config.dask_scheduler
    if scheduler_value:
        client = Client(str(scheduler_value), **client_kwargs)
    else:
        max_workers = executor_config.max_tasks if executor_config is not None else 4
        client = Client(n_workers=max_workers, threads_per_worker=1, **client_kwargs)
    visualize_kwargs_value = options.get("visualize_kwargs")
    visualize_kwargs = (
        dict(visualize_kwargs_value) if isinstance(visualize_kwargs_value, Mapping) else {}
    )
    use_delayed = bool(options.get("use_delayed", True))
    compute_at_end = bool(options.get("compute_at_end", True))
    adapter = h_dask.DaskGraphAdapter(
        dask_client=client,
        result_builder=hamilton_base.DictResult(),
        visualize_kwargs=visualize_kwargs,
        use_delayed=use_delayed,
        compute_at_end=compute_at_end,
    )
    return cast("HamiltonGraphAdapter", adapter)


def _apply_graph_adapter(
    builder: driver.Builder,
    *,
    config: Mapping[str, JsonValue],
    executor_config: ExecutorConfig | None,
    adapter_config: GraphAdapterConfig | None,
) -> driver.Builder:
    resolved = adapter_config or _graph_adapter_config_from_config(config)
    if resolved is None:
        return builder
    adapter = _graph_adapter_from_config(resolved, executor_config=executor_config)
    return builder.with_adapter(adapter)


def _build_materializers(
    _config: Mapping[str, JsonValue],
) -> list[MaterializerFactory]:
    return []


def _apply_materializers(
    builder: driver.Builder,
    *,
    config: Mapping[str, JsonValue],
) -> driver.Builder:
    materializers = _build_materializers(config)
    if not materializers:
        return builder
    return builder.with_materializers(*materializers)


@dataclass(frozen=True)
class CachePolicyProfile:
    """Explicit cache policy defaults for correctness boundaries."""

    name: str
    default_behavior: CacheBehavior
    default_loader_behavior: CacheBehavior
    default_saver_behavior: CacheBehavior
    log_to_file: bool


CacheBehavior = Literal["default", "disable", "ignore", "recompute"]


def _string_override(config: Mapping[str, JsonValue], key: str) -> str | None:
    value = config.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _cache_behavior_override(
    config: Mapping[str, JsonValue],
    key: str,
) -> CacheBehavior | None:
    value = _string_override(config, key)
    if value is None:
        return None
    if value in {"default", "disable", "ignore", "recompute"}:
        return cast("CacheBehavior", value)
    return None


def _cache_policy_profile(config: Mapping[str, JsonValue]) -> CachePolicyProfile:
    profile_value = config.get("cache_policy_profile")
    profile_name = profile_value.strip() if isinstance(profile_value, str) else ""
    cache_opt_in = bool(config.get("cache_opt_in", True))
    default_behavior: CacheBehavior = "disable" if cache_opt_in else "default"
    default_loader_behavior: CacheBehavior = "recompute"
    default_saver_behavior: CacheBehavior = "disable"
    if profile_name == "aggressive":
        default_loader_behavior = "default"
        default_saver_behavior = "default"
    default_override = _cache_behavior_override(config, "cache_default_behavior")
    if default_override is not None:
        default_behavior = default_override
    loader_override = _cache_behavior_override(
        config,
        "cache_default_loader_behavior",
    )
    if loader_override is not None:
        default_loader_behavior = loader_override
    saver_override = _cache_behavior_override(config, "cache_default_saver_behavior")
    if saver_override is not None:
        default_saver_behavior = saver_override
    log_to_file_value = config.get("cache_log_to_file")
    log_to_file = log_to_file_value if isinstance(log_to_file_value, bool) else True
    resolved_name = profile_name or "strict_causal"
    return CachePolicyProfile(
        name=resolved_name,
        default_behavior=default_behavior,
        default_loader_behavior=default_loader_behavior,
        default_saver_behavior=default_saver_behavior,
        log_to_file=log_to_file,
    )


def _apply_cache(
    builder: driver.Builder,
    *,
    config: Mapping[str, JsonValue],
) -> driver.Builder:
    cache_path = _cache_path_from_config(config)
    if cache_path is None:
        return builder
    from hamilton.caching.stores.file import FileResultStore
    from hamilton.caching.stores.sqlite import SQLiteMetadataStore

    base_path = Path(cache_path).expanduser()
    base_path.mkdir(parents=True, exist_ok=True)
    metadata_store = SQLiteMetadataStore(path=str(base_path / "meta.sqlite"))
    results_path = base_path / "results"
    results_path.mkdir(parents=True, exist_ok=True)
    result_store = FileResultStore(path=str(results_path))
    profile = _cache_policy_profile(config)
    return builder.with_cache(
        path=str(base_path),
        metadata_store=metadata_store,
        result_store=result_store,
        default_behavior=profile.default_behavior,
        default_loader_behavior=profile.default_loader_behavior,
        default_saver_behavior=profile.default_saver_behavior,
        log_to_file=profile.log_to_file,
    )


def _cache_path_from_config(config: Mapping[str, JsonValue]) -> str | None:
    value = config.get("cache_path")
    if not isinstance(value, str) or not value.strip():
        value = config.get("hamilton_cache_path")
    if not isinstance(value, str) or not value.strip():
        value = os.environ.get("CODEANATOMY_HAMILTON_CACHE_PATH")
    if not isinstance(value, str) or not value.strip():
        value = os.environ.get("HAMILTON_CACHE_PATH")
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def _apply_adapters(
    builder: driver.Builder,
    *,
    config: Mapping[str, JsonValue],
    diagnostics: DiagnosticsCollector,
    plan: ExecutionPlan,
    profile: DataFusionRuntimeProfile,
) -> driver.Builder:
    tracker = _maybe_build_tracker_adapter(config)
    if tracker is not None:
        builder = builder.with_adapters(tracker)
    if bool(config.get("enable_hamilton_type_checker", True)):
        builder = builder.with_adapters(FunctionInputOutputTypeChecker())
    if bool(config.get("enable_hamilton_node_diagnostics", True)):
        builder = builder.with_adapters(DiagnosticsNodeHook(diagnostics))
    if bool(config.get("enable_otel_node_tracing", True)):
        builder = builder.with_adapters(OtelNodeHook())
    if bool(config.get("enable_plan_diagnostics", True)):
        builder = builder.with_adapters(
            PlanDiagnosticsHook(plan=plan, profile=profile, collector=diagnostics)
        )
    if bool(config.get("enable_otel_plan_tracing", True)):
        builder = builder.with_adapters(OtelPlanHook())
    return builder


def _apply_async_adapters(
    builder: async_driver.Builder,
    *,
    config: Mapping[str, JsonValue],
    diagnostics: DiagnosticsCollector,
    plan: ExecutionPlan,
    profile: DataFusionRuntimeProfile,
) -> async_driver.Builder:
    tracker = _maybe_build_async_tracker_adapter(config)
    if tracker is not None:
        builder = cast("async_driver.Builder", builder.with_adapters(tracker))
    if bool(config.get("enable_hamilton_type_checker", True)):
        builder = cast(
            "async_driver.Builder",
            builder.with_adapters(FunctionInputOutputTypeChecker()),
        )
    if bool(config.get("enable_hamilton_node_diagnostics", True)):
        builder = cast(
            "async_driver.Builder",
            builder.with_adapters(DiagnosticsNodeHook(diagnostics)),
        )
    if bool(config.get("enable_otel_node_tracing", True)):
        builder = cast(
            "async_driver.Builder",
            builder.with_adapters(OtelNodeHook()),
        )
    if bool(config.get("enable_plan_diagnostics", True)):
        builder = cast(
            "async_driver.Builder",
            builder.with_adapters(
                PlanDiagnosticsHook(plan=plan, profile=profile, collector=diagnostics)
            ),
        )
    if bool(config.get("enable_otel_plan_tracing", True)):
        builder = cast(
            "async_driver.Builder",
            builder.with_adapters(OtelPlanHook()),
        )
    return builder


@dataclass(frozen=True)
class DriverBuildRequest:
    """Inputs required to assemble a Hamilton driver."""

    config: Mapping[str, JsonValue]
    modules: Sequence[ModuleType] | None = None
    view_ctx: ViewGraphContext | None = None
    plan: ExecutionPlan | None = None
    execution_mode: ExecutionMode | None = None
    executor_config: ExecutorConfig | None = None
    graph_adapter_config: GraphAdapterConfig | None = None


def build_driver(*, request: DriverBuildRequest) -> driver.Driver:
    """Build a Hamilton Driver for the pipeline.

    Key knobs supported via config:
      - cache_path: str | None
      - cache_opt_in: bool (if True, default_behavior="disable")
      - enable_hamilton_tracker and tracker config keys

    Returns
    -------
    driver.Driver
        Built Hamilton driver instance.
    """
    modules = list(request.modules) if request.modules is not None else default_modules()
    resolved_view_ctx = request.view_ctx or _view_graph_context(request.config)
    resolved_plan = request.plan or _compile_plan(
        resolved_view_ctx,
        request.config,
    )
    if request.plan is None:
        resolved_plan = _plan_with_incremental_pruning(
            view_ctx=resolved_view_ctx,
            plan=resolved_plan,
            config=request.config,
        )
    modules.append(build_execution_plan_module(resolved_plan))
    modules.append(
        build_task_execution_module(
            plan=resolved_plan,
            options=TaskExecutionModuleOptions(),
        )
    )

    config_payload = dict(request.config)
    config_payload.setdefault(
        "runtime_profile_name_override",
        _runtime_profile_name(config_payload),
    )
    determinism_override = _determinism_override(config_payload)
    if determinism_override is not None:
        config_payload.setdefault(
            "determinism_override_override",
            determinism_override.value,
        )
    config_payload.setdefault(
        "enable_dynamic_scan_units",
        (request.execution_mode or ExecutionMode.PLAN_PARALLEL)
        != ExecutionMode.DETERMINISTIC_SERIAL,
    )
    config_payload.setdefault("hamilton.enable_power_user_mode", True)
    config_payload = _apply_tracker_config_from_profile(
        config_payload,
        profile_spec=resolved_view_ctx.runtime_profile_spec,
    )
    config_payload = _with_graph_tags(config_payload, plan=resolved_plan)

    diagnostics = DiagnosticsCollector()
    set_hamilton_diagnostics_collector(diagnostics)

    builder = driver.Builder().allow_module_overrides()
    builder = builder.with_modules(*modules).with_config(config_payload)
    builder = _apply_dynamic_execution(
        builder,
        options=DynamicExecutionOptions(
            config=config_payload,
            plan=resolved_plan,
            diagnostics=diagnostics,
            execution_mode=request.execution_mode or ExecutionMode.PLAN_PARALLEL,
            executor_config=request.executor_config,
        ),
    )
    builder = _apply_graph_adapter(
        builder,
        config=config_payload,
        executor_config=request.executor_config,
        adapter_config=request.graph_adapter_config,
    )
    builder = _apply_cache(builder, config=config_payload)
    builder = _apply_materializers(builder, config=config_payload)
    builder = _apply_adapters(
        builder,
        config=config_payload,
        diagnostics=diagnostics,
        plan=resolved_plan,
        profile=resolved_view_ctx.profile,
    )
    semantic_registry_hook: SemanticRegistryHook | None = None
    if bool(config_payload.get("enable_semantic_registry", True)):
        from hamilton_pipeline.semantic_registry import (
            SemanticRegistryHook as _SemanticRegistryHook,
        )

        semantic_registry_hook = _SemanticRegistryHook(
            profile=resolved_view_ctx.profile,
            plan_signature=resolved_plan.plan_signature,
            config=config_payload,
        )
        builder = builder.with_adapters(semantic_registry_hook)
    cache_lineage_hook: CacheLineageHook | None = None
    cache_path = config_payload.get("cache_path")
    if isinstance(cache_path, str) and cache_path:
        from hamilton_pipeline.cache_lineage import (
            CacheLineageHook as _CacheLineageHook,
        )

        cache_lineage_hook = _CacheLineageHook(
            profile=resolved_view_ctx.profile,
            config=config_payload,
            plan_signature=resolved_plan.plan_signature,
        )
        builder = builder.with_adapters(cache_lineage_hook)
    driver_instance = builder.build()
    if semantic_registry_hook is not None:
        semantic_registry_hook.bind_driver(driver_instance)
    if cache_lineage_hook is not None:
        cache_lineage_hook.bind_driver(driver_instance)
    return driver_instance


async def build_async_driver(*, request: DriverBuildRequest) -> async_driver.AsyncDriver:
    """Build an async Hamilton driver for IO-bound execution flows.

    Returns
    -------
    async_driver.AsyncDriver
        Built async Hamilton driver instance.

    Raises
    ------
    ValueError
        Raised when dynamic scan units are enabled for async execution.
    """
    modules = list(request.modules) if request.modules is not None else default_modules()
    resolved_view_ctx = request.view_ctx or _view_graph_context(request.config)
    resolved_plan = request.plan or _compile_plan(
        resolved_view_ctx,
        request.config,
    )
    if request.plan is None:
        resolved_plan = _plan_with_incremental_pruning(
            view_ctx=resolved_view_ctx,
            plan=resolved_plan,
            config=request.config,
        )
    modules.append(build_execution_plan_module(resolved_plan))
    modules.append(
        build_task_execution_module(
            plan=resolved_plan,
            options=TaskExecutionModuleOptions(),
        )
    )

    config_payload = dict(request.config)
    config_payload.setdefault(
        "runtime_profile_name_override",
        _runtime_profile_name(config_payload),
    )
    determinism_override = _determinism_override(config_payload)
    if determinism_override is not None:
        config_payload.setdefault(
            "determinism_override_override",
            determinism_override.value,
        )
    if bool(config_payload.get("enable_dynamic_scan_units")):
        msg = "Async driver does not support dynamic scan units."
        raise ValueError(msg)
    config_payload["enable_dynamic_scan_units"] = False
    config_payload.setdefault("hamilton.enable_power_user_mode", True)
    config_payload = _apply_tracker_config_from_profile(
        config_payload,
        profile_spec=resolved_view_ctx.runtime_profile_spec,
    )
    config_payload = _with_graph_tags(config_payload, plan=resolved_plan)

    diagnostics = DiagnosticsCollector()
    set_hamilton_diagnostics_collector(diagnostics)

    builder = cast("async_driver.Builder", async_driver.Builder().allow_module_overrides())
    builder = cast(
        "async_driver.Builder",
        builder.with_modules(*modules).with_config(config_payload),
    )
    builder = _apply_async_adapters(
        builder,
        config=config_payload,
        diagnostics=diagnostics,
        plan=resolved_plan,
        profile=resolved_view_ctx.profile,
    )
    if bool(config_payload.get("enable_semantic_registry", True)):
        from hamilton_pipeline.semantic_registry import (
            SemanticRegistryHook as _SemanticRegistryHook,
        )

        semantic_registry_hook = _SemanticRegistryHook(
            profile=resolved_view_ctx.profile,
            plan_signature=resolved_plan.plan_signature,
            config=config_payload,
        )
        builder = builder.with_adapters(semantic_registry_hook)
    built = builder.build()
    if inspect.isawaitable(built):
        return await built
    return cast("async_driver.AsyncDriver", built)


def _relation_output_schema(session_runtime: SessionRuntime) -> SchemaLike:
    if not session_runtime.ctx.table_exist(RELATION_OUTPUT_NAME):
        msg = f"Relation output view {RELATION_OUTPUT_NAME!r} is not registered."
        raise ValueError(msg)
    schema = session_runtime.ctx.table(RELATION_OUTPUT_NAME).schema()
    return cast("SchemaLike", schema)


@dataclass
class DriverFactory:
    """
    Caches built Hamilton Drivers by plan-aware fingerprint.

    Use this when embedding the pipeline into a service where config changes
    are relatively infrequent but executions are frequent.
    """

    modules: Sequence[ModuleType] | None = None
    # fingerprint -> driver.Driver
    _cache: dict[str, driver.Driver] = field(default_factory=dict)

    def get(
        self,
        config: Mapping[str, JsonValue],
        *,
        execution_mode: ExecutionMode | None = None,
        executor_config: ExecutorConfig | None = None,
        graph_adapter_config: GraphAdapterConfig | None = None,
    ) -> driver.Driver:
        """Return a cached driver for the given config.

        Returns
        -------
        driver.Driver
            Cached or newly built Hamilton driver.
        """
        resolved_mode = execution_mode or ExecutionMode.PLAN_PARALLEL
        view_ctx = _view_graph_context(config)
        plan = _compile_plan(view_ctx, config)
        plan = _plan_with_incremental_pruning(
            view_ctx=view_ctx,
            plan=plan,
            config=config,
        )
        key = driver_cache_key(
            config,
            plan_signature=plan.plan_signature,
            execution_mode=resolved_mode,
            executor_config=executor_config,
            graph_adapter_config=graph_adapter_config,
        )
        cached = self._cache.get(key)
        if cached is None:
            cached = build_driver(
                request=DriverBuildRequest(
                    config=config,
                    modules=self.modules,
                    view_ctx=view_ctx,
                    plan=plan,
                    execution_mode=resolved_mode,
                    executor_config=executor_config,
                    graph_adapter_config=graph_adapter_config,
                )
            )
            self._cache[key] = cached
        return cached
