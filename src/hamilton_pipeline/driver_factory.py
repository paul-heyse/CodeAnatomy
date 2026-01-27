"""Hamilton driver construction helpers for the pipeline."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Literal, TypedDict, cast

import pyarrow as pa
from hamilton import driver
from hamilton.execution import executors
from hamilton.lifecycle import FunctionInputOutputTypeChecker
from hamilton.lifecycle import base as lifecycle_base

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from core_types import JsonValue
from engine.runtime_profile import resolve_runtime_profile
from hamilton_pipeline import modules as hamilton_modules
from hamilton_pipeline.lifecycle import (
    DiagnosticsNodeHook,
    PlanDiagnosticsHook,
    set_hamilton_diagnostics_collector,
)
from hamilton_pipeline.modules.execution_plan import build_execution_plan_module
from hamilton_pipeline.task_module_builder import (
    TaskExecutionModuleOptions,
    build_task_execution_module,
)
from obs.diagnostics import DiagnosticsCollector
from relspec.view_defs import RELATION_OUTPUT_NAME

if TYPE_CHECKING:
    from datafusion import SessionContext
    from hamilton.io.materialization import MaterializerFactory

    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.view_graph_registry import ViewNode
    from hamilton_pipeline.cache_lineage import CacheLineageHook
    from hamilton_pipeline.semantic_registry import SemanticRegistryHook
    from relspec.execution_plan import ExecutionPlan
    from relspec.incremental import IncrementalDiff
from storage.ipc import ipc_hash

try:
    from hamilton_sdk import adapters as hamilton_adapters
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    hamilton_adapters = None


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

    Hamilton build-time config is immutable after build; if config changes, rebuild a new driver.
    The Hamilton docs recommend a small driver-factory that caches by config fingerprint.
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
) -> str:
    """Compute a plan-aware driver cache key.

    Returns
    -------
    str
        SHA-256 fingerprint for the config and plan signature.
    """
    payload = {
        "version": 2,
        "plan_signature": plan_signature,
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


@dataclass(frozen=True)
class ViewGraphContext:
    """Runtime context needed to compile the execution plan."""

    ctx: ExecutionContext
    profile: DataFusionRuntimeProfile
    session: SessionContext
    snapshot: Mapping[str, object]
    view_nodes: tuple[ViewNode, ...]


def _view_graph_context(config: Mapping[str, JsonValue]) -> ViewGraphContext:
    runtime_profile_spec = resolve_runtime_profile(
        _runtime_profile_name(config),
        determinism=_determinism_override(config),
    )
    runtime_profile_spec.runtime.apply_global_thread_pools()
    ctx = ExecutionContext(runtime=runtime_profile_spec.runtime)
    profile = runtime_profile_spec.runtime.datafusion
    if profile is None:
        msg = "DataFusion runtime profile is required for view graph scheduling."
        raise ValueError(msg)
    from cpg.kind_catalog import validate_edge_kind_requirements
    from datafusion_engine.view_registry import ensure_view_graph
    from datafusion_engine.view_registry_specs import view_graph_nodes

    session = profile.session_context()
    snapshot = ensure_view_graph(
        session,
        runtime_profile=profile,
        include_registry_views=True,
    )
    validate_edge_kind_requirements(_relation_output_schema(session))
    nodes = view_graph_nodes(session, snapshot=snapshot)
    return ViewGraphContext(
        ctx=ctx,
        profile=profile,
        session=session,
        snapshot=snapshot,
        view_nodes=tuple(nodes),
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


def _compile_plan(view_ctx: ViewGraphContext, config: Mapping[str, JsonValue]) -> ExecutionPlan:
    from relspec.execution_plan import ExecutionPlanRequest, compile_execution_plan

    requested = _task_name_list_from_config(config, key="plan_requested_tasks")
    impacted = _task_name_list_from_config(config, key="plan_impacted_tasks")
    allow_partial = bool(config.get("plan_allow_partial", False))
    request = ExecutionPlanRequest(
        view_nodes=view_ctx.view_nodes,
        snapshot=view_ctx.snapshot,
        requested_task_names=requested,
        impacted_task_names=impacted,
        allow_partial=allow_partial,
    )
    return compile_execution_plan(session=view_ctx.session, request=request)


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
        runtime = IncrementalRuntime.build(ctx=view_ctx.ctx)
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
    impacted = downstream_task_closure(plan.task_graph, rebuild_active) & active
    if not impacted:
        return active
    impacted_with_deps = upstream_task_closure(plan.task_graph, impacted) & active
    if not plan.requested_task_names:
        return impacted_with_deps
    requested_anchor = upstream_task_closure(plan.task_graph, plan.requested_task_names) & active
    return impacted_with_deps | requested_anchor


def _plan_with_incremental_pruning(
    *,
    view_ctx: ViewGraphContext,
    plan: ExecutionPlan,
    config: Mapping[str, JsonValue],
) -> ExecutionPlan:
    diff, state_dir = _precompute_incremental_diff(view_ctx=view_ctx, plan=plan, config=config)
    if diff is None and state_dir is None:
        return plan
    plan_with_diff = replace(
        plan,
        incremental_diff=diff,
        incremental_state_dir=state_dir,
    )
    if diff is None:
        return plan_with_diff
    active_from_diff = _active_tasks_from_incremental_diff(plan=plan_with_diff, diff=diff)
    if active_from_diff == set(plan_with_diff.active_tasks):
        return plan_with_diff
    from relspec.execution_plan import prune_execution_plan

    return prune_execution_plan(plan_with_diff, active_tasks=active_from_diff)


def _maybe_build_tracker_adapter(
    config: Mapping[str, JsonValue],
) -> lifecycle_base.LifecycleAdapter | None:
    """Build an optional Hamilton UI tracker adapter.

    Docs show:
      tracker = adapters.HamiltonTracker(project_id=..., username=..., dag_name=..., tags=...)
      Builder().with_modules(...).with_config(...).with_adapters(tracker).build()
      :contentReference[oaicite:3]{index=3}

    Returns
    -------
    object | None
        Tracker adapter when enabled and available.
    """
    if hamilton_adapters is None:
        return None

    enable = bool(config.get("enable_hamilton_tracker", False))
    if not enable:
        return None

    project_id_value = config.get("hamilton_project_id")
    project_id: int | None = None
    if isinstance(project_id_value, int) and not isinstance(project_id_value, bool):
        project_id = project_id_value
    elif isinstance(project_id_value, str) and project_id_value.isdigit():
        project_id = int(project_id_value)

    username = config.get("hamilton_username")
    if project_id is None or not isinstance(username, str):
        return None

    dag_name_value = config.get("hamilton_dag_name")
    dag_name = dag_name_value if isinstance(dag_name_value, str) else "codeintel_cpg_v1"

    tags_value = config.get("hamilton_tags")
    tags: dict[str, str] = {}
    if isinstance(tags_value, Mapping):
        tags = {str(k): str(v) for k, v in tags_value.items()}

    api_url_value = config.get("hamilton_api_url")
    api_url = api_url_value if isinstance(api_url_value, str) else None
    ui_url_value = config.get("hamilton_ui_url")
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


def _with_graph_tags(
    config: Mapping[str, JsonValue],
    *,
    graph_signature: str,
) -> dict[str, JsonValue]:
    config_payload = dict(config)
    config_payload.setdefault("hamilton_dag_name", f"codeintel_{graph_signature}")
    tags_value = config_payload.get("hamilton_tags")
    merged_tags: dict[str, str] = {"plan_signature": graph_signature}
    if isinstance(tags_value, Mapping):
        merged_tags.update({str(k): str(v) for k, v in tags_value.items()})
    config_payload["hamilton_tags"] = merged_tags
    return config_payload


def _apply_dynamic_execution(
    builder: driver.Builder,
    *,
    config: Mapping[str, JsonValue],
    plan: ExecutionPlan,
    diagnostics: DiagnosticsCollector,
) -> driver.Builder:
    if not bool(config.get("enable_dynamic_execution", False)):
        return builder
    max_tasks_value = config.get("max_tasks")
    max_tasks = 4
    if isinstance(max_tasks_value, int) and not isinstance(max_tasks_value, bool):
        max_tasks = max_tasks_value
    from hamilton_pipeline.scheduling_hooks import (
        plan_grouping_strategy,
        plan_task_grouping_hook,
        plan_task_submission_hook,
    )

    enable_submission_hook = bool(config.get("enable_plan_task_submission_hook", True))
    enable_grouping_hook = bool(config.get("enable_plan_task_grouping_hook", True))
    enforce_submission = bool(config.get("enforce_plan_task_submission", True))

    dynamic_builder = (
        builder.enable_dynamic_execution(allow_experimental_mode=True)
        .with_local_executor(executors.SynchronousLocalTaskExecutor())
        .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=max_tasks))
        .with_grouping_strategy(plan_grouping_strategy(plan))
    )
    if enable_submission_hook:
        dynamic_builder = dynamic_builder.with_adapters(
            plan_task_submission_hook(
                plan,
                diagnostics,
                enforce_active=enforce_submission,
            )
        )
    if enable_grouping_hook:
        dynamic_builder = dynamic_builder.with_adapters(plan_task_grouping_hook(plan, diagnostics))
    return dynamic_builder


@lru_cache(maxsize=1)
def _ensure_materializer_registry() -> None:
    from hamilton.io.default_data_loaders import DATA_ADAPTERS
    from hamilton.registry import register_adapter

    for adapter in DATA_ADAPTERS:
        register_adapter(adapter)


def _materializer_base_dir(config: Mapping[str, JsonValue]) -> Path | None:
    for key in ("materializer_output_dir", "output_dir", "work_dir"):
        value = config.get(key)
        if isinstance(value, str) and value.strip():
            return Path(value).expanduser()
    return None


def _build_materializers(
    config: Mapping[str, JsonValue],
) -> list[MaterializerFactory]:
    if not bool(config.get("enable_materializers", False)):
        return []
    base_dir = _materializer_base_dir(config)
    if base_dir is None:
        return []
    base_dir.mkdir(parents=True, exist_ok=True)
    _ensure_materializer_registry()
    from hamilton.io import materialization

    materializers: list[MaterializerFactory] = [
        materialization.to.json(
            id="materialize_run_manifest",
            dependencies=["write_run_manifest_delta"],
            path=str(base_dir / "run_manifest.json"),
        ),
        materialization.to.json(
            id="materialize_extract_errors",
            dependencies=["write_extract_error_artifacts_delta"],
            path=str(base_dir / "extract_errors.json"),
        ),
        materialization.to.json(
            id="materialize_normalize_outputs",
            dependencies=["write_normalize_outputs_delta"],
            path=str(base_dir / "normalize_outputs.json"),
        ),
        materialization.to.json(
            id="materialize_run_bundle",
            dependencies=["write_run_bundle_dir"],
            path=str(base_dir / "run_bundle.json"),
        ),
    ]
    return materializers


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
    default_behavior = (
        _cache_behavior_override(config, "cache_default_behavior") or default_behavior
    )
    default_loader_behavior = (
        _cache_behavior_override(config, "cache_default_loader_behavior") or default_loader_behavior
    )
    default_saver_behavior = (
        _cache_behavior_override(config, "cache_default_saver_behavior") or default_saver_behavior
    )
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
    cache_path = config.get("cache_path")
    if not isinstance(cache_path, str) or not cache_path:
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
    if bool(config.get("enable_plan_diagnostics", True)):
        builder = builder.with_adapters(
            PlanDiagnosticsHook(plan=plan, profile=profile, collector=diagnostics)
        )
    return builder


def build_driver(
    *,
    config: Mapping[str, JsonValue],
    modules: Sequence[ModuleType] | None = None,
    view_ctx: ViewGraphContext | None = None,
    plan: ExecutionPlan | None = None,
) -> driver.Driver:
    """Build a Hamilton Driver for the pipeline.

    Key knobs supported via config:
      - enable_dynamic_execution: bool (optional)
      - cache_path: str | None
      - cache_opt_in: bool (if True, default_behavior="disable")
      - enable_hamilton_tracker + tracker config keys

    Returns
    -------
    driver.Driver
        Built Hamilton driver instance.
    """
    modules = list(modules) if modules is not None else default_modules()
    resolved_view_ctx = view_ctx or _view_graph_context(config)
    resolved_plan = plan or _compile_plan(resolved_view_ctx, config)
    if plan is None:
        resolved_plan = _plan_with_incremental_pruning(
            view_ctx=resolved_view_ctx,
            plan=resolved_plan,
            config=config,
        )
    modules.append(build_execution_plan_module(resolved_plan))
    modules.append(
        build_task_execution_module(plan=resolved_plan, options=TaskExecutionModuleOptions())
    )

    config_payload = _with_graph_tags(config, graph_signature=resolved_plan.plan_signature)
    config_payload.setdefault("runtime_profile_name_override", _runtime_profile_name(config))
    determinism_override = _determinism_override(config)
    if determinism_override is not None:
        config_payload.setdefault("determinism_override_override", determinism_override.value)

    diagnostics = DiagnosticsCollector()
    set_hamilton_diagnostics_collector(diagnostics)

    builder = (
        driver.Builder().allow_module_overrides().with_modules(*modules).with_config(config_payload)
    )
    builder = _apply_dynamic_execution(
        builder,
        config=config_payload,
        plan=resolved_plan,
        diagnostics=diagnostics,
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
        from hamilton_pipeline.cache_lineage import CacheLineageHook as _CacheLineageHook

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


def _relation_output_schema(session: SessionContext) -> SchemaLike:
    if not session.table_exist(RELATION_OUTPUT_NAME):
        msg = f"Relation output view {RELATION_OUTPUT_NAME!r} is not registered."
        raise ValueError(msg)
    return cast("SchemaLike", session.table(RELATION_OUTPUT_NAME).schema())


@dataclass
class DriverFactory:
    """
    Caches built Hamilton Drivers by plan-aware fingerprint.

    Use this if you're embedding the pipeline into a service where config changes
    are relatively infrequent but executions are frequent.
    """

    modules: Sequence[ModuleType] | None = None
    _cache: dict[str, driver.Driver] = field(default_factory=dict)  # fingerprint -> Driver

    def get(self, config: Mapping[str, JsonValue]) -> driver.Driver:
        """Return a cached driver for the given config.

        Returns
        -------
        driver.Driver
            Cached or newly built Hamilton driver.
        """
        view_ctx = _view_graph_context(config)
        plan = _compile_plan(view_ctx, config)
        plan = _plan_with_incremental_pruning(view_ctx=view_ctx, plan=plan, config=config)
        key = driver_cache_key(config, plan_signature=plan.plan_signature)
        cached = self._cache.get(key)
        if cached is None:
            cached = build_driver(
                config=config,
                modules=self.modules,
                view_ctx=view_ctx,
                plan=plan,
            )
            self._cache[key] = cached
        return cached
