"""Hamilton driver construction helpers for the pipeline."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast

from hamilton import driver
from hamilton.execution import executors
from hamilton.lifecycle import base as lifecycle_base
from opentelemetry import trace as otel_trace

from core.config_base import FingerprintableConfig
from core.config_base import config_fingerprint as hash_config_fingerprint
from core_types import DeterminismTier, JsonValue, parse_determinism_tier
from datafusion_engine.arrow.interop import SchemaLike, TableLike
from engine.runtime_profile import RuntimeProfileSpec, resolve_runtime_profile
from hamilton_pipeline import modules as hamilton_modules
from hamilton_pipeline.driver_builder import DriverBuilder
from hamilton_pipeline.execution_manager import PlanExecutionManager
from hamilton_pipeline.hamilton_tracker import CodeAnatomyHamiltonTracker
from hamilton_pipeline.lifecycle import (
    DiagnosticsNodeHook,
    PlanDiagnosticsHook,
    set_hamilton_diagnostics_collector,
)
from hamilton_pipeline.materializers import build_hamilton_materializers
from hamilton_pipeline.modules.execution_plan import build_execution_plan_module
from hamilton_pipeline.task_module_builder import (
    TaskExecutionModuleOptions,
    build_task_execution_module,
)
from hamilton_pipeline.type_checking import CodeAnatomyTypeChecker
from hamilton_pipeline.types import (
    ExecutionMode,
    ExecutorConfig,
    ExecutorKind,
    GraphAdapterConfig,
    GraphAdapterKind,
)
from obs.diagnostics import DiagnosticsCollector
from obs.otel.hamilton import OtelNodeHook, OtelPlanHook
from obs.otel.run_context import get_run_id
from relspec.view_defs import RELATION_OUTPUT_NAME
from utils.env_utils import env_bool, env_value
from utils.hashing import CacheKeyBuilder

if TYPE_CHECKING:
    from hamilton.base import HamiltonGraphAdapter
    from hamilton.io.materialization import MaterializerFactory

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
    from datafusion_engine.views.graph import ViewNode
    from hamilton_pipeline.cache_lineage import CacheLineageHook
    from hamilton_pipeline.graph_snapshot import GraphSnapshotHook
    from relspec.execution_plan import ExecutionPlan

try:
    from hamilton_sdk import adapters as hamilton_adapters
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    hamilton_adapters = None

_DEFAULT_DAG_NAME = "codeintel::semantic_v1"
_SEMANTIC_VERSION = "v1"
_PACKED_REF_FIELDS = 2


def _ensure_hamilton_dataframe_types() -> None:
    from hamilton import htypes
    from hamilton import registry as hamilton_registry

    registered = hamilton_registry.get_registered_dataframe_types()
    if any(htypes.custom_subclass_check(TableLike, df_type) for df_type in registered.values()):
        return
    hamilton_registry.register_types("datafusion", TableLike, None)


def default_modules() -> list[ModuleType]:
    """Return the default Hamilton module set for the pipeline.

    Returns
    -------
    list[ModuleType]
        Default module list for the pipeline.
    """
    return hamilton_modules.load_all_modules()


def driver_config_fingerprint(config: Mapping[str, JsonValue]) -> str:
    """Compute a stable driver config fingerprint for caching.

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
    return hash_config_fingerprint(payload)


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
    builder = CacheKeyBuilder(prefix="driver")
    builder.add("version", 3)
    builder.add("plan_signature", plan_signature)
    builder.add("execution_mode", execution_mode.value)
    builder.add("executor_config", _executor_config_payload(executor_config))
    builder.add("graph_adapter_config", _graph_adapter_config_payload(graph_adapter_config))
    builder.add("config", dict(config))
    return builder.build()


def _runtime_profile_name(config: Mapping[str, JsonValue]) -> str:
    value = config.get("runtime_profile_name")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return env_value("CODEANATOMY_RUNTIME_PROFILE") or "default"


def _determinism_override(config: Mapping[str, JsonValue]) -> DeterminismTier | None:
    value = config.get("determinism_override")
    if isinstance(value, str):
        resolved = parse_determinism_tier(value)
        if resolved is not None:
            return resolved
    force_flag = env_bool("CODEANATOMY_FORCE_TIER2", default=False, on_invalid="false")
    if force_flag:
        return DeterminismTier.CANONICAL
    tier = env_value("CODEANATOMY_DETERMINISM_TIER")
    return parse_determinism_tier(tier)


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


def build_view_graph_context(config: Mapping[str, JsonValue]) -> ViewGraphContext:
    """Build a view graph context from runtime configuration.

    **EXECUTION AUTHORITY: VIEW GRAPH REGISTRATION**

    This function ensures all semantic views are registered via the view graph
    infrastructure BEFORE Hamilton execution begins. Hamilton nodes consume
    these pre-registered views via ``source()`` inputs; they do NOT re-register.

    The registration happens in ``ensure_view_graph()`` which delegates to
    ``registry_specs.view_graph_nodes()`` - the single source of truth for
    all view definitions including semantic views.

    Returns
    -------
    ViewGraphContext
        Resolved view graph context with runtime metadata.

    See Also
    --------
    datafusion_engine.views.registry_specs._semantics_view_nodes : Semantic view registration.
    hamilton_pipeline.modules.subdags : Hamilton consumer of semantic outputs.
    """
    runtime_profile_spec = resolve_runtime_profile(
        _runtime_profile_name(config),
        determinism=_determinism_override(config),
    )
    profile = runtime_profile_spec.datafusion
    from cpg.kind_catalog import validate_edge_kind_requirements
    from datafusion_engine.session.runtime import refresh_session_runtime
    from datafusion_engine.views.registration import ensure_view_graph
    from datafusion_engine.views.registry_specs import view_graph_nodes
    from semantics.ir_pipeline import build_semantic_ir

    session_runtime = profile.session_runtime()
    semantic_ir = build_semantic_ir()
    # Single registration point: ensure_view_graph registers ALL views including
    # semantic views via registry_specs.view_graph_nodes(). Hamilton consumes only.
    snapshot = ensure_view_graph(
        session_runtime.ctx,
        runtime_profile=profile,
        semantic_ir=semantic_ir,
    )
    session_runtime = refresh_session_runtime(profile, ctx=session_runtime.ctx)
    validate_edge_kind_requirements(_relation_output_schema(session_runtime))
    nodes = view_graph_nodes(
        session_runtime.ctx,
        snapshot=snapshot,
        runtime_profile=profile,
        semantic_ir=semantic_ir,
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
    mode = (env_value("CODEANATOMY_PIPELINE_MODE") or "").lower()
    return mode in {"incremental", "streaming"}


def _resolve_incremental_state_dir(config: Mapping[str, JsonValue]) -> Path | None:
    if not _incremental_enabled(config):
        return None
    repo_root_value = config.get("repo_root")
    if not isinstance(repo_root_value, str) or not repo_root_value.strip():
        return None
    repo_root = Path(repo_root_value).expanduser()
    state_dir_value = config.get("incremental_state_dir")
    state_dir_env = env_value("CODEANATOMY_STATE_DIR")
    state_dir = state_dir_value if isinstance(state_dir_value, str) else state_dir_env
    if isinstance(state_dir, str) and state_dir.strip():
        return repo_root / Path(state_dir)
    return repo_root / "build" / "state"


def _cdf_impacted_tasks(
    *,
    view_ctx: ViewGraphContext,
    plan: ExecutionPlan,
    config: Mapping[str, JsonValue],
) -> tuple[tuple[str, ...] | None, str | None]:
    state_dir = _resolve_incremental_state_dir(config)
    if state_dir is None:
        return None, None
    from datafusion_engine.dataset.registry import dataset_catalog_from_profile
    from relspec.incremental import CdfImpactRequest, impacted_tasks_for_cdf
    from semantics.incremental.cdf_cursors import CdfCursorStore
    from semantics.incremental.delta_context import DeltaAccessContext
    from semantics.incremental.runtime import IncrementalRuntime
    from semantics.incremental.state_store import StateStore

    try:
        runtime = IncrementalRuntime.build(
            profile=view_ctx.profile,
            determinism_tier=view_ctx.determinism_tier,
        )
    except ValueError:
        return None, str(state_dir)
    context = DeltaAccessContext(runtime=runtime)
    state_store = StateStore(root=state_dir)
    cursor_store = CdfCursorStore(cursors_path=state_store.cdf_cursors_path())
    catalog = dataset_catalog_from_profile(view_ctx.profile)
    impacted = impacted_tasks_for_cdf(
        CdfImpactRequest(
            graph=plan.task_graph,
            catalog=catalog,
            context=context,
            cursor_store=cursor_store,
            evidence=plan.evidence,
        )
    )
    return impacted, str(state_dir)


def _active_tasks_from_impacted(
    *,
    plan: ExecutionPlan,
    impacted_tasks: Sequence[str],
) -> set[str]:
    from relspec.execution_plan import downstream_task_closure, upstream_task_closure

    active = set(plan.active_tasks)
    if not impacted_tasks:
        return active
    impacted = downstream_task_closure(plan.task_graph, impacted_tasks)
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
    impacted, state_dir = _cdf_impacted_tasks(
        view_ctx=view_ctx,
        plan=plan,
        config=config,
    )
    if impacted is None and state_dir is None:
        return plan
    impacted_names = impacted or ()
    plan_with_impacts = replace(
        plan,
        impacted_task_names=tuple(sorted(set(impacted_names))) or plan.impacted_task_names,
    )
    if impacted is None:
        return plan_with_impacts
    active_from_cdf = _active_tasks_from_impacted(
        plan=plan_with_impacts,
        impacted_tasks=impacted_names,
    )
    if active_from_cdf == set(plan_with_impacts.active_tasks):
        return plan_with_impacts
    from relspec.execution_plan import prune_execution_plan

    return prune_execution_plan(
        plan_with_impacts,
        active_tasks=active_from_cdf,
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
        value = env_value(env_key)
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
    *,
    profile_spec: RuntimeProfileSpec,
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

    tracker = CodeAnatomyHamiltonTracker(
        **tracker_kwargs,
        run_tag_provider=_run_tag_provider(config, profile_spec=profile_spec),
    )
    return cast("lifecycle_base.LifecycleAdapter", tracker)


def _with_graph_tags(
    config: Mapping[str, JsonValue],
    *,
    plan: ExecutionPlan,
) -> dict[str, JsonValue]:
    config_payload = dict(config)
    dag_name = _resolve_dag_name(config_payload)
    config_payload["hamilton_dag_name"] = dag_name
    merged_tags = _merge_graph_tags(config_payload, plan=plan)
    config_payload["hamilton_tags"] = merged_tags
    return config_payload


def _resolve_dag_name(config_payload: Mapping[str, JsonValue]) -> str:
    dag_name_value = config_payload.get("hamilton_dag_name")
    if isinstance(dag_name_value, str) and dag_name_value:
        return dag_name_value
    return _DEFAULT_DAG_NAME


def _merge_graph_tags(
    config_payload: Mapping[str, JsonValue],
    *,
    plan: ExecutionPlan,
) -> dict[str, str]:
    merged_tags = _base_graph_tags(config_payload)
    _append_plan_tags(merged_tags, plan=plan)
    _append_telemetry_tags(merged_tags, config_payload)
    return merged_tags


def _base_graph_tags(config_payload: Mapping[str, JsonValue]) -> dict[str, str]:
    tags_value = config_payload.get("hamilton_tags")
    merged_tags: dict[str, str] = {}
    if isinstance(tags_value, Mapping):
        merged_tags.update({str(k): str(v) for k, v in tags_value.items()})
    runtime_env = _string_override(config_payload, "runtime_environment") or env_value(
        "CODEANATOMY_ENV"
    )
    if runtime_env:
        merged_tags.setdefault("environment", runtime_env)
    team_value = _string_override(config_payload, "runtime_team") or env_value("CODEANATOMY_TEAM")
    if team_value:
        merged_tags.setdefault("team", team_value)
    merged_tags.setdefault("runtime_profile", _runtime_profile_name(config_payload))
    determinism = _determinism_override(config_payload)
    if determinism is not None:
        merged_tags.setdefault("determinism_tier", determinism.value)
    telemetry_profile = _string_override(config_payload, "hamilton_telemetry_profile")
    if telemetry_profile is not None:
        merged_tags.setdefault("telemetry_profile", telemetry_profile)
    merged_tags.setdefault("semantic_version", _SEMANTIC_VERSION)
    repo_hash = _repo_hash_from_root(config_payload.get("repo_root"))
    if repo_hash:
        merged_tags.setdefault("repo_hash", repo_hash)
    return merged_tags


def _repo_hash_from_root(repo_root: object | None) -> str | None:
    if not isinstance(repo_root, str) or not repo_root.strip():
        return None
    root = Path(repo_root).expanduser()
    git_dir = root / ".git"
    head_value = _read_text(git_dir / "HEAD")
    if head_value is None:
        return None
    if not head_value.startswith("ref:"):
        return head_value
    ref_name = head_value.split("ref:", maxsplit=1)[-1].strip()
    if not ref_name:
        return None
    return _read_text(git_dir / ref_name) or _resolve_packed_ref(git_dir, ref_name)


def _read_text(path: Path) -> str | None:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return value or None


def _resolve_packed_ref(git_dir: Path, ref_name: str) -> str | None:
    packed_text = _read_text(git_dir / "packed-refs")
    if packed_text is None:
        return None
    for line in packed_text.splitlines():
        if not line or line.startswith(("#", "^")):
            continue
        parts = line.split()
        if len(parts) != _PACKED_REF_FIELDS:
            continue
        sha, ref = parts
        if ref == ref_name:
            return sha
    return None


def _append_plan_tags(tags: dict[str, str], *, plan: ExecutionPlan) -> None:
    tags["plan_signature"] = plan.plan_signature
    tags["reduced_plan_signature"] = plan.reduced_task_dependency_signature
    tags["task_dependency_signature"] = plan.task_dependency_signature
    tags["plan_task_count"] = str(len(plan.active_tasks))
    tags["plan_task_signature_count"] = str(len(plan.plan_task_signatures))
    tags["plan_generation_count"] = str(len(plan.task_schedule.generations))
    tags["plan_reduction_edge_count"] = str(plan.reduction_edge_count)
    tags["plan_reduction_removed_edge_count"] = str(plan.reduction_removed_edge_count)
    if plan.session_runtime_hash is not None:
        tags["session_runtime_hash"] = plan.session_runtime_hash
    tags["semantic_version"] = _SEMANTIC_VERSION
    if plan.critical_path_length_weighted is not None:
        tags["plan_critical_path_length_weighted"] = str(plan.critical_path_length_weighted)


def _append_telemetry_tags(
    tags: dict[str, str],
    config_payload: Mapping[str, JsonValue],
) -> None:
    capture_stats_value = config_payload.get("hamilton_capture_data_statistics")
    if isinstance(capture_stats_value, bool):
        tags.setdefault(
            "capture_data_statistics",
            "true" if capture_stats_value else "false",
        )
    max_list_value = config_payload.get("hamilton_max_list_length_capture")
    if isinstance(max_list_value, int):
        tags.setdefault("max_list_length_capture", str(max_list_value))
    max_dict_value = config_payload.get("hamilton_max_dict_length_capture")
    if isinstance(max_dict_value, int):
        tags.setdefault("max_dict_length_capture", str(max_dict_value))


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


def _apply_hamilton_telemetry_profile(
    config: dict[str, JsonValue],
    *,
    profile_spec: RuntimeProfileSpec,
) -> dict[str, JsonValue]:
    telemetry = getattr(profile_spec, "hamilton_telemetry", None)
    if telemetry is None:
        return config
    config.setdefault("hamilton_telemetry_profile", telemetry.name)
    config.setdefault("hamilton_capture_data_statistics", telemetry.capture_data_statistics)
    config.setdefault("hamilton_max_list_length_capture", telemetry.max_list_length_capture)
    config.setdefault("hamilton_max_dict_length_capture", telemetry.max_dict_length_capture)
    if "enable_hamilton_tracker" not in config:
        config["enable_hamilton_tracker"] = telemetry.enable_tracker
    return config


def _configure_hamilton_sdk_capture(
    config: Mapping[str, JsonValue],
    *,
    profile_spec: RuntimeProfileSpec,
) -> None:
    telemetry = getattr(profile_spec, "hamilton_telemetry", None)
    if telemetry is None:
        return
    if hamilton_adapters is None:
        return
    try:
        from hamilton_sdk.tracking import constants as sdk_constants
    except ModuleNotFoundError:
        return
    capture = config.get("hamilton_capture_data_statistics")
    capture_value = capture if isinstance(capture, bool) else telemetry.capture_data_statistics
    max_list = config.get("hamilton_max_list_length_capture")
    max_list_value = max_list if isinstance(max_list, int) else telemetry.max_list_length_capture
    max_dict = config.get("hamilton_max_dict_length_capture")
    max_dict_value = max_dict if isinstance(max_dict, int) else telemetry.max_dict_length_capture
    sdk_constants.CAPTURE_DATA_STATISTICS = capture_value
    sdk_constants.MAX_LIST_LENGTH_CAPTURE = max_list_value
    sdk_constants.MAX_DICT_LENGTH_CAPTURE = max_dict_value


def _run_tag_provider(
    config: Mapping[str, JsonValue],
    *,
    profile_spec: RuntimeProfileSpec,
) -> Callable[[], dict[str, str]]:
    runtime_profile_hash = profile_spec.runtime_profile_hash
    runtime_profile_name = profile_spec.name
    determinism_tier = profile_spec.determinism_tier.value
    telemetry_profile = (
        profile_spec.hamilton_telemetry.name
        if profile_spec.hamilton_telemetry is not None
        else None
    )
    repo_id = env_value("CODEANATOMY_REPO_ID")
    git_head_ref = env_value("CODEANATOMY_GIT_HEAD_REF")
    git_base_ref = env_value("CODEANATOMY_GIT_BASE_REF")
    runtime_env = _string_override(config, "runtime_environment") or env_value("CODEANATOMY_ENV")

    def provider() -> dict[str, str]:
        tags: dict[str, str] = {
            "runtime_profile_hash": runtime_profile_hash,
            "runtime_profile_name": runtime_profile_name,
            "determinism_tier": determinism_tier,
        }
        if telemetry_profile is not None:
            tags["telemetry_profile"] = telemetry_profile
        if runtime_env:
            tags["environment"] = runtime_env
        if repo_id:
            tags["repo_id"] = repo_id
        if git_head_ref:
            tags["git_head_ref"] = git_head_ref
        if git_base_ref:
            tags["git_base_ref"] = git_base_ref
        run_id = get_run_id()
        if run_id:
            tags["codeanatomy.run_id"] = run_id
        span = otel_trace.get_current_span()
        span_context = span.get_span_context()
        if span_context.is_valid:
            tags["otel.trace_id"] = f"{span_context.trace_id:032x}"
            tags["otel.span_id"] = f"{span_context.span_id:016x}"
        return tags

    return provider


def _resolve_config_payload(
    config: Mapping[str, JsonValue],
    *,
    profile_spec: RuntimeProfileSpec,
    plan: ExecutionPlan,
    execution_mode: ExecutionMode | None,
) -> dict[str, JsonValue]:
    config_payload = dict(config)
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
    if (execution_mode or ExecutionMode.PLAN_PARALLEL) == ExecutionMode.DETERMINISTIC_SERIAL:
        config_payload["enable_dynamic_scan_units"] = False
    else:
        config_payload.setdefault("enable_dynamic_scan_units", True)
    config_payload.setdefault("enable_output_validation", True)
    config_payload.setdefault("enable_graph_snapshot", True)
    config_payload.setdefault("hamilton.enable_power_user_mode", True)
    config_payload = _apply_tracker_config_from_profile(
        config_payload,
        profile_spec=profile_spec,
    )
    config_payload = _apply_hamilton_telemetry_profile(
        config_payload,
        profile_spec=profile_spec,
    )
    return _with_graph_tags(config_payload, plan=plan)


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
    return build_hamilton_materializers()


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
class CachePolicyProfile(FingerprintableConfig):
    """Explicit cache policy defaults for correctness boundaries."""

    name: str
    default_behavior: CacheBehavior
    default_loader_behavior: CacheBehavior
    default_saver_behavior: CacheBehavior
    log_to_file: bool

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for cache policy defaults.

        Returns
        -------
        Mapping[str, object]
            Payload describing cache policy defaults.
        """
        return {
            "name": self.name,
            "default_behavior": self.default_behavior,
            "default_loader_behavior": self.default_loader_behavior,
            "default_saver_behavior": self.default_saver_behavior,
            "log_to_file": self.log_to_file,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for cache policy defaults.

        Returns
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return hash_config_fingerprint(self.fingerprint_payload())


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
    profile_spec: RuntimeProfileSpec,
) -> driver.Builder:
    cache_path = _cache_path_from_config(config)
    if cache_path is None:
        return builder
    from hamilton.caching.stores.file import FileResultStore
    from hamilton.caching.stores.sqlite import SQLiteMetadataStore

    from hamilton_pipeline.cache_versioning import register_cache_fingerprinters

    base_path = Path(cache_path).expanduser()
    base_path.mkdir(parents=True, exist_ok=True)
    metadata_store = SQLiteMetadataStore(path=str(base_path / "meta.sqlite"))
    results_path = base_path / "results"
    results_path.mkdir(parents=True, exist_ok=True)
    result_store = FileResultStore(path=str(results_path))
    profile = _cache_policy_profile(config)
    register_cache_fingerprinters()
    default_nodes = _cache_default_nodes(config=config, profile_spec=profile_spec)
    return builder.with_cache(
        path=str(base_path),
        metadata_store=metadata_store,
        result_store=result_store,
        default=default_nodes if default_nodes else None,
        default_behavior=profile.default_behavior,
        default_loader_behavior=profile.default_loader_behavior,
        default_saver_behavior=profile.default_saver_behavior,
        log_to_file=profile.log_to_file,
    )


def _cache_default_nodes(
    *,
    config: Mapping[str, JsonValue],
    profile_spec: RuntimeProfileSpec,
) -> tuple[str, ...]:
    explicit = _task_name_list_from_config(config, key="cache_default_nodes")
    if explicit is not None:
        return explicit
    from datafusion_engine.semantics_runtime import semantic_runtime_from_profile
    from hamilton_pipeline.io_contracts import delta_output_specs
    from semantics.naming import internal_name

    defaults: set[str] = {spec.table_node for spec in delta_output_specs()}
    runtime_config = semantic_runtime_from_profile(profile_spec.datafusion)
    for dataset_name, policy in runtime_config.cache_policy_overrides.items():
        node_name = internal_name(dataset_name)
        if policy in {"delta_output", "delta_staging"}:
            defaults.add(node_name)
        if policy == "none" and node_name in defaults:
            defaults.remove(node_name)
    if not runtime_config.cache_policy_overrides and runtime_config.output_locations:
        for dataset_name in runtime_config.output_locations:
            defaults.add(internal_name(dataset_name))
    return tuple(sorted(defaults))


def _cache_path_from_config(config: Mapping[str, JsonValue]) -> str | None:
    value = config.get("cache_path")
    if not isinstance(value, str) or not value.strip():
        value = config.get("hamilton_cache_path")
    if not isinstance(value, str) or not value.strip():
        value = env_value("CODEANATOMY_HAMILTON_CACHE_PATH")
    if not isinstance(value, str) or not value.strip():
        value = env_value("HAMILTON_CACHE_PATH")
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


@dataclass(frozen=True)
class _AdapterContext:
    diagnostics: DiagnosticsCollector
    plan: ExecutionPlan
    profile: DataFusionRuntimeProfile
    profile_spec: RuntimeProfileSpec


def _apply_adapters(
    builder: driver.Builder,
    *,
    config: Mapping[str, JsonValue],
    context: _AdapterContext,
) -> driver.Builder:
    tracker = _maybe_build_tracker_adapter(config, profile_spec=context.profile_spec)
    if tracker is not None:
        builder = builder.with_adapters(tracker)
    if bool(config.get("enable_hamilton_type_checker", True)):
        builder = builder.with_adapters(CodeAnatomyTypeChecker())
    if bool(config.get("enable_hamilton_node_diagnostics", True)):
        builder = builder.with_adapters(DiagnosticsNodeHook(context.diagnostics))
    if bool(config.get("enable_otel_node_tracing", True)):
        builder = builder.with_adapters(OtelNodeHook())
    if bool(config.get("enable_plan_diagnostics", True)):
        builder = builder.with_adapters(
            PlanDiagnosticsHook(
                plan=context.plan,
                profile=context.profile,
                collector=context.diagnostics,
            )
        )
    if bool(config.get("enable_structured_run_logs", True)):
        from hamilton_pipeline.structured_logs import StructuredLogHook

        builder = builder.with_adapters(
            StructuredLogHook(
                profile=context.profile,
                config=config,
                plan_signature=context.plan.plan_signature,
            )
        )
    if bool(config.get("enable_otel_plan_tracing", True)):
        builder = builder.with_adapters(OtelPlanHook())
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


@dataclass(frozen=True)
class PlanContext:
    """Intermediate plan context for driver construction."""

    view_ctx: ViewGraphContext
    plan: ExecutionPlan
    modules: tuple[ModuleType, ...]
    config_payload: dict[str, JsonValue]
    diagnostics: DiagnosticsCollector
    execution_mode: ExecutionMode
    executor_config: ExecutorConfig | None
    graph_adapter_config: GraphAdapterConfig | None


def build_plan_context(
    *,
    request: DriverBuildRequest,
) -> PlanContext:
    """Build the plan context used by driver builders.

    Returns
    -------
    PlanContext
        Resolved execution plan context.
    """
    modules = list(request.modules) if request.modules is not None else default_modules()
    resolved_view_ctx = request.view_ctx or build_view_graph_context(request.config)
    if bool(request.config.get("enable_dataset_readiness", True)):
        from datafusion_engine.session.runtime import record_dataset_readiness
        from semantics.catalog.dataset_rows import get_all_dataset_rows

        dataset_names = tuple(row.name for row in get_all_dataset_rows() if row.role == "input")
        record_dataset_readiness(
            resolved_view_ctx.profile,
            dataset_names=dataset_names,
        )
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
    from hamilton_pipeline.validators import set_schema_contracts

    set_schema_contracts(resolved_plan.output_contracts)
    modules.append(build_execution_plan_module(resolved_plan))
    modules.append(
        build_task_execution_module(
            plan=resolved_plan,
            options=TaskExecutionModuleOptions(),
        )
    )
    execution_mode = request.execution_mode or ExecutionMode.PLAN_PARALLEL
    config_payload = _resolve_config_payload(
        request.config,
        profile_spec=resolved_view_ctx.runtime_profile_spec,
        plan=resolved_plan,
        execution_mode=execution_mode,
    )
    _configure_hamilton_sdk_capture(
        config_payload,
        profile_spec=resolved_view_ctx.runtime_profile_spec,
    )
    diagnostics = DiagnosticsCollector()
    set_hamilton_diagnostics_collector(diagnostics)
    return PlanContext(
        view_ctx=resolved_view_ctx,
        plan=resolved_plan,
        modules=tuple(modules),
        config_payload=config_payload,
        diagnostics=diagnostics,
        execution_mode=execution_mode,
        executor_config=request.executor_config,
        graph_adapter_config=request.graph_adapter_config,
    )


def build_driver_builder(plan_ctx: PlanContext) -> DriverBuilder:
    """Build a synchronous driver builder from a plan context.

    Returns
    -------
    DriverBuilder
        Builder wrapper for synchronous drivers.
    """
    _ensure_hamilton_dataframe_types()
    config_payload = plan_ctx.config_payload
    builder = driver.Builder().allow_module_overrides()
    builder = builder.with_modules(*plan_ctx.modules).with_config(config_payload)
    builder = _apply_dynamic_execution(
        builder,
        options=DynamicExecutionOptions(
            config=config_payload,
            plan=plan_ctx.plan,
            diagnostics=plan_ctx.diagnostics,
            execution_mode=plan_ctx.execution_mode,
            executor_config=plan_ctx.executor_config,
        ),
    )
    builder = _apply_graph_adapter(
        builder,
        config=config_payload,
        executor_config=plan_ctx.executor_config,
        adapter_config=plan_ctx.graph_adapter_config,
    )
    builder = _apply_cache(
        builder,
        config=config_payload,
        profile_spec=plan_ctx.view_ctx.runtime_profile_spec,
    )
    builder = _apply_materializers(builder, config=config_payload)
    adapter_context = _AdapterContext(
        diagnostics=plan_ctx.diagnostics,
        plan=plan_ctx.plan,
        profile=plan_ctx.view_ctx.profile,
        profile_spec=plan_ctx.view_ctx.runtime_profile_spec,
    )
    builder = _apply_adapters(
        builder,
        config=config_payload,
        context=adapter_context,
    )
    cache_lineage_hook: CacheLineageHook | None = None
    cache_path = config_payload.get("cache_path")
    if isinstance(cache_path, str) and cache_path:
        from hamilton_pipeline.cache_lineage import (
            CacheLineageHook as _CacheLineageHook,
        )

        cache_lineage_hook = _CacheLineageHook(
            profile=plan_ctx.view_ctx.profile,
            config=config_payload,
            plan_signature=plan_ctx.plan.plan_signature,
        )
        builder = builder.with_adapters(cache_lineage_hook)
    graph_snapshot_hook: GraphSnapshotHook | None = None
    if bool(config_payload.get("enable_graph_snapshot", True)):
        from hamilton_pipeline.graph_snapshot import (
            GraphSnapshotHook as _GraphSnapshotHook,
        )

        graph_snapshot_hook = _GraphSnapshotHook(
            profile=plan_ctx.view_ctx.profile,
            plan_signature=plan_ctx.plan.plan_signature,
            config=config_payload,
        )
        builder = builder.with_adapters(graph_snapshot_hook)
    return DriverBuilder(
        builder=builder,
        cache_lineage_hook=cache_lineage_hook,
        graph_snapshot_hook=graph_snapshot_hook,
    )


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
    plan_ctx = build_plan_context(request=request)
    builder = build_driver_builder(plan_ctx)
    return builder.build()


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
        view_ctx = build_view_graph_context(config)
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
