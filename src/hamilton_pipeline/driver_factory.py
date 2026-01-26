"""Hamilton driver construction helpers for the pipeline."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, TypedDict, cast

import pyarrow as pa
from hamilton import driver
from hamilton.execution import executors, grouping
from hamilton.lifecycle import FunctionInputOutputTypeChecker
from hamilton.lifecycle import base as lifecycle_base

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from core_types import JsonValue
from cpg.kind_catalog import validate_edge_kind_requirements
from datafusion_engine.view_registry import ensure_view_graph
from datafusion_engine.view_registry_specs import view_graph_nodes
from engine.runtime_profile import resolve_runtime_profile
from hamilton_pipeline import modules as hamilton_modules
from hamilton_pipeline.lifecycle import (
    DiagnosticsNodeHook,
    set_hamilton_diagnostics_collector,
)
from hamilton_pipeline.task_module_builder import (
    TaskExecutionModuleOptions,
    build_task_execution_module,
)
from obs.diagnostics import DiagnosticsCollector
from relspec.evidence import initial_evidence_from_views
from relspec.graph_inference import build_task_graph_from_views
from relspec.inferred_deps import infer_deps_from_view_nodes
from relspec.rustworkx_graph import task_graph_signature, task_graph_snapshot
from relspec.rustworkx_schedule import schedule_tasks, task_schedule_metadata
from relspec.view_defs import RELATION_OUTPUT_NAME

if TYPE_CHECKING:
    from datafusion import SessionContext
    from hamilton.io.materialization import MaterializerFactory

    from datafusion_engine.view_graph_registry import ViewNode
    from relspec.schedule_events import TaskScheduleMetadata
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


def _build_dependency_map(
    config: Mapping[str, JsonValue],
) -> tuple[
    Mapping[str, tuple[str, ...]],
    tuple[ViewNode, ...],
    Mapping[str, str],
    str,
    Mapping[str, TaskScheduleMetadata],
]:
    ctx, nodes_with_ast = _view_graph_context(config)
    dependency_map, plan_fingerprints = _dependency_payloads(nodes_with_ast)
    signature, schedule_metadata = _task_graph_metadata(nodes_with_ast, ctx=ctx)
    return dependency_map, nodes_with_ast, plan_fingerprints, signature, schedule_metadata


def _view_graph_context(
    config: Mapping[str, JsonValue],
) -> tuple[ExecutionContext, tuple[ViewNode, ...]]:
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
    session = profile.session_context()
    snapshot = ensure_view_graph(
        session,
        runtime_profile=profile,
        include_registry_views=True,
    )
    validate_edge_kind_requirements(_relation_output_schema(session))
    nodes = view_graph_nodes(session, snapshot=snapshot)
    nodes_with_ast = tuple(node for node in nodes if node.sqlglot_ast is not None)
    return ctx, nodes_with_ast


def _dependency_payloads(
    nodes: Sequence[ViewNode],
) -> tuple[dict[str, tuple[str, ...]], dict[str, str]]:
    outputs = {node.name for node in nodes}
    dependency_map: dict[str, tuple[str, ...]] = {}
    plan_fingerprints: dict[str, str] = {}
    inferred = infer_deps_from_view_nodes(nodes)
    for dep in inferred:
        inputs = tuple(sorted(name for name in dep.inputs if name in outputs))
        dependency_map[dep.output] = inputs
        plan_fingerprints[dep.task_name] = dep.plan_fingerprint
    return dependency_map, plan_fingerprints


def _task_graph_metadata(
    nodes: Sequence[ViewNode],
    *,
    ctx: ExecutionContext | None = None,
) -> tuple[str, Mapping[str, TaskScheduleMetadata]]:
    inferred = infer_deps_from_view_nodes(nodes)
    task_signatures = {dep.task_name: dep.plan_fingerprint for dep in inferred}
    graph = build_task_graph_from_views(nodes)
    snapshot = task_graph_snapshot(
        graph,
        label="hamilton_pipeline",
        task_signatures=task_signatures,
    )
    signature = task_graph_signature(snapshot)
    session = None
    if ctx is not None and ctx.runtime.datafusion is not None:
        session = ctx.runtime.datafusion.session_context()
    evidence = initial_evidence_from_views(nodes, ctx=session)
    schedule = schedule_tasks(graph, evidence=evidence, allow_partial=True)
    schedule_metadata = task_schedule_metadata(schedule)
    return signature, schedule_metadata


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
) -> driver.Builder:
    if not bool(config.get("enable_dynamic_execution", False)):
        return builder
    max_tasks_value = config.get("max_tasks")
    max_tasks = 4
    if isinstance(max_tasks_value, int) and not isinstance(max_tasks_value, bool):
        max_tasks = max_tasks_value
    return (
        builder.enable_dynamic_execution(allow_experimental_mode=True)
        .with_local_executor(executors.SynchronousLocalTaskExecutor())
        .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=max_tasks))
        .with_grouping_strategy(grouping.GroupNodesByLevel())
    )


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


def _apply_cache(
    builder: driver.Builder,
    *,
    config: Mapping[str, JsonValue],
) -> driver.Builder:
    cache_path = config.get("cache_path")
    if not isinstance(cache_path, str) or not cache_path:
        return builder
    cache_opt_in = bool(config.get("cache_opt_in", True))
    if cache_opt_in:
        # cache only nodes annotated for caching
        return builder.with_cache(
            path=str(cache_path), default_behavior="disable", log_to_file=True
        )
    # cache everything (aggressive)
    return builder.with_cache(path=str(cache_path), log_to_file=True)


def _apply_adapters(
    builder: driver.Builder,
    *,
    config: Mapping[str, JsonValue],
    diagnostics: DiagnosticsCollector,
) -> driver.Builder:
    tracker = _maybe_build_tracker_adapter(config)
    if tracker is not None:
        builder = builder.with_adapters(tracker)
    if bool(config.get("enable_hamilton_type_checker", True)):
        builder = builder.with_adapters(FunctionInputOutputTypeChecker())
    if bool(config.get("enable_hamilton_node_diagnostics", True)):
        builder = builder.with_adapters(DiagnosticsNodeHook(diagnostics))
    return builder


def build_driver(
    *,
    config: Mapping[str, JsonValue],
    modules: Sequence[ModuleType] | None = None,
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
    (
        dependency_map,
        view_nodes,
        plan_fingerprints,
        graph_signature,
        schedule_metadata,
    ) = _build_dependency_map(config)
    enable_dynamic_execution = bool(config.get("enable_dynamic_execution", False))
    modules.append(
        build_task_execution_module(
            dependency_map=dependency_map,
            view_nodes=view_nodes,
            options=TaskExecutionModuleOptions(
                plan_fingerprints=plan_fingerprints,
                schedule_metadata=schedule_metadata,
                use_generation_gate=enable_dynamic_execution,
            ),
        )
    )

    config_payload = _with_graph_tags(config, graph_signature=graph_signature)

    diagnostics = DiagnosticsCollector()
    set_hamilton_diagnostics_collector(diagnostics)

    builder = driver.Builder().with_modules(*modules).with_config(config_payload)
    builder = _apply_dynamic_execution(builder, config=config_payload)
    builder = _apply_cache(builder, config=config_payload)
    builder = _apply_materializers(builder, config=config_payload)
    builder = _apply_adapters(builder, config=config_payload, diagnostics=diagnostics)
    return builder.build()


def _relation_output_schema(session: SessionContext) -> SchemaLike:
    if not session.table_exist(RELATION_OUTPUT_NAME):
        msg = f"Relation output view {RELATION_OUTPUT_NAME!r} is not registered."
        raise ValueError(msg)
    return cast("SchemaLike", session.table(RELATION_OUTPUT_NAME).schema())


@dataclass
class DriverFactory:
    """
    Caches built Hamilton Drivers by config fingerprint.

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
        fp = config_fingerprint(config)
        if fp in self._cache:
            return self._cache[fp]
        dr = build_driver(config=config, modules=self.modules)
        self._cache[fp] = dr
        return dr
