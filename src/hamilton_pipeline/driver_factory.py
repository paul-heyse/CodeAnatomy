"""Hamilton driver construction helpers for the pipeline."""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast

import msgspec
from hamilton import driver
from hamilton.execution import executors
from hamilton.lifecycle import base as lifecycle_base
from opentelemetry import trace as otel_trace

from cache.diskcache_factory import (
    DiskCacheKind,
    DiskCacheProfile,
    DiskCacheSettings,
    default_diskcache_profile,
)
from cli.config_models import (
    DataFusionCacheConfigSpec,
    DataFusionCachePolicySpec,
    DiskCacheProfileSpec,
    DiskCacheSettingsSpec,
)
from core.config_base import FingerprintableConfig
from core.config_base import config_fingerprint as hash_config_fingerprint
from core_types import DeterminismTier, JsonValue, parse_determinism_tier
from datafusion_engine.arrow.interop import SchemaLike, TableLike
from datafusion_engine.session.cache_policy import DEFAULT_CACHE_POLICY, CachePolicyConfig
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
from hamilton_pipeline.modules.execution_plan import (
    PlanModuleOptions,
    build_execution_plan_module,
)
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
from relspec.errors import RelspecValidationError
from relspec.execution_authority import ExecutionAuthorityContext
from relspec.view_defs import RELATION_OUTPUT_NAME
from serde_artifact_specs import (
    COMPILED_EXECUTION_POLICY_SPEC,
    DECISION_PROVENANCE_GRAPH_SPEC,
    EXECUTION_AUTHORITY_VALIDATION_SPEC,
    FALLBACK_QUARANTINE_SPEC,
    POLICY_COUNTERFACTUAL_REPLAY_SPEC,
    POLICY_VALIDATION_SPEC,
)
from utils.env_utils import env_bool, env_value
from utils.hashing import CacheKeyBuilder

if TYPE_CHECKING:
    from hamilton.base import HamiltonGraphAdapter
    from hamilton.io.materialization import MaterializerFactory

    from datafusion_engine.delta.scan_policy_inference import ScanPolicyOverride
    from datafusion_engine.extensions.runtime_capabilities import RuntimeCapabilitiesSnapshot
    from datafusion_engine.plan.signals import PlanSignals
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
    from datafusion_engine.views.graph import ViewNode
    from extract.coordination.evidence_plan import EvidencePlan
    from hamilton_pipeline.cache_lineage import CacheLineageHook
    from hamilton_pipeline.graph_snapshot import GraphSnapshotHook
    from relspec.compiled_policy import CompiledExecutionPolicy
    from relspec.counterfactual_replay import CounterfactualScenario
    from relspec.decision_provenance import DecisionProvenanceGraph
    from relspec.execution_plan import ExecutionPlan
    from relspec.inference_confidence import InferenceConfidence
    from relspec.pipeline_policy import DiagnosticsPolicy
    from relspec.policy_validation import PolicyValidationResult
    from semantics.compile_context import SemanticExecutionContext
    from semantics.ir import SemanticIR
    from semantics.program_manifest import ManifestDatasetResolver

try:
    from hamilton_sdk import adapters as hamilton_adapters
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    hamilton_adapters = None

_DEFAULT_DAG_NAME = "codeintel::semantic_v1"
_HAMILTON_FILE_META_PATCH_STATE: dict[str, bool] = {"patched": False}
_SEMANTIC_VERSION = "v1"
_PACKED_REF_FIELDS = 2


def _ensure_hamilton_dataframe_types() -> None:
    from hamilton import htypes
    from hamilton import registry as hamilton_registry

    registered = hamilton_registry.get_registered_dataframe_types()
    if any(htypes.custom_subclass_check(TableLike, df_type) for df_type in registered.values()):
        return
    hamilton_registry.register_types("datafusion", TableLike, None)


def _patch_hamilton_file_metadata() -> None:
    try:
        from hamilton.io import default_data_loaders as hamilton_loaders
        from hamilton.io import utils as hamilton_utils
    except ModuleNotFoundError:
        return

    if _HAMILTON_FILE_META_PATCH_STATE["patched"]:
        return

    import time
    from datetime import UTC, datetime
    from pathlib import Path
    from urllib import parse

    def _safe_get_file_metadata(path: object) -> dict[str, object]:
        path_str = str(path)
        parsed = parse.urlparse(path_str)
        size = None
        scheme = parsed.scheme
        last_modified = time.time()
        timestamp = datetime.now(UTC).timestamp()
        notes = (
            f"File metadata is unsupported for scheme: {scheme} or path: {path_str} does not exist."
        )

        is_win_path = parsed.scheme and len(parsed.scheme) == 1 and parsed.scheme.isalpha()
        if (not parsed.scheme or is_win_path) and Path(path_str).exists():
            size = Path(path_str).stat().st_size
            last_modified = Path(path_str).stat().st_mtime
            notes = ""

        return {
            "file_metadata": {
                "size": size,
                "path": path_str,
                "last_modified": last_modified,
                "timestamp": timestamp,
                "scheme": scheme,
                "notes": notes,
                "__version__": "1.0.0",
            }
        }

    hamilton_utils.get_file_metadata = _safe_get_file_metadata
    hamilton_loaders.get_file_metadata = _safe_get_file_metadata
    _HAMILTON_FILE_META_PATCH_STATE["patched"] = True


def default_modules() -> list[ModuleType]:
    """Return the default Hamilton module set for the pipeline.

    Returns:
    -------
    list[ModuleType]
        Default module list for the pipeline.
    """
    return hamilton_modules.load_all_modules()


def _config_section(
    config: Mapping[str, JsonValue],
    key: str,
) -> Mapping[str, JsonValue]:
    value = config.get(key)
    if isinstance(value, Mapping):
        return cast("Mapping[str, JsonValue]", value)
    return {}


def _mutable_config_section(
    config: dict[str, JsonValue],
    key: str,
) -> dict[str, JsonValue]:
    value = config.get(key)
    section = dict(value) if isinstance(value, Mapping) else {}
    config[key] = section
    return section


_DISKCACHE_KINDS: set[str] = {
    "plan",
    "extract",
    "schema",
    "repo_scan",
    "runtime",
    "queue",
    "index",
    "coordination",
}


def _datafusion_cache_config(
    config: Mapping[str, JsonValue],
) -> DataFusionCacheConfigSpec | None:
    section = _config_section(config, "datafusion_cache")
    if not section:
        return None
    return msgspec.convert(section, type=DataFusionCacheConfigSpec, strict=True)


def _coerce_diskcache_kind(value: str) -> DiskCacheKind:
    normalized = value.strip()
    if normalized in _DISKCACHE_KINDS:
        return cast("DiskCacheKind", normalized)
    msg = f"Unsupported diskcache kind: {value!r}."
    raise ValueError(msg)


def _merge_diskcache_settings(
    base: DiskCacheSettings,
    spec: DiskCacheSettingsSpec | None,
) -> DiskCacheSettings:
    if spec is None:
        return base
    return DiskCacheSettings(
        size_limit_bytes=spec.size_limit_bytes
        if spec.size_limit_bytes is not None
        else base.size_limit_bytes,
        cull_limit=spec.cull_limit if spec.cull_limit is not None else base.cull_limit,
        eviction_policy=spec.eviction_policy
        if spec.eviction_policy is not None
        else base.eviction_policy,
        statistics=spec.statistics if spec.statistics is not None else base.statistics,
        tag_index=spec.tag_index if spec.tag_index is not None else base.tag_index,
        shards=spec.shards if spec.shards is not None else base.shards,
        timeout_seconds=spec.timeout_seconds
        if spec.timeout_seconds is not None
        else base.timeout_seconds,
        disk_min_file_size=spec.disk_min_file_size
        if spec.disk_min_file_size is not None
        else base.disk_min_file_size,
        sqlite_journal_mode=spec.sqlite_journal_mode
        if spec.sqlite_journal_mode is not None
        else base.sqlite_journal_mode,
        sqlite_mmap_size=spec.sqlite_mmap_size
        if spec.sqlite_mmap_size is not None
        else base.sqlite_mmap_size,
        sqlite_synchronous=spec.sqlite_synchronous
        if spec.sqlite_synchronous is not None
        else base.sqlite_synchronous,
    )


def _diskcache_profile_from_spec(
    spec: DiskCacheProfileSpec | None,
    *,
    existing: DiskCacheProfile | None,
) -> DiskCacheProfile | None:
    if spec is None:
        return existing
    base_profile = existing or default_diskcache_profile()
    base_settings = _merge_diskcache_settings(base_profile.base_settings, spec.base_settings)
    overrides = dict(base_profile.overrides)
    if spec.overrides:
        for kind, override_spec in spec.overrides.items():
            resolved_kind = _coerce_diskcache_kind(str(kind))
            base_override = overrides.get(resolved_kind, base_settings)
            overrides[resolved_kind] = _merge_diskcache_settings(base_override, override_spec)
    ttl_seconds = dict(base_profile.ttl_seconds)
    if spec.ttl_seconds:
        for kind, ttl in spec.ttl_seconds.items():
            resolved_kind = _coerce_diskcache_kind(str(kind))
            ttl_seconds[resolved_kind] = ttl
    root = base_profile.root
    if spec.root is not None and str(spec.root).strip():
        root = Path(spec.root).expanduser()
    return DiskCacheProfile(
        root=root,
        base_settings=base_settings,
        overrides=overrides,
        ttl_seconds=ttl_seconds,
    )


def _cache_policy_from_spec(
    spec: DataFusionCachePolicySpec | None,
    *,
    existing: CachePolicyConfig | None,
) -> CachePolicyConfig | None:
    if spec is None:
        return existing
    base = existing or DEFAULT_CACHE_POLICY
    return CachePolicyConfig(
        listing_cache_size=spec.listing_cache_size
        if spec.listing_cache_size is not None
        else base.listing_cache_size,
        metadata_cache_size=spec.metadata_cache_size
        if spec.metadata_cache_size is not None
        else base.metadata_cache_size,
        stats_cache_size=spec.stats_cache_size
        if spec.stats_cache_size is not None
        else base.stats_cache_size,
    )


def _apply_datafusion_cache_config(
    profile: DataFusionRuntimeProfile,
    *,
    config: Mapping[str, JsonValue],
) -> DataFusionRuntimeProfile:
    resolved = _datafusion_cache_config(config)
    if resolved is None:
        return profile
    cache_policy = _cache_policy_from_spec(
        resolved.cache_policy,
        existing=profile.policies.cache_policy,
    )
    diskcache_profile = _diskcache_profile_from_spec(
        resolved.diskcache_profile,
        existing=profile.policies.diskcache_profile,
    )
    updated_policies = msgspec.structs.replace(
        profile.policies,
        cache_policy=cache_policy,
        diskcache_profile=diskcache_profile,
        snapshot_pinned_mode=resolved.snapshot_pinned_mode,
        cache_profile_name=resolved.cache_profile_name,
    )
    return msgspec.structs.replace(profile, policies=updated_policies)


def driver_config_fingerprint(config: Mapping[str, JsonValue]) -> str:
    """Compute a stable driver config fingerprint for caching.

    Hamilton build-time config is immutable after build; if config changes,
    rebuild a new driver. The Hamilton docs recommend a small driver-factory
    that caches by config fingerprint.
    :contentReference[oaicite:2]{index=2}

    Returns:
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

    Returns:
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
    semantic_context: SemanticExecutionContext


def build_view_graph_context(
    config: Mapping[str, JsonValue],
    *,
    execution_context: SemanticExecutionContext | None = None,
) -> ViewGraphContext:
    """Build a view graph context from runtime configuration.

    **EXECUTION AUTHORITY: VIEW GRAPH REGISTRATION**

    This function ensures all semantic views are registered via the view graph
    infrastructure BEFORE Hamilton execution begins. Hamilton nodes consume
    these pre-registered views via ``source()`` inputs; they do NOT re-register.

    The registration happens in ``ensure_view_graph()`` which delegates to
    ``registry_specs.view_graph_nodes()`` - the single source of truth for
    all view definitions including semantic views.

    Parameters
    ----------
    config
        Runtime configuration mapping.
    execution_context
        Pre-compiled semantic execution context. When provided, reuse
        its manifest and dataset resolver instead of compiling from scratch.

    Returns:
    -------
    ViewGraphContext
        Resolved view graph context with runtime metadata.

    See Also:
    --------
    datafusion_engine.views.registry_specs._semantics_view_nodes : Semantic view registration.
    hamilton_pipeline.task_module_builder.build_task_execution_module : Dynamic task module.
    """
    from datafusion_engine.session.runtime import (
        compile_resolver_invariants_strict_mode,
        datasource_config_from_manifest,
        datasource_config_from_profile,
        record_compile_resolver_invariants,
    )
    from semantics.compile_invariants import compile_tracking
    from semantics.resolver_identity import resolver_identity_tracking

    runtime_profile_spec = resolve_runtime_profile(
        _runtime_profile_name(config),
        determinism=_determinism_override(config),
    )
    profile = _apply_datafusion_cache_config(runtime_profile_spec.datafusion, config=config)
    if profile is not runtime_profile_spec.datafusion:
        runtime_profile_spec = msgspec.structs.replace(
            runtime_profile_spec,
            datafusion=profile,
        )
    from cpg.kind_catalog import validate_edge_kind_requirements
    from datafusion_engine.session.facade import DataFusionExecutionFacade
    from datafusion_engine.session.runtime import refresh_session_runtime
    from datafusion_engine.views.registry_specs import view_graph_nodes

    strict_invariants = compile_resolver_invariants_strict_mode()
    expected_compiles = 0 if execution_context is not None else 1
    with (
        compile_tracking(
            max_compiles=expected_compiles,
            label="build_view_graph_context",
            strict=False,
        ) as compile_tracker,
        resolver_identity_tracking(
            label="build_view_graph_context",
            strict=False,
        ) as resolver_tracker,
    ):
        session_runtime = profile.session_runtime()
        if execution_context is not None:
            semantic_context = execution_context
        else:
            from semantics.compile_context import build_semantic_execution_context

            semantic_context = build_semantic_execution_context(
                runtime_profile=profile,
                ctx=session_runtime.ctx,
            )
        semantic_manifest = semantic_context.manifest
        if semantic_manifest.dataset_bindings.locations:
            resolved_data_sources = datasource_config_from_manifest(
                semantic_manifest,
                runtime_profile=profile,
            )
        else:
            resolved_data_sources = datasource_config_from_profile(profile)
        replaced_profile = False
        try:
            profile = msgspec.structs.replace(profile, data_sources=resolved_data_sources)
            replaced_profile = True
        except (TypeError, ValueError, AttributeError):
            with contextlib.suppress(AttributeError, TypeError):
                cast("Any", profile).data_sources = resolved_data_sources
        if replaced_profile:
            runtime_profile_spec = msgspec.structs.replace(
                runtime_profile_spec,
                datafusion=profile,
            )
            semantic_context = replace(semantic_context, runtime_profile=profile)
        semantic_ir = semantic_manifest.semantic_ir
        facade = DataFusionExecutionFacade(ctx=session_runtime.ctx, runtime_profile=profile)
        # Single registration point: ensure_view_graph registers ALL views including
        # semantic views via registry_specs.view_graph_nodes(). Hamilton consumes only.
        snapshot = facade.ensure_view_graph(
            semantic_manifest=semantic_manifest,
            dataset_resolver=semantic_context.dataset_resolver,
        )
        session_runtime = refresh_session_runtime(profile, ctx=session_runtime.ctx)
        validate_edge_kind_requirements(_relation_output_schema(session_runtime))
        nodes = view_graph_nodes(
            session_runtime.ctx,
            snapshot=snapshot,
            runtime_profile=profile,
            semantic_ir=semantic_ir,
            manifest=semantic_manifest,
        )
    violations: list[str] = []
    try:
        compile_tracker.assert_compile_count()
    except RuntimeError as exc:
        violations.append(str(exc))
    violations.extend(resolver_tracker.verify_identity())
    record_compile_resolver_invariants(
        profile,
        label="build_view_graph_context",
        compile_count=compile_tracker.compile_count,
        max_compiles=compile_tracker.max_compiles,
        distinct_resolver_count=resolver_tracker.distinct_resolvers(),
        strict=strict_invariants,
        violations=violations,
    )
    return ViewGraphContext(
        profile=profile,
        session_runtime=session_runtime,
        determinism_tier=runtime_profile_spec.determinism_tier,
        snapshot=snapshot,
        view_nodes=tuple(nodes),
        runtime_profile_spec=runtime_profile_spec,
        semantic_context=semantic_context,
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

    plan_config = _config_section(config, "plan")
    requested = _task_name_list_from_config(plan_config, key="requested_tasks")
    impacted = _task_name_list_from_config(plan_config, key="impacted_tasks")
    allow_partial = bool(plan_config.get("allow_partial", False))
    enable_metric_scheduling = True
    metric_flag = plan_config.get("enable_metric_scheduling")
    if isinstance(metric_flag, bool):
        enable_metric_scheduling = metric_flag
    request = ExecutionPlanRequest(
        view_nodes=view_ctx.view_nodes,
        snapshot=view_ctx.snapshot,
        runtime_profile=view_ctx.profile,
        semantic_context=view_ctx.semantic_context,
        requested_task_names=requested,
        impacted_task_names=impacted,
        allow_partial=allow_partial,
        enable_metric_scheduling=enable_metric_scheduling,
    )
    return compile_execution_plan(session_runtime=view_ctx.session_runtime, request=request)


def _execution_authority_enforcement(
    config: Mapping[str, JsonValue],
) -> Literal["warn", "error"]:
    plan_config = _config_section(config, "plan")
    value = plan_config.get("execution_authority_enforcement")
    if value == "error":
        return "error"
    return "warn"


def _authority_evidence_plan(
    plan: ExecutionPlan,
) -> EvidencePlan | None:
    from extract.coordination.evidence_plan import compile_evidence_plan
    from relspec.extract_plan import extract_output_task_map

    task_map = extract_output_task_map()
    outputs = [task_map[name].output for name in sorted(plan.active_tasks) if name in task_map]
    if not outputs:
        return None
    return compile_evidence_plan(rules=outputs)


def _compile_authority_policy(
    *,
    plan: ExecutionPlan,
    profile: DataFusionRuntimeProfile,
    semantic_ir: SemanticIR | None = None,
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None = None,
) -> CompiledExecutionPolicy | None:
    from relspec.policy_compiler import compile_execution_policy

    try:
        output_locations = profile.data_sources.semantic_output.locations
        scan_overrides = _scan_overrides_from_plan(
            plan=plan,
            profile=profile,
            capability_snapshot=capability_snapshot,
        )
        diagnostics_policy = _diagnostics_policy_from_profile(profile)
        workload_class = _workload_class_from_plan(plan)
        return compile_execution_policy(
            task_graph=plan.task_graph,
            output_locations=output_locations,
            runtime_profile=profile,
            view_nodes=plan.view_nodes,
            semantic_ir=semantic_ir,
            scan_overrides=scan_overrides,
            diagnostics_policy=diagnostics_policy,
            workload_class=workload_class,
        )
    except Exception:  # noqa: BLE001
        return None


def _workload_class_from_plan(plan: ExecutionPlan) -> str | None:
    representative = _representative_plan_signals(plan)
    if representative is None:
        return None
    _task_name, signals = representative
    from datafusion_engine.workload.classifier import classify_workload

    return classify_workload(signals).value


def _scan_overrides_from_plan(
    *,
    plan: ExecutionPlan,
    profile: DataFusionRuntimeProfile,
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None,
) -> tuple[ScanPolicyOverride, ...]:
    from datafusion_engine.delta.scan_policy_inference import derive_scan_policy_overrides
    from datafusion_engine.plan.pipeline import _merge_scan_policy_overrides
    from datafusion_engine.plan.signals import extract_plan_signals

    overrides: list[ScanPolicyOverride] = []
    base_policy = profile.policies.scan_policy
    for node in sorted(plan.view_nodes, key=lambda item: item.name):
        bundle = node.plan_bundle
        if bundle is None:
            continue
        signals = extract_plan_signals(bundle)
        overrides.extend(
            derive_scan_policy_overrides(
                signals,
                base_policy=base_policy,
                capability_snapshot=capability_snapshot,
            )
        )
    return _merge_scan_policy_overrides(overrides)


def _diagnostics_policy_from_profile(
    profile: DataFusionRuntimeProfile,
) -> DiagnosticsPolicy:
    from relspec.pipeline_policy import DiagnosticsPolicy

    return DiagnosticsPolicy(
        capture_datafusion_metrics=bool(profile.features.enable_metrics),
        capture_datafusion_traces=bool(profile.features.enable_tracing),
        capture_datafusion_explains=bool(profile.diagnostics.capture_explain),
        explain_analyze=bool(profile.diagnostics.explain_analyze),
        explain_analyze_level=profile.diagnostics.explain_analyze_level,
        emit_kernel_lane_diagnostics=bool(profile.diagnostics.capture_plan_artifacts),
        emit_semantic_quality_diagnostics=bool(
            profile.diagnostics.emit_semantic_quality_diagnostics
        ),
    )


def _record_compiled_policy_artifact(
    profile: DataFusionRuntimeProfile,
    compiled_policy: CompiledExecutionPolicy | None,
) -> None:

    if compiled_policy is None:
        return
    cache_dist: dict[str, int] = {}
    for value in compiled_policy.cache_policy_by_view.values():
        cache_dist[value] = cache_dist.get(value, 0) + 1
    profile.record_artifact(
        COMPILED_EXECUTION_POLICY_SPEC,
        {
            "cache_policy_count": len(compiled_policy.cache_policy_by_view),
            "scan_override_count": len(compiled_policy.scan_policy_overrides),
            "udf_requirement_count": len(compiled_policy.udf_requirements_by_view),
            "join_strategy_count": len(compiled_policy.join_strategy_by_view),
            "inference_confidence_count": len(compiled_policy.inference_confidence_by_view),
            "validation_mode": compiled_policy.validation_mode,
            "workload_class": compiled_policy.workload_class,
            "policy_fingerprint": compiled_policy.policy_fingerprint,
            "cache_policy_distribution": cache_dist or None,
        },
    )


def _record_execution_package(
    *,
    profile: DataFusionRuntimeProfile,
    manifest: object | None,
    compiled_policy: object | None,
    capability_snapshot: object | None,
    plan_fingerprints: Mapping[str, str],
) -> None:
    from relspec.execution_package import build_execution_package
    from serde_artifact_specs import EXECUTION_PACKAGE_SPEC
    from serde_msgspec import to_builtins_mapping

    package = build_execution_package(
        manifest=manifest,
        compiled_policy=compiled_policy,
        capability_snapshot=capability_snapshot,
        plan_bundle_fingerprints=plan_fingerprints,
        session_config=profile,
    )
    profile.record_artifact(
        EXECUTION_PACKAGE_SPEC,
        to_builtins_mapping(package, str_keys=True),
    )


def _representative_plan_signals(
    plan: ExecutionPlan,
) -> tuple[str, PlanSignals] | None:
    if not plan.plan_signals_by_task:
        return None

    def _signal_sort_key(item: tuple[str, PlanSignals]) -> tuple[int, int, int, int, int]:
        _task_name, signals = item
        stats = signals.stats
        lineage = signals.lineage
        num_rows = int(stats.num_rows) if stats is not None and stats.num_rows is not None else 0
        total_bytes = (
            int(stats.total_bytes) if stats is not None and stats.total_bytes is not None else 0
        )
        scan_count = len(lineage.scans) if lineage is not None else 0
        return (
            1 if stats is not None else 0,
            1 if lineage is not None else 0,
            num_rows,
            total_bytes,
            scan_count,
        )

    ranked = sorted(
        plan.plan_signals_by_task.items(),
        key=_signal_sort_key,
        reverse=True,
    )
    return ranked[0] if ranked else None


def _apply_workload_session_profile(
    *,
    view_ctx: ViewGraphContext,
    plan: ExecutionPlan,
) -> ViewGraphContext:
    from datafusion_engine.session.runtime import refresh_session_runtime
    from datafusion_engine.workload.classifier import classify_workload, session_config_for_workload
    from serde_artifact_specs import WORKLOAD_CLASSIFICATION_SPEC
    from serde_artifacts import WorkloadClassificationArtifact
    from serde_msgspec import to_builtins_mapping

    representative = _representative_plan_signals(plan)
    if representative is None:
        return view_ctx
    task_name, signals = representative
    workload_class = classify_workload(signals)
    overrides = session_config_for_workload(workload_class)

    updated_ctx = view_ctx
    if overrides:
        normalized_overrides = {str(key): str(value) for key, value in overrides.items()}
        merged_overrides = {
            str(key): str(value)
            for key, value in view_ctx.profile.policies.settings_overrides.items()
        }
        merged_overrides.update(normalized_overrides)
        updated_profile = msgspec.structs.replace(
            view_ctx.profile,
            policies=msgspec.structs.replace(
                view_ctx.profile.policies,
                settings_overrides=merged_overrides,
            ),
        )
        updated_runtime_profile_spec = msgspec.structs.replace(
            view_ctx.runtime_profile_spec,
            datafusion=updated_profile,
        )
        updated_semantic_context = replace(
            view_ctx.semantic_context,
            runtime_profile=updated_profile,
        )
        updated_ctx = replace(
            view_ctx,
            profile=updated_profile,
            session_runtime=refresh_session_runtime(
                updated_profile,
                ctx=view_ctx.session_runtime.ctx,
            ),
            runtime_profile_spec=updated_runtime_profile_spec,
            semantic_context=updated_semantic_context,
        )

    stats = signals.stats
    lineage = signals.lineage
    artifact = WorkloadClassificationArtifact(
        workload_class=workload_class.value,
        plan_fingerprint=signals.plan_fingerprint,
        num_rows_signal=stats.num_rows if stats is not None else None,
        total_bytes_signal=stats.total_bytes if stats is not None else None,
        scan_count=len(lineage.scans) if lineage is not None else 0,
        classification_reason=f"representative_task={task_name}",
    )
    updated_ctx.profile.record_artifact(
        WORKLOAD_CLASSIFICATION_SPEC,
        to_builtins_mapping(artifact, str_keys=True),
    )
    return updated_ctx


def _inference_confidence_records_for_policy(
    compiled_policy: CompiledExecutionPolicy,
) -> dict[str, InferenceConfidence]:
    from relspec.inference_confidence import InferenceConfidence

    confidence_records: dict[str, InferenceConfidence] = {}
    for dataset_name, override_value in compiled_policy.scan_policy_overrides.items():
        if not isinstance(override_value, Mapping):
            continue
        raw_confidence = override_value.get("inference_confidence")
        if not isinstance(raw_confidence, Mapping):
            continue
        try:
            confidence_records[dataset_name] = msgspec.convert(
                raw_confidence,
                type=InferenceConfidence,
                strict=False,
            )
        except (msgspec.DecodeError, msgspec.ValidationError, TypeError, ValueError):
            continue
    for view_name, raw_confidence in compiled_policy.inference_confidence_by_view.items():
        if not isinstance(raw_confidence, Mapping):
            continue
        try:
            confidence_records[view_name] = msgspec.convert(
                raw_confidence,
                type=InferenceConfidence,
                strict=False,
            )
        except (msgspec.DecodeError, msgspec.ValidationError, TypeError, ValueError):
            continue
    return confidence_records


def _record_decision_provenance_artifact(
    *,
    profile: DataFusionRuntimeProfile,
    compiled_policy: CompiledExecutionPolicy | None,
    run_id: str,
) -> DecisionProvenanceGraph | None:
    if compiled_policy is None:
        return None
    from relspec.decision_provenance import build_provenance_graph
    from serde_artifacts import DecisionProvenanceGraphArtifact
    from serde_msgspec import to_builtins_mapping

    confidence_records = _inference_confidence_records_for_policy(compiled_policy)
    graph = build_provenance_graph(
        compiled_policy,
        confidence_records,
        run_id=run_id,
    )
    if not graph.decisions:
        return None

    domain_counts: dict[str, int] = {}
    fallback_count = 0
    confidence_sum = 0.0
    for decision in graph.decisions:
        domain_counts[decision.domain] = domain_counts.get(decision.domain, 0) + 1
        if decision.fallback_reason is not None:
            fallback_count += 1
        confidence_sum += float(decision.confidence_score)
    mean_confidence = confidence_sum / float(len(graph.decisions))

    payload = DecisionProvenanceGraphArtifact(
        run_id=run_id,
        decision_count=len(graph.decisions),
        root_count=len(graph.root_ids),
        domain_counts=domain_counts or None,
        fallback_count=fallback_count,
        mean_confidence=mean_confidence,
    )
    profile.record_artifact(
        DECISION_PROVENANCE_GRAPH_SPEC,
        to_builtins_mapping(payload, str_keys=True),
    )
    return graph


def _record_policy_counterfactual_artifact(
    *,
    profile: DataFusionRuntimeProfile,
    compiled_policy: CompiledExecutionPolicy | None,
    plan: ExecutionPlan,
) -> None:
    if compiled_policy is None:
        return
    from relspec.counterfactual_replay import replay_compiled_policy_counterfactuals
    from serde_artifacts import (
        CounterfactualScenarioOutcome,
        PolicyCounterfactualReplayArtifact,
    )
    from serde_msgspec import to_builtins_mapping

    scenarios = _counterfactual_scenarios_for_policy(compiled_policy)
    results = replay_compiled_policy_counterfactuals(
        compiled_policy,
        scenarios=scenarios,
        task_costs=plan.task_costs,
    )
    best = min(
        (result for result in results if result.estimated_cost_delta is not None),
        key=lambda result: float(result.estimated_cost_delta or 0.0),
        default=None,
    )
    payload = PolicyCounterfactualReplayArtifact(
        baseline_policy_fingerprint=compiled_policy.policy_fingerprint,
        baseline_workload_class=compiled_policy.workload_class,
        scenario_count=len(results),
        best_scenario=best.scenario_name if best is not None else None,
        scenarios=tuple(
            CounterfactualScenarioOutcome(
                scenario_name=result.scenario_name,
                policy_fingerprint=result.policy_fingerprint,
                changed_cache_views=result.changed_cache_views,
                changed_scan_overrides=result.changed_scan_overrides,
                estimated_cost_delta=result.estimated_cost_delta,
                notes=result.notes,
            )
            for result in results
        ),
    )
    profile.record_artifact(
        POLICY_COUNTERFACTUAL_REPLAY_SPEC,
        to_builtins_mapping(payload, str_keys=True),
    )


def _counterfactual_scenarios_for_policy(
    compiled_policy: CompiledExecutionPolicy,
) -> tuple[CounterfactualScenario, ...]:
    from relspec.counterfactual_replay import CounterfactualScenario

    scenarios: list[CounterfactualScenario] = []
    for workload in (
        "batch_ingest",
        "interactive_query",
        "compile_replay",
        "incremental_update",
    ):
        if workload == compiled_policy.workload_class:
            continue
        scenarios.append(
            CounterfactualScenario(
                name=f"workload::{workload}",
                workload_class=workload,
            )
        )
    no_staging_overrides = {
        view_name: "none"
        for view_name, cache_policy in compiled_policy.cache_policy_by_view.items()
        if cache_policy == "delta_staging"
    }
    if no_staging_overrides:
        scenarios.append(
            CounterfactualScenario(
                name="cache::no_staging",
                workload_class=compiled_policy.workload_class,
                cache_policy_overrides=no_staging_overrides,
            )
        )
    return tuple(scenarios)


def _record_fallback_quarantine_artifact(
    *,
    profile: DataFusionRuntimeProfile,
    run_id: str,
    graph: DecisionProvenanceGraph | None,
) -> None:
    if graph is None:
        return
    from relspec.fallback_quarantine import evaluate_fallback_quarantine
    from serde_artifacts import FallbackQuarantineArtifact
    from serde_msgspec import to_builtins_mapping

    report = evaluate_fallback_quarantine(graph)
    payload = FallbackQuarantineArtifact(
        run_id=run_id,
        decision_count=report.decision_count,
        fallback_count=report.fallback_count,
        quarantined_count=len(report.quarantined_contexts),
        quarantined_contexts=report.quarantined_contexts,
        reason_counts=dict(report.reason_counts) or None,
        threshold_confidence=report.thresholds.min_confidence,
        max_fallback_ratio=report.thresholds.max_fallback_ratio,
    )
    profile.record_artifact(
        FALLBACK_QUARANTINE_SPEC,
        to_builtins_mapping(payload, str_keys=True),
    )


def _build_execution_authority(
    *,
    view_ctx: ViewGraphContext,
    plan: ExecutionPlan,
    config: Mapping[str, JsonValue],
) -> ExecutionAuthorityContext:
    from datafusion_engine.extensions.runtime_capabilities import (
        build_runtime_capabilities_snapshot,
    )
    from datafusion_engine.session.runtime import session_runtime_hash
    from hamilton_pipeline.modules.task_execution import build_extract_executor_map

    evidence_plan = _authority_evidence_plan(plan)
    capability_snapshot = build_runtime_capabilities_snapshot(
        view_ctx.session_runtime.ctx,
        profile_name=view_ctx.profile.policies.config_policy_name,
        settings_hash=view_ctx.profile.settings_hash(),
        strict_native_provider_enabled=view_ctx.profile.features.enforce_delta_ffi_provider,
    )
    compiled_policy = _compile_authority_policy(
        plan=plan,
        profile=view_ctx.profile,
        semantic_ir=view_ctx.semantic_context.manifest.semantic_ir,
        capability_snapshot=capability_snapshot,
    )
    authority = ExecutionAuthorityContext(
        semantic_context=view_ctx.semantic_context,
        evidence_plan=evidence_plan,
        extract_executor_map=build_extract_executor_map(evidence_plan=evidence_plan),
        capability_snapshot=capability_snapshot,
        session_runtime_fingerprint=session_runtime_hash(view_ctx.session_runtime),
        enforcement_mode=_execution_authority_enforcement(config),
        compiled_policy=compiled_policy,
    )
    view_ctx.profile.record_artifact(
        EXECUTION_AUTHORITY_VALIDATION_SPEC,
        {
            "enforcement_mode": authority.enforcement_mode,
            "issues": [
                {
                    "code": issue.code,
                    "message": issue.message,
                }
                for issue in authority.validation_issues()
            ],
            "issue_count": len(authority.validation_issues()),
            "session_runtime_fingerprint": authority.session_runtime_fingerprint,
            "required_adapter_keys": list(authority.required_adapter_keys()),
        },
    )
    _record_compiled_policy_artifact(view_ctx.profile, compiled_policy)
    return authority


def _policy_validation_udf_snapshot(plan: ExecutionPlan) -> dict[str, object]:
    snapshot: dict[str, object] = {}
    for node in sorted(plan.view_nodes, key=lambda item: item.name):
        bundle = node.plan_bundle
        if bundle is None:
            continue
        bundle_snapshot = getattr(getattr(bundle, "artifacts", None), "udf_snapshot", None)
        if not isinstance(bundle_snapshot, Mapping):
            continue
        for name in sorted(bundle_snapshot):
            if name not in snapshot:
                snapshot[name] = bundle_snapshot[name]
    return snapshot


def _record_policy_validation_artifact(
    *,
    view_ctx: ViewGraphContext,
    result: PolicyValidationResult,
    mode: Literal["warn", "error"],
    runtime_hash: str,
    semantic_manifest_present: bool,
) -> None:
    from relspec.policy_validation import build_policy_validation_artifact
    from serde_artifacts import (
        PolicyValidationArtifact as PolicyValidationArtifactPayload,
    )
    from serde_artifacts import (
        PolicyValidationIssueArtifact,
    )
    from serde_msgspec import to_builtins_mapping

    artifact = build_policy_validation_artifact(
        result,
        validation_mode=mode,
        runtime_hash=runtime_hash,
    )
    payload = PolicyValidationArtifactPayload(
        validation_mode=artifact.validation_mode,
        issue_count=artifact.issue_count,
        error_codes=tuple(artifact.error_codes),
        runtime_hash=artifact.runtime_hash,
        is_deterministic=artifact.is_deterministic,
        semantic_manifest_present=semantic_manifest_present,
        errors=len(result.errors),
        warnings=len(result.warnings),
        issues=tuple(
            PolicyValidationIssueArtifact(
                code=issue.code,
                severity=issue.severity,
                task=issue.task,
                detail=issue.detail,
            )
            for issue in result.issues
        ),
    )
    view_ctx.profile.record_artifact(
        POLICY_VALIDATION_SPEC,
        to_builtins_mapping(payload, str_keys=True),
    )


def _normalized_compiled_cache_policy(
    policy: str | None,
) -> Literal["none", "delta_staging", "delta_output"] | None:
    if policy in {"none", "delta_staging", "delta_output"}:
        return cast('Literal["none", "delta_staging", "delta_output"]', policy)
    return None


def _plan_with_compiled_cache_policy(
    *,
    plan: ExecutionPlan,
    compiled_policy: CompiledExecutionPolicy | None,
) -> ExecutionPlan:
    if compiled_policy is None or not compiled_policy.cache_policy_by_view:
        return plan

    updated_nodes: list[ViewNode] = []
    changed = False
    for node in plan.view_nodes:
        compiled_value = _normalized_compiled_cache_policy(
            compiled_policy.cache_policy_by_view.get(node.name)
        )
        if compiled_value is None or node.cache_policy == compiled_value:
            updated_nodes.append(node)
            continue
        updated_nodes.append(replace(node, cache_policy=compiled_value))
        changed = True
    if not changed:
        return plan
    return replace(plan, view_nodes=tuple(updated_nodes))


def _enforce_policy_validation_result(
    *,
    result: PolicyValidationResult,
    mode: Literal["warn", "error"],
) -> None:
    if mode != "error" or not result.errors:
        return
    summary = "; ".join(
        f"{issue.code}{f'[{issue.task}]' if issue.task else ''}: {issue.detail or ''}".rstrip(": ")
        for issue in result.errors
    )
    msg = f"Policy validation failed in error mode: {summary}"
    raise RelspecValidationError(msg)


def _incremental_enabled(config: Mapping[str, JsonValue]) -> bool:
    incremental_config = _config_section(config, "incremental")
    if bool(incremental_config.get("enabled")):
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
    incremental_config = _config_section(config, "incremental")
    state_dir_value = incremental_config.get("state_dir")
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
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> tuple[tuple[str, ...] | None, str | None]:
    state_dir = _resolve_incremental_state_dir(config)
    if state_dir is None:
        return None, None
    if dataset_resolver is None:
        return None, str(state_dir)
    from relspec.incremental import CdfImpactRequest, impacted_tasks_for_cdf
    from semantics.incremental.cdf_cursors import CdfCursorStore
    from semantics.incremental.delta_context import DeltaAccessContext
    from semantics.incremental.runtime import IncrementalRuntime, IncrementalRuntimeBuildRequest
    from semantics.incremental.state_store import StateStore

    try:
        runtime = IncrementalRuntime.build(
            IncrementalRuntimeBuildRequest(
                profile=view_ctx.profile,
                determinism_tier=view_ctx.determinism_tier,
                dataset_resolver=dataset_resolver,
            )
        )
    except ValueError:
        return None, str(state_dir)
    context = DeltaAccessContext(runtime=runtime)
    state_store = StateStore(root=state_dir)
    cursor_store = CdfCursorStore(cursors_path=state_store.cdf_cursors_path())
    from datafusion_engine.dataset.registry import DatasetCatalog

    catalog = DatasetCatalog()
    for name in dataset_resolver.names():
        loc = dataset_resolver.location(name)
        if loc is not None:
            catalog.register(name, loc, overwrite=True)
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
    dataset_resolver: ManifestDatasetResolver,
) -> ExecutionPlan:
    impacted, state_dir = _cdf_impacted_tasks(
        view_ctx=view_ctx,
        plan=plan,
        config=config,
        dataset_resolver=dataset_resolver,
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

    Returns:
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

    Returns:
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

    Returns:
    -------
    str | None
        Parsed username when available.
    """
    if isinstance(value, str) and value.strip():
        return value
    return None


def _tracker_dag_name(value: object | None) -> str:
    """Return a non-empty DAG name or fall back to the default.

    Returns:
    -------
    str
        DAG name for tracker registration.
    """
    if isinstance(value, str) and value.strip():
        return value
    return _DEFAULT_DAG_NAME


def _tracker_tags(value: object | None) -> dict[str, str]:
    """Return normalized tracker tags from config sources.

    Returns:
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

    Returns:
    -------
    object | None
        Tracker adapter when enabled and available.
    """
    if hamilton_adapters is None:
        return None

    hamilton_config = _config_section(config, "hamilton")
    if not bool(hamilton_config.get("enable_tracker", False)):
        return None

    project_id = _tracker_project_id(
        _tracker_value(
            hamilton_config,
            config_key="project_id",
            env_key="HAMILTON_PROJECT_ID",
        )
    )
    username = _tracker_username(
        _tracker_value(
            hamilton_config,
            config_key="username",
            env_key="HAMILTON_USERNAME",
        )
    )
    if project_id is None or username is None:
        return None

    dag_name = _tracker_dag_name(
        _tracker_value(
            hamilton_config,
            config_key="dag_name",
            env_key="HAMILTON_DAG_NAME",
        )
    )
    tags = _tracker_tags(hamilton_config.get("tags"))
    api_url_value = _tracker_value(
        hamilton_config,
        config_key="api_url",
        env_key="HAMILTON_API_URL",
    )
    api_url = api_url_value if isinstance(api_url_value, str) else None
    ui_url_value = _tracker_value(
        hamilton_config,
        config_key="ui_url",
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
    hamilton_payload = _mutable_config_section(config_payload, "hamilton")
    dag_name = _resolve_dag_name(hamilton_payload)
    hamilton_payload["dag_name"] = dag_name
    merged_tags = _merge_graph_tags(config_payload, hamilton_payload, plan=plan)
    hamilton_payload["tags"] = merged_tags
    return config_payload


def _resolve_dag_name(hamilton_payload: Mapping[str, JsonValue]) -> str:
    dag_name_value = hamilton_payload.get("dag_name")
    if isinstance(dag_name_value, str) and dag_name_value:
        return dag_name_value
    return _DEFAULT_DAG_NAME


def _merge_graph_tags(
    config_payload: Mapping[str, JsonValue],
    hamilton_payload: Mapping[str, JsonValue],
    *,
    plan: ExecutionPlan,
) -> dict[str, str]:
    merged_tags = _base_graph_tags(config_payload, hamilton_payload)
    _append_plan_tags(merged_tags, plan=plan)
    _append_telemetry_tags(merged_tags, hamilton_payload)
    return merged_tags


def _base_graph_tags(
    config_payload: Mapping[str, JsonValue],
    hamilton_payload: Mapping[str, JsonValue],
) -> dict[str, str]:
    tags_value = hamilton_payload.get("tags")
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
    telemetry_profile = _string_override(hamilton_payload, "telemetry_profile")
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
    hamilton_payload: Mapping[str, JsonValue],
) -> None:
    capture_stats_value = hamilton_payload.get("capture_data_statistics")
    if isinstance(capture_stats_value, bool):
        tags.setdefault(
            "capture_data_statistics",
            "true" if capture_stats_value else "false",
        )
    max_list_value = hamilton_payload.get("max_list_length_capture")
    if isinstance(max_list_value, int):
        tags.setdefault("max_list_length_capture", str(max_list_value))
    max_dict_value = hamilton_payload.get("max_dict_length_capture")
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
    hamilton_payload = _mutable_config_section(config, "hamilton")
    if tracker.project_id is not None:
        hamilton_payload.setdefault("project_id", tracker.project_id)
    if tracker.username is not None:
        hamilton_payload.setdefault("username", tracker.username)
    if tracker.dag_name is not None:
        hamilton_payload.setdefault("dag_name", tracker.dag_name)
    if tracker.api_url is not None:
        hamilton_payload.setdefault("api_url", tracker.api_url)
    if tracker.ui_url is not None:
        hamilton_payload.setdefault("ui_url", tracker.ui_url)
    if tracker.enabled:
        hamilton_payload.setdefault("enable_tracker", True)
    return config


def _apply_hamilton_telemetry_profile(
    config: dict[str, JsonValue],
    *,
    profile_spec: RuntimeProfileSpec,
) -> dict[str, JsonValue]:
    telemetry = getattr(profile_spec, "hamilton_telemetry", None)
    if telemetry is None:
        return config
    hamilton_payload = _mutable_config_section(config, "hamilton")
    hamilton_payload.setdefault("telemetry_profile", telemetry.name)
    hamilton_payload.setdefault("capture_data_statistics", telemetry.capture_data_statistics)
    hamilton_payload.setdefault("max_list_length_capture", telemetry.max_list_length_capture)
    hamilton_payload.setdefault("max_dict_length_capture", telemetry.max_dict_length_capture)
    if "enable_tracker" not in hamilton_payload:
        hamilton_payload["enable_tracker"] = telemetry.enable_tracker
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
    hamilton_config = _config_section(config, "hamilton")
    capture = hamilton_config.get("capture_data_statistics")
    capture_value = capture if isinstance(capture, bool) else telemetry.capture_data_statistics
    max_list = hamilton_config.get("max_list_length_capture")
    max_list_value = max_list if isinstance(max_list, int) else telemetry.max_list_length_capture
    max_dict = hamilton_config.get("max_dict_length_capture")
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
    hamilton_payload = _mutable_config_section(config_payload, "hamilton")
    hamilton_payload.setdefault("enable_graph_snapshot", True)
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
        raw_kwargs = (
            cast("Mapping[str, JsonValue] | None", executor_config.dask_client_kwargs)
            if executor_config is not None
            else None
        )
        client_kwargs = _parse_dask_client_kwargs(raw_kwargs)
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

    plan_config = _config_section(options.config, "plan")
    submission_value = plan_config.get("enable_plan_task_submission_hook")
    enable_submission_hook = submission_value if isinstance(submission_value, bool) else True
    grouping_value = plan_config.get("enable_plan_task_grouping_hook")
    enable_grouping_hook = grouping_value if isinstance(grouping_value, bool) else True
    enforce_value = plan_config.get("enforce_plan_task_submission")
    enforce_submission = enforce_value if isinstance(enforce_value, bool) else True
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
    graph_adapter = _config_section(config, "graph_adapter")
    value = graph_adapter.get("kind")
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip().lower()
    if normalized not in {"threadpool", "dask", "ray"}:
        return None
    options_value = graph_adapter.get("options")
    options: dict[str, JsonValue] | None = None
    if isinstance(options_value, Mapping):
        options = {str(key): cast("JsonValue", value) for key, value in options_value.items()}
    return GraphAdapterConfig(kind=cast("GraphAdapterKind", normalized), options=options)


def _graph_adapter_from_config(
    adapter_config: GraphAdapterConfig,
    *,
    executor_config: ExecutorConfig | None,
) -> HamiltonGraphAdapter:
    options = cast("dict[str, JsonValue]", dict(adapter_config.options or {}))
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

    Returns:
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

    Returns:
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

    Args:
        options: Executor configuration mapping.
        executor_config: Optional executor config object.

    Returns:
        HamiltonGraphAdapter: Result.

    Raises:
        ValueError: If Dask extras are unavailable.
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

        Returns:
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

        Returns:
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
    cache_config = _config_section(config, "cache")
    profile_value = cache_config.get("policy_profile")
    profile_name = profile_value.strip() if isinstance(profile_value, str) else ""
    cache_opt_in_value = cache_config.get("opt_in")
    cache_opt_in = cache_opt_in_value if isinstance(cache_opt_in_value, bool) else True
    default_behavior: CacheBehavior = "disable" if cache_opt_in else "default"
    default_loader_behavior: CacheBehavior = "recompute"
    default_saver_behavior: CacheBehavior = "disable"
    if profile_name == "aggressive":
        default_loader_behavior = "default"
        default_saver_behavior = "default"
    default_override = _cache_behavior_override(cache_config, "default_behavior")
    if default_override is not None:
        default_behavior = default_override
    loader_override = _cache_behavior_override(
        cache_config,
        "default_loader_behavior",
    )
    if loader_override is not None:
        default_loader_behavior = loader_override
    saver_override = _cache_behavior_override(cache_config, "default_saver_behavior")
    if saver_override is not None:
        default_saver_behavior = saver_override
    log_to_file_value = cache_config.get("log_to_file")
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
    cache_config = _config_section(config, "cache")
    explicit = _task_name_list_from_config(cache_config, key="default_nodes")
    if explicit is not None:
        return explicit
    from hamilton_pipeline.io_contracts import delta_output_specs
    from semantics.naming import internal_name

    defaults: set[str] = {spec.table_node for spec in delta_output_specs()}
    semantic_output = profile_spec.datafusion.data_sources.semantic_output
    cache_overrides = dict(semantic_output.cache_overrides or {})
    for dataset_name, policy in cache_overrides.items():
        node_name = internal_name(dataset_name)
        if policy in {"delta_output", "delta_staging"}:
            defaults.add(node_name)
        if policy == "none" and node_name in defaults:
            defaults.remove(node_name)
    if not cache_overrides and semantic_output.locations:
        for dataset_name in semantic_output.locations:
            defaults.add(internal_name(dataset_name))
    return tuple(sorted(defaults))


def _cache_path_from_config(config: Mapping[str, JsonValue]) -> str | None:
    cache_config = _config_section(config, "cache")
    value = cache_config.get("path")
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


def _config_flag(
    section: Mapping[str, JsonValue],
    key: str,
    *,
    default: bool = True,
) -> bool:
    value = section.get(key)
    if isinstance(value, bool):
        return value
    return default


def _tracker_adapters(
    config: Mapping[str, JsonValue],
    context: _AdapterContext,
) -> list[lifecycle_base.LifecycleAdapter]:
    tracker = _maybe_build_tracker_adapter(config, profile_spec=context.profile_spec)
    return [tracker] if tracker is not None else []


def _hamilton_adapters(
    config: Mapping[str, JsonValue],
    context: _AdapterContext,
) -> list[lifecycle_base.LifecycleAdapter]:
    hamilton_config = _config_section(config, "hamilton")
    adapters: list[lifecycle_base.LifecycleAdapter] = []
    if _config_flag(hamilton_config, "enable_type_checker"):
        adapters.append(CodeAnatomyTypeChecker())
    if _config_flag(hamilton_config, "enable_node_diagnostics"):
        adapters.append(DiagnosticsNodeHook(context.diagnostics))
    if _config_flag(hamilton_config, "enable_structured_run_logs"):
        from hamilton_pipeline.structured_logs import StructuredLogHook

        adapters.append(
            StructuredLogHook(
                profile=context.profile,
                config=config,
                plan_signature=context.plan.plan_signature,
            )
        )
    return adapters


def _otel_adapters(
    config: Mapping[str, JsonValue],
    _context: _AdapterContext,
) -> list[lifecycle_base.LifecycleAdapter]:
    otel_config = _config_section(config, "otel")
    adapters: list[lifecycle_base.LifecycleAdapter] = []
    if _config_flag(otel_config, "enable_node_tracing"):
        adapters.append(OtelNodeHook())
    if _config_flag(otel_config, "enable_plan_tracing"):
        adapters.append(OtelPlanHook())
    return adapters


def _plan_adapters(
    config: Mapping[str, JsonValue],
    context: _AdapterContext,
) -> list[lifecycle_base.LifecycleAdapter]:
    plan_config = _config_section(config, "plan")
    if not _config_flag(plan_config, "enable_plan_diagnostics"):
        return []
    return [
        PlanDiagnosticsHook(
            plan=context.plan,
            profile=context.profile,
            collector=context.diagnostics,
        )
    ]


def _apply_adapters(
    builder: driver.Builder,
    *,
    config: Mapping[str, JsonValue],
    context: _AdapterContext,
) -> driver.Builder:
    adapters = (
        _tracker_adapters(config, context)
        + _hamilton_adapters(config, context)
        + _otel_adapters(config, context)
        + _plan_adapters(config, context)
    )
    for adapter in adapters:
        builder = builder.with_adapters(adapter)
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

    Returns:
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
            dataset_resolver=resolved_view_ctx.semantic_context.dataset_resolver,
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
            dataset_resolver=resolved_view_ctx.semantic_context.dataset_resolver,
        )
    resolved_view_ctx = _apply_workload_session_profile(
        view_ctx=resolved_view_ctx,
        plan=resolved_plan,
    )
    authority_context = _build_execution_authority(
        view_ctx=resolved_view_ctx,
        plan=resolved_plan,
        config=request.config,
    )
    resolved_plan = _plan_with_compiled_cache_policy(
        plan=resolved_plan,
        compiled_policy=authority_context.compiled_policy,
    )
    from relspec.policy_validation import validate_policy_bundle

    policy_validation_result = validate_policy_bundle(
        resolved_plan,
        runtime_profile=resolved_view_ctx.profile,
        udf_snapshot=_policy_validation_udf_snapshot(resolved_plan),
        capability_snapshot=authority_context.capability_snapshot,
        semantic_manifest=authority_context.semantic_context.manifest,
        compiled_policy=authority_context.compiled_policy,
    )
    _record_policy_validation_artifact(
        view_ctx=resolved_view_ctx,
        result=policy_validation_result,
        mode=authority_context.enforcement_mode,
        runtime_hash=authority_context.session_runtime_fingerprint or "",
        semantic_manifest_present=authority_context.semantic_context.manifest is not None,
    )
    _enforce_policy_validation_result(
        result=policy_validation_result,
        mode=authority_context.enforcement_mode,
    )
    _record_execution_package(
        profile=resolved_view_ctx.profile,
        manifest=authority_context.semantic_context.manifest,
        compiled_policy=authority_context.compiled_policy,
        capability_snapshot=authority_context.capability_snapshot,
        plan_fingerprints=resolved_plan.plan_fingerprints,
    )
    _record_policy_counterfactual_artifact(
        profile=resolved_view_ctx.profile,
        compiled_policy=authority_context.compiled_policy,
        plan=resolved_plan,
    )
    run_id = get_run_id() or authority_context.session_runtime_fingerprint or "unknown"
    provenance_graph = _record_decision_provenance_artifact(
        profile=resolved_view_ctx.profile,
        compiled_policy=authority_context.compiled_policy,
        run_id=run_id,
    )
    _record_fallback_quarantine_artifact(
        profile=resolved_view_ctx.profile,
        run_id=run_id,
        graph=provenance_graph,
    )
    from hamilton_pipeline.validators import set_schema_contracts

    set_schema_contracts(resolved_plan.output_contracts)
    modules.append(
        build_execution_plan_module(
            resolved_plan,
            plan_module_options=PlanModuleOptions(
                execution_authority_context=authority_context,
            ),
        )
    )
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

    Returns:
    -------
    DriverBuilder
        Builder wrapper for synchronous drivers.
    """
    _ensure_hamilton_dataframe_types()
    _patch_hamilton_file_metadata()
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
    cache_config = _config_section(config_payload, "cache")
    cache_path = cache_config.get("path")
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
    hamilton_config = _config_section(config_payload, "hamilton")
    enable_snapshot = hamilton_config.get("enable_graph_snapshot")
    if not isinstance(enable_snapshot, bool):
        enable_snapshot = True
    if enable_snapshot:
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
      - cache.path: str | None
      - cache.opt_in: bool (if True, default_behavior="disable")
      - hamilton.enable_tracker and tracker config keys

    Returns:
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
    """Caches built Hamilton Drivers by plan-aware fingerprint.

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

        Returns:
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
            dataset_resolver=view_ctx.semantic_context.dataset_resolver,
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
