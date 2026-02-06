"""Canonical DataFusion plan bundle for all planning and scheduling.

This module provides the single canonical plan artifact that all execution
and scheduling paths use.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, cast
from urllib.parse import urlparse

import datafusion as _datafusion
import msgspec
import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from core_types import JsonValue
from datafusion_engine.delta.protocol import DeltaProtocolSnapshot
from datafusion_engine.delta.store_policy import (
    apply_delta_store_policy,
    delta_store_policy_hash,
)
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.plan.cache import PlanProtoCacheEntry
from datafusion_engine.plan.diagnostics import PlanPhaseDiagnostics, record_plan_phase_diagnostics
from datafusion_engine.plan.normalization import normalize_substrait_plan
from datafusion_engine.plan.profiler import ExplainCapture, capture_explain
from datafusion_engine.schema.introspection import SchemaIntrospector
from datafusion_engine.session.runtime import (
    extract_output_locations_for_profile,
    normalize_dataset_locations_for_profile,
    semantic_output_locations_for_profile,
)
from obs.otel.scopes import SCOPE_PLANNING
from obs.otel.tracing import stage_span
from serde_artifacts import DeltaInputPin, PlanArtifacts, PlanProtoStatus
from serde_msgspec import to_builtins
from serde_msgspec_ext import (
    ExecutionPlanProtoBytes,
    LogicalPlanProtoBytes,
    OptimizedPlanProtoBytes,
)
from serde_schema_registry import schema_contract_hash
from utils.hashing import (
    hash_json_default,
    hash_msgpack_canonical,
    hash_settings,
    hash_sha256_hex,
)

if TYPE_CHECKING:
    from datafusion.plan import LogicalPlan as DataFusionLogicalPlan

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
    from datafusion_engine.sql.options import SQLOptions


_datafusion_internal = getattr(_datafusion, "_internal", None)

try:
    from datafusion.substrait import Producer as SubstraitProducer
except ImportError:
    SubstraitProducer = None

_SUBSTRAIT_INTERNAL = (
    getattr(_datafusion_internal, "substrait", None) if _datafusion_internal is not None else None
)

# Type alias for DataFrame builder functions
DataFrameBuilder = Callable[[SessionContext], DataFrame]


@dataclass(frozen=True)
class PlanBundleOptions:
    """Options for building a DataFusion plan bundle."""

    compute_execution_plan: bool = True
    compute_substrait: bool = True
    validate_udfs: bool = False
    enable_proto_serialization: bool | None = None
    registry_snapshot: Mapping[str, object] | None = None
    delta_inputs: Sequence[DeltaInputPin] = ()
    session_runtime: SessionRuntime | None = None
    scan_units: Sequence[ScanUnit] = ()


@dataclass(frozen=True)
class PlanDetailContext:
    """Optional inputs for plan detail diagnostics."""

    cdf_windows: Sequence[Mapping[str, object]] = ()
    delta_store_policy_hash: str | None = None
    information_schema_hash: str | None = None
    snapshot_keys: Sequence[Mapping[str, object]] = ()
    write_outcomes: Sequence[Mapping[str, object]] = ()


@dataclass(frozen=True)
class PlanDetailInputs:
    """Inputs used to assemble plan detail diagnostics."""

    artifacts: PlanArtifacts
    plan_fingerprint: str
    logical: object | None
    optimized: object | None
    execution: object | None
    explain_tree: ExplainCapture | None
    explain_verbose: ExplainCapture | None
    explain_analyze: ExplainCapture | None
    substrait_validation: Mapping[str, object] | None = None
    proto_status: PlanProtoStatus | None = None
    detail_context: PlanDetailContext | None = None


@dataclass(frozen=True)
class DataFusionPlanBundle:
    """Canonical plan artifact for all planning and scheduling.

    This is the single source of truth for DataFusion plan information,

    Attributes:
    ----------
    df : DataFrame
        The DataFusion DataFrame for this plan.
    logical_plan : DataFusionLogicalPlan
        The unoptimized logical plan.
    optimized_logical_plan : DataFusionLogicalPlan
        The optimized logical plan (used for lineage extraction).
    execution_plan : DataFusionExecutionPlan | None
        The physical execution plan (may be None for lazy evaluation).
    substrait_bytes : bytes
        Substrait serialization of the plan (used for fingerprinting).
    plan_fingerprint : str
        Stable hash for caching and comparison.
    artifacts : PlanArtifacts
        Serializable artifacts used for determinism, caching, and scheduling.
    plan_details : Mapping[str, object]
        Additional plan metadata for diagnostics.
    """

    df: DataFrame
    logical_plan: object  # DataFusionLogicalPlan
    optimized_logical_plan: object  # DataFusionLogicalPlan
    execution_plan: object | None  # DataFusionExecutionPlan | None
    substrait_bytes: bytes
    plan_fingerprint: str
    artifacts: PlanArtifacts
    delta_inputs: tuple[DeltaInputPin, ...] = ()
    scan_units: tuple[ScanUnit, ...] = ()
    plan_identity_hash: str | None = None
    required_udfs: tuple[str, ...] = ()
    required_rewrite_tags: tuple[str, ...] = ()
    plan_details: Mapping[str, object] = field(default_factory=dict)

    def display_logical_plan(self) -> str | None:
        """Return a string representation of the logical plan.

        Returns:
        -------
        str | None
            Indented logical plan display, or None if unavailable.
        """
        return _plan_display(self.logical_plan, method="display_indent_schema")

    def display_optimized_plan(self) -> str | None:
        """Return a string representation of the optimized logical plan.

        Returns:
        -------
        str | None
            Indented optimized plan display, or None if unavailable.
        """
        return _plan_display(self.optimized_logical_plan, method="display_indent_schema")

    def display_execution_plan(self) -> str | None:
        """Return a string representation of the physical execution plan.

        Returns:
        -------
        str | None
            Indented execution plan display, or None if unavailable.
        """
        if self.execution_plan is None:
            return None
        return _plan_display(self.execution_plan, method="display_indent")

    def graphviz(self) -> str | None:
        """Return GraphViz DOT representation of the optimized plan.

        Returns:
        -------
        str | None
            GraphViz DOT string, or None if unavailable.
        """
        method = getattr(self.optimized_logical_plan, "display_graphviz", None)
        if not callable(method):
            return None
        try:
            return str(method())
        except (RuntimeError, TypeError, ValueError):
            return None


def _delta_inputs_from_scan_units(
    scan_units: Sequence[ScanUnit],
) -> tuple[DeltaInputPin, ...]:
    """Derive DeltaInputPin entries from scan units.

    Args:
        scan_units: Description.

    Returns:
        tuple[DeltaInputPin, ...]: Result.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    pins: dict[str, DeltaInputPin] = {}
    for unit in scan_units:
        timestamp = unit.delta_timestamp
        if timestamp is None and unit.snapshot_timestamp is not None:
            timestamp = str(unit.snapshot_timestamp)
        protocol = unit.delta_protocol
        scan_config = unit.delta_scan_config
        scan_config_hash = unit.delta_scan_config_hash
        provider = unit.datafusion_provider
        protocol_compatible = unit.protocol_compatible
        protocol_compatibility = unit.protocol_compatibility
        if unit.delta_version is None and timestamp is None:
            continue
        existing = pins.get(unit.dataset_name)
        candidate_pin = DeltaInputPin(
            dataset_name=unit.dataset_name,
            version=unit.delta_version,
            timestamp=timestamp,
            protocol=protocol,
            delta_scan_config=scan_config,
            delta_scan_config_hash=scan_config_hash,
            datafusion_provider=provider,
            protocol_compatible=protocol_compatible,
            protocol_compatibility=protocol_compatibility,
        )
        if existing is not None:
            existing_state = _delta_pin_state_from_pin(existing)
            candidate_state = _delta_pin_state_from_pin(candidate_pin)
            if existing_state != candidate_state:
                msg = (
                    "Conflicting Delta pins for dataset "
                    f"{unit.dataset_name!r}: "
                    f"{existing_state} vs {candidate_state}"
                )
                raise ValueError(msg)
        pins[unit.dataset_name] = candidate_pin
    return tuple(pins[name] for name in sorted(pins))


def _delta_pin_state_from_pin(
    pin: DeltaInputPin,
) -> tuple[
    int | None,
    str | None,
    DeltaProtocolSnapshot | None,
    str | None,
    str | None,
    bool | None,
]:
    """Build a comparable state tuple from a Delta pin.

    Returns:
    -------
    tuple
        Tuple of comparable pin fields used for conflict detection.
    """
    return (
        pin.version,
        pin.timestamp,
        pin.protocol,
        pin.delta_scan_config_hash,
        pin.datafusion_provider,
        pin.protocol_compatible,
    )


def build_plan_bundle(
    ctx: SessionContext,
    df: DataFrame,
    *,
    options: PlanBundleOptions | None = None,
) -> DataFusionPlanBundle:
    """Build a canonical plan bundle from a DataFusion DataFrame.

    Args:
        ctx: DataFusion session context.
        df: DataFrame to plan.
        options: Optional plan-bundle options.

    Returns:
        DataFusionPlanBundle: Result.

    Raises:
        ValueError: If required bundle options are missing.
    """
    with stage_span(
        "planning.plan_bundle",
        stage="planning",
        scope_name=SCOPE_PLANNING,
        attributes={"codeanatomy.plan_kind": "bundle"},
    ):
        resolved = options or PlanBundleOptions()
        if not resolved.compute_substrait:
            msg = "Substrait bytes are required for plan bundle construction."
            raise ValueError(msg)
        if resolved.session_runtime is None:
            msg = "SessionRuntime is required for plan bundle construction."
            raise ValueError(msg)
        components = _bundle_components(
            ctx,
            df,
            options=resolved,
        )

        bundle = DataFusionPlanBundle(
            df=df,
            logical_plan=components.logical,
            optimized_logical_plan=components.optimized,
            execution_plan=components.execution,
            substrait_bytes=components.substrait_bytes,
            plan_fingerprint=components.fingerprint,
            artifacts=components.artifacts,
            delta_inputs=components.merged_delta_inputs,
            scan_units=components.scan_units,
            plan_identity_hash=components.plan_identity_hash,
            required_udfs=components.required_udfs,
            required_rewrite_tags=components.required_rewrite_tags,
            plan_details=components.plan_details,
        )
        _store_plan_cache_entry(
            bundle=bundle,
            runtime_profile=resolved.session_runtime.profile,
        )
        return bundle


def _store_plan_cache_entry(
    *,
    bundle: DataFusionPlanBundle,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    cache = runtime_profile.plan_proto_cache
    if cache is None:
        return
    if bundle.plan_identity_hash is None:
        return
    artifacts = bundle.artifacts
    entry = PlanProtoCacheEntry(
        plan_identity_hash=bundle.plan_identity_hash,
        plan_fingerprint=bundle.plan_fingerprint,
        substrait_bytes=bundle.substrait_bytes,
        logical_plan_proto=_plan_proto_data(artifacts.logical_plan_proto),
        optimized_plan_proto=_plan_proto_data(artifacts.optimized_plan_proto),
        execution_plan_proto=_plan_proto_data(artifacts.execution_plan_proto),
    )
    cache.put(entry)


def _plan_proto_data(
    payload: LogicalPlanProtoBytes | OptimizedPlanProtoBytes | ExecutionPlanProtoBytes | None,
) -> bytes | None:
    """Return proto bytes from wrapped payloads.

    Parameters
    ----------
    payload
        Wrapped plan proto payload.

    Returns:
    -------
    bytes | None
        Raw proto bytes when available.
    """
    if payload is None:
        return None
    return payload.data


@dataclass(frozen=True)
class _BundleComponents:
    """Bundle derived artifacts and plan metadata."""

    logical: DataFusionLogicalPlan
    optimized: DataFusionLogicalPlan | None
    execution: object | None
    substrait_bytes: bytes
    fingerprint: str
    artifacts: PlanArtifacts
    merged_delta_inputs: tuple[DeltaInputPin, ...]
    scan_units: tuple[ScanUnit, ...]
    plan_identity_hash: str | None
    required_udfs: tuple[str, ...]
    required_rewrite_tags: tuple[str, ...]
    plan_details: Mapping[str, object]


@dataclass(frozen=True)
class _BundleAssemblyState:
    """Intermediate state for bundle component assembly."""

    plan_core: _PlanCoreComponents
    explain_artifacts: _ExplainArtifacts
    proto_enabled: bool
    proto_status: PlanProtoStatus | None
    udf_artifacts: _UdfArtifacts
    registry_artifacts: _RegistryArtifacts
    environment: _EnvironmentArtifacts
    required: _RequiredUdfArtifacts
    substrait_validation: Mapping[str, object] | None
    merged_delta_inputs: tuple[DeltaInputPin, ...]
    snapshot_keys: tuple[dict[str, object], ...]
    fingerprint: str
    artifacts: PlanArtifacts


@dataclass(frozen=True)
class _BundleIdentityResult:
    """Identity payload/hash derived for a bundle assembly state."""

    payload: Mapping[str, object] | None
    plan_identity_hash: str | None


@dataclass(frozen=True)
class _PlanCoreComponents:
    """Core logical/physical plan objects for bundling."""

    logical: DataFusionLogicalPlan
    optimized: DataFusionLogicalPlan | None
    execution: object | None
    substrait_bytes: bytes
    logical_ms: float | None
    optimized_ms: float | None
    execution_ms: float | None
    substrait_ms: float | None


@dataclass(frozen=True)
class _ExplainArtifacts:
    """Explain outputs captured during planning."""

    tree: ExplainCapture | None
    verbose: ExplainCapture | None
    analyze: ExplainCapture | None


@dataclass(frozen=True)
class _EnvironmentArtifacts:
    """Captured environment snapshots used for plan fingerprinting."""

    df_settings: Mapping[str, str]
    planning_env_snapshot: Mapping[str, object]
    planning_env_hash: str
    rulepack_snapshot: Mapping[str, object] | None
    rulepack_hash: str | None
    information_schema_snapshot: Mapping[str, object]
    information_schema_hash: str
    cdf_windows: tuple[dict[str, object], ...]
    delta_store_policy_hash: str | None


@dataclass(frozen=True)
class _PlanArtifactsInputs:
    """Inputs for assembling PlanArtifacts."""

    plan_core: _PlanCoreComponents
    explain_artifacts: _ExplainArtifacts
    udf_artifacts: _UdfArtifacts
    registry_artifacts: _RegistryArtifacts
    environment: _EnvironmentArtifacts
    substrait_validation: Mapping[str, object] | None
    proto_enabled: bool = True


@dataclass(frozen=True)
class _PlanDisplaySection:
    """Display-oriented plan details and physical plan text."""

    payload: dict[str, object]
    physical_plan: str | None


@dataclass
class _PlanDetailsBuilder:
    """Mutable builder for plan detail payload assembly."""

    details: dict[str, object] = field(default_factory=dict)

    def add_section(self, section: Mapping[str, object]) -> None:
        self.details.update(section)

    def build(self) -> dict[str, object]:
        return dict(self.details)


def _plan_core_components(
    ctx: SessionContext,
    df: DataFrame,
    *,
    options: PlanBundleOptions,
) -> _PlanCoreComponents:
    """Collect core logical/physical plan objects.

    Args:
        ctx: DataFusion session context.
        df: DataFrame to plan.
        options: Bundle planning options.

    Returns:
        _PlanCoreComponents: Result.

    Raises:
        ValueError: If substrait generation is disabled.
    """
    t0 = time.perf_counter()
    logical = cast("DataFusionLogicalPlan", _safe_logical_plan(df))
    logical_ms = (time.perf_counter() - t0) * 1000.0
    t1 = time.perf_counter()
    optimized = cast("DataFusionLogicalPlan | None", _safe_optimized_logical_plan(df))
    optimized_ms = (time.perf_counter() - t1) * 1000.0
    execution = None
    execution_ms = None
    if options.compute_execution_plan:
        t2 = time.perf_counter()
        execution = _safe_execution_plan(df)
        execution_ms = (time.perf_counter() - t2) * 1000.0
    if not options.compute_substrait:
        msg = "Substrait bytes are required for plan bundle construction."
        raise ValueError(msg)
    t3 = time.perf_counter()
    substrait_bytes = _to_substrait_bytes(ctx, optimized)
    substrait_ms = (time.perf_counter() - t3) * 1000.0
    return _PlanCoreComponents(
        logical=logical,
        optimized=optimized,
        execution=execution,
        substrait_bytes=substrait_bytes,
        logical_ms=logical_ms,
        optimized_ms=optimized_ms,
        execution_ms=execution_ms,
        substrait_ms=substrait_ms,
    )


def _record_plan_phase_telemetry(
    plan_core: _PlanCoreComponents,
    *,
    plan_hash: str,
    plan_identity_hash: str | None,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> None:
    if runtime_profile is None:
        return

    def _emit_phase(phase: str, duration_ms: float | None) -> None:
        if duration_ms is None:
            return
        record_plan_phase_diagnostics(
            request=PlanPhaseDiagnostics(
                runtime_profile=runtime_profile,
                plan_hash=plan_hash,
                plan_identity_hash=plan_identity_hash,
                phase=phase,
                duration_ms=duration_ms,
            )
        )

    _emit_phase("logical", plan_core.logical_ms)
    if plan_core.optimized is not None:
        _emit_phase("optimized", plan_core.optimized_ms)
    if plan_core.execution is not None:
        _emit_phase("physical", plan_core.execution_ms)


def _capture_explain_artifacts(
    df: DataFrame,
    *,
    session_runtime: SessionRuntime | None,
) -> _ExplainArtifacts:
    """Capture explain outputs for plan artifacts.

    Returns:
    -------
    _ExplainArtifacts
        Explain outputs captured for the bundle.
    """
    if _is_explain_plan(_safe_logical_plan(df)):
        return _ExplainArtifacts(tree=None, verbose=None, analyze=None)
    verbose = None
    if session_runtime is not None and session_runtime.profile.diagnostics.explain_verbose:
        verbose = capture_explain(df, verbose=True, analyze=False)
    return _ExplainArtifacts(
        tree=capture_explain(df, verbose=False, analyze=False),
        verbose=verbose,
        analyze=_capture_explain_analyze(df, session_runtime=session_runtime),
    )


def _is_explain_plan(plan: object | None) -> bool:
    if plan is None:
        return False
    to_variant = getattr(plan, "to_variant", None)
    if not callable(to_variant):
        return False
    try:
        variant = to_variant()
    except (RuntimeError, TypeError, ValueError):
        return False
    return type(variant).__name__ in {"Analyze", "Explain"}


def _environment_artifacts(
    ctx: SessionContext,
    *,
    session_runtime: SessionRuntime | None,
) -> _EnvironmentArtifacts:
    """Capture planning environment snapshots.

    Returns:
    -------
    _EnvironmentArtifacts
        Environment artifacts for fingerprints and diagnostics.
    """
    df_settings = _df_settings_snapshot(ctx, session_runtime=session_runtime)
    planning_env_snapshot = _planning_env_snapshot(session_runtime)
    planning_env_hash = _planning_env_hash(planning_env_snapshot)
    rulepack_snapshot = _rulepack_snapshot(ctx)
    rulepack_hash = _rulepack_hash(rulepack_snapshot)
    info_schema_snapshot = _information_schema_snapshot(ctx, session_runtime=session_runtime)
    info_schema_hash = _information_schema_hash(info_schema_snapshot)
    cdf_windows = _cdf_window_snapshot(session_runtime)
    store_policy_hash = None
    if session_runtime is not None:
        store_policy_hash = delta_store_policy_hash(
            session_runtime.profile.policies.delta_store_policy
        )
    return _EnvironmentArtifacts(
        df_settings=df_settings,
        planning_env_snapshot=planning_env_snapshot,
        planning_env_hash=planning_env_hash,
        rulepack_snapshot=rulepack_snapshot,
        rulepack_hash=rulepack_hash,
        information_schema_snapshot=info_schema_snapshot,
        information_schema_hash=info_schema_hash,
        cdf_windows=cdf_windows,
        delta_store_policy_hash=store_policy_hash,
    )


def _session_config_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    config_method = getattr(ctx, "config", None)
    if not callable(config_method):
        return {}
    try:
        config = config_method()
    except (RuntimeError, TypeError, ValueError):
        return {}
    to_dict = getattr(config, "to_dict", None)
    if not callable(to_dict):
        return {}
    try:
        payload = to_dict()
    except (RuntimeError, TypeError, ValueError):
        return {}
    if isinstance(payload, Mapping):
        return dict(payload)
    return {}


def _planning_env_snapshot(
    session_runtime: SessionRuntime | None,
) -> Mapping[str, object]:
    """Return planning-relevant environment settings for determinism.

    Parameters
    ----------
    session_runtime
        Session runtime used to resolve profile settings.

    Returns:
    -------
    Mapping[str, object]
        Planning environment snapshot payload.
    """
    if session_runtime is None:
        return {}
    profile = session_runtime.profile
    session_config = _session_config_snapshot(session_runtime.ctx)
    sql_policy_payload = None
    if profile.policies.sql_policy is not None:
        sql_policy_payload = {
            "allow_ddl": profile.policies.sql_policy.allow_ddl,
            "allow_dml": profile.policies.sql_policy.allow_dml,
            "allow_statements": profile.policies.sql_policy.allow_statements,
        }
    schema_hardening = profile.policies.schema_hardening
    return {
        "datafusion_version": getattr(profile, "datafusion_version", None),
        "session_config": session_config,
        "settings_payload": profile.settings_payload(),
        "settings_hash": profile.settings_hash(),
        "sql_policy_name": profile.policies.sql_policy_name,
        "sql_policy": sql_policy_payload,
        "explain_controls": {
            "capture_explain": profile.diagnostics.capture_explain,
            "explain_verbose": profile.diagnostics.explain_verbose,
            "explain_analyze": profile.diagnostics.explain_analyze,
            "explain_analyze_level": profile.diagnostics.explain_analyze_level,
        },
        "execution": {
            "target_partitions": profile.execution.target_partitions,
            "batch_size": profile.execution.batch_size,
            "repartition_aggregations": profile.execution.repartition_aggregations,
            "repartition_windows": profile.execution.repartition_windows,
            "repartition_file_scans": profile.execution.repartition_file_scans,
            "repartition_file_min_size": profile.execution.repartition_file_min_size,
        },
        "runtime_env": {
            "spill_dir": profile.execution.spill_dir,
            "memory_pool": profile.execution.memory_pool,
            "memory_limit_bytes": profile.execution.memory_limit_bytes,
            "enable_cache_manager": profile.features.enable_cache_manager,
        },
        "async_udf": {
            "enable_async_udfs": profile.features.enable_async_udfs,
            "async_udf_timeout_ms": profile.policies.async_udf_timeout_ms,
            "async_udf_batch_size": profile.policies.async_udf_batch_size,
        },
        "delta_protocol": {
            "mode": profile.policies.delta_protocol_mode,
            "support": _delta_protocol_support_payload(profile.policies.delta_protocol_support),
        },
        "schema_hardening": {
            "explain_format": schema_hardening.explain_format if schema_hardening else None,
            "enable_view_types": schema_hardening.enable_view_types if schema_hardening else None,
        },
    }


def _delta_protocol_support_payload(
    support: object | None,
) -> Mapping[str, object] | None:
    if support is None:
        return None
    max_reader = getattr(support, "max_reader_version", None)
    max_writer = getattr(support, "max_writer_version", None)
    reader_features = getattr(support, "supported_reader_features", ())
    writer_features = getattr(support, "supported_writer_features", ())
    return {
        "max_reader_version": max_reader,
        "max_writer_version": max_writer,
        "supported_reader_features": list(reader_features),
        "supported_writer_features": list(writer_features),
    }


def _planning_env_hash(snapshot: Mapping[str, object]) -> str:
    return hash_msgpack_canonical(snapshot)


def _rulepack_snapshot(ctx: SessionContext) -> Mapping[str, object] | None:
    """Return a snapshot of planner rulepacks when available.

    Returns:
    -------
    Mapping[str, object] | None
        Rulepack metadata payload, or ``None`` when unavailable.
    """
    containers: list[object] = [ctx]
    for attr in ("state", "session_state"):
        candidate = getattr(ctx, attr, None)
        if callable(candidate):
            with _suppress_errors():
                candidate = candidate()
        if candidate is not None:
            containers.append(candidate)
    snapshot: dict[str, object] = {}
    for container in containers:
        analyzer = _extract_rule_names(container, "analyzer_rules")
        optimizer = _extract_rule_names(container, "optimizer_rules")
        physical = _extract_rule_names(container, "physical_optimizer_rules")
        if analyzer is not None:
            snapshot["analyzer_rules"] = analyzer
        if optimizer is not None:
            snapshot["optimizer_rules"] = optimizer
        if physical is not None:
            snapshot["physical_optimizer_rules"] = physical
    if snapshot:
        snapshot["status"] = "ok"
        snapshot["source"] = type(containers[-1]).__name__
        return snapshot
    return {"status": "unavailable", "reason": "rulepack APIs not exposed"}


def _extract_rule_names(container: object, attr: str) -> list[str] | None:
    rules = getattr(container, attr, None)
    if callable(rules):
        with _suppress_errors():
            rules = rules()
    if rules is None:
        return None
    if isinstance(rules, Sequence) and not isinstance(rules, (str, bytes)):
        names: list[str] = []
        for rule in rules:
            name = _rule_name(rule)
            if name:
                names.append(name)
        return names
    return None


def _rule_name(rule: object) -> str | None:
    if rule is None:
        return None
    name_attr = getattr(rule, "name", None)
    if callable(name_attr):
        with _suppress_errors():
            name_attr = name_attr()
    if isinstance(name_attr, str) and name_attr:
        return name_attr
    try:
        return type(rule).__name__
    except (TypeError, ValueError):
        return None


def _rulepack_hash(snapshot: Mapping[str, object] | None) -> str | None:
    if not snapshot:
        return None
    return hash_msgpack_canonical(snapshot)


def _suppress_errors() -> contextlib.AbstractContextManager[None]:
    return contextlib.suppress(RuntimeError, TypeError, ValueError)


def _merged_delta_inputs_for_bundle(
    ctx: SessionContext,
    *,
    plan: DataFusionLogicalPlan,
    options: PlanBundleOptions,
) -> tuple[DeltaInputPin, ...]:
    """Resolve Delta input pins for plan fingerprinting.

    Returns:
    -------
    tuple[DeltaInputPin, ...]
        Merged Delta input pins from explicit inputs and scan units.
    """
    scan_units = options.scan_units
    if not scan_units:
        scan_units = _scan_units_for_bundle(
            ctx,
            plan=plan,
            session_runtime=options.session_runtime,
        )
    scan_unit_pins = _delta_inputs_from_scan_units(scan_units)
    return _merge_delta_inputs(options.delta_inputs, scan_unit_pins)


def _explain_rows_from_text(text: str) -> tuple[dict[str, object], ...] | None:
    """Parse explain output text into row dictionaries when possible.

    Returns:
    -------
    list[dict[str, object]] | None
        Parsed row payloads or ``None`` when parsing fails.
    """
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    table_lines = [line for line in lines if line.startswith("|") and line.endswith("|")]
    if not table_lines:
        return None
    header: list[str] | None = None
    rows: list[dict[str, object]] = []
    for line in table_lines:
        parts = [part.strip() for part in line.strip("|").split("|")]
        if header is None and {"plan_type", "plan"}.issubset(set(parts)):
            header = parts
            continue
        if header is None:
            continue
        if len(parts) != len(header):
            continue
        rows.append(dict(zip(header, parts, strict=False)))
    return tuple(rows) if rows else None


def _plan_proto_bytes(plan: object | None, *, enabled: bool) -> bytes | None:
    """Serialize a plan to proto bytes when supported.

    Returns:
    -------
    bytes | None
        Serialized proto bytes when supported.
    """
    if plan is None:
        return None
    if not enabled or not _proto_serialization_enabled():
        return None
    method = getattr(plan, "to_proto", None)
    if not callable(method):
        return None
    try:
        payload = method()
    except (RuntimeError, TypeError, ValueError, AttributeError):
        # DataFusion may raise errors when proto codecs are unavailable.
        return None
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return bytes(payload)
    return None


def _proto_serialization_enabled() -> bool:
    """Return True when plan proto serialization should be attempted.

    Returns:
    -------
    bool
        True when plan proto serialization should be attempted.
    """
    try:
        import datafusion_ext
    except ImportError:
        return True
    return not bool(getattr(datafusion_ext, "IS_STUB", False))


def _proto_serialization_context(
    ctx: SessionContext,
    *,
    options: PlanBundleOptions,
) -> tuple[bool, PlanProtoStatus | None]:
    enabled = options.enable_proto_serialization
    if enabled is None:
        enabled = _proto_serialization_enabled()
    session_runtime = options.session_runtime
    if session_runtime is None:
        return enabled, None
    profile = session_runtime.profile
    if not profile.features.enable_delta_plan_codecs:
        return False, PlanProtoStatus(
            enabled=False,
            installed=None,
            reason="delta_plan_codecs_disabled",
        )
    installed = profile.delta_ops.ensure_delta_plan_codecs(ctx)
    if not installed:
        return False, PlanProtoStatus(
            enabled=False,
            installed=False,
            reason="delta_plan_codecs_unavailable",
        )
    if not enabled:
        return False, PlanProtoStatus(
            enabled=False,
            installed=True,
            reason="proto_serialization_disabled",
        )
    return True, PlanProtoStatus(enabled=True, installed=True, reason=None)


def _plan_proto_payload[T](
    plan: object | None,
    wrapper: Callable[[bytes], T],
    *,
    enabled: bool,
) -> T | None:
    payload = _plan_proto_bytes(plan, enabled=enabled)
    if payload is None:
        return None
    return wrapper(payload)


def _normalize_json_mapping(value: object | None) -> dict[str, JsonValue] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, msgspec.Struct):
        payload = to_builtins(value, str_keys=True)
        if isinstance(payload, Mapping):
            return {str(key): item for key, item in payload.items()}
    return None


def _udf_planner_snapshot(snapshot: Mapping[str, object]) -> Mapping[str, object] | None:
    """Return a normalized UDF planner metadata snapshot when available.

    Returns:
    -------
    Mapping[str, object] | None
        Normalized UDF planner metadata payload.
    """
    try:
        from datafusion_engine.udf.catalog import udf_planner_snapshot
    except ImportError:
        return {"status": "unavailable"}
    try:
        payload = udf_planner_snapshot(snapshot)
    except (TypeError, ValueError):
        return {"status": "unavailable"}
    if not isinstance(payload, Mapping):
        return {"status": "unavailable"}
    return dict(payload)


def _plan_artifacts_from_components(
    inputs: _PlanArtifactsInputs,
) -> PlanArtifacts:
    """Assemble PlanArtifacts from captured components.

    Returns:
    -------
    PlanArtifacts
        Serializable plan artifacts.
    """
    explain_artifacts = inputs.explain_artifacts
    explain_tree_rows = (
        _explain_rows_from_text(explain_artifacts.tree.text) if explain_artifacts.tree else None
    )
    explain_verbose_rows = (
        _explain_rows_from_text(explain_artifacts.verbose.text)
        if explain_artifacts.verbose is not None
        else None
    )
    plan_core = inputs.plan_core
    return PlanArtifacts(
        explain_tree_rows=explain_tree_rows,
        explain_verbose_rows=explain_verbose_rows,
        explain_analyze_duration_ms=(
            explain_artifacts.analyze.duration_ms if explain_artifacts.analyze is not None else None
        ),
        explain_analyze_output_rows=(
            explain_artifacts.analyze.output_rows if explain_artifacts.analyze is not None else None
        ),
        df_settings=dict(inputs.environment.df_settings),
        planning_env_snapshot=dict(inputs.environment.planning_env_snapshot),
        planning_env_hash=inputs.environment.planning_env_hash,
        rulepack_snapshot=(
            dict(inputs.environment.rulepack_snapshot)
            if inputs.environment.rulepack_snapshot is not None
            else None
        ),
        rulepack_hash=inputs.environment.rulepack_hash,
        information_schema_snapshot=dict(inputs.environment.information_schema_snapshot),
        information_schema_hash=inputs.environment.information_schema_hash,
        substrait_validation=(
            dict(inputs.substrait_validation) if inputs.substrait_validation is not None else None
        ),
        logical_plan_proto=_plan_proto_payload(
            plan_core.logical,
            LogicalPlanProtoBytes,
            enabled=inputs.proto_enabled,
        ),
        optimized_plan_proto=_plan_proto_payload(
            plan_core.optimized,
            OptimizedPlanProtoBytes,
            enabled=inputs.proto_enabled,
        ),
        execution_plan_proto=_plan_proto_payload(
            plan_core.execution,
            ExecutionPlanProtoBytes,
            enabled=inputs.proto_enabled,
        ),
        udf_snapshot_hash=inputs.udf_artifacts.snapshot_hash,
        function_registry_hash=inputs.registry_artifacts.registry_hash,
        rewrite_tags=tuple(inputs.udf_artifacts.rewrite_tags),
        domain_planner_names=tuple(inputs.udf_artifacts.domain_planner_names),
        udf_snapshot=dict(inputs.udf_artifacts.snapshot),
        udf_planner_snapshot=(
            dict(planner_snapshot)
            if (planner_snapshot := _udf_planner_snapshot(inputs.udf_artifacts.snapshot))
            is not None
            else None
        ),
    )


def _bundle_components(
    ctx: SessionContext,
    df: DataFrame,
    *,
    options: PlanBundleOptions,
) -> _BundleComponents:
    state = _collect_bundle_assembly_state(ctx, df, options=options)
    identity = _resolve_bundle_identity(state, options=options)
    runtime_profile = options.session_runtime.profile if options.session_runtime else None
    _record_plan_phase_telemetry(
        state.plan_core,
        plan_hash=state.fingerprint,
        plan_identity_hash=identity.plan_identity_hash,
        runtime_profile=runtime_profile,
    )
    detail_inputs = _build_plan_detail_inputs(state, identity=identity)
    return _finalize_bundle_components(
        df,
        state=state,
        identity=identity,
        detail_inputs=detail_inputs,
        options=options,
    )


def _collect_bundle_assembly_state(
    ctx: SessionContext,
    df: DataFrame,
    *,
    options: PlanBundleOptions,
) -> _BundleAssemblyState:
    plan_core = _plan_core_components(ctx, df, options=options)
    explain_artifacts = _capture_explain_artifacts(
        df,
        session_runtime=options.session_runtime,
    )
    proto_enabled, proto_status = _proto_serialization_context(ctx, options=options)
    udf_artifacts = _udf_artifacts(
        ctx,
        registry_snapshot=options.registry_snapshot,
        session_runtime=options.session_runtime,
    )
    registry_artifacts = _function_registry_artifacts(
        ctx,
        session_runtime=options.session_runtime,
    )
    environment = _environment_artifacts(ctx, session_runtime=options.session_runtime)
    required = _required_udf_artifacts(
        plan_core.optimized or plan_core.logical,
        snapshot=udf_artifacts.snapshot,
    )
    _validate_bundle_required_udfs(
        required=required, options=options, snapshot=udf_artifacts.snapshot
    )
    substrait_validation = _bundle_substrait_validation(
        ctx,
        df=df,
        options=options,
        substrait_bytes=plan_core.substrait_bytes,
    )
    merged_delta_inputs = _merged_delta_inputs_for_bundle(
        ctx,
        plan=plan_core.optimized or plan_core.logical,
        options=options,
    )
    snapshot_keys = _snapshot_keys_for_manifest(
        delta_inputs=merged_delta_inputs,
        session_runtime=options.session_runtime,
    )
    fingerprint = _hash_plan(
        PlanFingerprintInputs(
            substrait_bytes=plan_core.substrait_bytes,
            df_settings=environment.df_settings,
            planning_env_hash=environment.planning_env_hash,
            rulepack_hash=environment.rulepack_hash,
            udf_snapshot_hash=udf_artifacts.snapshot_hash,
            required_udfs=required.required_udfs,
            required_rewrite_tags=required.required_rewrite_tags,
            delta_inputs=merged_delta_inputs,
            delta_store_policy_hash=environment.delta_store_policy_hash,
            information_schema_hash=environment.information_schema_hash,
        )
    )
    artifacts = _plan_artifacts_from_components(
        _PlanArtifactsInputs(
            plan_core=plan_core,
            explain_artifacts=explain_artifacts,
            udf_artifacts=udf_artifacts,
            registry_artifacts=registry_artifacts,
            environment=environment,
            substrait_validation=substrait_validation,
            proto_enabled=proto_enabled,
        )
    )
    return _BundleAssemblyState(
        plan_core=plan_core,
        explain_artifacts=explain_artifacts,
        proto_enabled=proto_enabled,
        proto_status=proto_status,
        udf_artifacts=udf_artifacts,
        registry_artifacts=registry_artifacts,
        environment=environment,
        required=required,
        substrait_validation=substrait_validation,
        merged_delta_inputs=merged_delta_inputs,
        snapshot_keys=snapshot_keys,
        fingerprint=fingerprint,
        artifacts=artifacts,
    )


def _validate_bundle_required_udfs(
    *,
    required: _RequiredUdfArtifacts,
    options: PlanBundleOptions,
    snapshot: Mapping[str, object],
) -> None:
    if not options.validate_udfs or not required.required_udfs:
        return
    from datafusion_engine.udf.runtime import validate_required_udfs

    validate_required_udfs(snapshot, required=required.required_udfs)


def _bundle_substrait_validation(
    ctx: SessionContext,
    *,
    df: DataFrame,
    options: PlanBundleOptions,
    substrait_bytes: bytes,
) -> Mapping[str, object] | None:
    if (
        options.session_runtime is None
        or not options.session_runtime.profile.diagnostics.substrait_validation
    ):
        return None
    return _substrait_validation_payload(
        substrait_bytes,
        df=df,
        ctx=ctx,
    )


def _resolve_bundle_identity(
    state: _BundleAssemblyState,
    *,
    options: PlanBundleOptions,
) -> _BundleIdentityResult:
    if options.session_runtime is None:
        return _BundleIdentityResult(payload=None, plan_identity_hash=None)
    payload = _plan_identity_payload(
        _PlanIdentityInputs(
            plan_fingerprint=state.fingerprint,
            artifacts=state.artifacts,
            required_udfs=state.required.required_udfs,
            required_rewrite_tags=state.required.required_rewrite_tags,
            delta_inputs=state.merged_delta_inputs,
            scan_units=options.scan_units,
            profile=options.session_runtime.profile,
        )
    )
    return _BundleIdentityResult(
        payload=payload,
        plan_identity_hash=hash_json_default(payload, str_keys=True),
    )


def _build_plan_detail_inputs(
    state: _BundleAssemblyState,
    *,
    identity: _BundleIdentityResult,
) -> PlanDetailInputs:
    _ = identity
    return PlanDetailInputs(
        artifacts=state.artifacts,
        plan_fingerprint=state.fingerprint,
        logical=state.plan_core.logical,
        optimized=state.plan_core.optimized,
        execution=state.plan_core.execution,
        explain_tree=state.explain_artifacts.tree,
        explain_verbose=state.explain_artifacts.verbose,
        explain_analyze=state.explain_artifacts.analyze,
        substrait_validation=state.substrait_validation,
        proto_status=state.proto_status,
        detail_context=PlanDetailContext(
            cdf_windows=state.environment.cdf_windows,
            delta_store_policy_hash=state.environment.delta_store_policy_hash,
            information_schema_hash=state.environment.information_schema_hash,
            snapshot_keys=state.snapshot_keys,
        ),
    )


def _finalize_bundle_components(
    df: DataFrame,
    *,
    state: _BundleAssemblyState,
    identity: _BundleIdentityResult,
    detail_inputs: PlanDetailInputs,
    options: PlanBundleOptions,
) -> _BundleComponents:
    return _BundleComponents(
        logical=state.plan_core.logical,
        optimized=state.plan_core.optimized,
        execution=state.plan_core.execution,
        substrait_bytes=state.plan_core.substrait_bytes,
        fingerprint=state.fingerprint,
        artifacts=state.artifacts,
        merged_delta_inputs=state.merged_delta_inputs,
        scan_units=tuple(options.scan_units),
        plan_identity_hash=identity.plan_identity_hash,
        required_udfs=state.required.required_udfs,
        required_rewrite_tags=state.required.required_rewrite_tags,
        plan_details=_plan_details(df, detail_inputs=detail_inputs),
    )


@dataclass(frozen=True)
class _UdfArtifacts:
    """UDF snapshot and metadata for planning."""

    snapshot: Mapping[str, object]
    snapshot_hash: str
    rewrite_tags: tuple[str, ...]
    domain_planner_names: tuple[str, ...]


@dataclass(frozen=True)
class _RegistryArtifacts:
    """Function registry metadata hash."""

    registry_hash: str


@dataclass(frozen=True)
class _RequiredUdfArtifacts:
    """Required UDFs and rewrite tags for a plan."""

    required_udfs: tuple[str, ...]
    required_rewrite_tags: tuple[str, ...]


def _merge_delta_inputs(
    explicit: Sequence[DeltaInputPin],
    from_scan_units: Sequence[DeltaInputPin],
) -> tuple[DeltaInputPin, ...]:
    """Merge explicit and scan-unit derived Delta input pins.

    Explicit pins take precedence over scan-unit derived pins.

    Returns:
    -------
    tuple[DeltaInputPin, ...]
        Merged delta input pins sorted by dataset name.
    """
    pins: dict[str, DeltaInputPin] = {}
    for pin in from_scan_units:
        pins[pin.dataset_name] = pin
    for pin in explicit:
        pins[pin.dataset_name] = pin
    return tuple(pins[name] for name in sorted(pins))


def _delta_inputs_payload(
    delta_inputs: Sequence[DeltaInputPin],
) -> tuple[dict[str, object], ...]:
    payloads: list[dict[str, object]] = [
        {
            "dataset_name": pin.dataset_name,
            "version": pin.version,
            "timestamp": pin.timestamp,
            "protocol": (
                to_builtins(pin.protocol, str_keys=True) if pin.protocol is not None else None
            ),
            "delta_scan_config": (
                to_builtins(pin.delta_scan_config) if pin.delta_scan_config is not None else None
            ),
            "delta_scan_config_hash": pin.delta_scan_config_hash,
            "datafusion_provider": pin.datafusion_provider,
            "protocol_compatible": pin.protocol_compatible,
            "protocol_compatibility": (
                to_builtins(pin.protocol_compatibility)
                if pin.protocol_compatibility is not None
                else None
            ),
        }
        for pin in delta_inputs
    ]
    payloads.sort(key=lambda item: str(item["dataset_name"]))
    return tuple(payloads)


def _scan_units_payload(
    scan_units: Sequence[ScanUnit],
) -> tuple[dict[str, object], ...]:
    payloads: list[dict[str, object]] = [
        {
            "key": unit.key,
            "dataset_name": unit.dataset_name,
            "delta_version": unit.delta_version,
            "delta_timestamp": unit.delta_timestamp,
            "snapshot_timestamp": unit.snapshot_timestamp,
            "delta_protocol": (
                to_builtins(unit.delta_protocol, str_keys=True)
                if unit.delta_protocol is not None
                else None
            ),
            "delta_scan_config": (
                to_builtins(unit.delta_scan_config) if unit.delta_scan_config is not None else None
            ),
            "delta_scan_config_hash": unit.delta_scan_config_hash,
            "datafusion_provider": unit.datafusion_provider,
            "protocol_compatible": unit.protocol_compatible,
            "protocol_compatibility": (
                to_builtins(unit.protocol_compatibility)
                if unit.protocol_compatibility is not None
                else None
            ),
            "total_files": unit.total_files,
            "candidate_file_count": unit.candidate_file_count,
            "pruned_file_count": unit.pruned_file_count,
            "candidate_files": [str(path) for path in unit.candidate_files],
            "pushed_filters": list(unit.pushed_filters),
            "projected_columns": list(unit.projected_columns),
        }
        for unit in scan_units
    ]
    payloads.sort(key=lambda item: str(item["key"]))
    return tuple(payloads)


@dataclass(frozen=True)
class _PlanIdentityInputs:
    plan_fingerprint: str
    artifacts: PlanArtifacts
    required_udfs: tuple[str, ...]
    required_rewrite_tags: tuple[str, ...]
    delta_inputs: Sequence[DeltaInputPin]
    scan_units: Sequence[ScanUnit]
    profile: DataFusionRuntimeProfile


def _plan_identity_payload(inputs: _PlanIdentityInputs) -> Mapping[str, object]:
    df_settings_entries = tuple(
        sorted((str(key), str(value)) for key, value in inputs.artifacts.df_settings.items())
    )
    return {
        "version": 4,
        "plan_fingerprint": inputs.plan_fingerprint,
        "udf_snapshot_hash": inputs.artifacts.udf_snapshot_hash,
        "function_registry_hash": inputs.artifacts.function_registry_hash,
        "required_udfs": tuple(sorted(inputs.required_udfs)),
        "required_rewrite_tags": tuple(sorted(inputs.required_rewrite_tags)),
        "domain_planner_names": tuple(sorted(inputs.artifacts.domain_planner_names)),
        "df_settings_entries": df_settings_entries,
        "planning_env_hash": inputs.artifacts.planning_env_hash,
        "rulepack_hash": inputs.artifacts.rulepack_hash,
        "information_schema_hash": inputs.artifacts.information_schema_hash,
        "delta_inputs": tuple(_delta_inputs_payload(inputs.delta_inputs)),
        "scan_units": tuple(_scan_units_payload(inputs.scan_units)),
        "scan_keys": (),
        "profile_settings_hash": inputs.profile.settings_hash(),
        "profile_context_key": inputs.profile.context_cache_key(),
    }


def _safe_logical_plan(df: DataFrame) -> object | None:
    """Safely extract the logical plan from a DataFrame.

    Returns:
    -------
    object | None
        Logical plan, or None if unavailable.
    """
    method = getattr(df, "logical_plan", None)
    if not callable(method):
        return None
    try:
        return method()
    except (RuntimeError, TypeError, ValueError):
        return None


def _safe_optimized_logical_plan(df: DataFrame) -> object | None:
    """Safely extract the optimized logical plan from a DataFrame.

    Returns:
    -------
    object | None
        Optimized logical plan, or None if unavailable.
    """
    method = getattr(df, "optimized_logical_plan", None)
    if not callable(method):
        return None
    try:
        return method()
    except (RuntimeError, TypeError, ValueError):
        return None


def _safe_execution_plan(df: DataFrame) -> object | None:
    """Safely extract the execution plan from a DataFrame.

    Returns:
    -------
    object | None
        Execution plan, or None if unavailable.
    """
    method = getattr(df, "execution_plan", None)
    if not callable(method):
        return None
    try:
        return method()
    except Exception as exc:
        if str(exc).startswith("DataFusion error:"):
            return None
        raise


def _encode_substrait_bytes(plan_obj: object) -> bytes:
    encode = getattr(plan_obj, "encode", None)
    if not callable(encode):
        msg = "Substrait plan missing encode method."
        raise TypeError(msg)
    try:
        encoded = encode()
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        msg = f"Failed to encode Substrait plan bytes: {exc}"
        raise ValueError(msg) from exc
    if not isinstance(encoded, (bytes, bytearray)):
        msg = f"Substrait encode returned {type(encoded).__name__}, expected bytes."
        raise TypeError(msg)
    return bytes(encoded)


def _internal_substrait_bytes(
    ctx: SessionContext,
    normalized: object,
) -> bytes | None:
    if _SUBSTRAIT_INTERNAL is None:
        return None
    internal_producer = getattr(_SUBSTRAIT_INTERNAL, "Producer", None)
    to_substrait = getattr(internal_producer, "to_substrait_plan", None)
    if not callable(to_substrait):
        return None
    raw_plan = getattr(normalized, "_raw_plan", normalized)
    try:
        substrait_plan = to_substrait(raw_plan, ctx.ctx)
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        msg = f"Failed to encode Substrait plan bytes: {exc}"
        raise ValueError(msg) from exc
    return _encode_substrait_bytes(substrait_plan)


def _public_substrait_bytes(
    ctx: SessionContext,
    normalized: object,
) -> bytes:
    to_substrait = getattr(SubstraitProducer, "to_substrait_plan", None)
    if not callable(to_substrait):
        msg = "Substrait producer missing to_substrait_plan."
        raise TypeError(msg)
    try:
        substrait_plan = to_substrait(normalized, ctx)
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        msg = f"Failed to encode Substrait plan bytes: {exc}"
        raise ValueError(msg) from exc
    return _encode_substrait_bytes(substrait_plan)


def _to_substrait_bytes(ctx: SessionContext, optimized: object | None) -> bytes:
    """Convert an optimized plan to Substrait bytes.

    Args:
        ctx: DataFusion session context.
        optimized: Optimized logical plan object.

    Returns:
        bytes: Result.

    Raises:
        ValueError: If substrait generation prerequisites are not met.
    """
    if SubstraitProducer is None:
        msg = "Substrait producer is unavailable."
        raise ValueError(msg)
    if optimized is None:
        msg = "Substrait serialization requires an optimized logical plan."
        raise ValueError(msg)
    normalized = normalize_substrait_plan(ctx, cast("DataFusionLogicalPlan", optimized))
    internal_bytes = _internal_substrait_bytes(ctx, normalized)
    if internal_bytes is not None:
        return internal_bytes
    return _public_substrait_bytes(ctx, normalized)


def _capture_explain_analyze(
    df: DataFrame,
    *,
    session_runtime: SessionRuntime | None,
) -> ExplainCapture | None:
    """Capture EXPLAIN ANALYZE output when enabled.

    Parameters
    ----------
    df
        DataFusion DataFrame to profile.
    session_runtime
        Session runtime controlling explain settings.

    Returns:
    -------
    ExplainCapture | None
        Captured explain output, or ``None`` when disabled.
    """
    if session_runtime is None:
        return None
    if not session_runtime.profile.diagnostics.explain_analyze:
        return None
    return capture_explain(df, verbose=True, analyze=True)


def _substrait_validation_payload(
    substrait_bytes: bytes,
    *,
    df: DataFrame,
    ctx: SessionContext,
) -> Mapping[str, object] | None:
    """Validate Substrait bytes and return the validation payload.

    Args:
        substrait_bytes: Encoded substrait bytes.
        df: Source DataFrame used to compare replay output.
        ctx: DataFusion session context.

    Returns:
        Mapping[str, object] | None: Result.

    Raises:
        ValueError: If substrait validation reports mismatch.
    """
    from datafusion_engine.plan.execution import validate_substrait_plan

    validation = validate_substrait_plan(substrait_bytes, df=df, ctx=ctx)
    if validation is None:
        return None
    match = validation.get("match")
    if match is False:
        msg = f"Substrait validation failed: {validation}"
        raise ValueError(msg)
    return validation


def _information_schema_sql_options(
    session_runtime: SessionRuntime,
) -> SQLOptions | None:
    try:
        from datafusion_engine.sql.options import planning_sql_options

        return planning_sql_options(session_runtime.profile)
    except (RuntimeError, TypeError, ValueError, ImportError):
        return None


def _build_introspector(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None,
) -> SchemaIntrospector | None:
    try:
        return SchemaIntrospector(ctx, sql_options=sql_options)
    except (RuntimeError, TypeError, ValueError):
        return None


def _table_definitions_snapshot(
    introspector: SchemaIntrospector,
    *,
    tables: Sequence[Mapping[str, object]],
) -> dict[str, str]:
    table_definitions: dict[str, str] = {}
    for row in tables:
        name = row.get("table_name")
        if name is None:
            continue
        definition = introspector.table_definition(str(name))
        if definition:
            table_definitions[str(name)] = definition
    return table_definitions


def _safe_introspection_rows(
    fetch: Callable[[], Sequence[Mapping[str, object]]],
) -> list[Mapping[str, object]]:
    try:
        return list(fetch())
    except (RuntimeError, TypeError, ValueError, Warning):
        return []


def _routine_metadata_snapshot(
    introspector: SchemaIntrospector,
    *,
    capture_udf_metadata: bool,
) -> tuple[list[Mapping[str, object]], list[Mapping[str, object]], list[Mapping[str, object]]]:
    if not capture_udf_metadata:
        return [], [], []
    routines = _safe_introspection_rows(introspector.routines_snapshot)
    parameters = _safe_introspection_rows(introspector.parameters_snapshot)
    function_catalog = _safe_introspection_rows(
        lambda: introspector.function_catalog_snapshot(include_parameters=True)
    )
    return routines, parameters, function_catalog


def _information_schema_snapshot(
    ctx: SessionContext,
    *,
    session_runtime: SessionRuntime | None,
) -> Mapping[str, object]:
    """Return a full information_schema snapshot for plan artifacts.

    Parameters
    ----------
    ctx
        DataFusion session context used for introspection.
    session_runtime
        Optional session runtime for policy-aware introspection.

    Returns:
    -------
    Mapping[str, object]
        Snapshot payload containing settings, tables, columns, and routines.
    """
    if session_runtime is None:
        return {}
    sql_options = _information_schema_sql_options(session_runtime)
    introspector = _build_introspector(ctx, sql_options=sql_options)
    if introspector is None:
        return {}
    tables = introspector.tables_snapshot()
    table_definitions = _table_definitions_snapshot(introspector, tables=tables)
    capture_udf_metadata = _capture_udf_metadata_for_plan(session_runtime)
    routines, parameters, function_catalog = _routine_metadata_snapshot(
        introspector,
        capture_udf_metadata=capture_udf_metadata,
    )
    return {
        "df_settings": _df_settings_snapshot(ctx, session_runtime=session_runtime),
        "settings": introspector.settings_snapshot(),
        "tables": tables,
        "schemata": introspector.schemata_snapshot(),
        "columns": introspector.columns_snapshot(),
        "routines": routines,
        "parameters": parameters,
        "function_catalog": function_catalog,
        "table_definitions": table_definitions,
    }


def _information_schema_hash(snapshot: Mapping[str, object]) -> str:
    """Return a stable hash for an information_schema snapshot.

    Parameters
    ----------
    snapshot
        Information schema snapshot payload.

    Returns:
    -------
    str
        SHA-256 hash of the snapshot payload.
    """
    canonical = _canonicalize_snapshot(snapshot)
    return hash_msgpack_canonical(canonical)


def _canonical_sort_key(value: object) -> str:
    return hash_msgpack_canonical(value)


def _canonicalize_snapshot(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _canonicalize_snapshot(val) for key, val in sorted(value.items())}
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray, memoryview),
    ):
        normalized = [_canonicalize_snapshot(item) for item in value]
        return sorted(normalized, key=_canonical_sort_key)
    return value


@dataclass(frozen=True)
class PlanFingerprintInputs:
    """Inputs required to fingerprint a plan bundle."""

    substrait_bytes: bytes
    df_settings: Mapping[str, str]
    planning_env_hash: str | None
    rulepack_hash: str | None
    information_schema_hash: str | None
    udf_snapshot_hash: str
    required_udfs: Sequence[str]
    required_rewrite_tags: Sequence[str]
    delta_inputs: Sequence[DeltaInputPin]
    delta_store_policy_hash: str | None


def _hash_plan(inputs: PlanFingerprintInputs) -> str:
    """Compute a stable fingerprint for the plan bundle.

    Substrait bytes are required for reproducibility. The fingerprint also
    incorporates session settings, UDF requirements, and Delta input pins.

    Returns:
    -------
    str
        Stable plan fingerprint.
    """
    settings_items = tuple(sorted(inputs.df_settings.items()))
    settings_hash = hash_msgpack_canonical(settings_items)
    planning_env_hash = inputs.planning_env_hash or ""
    rulepack_hash = inputs.rulepack_hash or ""
    substrait_hash = hash_sha256_hex(inputs.substrait_bytes)
    delta_payload = tuple(
        sorted(
            (
                (
                    pin.dataset_name,
                    pin.version,
                    pin.timestamp,
                    _delta_protocol_payload(pin.protocol),
                    pin.delta_scan_config_hash,
                    pin.datafusion_provider,
                    pin.protocol_compatible,
                )
                for pin in inputs.delta_inputs
            ),
            key=lambda item: item[0],
        )
    )
    payload = (
        ("substrait_hash", substrait_hash),
        ("settings_hash", settings_hash),
        ("planning_env_hash", planning_env_hash),
        ("rulepack_hash", rulepack_hash),
        ("information_schema_hash", inputs.information_schema_hash or ""),
        ("udf_snapshot_hash", inputs.udf_snapshot_hash),
        ("required_udfs", tuple(sorted(inputs.required_udfs))),
        ("required_rewrite_tags", tuple(sorted(inputs.required_rewrite_tags))),
        ("delta_inputs", delta_payload),
        ("delta_store_policy_hash", inputs.delta_store_policy_hash),
    )
    return hash_msgpack_canonical(payload)


def _delta_protocol_payload(
    protocol: object | None,
) -> tuple[tuple[str, object], ...] | None:
    if protocol is None:
        return None
    resolved: Mapping[str, object] | None
    if isinstance(protocol, Mapping):
        resolved = protocol
    elif isinstance(protocol, msgspec.Struct):
        payload = to_builtins(protocol, str_keys=True)
        resolved = payload if isinstance(payload, Mapping) else None
    else:
        resolved = None
    if resolved is None:
        return None
    items: list[tuple[str, object]] = []
    for key, value in resolved.items():
        if isinstance(value, (str, int, float)) or value is None:
            items.append((str(key), value))
            continue
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            items.append((str(key), tuple(str(item) for item in value)))
            continue
        items.append((str(key), str(value)))
    return tuple(sorted(items, key=lambda item: item[0]))


def _scan_units_for_bundle(
    ctx: SessionContext,
    *,
    plan: object,
    session_runtime: SessionRuntime | None,
) -> tuple[ScanUnit, ...]:
    scan_units: tuple[ScanUnit, ...] = ()
    if session_runtime is not None and plan is not None:
        try:
            from datafusion_engine.lineage.datafusion import extract_lineage
            from datafusion_engine.lineage.scan import plan_scan_units
        except ImportError:
            pass
        else:
            lineage = extract_lineage(plan)
            if lineage.scans:
                locations = _dataset_location_map(session_runtime)
                if locations:
                    try:
                        scan_units, _ = plan_scan_units(
                            ctx,
                            dataset_locations=locations,
                            scans_by_task={"plan_bundle": lineage.scans},
                            runtime_profile=session_runtime.profile,
                        )
                    except (RuntimeError, TypeError, ValueError):
                        scan_units = ()
    return scan_units


def _dataset_location_map(session_runtime: SessionRuntime | object) -> dict[str, DatasetLocation]:
    locations: dict[str, DatasetLocation] = {}
    runtime_profile = getattr(session_runtime, "profile", None)
    if runtime_profile is None:
        return locations
    for name, location in extract_output_locations_for_profile(runtime_profile).items():
        locations.setdefault(
            name,
            apply_delta_store_policy(
                location,
                policy=runtime_profile.policies.delta_store_policy,
            ),
        )
    for (
        name,
        location,
    ) in runtime_profile.data_sources.extract_output.scip_dataset_locations.items():
        locations.setdefault(
            name,
            apply_delta_store_policy(
                location,
                policy=runtime_profile.policies.delta_store_policy,
            ),
        )
    for name, location in normalize_dataset_locations_for_profile(runtime_profile).items():
        locations.setdefault(
            name,
            apply_delta_store_policy(
                location,
                policy=runtime_profile.policies.delta_store_policy,
            ),
        )
    for name, location in semantic_output_locations_for_profile(runtime_profile).items():
        locations.setdefault(
            name,
            apply_delta_store_policy(
                location,
                policy=runtime_profile.policies.delta_store_policy,
            ),
        )
    for catalog in runtime_profile.catalog.registry_catalogs.values():
        for name in catalog.names():
            if name in locations:
                continue
            try:
                locations[name] = apply_delta_store_policy(
                    cast("DatasetLocation", catalog.get(name)),
                    policy=runtime_profile.policies.delta_store_policy,
                )
            except KeyError:
                continue
    return locations


def _cdf_window_snapshot(
    session_runtime: SessionRuntime | None,
) -> tuple[dict[str, object], ...]:
    if session_runtime is None:
        return ()
    locations = _dataset_location_map(session_runtime)
    payloads: list[dict[str, object]] = []
    for name, location in sorted(locations.items(), key=lambda item: item[0]):
        options = location.delta_cdf_options
        if options is None:
            continue
        payloads.append(
            {
                "dataset_name": name,
                "table_uri": str(location.path),
                "starting_version": options.starting_version,
                "ending_version": options.ending_version,
                "starting_timestamp": options.starting_timestamp,
                "ending_timestamp": options.ending_timestamp,
                "allow_out_of_range": options.allow_out_of_range,
            }
        )
    return tuple(payloads)


def _canonical_table_uri_for_manifest(table_uri: str) -> str:
    raw = str(table_uri).strip()
    parsed = urlparse(raw)
    if not parsed.scheme:
        return str(Path(raw).expanduser().resolve())
    scheme = parsed.scheme.lower()
    if scheme in {"s3a", "s3n"}:
        scheme = "s3"
    netloc = parsed.netloc
    if scheme in {"s3", "gs", "az", "abfs", "abfss", "http", "https"}:
        netloc = netloc.lower()
    path = parsed.path or ""
    if netloc and path and not path.startswith("/"):
        path = f"/{path}"
    return parsed._replace(scheme=scheme, netloc=netloc, path=path).geturl()


def _snapshot_keys_for_manifest(
    *,
    delta_inputs: Sequence[DeltaInputPin],
    session_runtime: SessionRuntime | None,
) -> tuple[dict[str, object], ...]:
    if session_runtime is None:
        return ()
    locations = _dataset_location_map(session_runtime)
    payloads: list[dict[str, object]] = []
    seen: set[tuple[str, str, int]] = set()
    for pin in delta_inputs:
        if pin.version is None:
            continue
        location = locations.get(pin.dataset_name)
        if location is None:
            continue
        canonical_uri = _canonical_table_uri_for_manifest(str(location.path))
        resolved_version = int(pin.version)
        key = (pin.dataset_name, canonical_uri, resolved_version)
        if key in seen:
            continue
        seen.add(key)
        payloads.append(
            {
                "dataset_name": pin.dataset_name,
                "canonical_uri": canonical_uri,
                "resolved_version": resolved_version,
            }
        )
    payloads.sort(
        key=lambda row: (
            str(row["dataset_name"]),
            str(row["canonical_uri"]),
            _manifest_resolved_version(row),
        )
    )
    return tuple(payloads)


def _manifest_resolved_version(row: Mapping[str, object]) -> int:
    value = row.get("resolved_version")
    if isinstance(value, int):
        return value
    return 0


def _plan_display(plan: object | None, *, method: str) -> str | None:
    """Extract a display string from a plan object.

    Returns:
    -------
    str | None
        Display string for the plan, if available.
    """
    if plan is None:
        return None
    if isinstance(plan, str):
        return plan
    display_method = getattr(plan, method, None)
    if callable(display_method):
        try:
            return str(display_method())
        except (RuntimeError, TypeError, ValueError):
            return None
    return str(plan)


def _plan_pgjson(plan: object | None) -> str | None:
    """Extract PostgreSQL JSON representation from a plan when available.

    Returns:
    -------
    str | None
        PG-JSON representation when available.
    """
    if plan is None:
        return None
    method = getattr(plan, "display_pgjson", None)
    if not callable(method):
        method = getattr(plan, "display_pg_json", None)
        if not callable(method):
            return None
    try:
        return str(method())
    except (RuntimeError, TypeError, ValueError):
        return None


def _plan_display_section(detail_inputs: PlanDetailInputs) -> _PlanDisplaySection:
    physical_plan = _plan_display(
        detail_inputs.execution,
        method="display_indent",
    )
    return _PlanDisplaySection(
        payload={
            "logical_plan": _plan_display(
                detail_inputs.logical,
                method="display_indent_schema",
            ),
            "optimized_plan": _plan_display(
                detail_inputs.optimized,
                method="display_indent_schema",
            ),
            "physical_plan": physical_plan,
            "graphviz": _plan_graphviz(detail_inputs.optimized),
            "optimized_plan_pgjson": _plan_pgjson(detail_inputs.optimized),
        },
        physical_plan=physical_plan,
    )


def _plan_statistics_section(
    *,
    execution: object | None,
    physical_plan: str | None,
) -> dict[str, object]:
    section: dict[str, object] = {
        "partition_count": _plan_partition_count(execution),
        "repartition_count": _repartition_count_from_display(physical_plan),
        "dynamic_filter_count": _dynamic_filter_count_from_display(physical_plan),
    }
    stats_payload = _plan_statistics_payload(execution)
    if stats_payload is not None:
        section["statistics"] = stats_payload
    return section


def _plan_explain_section(detail_inputs: PlanDetailInputs) -> dict[str, object]:
    section: dict[str, object] = {}
    if detail_inputs.explain_tree is not None:
        section["explain_tree"] = detail_inputs.explain_tree.text
    if detail_inputs.explain_verbose is not None:
        section["explain_verbose"] = detail_inputs.explain_verbose.text
    if detail_inputs.explain_analyze is not None:
        section["explain_analyze"] = detail_inputs.explain_analyze.text
        section["explain_analyze_duration_ms"] = detail_inputs.explain_analyze.duration_ms
        section["explain_analyze_output_rows"] = detail_inputs.explain_analyze.output_rows
    if detail_inputs.substrait_validation is not None:
        section["substrait_validation"] = detail_inputs.substrait_validation
    if detail_inputs.proto_status is not None:
        section["proto_serialization"] = to_builtins(
            detail_inputs.proto_status,
            str_keys=True,
        )
    return section


def _plan_schema_section(df: DataFrame) -> dict[str, object]:
    schema_names: list[str] = list(df.schema().names) if hasattr(df.schema(), "names") else []
    return {
        "schema_names": schema_names,
        "schema_describe": _schema_describe_rows(df),
        "schema_provenance": _schema_provenance(df),
    }


def _plan_context_section(context: PlanDetailContext) -> dict[str, object]:
    section: dict[str, object] = {}
    if context.cdf_windows:
        section["cdf_windows"] = [dict(window) for window in context.cdf_windows]
    if context.delta_store_policy_hash is not None:
        section["delta_store_policy_hash"] = context.delta_store_policy_hash
    if context.information_schema_hash is not None:
        section["information_schema_hash"] = context.information_schema_hash
    if context.snapshot_keys:
        section["snapshot_keys"] = [dict(key) for key in context.snapshot_keys]
    if context.write_outcomes:
        section["write_outcomes"] = [dict(outcome) for outcome in context.write_outcomes]
    return section


def _plan_details(
    df: DataFrame,
    *,
    detail_inputs: PlanDetailInputs,
) -> dict[str, object]:
    """Collect plan details for diagnostics.

    Returns:
    -------
    dict[str, object]
        Diagnostic plan metadata.
    """
    context = detail_inputs.detail_context or PlanDetailContext()
    display_section = _plan_display_section(detail_inputs)
    builder = _PlanDetailsBuilder()
    builder.add_section(display_section.payload)
    builder.add_section(
        _plan_statistics_section(
            execution=detail_inputs.execution,
            physical_plan=display_section.physical_plan,
        )
    )
    builder.add_section(_plan_explain_section(detail_inputs))
    builder.add_section(_plan_schema_section(df))
    builder.add_section(_plan_context_section(context))
    details = builder.build()
    details["plan_manifest"] = _plan_manifest_payload(
        detail_inputs=detail_inputs,
        details=details,
        context=context,
    )
    details["determinism_audit"] = _determinism_audit_bundle(
        detail_inputs,
        context=context,
    )
    return details


def _plan_manifest_payload(
    *,
    detail_inputs: PlanDetailInputs,
    details: Mapping[str, object],
    context: PlanDetailContext,
) -> dict[str, object]:
    settings = {
        str(key): str(value) for key, value in sorted(detail_inputs.artifacts.df_settings.items())
    }
    return {
        "version": 1,
        "plan_fingerprint": detail_inputs.plan_fingerprint,
        "logical_plan_text": details.get("logical_plan"),
        "optimized_plan_text": details.get("optimized_plan"),
        "physical_plan_text": details.get("physical_plan"),
        "optimized_plan_pgjson": details.get("optimized_plan_pgjson"),
        "logical_plan_proto_present": detail_inputs.artifacts.logical_plan_proto is not None,
        "optimized_plan_proto_present": detail_inputs.artifacts.optimized_plan_proto is not None,
        "execution_plan_proto_present": detail_inputs.artifacts.execution_plan_proto is not None,
        "planning_env_hash": detail_inputs.artifacts.planning_env_hash,
        "information_schema_hash": detail_inputs.artifacts.information_schema_hash,
        "df_settings": settings,
        "df_settings_hash": hash_settings(detail_inputs.artifacts.df_settings),
        "snapshot_keys": [dict(key) for key in context.snapshot_keys],
        "write_outcomes": [dict(outcome) for outcome in context.write_outcomes],
    }


def _plan_graphviz(plan: object | None) -> str | None:
    """Extract GraphViz representation from a plan.

    Returns:
    -------
    str | None
        GraphViz DOT string, if available.
    """
    if plan is None:
        return None
    method = getattr(plan, "display_graphviz", None)
    if not callable(method):
        return None
    try:
        return str(method())
    except (RuntimeError, TypeError, ValueError):
        return None


def _plan_partition_count(plan: object | None) -> int | None:
    """Extract partition count from an execution plan.

    Returns:
    -------
    int | None
        Partition count, if available.
    """
    if plan is None:
        return None
    count = getattr(plan, "partition_count", None)
    if count is None:
        return None
    if isinstance(count, bool):
        return None
    if isinstance(count, (int, float)):
        return int(count)
    try:
        return int(count)
    except (TypeError, ValueError):
        return None


def _repartition_count_from_display(plan_display: str | None) -> int | None:
    """Count repartition operators from a physical plan display string.

    Parameters
    ----------
    plan_display
        Physical plan display string produced by DataFusion.

    Returns:
    -------
    int | None
        Count of repartition operators, or ``None`` if unavailable.
    """
    if plan_display is None:
        return None
    token = "RepartitionExec"
    count = plan_display.count(token)
    return count if count > 0 else None


def _dynamic_filter_count_from_display(plan_display: str | None) -> int | None:
    """Count dynamic filter operators from a physical plan display string.

    Returns:
    -------
    int | None
        Count of dynamic filter operators, or ``None`` if unavailable.
    """
    if plan_display is None:
        return None
    token = "DynamicFilter"
    count = plan_display.count(token)
    return count if count > 0 else None


def _plan_statistics_payload(plan: object | None) -> Mapping[str, object] | None:
    """Return a summary of execution plan statistics when available.

    Returns:
    -------
    Mapping[str, object] | None
        Normalized statistics payload, or ``None`` if unavailable.
    """
    if plan is None:
        return None
    stats_method = getattr(plan, "statistics", None)
    if callable(stats_method):
        with _suppress_errors():
            stats = stats_method()
        normalized = _normalize_statistics_payload(stats)
        if normalized is None:
            return None
        column_stats = normalized.get("column_statistics")
        column_present = (
            isinstance(column_stats, Sequence)
            and not isinstance(column_stats, (str, bytes, bytearray))
            and len(column_stats) > 0
        )
        return {
            "source": "execution_plan.statistics",
            "column_statistics_present": column_present,
            **normalized,
        }
    return None


def _normalize_statistics_payload(stats: object) -> Mapping[str, object] | None:
    """Normalize a statistics object into a JSON-ready payload.

    Returns:
    -------
    Mapping[str, object] | None
        Normalized statistics payload, or ``None`` when empty.
    """
    if stats is None:
        return None
    if isinstance(stats, Mapping):
        return dict(stats)
    payload: dict[str, object] = {}
    for key in ("num_rows", "row_count", "total_byte_size", "total_bytes", "column_statistics"):
        value = getattr(stats, key, None)
        if value is None:
            continue
        payload[key] = _statistics_value(value)
    return payload or None


def _statistics_value(value: object) -> object:
    """Normalize a statistics value for JSON serialization.

    Returns:
    -------
    object
        JSON-ready statistics value.
    """
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    inner = getattr(value, "value", None)
    if isinstance(inner, (int, float, str, bool)) or inner is None:
        return inner
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_statistics_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(k): _statistics_value(v) for k, v in value.items()}
    return str(value)


def _arrow_schema_from_df(df: DataFrame) -> pa.Schema | None:
    schema: pa.Schema | object = df.schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        with _suppress_errors():
            resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    return None


def _schema_metadata_payload(schema: pa.Schema) -> dict[str, str]:
    metadata = schema.metadata or {}
    items = sorted(metadata.items(), key=lambda item: str(item[0]))
    return {
        (key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)): (
            value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
        )
        for key, value in items
    }


def _schema_provenance(df: DataFrame) -> Mapping[str, object]:
    schema = _arrow_schema_from_df(df)
    if schema is None:
        return {}

    try:
        from datafusion_engine.schema.contracts import SCHEMA_ABI_FINGERPRINT_META

        abi_meta = SCHEMA_ABI_FINGERPRINT_META
    except ImportError:
        abi_meta = b"schema_abi_fingerprint"

    metadata_payload = _schema_metadata_payload(schema)
    abi_key = abi_meta.decode("utf-8") if isinstance(abi_meta, bytes) else str(abi_meta)
    abi_value = metadata_payload.get(abi_key)
    return {
        "source": "arrow_schema",
        "schema_identity_hash": schema_identity_hash(schema),
        "schema_metadata": metadata_payload,
        "explicit_schema": abi_value is not None,
        "schema_abi_fingerprint": abi_value,
    }


def _schema_describe_rows(df: DataFrame) -> list[dict[str, object]]:
    resolved_schema = _arrow_schema_from_df(df)
    if resolved_schema is None:
        return []
    return [
        {
            "column_name": field.name,
            "data_type": str(field.type),
            "nullable": field.nullable,
            "source": "arrow_schema",
        }
        for field in resolved_schema
    ]


def _determinism_audit_bundle(
    detail_inputs: PlanDetailInputs,
    *,
    context: PlanDetailContext,
) -> Mapping[str, object]:
    artifacts = detail_inputs.artifacts
    return {
        "plan_fingerprint": detail_inputs.plan_fingerprint,
        "planning_env_hash": artifacts.planning_env_hash,
        "rulepack_hash": artifacts.rulepack_hash,
        "information_schema_hash": context.information_schema_hash,
        "df_settings_hash": hash_settings(artifacts.df_settings),
        "udf_snapshot_hash": artifacts.udf_snapshot_hash,
        "function_registry_hash": artifacts.function_registry_hash,
        "schema_contract_hash": schema_contract_hash(),
    }


def _settings_rows_to_mapping(rows: Sequence[Mapping[str, object]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for row in rows:
        name = row.get("name") or row.get("setting_name") or row.get("key")
        if name is None:
            continue
        value = row.get("value")
        mapping[str(name)] = "" if value is None else str(value)
    return mapping


def _df_settings_snapshot(
    ctx: SessionContext,
    *,
    session_runtime: SessionRuntime | None,
) -> Mapping[str, str]:
    if session_runtime is not None and session_runtime.ctx is ctx:
        return dict(session_runtime.df_settings)
    from datafusion_engine.schema.introspection import SchemaIntrospector

    try:
        sql_options = None
        if session_runtime is not None:
            try:
                from datafusion_engine.sql.options import planning_sql_options

                sql_options = planning_sql_options(session_runtime.profile)
            except (RuntimeError, TypeError, ValueError, ImportError):
                sql_options = None
        introspector = SchemaIntrospector(ctx, sql_options=sql_options)
        rows = introspector.settings_snapshot()
        if not rows:
            return {}
        return _settings_rows_to_mapping(rows)
    except (RuntimeError, TypeError, ValueError):
        return {}


def _function_registry_hash(snapshot: Mapping[str, object]) -> str:
    canonical = _canonicalize_snapshot(snapshot)
    return hash_msgpack_canonical(canonical)


def _capture_udf_metadata_for_plan(session_runtime: SessionRuntime | None) -> bool:
    if session_runtime is None:
        return False
    profile = session_runtime.profile
    return profile.catalog.enable_information_schema and profile.features.enable_udfs


def _function_registry_artifacts(
    ctx: SessionContext,
    *,
    session_runtime: SessionRuntime | None,
) -> _RegistryArtifacts:
    from datafusion_engine.schema.introspection import SchemaIntrospector

    if not _capture_udf_metadata_for_plan(session_runtime):
        return _RegistryArtifacts(
            registry_hash=_function_registry_hash({"functions": []}),
        )

    functions: Sequence[Mapping[str, object]] = ()
    try:
        sql_options = None
        if session_runtime is not None:
            try:
                from datafusion_engine.sql.options import planning_sql_options

                sql_options = planning_sql_options(session_runtime.profile)
            except (RuntimeError, TypeError, ValueError, ImportError):
                sql_options = None
        introspector = SchemaIntrospector(ctx, sql_options=sql_options)
        functions = introspector.function_catalog_snapshot(include_parameters=True)
    except (RuntimeError, TypeError, ValueError, Warning):
        functions = ()
    snapshot: Mapping[str, object] = {"functions": list(functions)}
    return _RegistryArtifacts(
        registry_hash=_function_registry_hash(snapshot),
    )


def _udf_artifacts(
    ctx: SessionContext,
    *,
    registry_snapshot: Mapping[str, object] | None,
    session_runtime: SessionRuntime | None,
) -> _UdfArtifacts:
    if registry_snapshot is not None:
        snapshot = registry_snapshot
    elif session_runtime is not None:
        return _UdfArtifacts(
            snapshot=session_runtime.udf_snapshot,
            snapshot_hash=session_runtime.udf_snapshot_hash,
            rewrite_tags=session_runtime.udf_rewrite_tags,
            domain_planner_names=session_runtime.domain_planner_names,
        )
    else:
        from datafusion_engine.udf.runtime import rust_udf_snapshot

        snapshot = rust_udf_snapshot(ctx)
    from datafusion_engine.expr.domain_planner import domain_planner_names_from_snapshot
    from datafusion_engine.udf.catalog import rewrite_tag_index
    from datafusion_engine.udf.runtime import rust_udf_snapshot_hash, validate_rust_udf_snapshot

    validate_rust_udf_snapshot(snapshot)
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    tag_index = rewrite_tag_index(snapshot)
    rewrite_tags = tuple(sorted(tag_index))
    planner_names = domain_planner_names_from_snapshot(snapshot)
    return _UdfArtifacts(
        snapshot=snapshot,
        snapshot_hash=snapshot_hash,
        rewrite_tags=rewrite_tags,
        domain_planner_names=planner_names,
    )


def _required_udf_artifacts(
    plan: object | None,
    *,
    snapshot: Mapping[str, object],
) -> _RequiredUdfArtifacts:
    if plan is None:
        return _RequiredUdfArtifacts(required_udfs=(), required_rewrite_tags=())
    from datafusion_engine.lineage.datafusion import extract_lineage

    lineage = extract_lineage(plan, udf_snapshot=snapshot)
    return _RequiredUdfArtifacts(
        required_udfs=lineage.required_udfs,
        required_rewrite_tags=lineage.required_rewrite_tags,
    )


__all__ = [
    "DataFrameBuilder",
    "DataFusionPlanBundle",
    "DeltaInputPin",
    "PlanArtifacts",
    "build_plan_bundle",
]
