"""Canonical DataFusion plan bundle for all planning and scheduling.

This module provides the single canonical plan artifact that all execution
and scheduling paths use.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import msgspec
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from core_types import JsonValue
from datafusion_engine.delta.protocol import DeltaProtocolSnapshot
from datafusion_engine.delta.store_policy import delta_store_policy_hash
from datafusion_engine.plan.bundle_environment import (
    df_settings_snapshot as _df_settings_snapshot,
)
from datafusion_engine.plan.bundle_environment import (
    information_schema_hash as _information_schema_hash,
)
from datafusion_engine.plan.bundle_environment import (
    information_schema_snapshot as _information_schema_snapshot,
)
from datafusion_engine.plan.bundle_scan_inputs import (
    cdf_window_snapshot as _cdf_window_snapshot,
)
from datafusion_engine.plan.bundle_scan_inputs import (
    scan_units_for_bundle as _scan_units_for_bundle,
)
from datafusion_engine.plan.bundle_scan_inputs import (
    snapshot_keys_for_manifest as _snapshot_keys_for_manifest,
)
from datafusion_engine.plan.cache import PlanProtoCacheEntry
from datafusion_engine.plan.diagnostics import PlanPhaseDiagnostics, record_plan_phase_diagnostics
from datafusion_engine.plan.plan_diagnostics import (
    PlanDetailContext,
    PlanDetailInputs,
    collect_plan_details,
)
from datafusion_engine.plan.plan_fingerprint import (
    PlanFingerprintInputs,
    compute_plan_fingerprint,
)
from datafusion_engine.plan.plan_identity import PlanIdentityInputs, plan_identity_payload
from datafusion_engine.plan.plan_proto import plan_proto_payload, proto_serialization_enabled
from datafusion_engine.plan.plan_utils import (
    explain_rows_from_text,
    plan_display,
    safe_execution_plan,
    safe_logical_plan,
    safe_optimized_logical_plan,
)
from datafusion_engine.plan.planning_env import (
    function_registry_artifacts,
    planning_env_hash,
    planning_env_snapshot,
    rulepack_hash,
    rulepack_snapshot,
)
from datafusion_engine.plan.profiler import ExplainCapture, capture_explain
from datafusion_engine.plan.substrait_artifacts import (
    substrait_bytes_from_rust_bundle,
    substrait_validation_payload,
)
from datafusion_engine.plan.udf_snapshot import (
    RequiredUdfArtifacts,
    UdfArtifacts,
    collect_udf_artifacts,
    required_udf_artifacts,
)
from obs.otel import SCOPE_PLANNING, stage_span
from serde_artifacts import DeltaInputPin, PlanArtifacts, PlanProtoStatus
from serde_msgspec import to_builtins
from serde_msgspec_ext import (
    ExecutionPlanProtoBytes,
    LogicalPlanProtoBytes,
    OptimizedPlanProtoBytes,
)
from utils.hashing import (
    hash_json_default,
)

if TYPE_CHECKING:
    from datafusion.plan import LogicalPlan as DataFusionLogicalPlan

    from datafusion_engine.lineage.scheduling import ScanUnit
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.session.runtime_session import SessionRuntime
    from semantics.program_manifest import ManifestDatasetResolver

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
    dataset_resolver: ManifestDatasetResolver | None = None


@dataclass(frozen=True)
class DataFusionPlanArtifact:
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
        return plan_display(self.logical_plan, method="display_indent_schema")

    def display_optimized_plan(self) -> str | None:
        """Return a string representation of the optimized logical plan.

        Returns:
        -------
        str | None
            Indented optimized plan display, or None if unavailable.
        """
        return plan_display(self.optimized_logical_plan, method="display_indent_schema")

    def display_execution_plan(self) -> str | None:
        """Return a string representation of the physical execution plan.

        Returns:
        -------
        str | None
            Indented execution plan display, or None if unavailable.
        """
        if self.execution_plan is None:
            return None
        return plan_display(self.execution_plan, method="display_indent")

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


def build_plan_artifact(
    ctx: SessionContext,
    df: DataFrame,
    *,
    options: PlanBundleOptions | None = None,
) -> DataFusionPlanArtifact:
    """Build a canonical plan bundle from a DataFusion DataFrame.

    Args:
        ctx: DataFusion session context.
        df: DataFrame to plan.
        options: Optional plan-bundle options.

    Returns:
        DataFusionPlanArtifact: Result.

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

        bundle = DataFusionPlanArtifact(
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
        if not bundle.plan_fingerprint:
            msg = "Plan bundle build produced an empty plan_fingerprint."
            raise ValueError(msg)
        _store_plan_cache_entry(
            bundle=bundle,
            runtime_profile=resolved.session_runtime.profile,
        )
        return bundle


def _store_plan_cache_entry(
    *,
    bundle: DataFusionPlanArtifact,
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
    udf_artifacts: UdfArtifacts
    function_registry_hash: str
    environment: _EnvironmentArtifacts
    required: RequiredUdfArtifacts
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
    rust_required_udfs: tuple[str, ...] | None = None


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
    udf_artifacts: UdfArtifacts
    function_registry_hash: str
    environment: _EnvironmentArtifacts
    substrait_validation: Mapping[str, object] | None
    proto_enabled: bool = True


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
    logical = cast("DataFusionLogicalPlan", safe_logical_plan(df))
    logical_ms = (time.perf_counter() - t0) * 1000.0
    t1 = time.perf_counter()
    optimized = cast("DataFusionLogicalPlan | None", safe_optimized_logical_plan(df))
    optimized_ms = (time.perf_counter() - t1) * 1000.0
    execution = None
    execution_ms = None
    if options.compute_execution_plan:
        t2 = time.perf_counter()
        execution = safe_execution_plan(df)
        execution_ms = (time.perf_counter() - t2) * 1000.0
    if not options.compute_substrait:
        msg = "Substrait bytes are required for plan bundle construction."
        raise ValueError(msg)
    t3 = time.perf_counter()
    substrait_bytes, rust_required_udfs = substrait_bytes_from_rust_bundle(
        ctx,
        df,
        session_runtime=options.session_runtime,
    )
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
        rust_required_udfs=rust_required_udfs,
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
    if _is_explain_plan(safe_logical_plan(df)):
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
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> _EnvironmentArtifacts:
    """Capture planning environment snapshots.

    Returns:
    -------
    _EnvironmentArtifacts
        Environment artifacts for fingerprints and diagnostics.
    """
    df_settings = _df_settings_snapshot(ctx, session_runtime=session_runtime)
    planning_snapshot = planning_env_snapshot(session_runtime)
    planning_hash = planning_env_hash(planning_snapshot)
    rules_snapshot = rulepack_snapshot(ctx)
    rules_hash = rulepack_hash(rules_snapshot)
    info_schema_snapshot = _information_schema_snapshot(ctx, session_runtime=session_runtime)
    info_schema_hash = _information_schema_hash(info_schema_snapshot)
    cdf_windows = _cdf_window_snapshot(session_runtime, dataset_resolver=dataset_resolver)
    store_policy_hash = None
    if session_runtime is not None:
        store_policy_hash = delta_store_policy_hash(
            session_runtime.profile.policies.delta_store_policy
        )
    return _EnvironmentArtifacts(
        df_settings=df_settings,
        planning_env_snapshot=planning_snapshot,
        planning_env_hash=planning_hash,
        rulepack_snapshot=rules_snapshot,
        rulepack_hash=rules_hash,
        information_schema_snapshot=info_schema_snapshot,
        information_schema_hash=info_schema_hash,
        cdf_windows=cdf_windows,
        delta_store_policy_hash=store_policy_hash,
    )


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
            dataset_resolver=options.dataset_resolver,
        )
    scan_unit_pins = _delta_inputs_from_scan_units(scan_units)
    return _merge_delta_inputs(options.delta_inputs, scan_unit_pins)


def _proto_serialization_context(
    ctx: SessionContext,
    *,
    options: PlanBundleOptions,
) -> tuple[bool, PlanProtoStatus | None]:
    enabled = options.enable_proto_serialization
    if enabled is None:
        enabled = proto_serialization_enabled()
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
        from datafusion_engine.udf.metadata import udf_planner_snapshot
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
        explain_rows_from_text(explain_artifacts.tree.text) if explain_artifacts.tree else None
    )
    explain_verbose_rows = (
        explain_rows_from_text(explain_artifacts.verbose.text)
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
        logical_plan_proto=plan_proto_payload(
            plan_core.logical,
            LogicalPlanProtoBytes,
            enabled=inputs.proto_enabled,
        ),
        optimized_plan_proto=plan_proto_payload(
            plan_core.optimized,
            OptimizedPlanProtoBytes,
            enabled=inputs.proto_enabled,
        ),
        execution_plan_proto=plan_proto_payload(
            plan_core.execution,
            ExecutionPlanProtoBytes,
            enabled=inputs.proto_enabled,
        ),
        udf_snapshot_hash=inputs.udf_artifacts.snapshot_hash,
        function_registry_hash=inputs.function_registry_hash,
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
    udf_artifacts = collect_udf_artifacts(
        ctx,
        registry_snapshot=options.registry_snapshot,
        session_runtime=options.session_runtime,
    )
    function_registry_hash = function_registry_artifacts(
        ctx,
        session_runtime=options.session_runtime,
    )
    environment = _environment_artifacts(
        ctx, session_runtime=options.session_runtime, dataset_resolver=options.dataset_resolver
    )
    required = required_udf_artifacts(
        plan_core.optimized or plan_core.logical,
        snapshot=udf_artifacts.snapshot,
        rust_required_udfs=plan_core.rust_required_udfs,
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
        dataset_resolver=options.dataset_resolver,
    )
    fingerprint = compute_plan_fingerprint(
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
            function_registry_hash=function_registry_hash,
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
        function_registry_hash=function_registry_hash,
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
    required: RequiredUdfArtifacts,
    options: PlanBundleOptions,
    snapshot: Mapping[str, object],
) -> None:
    if not options.validate_udfs or not required.required_udfs:
        return
    from datafusion_engine.udf.extension_core import validate_required_udfs

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
    return substrait_validation_payload(
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
    payload = plan_identity_payload(
        PlanIdentityInputs(
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
        plan_details=collect_plan_details(df, detail_inputs=detail_inputs),
    )


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


__all__ = [
    "DataFrameBuilder",
    "DataFusionPlanArtifact",
    "DeltaInputPin",
    "PlanArtifacts",
    "build_plan_artifact",
]
