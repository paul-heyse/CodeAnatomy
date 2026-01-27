"""Canonical DataFusion plan bundle for all planning and scheduling.

This module provides the single canonical plan artifact that all execution
and scheduling paths use, replacing SQLGlot/Ibis compilation pipelines.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from serde_msgspec import dumps_msgpack

if TYPE_CHECKING:
    from datafusion.plan import LogicalPlan as DataFusionLogicalPlan

    from datafusion_engine.runtime import SessionRuntime


try:
    from datafusion.substrait import Producer as SubstraitProducer
except ImportError:
    SubstraitProducer = None

# Type alias for DataFrame builder functions
DataFrameBuilder = Callable[[SessionContext], DataFrame]


@dataclass(frozen=True)
class PlanArtifacts:
    """Serializable planning artifacts for reproducibility and scheduling."""

    logical_plan_display: str | None
    optimized_plan_display: str | None
    optimized_plan_graphviz: str | None
    optimized_plan_pgjson: str | None
    execution_plan_display: str | None
    df_settings: Mapping[str, str]
    udf_snapshot_hash: str
    function_registry_hash: str
    function_registry_snapshot: Mapping[str, object]
    rewrite_tags: tuple[str, ...]
    domain_planner_names: tuple[str, ...]
    udf_snapshot: Mapping[str, object]


@dataclass(frozen=True)
class DeltaInputPin:
    """Pinned Delta version information for a scan input."""

    dataset_name: str
    version: int | None
    timestamp: str | None


@dataclass(frozen=True)
class PlanBundleOptions:
    """Options for building a DataFusion plan bundle."""

    compute_execution_plan: bool = False
    compute_substrait: bool = True
    validate_udfs: bool = False
    registry_snapshot: Mapping[str, object] | None = None
    delta_inputs: Sequence[DeltaInputPin] = ()
    session_runtime: SessionRuntime | None = None


@dataclass(frozen=True)
class DataFusionPlanBundle:
    """Canonical plan artifact for all planning and scheduling.

    This is the single source of truth for DataFusion plan information,
    replacing SQLGlot AST artifacts and Ibis plan wrappers.

    Attributes
    ----------
    df : DataFrame
        The DataFusion DataFrame for this plan.
    logical_plan : DataFusionLogicalPlan
        The unoptimized logical plan.
    optimized_logical_plan : DataFusionLogicalPlan
        The optimized logical plan (used for lineage extraction).
    execution_plan : DataFusionExecutionPlan | None
        The physical execution plan (may be None for lazy evaluation).
    substrait_bytes : bytes | None
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
    substrait_bytes: bytes | None
    plan_fingerprint: str
    artifacts: PlanArtifacts
    delta_inputs: tuple[DeltaInputPin, ...] = ()
    required_udfs: tuple[str, ...] = ()
    required_rewrite_tags: tuple[str, ...] = ()
    plan_details: Mapping[str, object] = field(default_factory=dict)

    def display_logical_plan(self) -> str | None:
        """Return a string representation of the logical plan.

        Returns
        -------
        str | None
            Indented logical plan display, or None if unavailable.
        """
        return _plan_display(self.logical_plan, method="display_indent_schema")

    def display_optimized_plan(self) -> str | None:
        """Return a string representation of the optimized logical plan.

        Returns
        -------
        str | None
            Indented optimized plan display, or None if unavailable.
        """
        return _plan_display(self.optimized_logical_plan, method="display_indent_schema")

    def display_execution_plan(self) -> str | None:
        """Return a string representation of the physical execution plan.

        Returns
        -------
        str | None
            Indented execution plan display, or None if unavailable.
        """
        if self.execution_plan is None:
            return None
        return _plan_display(self.execution_plan, method="display_indent")

    def graphviz(self) -> str | None:
        """Return GraphViz DOT representation of the optimized plan.

        Returns
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


def build_plan_bundle(  # noqa: PLR0913
    ctx: SessionContext,
    df: DataFrame,
    *,
    compute_execution_plan: bool = False,
    compute_substrait: bool = True,
    validate_udfs: bool = False,
    registry_snapshot: Mapping[str, object] | None = None,
    delta_inputs: Sequence[DeltaInputPin] = (),
    session_runtime: SessionRuntime | None = None,
) -> DataFusionPlanBundle:
    """Build a canonical plan bundle from a DataFusion DataFrame.

    This is the single entrypoint for plan construction. All execution
    and scheduling paths should use this function.

    Planner extensions (UDFs, ExprPlanner, FunctionFactory) should be
    installed in the SessionContext before calling this function.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context for plan operations.
    df : DataFrame
        DataFusion DataFrame to build the plan from.
    compute_execution_plan : bool
        Whether to compute the physical execution plan (expensive).
    compute_substrait : bool
        Whether to compute Substrait bytes for fingerprinting.
    validate_udfs : bool
        Whether to validate that all referenced UDFs are registered.
    registry_snapshot : Mapping[str, object] | None
        Optional registry snapshot for UDF validation.
    delta_inputs : Sequence[DeltaInputPin]
        Optional pinned Delta inputs for deterministic scans.
    session_runtime : SessionRuntime | None
        Optional session runtime carrying UDF and settings snapshots.

    Returns
    -------
    DataFusionPlanBundle
        Canonical plan artifact for execution and scheduling.
    """
    logical = _safe_logical_plan(df)
    optimized = _safe_optimized_logical_plan(df)
    execution = _safe_execution_plan(df) if compute_execution_plan else None
    substrait_bytes = (
        _to_substrait_bytes(ctx, optimized) if compute_substrait and optimized is not None else None
    )
    fingerprint = _hash_plan(substrait_bytes=substrait_bytes, optimized=optimized)
    snapshot, snapshot_hash, rewrite_tags, domain_planner_names = _udf_artifacts(
        ctx,
        registry_snapshot=registry_snapshot,
        session_runtime=session_runtime,
    )
    function_registry_hash, function_registry_snapshot = _function_registry_artifacts(ctx)
    df_settings = _df_settings_snapshot(ctx, session_runtime=session_runtime)
    required_udfs, required_rewrite_tags = _required_udf_artifacts(
        optimized or logical,
        snapshot=snapshot,
    )

    # Optionally validate UDFs are registered before returning the bundle
    if validate_udfs:
        from datafusion_engine.udf_runtime import validate_required_udfs

        if required_udfs:
            validate_required_udfs(snapshot, required=required_udfs)

    artifacts = PlanArtifacts(
        logical_plan_display=_plan_display(logical, method="display_indent_schema"),
        optimized_plan_display=_plan_display(optimized, method="display_indent_schema"),
        optimized_plan_graphviz=_plan_graphviz(optimized),
        optimized_plan_pgjson=_plan_pgjson(optimized),
        execution_plan_display=_plan_display(execution, method="display_indent"),
        df_settings=df_settings,
        udf_snapshot_hash=snapshot_hash,
        function_registry_hash=function_registry_hash,
        function_registry_snapshot=function_registry_snapshot,
        rewrite_tags=rewrite_tags,
        domain_planner_names=domain_planner_names,
        udf_snapshot=snapshot,
    )

    return DataFusionPlanBundle(
        df=df,
        logical_plan=logical,
        optimized_logical_plan=optimized,
        execution_plan=execution,
        substrait_bytes=substrait_bytes,
        plan_fingerprint=fingerprint,
        artifacts=artifacts,
        delta_inputs=tuple(delta_inputs),
        required_udfs=required_udfs,
        required_rewrite_tags=required_rewrite_tags,
        plan_details=_plan_details(df, logical=logical, optimized=optimized, execution=execution),
    )


def build_plan_bundle_from_builder(
    ctx: SessionContext,
    builder: DataFrameBuilder,
    *,
    options: PlanBundleOptions | None = None,
) -> DataFusionPlanBundle:
    """Build a plan bundle from a DataFrame builder function.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context.
    builder : DataFrameBuilder
        Callable that returns a DataFrame given a SessionContext.
    options : PlanBundleOptions | None
        Optional configuration overrides for plan bundle construction.

    Returns
    -------
    DataFusionPlanBundle
        Canonical plan artifact.
    """
    df = builder(ctx)
    resolved = options or PlanBundleOptions()
    return build_plan_bundle(
        ctx,
        df,
        compute_execution_plan=resolved.compute_execution_plan,
        compute_substrait=resolved.compute_substrait,
        validate_udfs=resolved.validate_udfs,
        registry_snapshot=resolved.registry_snapshot,
        delta_inputs=resolved.delta_inputs,
        session_runtime=resolved.session_runtime,
    )


def _safe_logical_plan(df: DataFrame) -> object | None:
    """Safely extract the logical plan from a DataFrame.

    Returns
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

    Returns
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

    Returns
    -------
    object | None
        Execution plan, or None if unavailable.
    """
    method = getattr(df, "execution_plan", None)
    if not callable(method):
        # Try physical_plan as fallback for older versions
        method = getattr(df, "physical_plan", None)
        if not callable(method):
            return None
    try:
        return method()
    except (RuntimeError, TypeError, ValueError):
        return None


def _to_substrait_bytes(ctx: SessionContext, optimized: object | None) -> bytes | None:
    """Convert an optimized plan to Substrait bytes.

    Uses DataFusion's Substrait Producer to serialize the plan for
    portable storage and fingerprinting.

    Returns
    -------
    bytes | None
        Substrait plan bytes, or None if unavailable.
    """
    if SubstraitProducer is None:
        return None
    if optimized is None:
        return None
    # Use Producer.to_substrait_plan(logical_plan, ctx) -> Plan, then Plan.encode() -> bytes
    try:
        to_substrait = getattr(SubstraitProducer, "to_substrait_plan", None)
        if callable(to_substrait):
            substrait_plan = to_substrait(cast("DataFusionLogicalPlan", optimized), ctx)
            encode = getattr(substrait_plan, "encode", None)
            if callable(encode):
                return cast("bytes | None", encode())
    except (RuntimeError, TypeError, ValueError, AttributeError):
        pass

    # Fallback: use plan display string for fingerprinting
    # This is less portable but still deterministic
    return None


def _hash_plan(
    *,
    substrait_bytes: bytes | None,
    optimized: object | None,
) -> str:
    """Compute a stable fingerprint for the plan.

    Prefers Substrait bytes when available, falls back to plan display.

    Returns
    -------
    str
        Stable plan fingerprint.
    """
    if substrait_bytes is not None:
        return hashlib.sha256(substrait_bytes).hexdigest()
    if optimized is not None:
        display = _plan_display(optimized, method="display_indent_schema")
        if display is not None:
            return hashlib.sha256(display.encode("utf-8")).hexdigest()
    return hashlib.sha256(b"empty_plan").hexdigest()


def _plan_display(plan: object | None, *, method: str) -> str | None:
    """Extract a display string from a plan object.

    Returns
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

    Returns
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


def _plan_details(
    df: DataFrame,
    *,
    logical: object | None,
    optimized: object | None,
    execution: object | None,
) -> dict[str, object]:
    """Collect plan details for diagnostics.

    Returns
    -------
    dict[str, object]
        Diagnostic plan metadata.
    """
    details: dict[str, object] = {}
    details["logical_plan"] = _plan_display(logical, method="display_indent_schema")
    details["optimized_plan"] = _plan_display(optimized, method="display_indent_schema")
    details["physical_plan"] = _plan_display(execution, method="display_indent")
    details["graphviz"] = _plan_graphviz(optimized)
    details["optimized_plan_pgjson"] = _plan_pgjson(optimized)
    details["partition_count"] = _plan_partition_count(execution)
    schema_names: list[str] = list(df.schema().names) if hasattr(df.schema(), "names") else []
    details["schema_names"] = schema_names
    return details


def _plan_graphviz(plan: object | None) -> str | None:
    """Extract GraphViz representation from a plan.

    Returns
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

    Returns
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
    from datafusion_engine.schema_introspection import SchemaIntrospector

    try:
        introspector = SchemaIntrospector(ctx)
        rows = introspector.settings_snapshot()
        if not rows:
            return {}
        return _settings_rows_to_mapping(rows)
    except (RuntimeError, TypeError, ValueError):
        return {}


def _function_registry_hash(snapshot: Mapping[str, object]) -> str:
    payload = dumps_msgpack(snapshot)
    return hashlib.sha256(payload).hexdigest()


def _function_registry_artifacts(ctx: SessionContext) -> tuple[str, Mapping[str, object]]:
    from datafusion_engine.schema_introspection import SchemaIntrospector

    functions: Sequence[Mapping[str, object]] = ()
    try:
        introspector = SchemaIntrospector(ctx)
        functions = introspector.function_catalog_snapshot(include_parameters=True)
    except (RuntimeError, TypeError, ValueError):
        functions = ()
    snapshot: Mapping[str, object] = {"functions": list(functions)}
    return _function_registry_hash(snapshot), snapshot


def _udf_artifacts(
    ctx: SessionContext,
    *,
    registry_snapshot: Mapping[str, object] | None,
    session_runtime: SessionRuntime | None,
) -> tuple[Mapping[str, object], str, tuple[str, ...], tuple[str, ...]]:
    if session_runtime is not None and session_runtime.ctx is ctx:
        return (
            session_runtime.udf_snapshot,
            session_runtime.udf_snapshot_hash,
            session_runtime.udf_rewrite_tags,
            session_runtime.domain_planner_names,
        )
    if registry_snapshot is not None:
        snapshot = registry_snapshot
    else:
        from datafusion_engine.udf_runtime import rust_udf_snapshot

        snapshot = rust_udf_snapshot(ctx)
    from datafusion_engine.domain_planner import domain_planner_names_from_snapshot
    from datafusion_engine.udf_catalog import rewrite_tag_index
    from datafusion_engine.udf_runtime import rust_udf_snapshot_hash, validate_rust_udf_snapshot

    validate_rust_udf_snapshot(snapshot)
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    tag_index = rewrite_tag_index(snapshot)
    rewrite_tags = tuple(sorted(tag_index))
    planner_names = domain_planner_names_from_snapshot(snapshot)
    return snapshot, snapshot_hash, rewrite_tags, planner_names


def _required_udf_artifacts(
    plan: object | None,
    *,
    snapshot: Mapping[str, object],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if plan is None:
        return (), ()
    from datafusion_engine.lineage_datafusion import extract_lineage

    lineage = extract_lineage(plan, udf_snapshot=snapshot)
    return lineage.required_udfs, lineage.required_rewrite_tags


__all__ = [
    "DataFrameBuilder",
    "DataFusionPlanBundle",
    "DeltaInputPin",
    "PlanArtifacts",
    "build_plan_bundle",
    "build_plan_bundle_from_builder",
]
