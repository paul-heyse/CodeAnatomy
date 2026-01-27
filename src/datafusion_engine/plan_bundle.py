"""Canonical DataFusion plan bundle for all planning and scheduling.

This module provides the single canonical plan artifact that all execution
and scheduling paths use, replacing SQLGlot/Ibis compilation pipelines.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

if TYPE_CHECKING:
    from datafusion.plan import LogicalPlan as DataFusionLogicalPlan

try:
    from datafusion.substrait import Producer as SubstraitProducer
except ImportError:
    SubstraitProducer = None

# Type alias for DataFrame builder functions
DataFrameBuilder = Callable[[SessionContext], DataFrame]


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
    plan_details : Mapping[str, object]
        Additional plan metadata for diagnostics.
    """

    df: DataFrame
    logical_plan: object  # DataFusionLogicalPlan
    optimized_logical_plan: object  # DataFusionLogicalPlan
    execution_plan: object | None  # DataFusionExecutionPlan | None
    substrait_bytes: bytes | None
    plan_fingerprint: str
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
    details = _plan_details(df, logical=logical, optimized=optimized, execution=execution)

    # Optionally validate UDFs are registered before returning the bundle
    if validate_udfs:
        if registry_snapshot is None:
            from datafusion_engine.udf_runtime import rust_udf_snapshot

            registry_snapshot = rust_udf_snapshot(ctx)
        from datafusion_engine.plan_udf_analysis import extract_udfs_from_logical_plan
        from datafusion_engine.udf_runtime import validate_required_udfs

        required_udfs = extract_udfs_from_logical_plan(optimized or logical)
        if required_udfs:
            validate_required_udfs(registry_snapshot, required=tuple(required_udfs))

    return DataFusionPlanBundle(
        df=df,
        logical_plan=logical,
        optimized_logical_plan=optimized,
        execution_plan=execution,
        substrait_bytes=substrait_bytes,
        plan_fingerprint=fingerprint,
        plan_details=details,
    )


def build_plan_bundle_from_builder(
    ctx: SessionContext,
    builder: DataFrameBuilder,
    *,
    compute_execution_plan: bool = False,
    compute_substrait: bool = True,
) -> DataFusionPlanBundle:
    """Build a plan bundle from a DataFrame builder function.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context.
    builder : DataFrameBuilder
        Callable that returns a DataFrame given a SessionContext.
    compute_execution_plan : bool
        Whether to compute the physical execution plan.
    compute_substrait : bool
        Whether to compute Substrait bytes.

    Returns
    -------
    DataFusionPlanBundle
        Canonical plan artifact.
    """
    df = builder(ctx)
    return build_plan_bundle(
        ctx,
        df,
        compute_execution_plan=compute_execution_plan,
        compute_substrait=compute_substrait,
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


__all__ = [
    "DataFrameBuilder",
    "DataFusionPlanBundle",
    "build_plan_bundle",
    "build_plan_bundle_from_builder",
]
