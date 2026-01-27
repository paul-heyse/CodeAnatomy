"""UDF extraction and analysis from DataFusion logical plans.

This module provides utilities for deriving required UDFs from DataFusion
logical plans, enabling planning-critical extension validation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame

    from datafusion_engine.plan_bundle import DataFusionPlanBundle


def extract_udfs_from_logical_plan(logical_plan: object) -> frozenset[str]:
    """Extract UDF names referenced in a logical plan.

    This uses DataFusion's logical plan introspection to identify all
    user-defined function calls.

    Parameters
    ----------
    logical_plan
        DataFusion logical plan object.

    Returns
    -------
    frozenset[str]
        Set of UDF names referenced in the plan.
    """
    if logical_plan is None:
        return frozenset()

    # Try to get the plan display string and parse for function calls
    display_method = getattr(logical_plan, "display_indent_schema", None)
    if not callable(display_method):
        return frozenset()

    try:
        plan_str = str(display_method())
    except (RuntimeError, TypeError, ValueError):
        return frozenset()

    # Extract function names from the plan display
    # DataFusion plan format: "ScalarFunction { func: FunctionName, ..."
    udfs: set[str] = set()
    for line in plan_str.split("\n"):
        # Look for ScalarFunction, AggregateFunction, WindowFunction patterns
        if (
            "ScalarFunction" in line or "AggregateFunction" in line or "WindowFunction" in line
        ) and "func:" in line:
            # Extract function name from "func: name" pattern
            func_part = line.split("func:")[1].strip()
            # Get the function name before any comma or brace
            func_name = func_part.split(",")[0].split("}")[0].strip()
            if func_name:
                udfs.add(func_name)

    return frozenset(udfs)


def extract_udfs_from_dataframe(df: DataFrame) -> frozenset[str]:
    """Extract UDF names from a DataFusion DataFrame.

    Parameters
    ----------
    df
        DataFusion DataFrame to analyze.

    Returns
    -------
    frozenset[str]
        Set of UDF names referenced in the DataFrame's plan.
    """
    # Get the logical plan
    logical_plan_method = getattr(df, "logical_plan", None)
    if not callable(logical_plan_method):
        return frozenset()

    try:
        logical_plan = logical_plan_method()
    except (RuntimeError, TypeError, ValueError):
        return frozenset()

    return extract_udfs_from_logical_plan(logical_plan)


def extract_udfs_from_plan_bundle(bundle: DataFusionPlanBundle) -> frozenset[str]:
    """Extract UDF names from a plan bundle.

    Uses both the logical and optimized logical plans to ensure all
    referenced UDFs are captured.

    Parameters
    ----------
    bundle
        DataFusion plan bundle to analyze.

    Returns
    -------
    frozenset[str]
        Set of UDF names referenced in the plan bundle.
    """
    udfs: set[str] = set()

    # Extract from logical plan
    if bundle.logical_plan is not None:
        udfs.update(extract_udfs_from_logical_plan(bundle.logical_plan))

    # Extract from optimized logical plan
    if bundle.optimized_logical_plan is not None:
        udfs.update(extract_udfs_from_logical_plan(bundle.optimized_logical_plan))

    return frozenset(udfs)


def validate_required_udfs_from_plan(
    logical_plan: object,
    *,
    registry_snapshot: Mapping[str, object],
) -> None:
    """Validate that all UDFs in a plan are registered.

    Parameters
    ----------
    logical_plan
        DataFusion logical plan to validate.
    registry_snapshot
        Rust UDF registry snapshot for validation.
    """
    from datafusion_engine.udf_runtime import validate_required_udfs

    required_udfs = extract_udfs_from_logical_plan(logical_plan)
    if required_udfs:
        validate_required_udfs(registry_snapshot, required=tuple(required_udfs))


def validate_required_udfs_from_bundle(
    bundle: DataFusionPlanBundle,
    *,
    registry_snapshot: Mapping[str, object],
) -> None:
    """Validate that all UDFs in a plan bundle are registered.

    Parameters
    ----------
    bundle
        DataFusion plan bundle to validate.
    registry_snapshot
        Rust UDF registry snapshot for validation.
    """
    required_udfs = extract_udfs_from_plan_bundle(bundle)
    if required_udfs:
        from datafusion_engine.udf_runtime import validate_required_udfs

        validate_required_udfs(registry_snapshot, required=tuple(required_udfs))


def derive_required_udfs_from_plans(
    plans: Sequence[object],
) -> frozenset[str]:
    """Derive all required UDFs from a collection of logical plans.

    Parameters
    ----------
    plans
        Sequence of DataFusion logical plan objects.

    Returns
    -------
    frozenset[str]
        Union of all UDF names referenced across plans.
    """
    udfs: set[str] = set()
    for plan in plans:
        udfs.update(extract_udfs_from_logical_plan(plan))
    return frozenset(udfs)


def ensure_plan_udfs_available(
    df: DataFrame,
    *,
    registry_snapshot: Mapping[str, object],
) -> frozenset[str]:
    """Ensure all UDFs required by a DataFrame plan are registered.

    This is a convenience function for validating plan UDFs in one step.
    Planner extensions should be installed before calling this function.

    Parameters
    ----------
    df
        DataFusion DataFrame to validate.
    registry_snapshot
        Rust UDF registry snapshot for validation.

    Returns
    -------
    frozenset[str]
        Set of UDF names referenced in the DataFrame's plan.
    """
    from datafusion_engine.udf_runtime import validate_required_udfs

    required_udfs = extract_udfs_from_dataframe(df)
    if required_udfs:
        validate_required_udfs(registry_snapshot, required=tuple(required_udfs))
    return required_udfs


__all__ = [
    "derive_required_udfs_from_plans",
    "ensure_plan_udfs_available",
    "extract_udfs_from_dataframe",
    "extract_udfs_from_logical_plan",
    "extract_udfs_from_plan_bundle",
    "validate_required_udfs_from_bundle",
    "validate_required_udfs_from_plan",
]
