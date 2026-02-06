"""UDF extraction and validation from DataFusion logical plans."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame

    from datafusion_engine.plan.bundle import DataFusionPlanBundle


def extract_udfs_from_logical_plan(
    logical_plan: object,
    *,
    udf_snapshot: Mapping[str, object] | None = None,
) -> frozenset[str]:
    """Extract UDF names referenced in a logical plan.

    Returns:
    -------
    frozenset[str]
        UDF names referenced in the plan.
    """
    if logical_plan is None:
        return frozenset()
    from datafusion_engine.lineage.datafusion import extract_lineage

    lineage = extract_lineage(logical_plan, udf_snapshot=udf_snapshot)
    return frozenset(lineage.required_udfs)


def extract_udfs_from_dataframe(
    df: DataFrame,
    *,
    udf_snapshot: Mapping[str, object] | None = None,
) -> frozenset[str]:
    """Extract UDF names from a DataFusion DataFrame.

    Returns:
    -------
    frozenset[str]
        UDF names referenced in the DataFrame plan.
    """
    logical_plan_method = getattr(df, "logical_plan", None)
    if not callable(logical_plan_method):
        return frozenset()
    try:
        logical_plan = logical_plan_method()
    except (RuntimeError, TypeError, ValueError):
        return frozenset()
    return extract_udfs_from_logical_plan(logical_plan, udf_snapshot=udf_snapshot)


def extract_udfs_from_plan_bundle(bundle: DataFusionPlanBundle) -> frozenset[str]:
    """Extract UDF names from a plan bundle.

    Returns:
    -------
    frozenset[str]
        UDF names referenced in the bundle plans.
    """
    if bundle.required_udfs:
        return frozenset(bundle.required_udfs)
    snapshot = bundle.artifacts.udf_snapshot
    if bundle.optimized_logical_plan is not None:
        return extract_udfs_from_logical_plan(
            bundle.optimized_logical_plan,
            udf_snapshot=snapshot,
        )
    if bundle.logical_plan is not None:
        return extract_udfs_from_logical_plan(
            bundle.logical_plan,
            udf_snapshot=snapshot,
        )
    return frozenset()


def validate_required_udfs_from_plan(
    logical_plan: object,
    *,
    registry_snapshot: Mapping[str, object],
) -> None:
    """Validate that all UDFs in a plan are registered."""
    from datafusion_engine.udf.runtime import validate_required_udfs

    required_udfs = extract_udfs_from_logical_plan(
        logical_plan,
        udf_snapshot=registry_snapshot,
    )
    if required_udfs:
        validate_required_udfs(registry_snapshot, required=tuple(sorted(required_udfs)))


def validate_required_udfs_from_bundle(
    bundle: DataFusionPlanBundle,
    *,
    registry_snapshot: Mapping[str, object],
) -> None:
    """Validate that all UDFs in a plan bundle are registered."""
    from datafusion_engine.udf.runtime import validate_required_udfs

    required_udfs = extract_udfs_from_plan_bundle(bundle)
    if required_udfs:
        validate_required_udfs(registry_snapshot, required=tuple(sorted(required_udfs)))


def derive_required_udfs_from_plans(
    plans: Sequence[object],
    *,
    udf_snapshot: Mapping[str, object] | None = None,
) -> frozenset[str]:
    """Derive all required UDFs from a collection of logical plans.

    Returns:
    -------
    frozenset[str]
        Union of required UDFs for all plans.
    """
    required: set[str] = set()
    for plan in plans:
        required.update(extract_udfs_from_logical_plan(plan, udf_snapshot=udf_snapshot))
    return frozenset(required)


def ensure_plan_udfs_available(
    df: DataFrame,
    *,
    registry_snapshot: Mapping[str, object],
) -> frozenset[str]:
    """Ensure all UDFs required by a DataFrame plan are registered.

    Returns:
    -------
    frozenset[str]
        UDF names required by the DataFrame plan.
    """
    from datafusion_engine.udf.runtime import validate_required_udfs

    required_udfs = extract_udfs_from_dataframe(df, udf_snapshot=registry_snapshot)
    if required_udfs:
        validate_required_udfs(registry_snapshot, required=tuple(sorted(required_udfs)))
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
