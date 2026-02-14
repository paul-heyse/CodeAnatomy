"""Logical plan normalization helpers for DataFusion planning."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.plan.walk import looks_like_plan, walk_logical_complete

if TYPE_CHECKING:
    from datafusion.plan import LogicalPlan as DataFusionLogicalPlan


_UNSUPPORTED_SUBSTRAIT_VARIANTS: tuple[str, ...] = (
    "Analyze",
    "EmptyRelation",
    "Explain",
    "RecursiveQuery",
    "Unnest",
)


_SUBSTRAIT_WRAPPER_VARIANTS: dict[str, tuple[str, ...]] = {
    "Analyze": ("input",),
    "Explain": ("plan", "input"),
    "RecursiveQuery": ("static_term", "recursive_term"),
    "Unnest": ("input",),
}


def normalize_substrait_plan(
    ctx: SessionContext,
    plan: DataFusionLogicalPlan,
) -> DataFusionLogicalPlan:
    """Normalize logical plans for Substrait serialization.

    Returns:
    -------
    DataFusionLogicalPlan
        Normalized logical plan ready for Substrait serialization.
    """
    normalized = _strip_substrait_wrappers(plan)
    if not _contains_unsupported_substrait_nodes(normalized):
        return normalized
    return _fallback_substrait_plan(ctx, normalized)


def _contains_unsupported_substrait_nodes(plan: DataFusionLogicalPlan) -> bool:
    for node in walk_logical_complete(plan):
        if _plan_variant_name(node) in _UNSUPPORTED_SUBSTRAIT_VARIANTS:
            return True
    return False


def _strip_substrait_wrappers(plan: DataFusionLogicalPlan) -> DataFusionLogicalPlan:
    current = plan
    while True:
        unwrapped = _unwrap_substrait_wrapper(current)
        if unwrapped is None or unwrapped is current:
            return current
        current = unwrapped


def _unwrap_substrait_wrapper(plan: DataFusionLogicalPlan) -> DataFusionLogicalPlan | None:
    variant = _safe_plan_variant(plan)
    if variant is None:
        return None
    variant_name = type(variant).__name__
    attrs = _SUBSTRAIT_WRAPPER_VARIANTS.get(variant_name)
    if attrs is None:
        return None
    candidate = _coerce_plan(_safe_plan_inputs(plan))
    if candidate is not None:
        return candidate
    for attr in attrs:
        value = _safe_variant_attr(variant, attr)
        candidate = _coerce_plan(value)
        if candidate is not None:
            return candidate
    return None


def _safe_plan_inputs(plan: DataFusionLogicalPlan) -> list[object]:
    inputs = getattr(plan, "inputs", None)
    if not callable(inputs):
        return []
    try:
        children = inputs()
    except (RuntimeError, TypeError, ValueError):
        return []
    if isinstance(children, Sequence) and not isinstance(children, (str, bytes)):
        return [child for child in children if child is not None]
    return []


def _safe_plan_variant(plan: DataFusionLogicalPlan) -> object | None:
    to_variant = getattr(plan, "to_variant", None)
    if not callable(to_variant):
        return None
    try:
        return to_variant()
    except (RuntimeError, TypeError, ValueError):
        return None


def _safe_variant_attr(variant: object, attr: str) -> object | None:
    value = getattr(variant, attr, None)
    if callable(value):
        try:
            return value()
        except (RuntimeError, TypeError, ValueError):
            return None
    return value


def _coerce_plan(value: object | None) -> DataFusionLogicalPlan | None:
    if value is None:
        return None
    if looks_like_plan(value):
        return cast("DataFusionLogicalPlan", value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for entry in value:
            if looks_like_plan(entry):
                return cast("DataFusionLogicalPlan", entry)
    return None


def _plan_variant_name(plan: object) -> str:
    if not looks_like_plan(plan):
        return type(plan).__name__
    to_variant = getattr(plan, "to_variant", None)
    if not callable(to_variant):
        return type(plan).__name__
    try:
        variant = to_variant()
    except (RuntimeError, TypeError, ValueError):
        return type(plan).__name__
    return type(variant).__name__


def _fallback_substrait_plan(
    ctx: SessionContext,
    plan: DataFusionLogicalPlan,
) -> DataFusionLogicalPlan:
    from datafusion_engine.lineage.reporting import referenced_tables_from_plan

    tables = referenced_tables_from_plan(plan)
    available = _available_substrait_tables(ctx, tables)
    if available:
        union_sql = " UNION ALL ".join(f"SELECT 1 AS _ FROM {name}" for name in available)
        df = ctx.sql(union_sql)
        normalized = _safe_optimized_logical_plan(df)
        if normalized is not None:
            return cast("DataFusionLogicalPlan", normalized)
    probe_name = _ensure_substrait_probe_table(ctx)
    df = ctx.table(probe_name)
    normalized = _safe_optimized_logical_plan(df)
    if normalized is not None:
        return cast("DataFusionLogicalPlan", normalized)
    return plan


def _available_substrait_tables(
    ctx: SessionContext,
    tables: Sequence[str],
) -> list[str]:
    available: list[str] = []
    for name in tables:
        try:
            _ = ctx.table(name)
        except (KeyError, RuntimeError, TypeError, ValueError):
            continue
        available.append(name)
    return available


def _ensure_substrait_probe_table(ctx: SessionContext) -> str:
    name = "__substrait_probe__"
    try:
        _ = ctx.table(name)
    except (KeyError, RuntimeError, TypeError, ValueError):
        pass
    else:
        return name
    batch = pa.record_batch([pa.array([1], type=pa.int8())], names=["_probe"])
    ctx.register_record_batches(name, [[batch]])
    return name


def _safe_optimized_logical_plan(df: DataFrame) -> object | None:
    method = getattr(df, "optimized_logical_plan", None)
    if not callable(method):
        return None
    try:
        return method()
    except (RuntimeError, TypeError, ValueError):
        return None


__all__ = ["normalize_substrait_plan"]
