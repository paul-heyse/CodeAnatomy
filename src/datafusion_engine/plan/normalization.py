"""Logical plan normalization helpers for DataFusion planning."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, cast

from datafusion import SessionContext

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


def _looks_like_plan(value: object | None) -> bool:
    if value is None:
        return False
    return all(hasattr(value, name) for name in ("inputs", "to_variant", "display_indent"))


def _embedded_plans(variant: object) -> list[object]:
    plans: list[object] = []
    for attr in ("subquery", "plan", "input"):
        value = getattr(variant, attr, None)
        if _looks_like_plan(value):
            plans.append(value)
    for attr in getattr(variant, "__dict__", {}):
        value = getattr(variant, attr, None)
        if _looks_like_plan(value):
            plans.append(value)
    return plans


def _walk_logical_complete(root: object) -> tuple[object, ...]:
    stack: list[object] = [root]
    seen: set[int] = set()
    ordered: list[object] = []
    while stack:
        node = stack.pop()
        node_id = id(node)
        if node_id in seen:
            continue
        seen.add(node_id)
        ordered.append(node)
        variant = _safe_plan_variant(node)
        stack.extend(reversed(_safe_plan_inputs(node)))
        if variant is not None:
            stack.extend(reversed(_embedded_plans(variant)))
    return tuple(ordered)


def normalize_substrait_plan(
    ctx: SessionContext,
    plan: DataFusionLogicalPlan,
) -> DataFusionLogicalPlan:
    """Normalize logical plans for Substrait serialization.

    Returns:
    -------
    DataFusionLogicalPlan
        Normalized logical plan ready for Substrait serialization.

    Raises:
        ValueError: If unsupported plan variants remain after wrapper stripping.
    """
    _ = ctx
    normalized = _strip_substrait_wrappers(plan)
    unsupported = _unsupported_substrait_variants(normalized)
    if unsupported:
        msg = (
            "Substrait normalization failed: unsupported logical-plan variants "
            f"remain after wrapper stripping: {sorted(unsupported)}"
        )
        raise ValueError(msg)
    return normalized


def _unsupported_substrait_variants(plan: DataFusionLogicalPlan) -> set[str]:
    unsupported: set[str] = set()
    for node in _walk_logical_complete(plan):
        variant_name = _plan_variant_name(node)
        if variant_name in _UNSUPPORTED_SUBSTRAIT_VARIANTS:
            unsupported.add(variant_name)
    return unsupported


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


def _safe_plan_inputs(plan: object) -> list[object]:
    inputs = getattr(plan, "inputs", None)
    if not callable(inputs):
        return []
    try:
        children = inputs()
    except (RuntimeError, TypeError, ValueError):
        return []
    if isinstance(children, Iterable) and not isinstance(children, (str, bytes)):
        return [child for child in children if child is not None]
    return []


def _safe_plan_variant(plan: object) -> object | None:
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
    if _looks_like_plan(value):
        return cast("DataFusionLogicalPlan", value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for entry in value:
            if _looks_like_plan(entry):
                return cast("DataFusionLogicalPlan", entry)
    return None


def _plan_variant_name(plan: object) -> str:
    if not _looks_like_plan(plan):
        return type(plan).__name__
    to_variant = getattr(plan, "to_variant", None)
    if not callable(to_variant):
        return type(plan).__name__
    try:
        variant = to_variant()
    except (RuntimeError, TypeError, ValueError):
        return type(plan).__name__
    return type(variant).__name__


__all__ = ["normalize_substrait_plan"]
