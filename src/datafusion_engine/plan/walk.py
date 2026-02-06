"""Helpers for walking DataFusion logical plans with embedded subplans."""

from __future__ import annotations

from collections.abc import Iterable


def looks_like_plan(value: object) -> bool:
    """Return whether a value behaves like a DataFusion LogicalPlan.

    Parameters
    ----------
    value : object
        Candidate object to inspect.

    Returns:
    -------
    bool
        True when the object exposes plan-like APIs.
    """
    return all(hasattr(value, name) for name in ("inputs", "to_variant", "display_indent"))


def _safe_inputs(plan: object) -> list[object]:
    """Return plan inputs while tolerating variant failures.

    Parameters
    ----------
    plan : object
        Logical plan to query for inputs.

    Returns:
    -------
    list[object]
        Child plan inputs, or an empty list when unavailable.
    """
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


def embedded_plans(variant: object) -> list[object]:
    """Return embedded plan objects from a plan variant payload.

    Parameters
    ----------
    variant : object
        Plan variant payload with embedded plan references.

    Returns:
    -------
    list[object]
        Embedded plan objects discovered on the variant.
    """
    plans: list[object] = []
    for attr in ("subquery", "plan", "input"):
        value = getattr(variant, attr, None)
        if looks_like_plan(value):
            plans.append(value)
    for attr in getattr(variant, "__dict__", {}):
        value = getattr(variant, attr, None)
        if looks_like_plan(value):
            plans.append(value)
    return plans


def walk_logical_complete(root: object) -> list[object]:
    """Return a preorder walk that includes embedded subplans.

    Parameters
    ----------
    root : object
        Root logical plan to traverse.

    Returns:
    -------
    list[object]
        Ordered plan nodes including embedded subplans.
    """
    stack: list[object] = [root]
    seen: set[int] = set()
    ordered: list[object] = []
    while stack:
        node = stack.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        ordered.append(node)
        variant_method = getattr(node, "to_variant", None)
        variant = None
        if callable(variant_method):
            try:
                variant = variant_method()
            except (RuntimeError, TypeError, ValueError):
                variant = None
        stack.extend(reversed(_safe_inputs(node)))
        if variant is not None:
            stack.extend(reversed(embedded_plans(variant)))
    return ordered


__all__ = ["embedded_plans", "looks_like_plan", "walk_logical_complete"]
