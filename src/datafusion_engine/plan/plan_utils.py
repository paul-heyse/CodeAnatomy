"""General plan-object utility helpers."""

from __future__ import annotations

import contextlib
from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame


def explain_rows_from_text(text: str) -> tuple[dict[str, object], ...] | None:
    """Parse EXPLAIN output text into row dictionaries when possible.

    Returns:
        tuple[dict[str, object], ...] | None: Parsed row payloads when available.
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


def safe_logical_plan(df: DataFrame) -> object | None:
    """Safely extract the logical plan from a DataFrame.

    Returns:
        object | None: Logical plan payload or ``None`` when unavailable.
    """
    method = getattr(df, "logical_plan", None)
    if not callable(method):
        return None
    try:
        return method()
    except (RuntimeError, TypeError, ValueError):
        return None


def safe_optimized_logical_plan(df: DataFrame) -> object | None:
    """Safely extract the optimized logical plan from a DataFrame.

    Returns:
        object | None: Optimized logical plan payload or ``None``.
    """
    method = getattr(df, "optimized_logical_plan", None)
    if not callable(method):
        return None
    try:
        return method()
    except (RuntimeError, TypeError, ValueError):
        return None


def safe_execution_plan(df: DataFrame) -> object | None:
    """Safely extract the execution plan from a DataFrame.

    Returns:
        object | None: Execution plan payload or ``None`` when unavailable.

    Raises:
        AttributeError: If the underlying execution-plan call surface is malformed.
        RuntimeError: If DataFusion returns an unexpected runtime error.
        TypeError: If DataFusion returns an unexpected type error.
        ValueError: If DataFusion returns an unexpected value error.
    """
    method = getattr(df, "execution_plan", None)
    if not callable(method):
        return None
    try:
        return method()
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        if str(exc).startswith("DataFusion error:"):
            return None
        raise


def plan_display(plan: object | None, *, method: str) -> str | None:
    """Extract a display string from a plan object.

    Returns:
        str | None: Display string when available.
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


def extract_rule_names(container: object, attr: str) -> list[str] | None:
    """Extract planner-rule names from a container attribute.

    Returns:
        list[str] | None: Rule names when the attribute is exposed.
    """
    rules = getattr(container, attr, None)
    if callable(rules):
        with suppress_runtime_errors():
            rules = rules()
    if rules is None:
        return None
    if isinstance(rules, Sequence) and not isinstance(rules, (str, bytes)):
        names: list[str] = []
        for rule in rules:
            name = rule_name(rule)
            if name:
                names.append(name)
        return names
    return None


def rule_name(rule: object) -> str | None:
    """Resolve a stable rule name from a planner-rule object.

    Returns:
        str | None: Stable rule name or ``None`` when unavailable.
    """
    if rule is None:
        return None
    name_attr = getattr(rule, "name", None)
    if callable(name_attr):
        with suppress_runtime_errors():
            name_attr = name_attr()
    if isinstance(name_attr, str) and name_attr:
        return name_attr
    try:
        return type(rule).__name__
    except (TypeError, ValueError):
        return None


def suppress_runtime_errors() -> contextlib.AbstractContextManager[None]:
    """Return a shared suppression context for runtime probing."""
    return contextlib.suppress(RuntimeError, TypeError, ValueError)


__all__ = [
    "explain_rows_from_text",
    "extract_rule_names",
    "plan_display",
    "rule_name",
    "safe_execution_plan",
    "safe_logical_plan",
    "safe_optimized_logical_plan",
    "suppress_runtime_errors",
]
