"""AST-grep metavariable pattern helper utilities."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.query.ir import MetaVarCapture, MetaVarFilter, MetaVarKind
    from tools.cq.query.planner import AstGrepRule

_METAVAR_TOKEN_RE = re.compile(r"\${1,3}([A-Z][A-Z0-9_]*)")
_VARIADIC_METAVAR_TOKEN_RE = re.compile(r"\${3}([A-Z][A-Z0-9_]*)")


def extract_metavar_names(text: str) -> tuple[str, ...]:
    """Extract unique metavariable names from one pattern string.

    Returns:
        tuple[str, ...]: Sorted unique metavariable names.
    """
    if not text:
        return ()
    return tuple(sorted({match.group(1) for match in _METAVAR_TOKEN_RE.finditer(text)}))


def extract_variadic_metavar_names(text: str) -> tuple[str, ...]:
    """Extract unique variadic metavariable names from one pattern string.

    Returns:
        tuple[str, ...]: Sorted unique variadic metavariable names.
    """
    if not text:
        return ()
    return tuple(sorted({match.group(1) for match in _VARIADIC_METAVAR_TOKEN_RE.finditer(text)}))


def extract_rule_metavars(rule: AstGrepRule) -> tuple[str, ...]:
    """Extract all metavariable names referenced by a compiled ast-grep rule.

    Returns:
        tuple[str, ...]: Sorted unique metavariable names.
    """
    parts: list[str] = [rule.pattern]
    parts.extend(
        value
        for value in (rule.context, rule.inside, rule.has, rule.precedes, rule.follows)
        if value
    )
    if rule.composite is not None:
        parts.extend(rule.composite.patterns)
    if rule.nth_child is not None and isinstance(rule.nth_child.of_rule, str):
        parts.append(rule.nth_child.of_rule)
    names = {name for part in parts for name in extract_metavar_names(part)}
    return tuple(sorted(names))


def extract_rule_variadic_metavars(rule: AstGrepRule) -> frozenset[str]:
    """Extract all variadic metavariable names referenced by a compiled ast-grep rule.

    Returns:
        frozenset[str]: Unique variadic metavariable names.
    """
    parts: list[str] = [rule.pattern]
    parts.extend(
        value
        for value in (rule.context, rule.inside, rule.has, rule.precedes, rule.follows)
        if value
    )
    if rule.composite is not None:
        parts.extend(rule.composite.patterns)
    if rule.nth_child is not None and isinstance(rule.nth_child.of_rule, str):
        parts.append(rule.nth_child.of_rule)
    names = {name for part in parts for name in extract_variadic_metavar_names(part)}
    return frozenset(names)


def parse_metavariables(match_result: dict[str, object]) -> dict[str, MetaVarCapture]:
    """Extract metavariable captures from ast-grep match payload.

    Returns:
        dict[str, MetaVarCapture]: Captures keyed by metavariable name.
    """
    from tools.cq.query.ir import MetaVarCapture

    captures: dict[str, MetaVarCapture] = {}
    meta_vars = match_result.get("metaVariables")
    if not isinstance(meta_vars, dict):
        return captures

    single = meta_vars.get("single", {})
    if isinstance(single, dict):
        for name, capture_info in single.items():
            if not isinstance(name, str) or name.startswith("_"):
                continue
            text = capture_info.get("text", "") if isinstance(capture_info, dict) else ""
            captures[name] = MetaVarCapture(name=name, kind="single", text=text)

    multi = meta_vars.get("multi", {})
    if isinstance(multi, dict):
        for name, capture_list in multi.items():
            if not isinstance(name, str) or name.startswith("_"):
                continue
            if not isinstance(capture_list, list):
                continue
            node_texts = [
                item.get("text", "")
                for item in capture_list
                if isinstance(item, dict) and isinstance(item.get("text"), str)
            ]
            captures[name] = MetaVarCapture(
                name=name,
                kind="multi",
                text=", ".join(node_texts),
                nodes=node_texts,
            )

    return captures


def validate_pattern_metavars(pattern: str) -> list[str]:
    """Extract and validate metavariable names from a pattern string.

    Returns:
        list[str]: Ordered metavar tokens discovered in the pattern.

    Raises:
        ValueError: If the pattern contains an invalid metavariable token.
    """
    all_metavars = re.findall(r"\$+[A-Za-z_][A-Za-z0-9_-]*", pattern)
    for metavar in all_metavars:
        if re.match(r"\$[a-z]", metavar):
            msg = f"Metavariable must be UPPERCASE: {metavar!r} in pattern"
            raise ValueError(msg)
        if re.match(r"\$[0-9]", metavar):
            msg = f"Metavariable cannot start with digit: {metavar!r} in pattern"
            raise ValueError(msg)
        if "-" in metavar:
            msg = f"Metavariable cannot contain hyphens: {metavar!r} in pattern"
            raise ValueError(msg)
    return all_metavars


def apply_metavar_filters(
    captures: dict[str, MetaVarCapture],
    filters: tuple[MetaVarFilter, ...],
) -> bool:
    """Return whether capture set passes all metavar filters.

    Returns:
        bool: ``True`` when every filter matches the captured values.
    """
    for filter_spec in filters:
        capture = captures.get(filter_spec.name)
        if capture is None:
            return False
        if not filter_spec.matches(capture):
            return False
    return True


def get_metavar_kind(metavar: str) -> MetaVarKind:
    """Determine metavariable kind from syntax token prefix.

    Returns:
        MetaVarKind: Classified metavar kind.
    """
    if metavar.startswith("$$$"):
        return "multi"
    if metavar.startswith("$$"):
        return "unnamed"
    return "single"


def partition_metavar_filters(
    filters: tuple[MetaVarFilter, ...],
) -> tuple[dict[str, dict[str, str]], tuple[MetaVarFilter, ...]]:
    """Split filters into ast-grep pushdown constraints and residual filters.

    Returns:
        tuple[dict[str, dict[str, str]], tuple[MetaVarFilter, ...]]: Pushdown constraints and
        non-pushdown residual filters.
    """
    constraints: dict[str, dict[str, str]] = {}
    residual: list[MetaVarFilter] = []
    for item in filters:
        if item.negate:
            residual.append(item)
            continue
        if item.name in constraints:
            residual.append(item)
            continue
        constraints[item.name] = {"regex": item.pattern}
    return constraints, tuple(residual)


__all__ = [
    "apply_metavar_filters",
    "extract_metavar_names",
    "extract_rule_metavars",
    "extract_rule_variadic_metavars",
    "extract_variadic_metavar_names",
    "get_metavar_kind",
    "parse_metavariables",
    "partition_metavar_filters",
    "validate_pattern_metavars",
]
