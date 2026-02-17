"""AST-grep metavariable pattern helper utilities."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
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


__all__ = [
    "extract_metavar_names",
    "extract_rule_metavars",
    "extract_rule_variadic_metavars",
    "extract_variadic_metavar_names",
]
