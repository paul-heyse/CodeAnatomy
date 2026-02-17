"""Metavariable capture parsing and filtering for ast-grep results.

Handles extraction of metavariable captures from ast-grep JSON output
and provides filtering utilities.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from tools.cq.astgrep.metavar import (
    extract_metavar_names as _extract_metavar_names,
)
from tools.cq.astgrep.metavar import (
    extract_rule_metavars as _extract_rule_metavars,
)
from tools.cq.astgrep.metavar import (
    extract_rule_variadic_metavars as _extract_rule_variadic_metavars,
)
from tools.cq.astgrep.metavar import (
    extract_variadic_metavar_names as _extract_variadic_metavar_names,
)

if TYPE_CHECKING:
    from tools.cq.query.ir import MetaVarCapture, MetaVarFilter, MetaVarKind
    from tools.cq.query.planner import AstGrepRule


def extract_metavar_names(text: str) -> tuple[str, ...]:
    """Extract metavariable names from a single ast-grep pattern string.

    Returns:
        tuple[str, ...]: Ordered metavariable names.
    """
    return _extract_metavar_names(text)


def extract_variadic_metavar_names(text: str) -> tuple[str, ...]:
    """Extract variadic metavariable names from a pattern string.

    Returns:
        tuple[str, ...]: Ordered variadic metavariable names.
    """
    return _extract_variadic_metavar_names(text)


def extract_rule_metavars(rule: AstGrepRule) -> tuple[str, ...]:
    """Extract all metavariable names from a compiled ast-grep rule.

    Returns:
        tuple[str, ...]: Ordered metavariable names referenced by the rule.
    """
    return _extract_rule_metavars(rule)


def extract_rule_variadic_metavars(rule: AstGrepRule) -> frozenset[str]:
    """Extract all variadic metavariable names from a compiled ast-grep rule.

    Returns:
        frozenset[str]: Variadic metavariable names referenced by the rule.
    """
    return _extract_rule_variadic_metavars(rule)


def parse_metavariables(match_result: dict[str, object]) -> dict[str, MetaVarCapture]:
    """Extract metavariable captures from ast-grep match result.

    Parameters
    ----------
    match_result
        Raw match result from ast-grep JSON output.

    Returns:
    -------
    dict[str, MetaVarCapture]
        Map of metavariable name to capture info.

    Notes:
    -----
    ast-grep returns metaVariables with structure:
    ```json
    {
        "metaVariables": {
            "single": {"VAR_NAME": {"text": "...", ...}},
            "multi": {"VAR_NAME": [{"text": "..."}, ...]}
        }
    }
    ```
    """
    from tools.cq.query.ir import MetaVarCapture

    captures: dict[str, MetaVarCapture] = {}

    meta_vars = match_result.get("metaVariables")
    if not isinstance(meta_vars, dict):
        return captures

    # Single captures ($NAME)
    for name, capture_info in meta_vars.get("single", {}).items():
        # Skip non-capturing wildcards ($_NAME)
        if name.startswith("_"):
            continue

        captures[name] = MetaVarCapture(
            name=name,
            kind="single",
            text=capture_info.get("text", ""),
        )

    # Multi captures ($$$NAME)
    for name, capture_list in meta_vars.get("multi", {}).items():
        # Skip non-capturing wildcards
        if name.startswith("_"):
            continue

        # Multi captures are lists of nodes
        if isinstance(capture_list, list):
            node_texts = [c.get("text", "") for c in capture_list if isinstance(c, dict)]
            captures[name] = MetaVarCapture(
                name=name,
                kind="multi",
                text=", ".join(node_texts),
                nodes=node_texts,
            )

    # Unnamed node captures ($$NAME) - handled same as single in output
    # but may capture unnamed tree-sitter nodes

    return captures


def validate_pattern_metavars(pattern: str) -> list[str]:
    """Extract and validate metavariable names from pattern.

    Args:
        pattern: Query pattern containing metavariables.

    Returns:
        list[str]: Extracted metavariable names.

    Raises:
        ValueError: If a metavariable name is invalid.
    """
    # Find all metavar-like patterns
    all_metavars = re.findall(r"\$+[A-Za-z_][A-Za-z0-9_-]*", pattern)

    for metavar in all_metavars:
        # Check for invalid patterns
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
    """Check if captures pass all metavar filters.

    Parameters
    ----------
    captures
        Extracted metavariable captures from a match.
    filters
        Filters to apply.

    Returns:
    -------
    bool
        True if all filters pass, False otherwise.
    """
    for filter_spec in filters:
        capture = captures.get(filter_spec.name)
        if capture is None:
            # Filter references a metavar that wasn't captured
            # This is a non-match
            return False

        if not filter_spec.matches(capture):
            return False

    return True


def get_metavar_kind(metavar: str) -> MetaVarKind:
    """Determine the kind of a metavariable from its syntax.

    Parameters
    ----------
    metavar
        Metavariable string (e.g., '$NAME', '$$$ARGS', '$$OP')

    Returns:
    -------
    MetaVarKind
        The kind of metavariable: 'single', 'multi', or 'unnamed'.
    """
    if metavar.startswith("$$$"):
        return "multi"
    if metavar.startswith("$$"):
        return "unnamed"
    return "single"


def partition_metavar_filters(
    filters: tuple[MetaVarFilter, ...],
) -> tuple[dict[str, dict[str, str]], tuple[MetaVarFilter, ...]]:
    """Partition filters into ast-grep constraints and residual Python filters.

    Returns:
    -------
    tuple[dict[str, dict[str, str]], tuple[MetaVarFilter, ...]]
        Pushdown constraints and residual filters for Python-side evaluation.
    """
    constraints: dict[str, dict[str, str]] = {}
    residual: list[MetaVarFilter] = []
    for item in filters:
        if item.negate:
            residual.append(item)
            continue
        # ast-grep accepts one regex per metavariable in constraints.
        # Keep duplicates in residual path to preserve full semantics.
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
