"""Query-pack lint contracts and linting helpers."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter_node_schema import GrammarSchemaIndex
from tools.cq.search.tree_sitter_pack_contracts import load_pack_rules

if TYPE_CHECKING:
    from tree_sitter import Query


class QueryPackLintIssueV1(CqStruct, frozen=True):
    """One query-pack lint issue row."""

    language: str
    pack_name: str
    code: str
    message: str


class QueryPackLintSummaryV1(CqStruct, frozen=True):
    """Aggregate lint summary for one language lane."""

    language: str
    issues: tuple[QueryPackLintIssueV1, ...] = ()


_NODE_PATTERN = re.compile(r"\(([A-Za-z_][A-Za-z0-9_]*)")
_FIELD_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*:")
_IGNORED_NODE_KINDS = frozenset({"ERROR", "MISSING", "_"})


def lint_query_pack_source(
    *,
    language: str,
    pack_name: str,
    source: str,
    schema_index: GrammarSchemaIndex,
    compile_query: Callable[[str], Query],
) -> tuple[QueryPackLintIssueV1, ...]:
    """Lint one query source using schema checks and query introspection.

    Returns:
    -------
    tuple[QueryPackLintIssueV1, ...]
        Stable lint issue rows for the query pack source.
    """
    issues: list[QueryPackLintIssueV1] = []

    try:
        query = compile_query(source)
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        issues.append(
            QueryPackLintIssueV1(
                language=language,
                pack_name=pack_name,
                code="compile_error",
                message=type(exc).__name__,
            )
        )
        return tuple(issues)

    unknown_nodes = [
        node_kind
        for node_kind in _NODE_PATTERN.findall(source)
        if node_kind not in _IGNORED_NODE_KINDS
        and node_kind not in schema_index.named_node_kinds
        and node_kind not in schema_index.all_node_kinds
    ]
    issues.extend(
        QueryPackLintIssueV1(
            language=language,
            pack_name=pack_name,
            code="unknown_node",
            message=node_kind,
        )
        for node_kind in unknown_nodes
    )

    unknown_fields = [
        field_name
        for field_name in _FIELD_PATTERN.findall(source)
        if field_name not in schema_index.field_names
    ]
    issues.extend(
        QueryPackLintIssueV1(
            language=language,
            pack_name=pack_name,
            code="unknown_field",
            message=field_name,
        )
        for field_name in unknown_fields
    )

    rules = load_pack_rules(language)
    pattern_count = int(getattr(query, "pattern_count", 0))
    pattern_issues: list[QueryPackLintIssueV1] = []
    for pattern_idx in range(pattern_count):
        is_rooted = bool(query.is_pattern_rooted(pattern_idx))
        if rules.require_rooted and not is_rooted:
            pattern_issues.append(
                QueryPackLintIssueV1(
                    language=language,
                    pack_name=pack_name,
                    code="pattern_not_rooted",
                    message=f"pattern={pattern_idx}",
                )
            )
        is_non_local = bool(query.is_pattern_non_local(pattern_idx))
        if rules.forbid_non_local and is_non_local:
            pattern_issues.append(
                QueryPackLintIssueV1(
                    language=language,
                    pack_name=pack_name,
                    code="pattern_non_local",
                    message=f"pattern={pattern_idx}",
                )
            )
    issues.extend(pattern_issues)

    return tuple(issues)


__all__ = [
    "QueryPackLintIssueV1",
    "QueryPackLintSummaryV1",
    "lint_query_pack_source",
]
