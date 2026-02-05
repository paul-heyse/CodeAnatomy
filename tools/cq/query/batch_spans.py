"""Batch collection of relational match spans for multiple queries."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from ast_grep_py import SgRoot

from tools.cq.core.locations import SourceSpan
from tools.cq.query.executor import (
    AstGrepMatchSpan,
    _filter_match_spans_by_metavars,
    _group_match_spans,
    _iter_rule_matches_for_spans,
    _normalize_match_file,
)

if TYPE_CHECKING:
    from tools.cq.query.ir import Query
    from tools.cq.query.planner import ToolPlan


def collect_span_filters(
    *,
    root: Path,
    files: list[Path],
    queries: list[Query],
    plans: list[ToolPlan],
) -> list[dict[str, list[tuple[int, int]]]]:
    """Collect relational match spans for each query using a shared file parse.

    Returns
    -------
    list[dict[str, list[tuple[int, int]]]]
        Per-query mapping of file to match spans.
    """
    per_query: list[list[AstGrepMatchSpan]] = [[] for _ in queries]
    rule_sets = [plan.sg_rules if plan.sg_rules else () for plan in plans]

    for file_path in files:
        try:
            src = file_path.read_text(encoding="utf-8")
        except OSError:
            continue
        sg_root = SgRoot(src, "python")
        node = sg_root.root()
        rel_path = _normalize_match_file(str(file_path), root)

        for idx, rules in enumerate(rule_sets):
            if not rules:
                continue
            for rule in rules:
                for match in _iter_rule_matches_for_spans(node, rule):
                    range_obj = match.range()
                    per_query[idx].append(
                        AstGrepMatchSpan(
                            span=SourceSpan(
                                file=rel_path,
                                start_line=range_obj.start.line + 1,
                                start_col=range_obj.start.column,
                                end_line=range_obj.end.line + 1,
                                end_col=range_obj.end.column,
                            ),
                            match=match,
                        )
                    )

    spans_by_query: list[dict[str, list[tuple[int, int]]]] = []
    for idx, matches in enumerate(per_query):
        if not matches:
            spans_by_query.append({})
            continue
        query = queries[idx]
        if not query.metavar_filters:
            spans_by_query.append(_group_match_spans(matches))
        else:
            spans_by_query.append(_filter_match_spans_by_metavars(matches, query.metavar_filters))
    return spans_by_query


__all__ = [
    "collect_span_filters",
]
