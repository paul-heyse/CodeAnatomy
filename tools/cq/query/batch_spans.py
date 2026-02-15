"""Batch collection of relational match spans for multiple queries."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from ast_grep_py import SgNode, SgRoot

from tools.cq.core.locations import SourceSpan
from tools.cq.core.pathing import normalize_repo_relative_path
from tools.cq.query.executor import (
    AstGrepMatchSpan,
    _filter_match_spans_by_metavars,
    _group_match_spans,
    _iter_rule_matches_for_spans,
)
from tools.cq.query.language import QueryLanguage, file_extensions_for_language

if TYPE_CHECKING:
    from tools.cq.query.ir import Query
    from tools.cq.query.planner import AstGrepRule, ToolPlan


def collect_span_filters(
    *,
    root: Path,
    files: list[Path],
    queries: list[Query],
    plans: list[ToolPlan],
) -> list[dict[str, list[tuple[int, int]]]]:
    """Collect relational match spans for each query using a shared file parse.

    Returns:
    -------
    list[dict[str, list[tuple[int, int]]]]
        Per-query mapping of file to match spans.
    """
    per_query: list[list[AstGrepMatchSpan]] = [[] for _ in queries]
    rule_sets = [plan.sg_rules if plan.sg_rules else () for plan in plans]
    query_langs: list[QueryLanguage] = [query.primary_language for query in queries]

    for file_path in files:
        _collect_file_matches(
            file_path=file_path,
            root=root,
            rule_sets=rule_sets,
            query_langs=query_langs,
            per_query=per_query,
        )

    return _build_spans_by_query(per_query=per_query, queries=queries)


def _collect_file_matches(
    *,
    file_path: Path,
    root: Path,
    rule_sets: list[tuple[AstGrepRule, ...]],
    query_langs: list[QueryLanguage],
    per_query: list[list[AstGrepMatchSpan]],
) -> None:
    try:
        src = file_path.read_text(encoding="utf-8")
    except OSError:
        return
    rel_path = normalize_repo_relative_path(str(file_path), root=root)
    roots_by_lang: dict[QueryLanguage, SgNode] = {}

    for idx, rules in enumerate(rule_sets):
        if not rules:
            continue
        lang = query_langs[idx]
        if file_path.suffix not in file_extensions_for_language(lang):
            continue
        if lang not in roots_by_lang:
            roots_by_lang[lang] = SgRoot(src, lang).root()
        node = roots_by_lang[lang]
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


def _build_spans_by_query(
    *,
    per_query: list[list[AstGrepMatchSpan]],
    queries: list[Query],
) -> list[dict[str, list[tuple[int, int]]]]:
    spans_by_query: list[dict[str, list[tuple[int, int]]]] = []
    for idx, matches in enumerate(per_query):
        spans_by_query.append(_query_spans(matches=matches, query=queries[idx]))
    return spans_by_query


def _query_spans(
    *,
    matches: list[AstGrepMatchSpan],
    query: Query,
) -> dict[str, list[tuple[int, int]]]:
    if not matches:
        return {}
    if not query.metavar_filters:
        return _group_match_spans(matches)
    return _filter_match_spans_by_metavars(matches, query.metavar_filters)


__all__ = [
    "collect_span_filters",
]
