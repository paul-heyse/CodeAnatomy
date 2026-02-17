"""Summary/runmeta helpers for query execution flows."""

from __future__ import annotations

import msgspec

from tools.cq.core.contracts import SummaryBuildRequest
from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import CqResult, RunMeta
from tools.cq.core.summary_contract import (
    build_semantic_telemetry,
    summary_from_mapping,
)
from tools.cq.orchestration.multilang_summary import (
    build_multilang_summary,
    partition_stats_from_result_summary,
)
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.ir import Query
from tools.cq.query.shared_utils import count_result_matches
from tools.cq.search.semantic.diagnostics import build_language_capabilities

__all__ = [
    "build_runmeta",
    "finalize_single_scope_summary",
    "query_mode",
    "query_text",
    "summary_common_for_context",
    "summary_common_for_query",
]


def build_runmeta(ctx: QueryExecutionContext) -> RunMeta:
    """Build run metadata for a query execution context.

    Returns:
        RunMeta: Query run metadata contract.
    """
    run_ctx = RunContext.from_parts(
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.tc,
        started_ms=ctx.started_ms,
        run_id=ctx.run_id,
    )
    return run_ctx.to_runmeta("q")


def query_mode(query: Query) -> str:
    """Return query mode label for summary payloads.

    Returns:
        str: `"pattern"` or `"entity"` query mode label.
    """
    return "pattern" if query.is_pattern_query else "entity"


def query_text(query: Query) -> str:
    """Render user-facing query text for summary payloads.

    Returns:
        str: Query text suitable for result summaries.
    """
    if query.pattern_spec is not None:
        return query.pattern_spec.pattern
    parts: list[str] = []
    if query.entity is not None:
        parts.append(f"entity={query.entity}")
    if query.name:
        parts.append(f"name={query.name}")
    return " ".join(parts) if parts else "q"


def summary_common_for_query(
    query: Query,
    *,
    query_text_override: str | None = None,
) -> dict[str, object]:
    """Build common summary mapping for a query.

    Returns:
        dict[str, object]: Common summary fields for query execution output.
    """
    text = (
        query_text_override.strip()
        if isinstance(query_text_override, str) and query_text_override.strip()
        else query_text(query)
    )
    common: dict[str, object] = {
        "query": text,
        "mode": query_mode(query),
        "python_semantic_overview": dict[str, object](),
        "python_semantic_telemetry": build_semantic_telemetry(
            attempted=0, applied=0, failed=0, skipped=0, timed_out=0
        ),
        "rust_semantic_telemetry": build_semantic_telemetry(
            attempted=0, applied=0, failed=0, skipped=0, timed_out=0
        ),
        "semantic_planes": dict[str, object](),
        "python_semantic_diagnostics": list[dict[str, object]](),
    }
    if query.pattern_spec is not None:
        common["pattern"] = query.pattern_spec.pattern
        if query.pattern_spec.context is not None:
            common["pattern_context"] = query.pattern_spec.context
        if query.pattern_spec.selector is not None:
            common["pattern_selector"] = query.pattern_spec.selector
    return common


def summary_common_for_context(ctx: QueryExecutionContext) -> dict[str, object]:
    """Build common summary mapping for an execution context.

    Returns:
        dict[str, object]: Common summary fields derived from execution context.
    """
    return summary_common_for_query(ctx.query, query_text_override=ctx.query_text)


def finalize_single_scope_summary(ctx: QueryExecutionContext, result: CqResult) -> CqResult:
    """Finalize per-language summary for non-auto language scopes.

    Returns:
        CqResult: Result with a normalized single-language summary contract.
    """
    if ctx.query.lang_scope == "auto":
        return result
    lang = ctx.query.primary_language
    common = result.summary.to_dict()
    partition = partition_stats_from_result_summary(
        result.summary,
        fallback_matches=count_result_matches(result),
    )
    summary = summary_from_mapping(
        build_multilang_summary(
            SummaryBuildRequest(
                common=common,
                lang_scope=ctx.query.lang_scope,
                language_order=(lang,),
                languages={lang: partition},
                cross_language_diagnostics=[],
                language_capabilities=build_language_capabilities(lang_scope=ctx.query.lang_scope),
            )
        )
    )
    return msgspec.structs.replace(result, summary=summary)
