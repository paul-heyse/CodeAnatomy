"""Pattern-query execution entry points extracted from executor_runtime."""

from __future__ import annotations

import msgspec

from tools.cq.core.cache.diagnostics import snapshot_backend_metrics
from tools.cq.core.schema import (
    CqResult,
    assign_result_finding_ids,
    mk_result,
    ms,
    update_result_summary,
)
from tools.cq.core.summary_types import apply_summary_mapping
from tools.cq.index.files import FileTabulationResult
from tools.cq.query.enrichment import filter_by_scope
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import PatternQueryRequest
from tools.cq.query.executor_ast_grep import execute_ast_grep_rules
from tools.cq.query.planner import scope_to_globs
from tools.cq.query.query_summary import (
    build_runmeta,
    summary_common_for_context,
)
from tools.cq.query.section_fallbacks import ensure_query_sections

__all__ = ["execute_pattern_query", "execute_pattern_query_with_files"]


def _execute_pattern_query_impl(ctx: QueryExecutionContext) -> CqResult:
    """Execute a pattern-based query using inline ast-grep rules.

    Raises:
        ValueError: If called with an entity query plan.

    Returns:
        CqResult: Pattern query result with ast-grep findings and summary metadata.
    """
    from tools.cq.query import executor_runtime as runtime

    if not ctx.plan.is_pattern_query:
        msg = "execute_pattern_query called with an entity query plan; use execute_entity_query instead"
        raise ValueError(msg)
    state = runtime.prepare_pattern_state(ctx)
    if isinstance(state, CqResult):
        return state

    findings, records, _ = execute_ast_grep_rules(
        state.ctx.plan.sg_rules,
        state.file_result.files,
        state.ctx.root,
        state.ctx.query,
        state.scope_globs,
        state.ctx.run_id,
    )
    result = mk_result(build_runmeta(ctx))
    summary = apply_summary_mapping(result.summary, summary_common_for_context(ctx))
    key_findings = list(findings)
    if state.ctx.query.scope_filter and findings:
        enricher = runtime.resolve_symtable_enricher(state.ctx)
        key_findings = filter_by_scope(
            key_findings,
            state.ctx.query.scope_filter,
            enricher,
            records,
        )
    if state.ctx.query.limit and len(key_findings) > state.ctx.query.limit:
        key_findings = key_findings[: state.ctx.query.limit]

    summary = apply_summary_mapping(
        summary,
        {
            "matches": len(key_findings),
            "total_matches": len(key_findings),
            "files_scanned": len({r.file for r in records}),
        },
    )
    result = msgspec.structs.replace(
        result,
        summary=summary,
        key_findings=key_findings,
    )
    result = ensure_query_sections(result, title="Findings")
    result = runtime.maybe_add_pattern_explain(state, result)
    result = runtime.finalize_single_scope_summary(ctx, result)
    result = update_result_summary(
        result,
        {"cache_backend": snapshot_backend_metrics(root=ctx.root)},
    )
    return assign_result_finding_ids(result)


def _execute_pattern_query_with_files_impl(request: PatternQueryRequest) -> CqResult:
    """Execute a pattern query using a pre-tabulated file list.

    Returns:
        CqResult: Pattern query result assembled from supplied file/decision inputs.
    """
    from tools.cq.query import executor_runtime as runtime
    from tools.cq.query.execution_context import QueryExecutionContext
    from tools.cq.query.executor_runtime import PatternExecutionState

    ctx = QueryExecutionContext(
        plan=request.plan,
        query=request.query,
        tc=request.tc,
        root=request.root,
        argv=request.argv,
        started_ms=ms(),
        run_id=request.run_id or runtime.uuid7_str(),
        services=request.services,
        symtable_enricher=request.symtable,
        query_text=request.query_text,
    )
    if not request.files:
        result = runtime.empty_result(ctx, "No files match scope after filtering")
        if request.plan.explain and request.decisions is not None:
            result = update_result_summary(
                result,
                {"file_filters": runtime.serialize_file_filter_decisions(request.decisions)},
            )
        return result

    state = PatternExecutionState(
        ctx=ctx,
        scope_globs=scope_to_globs(request.plan.scope),
        file_result=FileTabulationResult(files=request.files, decisions=request.decisions or []),
    )
    findings, records, _ = execute_ast_grep_rules(
        state.ctx.plan.sg_rules,
        state.file_result.files,
        state.ctx.root,
        state.ctx.query,
        state.scope_globs,
        state.ctx.run_id,
    )

    result = mk_result(build_runmeta(ctx))
    summary = apply_summary_mapping(result.summary, summary_common_for_context(ctx))
    key_findings = list(findings)

    if state.ctx.query.scope_filter and findings:
        enricher = runtime.resolve_symtable_enricher(state.ctx)
        key_findings = filter_by_scope(
            key_findings,
            state.ctx.query.scope_filter,
            enricher,
            records,
        )

    if state.ctx.query.limit and len(key_findings) > state.ctx.query.limit:
        key_findings = key_findings[: state.ctx.query.limit]

    summary = apply_summary_mapping(
        summary,
        {
            "matches": len(key_findings),
            "total_matches": len(key_findings),
            "files_scanned": len({r.file for r in records}),
        },
    )
    result = msgspec.structs.replace(
        result,
        summary=summary,
        key_findings=key_findings,
    )
    result = ensure_query_sections(result, title="Findings")
    result = runtime.maybe_add_pattern_explain(state, result)
    result = runtime.finalize_single_scope_summary(ctx, result)
    result = update_result_summary(
        result,
        {"cache_backend": snapshot_backend_metrics(root=ctx.root)},
    )
    return assign_result_finding_ids(result)


def execute_pattern_query(ctx: QueryExecutionContext) -> CqResult:
    """Delegate pattern query execution through the canonical runtime entrypoint.

    Returns:
        CqResult: Executed query result.
    """
    from tools.cq.query import executor_runtime as runtime

    return runtime.execute_pattern_query(ctx)


def execute_pattern_query_with_files(request: PatternQueryRequest) -> CqResult:
    """Delegate file-scoped pattern execution through the canonical runtime entrypoint.

    Returns:
        CqResult: Executed query result.
    """
    from tools.cq.query import executor_runtime as runtime

    return runtime.execute_pattern_query_with_files(request)
