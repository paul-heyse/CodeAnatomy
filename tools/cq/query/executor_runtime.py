"""Query executor for cq queries.

Executes ToolPlans and returns CqResult objects.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.cache.diagnostics import snapshot_backend_metrics
from tools.cq.core.cache.run_lifecycle import maybe_evict_run_cache_tag
from tools.cq.core.result_factory import build_error_result
from tools.cq.core.run_context import SymtableEnricherPort
from tools.cq.core.schema import (
    CqResult,
    Finding,
    Section,
    assign_result_finding_ids,
    ms,
    update_result_summary,
)
from tools.cq.core.structs import CqStruct
from tools.cq.core.summary_types import (
    apply_summary_mapping,
)
from tools.cq.core.summary_update_contracts import (
    EntitySummaryUpdateV1,
)
from tools.cq.core.types import (
    QueryLanguage,
    QueryLanguageScope,
    expand_language_scope,
    file_extensions_for_language,
)
from tools.cq.orchestration.language_scope import execute_by_language_scope
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import (
    EntityQueryRequest,
    PatternQueryRequest,
)
from tools.cq.query.executor_ast_grep import (
    collect_match_spans as _collect_match_spans,
)
from tools.cq.query.executor_ast_grep import (
    filter_records_by_spans as _filter_records_by_spans,
)
from tools.cq.query.executor_runtime_entity import (
    apply_entity_handlers as _apply_entity_handlers_impl,
)
from tools.cq.query.planner import ToolPlan, scope_to_globs, scope_to_paths
from tools.cq.query.query_scan import (
    scan_entity_records as _scan_entity_records,
)
from tools.cq.query.query_summary import (
    finalize_single_scope_summary as _finalize_single_scope_summary,
)
from tools.cq.query.query_summary import (
    summary_common_for_context as _summary_common_for_context,
)
from tools.cq.query.query_summary import (
    summary_common_for_query as _summary_common_for_query,
)
from tools.cq.query.scan import (
    EntityCandidates,
    ScanContext,
)
from tools.cq.query.scan import (
    build_entity_candidates as _build_entity_candidates,
)
from tools.cq.query.scan import (
    build_scan_context as _build_scan_context,
)
from tools.cq.query.sg_parser import list_scan_files
from tools.cq.search._shared.types import SearchLimits
from tools.cq.search.rg.adapter import FilePatternSearchOptions, find_files_with_pattern
from tools.cq.utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from tools.cq.core.bootstrap import CqRuntimeServices
    from tools.cq.core.toolchain import Toolchain
from tools.cq.index.files import (
    FileFilterDecision,
    FileTabulationResult,
    build_repo_file_index,
    tabulate_files,
)
from tools.cq.index.repo import resolve_repo_context
from tools.cq.query.ir import Query, Scope
from tools.cq.query.merge import (
    MergeAutoScopeQueryRequestV1,
    merge_auto_scope_query_results,
)

_ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES = 50
logger = logging.getLogger(__name__)


@dataclass
class EntityExecutionState:
    """Prepared execution state for entity queries."""

    ctx: QueryExecutionContext
    paths: list[Path]
    scope_globs: list[str] | None
    records: list[SgRecord]
    scan: ScanContext
    candidates: EntityCandidates


@dataclass
class PatternExecutionState:
    """Prepared execution state for pattern queries."""

    ctx: QueryExecutionContext
    scope_globs: list[str] | None
    file_result: FileTabulationResult


class ExecutePlanRequestV1(CqStruct, frozen=True):
    """Request contract for query plan execution."""

    plan: ToolPlan
    query: Query
    root: str
    services: CqRuntimeServices
    symtable_enricher: SymtableEnricherPort
    argv: tuple[str, ...] = ()
    query_text: str | None = None
    run_id: str | None = None


@dataclass(frozen=True)
class _AutoScopePlanRequest:
    query: Query
    tc: Toolchain
    root: Path
    argv: tuple[str, ...]
    query_text: str | None
    run_id: str
    services: CqRuntimeServices
    symtable_enricher: SymtableEnricherPort


def _empty_result(ctx: QueryExecutionContext, message: str) -> CqResult:
    result = build_error_result(
        macro="q",
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.tc,
        started_ms=ctx.started_ms,
        error=message,
    )
    result = msgspec.structs.replace(
        result,
        summary=apply_summary_mapping(result.summary, _summary_common_for_context(ctx)),
    )
    return assign_result_finding_ids(_finalize_single_scope_summary(ctx, result))


def _resolve_symtable_enricher(ctx: QueryExecutionContext) -> SymtableEnricherPort:
    return ctx.symtable_enricher


def _resolve_entity_paths(
    ctx: QueryExecutionContext,
) -> tuple[list[Path], list[str] | None, CqResult | None]:
    plan = ctx.plan
    paths = scope_to_paths(plan.scope, ctx.root)
    if not paths:
        return [], None, _empty_result(ctx, "No files match scope")
    return paths, scope_to_globs(plan.scope), None


def _apply_rule_spans(
    ctx: QueryExecutionContext,
    paths: list[Path],
    scope_globs: list[str] | None,
    candidates: EntityCandidates,
    *,
    match_spans: dict[str, list[tuple[int, int]]] | None = None,
) -> EntityCandidates:
    plan = ctx.plan
    query = ctx.query
    if not plan.sg_rules:
        return candidates

    if match_spans is None:
        match_spans = _collect_match_spans(plan.sg_rules, paths, ctx.root, query, scope_globs)
    if not match_spans:
        return candidates

    def_records = list(candidates.def_records)
    import_records = list(candidates.import_records)
    call_records = list(candidates.call_records)

    if query.entity in {"function", "class", "method", "decorator"}:
        def_records = _filter_records_by_spans(def_records, match_spans)
    elif query.entity == "import":
        import_records = _filter_records_by_spans(import_records, match_spans)
    elif query.entity == "callsite":
        call_records = _filter_records_by_spans(call_records, match_spans)

    return EntityCandidates(
        def_records=tuple(def_records),
        import_records=tuple(import_records),
        call_records=tuple(call_records),
    )


def _prepare_entity_state(ctx: QueryExecutionContext) -> EntityExecutionState | CqResult:
    if not ctx.tc.has_sgpy:
        return _empty_result(ctx, "ast-grep not available")

    paths, scope_globs, error = _resolve_entity_paths(ctx)
    if error is not None:
        return error

    records = _scan_entity_records(ctx, paths, scope_globs)
    scan_ctx = _build_scan_context(records)
    candidates = _build_entity_candidates(scan_ctx, records)
    candidates = _apply_rule_spans(ctx, paths, scope_globs, candidates)

    return EntityExecutionState(
        ctx=ctx,
        paths=paths,
        scope_globs=scope_globs,
        records=records,
        scan=scan_ctx,
        candidates=candidates,
    )


def _tabulate_scope_files(
    root: Path,
    paths: list[Path],
    scope_globs: list[str] | None,
    *,
    lang: QueryLanguage,
    explain: bool,
) -> FileTabulationResult:
    if not explain:
        return FileTabulationResult(
            files=list_scan_files(paths=paths, root=root, globs=scope_globs, lang=lang),
            decisions=[],
        )
    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    return tabulate_files(
        repo_index,
        paths,
        scope_globs,
        extensions=file_extensions_for_language(lang),
        explain=explain,
    )


def _serialize_file_filter_decisions(decisions: list[FileFilterDecision]) -> list[str]:
    return [
        (
            f"{decision.file} ignored={int(decision.ignored)} "
            f"glob_excluded={int(decision.glob_excluded)} "
            f"scope_excluded={int(decision.scope_excluded)} "
            f"ignore_rule_index={decision.ignore_rule_index}"
        )
        for decision in decisions
    ]


def _prepare_pattern_state(ctx: QueryExecutionContext) -> PatternExecutionState | CqResult:
    if not ctx.tc.has_sgpy:
        return _empty_result(ctx, "ast-grep not available")

    paths = scope_to_paths(ctx.plan.scope, ctx.root)
    if not paths:
        return _empty_result(ctx, "No files match scope")

    scope_globs = scope_to_globs(ctx.plan.scope)
    file_result = _tabulate_scope_files(
        ctx.root,
        paths,
        scope_globs,
        lang=ctx.plan.lang,
        explain=ctx.plan.explain,
    )
    if not file_result.files:
        result = _empty_result(ctx, "No files match scope after filtering")
        if ctx.plan.explain:
            result = update_result_summary(
                result,
                {"file_filters": _serialize_file_filter_decisions(file_result.decisions)},
            )
        return result

    return PatternExecutionState(
        ctx=ctx,
        scope_globs=scope_globs,
        file_result=file_result,
    )


def _apply_entity_handlers(
    state: EntityExecutionState,
    *,
    symtable: SymtableEnricherPort,
) -> tuple[list[Finding], list[Section], EntitySummaryUpdateV1]:
    return _apply_entity_handlers_impl(state, symtable=symtable)


def _maybe_add_entity_explain(state: EntityExecutionState, result: CqResult) -> CqResult:
    plan = state.ctx.plan
    if not plan.explain:
        return result
    plan_summary = {
        "sg_record_types": list(plan.sg_record_types),
        "need_symtable": plan.need_symtable,
        "need_bytecode": plan.need_bytecode,
        "is_pattern_query": plan.is_pattern_query,
        "lang": plan.lang,
        "lang_scope": plan.lang_scope,
    }
    file_result = _tabulate_scope_files(
        state.ctx.root,
        state.paths,
        state.scope_globs,
        lang=state.ctx.plan.lang,
        explain=True,
    )
    return update_result_summary(
        result,
        {
            "plan": plan_summary,
            "file_filters": _serialize_file_filter_decisions(file_result.decisions),
        },
    )


def _maybe_add_pattern_explain(state: PatternExecutionState, result: CqResult) -> CqResult:
    plan = state.ctx.plan
    query = state.ctx.query
    if not plan.explain:
        return result
    plan_summary = {
        "is_pattern_query": True,
        "lang": plan.lang,
        "lang_scope": plan.lang_scope,
        "pattern": query.pattern_spec.pattern if query.pattern_spec else None,
        "strictness": query.pattern_spec.strictness if query.pattern_spec else None,
        "context": query.pattern_spec.context if query.pattern_spec else None,
        "selector": query.pattern_spec.selector if query.pattern_spec else None,
        "rules_count": len(plan.sg_rules),
        "metavar_filters": len(query.metavar_filters),
    }
    return update_result_summary(
        result,
        {
            "plan": plan_summary,
            "file_filters": _serialize_file_filter_decisions(state.file_result.decisions),
        },
    )


def empty_result(ctx: QueryExecutionContext, message: str) -> CqResult:
    """Build a normalized empty result payload.

    Returns:
        CqResult: Empty result with scoped summary metadata applied.
    """
    return _empty_result(ctx, message)


def resolve_symtable_enricher(ctx: QueryExecutionContext) -> SymtableEnricherPort:
    """Resolve the symtable enricher bound to the execution context.

    Returns:
        SymtableEnricherPort: Enricher used for scope filtering/enrichment.
    """
    return _resolve_symtable_enricher(ctx)


def apply_rule_spans(
    ctx: QueryExecutionContext,
    paths: list[Path],
    scope_globs: list[str] | None,
    candidates: EntityCandidates,
    *,
    match_spans: dict[str, list[tuple[int, int]]] | None = None,
) -> EntityCandidates:
    """Apply ast-grep rule spans to entity candidates.

    Returns:
        EntityCandidates: Candidates filtered by rule span constraints.
    """
    return _apply_rule_spans(
        ctx,
        paths,
        scope_globs,
        candidates,
        match_spans=match_spans,
    )


def prepare_entity_state(ctx: QueryExecutionContext) -> EntityExecutionState | CqResult:
    """Prepare state required to execute an entity query.

    Returns:
        EntityExecutionState | CqResult: Prepared state or early empty/error result.
    """
    return _prepare_entity_state(ctx)


def serialize_file_filter_decisions(decisions: list[FileFilterDecision]) -> list[str]:
    """Render file filter decisions for explain output.

    Returns:
        list[str]: Stable, human-readable file decision strings.
    """
    return _serialize_file_filter_decisions(decisions)


def prepare_pattern_state(ctx: QueryExecutionContext) -> PatternExecutionState | CqResult:
    """Prepare state required to execute a pattern query.

    Returns:
        PatternExecutionState | CqResult: Prepared state or early empty/error result.
    """
    return _prepare_pattern_state(ctx)


def summary_common_for_context(ctx: QueryExecutionContext) -> dict[str, object]:
    """Build canonical query-summary common fields for one execution context.

    Returns:
        dict[str, object]: Summary payload fields shared by query result types.
    """
    return _summary_common_for_context(ctx)


def maybe_add_entity_explain(state: EntityExecutionState, result: CqResult) -> CqResult:
    """Attach entity explain metadata when requested.

    Returns:
        CqResult: Result with optional explain metadata applied.
    """
    return _maybe_add_entity_explain(state, result)


def maybe_add_pattern_explain(state: PatternExecutionState, result: CqResult) -> CqResult:
    """Attach pattern explain metadata when requested.

    Returns:
        CqResult: Result with optional explain metadata applied.
    """
    return _maybe_add_pattern_explain(state, result)


def finalize_single_scope_summary(ctx: QueryExecutionContext, result: CqResult) -> CqResult:
    """Finalize summary fields for a single-language query result.

    Returns:
        CqResult: Result with finalized single-scope summary fields.
    """
    return _finalize_single_scope_summary(ctx, result)


def attach_entity_insight(result: CqResult, *, services: CqRuntimeServices) -> CqResult:
    """Attach entity front-door insight to the result.

    Returns:
        CqResult: Result with front-door insight payload embedded.
    """
    return _attach_entity_insight(result, services=services)


def execute_plan(request: ExecutePlanRequestV1, *, tc: Toolchain) -> CqResult:
    """Execute a ToolPlan and return results.

    Parameters
    ----------
    plan
        Compiled tool plan
    query
        Original query (for metadata)
    tc
        Toolchain with tool availability info
    root
        Repository root
    argv
        Original command line arguments

    Returns:
    -------
    CqResult
        Query results
    """
    root = Path(request.root).resolve()
    services = request.services
    active_run_id = request.run_id or uuid7_str()
    logger.debug(
        "Executing query plan mode=%s lang=%s lang_scope=%s root=%s",
        "pattern" if request.plan.is_pattern_query else "entity",
        request.plan.lang,
        request.query.lang_scope,
        root,
    )
    if request.query.lang_scope == "auto":
        return _execute_auto_scope_plan(
            _AutoScopePlanRequest(
                query=request.query,
                tc=tc,
                root=root,
                argv=tuple(request.argv),
                query_text=request.query_text,
                run_id=active_run_id,
                services=services,
                symtable_enricher=request.symtable_enricher,
            )
        )

    ctx = QueryExecutionContext(
        plan=request.plan,
        query=request.query,
        tc=tc,
        root=root,
        argv=list(request.argv),
        started_ms=ms(),
        run_id=active_run_id,
        services=services,
        symtable_enricher=request.symtable_enricher,
        query_text=request.query_text,
    )
    result = _execute_single_context(ctx)
    maybe_evict_run_cache_tag(root=root, language=request.plan.lang, run_id=active_run_id)
    return result


def _execute_single_context(ctx: QueryExecutionContext) -> CqResult:
    from tools.cq.query.executor_runtime_entity import (
        execute_entity_query as execute_entity_query_impl,
    )
    from tools.cq.query.executor_runtime_pattern import (
        execute_pattern_query as execute_pattern_query_impl,
    )

    if ctx.plan.is_pattern_query:
        logger.debug("Dispatching pattern query execution for lang=%s", ctx.plan.lang)
        return execute_pattern_query_impl(ctx)
    logger.debug("Dispatching entity query execution for lang=%s", ctx.plan.lang)
    return execute_entity_query_impl(ctx)


def _execute_auto_scope_plan(request: _AutoScopePlanRequest) -> CqResult:
    query = request.query
    results = execute_by_language_scope(
        query.lang_scope,
        lambda lang: _run_scoped_auto_query(
            request=request,
            lang=lang,
        ),
    )
    merged = merge_auto_scope_query_results(
        MergeAutoScopeQueryRequestV1(
            query=query,
            results=results,
            root=request.root,
            argv=request.argv,
            tc=request.tc,
            summary_common=_summary_common_for_query(
                query,
                query_text_override=request.query_text,
            ),
            services=request.services,
        )
    )
    merged = update_result_summary(
        merged,
        {"cache_backend": snapshot_backend_metrics(root=request.root)},
    )
    merged = assign_result_finding_ids(merged)
    for lang in expand_language_scope(query.lang_scope):
        maybe_evict_run_cache_tag(root=request.root, language=lang, run_id=request.run_id)
    return merged


def _run_scoped_auto_query(*, request: _AutoScopePlanRequest, lang: QueryLanguage) -> CqResult:
    from tools.cq.query.planner import compile_query

    scoped_query = msgspec.structs.replace(
        request.query, lang_scope=cast("QueryLanguageScope", lang)
    )
    scoped_plan = compile_query(scoped_query)
    scoped_ctx = QueryExecutionContext(
        plan=scoped_plan,
        query=scoped_query,
        tc=request.tc,
        root=request.root,
        argv=list(request.argv),
        started_ms=ms(),
        run_id=request.run_id,
        services=request.services,
        symtable_enricher=request.symtable_enricher,
    )
    return _execute_single_context(scoped_ctx)


def _attach_entity_insight(result: CqResult, *, services: CqRuntimeServices) -> CqResult:
    """Build and attach front-door insight card to entity result.

    Returns:
        CqResult: Updated result with front-door insight attached.
    """
    from tools.cq.core.services import EntityFrontDoorRequest

    return services.entity.attach_front_door(
        EntityFrontDoorRequest(
            result=result,
            relationship_detail_max_matches=_ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES,
        )
    )


def execute_entity_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute an entity-based query via extracted entity runtime module.

    Returns:
        CqResult: Entity query result from the extracted runtime path.
    """
    from tools.cq.query.executor_runtime_entity import _execute_entity_query_impl as impl

    return impl(ctx)


def execute_entity_query_from_records(request: EntityQueryRequest) -> CqResult:
    """Execute entity query from pre-scanned records via extracted module.

    Returns:
        CqResult: Entity query result built from caller-provided records.
    """
    from tools.cq.query.executor_runtime_entity import (
        _execute_entity_query_from_records_impl as impl,
    )

    return impl(request)


def execute_pattern_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute pattern query via extracted pattern runtime module.

    Returns:
        CqResult: Pattern query result from the extracted runtime path.
    """
    from tools.cq.query.executor_runtime_pattern import _execute_pattern_query_impl as impl

    return impl(ctx)


def execute_pattern_query_with_files(request: PatternQueryRequest) -> CqResult:
    """Execute pattern query with pre-tabulated files via extracted module.

    Returns:
        CqResult: Pattern query result built from caller-provided file inputs.
    """
    from tools.cq.query.executor_runtime_pattern import (
        _execute_pattern_query_with_files_impl as impl,
    )

    return impl(request)


def process_decorator_query(
    ctx: ScanContext,
    query: Query,
    root: Path,
    def_candidates: list[SgRecord] | tuple[SgRecord, ...] | None = None,
) -> tuple[list[Finding], EntitySummaryUpdateV1]:
    """Process a decorator query through extracted entity runtime helpers.

    Returns:
        Decorator findings and typed summary counters.
    """
    from tools.cq.query.executor_runtime_entity import process_decorator_query as impl

    return impl(
        ctx=ctx,
        query=query,
        root=root,
        def_candidates=def_candidates,
    )


def process_call_query(
    ctx: ScanContext,
    query: Query,
    root: Path,
) -> tuple[list[Finding], EntitySummaryUpdateV1]:
    """Process a callsite query through extracted entity runtime helpers.

    Returns:
        Callsite findings and typed summary counters.
    """
    from tools.cq.query.executor_runtime_entity import process_call_query as impl

    return impl(ctx=ctx, query=query, root=root)


def rg_files_with_matches(
    root: Path,
    pattern: str,
    scope: Scope,
    *,
    limits: SearchLimits | None = None,
) -> list[Path]:
    """Use ripgrep to find files matching pattern.

    Parameters
    ----------
    root
        Repository root
    pattern
        Regex pattern to search
    scope
        Scope constraints
    limits
        Optional search safety limits. Uses scope.max_depth if not provided.

    Returns:
    -------
    list[Path]
        Files containing matches
    """
    # Determine search root from scope
    search_root = root / scope.in_dir if scope.in_dir else root

    # Build limits from scope or defaults
    effective_limits = limits or SearchLimits(max_depth=50)

    return find_files_with_pattern(
        search_root,
        pattern,
        options=FilePatternSearchOptions(
            include_globs=tuple(scope.globs) if scope.globs else (),
            exclude_globs=tuple(scope.exclude) if scope.exclude else (),
            limits=effective_limits,
        ),
    )


__all__ = [
    "ExecutePlanRequestV1",
    "apply_rule_spans",
    "attach_entity_insight",
    "execute_entity_query",
    "execute_entity_query_from_records",
    "execute_pattern_query",
    "execute_pattern_query_with_files",
    "execute_plan",
    "finalize_single_scope_summary",
    "maybe_add_entity_explain",
    "maybe_add_pattern_explain",
    "prepare_entity_state",
    "prepare_pattern_state",
    "process_call_query",
    "process_decorator_query",
    "resolve_symtable_enricher",
    "rg_files_with_matches",
    "serialize_file_filter_decisions",
    "summary_common_for_context",
]
