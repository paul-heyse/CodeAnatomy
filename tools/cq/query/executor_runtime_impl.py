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
from tools.cq.core.entity_kinds import ENTITY_KINDS
from tools.cq.core.result_factory import build_error_result
from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    Section,
    assign_result_finding_ids,
    mk_result,
    ms,
    update_result_summary,
)
from tools.cq.core.structs import CqStruct
from tools.cq.core.summary_contract import (
    apply_summary_mapping,
    as_search_summary,
)
from tools.cq.core.types import QueryLanguage, QueryLanguageScope
from tools.cq.orchestration.multilang_orchestrator import (
    execute_by_language_scope,
)
from tools.cq.query.enrichment import SymtableEnricher, filter_by_scope
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import (
    DefQueryContext,
    EntityQueryRequest,
    PatternQueryRequest,
)
from tools.cq.query.executor_ast_grep import (
    collect_match_spans as _collect_match_spans,
)
from tools.cq.query.executor_ast_grep import (
    execute_ast_grep_rules as _execute_ast_grep_rules,
)
from tools.cq.query.executor_ast_grep import (
    filter_records_by_spans as _filter_records_by_spans,
)
from tools.cq.query.executor_definitions import (
    def_to_finding as _def_to_finding,
)
from tools.cq.query.executor_definitions import (
    filter_to_matching as _filter_to_matching,
)
from tools.cq.query.executor_definitions import (
    matches_name as _matches_name,
)
from tools.cq.query.executor_definitions import (
    process_def_query as _process_def_query,
)
from tools.cq.query.executor_definitions import (
    process_import_query as _process_import_query,
)
from tools.cq.query.finding_builders import (
    apply_call_evidence as _apply_call_evidence,
)
from tools.cq.query.finding_builders import (
    build_def_evidence_map as _build_def_evidence_map,
)
from tools.cq.query.finding_builders import (
    call_to_finding as _call_to_finding,
)
from tools.cq.query.finding_builders import (
    extract_call_target as _extract_call_target,
)
from tools.cq.query.finding_builders import (
    record_key as _record_key,
)
from tools.cq.query.language import expand_language_scope, file_extensions_for_language
from tools.cq.query.planner import ToolPlan, scope_to_globs, scope_to_paths
from tools.cq.query.query_scan import (
    scan_entity_records as _scan_entity_records,
)
from tools.cq.query.query_summary import (
    build_runmeta as _build_runmeta,
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
from tools.cq.query.shared_utils import extract_def_name
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
    symtable: SymtableEnricher | None = None,
) -> tuple[list[Finding], list[Section], dict[str, object]]:
    query = state.ctx.query
    root = state.ctx.root
    candidates = state.candidates

    if query.entity == "import":
        temp_result = mk_result(_build_runmeta(state.ctx))
        temp_result = _process_import_query(
            list(candidates.import_records),
            query,
            temp_result,
            root,
            symtable=symtable,
        )
        return (
            list(temp_result.key_findings),
            list(temp_result.sections),
            _entity_summary_updates(temp_result),
        )
    if query.entity == "decorator":
        findings, summary_updates = _process_decorator_query(
            state.scan,
            query,
            root,
            list(candidates.def_records),
        )
        return findings, [], summary_updates
    if query.entity == "callsite":
        findings, summary_updates = _process_call_query(state.scan, query, root)
        return findings, [], summary_updates

    temp_result = mk_result(_build_runmeta(state.ctx))
    def_ctx = DefQueryContext(state=state, result=temp_result, symtable=symtable)
    temp_result = _process_def_query(def_ctx, query, list(candidates.def_records))
    return (
        list(temp_result.key_findings),
        list(temp_result.sections),
        _entity_summary_updates(temp_result),
    )


def _entity_summary_updates(result: CqResult) -> dict[str, object]:
    summary = as_search_summary(result.summary)
    return {
        "matches": summary.matches,
        "total_defs": summary.total_defs,
        "total_calls": summary.total_calls,
        "total_imports": summary.total_imports,
    }


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
        query_text=request.query_text,
    )
    result = _execute_single_context(ctx)
    maybe_evict_run_cache_tag(root=root, language=request.plan.lang, run_id=active_run_id)
    return result


def _execute_single_context(ctx: QueryExecutionContext) -> CqResult:
    from tools.cq.query.executor_entity import execute_entity_query
    from tools.cq.query.executor_pattern import execute_pattern_query

    if ctx.plan.is_pattern_query:
        logger.debug("Dispatching pattern query execution for lang=%s", ctx.plan.lang)
        return execute_pattern_query(ctx)
    logger.debug("Dispatching entity query execution for lang=%s", ctx.plan.lang)
    return execute_entity_query(ctx)


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
    """Execute an entity-based query.

    Returns:
    -------
    CqResult
        Query result with findings and summary metadata.
    """
    assert not ctx.plan.is_pattern_query, (
        "execute_entity_query called with a pattern query plan; use execute_pattern_query instead"
    )
    state = _prepare_entity_state(ctx)
    if isinstance(state, CqResult):
        return state

    result = mk_result(_build_runmeta(ctx))
    summary = apply_summary_mapping(result.summary, _summary_common_for_context(ctx))
    findings, sections, summary_updates = _apply_entity_handlers(state)
    summary = apply_summary_mapping(
        summary,
        {
            **summary_updates,
            "files_scanned": len({r.file for r in state.records}),
        },
    )
    result = msgspec.structs.replace(
        result,
        summary=summary,
        key_findings=findings,
        sections=sections,
    )
    result = _maybe_add_entity_explain(state, result)
    result = _finalize_single_scope_summary(ctx, result)
    result = _attach_entity_insight(result, services=ctx.services)
    result = update_result_summary(
        result,
        {"cache_backend": snapshot_backend_metrics(root=ctx.root)},
    )
    return assign_result_finding_ids(result)


def execute_entity_query_from_records(request: EntityQueryRequest) -> CqResult:
    """Execute an entity query using pre-scanned records.

    Returns:
    -------
    CqResult
        Query result with findings and summary metadata.
    """
    ctx = QueryExecutionContext(
        plan=request.plan,
        query=request.query,
        tc=request.tc,
        root=request.root,
        argv=request.argv,
        started_ms=ms(),
        run_id=request.run_id or uuid7_str(),
        services=request.services,
        query_text=request.query_text,
    )
    scan_ctx = _build_scan_context(request.records)
    candidates = _build_entity_candidates(scan_ctx, request.records)
    candidates = _apply_rule_spans(
        ctx,
        request.paths,
        request.scope_globs,
        candidates,
        match_spans=request.match_spans,
    )
    state = EntityExecutionState(
        ctx=ctx,
        paths=request.paths,
        scope_globs=request.scope_globs,
        records=request.records,
        scan=scan_ctx,
        candidates=candidates,
    )
    result = mk_result(_build_runmeta(ctx))
    summary = apply_summary_mapping(result.summary, _summary_common_for_context(ctx))
    findings, sections, summary_updates = _apply_entity_handlers(state, symtable=request.symtable)
    summary = apply_summary_mapping(
        summary,
        {
            **summary_updates,
            "files_scanned": len({r.file for r in state.records}),
        },
    )
    result = msgspec.structs.replace(
        result,
        summary=summary,
        key_findings=findings,
        sections=sections,
    )
    result = _maybe_add_entity_explain(state, result)
    result = _finalize_single_scope_summary(ctx, result)
    result = _attach_entity_insight(result, services=ctx.services)
    result = update_result_summary(
        result,
        {"cache_backend": snapshot_backend_metrics(root=ctx.root)},
    )
    return assign_result_finding_ids(result)


def execute_pattern_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute a pattern-based query using inline ast-grep rules.

    Returns:
    -------
    CqResult
        Query result with findings and summary metadata.
    """
    assert ctx.plan.is_pattern_query, (
        "execute_pattern_query called with an entity query plan; use execute_entity_query instead"
    )
    state = _prepare_pattern_state(ctx)
    if isinstance(state, CqResult):
        return state

    findings, records, _ = _execute_ast_grep_rules(
        state.ctx.plan.sg_rules,
        state.file_result.files,
        state.ctx.root,
        state.ctx.query,
        state.scope_globs,
        state.ctx.run_id,
    )

    result = mk_result(_build_runmeta(ctx))
    summary = apply_summary_mapping(result.summary, _summary_common_for_context(ctx))
    key_findings = list(findings)

    if state.ctx.query.scope_filter and findings:
        enricher = SymtableEnricher(state.ctx.root)
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
            "files_scanned": len({r.file for r in records}),
        },
    )
    result = msgspec.structs.replace(
        result,
        summary=summary,
        key_findings=key_findings,
    )
    result = _maybe_add_pattern_explain(state, result)
    result = _finalize_single_scope_summary(ctx, result)
    result = update_result_summary(
        result,
        {"cache_backend": snapshot_backend_metrics(root=ctx.root)},
    )
    return assign_result_finding_ids(result)


def execute_pattern_query_with_files(request: PatternQueryRequest) -> CqResult:
    """Execute a pattern query using a pre-tabulated file list.

    Returns:
    -------
    CqResult
        Query result with findings and summary metadata.
    """
    ctx = QueryExecutionContext(
        plan=request.plan,
        query=request.query,
        tc=request.tc,
        root=request.root,
        argv=request.argv,
        started_ms=ms(),
        run_id=request.run_id or uuid7_str(),
        services=request.services,
        query_text=request.query_text,
    )
    if not request.files:
        result = _empty_result(ctx, "No files match scope after filtering")
        if request.plan.explain and request.decisions is not None:
            result = update_result_summary(
                result,
                {"file_filters": _serialize_file_filter_decisions(request.decisions)},
            )
        return result

    state = PatternExecutionState(
        ctx=ctx,
        scope_globs=scope_to_globs(request.plan.scope),
        file_result=FileTabulationResult(files=request.files, decisions=request.decisions or []),
    )

    findings, records, _ = _execute_ast_grep_rules(
        state.ctx.plan.sg_rules,
        state.file_result.files,
        state.ctx.root,
        state.ctx.query,
        state.scope_globs,
        state.ctx.run_id,
    )

    result = mk_result(_build_runmeta(ctx))
    summary = apply_summary_mapping(result.summary, _summary_common_for_context(ctx))
    key_findings = list(findings)

    if state.ctx.query.scope_filter and findings:
        enricher = SymtableEnricher(state.ctx.root)
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
            "files_scanned": len({r.file for r in records}),
        },
    )
    result = msgspec.structs.replace(
        result,
        summary=summary,
        key_findings=key_findings,
    )
    result = _maybe_add_pattern_explain(state, result)
    result = _finalize_single_scope_summary(ctx, result)
    result = update_result_summary(
        result,
        {"cache_backend": snapshot_backend_metrics(root=ctx.root)},
    )
    return assign_result_finding_ids(result)


def _process_decorator_query(
    ctx: ScanContext,
    query: Query,
    root: Path,
    def_candidates: list[SgRecord] | tuple[SgRecord, ...] | None = None,
) -> tuple[list[Finding], dict[str, object]]:
    """Process a decorator entity query.

    Returns:
        tuple[list[Finding], dict[str, object]]: Decorator findings and summary metrics.
    """
    from tools.cq.query.enrichment import enrich_with_decorators

    # Look for decorated definitions
    findings: list[Finding] = []

    candidate_records = def_candidates if def_candidates is not None else ctx.def_records
    for def_record in candidate_records:
        # Skip non-function/class definitions
        if def_record.kind not in ENTITY_KINDS.decorator_kinds:
            continue

        # Check if matches name pattern
        if query.name and not _matches_name(def_record, query.name):
            continue

        # Read source to check for decorators
        file_path = root / def_record.file
        try:
            source = file_path.read_text(encoding="utf-8")
        except OSError:
            logger.warning("Skipping unreadable file during decorator query: %s", file_path)
            continue

        decorator_info = enrich_with_decorators(
            Finding(
                category="definition",
                message="",
                anchor=Anchor(file=def_record.file, line=def_record.start_line),
            ),
            source,
        )

        decorators_value = decorator_info.get("decorators", [])
        decorators: list[str] = (
            [str(item) for item in decorators_value] if isinstance(decorators_value, list) else []
        )
        count = len(decorators)

        # Apply decorator filter if present
        if query.decorator_filter:
            # Filter by decorated_by
            if (
                query.decorator_filter.decorated_by
                and query.decorator_filter.decorated_by not in decorators
            ):
                continue

            # Filter by count
            if (
                query.decorator_filter.decorator_count_min is not None
                and count < query.decorator_filter.decorator_count_min
            ):
                continue
            if (
                query.decorator_filter.decorator_count_max is not None
                and count > query.decorator_filter.decorator_count_max
            ):
                continue

        # Only include if has decorators (for entity=decorator queries)
        if count > 0:
            finding = _def_to_finding(def_record, list(ctx.calls_by_def.get(def_record, ())))
            details = finding.details.with_entry("decorators", decorators)
            details = details.with_entry("decorator_count", count)
            finding = msgspec.structs.replace(finding, details=details)
            findings.append(finding)

    return findings, {
        "total_defs": len(ctx.def_records),
        "matches": len(findings),
    }


def _process_call_query(
    ctx: ScanContext,
    query: Query,
    root: Path,
) -> tuple[list[Finding], dict[str, object]]:
    """Process a callsite entity query.

    Returns:
        tuple[list[Finding], dict[str, object]]: Callsite findings and summary metrics.
    """
    matching_calls = _filter_to_matching(list(ctx.call_records), query)
    call_contexts: list[tuple[SgRecord, SgRecord | None]] = []
    for call_record in matching_calls:
        containing = ctx.file_index.find_containing(call_record)
        call_contexts.append((call_record, containing))

    containing_defs = [containing for _, containing in call_contexts if containing is not None]
    evidence_map = _build_def_evidence_map(containing_defs, root)

    findings: list[Finding] = []
    for call_record, containing in call_contexts:
        details: dict[str, object] = {}
        call_target = _extract_call_target(call_record)
        if containing is not None:
            caller_name = extract_def_name(containing) or "<module>"
            details["caller"] = caller_name
            evidence = evidence_map.get(_record_key(containing))
            _apply_call_evidence(details, evidence, call_target)
        finding = _call_to_finding(call_record, extra_details=details)
        findings.append(finding)

    return findings, {
        "total_calls": len(ctx.call_records),
        "matches": len(findings),
    }


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
    "execute_entity_query_from_records",
    "execute_pattern_query_with_files",
    "execute_plan",
    "rg_files_with_matches",
]
