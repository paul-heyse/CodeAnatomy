"""Query executor for cq queries.

Executes ToolPlans and returns CqResult objects.
"""
# ruff: noqa: DOC201,C901,PLR0914,PLR0915

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec
from ast_grep_py import Config, Rule, SgRoot

from tools.cq.astgrep.sgpy_scanner import SgRecord, group_records_by_file
from tools.cq.core.locations import SourceSpan
from tools.cq.core.multilang_orchestrator import (
    execute_by_language_scope,
    merge_language_cq_results,
    runmeta_for_scope_merge,
)
from tools.cq.core.multilang_summary import (
    build_multilang_summary,
    partition_stats_from_result_summary,
)
from tools.cq.core.requests import MergeResultsRequest, SummaryBuildRequest
from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    RunMeta,
    Section,
    mk_result,
    ms,
)
from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    build_detail_payload,
    build_score_details,
)
from tools.cq.core.serialization import to_builtins
from tools.cq.query.enrichment import SymtableEnricher, filter_by_scope
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import (
    DefQueryContext,
    EntityQueryRequest,
    PatternQueryRequest,
)
from tools.cq.query.language import (
    DEFAULT_QUERY_LANGUAGE,
    QueryLanguage,
    QueryLanguageScope,
    file_extensions_for_language,
    file_extensions_for_scope,
)
from tools.cq.query.planner import AstGrepRule, ToolPlan, scope_to_globs, scope_to_paths
from tools.cq.query.sg_parser import filter_records_by_kind, sg_scan
from tools.cq.search import SearchLimits, find_files_with_pattern
from tools.cq.search.multilang_diagnostics import (
    build_capability_diagnostics,
    build_cross_language_diagnostics,
    build_language_capabilities,
    diagnostics_to_summary_payload,
    features_from_query,
    is_python_oriented_query_ir,
)
from tools.cq.utils.interval_index import FileIntervalIndex, IntervalIndex

if TYPE_CHECKING:
    from ast_grep_py import SgNode

    from tools.cq.core.toolchain import Toolchain
    from tools.cq.query.ir import MetaVarCapture, MetaVarFilter
from tools.cq.index.files import FileTabulationResult, build_repo_file_index, tabulate_files
from tools.cq.index.repo import resolve_repo_context
from tools.cq.query.ir import Query, Scope

_ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES = 50


@dataclass
class ScanContext:
    """Bundled context from ast-grep scan for query processing."""

    def_records: list[SgRecord]
    call_records: list[SgRecord]
    interval_index: IntervalIndex[SgRecord]
    file_index: FileIntervalIndex
    calls_by_def: dict[SgRecord, list[SgRecord]]
    all_records: list[SgRecord]


@dataclass
class EntityCandidates:
    """Candidate record buckets for entity queries."""

    def_records: list[SgRecord]
    import_records: list[SgRecord]
    call_records: list[SgRecord]


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


@dataclass(frozen=True)
class AstGrepExecutionContext:
    """Inputs for executing inline ast-grep rules."""

    rules: tuple[AstGrepRule, ...]
    paths: list[Path]
    root: Path
    query: Query | None = None
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE


@dataclass
class AstGrepExecutionState:
    """Mutable state for ast-grep rule execution."""

    findings: list[Finding]
    records: list[SgRecord]
    raw_matches: list[dict[str, object]]


@dataclass(frozen=True)
class AstGrepRuleContext:
    """Per-rule execution context for ast-grep-py scanning."""

    node: SgNode
    rule: AstGrepRule
    rel_path: str
    rule_id: str


@dataclass(frozen=True)
class AstGrepMatchSpan:
    """Captured match span for relational filtering."""

    span: SourceSpan
    match: SgNode

    @property
    def file(self) -> str:
        """Return the file path for this match span."""
        return self.span.file

    @property
    def start_line(self) -> int:
        """Return the starting line for this match span."""
        return self.span.start_line

    @property
    def end_line(self) -> int:
        """Return the ending line for this match span."""
        return self.span.end_line if self.span.end_line is not None else self.span.start_line


_COMMON_METAVAR_NAMES: tuple[str, ...] = (
    "FUNC",
    "F",
    "CLASS",
    "METHOD",
    "M",
    "X",
    "Y",
    "Z",
    "A",
    "B",
    "OBJ",
    "ATTR",
    "VAL",
    "E",
    "NAME",
    "MODULE",
    "ARGS",
    "KWARGS",
    "COND",
    "VAR",
    "P",
    "L",
    "DECORATOR",
)


def _build_runmeta(ctx: QueryExecutionContext) -> RunMeta:
    run_ctx = RunContext.from_parts(
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.tc,
        started_ms=ctx.started_ms,
    )
    return run_ctx.to_runmeta("q")


def _query_mode(query: Query) -> str:
    return "pattern" if query.is_pattern_query else "entity"


def _query_text(query: Query) -> str:
    if query.pattern_spec is not None:
        return query.pattern_spec.pattern
    parts: list[str] = []
    if query.entity is not None:
        parts.append(f"entity={query.entity}")
    if query.name:
        parts.append(f"name={query.name}")
    return " ".join(parts) if parts else "q"


def _summary_common_for_query(
    query: Query,
    *,
    query_text: str | None = None,
) -> dict[str, object]:
    text = (
        query_text.strip()
        if isinstance(query_text, str) and query_text.strip()
        else _query_text(query)
    )
    common: dict[str, object] = {
        "query": text,
        "mode": _query_mode(query),
        "pyrefly_overview": dict[str, object](),
        "pyrefly_telemetry": {
            "attempted": 0,
            "applied": 0,
            "failed": 0,
            "skipped": 0,
            "timed_out": 0,
        },
        "rust_lsp_telemetry": {
            "attempted": 0,
            "applied": 0,
            "failed": 0,
            "skipped": 0,
            "timed_out": 0,
        },
        "lsp_advanced_planes": dict[str, object](),
        "pyrefly_diagnostics": list[dict[str, object]](),
    }
    if query.pattern_spec is not None:
        common["pattern"] = query.pattern_spec.pattern
        if query.pattern_spec.context is not None:
            common["pattern_context"] = query.pattern_spec.context
        if query.pattern_spec.selector is not None:
            common["pattern_selector"] = query.pattern_spec.selector
    return common


def _summary_common_for_context(ctx: QueryExecutionContext) -> dict[str, object]:
    return _summary_common_for_query(ctx.query, query_text=ctx.query_text)


def _finalize_single_scope_summary(ctx: QueryExecutionContext, result: CqResult) -> None:
    if ctx.query.lang_scope == "auto":
        return
    lang = ctx.query.primary_language
    common = dict(result.summary)
    partition = partition_stats_from_result_summary(
        result.summary,
        fallback_matches=_count_result_matches(result),
    )
    result.summary = build_multilang_summary(
        SummaryBuildRequest(
            common=common,
            lang_scope=ctx.query.lang_scope,
            language_order=(lang,),
            languages={lang: partition},
            cross_language_diagnostics=[],
            language_capabilities=build_language_capabilities(lang_scope=ctx.query.lang_scope),
        )
    )


def _empty_result(ctx: QueryExecutionContext, message: str) -> CqResult:
    result = mk_result(_build_runmeta(ctx))
    result.summary.update(_summary_common_for_context(ctx))
    result.summary["error"] = message
    _finalize_single_scope_summary(ctx, result)
    return result


def _resolve_entity_paths(
    ctx: QueryExecutionContext,
) -> tuple[list[Path], list[str] | None, CqResult | None]:
    plan = ctx.plan
    paths = scope_to_paths(plan.scope, ctx.root)
    if not paths:
        return [], None, _empty_result(ctx, "No files match scope")
    return paths, scope_to_globs(plan.scope), None


def _scan_entity_records(
    ctx: QueryExecutionContext,
    paths: list[Path],
    scope_globs: list[str] | None,
) -> list[SgRecord]:
    return sg_scan(
        paths=paths,
        record_types=ctx.plan.sg_record_types,
        root=ctx.root,
        globs=scope_globs,
        lang=ctx.plan.lang,
    )


def _build_scan_context(records: list[SgRecord]) -> ScanContext:
    def_records = filter_records_by_kind(records, "def")
    interval_index = IntervalIndex.from_records(def_records)
    file_index = FileIntervalIndex.from_records(def_records)
    call_records = filter_records_by_kind(records, "call")
    calls_by_def = assign_calls_to_defs(interval_index, call_records)
    return ScanContext(
        def_records=def_records,
        call_records=call_records,
        interval_index=interval_index,
        file_index=file_index,
        calls_by_def=calls_by_def,
        all_records=records,
    )


def _build_entity_candidates(scan: ScanContext, records: list[SgRecord]) -> EntityCandidates:
    return EntityCandidates(
        def_records=scan.def_records,
        import_records=filter_records_by_kind(records, "import"),
        call_records=scan.call_records,
    )


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

    def_records = candidates.def_records
    import_records = candidates.import_records
    call_records = candidates.call_records

    if query.entity in {"function", "class", "method", "decorator"}:
        def_records = _filter_records_by_spans(def_records, match_spans)
    elif query.entity == "import":
        import_records = _filter_records_by_spans(import_records, match_spans)
    elif query.entity == "callsite":
        call_records = _filter_records_by_spans(call_records, match_spans)

    return EntityCandidates(
        def_records=def_records,
        import_records=import_records,
        call_records=call_records,
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
    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    return tabulate_files(
        repo_index,
        paths,
        scope_globs,
        extensions=file_extensions_for_language(lang),
        explain=explain,
    )


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
            result.summary["file_filters"] = [
                to_builtins(decision) for decision in file_result.decisions
            ]
        return result

    return PatternExecutionState(
        ctx=ctx,
        scope_globs=scope_globs,
        file_result=file_result,
    )


def _apply_entity_handlers(
    state: EntityExecutionState,
    result: CqResult,
    *,
    symtable: SymtableEnricher | None = None,
) -> None:
    query = state.ctx.query
    root = state.ctx.root
    candidates = state.candidates
    def_ctx = DefQueryContext(state=state, result=result, symtable=symtable)

    if query.entity == "import":
        _process_import_query(
            candidates.import_records,
            query,
            result,
            root,
            symtable=symtable,
        )
    elif query.entity == "decorator":
        _process_decorator_query(state.scan, query, result, root, candidates.def_records)
    elif query.entity == "callsite":
        _process_call_query(state.scan, query, result, root)
    else:
        _process_def_query(def_ctx, query, candidates.def_records)


def _maybe_add_entity_explain(state: EntityExecutionState, result: CqResult) -> None:
    plan = state.ctx.plan
    if not plan.explain:
        return
    result.summary["plan"] = {
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
    result.summary["file_filters"] = [to_builtins(decision) for decision in file_result.decisions]


def _maybe_add_pattern_explain(state: PatternExecutionState, result: CqResult) -> None:
    plan = state.ctx.plan
    query = state.ctx.query
    if not plan.explain:
        return
    result.summary["plan"] = {
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
    result.summary["file_filters"] = [
        to_builtins(decision) for decision in state.file_result.decisions
    ]


def execute_plan(
    plan: ToolPlan,
    query: Query,
    tc: Toolchain,
    root: Path,
    argv: list[str] | None = None,
    query_text: str | None = None,
) -> CqResult:
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
    if query.lang_scope == "auto":
        return _execute_auto_scope_plan(
            query,
            tc=tc,
            root=root,
            argv=argv or [],
            query_text=query_text,
        )

    ctx = QueryExecutionContext(
        plan=plan,
        query=query,
        tc=tc,
        root=root,
        argv=argv or [],
        started_ms=ms(),
        query_text=query_text,
    )
    return _execute_single_context(ctx)


def _execute_single_context(ctx: QueryExecutionContext) -> CqResult:
    if ctx.plan.is_pattern_query:
        return _execute_pattern_query(ctx)
    return _execute_entity_query(ctx)


def _execute_auto_scope_plan(
    query: Query,
    *,
    tc: Toolchain,
    root: Path,
    argv: list[str],
    query_text: str | None = None,
) -> CqResult:

    results = execute_by_language_scope(
        query.lang_scope,
        lambda lang: _run_scoped_auto_query(
            query=query,
            lang=lang,
            tc=tc,
            root=root,
            argv=argv,
        ),
    )
    return _merge_auto_scope_results(
        query,
        results,
        root=root,
        argv=argv,
        tc=tc,
        query_text=query_text,
    )


def _run_scoped_auto_query(
    *,
    query: Query,
    lang: QueryLanguage,
    tc: Toolchain,
    root: Path,
    argv: list[str],
) -> CqResult:
    from tools.cq.query.planner import compile_query

    scoped_query = msgspec.structs.replace(query, lang_scope=cast("QueryLanguageScope", lang))
    scoped_plan = compile_query(scoped_query)
    scoped_ctx = QueryExecutionContext(
        plan=scoped_plan,
        query=scoped_query,
        tc=tc,
        root=root,
        argv=argv,
        started_ms=ms(),
    )
    return _execute_single_context(scoped_ctx)


def _count_result_matches(result: CqResult | None) -> int:
    if result is None:
        return 0
    summary_matches = result.summary.get("matches")
    if isinstance(summary_matches, int):
        return summary_matches
    summary_total = result.summary.get("total_matches")
    if isinstance(summary_total, int):
        return summary_total
    return len(result.key_findings)


def _merge_auto_scope_results(
    query: Query,
    results: dict[QueryLanguage, CqResult],
    *,
    root: Path,
    argv: list[str],
    tc: Toolchain,
    query_text: str | None = None,
) -> CqResult:
    diagnostics = build_cross_language_diagnostics(
        lang_scope=query.lang_scope,
        python_matches=_count_result_matches(results.get("python")),
        rust_matches=_count_result_matches(results.get("rust")),
        python_oriented=is_python_oriented_query_ir(query),
    )
    capability_diagnostics = build_capability_diagnostics(
        features=features_from_query(query),
        lang_scope=query.lang_scope,
    )
    diagnostics = list(diagnostics) + capability_diagnostics
    diagnostic_payloads = diagnostics_to_summary_payload(diagnostics)
    language_capabilities = build_language_capabilities(lang_scope=query.lang_scope)
    run = runmeta_for_scope_merge(
        macro="q",
        root=root,
        argv=argv,
        tc=tc,
    )
    merged = merge_language_cq_results(
        MergeResultsRequest(
            scope=query.lang_scope,
            results=results,
            run=run,
            diagnostics=diagnostics,
            diagnostic_payloads=diagnostic_payloads,
            language_capabilities=language_capabilities,
            summary_common=_summary_common_for_query(query, query_text=query_text),
        )
    )
    if not query.is_pattern_query:
        if "front_door_insight" not in merged.summary:
            _attach_entity_insight(merged)
        else:
            _mark_entity_insight_partial_from_summary(merged)
    return merged


def _missing_languages_from_summary(summary: dict[str, object]) -> list[str]:
    languages = summary.get("languages")
    if not isinstance(languages, dict):
        return []
    missing: list[str] = []
    for lang, payload in languages.items():
        lang_name = str(lang)
        if not isinstance(payload, dict):
            missing.append(lang_name)
            continue
        total = payload.get("total_matches")
        if isinstance(total, int):
            if total <= 0:
                missing.append(lang_name)
            continue
        matches = payload.get("matches")
        if isinstance(matches, int) and matches <= 0:
            missing.append(lang_name)
    return missing


def _mark_entity_insight_partial_from_summary(result: CqResult) -> None:
    from tools.cq.core.front_door_insight import (
        coerce_front_door_insight,
        mark_partial_for_missing_languages,
        to_public_front_door_insight_dict,
    )

    insight = coerce_front_door_insight(result.summary.get("front_door_insight"))
    if insight is None:
        return
    lang_scope = result.summary.get("lang_scope")
    if isinstance(lang_scope, str) and lang_scope == "auto":
        missing = _missing_languages_from_summary(result.summary)
        if missing:
            insight = mark_partial_for_missing_languages(insight, missing_languages=missing)
    result.summary["front_door_insight"] = to_public_front_door_insight_dict(insight)


def _attach_entity_insight(result: CqResult) -> None:  # noqa: PLR0912
    """Build and attach front-door insight card to entity result."""
    from tools.cq.core.front_door_insight import (
        InsightBudgetV1,
        InsightConfidenceV1,
        InsightDegradationV1,
        InsightNeighborhoodV1,
        InsightRiskCountersV1,
        InsightSliceV1,
        augment_insight_with_lsp,
        build_entity_insight,
        mark_partial_for_missing_languages,
        risk_from_counters,
        to_public_front_door_insight_dict,
    )
    from tools.cq.core.snb_schema import SemanticNodeRefV1
    from tools.cq.search.lsp_contract_state import LspProvider, derive_lsp_contract_state
    from tools.cq.search.lsp_front_door_adapter import (
        enrich_with_language_lsp,
        infer_language_for_path,
        lsp_runtime_enabled,
        provider_for_language,
    )

    mode_value = result.summary.get("mode")
    if isinstance(mode_value, str) and mode_value == "pattern":
        return

    definition_findings = [f for f in result.key_findings if f.category == "definition"]
    primary_target = definition_findings[0] if definition_findings else None
    candidates = definition_findings[:3]

    def _detail_int(finding: Finding, key: str) -> int:
        value = finding.details.get(key)
        return value if isinstance(value, int) else 0

    def _preview_node(finding: Finding, suffix: str, label: str | None = None) -> SemanticNodeRefV1:
        anchor = finding.anchor
        name = str(finding.details.get("name") or finding.message)
        file_path = anchor.file if anchor else ""
        line = anchor.line if anchor else 0
        node_id = f"entity:{suffix}:{file_path}:{line}:{name}"
        return SemanticNodeRefV1(
            node_id=node_id,
            kind="definition",
            name=name,
            display_label=label or name,
            file_path=file_path,
        )

    callers_total = sum(_detail_int(finding, "caller_count") for finding in candidates)
    caller_preview = tuple(
        _preview_node(finding, "caller")
        for finding in candidates
        if _detail_int(finding, "caller_count") > 0
    )
    callees_total = sum(_detail_int(finding, "callee_count") for finding in candidates)
    callee_preview = tuple(
        _preview_node(finding, "callee")
        for finding in candidates
        if _detail_int(finding, "callee_count") > 0
    )
    scope_values = {
        str(finding.details.get("enclosing_scope"))
        for finding in candidates
        if isinstance(finding.details.get("enclosing_scope"), str)
        and str(finding.details.get("enclosing_scope")) not in {"", "<module>"}
    }
    scope_preview = tuple(
        SemanticNodeRefV1(
            node_id=f"scope:{value}",
            kind="scope",
            name=value,
            display_label=value,
            file_path="",
        )
        for value in sorted(scope_values)
    )
    neighborhood = InsightNeighborhoodV1(
        callers=InsightSliceV1(
            total=callers_total,
            preview=caller_preview,
            availability="partial" if callers_total > 0 else "unavailable",
            source="heuristic",
        ),
        callees=InsightSliceV1(
            total=callees_total,
            preview=callee_preview,
            availability="partial" if callees_total > 0 else "unavailable",
            source="heuristic",
        ),
        references=InsightSliceV1(availability="unavailable", source="none"),
        hierarchy_or_scope=InsightSliceV1(
            total=len(scope_preview),
            preview=scope_preview,
            availability="partial" if scope_preview else "unavailable",
            source="heuristic",
        ),
    )
    counters = InsightRiskCountersV1(
        callers=callers_total,
        callees=callees_total,
        closure_capture_count=len(scope_preview),
    )
    risk = risk_from_counters(counters)

    confidence = InsightConfidenceV1(evidence_kind="resolved_ast", score=0.8, bucket="high")
    for finding in candidates:
        score = finding.details.score
        if score is None:
            continue
        confidence = InsightConfidenceV1(
            evidence_kind=score.evidence_kind or confidence.evidence_kind,
            score=float(score.confidence_score) if score.confidence_score is not None else 0.8,
            bucket=score.confidence_bucket or confidence.bucket,
        )
        break

    scope_filter_status = "none"
    dropped = result.summary.get("dropped_by_scope")
    if isinstance(dropped, dict) and dropped:
        scope_filter_status = "dropped"
    notes: list[str] = []
    if isinstance(dropped, dict) and dropped:
        notes.append(f"dropped_by_scope={dropped}")
    degradation = InsightDegradationV1(
        lsp="skipped",
        scan=(
            "timed_out"
            if bool(result.summary.get("timed_out"))
            else "truncated"
            if bool(result.summary.get("truncated"))
            else "ok"
        ),
        scope_filter=scope_filter_status,
        notes=tuple(notes),
    )
    insight = build_entity_insight(
        summary=result.summary,
        primary_target=primary_target,
        neighborhood=neighborhood,
        risk=risk,
        confidence=confidence,
        degradation=degradation,
        budget=InsightBudgetV1(top_candidates=3, preview_per_slice=5, lsp_targets=3),
    )

    lsp_attempted = 0
    lsp_applied = 0
    lsp_failed = 0
    lsp_timed_out = 0
    lsp_provider: LspProvider = "none"
    py_attempted = 0
    py_applied = 0
    py_failed = 0
    py_timed_out = 0
    rust_attempted = 0
    rust_applied = 0
    rust_failed = 0
    rust_timed_out = 0
    lsp_reasons: list[str] = []
    summary_matches = result.summary.get("matches")
    match_count = summary_matches if isinstance(summary_matches, int) else len(result.key_findings)
    runtime_lsp_enabled = lsp_runtime_enabled()
    run_entity_lsp = runtime_lsp_enabled and (
        match_count <= _ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES
    )
    if run_entity_lsp:
        for finding in candidates:
            if finding.anchor is None:
                continue
            target_file = Path(result.run.root) / finding.anchor.file
            target_language = infer_language_for_path(target_file)
            if target_language not in {"python", "rust"}:
                lsp_reasons.append("provider_unavailable")
                continue
            if lsp_provider == "none":
                lsp_provider = provider_for_language(target_language)
            lsp_attempted += 1
            if target_language == "python":
                py_attempted += 1
            else:
                rust_attempted += 1
            payload, timed_out = enrich_with_language_lsp(
                language=target_language,
                mode="entity",
                root=Path(result.run.root),
                file_path=target_file,
                line=max(1, int(finding.anchor.line)),
                col=int(finding.anchor.col or 0),
                symbol_hint=(
                    str(finding.details.get("name"))
                    if isinstance(finding.details.get("name"), str)
                    else None
                ),
            )
            lsp_timed_out += int(timed_out)
            if target_language == "python":
                py_timed_out += int(timed_out)
            else:
                rust_timed_out += int(timed_out)
            if payload is None:
                lsp_failed += 1
                if target_language == "python":
                    py_failed += 1
                else:
                    rust_failed += 1
                lsp_reasons.append("request_timeout" if timed_out else "request_failed")
                continue
            lsp_applied += 1
            if target_language == "python":
                py_applied += 1
            else:
                rust_applied += 1
            insight = augment_insight_with_lsp(insight, payload, preview_per_slice=5)
            advanced_planes = payload.get("advanced_planes")
            if isinstance(advanced_planes, dict):
                result.summary["lsp_advanced_planes"] = dict(advanced_planes)
    elif not runtime_lsp_enabled:
        for finding in candidates:
            if finding.anchor is None:
                continue
            target_file = Path(result.run.root) / finding.anchor.file
            target_language = infer_language_for_path(target_file)
            if target_language in {"python", "rust"}:
                lsp_provider = provider_for_language(target_language)
                lsp_reasons.append("not_attempted_runtime_disabled")
                break
    else:
        for finding in candidates:
            if finding.anchor is None:
                continue
            target_file = Path(result.run.root) / finding.anchor.file
            target_language = infer_language_for_path(target_file)
            if target_language in {"python", "rust"}:
                lsp_provider = provider_for_language(target_language)
                lsp_reasons.append("not_attempted_by_budget")
                break

    lsp_available = lsp_provider != "none"
    if lsp_provider == "none":
        lsp_reasons.append("provider_unavailable")
    elif (
        lsp_attempted <= 0
        and "not_attempted_by_budget" not in lsp_reasons
        and "not_attempted_runtime_disabled" not in lsp_reasons
    ):
        lsp_reasons.append("not_attempted_by_design")
    lsp_state = derive_lsp_contract_state(
        provider=lsp_provider,
        available=lsp_available,
        attempted=lsp_attempted,
        applied=lsp_applied,
        failed=max(lsp_failed, lsp_attempted - lsp_applied),
        timed_out=lsp_timed_out,
        reasons=tuple(dict.fromkeys(lsp_reasons)),
    )
    insight = msgspec.structs.replace(
        insight,
        degradation=msgspec.structs.replace(
            insight.degradation,
            lsp=lsp_state.status,
            notes=tuple(dict.fromkeys([*insight.degradation.notes, *lsp_state.reasons])),
        ),
    )
    missing = _missing_languages_from_summary(result.summary)
    if missing:
        insight = mark_partial_for_missing_languages(insight, missing_languages=missing)

    result.summary["pyrefly_telemetry"] = {
        "attempted": py_attempted,
        "applied": py_applied,
        "failed": max(py_failed, py_attempted - py_applied),
        "skipped": 0,
        "timed_out": py_timed_out,
    }
    result.summary["rust_lsp_telemetry"] = {
        "attempted": rust_attempted,
        "applied": rust_applied,
        "failed": max(rust_failed, rust_attempted - rust_applied),
        "skipped": 0,
        "timed_out": rust_timed_out,
    }
    result.summary["front_door_insight"] = to_public_front_door_insight_dict(insight)


def _execute_entity_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute an entity-based query.

    Returns:
    -------
    CqResult
        Query result with findings and summary metadata.
    """
    state = _prepare_entity_state(ctx)
    if isinstance(state, CqResult):
        return state

    result = mk_result(_build_runmeta(ctx))
    result.summary.update(_summary_common_for_context(ctx))
    _apply_entity_handlers(state, result)
    result.summary["files_scanned"] = len({r.file for r in state.records})
    _maybe_add_entity_explain(state, result)
    _finalize_single_scope_summary(ctx, result)
    _attach_entity_insight(result)
    return result


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
    result.summary.update(_summary_common_for_context(ctx))
    _apply_entity_handlers(state, result, symtable=request.symtable)
    result.summary["files_scanned"] = len({r.file for r in state.records})
    _maybe_add_entity_explain(state, result)
    _finalize_single_scope_summary(ctx, result)
    _attach_entity_insight(result)
    return result


def _execute_pattern_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute a pattern-based query using inline ast-grep rules.

    Returns:
    -------
    CqResult
        Query result with findings and summary metadata.
    """
    state = _prepare_pattern_state(ctx)
    if isinstance(state, CqResult):
        return state

    findings, records, _ = _execute_ast_grep_rules(
        state.ctx.plan.sg_rules,
        state.file_result.files,
        state.ctx.root,
        state.ctx.query,
        None,
    )

    result = mk_result(_build_runmeta(ctx))
    result.summary.update(_summary_common_for_context(ctx))
    result.key_findings.extend(findings)

    if state.ctx.query.scope_filter and findings:
        enricher = SymtableEnricher(state.ctx.root)
        result.key_findings = filter_by_scope(
            result.key_findings,
            state.ctx.query.scope_filter,
            enricher,
            records,
        )

    if state.ctx.query.limit and len(result.key_findings) > state.ctx.query.limit:
        result.key_findings = result.key_findings[: state.ctx.query.limit]

    result.summary["matches"] = len(result.key_findings)
    result.summary["files_scanned"] = len({r.file for r in records})
    _maybe_add_pattern_explain(state, result)
    _finalize_single_scope_summary(ctx, result)
    return result


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
        query_text=request.query_text,
    )
    if not request.files:
        result = _empty_result(ctx, "No files match scope after filtering")
        if request.plan.explain and request.decisions is not None:
            result.summary["file_filters"] = [
                to_builtins(decision) for decision in request.decisions
            ]
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
        None,
    )

    result = mk_result(_build_runmeta(ctx))
    result.summary.update(_summary_common_for_context(ctx))
    result.key_findings.extend(findings)

    if state.ctx.query.scope_filter and findings:
        enricher = SymtableEnricher(state.ctx.root)
        result.key_findings = filter_by_scope(
            result.key_findings,
            state.ctx.query.scope_filter,
            enricher,
            records,
        )

    if state.ctx.query.limit and len(result.key_findings) > state.ctx.query.limit:
        result.key_findings = result.key_findings[: state.ctx.query.limit]

    result.summary["matches"] = len(result.key_findings)
    result.summary["files_scanned"] = len({r.file for r in records})
    _maybe_add_pattern_explain(state, result)
    _finalize_single_scope_summary(ctx, result)
    return result


def _execute_ast_grep_rules(
    rules: tuple[AstGrepRule, ...],
    paths: list[Path],
    root: Path,
    query: Query | None = None,
    _globs: list[str] | None = None,
) -> tuple[list[Finding], list[SgRecord], list[dict[str, object]]]:
    """Execute ast-grep rules using ast-grep-py and return findings.

    Parameters
    ----------
    rules
        ast-grep rules to execute
    paths
        Paths to scan
    root
        Repository root
    query
        Optional query for metavar filtering
    _globs
        Optional glob filters (not used with ast-grep-py, filtering done upstream)

    Returns:
    -------
    tuple[list[Finding], list[SgRecord], list[dict[str, object]]]
        Findings, underlying records, and raw match data.
    """
    if not rules:
        return [], [], []
    ctx = AstGrepExecutionContext(
        rules=rules,
        paths=paths,
        root=root,
        query=query,
        lang=query.primary_language if query is not None else DEFAULT_QUERY_LANGUAGE,
    )
    state = AstGrepExecutionState(findings=[], records=[], raw_matches=[])
    _run_ast_grep(ctx, state)
    return state.findings, state.records, state.raw_matches


def _run_ast_grep(ctx: AstGrepExecutionContext, state: AstGrepExecutionState) -> None:
    for file_path in ctx.paths:
        _process_ast_grep_file(ctx, state, file_path)


def _process_ast_grep_file(
    ctx: AstGrepExecutionContext,
    state: AstGrepExecutionState,
    file_path: Path,
) -> None:
    try:
        src = file_path.read_text(encoding="utf-8")
    except OSError:
        return

    sg_root = SgRoot(src, ctx.lang)
    node = sg_root.root()
    rel_path = _normalize_match_file(str(file_path), ctx.root)

    for idx, rule in enumerate(ctx.rules):
        rule_ctx = AstGrepRuleContext(
            node=node,
            rule=rule,
            rel_path=rel_path,
            rule_id=f"pattern_{idx}",
        )
        _process_ast_grep_rule(ctx, state, rule_ctx)


def _process_ast_grep_rule(
    ctx: AstGrepExecutionContext,
    state: AstGrepExecutionState,
    rule_ctx: AstGrepRuleContext,
) -> None:
    for match in _iter_rule_matches(rule_ctx.node, rule_ctx.rule):
        match_data = _build_match_data(
            match,
            rule_id=rule_ctx.rule_id,
            rel_path=rule_ctx.rel_path,
        )
        state.raw_matches.append(match_data)
        if not _match_passes_filters(ctx, match):
            continue
        finding, record = _match_to_finding(match_data)
        if finding:
            _apply_metavar_details(ctx, match, finding)
            state.findings.append(finding)
        if record:
            state.records.append(record)


def _iter_rule_matches(node: SgNode, rule: AstGrepRule) -> list[SgNode]:
    pattern = rule.pattern
    if not pattern or pattern in {"$FUNC", "$METHOD", "$CLASS"}:
        if rule.kind:
            return list(node.find_all(kind=rule.kind))
        return []
    return list(node.find_all(pattern=pattern))


def _build_match_data(match: SgNode, *, rule_id: str, rel_path: str) -> dict[str, object]:
    range_obj = match.range()
    return {
        "ruleId": rule_id,
        "file": rel_path,
        "text": match.text(),
        "range": {
            "start": {"line": range_obj.start.line, "column": range_obj.start.column},
            "end": {"line": range_obj.end.line, "column": range_obj.end.column},
        },
        "metaVariables": _extract_match_metavars(match),
    }


def _match_passes_filters(ctx: AstGrepExecutionContext, match: SgNode) -> bool:
    from tools.cq.query.metavar import apply_metavar_filters

    if ctx.query and ctx.query.metavar_filters:
        captures = _parse_sgpy_metavariables(match)
        return apply_metavar_filters(captures, ctx.query.metavar_filters)
    return True


def _apply_metavar_details(
    ctx: AstGrepExecutionContext,
    match: SgNode,
    finding: Finding,
) -> None:
    if ctx.query and ctx.query.metavar_filters:
        captures = _extract_match_metavars(match)
        finding.details["metavar_captures"] = captures


def _extract_match_metavars(match: SgNode) -> dict[str, str]:
    """Extract metavariable captures from an ast-grep-py match.

    Parameters
    ----------
    match
        ast-grep-py SgNode match.

    Returns:
    -------
    dict[str, str]
        Dictionary of metavariable name to captured text.
    """
    metavars: dict[str, str] = {}
    for bare_name in _COMMON_METAVAR_NAMES:
        captured = match.get_match(bare_name)
        if captured is None:
            continue
        text = captured.text()
        # Keep both bare and `$`-prefixed keys for output compatibility.
        metavars[bare_name] = text
        metavars[f"${bare_name}"] = text
    return metavars


def _parse_sgpy_metavariables(match: SgNode) -> dict[str, MetaVarCapture]:
    """Parse metavariables from ast-grep-py match for filter application.

    Parameters
    ----------
    match
        ast-grep-py SgNode match.

    Returns:
    -------
    dict[str, object]
        Dictionary of metavariable info for filtering.
    """
    from tools.cq.query.ir import MetaVarCapture

    result: dict[str, MetaVarCapture] = {}
    for bare_name in _COMMON_METAVAR_NAMES:
        captured = match.get_match(bare_name)
        if captured is None:
            continue
        result[bare_name] = MetaVarCapture(name=bare_name, kind="single", text=captured.text())
    return result


def _coerce_int(value: object) -> int:
    if isinstance(value, int):
        return value
    return 0


def _match_to_finding(data: dict[str, object]) -> tuple[Finding | None, SgRecord | None]:
    """Convert ast-grep match to Finding and SgRecord.

    Returns:
    -------
    tuple[Finding | None, SgRecord | None]
        Finding and record for the match when available.
    """
    if "range" not in data or "file" not in data:
        return None, None

    range_data = data["range"]
    if not isinstance(range_data, dict):
        return None, None
    start = cast("dict[str, object]", range_data.get("start", {}))
    end = cast("dict[str, object]", range_data.get("end", {}))
    file_value = data.get("file", "")
    file_name = str(file_value) if file_value is not None else ""

    anchor = Anchor(
        file=file_name,
        line=_coerce_int(start.get("line", 0)) + 1,  # Convert to 1-indexed
        col=_coerce_int(start.get("column", 0)),
        end_line=_coerce_int(end.get("line", 0)) + 1,
        end_col=_coerce_int(end.get("column", 0)),
    )

    finding = Finding(
        category="pattern_match",
        message=str(data.get("message", "Pattern match")),
        anchor=anchor,
        severity="info",
        details=build_detail_payload(
            data={
                "text": data.get("text", ""),
                "rule_id": data.get("ruleId", "pattern_query"),
            }
        ),
    )

    record = SgRecord(
        record="def",  # Default, may not be accurate for all patterns
        kind="pattern_match",
        file=file_name,
        start_line=_coerce_int(start.get("line", 0)) + 1,
        start_col=_coerce_int(start.get("column", 0)),
        end_line=_coerce_int(end.get("line", 0)) + 1,
        end_col=_coerce_int(end.get("column", 0)),
        text=str(data.get("text", "")),
        rule_id=str(data.get("ruleId", "pattern_query")),
    )

    return finding, record


def _collect_match_spans(
    rules: tuple[AstGrepRule, ...],
    paths: list[Path],
    root: Path,
    query: Query,
    globs: list[str] | None,
) -> dict[str, list[tuple[int, int]]]:
    """Collect matched spans for relational constraints using ast-grep-py.

    Returns:
    -------
    dict[str, list[tuple[int, int]]]
        Mapping from file to matched (start_line, end_line) spans.
    """
    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    file_result = tabulate_files(
        repo_index,
        paths,
        globs,
        extensions=file_extensions_for_scope(query.lang_scope),
    )
    matches = _collect_ast_grep_match_spans(
        file_result.files,
        rules,
        root,
        query.primary_language,
    )
    if not matches:
        return {}
    if not query.metavar_filters:
        return _group_match_spans(matches)
    return _filter_match_spans_by_metavars(matches, query.metavar_filters)


def _collect_ast_grep_match_spans(
    files: list[Path],
    rules: tuple[AstGrepRule, ...],
    root: Path,
    lang: QueryLanguage,
) -> list[AstGrepMatchSpan]:
    matches: list[AstGrepMatchSpan] = []
    for file_path in files:
        try:
            src = file_path.read_text(encoding="utf-8")
        except OSError:
            continue
        sg_root = SgRoot(src, lang)
        node = sg_root.root()
        rel_path = _normalize_match_file(str(file_path), root)
        for rule in rules:
            for match in _iter_rule_matches_for_spans(node, rule):
                range_obj = match.range()
                matches.append(
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
    return matches


def _iter_rule_matches_for_spans(node: SgNode, rule: AstGrepRule) -> list[SgNode]:
    if rule.requires_inline_rule():
        rule_config: Config = {"rule": cast("Rule", rule.to_yaml_dict())}
        return list(node.find_all(rule_config))
    return _iter_rule_matches(node, rule)


def _group_match_spans(
    matches: list[AstGrepMatchSpan],
) -> dict[str, list[tuple[int, int]]]:
    spans: dict[str, list[tuple[int, int]]] = {}
    for match in matches:
        spans.setdefault(match.file, []).append((match.start_line, match.end_line))
    return spans


def _filter_match_spans_by_metavars(
    matches: list[AstGrepMatchSpan],
    metavar_filters: tuple[MetaVarFilter, ...],
) -> dict[str, list[tuple[int, int]]]:
    from tools.cq.query.metavar import apply_metavar_filters

    filtered: dict[str, list[tuple[int, int]]] = {}
    for match in matches:
        captures = _parse_sgpy_metavariables(match.match)
        if not apply_metavar_filters(captures, metavar_filters):
            continue
        filtered.setdefault(match.file, []).append((match.start_line, match.end_line))
    return filtered


def _filter_records_by_spans(
    records: list[SgRecord],
    spans: dict[str, list[tuple[int, int]]],
) -> list[SgRecord]:
    """Filter records to those overlapping matched spans.

    Returns:
    -------
    list[SgRecord]
        Records that overlap the provided spans.
    """
    if not spans:
        return records

    filtered: list[SgRecord] = []
    for record in records:
        ranges = spans.get(record.file)
        if not ranges:
            continue
        for start_line, end_line in ranges:
            if start_line <= record.start_line <= end_line:
                filtered.append(record)
                break
    return filtered


def _record_key(record: SgRecord) -> tuple[str, int, int, int, int]:
    """Return a stable key for a record.

    Returns:
    -------
    tuple[str, int, int, int, int]
        Stable key identifying a record location.
    """
    return (
        record.file,
        record.start_line,
        record.start_col,
        record.end_line,
        record.end_col,
    )


def _normalize_match_file(file_path: str, root: Path) -> str:
    """Normalize match paths to repo-relative POSIX strings.

    Returns:
    -------
    str
        Repository-relative POSIX path.
    """
    path = Path(file_path)
    if path.is_absolute():
        try:
            return path.relative_to(root).as_posix()
        except ValueError:
            return file_path
    return path.as_posix()


def _build_def_evidence_map(
    def_records: list[SgRecord],
    root: Path,
) -> dict[tuple[str, int, int, int, int], dict[str, object]]:
    """Build a map of definition records to symtable/bytecode evidence.

    Returns:
    -------
    dict[tuple[str, int, int, int, int], dict[str, object]]
        Evidence details keyed by record location.
    """
    from tools.cq.query.enrichment import BytecodeInfo, SymtableInfo, enrich_records

    unique_records: dict[tuple[str, int, int, int, int], SgRecord] = {}
    for record in def_records:
        unique_records[_record_key(record)] = record

    if not unique_records:
        return {}

    enrichment = enrich_records(list(unique_records.values()), root)
    evidence_map: dict[tuple[str, int, int, int, int], dict[str, object]] = {}

    for record_key, record in unique_records.items():
        location = f"{record.file}:{record.start_line}:{record.start_col}"
        info = enrichment.get(location)
        if not info:
            continue
        details: dict[str, object] = {}
        symtable_info = info.get("symtable_info")
        if isinstance(symtable_info, SymtableInfo):
            details["resolved_globals"] = list(symtable_info.globals_used)
        bytecode_info = info.get("bytecode_info")
        if isinstance(bytecode_info, BytecodeInfo):
            details["bytecode_calls"] = list(bytecode_info.call_functions)
        if details:
            evidence_map[record_key] = details

    return evidence_map


def _apply_call_evidence(
    details: dict[str, object],
    evidence: dict[str, object] | None,
    call_target: str,
) -> None:
    """Attach call evidence details to the finding payload."""
    if not evidence:
        return
    resolved_globals = evidence.get("resolved_globals")
    if isinstance(resolved_globals, list):
        details["resolved_globals"] = resolved_globals
        if call_target:
            details["globals_has_target"] = call_target in resolved_globals

    bytecode_calls = evidence.get("bytecode_calls")
    if isinstance(bytecode_calls, list):
        details["bytecode_calls"] = bytecode_calls
        if call_target:
            details["bytecode_has_target"] = call_target in bytecode_calls


def _process_import_query(
    import_records: list[SgRecord],
    query: Query,
    result: CqResult,
    root: Path,
    *,
    symtable: SymtableEnricher | None = None,
) -> None:
    """Process an import entity query."""
    matching_imports = _filter_to_matching(import_records, query)

    for import_record in matching_imports:
        finding = _import_to_finding(import_record)
        result.key_findings.append(finding)

    # Apply scope filter if present
    if query.scope_filter and matching_imports:
        enricher = symtable if symtable is not None else SymtableEnricher(root)
        result.key_findings = filter_by_scope(
            result.key_findings,
            query.scope_filter,
            enricher,
            matching_imports,
        )

    result.summary["total_imports"] = len(import_records)
    result.summary["matches"] = len(result.key_findings)


def _process_def_query(
    ctx: DefQueryContext,
    query: Query,
    def_candidates: list[SgRecord] | None = None,
) -> None:
    """Process a definition entity query."""
    state = ctx.state
    result = ctx.result
    scan_ctx = state.scan
    root = state.ctx.root

    candidate_records = def_candidates if def_candidates is not None else scan_ctx.def_records
    matching_defs = _filter_to_matching(candidate_records, query)
    matching_records = list(matching_defs)  # Keep copy for scope filtering
    has_scope_constraints = bool(query.scope.in_dir or query.scope.exclude or query.scope.globs)
    compute_relationship_details = (
        len(matching_records) <= _ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES
        or query.name is not None
        or has_scope_constraints
        or "callers" in query.fields
        or "callees" in query.fields
    )

    for def_record in matching_defs:
        calls_within: list[SgRecord]
        if compute_relationship_details:
            calls_within = scan_ctx.calls_by_def.get(def_record, [])
            caller_count = _count_callers_for_definition(
                def_record,
                scan_ctx.call_records,
                scan_ctx.file_index,
            )
            enclosing_scope = _resolve_enclosing_scope(def_record, scan_ctx.file_index)
        else:
            calls_within = []
            caller_count = 0
            enclosing_scope = "<module>"
        finding = _def_to_finding(
            def_record,
            calls_within,
            caller_count=caller_count,
            callee_count=len(calls_within),
            enclosing_scope=enclosing_scope,
        )
        result.key_findings.append(finding)

    # Apply scope filter if present
    if query.scope_filter:
        enricher = ctx.symtable if ctx.symtable is not None else SymtableEnricher(root)
        result.key_findings = filter_by_scope(
            result.key_findings,
            query.scope_filter,
            enricher,
            matching_records,
        )

    # Add sections based on query fields
    if "callers" in query.fields:
        callers_section = _build_callers_section(
            matching_defs,
            scan_ctx.call_records,
            scan_ctx.file_index,
            root,
        )
        if callers_section.findings:
            result.sections.append(callers_section)

    if "callees" in query.fields:
        callees_section = _build_callees_section(matching_defs, scan_ctx.calls_by_def, root)
        if callees_section.findings:
            result.sections.append(callees_section)

    if "imports" in query.fields:
        imports_section = _build_imports_section(matching_defs, scan_ctx.all_records)
        if imports_section.findings:
            result.sections.append(imports_section)

    preview_section = _build_entity_neighborhood_preview_section(result.key_findings)
    if preview_section.findings:
        result.sections.insert(0, preview_section)

    _append_expander_sections(result, matching_defs, scan_ctx, root, query)

    result.summary["total_defs"] = len(scan_ctx.def_records)
    result.summary["total_calls"] = len(scan_ctx.call_records)
    result.summary["matches"] = len(result.key_findings)


def _process_decorator_query(
    ctx: ScanContext,
    query: Query,
    result: CqResult,
    root: Path,
    def_candidates: list[SgRecord] | None = None,
) -> None:
    """Process a decorator entity query."""
    from tools.cq.query.enrichment import enrich_with_decorators

    # Look for decorated definitions
    matching_defs: list[SgRecord] = []

    candidate_records = def_candidates if def_candidates is not None else ctx.def_records
    for def_record in candidate_records:
        # Skip non-function/class definitions
        if def_record.kind not in {
            "function",
            "async_function",
            "function_typeparams",
            "class",
            "class_bases",
        }:
            continue

        # Check if matches name pattern
        if query.name and not _matches_name(def_record, query.name):
            continue

        # Read source to check for decorators
        file_path = root / def_record.file
        try:
            source = file_path.read_text(encoding="utf-8")
        except OSError:
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
            matching_defs.append(def_record)

            finding = _def_to_finding(def_record, ctx.calls_by_def.get(def_record, []))
            finding.details["decorators"] = decorators
            finding.details["decorator_count"] = count
            result.key_findings.append(finding)

    result.summary["total_defs"] = len(ctx.def_records)
    result.summary["matches"] = len(result.key_findings)


def _process_call_query(
    ctx: ScanContext,
    query: Query,
    result: CqResult,
    root: Path,
) -> None:
    """Process a callsite entity query."""
    matching_calls = _filter_to_matching(ctx.call_records, query)
    call_contexts: list[tuple[SgRecord, SgRecord | None]] = []
    for call_record in matching_calls:
        containing = ctx.file_index.find_containing(call_record)
        call_contexts.append((call_record, containing))

    containing_defs = [containing for _, containing in call_contexts if containing is not None]
    evidence_map = _build_def_evidence_map(containing_defs, root)

    for call_record, containing in call_contexts:
        details: dict[str, object] = {}
        call_target = _extract_call_target(call_record)
        if containing is not None:
            caller_name = _extract_def_name(containing) or "<module>"
            details["caller"] = caller_name
            evidence = evidence_map.get(_record_key(containing))
            _apply_call_evidence(details, evidence, call_target)
        finding = _call_to_finding(call_record, extra_details=details)
        result.key_findings.append(finding)

    result.summary["total_calls"] = len(ctx.call_records)
    result.summary["matches"] = len(result.key_findings)


def _count_callers_for_definition(
    def_record: SgRecord,
    all_calls: list[SgRecord],
    index: FileIntervalIndex,
) -> int:
    """Count callsites that target the given definition."""
    target_name = _extract_def_name(def_record)
    if not target_name:
        return 0
    count = 0
    for call in all_calls:
        if _extract_call_target(call) != target_name:
            continue
        receiver = _extract_call_receiver(call)
        containing = index.find_containing(call)
        if receiver in {"self", "cls"}:
            target_class = _find_enclosing_class(def_record, index)
            caller_class = (
                _find_enclosing_class(containing, index) if containing is not None else None
            )
            target_class_name = (
                _extract_def_name(target_class) if target_class is not None else None
            )
            caller_class_name = (
                _extract_def_name(caller_class) if caller_class is not None else None
            )
            if target_class_name and caller_class_name and target_class_name != caller_class_name:
                continue
        count += 1
    return count


def _resolve_enclosing_scope(def_record: SgRecord, index: FileIntervalIndex) -> str:
    """Resolve human-readable enclosing scope for a definition."""
    file_index = index.by_file.get(def_record.file)
    if file_index is None:
        return "<module>"
    parents: list[SgRecord] = []
    for start, end, candidate in file_index.intervals:
        if _record_key(candidate) == _record_key(def_record):
            continue
        if start <= def_record.start_line <= end:
            parents.append(candidate)
    if not parents:
        return "<module>"
    parent = min(parents, key=lambda candidate: candidate.end_line - candidate.start_line)
    name = _extract_def_name(parent)
    return name or "<module>"


def _build_entity_neighborhood_preview_section(
    findings: list[Finding],
) -> Section:
    """Build bounded neighborhood preview for entity query top results."""
    preview_findings: list[Finding] = []
    definition_findings = [finding for finding in findings if finding.category == "definition"][:3]
    for finding in definition_findings:
        name = (
            str(finding.details.get("name"))
            if isinstance(finding.details.get("name"), str)
            else finding.message
        )
        caller_count = finding.details.get("caller_count")
        callee_count = finding.details.get("callee_count")
        enclosing_scope = finding.details.get("enclosing_scope")
        caller_total = caller_count if isinstance(caller_count, int) else 0
        callee_total = callee_count if isinstance(callee_count, int) else 0
        scope_name = enclosing_scope if isinstance(enclosing_scope, str) else "<module>"
        preview_findings.append(
            Finding(
                category="entity_neighborhood",
                message=(
                    f"{name}: callers={caller_total}, callees={callee_total}, scope={scope_name}"
                ),
                anchor=finding.anchor,
                severity="info",
                details=build_detail_payload(
                    data={
                        "name": name,
                        "caller_count": caller_total,
                        "callee_count": callee_total,
                        "enclosing_scope": scope_name,
                    },
                    score=finding.details.score,
                ),
            )
        )
    return Section(title="Neighborhood Preview", findings=preview_findings)


def _append_expander_sections(
    result: CqResult,
    target_defs: list[SgRecord],
    ctx: ScanContext,
    root: Path,
    query: Query,
) -> None:
    """Append sections for requested expanders."""
    if not query.expand:
        return

    expand_kinds = {expander.kind for expander in query.expand}
    field_kinds = set(query.fields)
    expander_specs: list[tuple[str, bool, Callable[[], Section]]] = [
        (
            "callers",
            True,
            lambda: _build_callers_section(
                target_defs,
                ctx.call_records,
                ctx.file_index,
                root,
            ),
        ),
        ("callees", True, lambda: _build_callees_section(target_defs, ctx.calls_by_def, root)),
        ("imports", True, lambda: _build_imports_section(target_defs, ctx.all_records)),
        (
            "raises",
            False,
            lambda: _build_raises_section(
                target_defs,
                ctx.all_records,
                ctx.file_index,
            ),
        ),
        ("scope", False, lambda: _build_scope_section(target_defs, root, ctx.calls_by_def)),
        ("bytecode_surface", False, lambda: _build_bytecode_surface_section(target_defs, root)),
    ]

    for kind, skip_field, builder in expander_specs:
        if kind not in expand_kinds:
            continue
        if skip_field and kind in field_kinds:
            continue
        section = builder()
        if section.findings:
            result.sections.append(section)


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
        include_globs=list(scope.globs) if scope.globs else None,
        exclude_globs=list(scope.exclude) if scope.exclude else None,
        limits=effective_limits,
    )


def assign_calls_to_defs(
    index: IntervalIndex[SgRecord],
    calls: list[SgRecord],
) -> dict[SgRecord, list[SgRecord]]:
    """Assign call records to their containing definitions.

    Parameters
    ----------
    index
        Interval index of definitions
    calls
        Call records to assign

    Returns:
    -------
    dict[SgRecord, list[SgRecord]]
        Mapping from definition to calls within it
    """
    result: dict[SgRecord, list[SgRecord]] = {}

    # Group calls by file
    calls_by_file = group_records_by_file(calls)

    # Group defs by file
    defs_by_file: dict[str, list[SgRecord]] = {}
    for _start, _end, record in index.intervals:
        if record.file not in defs_by_file:
            defs_by_file[record.file] = []
        defs_by_file[record.file].append(record)

    # For each file, build local index and assign
    for file_path, file_calls in calls_by_file.items():
        file_defs = defs_by_file.get(file_path, [])
        if not file_defs:
            continue

        # Build local index
        local_index = IntervalIndex.from_records(file_defs)

        # Assign each call
        for call in file_calls:
            containing_def = local_index.find_containing(call.start_line)
            if containing_def:
                if containing_def not in result:
                    result[containing_def] = []
                result[containing_def].append(call)

    return result


def _filter_to_matching(
    def_records: list[SgRecord],
    query: Query,
) -> list[SgRecord]:
    """Filter definitions to those matching the query.

    Returns:
    -------
    list[SgRecord]
        Records that match the query filters.
    """
    matching: list[SgRecord] = []

    for record in def_records:
        # Filter by entity type
        if not _matches_entity(record, query.entity):
            continue

        # Filter by name pattern
        if query.name and not _matches_name(record, query.name):
            continue

        matching.append(record)

    return matching


def _matches_entity(record: SgRecord, entity: str | None) -> bool:
    """Check if record matches entity type.

    Returns:
    -------
    bool
        True if the record matches the entity type.
    """
    function_kinds = {
        "function",
        "async_function",
        "function_typeparams",
    }
    class_kinds = {
        "class",
        "class_bases",
        "class_typeparams",
        "class_typeparams_bases",
        "struct",
        "enum",
        "trait",
    }
    import_kinds = {
        "import",
        "import_as",
        "from_import",
        "from_import_as",
        "from_import_multi",
        "from_import_paren",
        "use_declaration",
    }
    decorator_kinds = function_kinds | class_kinds
    if entity is None:
        return False

    if entity == "function":
        is_match = record.kind in function_kinds
    elif entity == "class":
        is_match = record.kind in class_kinds
    elif entity == "method":
        # Methods are functions inside classes - would need context analysis
        is_match = record.kind in {"function", "async_function"}
    elif entity == "module":
        is_match = False  # Module-level would need different handling
    elif entity == "callsite":
        is_match = record.record == "call"
    elif entity == "import":
        is_match = record.kind in import_kinds
    elif entity == "decorator":
        # Decorators are applied to functions/classes - check for decorated definitions
        is_match = record.kind in decorator_kinds
    else:
        is_match = False

    return is_match


def _matches_name(record: SgRecord, name: str) -> bool:
    """Check if record matches name pattern.

    Returns:
    -------
    bool
        True if the record matches the name pattern.
    """
    # Extract name based on record type
    if record.record == "import":
        extracted_name = _extract_import_name(record)
    elif record.record == "call":
        extracted_name = _extract_call_name(record)
    else:
        extracted_name = _extract_def_name(record)

    if not extracted_name:
        return False

    # Regex match if pattern starts with ~
    if name.startswith("~"):
        pattern = name[1:]
        return bool(re.search(pattern, extracted_name))

    # Exact match otherwise
    return extracted_name == name


def _extract_def_name(record: SgRecord) -> str | None:
    """Extract the name from a definition record.

    Returns:
    -------
    str | None
        Definition name when available.
    """
    text = record.text.lstrip()

    if record.record == "def":
        patterns = (
            r"(?:async\s+)?(?:def|class)\s+([A-Za-z_][A-Za-z0-9_]*)",
            r"fn\s+([A-Za-z_][A-Za-z0-9_]*)",
            r"(?:struct|enum|trait)\s+([A-Za-z_][A-Za-z0-9_]*)",
        )
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)

    return None


def _extract_import_name(record: SgRecord) -> str | None:
    """Extract the imported name from an import record.

    For single imports, returns the imported name or alias.
    For multi-imports (comma-separated or parenthesized), returns the module name.

    Returns:
    -------
    str | None
        Imported name or module name when extractable.
    """
    text = record.text.strip()
    kind = record.kind

    extractor_by_kind: dict[str, Callable[[str], str | None]] = {
        "import": _extract_simple_import,
        "import_as": _extract_import_alias,
        "from_import": _extract_from_import,
        "from_import_as": _extract_from_import_alias,
        "from_import_multi": _extract_from_module,
        "from_import_paren": _extract_from_module,
        "use_declaration": _extract_rust_use_name,
    }
    extractor = extractor_by_kind.get(kind)
    if extractor is None:
        return None
    return extractor(text)


def _extract_simple_import(text: str) -> str | None:
    """Extract name from 'import foo' or 'import foo.bar'.

    Returns:
    -------
    str | None
        Imported module name when found.
    """
    # Import statements don't contain string literals; stripping inline comments
    # prevents commas/parentheses in comments from affecting extraction.
    text = text.split("#", maxsplit=1)[0].strip()
    match = re.match(r"import\s+([\w.]+)", text)
    return match.group(1) if match else None


def _extract_import_alias(text: str) -> str | None:
    """Extract alias from 'import foo as bar'.

    Returns:
    -------
    str | None
        Alias name when found.
    """
    text = text.split("#", maxsplit=1)[0].strip()
    match = re.match(r"import\s+[\w.]+\s+as\s+(\w+)", text)
    return match.group(1) if match else None


def _extract_from_import(text: str) -> str | None:
    """Extract name from 'from x import y' (single import only).

    Returns:
    -------
    str | None
        Imported name or module name when extractable.
    """
    text = text.split("#", maxsplit=1)[0].strip()
    if "," not in text:
        match = re.search(r"import\s+(\w+)\s*$", text)
        if match:
            return match.group(1)
    # Fall back to module name for multi-imports
    return _extract_from_module(text)


def _extract_from_import_alias(text: str) -> str | None:
    """Extract alias from 'from x import y as z'.

    Returns:
    -------
    str | None
        Alias name when found.
    """
    text = text.split("#", maxsplit=1)[0].strip()
    match = re.search(r"as\s+(\w+)\s*$", text)
    return match.group(1) if match else None


def _extract_from_module(text: str) -> str | None:
    """Extract module name from 'from x import ...'.

    Returns:
    -------
    str | None
        Module name when found.
    """
    text = text.split("#", maxsplit=1)[0].strip()
    match = re.match(r"from\s+([\w.]+)", text)
    return match.group(1) if match else None


def _extract_rust_use_name(text: str) -> str | None:
    """Extract import target from Rust use declarations.

    Returns:
    -------
    str | None
        Imported Rust symbol name or alias when extractable.
    """
    text = text.split("//", maxsplit=1)[0].strip()
    match = re.match(r"use\s+([^;]+);?", text)
    if not match:
        return None
    use_target = match.group(1).strip()
    if " as " in use_target:
        return use_target.rsplit(" as ", maxsplit=1)[1].strip()
    return use_target.rsplit("::", maxsplit=1)[-1].strip("{} ").strip()


def _def_to_finding(
    def_record: SgRecord,
    calls_within: list[SgRecord],
    *,
    caller_count: int = 0,
    callee_count: int | None = None,
    enclosing_scope: str | None = None,
) -> Finding:
    """Convert a definition record to a Finding.

    Returns:
    -------
    Finding
        Finding describing the definition record.
    """
    def_name = _extract_def_name(def_record) or "unknown"

    # Build anchor
    anchor = Anchor(
        file=def_record.file,
        line=def_record.start_line,
        col=def_record.start_col,
        end_line=def_record.end_line,
        end_col=def_record.end_col,
    )

    # Calculate scores
    effective_callee_count = len(calls_within) if callee_count is None else callee_count
    scope_label = enclosing_scope or "<module>"
    impact_signals = ImpactSignals(
        sites=max(caller_count, effective_callee_count),
        files=1,
        depth=1,
    )
    conf_signals = ConfidenceSignals(evidence_kind="resolved_ast")

    score = build_score_details(impact=impact_signals, confidence=conf_signals)
    return Finding(
        category="definition",
        message=f"{def_record.kind}: {def_name}",
        anchor=anchor,
        severity="info",
        details=build_detail_payload(
            data={
                "kind": def_record.kind,
                "name": def_name,
                "calls_within": len(calls_within),
                "caller_count": caller_count,
                "callee_count": effective_callee_count,
                "enclosing_scope": scope_label,
            },
            score=score,
        ),
    )


def _import_to_finding(import_record: SgRecord) -> Finding:
    """Convert an import record to a Finding.

    Returns:
    -------
    Finding
        Finding describing the import record.
    """
    import_name = _extract_import_name(import_record) or "unknown"

    anchor = Anchor(
        file=import_record.file,
        line=import_record.start_line,
        col=import_record.start_col,
        end_line=import_record.end_line,
        end_col=import_record.end_col,
    )

    # Determine category based on import kind
    if import_record.kind in {
        "from_import",
        "from_import_as",
        "from_import_multi",
        "from_import_paren",
    }:
        category = "from_import"
    else:
        category = "import"

    return Finding(
        category=category,
        message=f"{category}: {import_name}",
        anchor=anchor,
        severity="info",
        details=build_detail_payload(
            data={
                "kind": import_record.kind,
                "name": import_name,
                "text": import_record.text.strip(),
            }
        ),
    )


@dataclass(frozen=True)
class CallTargetContext:
    """Resolved target names for caller expansion."""

    target_names: set[str]
    function_targets: set[str]
    method_targets: set[str]
    class_methods: dict[str, set[str]]


def _build_callers_section(
    target_defs: list[SgRecord],
    all_calls: list[SgRecord],
    index: FileIntervalIndex,
    root: Path,
) -> Section:
    """Build section showing callers of target definitions.

    Returns:
    -------
    Section
        Callers section for the report.
    """
    target_ctx = _build_call_target_context(target_defs, index)
    call_contexts = _collect_call_contexts(all_calls, index, target_ctx)
    evidence_map = _build_def_evidence_map(
        [containing for _, _, containing in call_contexts if containing is not None],
        root,
    )
    findings = _build_caller_findings(call_contexts, evidence_map)
    return Section(title="Callers", findings=findings)


def _build_call_target_context(
    target_defs: list[SgRecord],
    index: FileIntervalIndex,
) -> CallTargetContext:
    target_names: set[str] = set()
    method_targets: set[str] = set()
    function_targets: set[str] = set()
    class_methods: dict[str, set[str]] = {}
    for def_record in target_defs:
        def_name = _extract_def_name(def_record)
        if not def_name:
            continue
        target_names.add(def_name)
        enclosing_class = _find_enclosing_class(def_record, index)
        if enclosing_class is None:
            function_targets.add(def_name)
            continue
        method_targets.add(def_name)
        class_name = _extract_def_name(enclosing_class)
        if class_name:
            class_methods.setdefault(class_name, set()).add(def_name)
    return CallTargetContext(
        target_names=target_names,
        function_targets=function_targets,
        method_targets=method_targets,
        class_methods=class_methods,
    )


def _collect_call_contexts(
    all_calls: list[SgRecord],
    index: FileIntervalIndex,
    target_ctx: CallTargetContext,
) -> list[tuple[SgRecord, str, SgRecord | None]]:
    call_contexts: list[tuple[SgRecord, str, SgRecord | None]] = []
    for call in all_calls:
        call_target = _extract_call_target(call)
        if call_target not in target_ctx.target_names:
            continue
        receiver = _extract_call_receiver(call)
        containing = index.find_containing(call)
        if not _call_matches_target(call_target, receiver, containing, index, target_ctx):
            continue
        call_contexts.append((call, call_target, containing))
    return call_contexts


def _call_matches_target(
    call_target: str | None,
    receiver: str | None,
    containing: SgRecord | None,
    index: FileIntervalIndex,
    target_ctx: CallTargetContext,
) -> bool:
    if call_target is None:
        return False
    if receiver in {"self", "cls"} and containing is not None:
        caller_class = _find_enclosing_class(containing, index)
        if caller_class is not None:
            caller_class_name = _extract_def_name(caller_class)
            if caller_class_name:
                methods = target_ctx.class_methods.get(caller_class_name, set())
                if call_target not in methods:
                    return False
    return not (
        receiver is None
        and call_target in target_ctx.method_targets
        and call_target not in target_ctx.function_targets
    )


def _build_caller_findings(
    call_contexts: list[tuple[SgRecord, str, SgRecord | None]],
    evidence_map: dict[tuple[str, int, int, int, int], dict[str, object]],
) -> list[Finding]:
    findings: list[Finding] = []
    for call, call_target, containing in call_contexts:
        caller_name = _extract_def_name(containing) if containing else "<module>"
        anchor = Anchor(file=call.file, line=call.start_line, col=call.start_col)
        details: dict[str, object] = {"caller": caller_name, "callee": call_target}
        if containing is not None:
            evidence = evidence_map.get(_record_key(containing))
            _apply_call_evidence(details, evidence, call_target)
        findings.append(
            Finding(
                category="caller",
                message=f"caller: {caller_name} calls {call_target}",
                anchor=anchor,
                severity="info",
                details=build_detail_payload(data=details),
            )
        )
    return findings


def _build_callees_section(
    target_defs: list[SgRecord],
    calls_by_def: dict[SgRecord, list[SgRecord]],
    root: Path,
) -> Section:
    """Build section showing callees for target definitions.

    Returns:
    -------
    Section
        Callees section for the report.
    """
    findings: list[Finding] = []
    evidence_map = _build_def_evidence_map(target_defs, root)

    for def_record in target_defs:
        def_name = _extract_def_name(def_record) or "<unknown>"
        evidence = evidence_map.get(_record_key(def_record))
        for call in calls_by_def.get(def_record, []):
            call_target = _extract_call_target(call)
            if not call_target:
                continue
            anchor = Anchor(
                file=call.file,
                line=call.start_line,
                col=call.start_col,
            )
            details: dict[str, object] = {
                "caller": def_name,
                "callee": call_target,
            }
            _apply_call_evidence(details, evidence, call_target)
            findings.append(
                Finding(
                    category="callee",
                    message=f"callee: {def_name} calls {call_target}",
                    anchor=anchor,
                    severity="info",
                    details=build_detail_payload(data=details),
                )
            )

    return Section(
        title="Callees",
        findings=findings,
    )


def _build_imports_section(
    target_defs: list[SgRecord],
    all_records: list[SgRecord],
) -> Section:
    """Build section showing imports within target files.

    Returns:
    -------
    Section
        Imports section for the report.
    """
    target_files = {record.file for record in target_defs}
    findings: list[Finding] = []

    for record in all_records:
        if record.record != "import":
            continue
        if record.file not in target_files:
            continue
        findings.append(_import_to_finding(record))

    return Section(
        title="Imports",
        findings=findings,
    )


def _build_raises_section(
    target_defs: list[SgRecord],
    all_records: list[SgRecord],
    index: FileIntervalIndex,
) -> Section:
    """Build section showing raises/excepts within target definitions.

    Returns:
    -------
    Section
        Raises section for the report.
    """
    findings: list[Finding] = []
    target_def_keys = {_record_key(record) for record in target_defs}

    for record in all_records:
        if record.record not in {"raise", "except"}:
            continue
        containing = index.find_containing(record)
        if containing is None or _record_key(containing) not in target_def_keys:
            continue
        category = "raise" if record.record == "raise" else "except"
        anchor = Anchor(
            file=record.file,
            line=record.start_line,
            col=record.start_col,
        )
        findings.append(
            Finding(
                category=category,
                message=f"{category}: {record.text.strip()}",
                anchor=anchor,
                severity="info",
                details=build_detail_payload(
                    data={
                        "context_def": _extract_def_name(containing) or "<module>",
                    }
                ),
            )
        )

    return Section(
        title="Raises",
        findings=findings,
    )


def _build_scope_section(
    target_defs: list[SgRecord],
    root: Path,
    calls_by_def: dict[SgRecord, list[SgRecord]],
) -> Section:
    """Build section showing scope details for target definitions.

    Returns:
    -------
    Section
        Scope section for the report.
    """
    from tools.cq.query.enrichment import SymtableEnricher

    findings: list[Finding] = []
    enricher = SymtableEnricher(root)

    for def_record in target_defs:
        base_finding = _def_to_finding(def_record, calls_by_def.get(def_record, []))
        scope_info = enricher.enrich_function_finding(base_finding, def_record)
        if not scope_info:
            continue
        def_name = _extract_def_name(def_record) or "<unknown>"
        free_vars_value = scope_info.get("free_vars", [])
        free_vars: list[str] = free_vars_value if isinstance(free_vars_value, list) else []
        cell_vars_value = scope_info.get("cell_vars", [])
        cell_vars: list[str] = cell_vars_value if isinstance(cell_vars_value, list) else []
        label = "closure" if scope_info.get("is_closure") else "toplevel"
        message = (
            f"scope: {def_name} ({label}) free_vars={len(free_vars)} cell_vars={len(cell_vars)}"
        )
        findings.append(
            Finding(
                category="scope",
                message=message,
                anchor=base_finding.anchor,
                severity="info",
                details=build_detail_payload(data=scope_info),
            )
        )

    return Section(
        title="Scope",
        findings=findings,
    )


def _build_bytecode_surface_section(
    target_defs: list[SgRecord],
    root: Path,
) -> Section:
    """Build section showing bytecode surface info for target definitions.

    Returns:
    -------
    Section
        Bytecode surface section for the report.
    """
    from tools.cq.query.enrichment import BytecodeInfo, enrich_records

    findings: list[Finding] = []
    enrichment = enrich_records(target_defs, root)

    for record in target_defs:
        location = f"{record.file}:{record.start_line}:{record.start_col}"
        info = enrichment.get(location, {})
        bytecode_info = info.get("bytecode_info")
        if not isinstance(bytecode_info, BytecodeInfo):
            continue
        def_name = _extract_def_name(record) or "<unknown>"
        globals_list = [str(item) for item in bytecode_info.load_globals]
        attrs_list = [str(item) for item in bytecode_info.load_attrs]
        calls_list = [str(item) for item in bytecode_info.call_functions]
        details: dict[str, object] = {
            "globals": globals_list,
            "attrs": attrs_list,
            "calls": calls_list,
        }
        anchor = Anchor(
            file=record.file,
            line=record.start_line,
            col=record.start_col,
        )
        message = (
            f"bytecode: {def_name} globals={len(globals_list)} "
            f"attrs={len(attrs_list)} calls={len(calls_list)}"
        )
        findings.append(
            Finding(
                category="bytecode_surface",
                message=message,
                anchor=anchor,
                severity="info",
                details=build_detail_payload(data=details),
            )
        )

    return Section(
        title="Bytecode Surface",
        findings=findings,
    )


def _call_to_finding(
    record: SgRecord,
    *,
    extra_details: dict[str, object] | None = None,
) -> Finding:
    """Convert a call record to a Finding.

    Returns:
    -------
    Finding
        Finding describing the callsite.
    """
    call_target = _extract_call_target(record) or "<unknown>"
    anchor = Anchor(
        file=record.file,
        line=record.start_line,
        col=record.start_col,
        end_line=record.end_line,
        end_col=record.end_col,
    )
    details: dict[str, object] = {"text": record.text.strip()}
    if extra_details:
        details.update(extra_details)
    return Finding(
        category="callsite",
        message=f"call: {call_target}",
        anchor=anchor,
        severity="info",
        details=build_detail_payload(data=details),
    )


def _extract_call_target(call: SgRecord) -> str:
    """Extract the target name from a call record.

    Returns:
    -------
    str
        Extracted target name.
    """
    text = call.text.lstrip()

    # For attribute calls (obj.method()), extract the method name
    if call.kind in {"attr_call", "attr"}:
        match = re.search(r"\.(\w+)\s*\(", text)
        if match:
            return match.group(1)

    # For name calls (func()), extract the function name
    match = re.search(r"\b(\w+)\s*\(", text)
    if match:
        return match.group(1)

    return ""


def _extract_call_receiver(call: SgRecord) -> str | None:
    """Extract the receiver name for attribute calls.

    Returns:
    -------
    str | None
        Receiver name when present.
    """
    if call.kind not in {"attr_call", "attr"}:
        return None
    match = re.search(r"(\w+)\s*\.", call.text.lstrip())
    if match:
        return match.group(1)
    return None


def _find_enclosing_class(
    record: SgRecord,
    index: FileIntervalIndex,
) -> SgRecord | None:
    """Find the innermost class containing a record.

    Returns:
    -------
    SgRecord | None
        Enclosing class record when found.
    """
    class_kinds = {
        "class",
        "class_bases",
        "class_typeparams",
        "class_typeparams_bases",
        "struct",
        "enum",
        "trait",
    }
    file_index = index.by_file.get(record.file)
    if file_index is None:
        return None
    candidates: list[SgRecord] = []
    for start, end, candidate in file_index.intervals:
        if candidate.kind not in class_kinds:
            continue
        if _record_key(candidate) == _record_key(record):
            continue
        if start <= record.start_line <= end:
            candidates.append(candidate)
    if not candidates:
        return None
    return min(candidates, key=lambda candidate: candidate.end_line - candidate.start_line)


def _extract_call_name(call: SgRecord) -> str | None:
    """Extract the name for callsite matching.

    Returns:
    -------
    str | None
        Extracted call name when available.
    """
    target = _extract_call_target(call)
    return target or None
