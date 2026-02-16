"""Query executor for cq queries.

Executes ToolPlans and returns CqResult objects.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.astgrep.sgpy_scanner import SgRecord, group_records_by_file
from tools.cq.core.bootstrap import resolve_runtime_services
from tools.cq.core.cache import (
    CacheWriteTagRequestV1,
    CqCacheBackend,
    FragmentEntryV1,
    FragmentHitV1,
    FragmentPersistRuntimeV1,
    FragmentProbeRuntimeV1,
    FragmentRequestV1,
    FragmentWriteV1,
    build_cache_key,
    build_scope_hash,
    build_scope_snapshot_fingerprint,
    decode_fragment_payload,
    default_cache_policy,
    file_content_hash,
    get_cq_cache_backend,
    is_namespace_cache_enabled,
    maybe_evict_run_cache_tag,
    partition_fragment_entries,
    persist_fragment_writes,
    resolve_namespace_ttl_seconds,
    resolve_write_cache_tag,
    snapshot_backend_metrics,
)
from tools.cq.core.cache.contracts import (
    QueryEntityScanCacheV1,
    SgRecordCacheV1,
)
from tools.cq.core.cache.telemetry import (
    record_cache_decode_failure,
    record_cache_get,
    record_cache_set,
)
from tools.cq.core.contracts import SummaryBuildRequest, contract_to_builtins
from tools.cq.core.multilang_orchestrator import (
    execute_by_language_scope,
)
from tools.cq.core.multilang_summary import (
    build_multilang_summary,
    partition_stats_from_result_summary,
)
from tools.cq.core.pathing import normalize_repo_relative_path
from tools.cq.core.result_factory import build_error_result
from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    RunMeta,
    assign_result_finding_ids,
    mk_result,
    ms,
)
from tools.cq.core.structs import CqStruct
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
from tools.cq.query.language import (
    QueryLanguage,
    QueryLanguageScope,
    expand_language_scope,
    file_extensions_for_language,
)
from tools.cq.query.planner import ToolPlan, scope_to_globs, scope_to_paths
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
from tools.cq.query.sg_parser import list_scan_files, sg_scan
from tools.cq.query.shared_utils import count_result_matches, extract_def_name
from tools.cq.search._shared.types import SearchLimits
from tools.cq.search.rg.adapter import FilePatternSearchOptions, find_files_with_pattern
from tools.cq.search.semantic.diagnostics import (
    build_language_capabilities,
)
from tools.cq.utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain
from tools.cq.index.files import FileTabulationResult, build_repo_file_index, tabulate_files
from tools.cq.index.repo import resolve_repo_context
from tools.cq.query.ir import Query, Scope
from tools.cq.query.merge import merge_auto_scope_query_results

_ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES = 50
_ENTITY_FRAGMENT_PAYLOAD_LEN = 3
_MIN_PREFILTER_LITERAL_LEN = 3


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
class _EntityFragmentContext:
    namespace: str
    root: Path
    language: QueryLanguage
    files: list[Path]
    record_types: tuple[str, ...]
    cache: CqCacheBackend
    cache_enabled: bool
    ttl_seconds: int
    tag: str


class ExecutePlanRequestV1(CqStruct, frozen=True):
    """Request contract for query plan execution."""

    plan: ToolPlan
    query: Query
    root: str
    argv: tuple[str, ...] = ()
    query_text: str | None = None
    run_id: str | None = None


def _build_runmeta(ctx: QueryExecutionContext) -> RunMeta:
    run_ctx = RunContext.from_parts(
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.tc,
        started_ms=ctx.started_ms,
        run_id=ctx.run_id,
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
        "python_semantic_overview": dict[str, object](),
        "python_semantic_telemetry": {
            "attempted": 0,
            "applied": 0,
            "failed": 0,
            "skipped": 0,
            "timed_out": 0,
        },
        "rust_semantic_telemetry": {
            "attempted": 0,
            "applied": 0,
            "failed": 0,
            "skipped": 0,
            "timed_out": 0,
        },
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


def _summary_common_for_context(ctx: QueryExecutionContext) -> dict[str, object]:
    return _summary_common_for_query(ctx.query, query_text=ctx.query_text)


def _finalize_single_scope_summary(ctx: QueryExecutionContext, result: CqResult) -> None:
    if ctx.query.lang_scope == "auto":
        return
    lang = ctx.query.primary_language
    common = dict(result.summary)
    partition = partition_stats_from_result_summary(
        result.summary,
        fallback_matches=count_result_matches(result),
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
    result = build_error_result(
        macro="q",
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.tc,
        started_ms=ctx.started_ms,
        error=message,
    )
    result.summary.update(_summary_common_for_context(ctx))
    _finalize_single_scope_summary(ctx, result)
    return assign_result_finding_ids(result)


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
    fragment_ctx = _build_entity_fragment_context(ctx, paths=paths, scope_globs=scope_globs)
    if not fragment_ctx.files:
        return []

    entries = _entity_fragment_entries(fragment_ctx)
    request = FragmentRequestV1(
        namespace=fragment_ctx.namespace,
        workspace=str(fragment_ctx.root),
        language=ctx.plan.lang,
        ttl_seconds=fragment_ctx.ttl_seconds,
        tag=fragment_ctx.tag,
        run_id=ctx.run_id,
    )
    partition = partition_fragment_entries(
        request,
        entries,
        FragmentProbeRuntimeV1(
            cache_get=fragment_ctx.cache.get,
            decode=_decode_entity_fragment_payload,
            cache_enabled=fragment_ctx.cache_enabled,
            record_get=record_cache_get,
            record_decode_failure=record_cache_decode_failure,
        ),
    )

    records_by_rel = _entity_records_from_hits(partition.hits)
    if partition.misses:
        miss_paths = [fragment_ctx.root / miss.entry.file for miss in partition.misses]
        scanned = sg_scan(
            paths=miss_paths,
            record_types=ctx.plan.sg_record_types,
            root=fragment_ctx.root,
            globs=None,
            lang=ctx.plan.lang,
        )
        grouped = group_records_by_file(scanned)
        writes: list[FragmentWriteV1] = []
        for miss in partition.misses:
            rel_path = miss.entry.file
            fragment_records = sorted(grouped.get(rel_path, []), key=_record_sort_key)
            records_by_rel[rel_path] = fragment_records
            writes.append(
                FragmentWriteV1(
                    entry=miss.entry,
                    payload=QueryEntityScanCacheV1(
                        records=[_record_to_cache_record(item) for item in fragment_records]
                    ),
                )
            )
        persist_fragment_writes(
            request,
            writes,
            FragmentPersistRuntimeV1(
                cache_set=lambda key, value, *, expire=None, tag=None: fragment_ctx.cache.set(
                    key,
                    value,
                    expire=expire,
                    tag=tag,
                ),
                cache_set_many=lambda rows, *, expire=None, tag=None: fragment_ctx.cache.set_many(
                    rows,
                    expire=expire,
                    tag=tag,
                ),
                encode=contract_to_builtins,
                cache_enabled=fragment_ctx.cache_enabled,
                transact=fragment_ctx.cache.transact,
                record_set=record_cache_set,
            ),
        )

    return _assemble_entity_records(fragment_ctx, records_by_rel)


def _build_entity_fragment_context(
    ctx: QueryExecutionContext,
    *,
    paths: list[Path],
    scope_globs: list[str] | None,
) -> _EntityFragmentContext:
    namespace = "query_entity_fragment"
    resolved_root = ctx.root.resolve()
    files = list_scan_files(
        paths=paths,
        root=resolved_root,
        globs=scope_globs,
        lang=ctx.plan.lang,
    )
    record_types = tuple(sorted(ctx.plan.sg_record_types))
    scope_hash = build_scope_hash(
        {
            "paths": tuple(sorted(str(path.resolve()) for path in paths)),
            "scope_globs": tuple(scope_globs or ()),
            "record_types": record_types,
            "lang": ctx.plan.lang,
        }
    )
    snapshot = build_scope_snapshot_fingerprint(
        root=resolved_root,
        files=files,
        language=ctx.plan.lang,
        scope_globs=scope_globs or [],
        scope_roots=paths,
    )
    policy = default_cache_policy(root=resolved_root)
    cache = get_cq_cache_backend(root=resolved_root)
    cache_enabled = is_namespace_cache_enabled(policy=policy, namespace=namespace)
    tag = resolve_write_cache_tag(
        CacheWriteTagRequestV1(
            policy=policy,
            workspace=str(resolved_root),
            language=ctx.plan.lang,
            namespace=namespace,
            scope_hash=scope_hash,
            snapshot=snapshot.digest,
            run_id=ctx.run_id,
        )
    )
    return _EntityFragmentContext(
        namespace=namespace,
        root=resolved_root,
        language=ctx.plan.lang,
        files=files,
        record_types=record_types,
        cache=cache,
        cache_enabled=cache_enabled,
        ttl_seconds=resolve_namespace_ttl_seconds(policy=policy, namespace=namespace),
        tag=tag,
    )


def _entity_fragment_entries(fragment_ctx: _EntityFragmentContext) -> list[FragmentEntryV1]:
    entries: list[FragmentEntryV1] = []
    for file_path in fragment_ctx.files:
        rel_path = normalize_repo_relative_path(str(file_path), root=fragment_ctx.root)
        content_hash = file_content_hash(file_path).digest
        entries.append(
            FragmentEntryV1(
                file=rel_path,
                content_hash=content_hash,
                cache_key=build_cache_key(
                    fragment_ctx.namespace,
                    version="v1",
                    workspace=str(fragment_ctx.root),
                    language=fragment_ctx.language,
                    target=rel_path,
                    extras={
                        "file_content_hash": content_hash,
                        "record_types": fragment_ctx.record_types,
                    },
                ),
            )
        )
    return entries


def _decode_entity_fragment_payload(payload: object) -> list[SgRecord] | None:
    decoded = decode_fragment_payload(payload, type_=QueryEntityScanCacheV1)
    if decoded is None:
        return None
    return [_cache_record_to_record(item) for item in decoded.records]


def _entity_records_from_hits(hits: tuple[FragmentHitV1, ...]) -> dict[str, list[SgRecord]]:
    records_by_rel: dict[str, list[SgRecord]] = {}
    for hit in hits:
        if not isinstance(hit.payload, list):
            continue
        records_by_rel[hit.entry.file] = cast("list[SgRecord]", hit.payload)
    return records_by_rel


def _assemble_entity_records(
    fragment_ctx: _EntityFragmentContext,
    records_by_rel: dict[str, list[SgRecord]],
) -> list[SgRecord]:
    ordered_records: list[SgRecord] = []
    for file_path in fragment_ctx.files:
        rel_path = normalize_repo_relative_path(str(file_path), root=fragment_ctx.root)
        ordered_records.extend(records_by_rel.get(rel_path, []))
    ordered_records.sort(key=_record_sort_key)
    return ordered_records


def _record_to_cache_record(record: SgRecord) -> SgRecordCacheV1:
    return SgRecordCacheV1(
        record=record.record,
        kind=record.kind,
        file=record.file,
        start_line=record.start_line,
        start_col=record.start_col,
        end_line=record.end_line,
        end_col=record.end_col,
        text=record.text,
        rule_id=record.rule_id,
    )


def _cache_record_to_record(payload: SgRecordCacheV1) -> SgRecord:
    return SgRecord(
        record=payload.record,
        kind=payload.kind,
        file=payload.file,
        start_line=payload.start_line,
        start_col=payload.start_col,
        end_line=payload.end_line,
        end_col=payload.end_col,
        text=payload.text,
        rule_id=payload.rule_id,
    )


def _record_sort_key(record: SgRecord) -> tuple[str, int, int, str, str, str, str]:
    return (
        record.file,
        int(record.start_line),
        int(record.start_col),
        record.record,
        record.kind,
        record.rule_id,
        record.text,
    )


def _finding_sort_key(finding: Finding) -> tuple[str, int, int, str]:
    if finding.anchor is None:
        return ("", 0, 0, finding.message)
    return (
        finding.anchor.file,
        int(finding.anchor.line),
        int(finding.anchor.col or 0),
        finding.message,
    )


def _raw_match_sort_key(row: dict[str, object]) -> tuple[str, int, int, str]:
    file = row.get("file")
    line = row.get("line")
    col = row.get("col")
    rule_id = row.get("ruleId")
    return (
        str(file) if isinstance(file, str) else "",
        int(line) if isinstance(line, int) else 0,
        int(col) if isinstance(col, int) else 0,
        str(rule_id) if isinstance(rule_id, str) else "",
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
            result.summary["file_filters"] = list(file_result.decisions)
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
    result.summary["file_filters"] = list(file_result.decisions)


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
    result.summary["file_filters"] = list(state.file_result.decisions)


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
    active_run_id = request.run_id or uuid7_str()
    if request.query.lang_scope == "auto":
        return _execute_auto_scope_plan(
            request.query,
            tc=tc,
            root=root,
            argv=list(request.argv),
            query_text=request.query_text,
            run_id=active_run_id,
        )

    ctx = QueryExecutionContext(
        plan=request.plan,
        query=request.query,
        tc=tc,
        root=root,
        argv=list(request.argv),
        started_ms=ms(),
        run_id=active_run_id,
        query_text=request.query_text,
    )
    result = _execute_single_context(ctx)
    maybe_evict_run_cache_tag(root=root, language=request.plan.lang, run_id=active_run_id)
    return result


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
    run_id: str,
) -> CqResult:

    results = execute_by_language_scope(
        query.lang_scope,
        lambda lang: _run_scoped_auto_query(
            query=query,
            lang=lang,
            tc=tc,
            root=root,
            argv=argv,
            run_id=run_id,
        ),
    )
    merged = merge_auto_scope_query_results(
        query=query,
        results=results,
        root=root,
        argv=argv,
        tc=tc,
        summary_common=_summary_common_for_query(query, query_text=query_text),
    )
    merged.summary["cache_backend"] = snapshot_backend_metrics(root=root)
    assign_result_finding_ids(merged)
    for lang in expand_language_scope(query.lang_scope):
        maybe_evict_run_cache_tag(root=root, language=lang, run_id=run_id)
    return merged


def _run_scoped_auto_query(
    *,
    query: Query,
    lang: QueryLanguage,
    tc: Toolchain,
    root: Path,
    argv: list[str],
    run_id: str,
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
        run_id=run_id,
    )
    return _execute_single_context(scoped_ctx)


def _attach_entity_insight(result: CqResult, *, root: Path) -> None:
    """Build and attach front-door insight card to entity result."""
    from tools.cq.core.services import EntityFrontDoorRequest

    services = resolve_runtime_services(root)
    services.entity.attach_front_door(
        EntityFrontDoorRequest(
            result=result,
            relationship_detail_max_matches=_ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES,
        )
    )


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
    _attach_entity_insight(result, root=ctx.root)
    result.summary["cache_backend"] = snapshot_backend_metrics(root=ctx.root)
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
    _attach_entity_insight(result, root=ctx.root)
    result.summary["cache_backend"] = snapshot_backend_metrics(root=ctx.root)
    return assign_result_finding_ids(result)


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
        state.scope_globs,
        state.ctx.run_id,
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
    result.summary["cache_backend"] = snapshot_backend_metrics(root=ctx.root)
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
        query_text=request.query_text,
    )
    if not request.files:
        result = _empty_result(ctx, "No files match scope after filtering")
        if request.plan.explain and request.decisions is not None:
            result.summary["file_filters"] = list(request.decisions)
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
    result.summary["cache_backend"] = snapshot_backend_metrics(root=ctx.root)
    return assign_result_finding_ids(result)


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
            caller_name = extract_def_name(containing) or "<module>"
            details["caller"] = caller_name
            evidence = evidence_map.get(_record_key(containing))
            _apply_call_evidence(details, evidence, call_target)
        finding = _call_to_finding(call_record, extra_details=details)
        result.key_findings.append(finding)

    result.summary["total_calls"] = len(ctx.call_records)
    result.summary["matches"] = len(result.key_findings)


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
