"""Runtime extraction/orchestration helpers for tree-sitter builders."""

from __future__ import annotations

import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from functools import partial
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

from tree_sitter import (
    Node,
    Parser,
    QueryCursor,
)

from core_types import RowPermissive as Row
from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.extract.registry import normalize_options
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from extract.coordination.context import (
    ExtractExecutionContext,
    FileContext,
    attrs_map,
)
from extract.coordination.line_offsets import LineOffsets
from extract.coordination.materialization import (
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    extract_plan_from_row_batches,
    extract_plan_from_rows,
    materialize_extract_plan,
)
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.extractors.tree_sitter.builders import (
    PY_LANGUAGE,
    SEMVER_PARTS,
    _edge_entry,
    _error_entry,
    _included_ranges,
    _iter_nodes,
    _iter_query_matches,
    _missing_entry,
    _node_entry,
    _parse_callback,
    _QueryCollector,
    _should_parse,
    _source_buffer,
    _worker_state,
)
from extract.extractors.tree_sitter.cache import TreeSitterCache, TreeSitterParseResult
from extract.extractors.tree_sitter.queries import TreeSitterQueryPack
from extract.extractors.tree_sitter.setup import TreeSitterExtractOptions
from extract.extractors.tree_sitter.visitors import (
    _NodeStats,
    _ParseContext,
    _ParseStats,
    _QueryContext,
    _QueryRows,
    _QueryStats,
)
from extract.infrastructure.parallel import parallel_map, resolve_max_workers
from extract.infrastructure.result_types import ExtractResult
from extract.infrastructure.worklists import (
    WorklistRequest,
    iter_worklist_contexts,
    worklist_queue_name,
)
from obs.otel import SCOPE_EXTRACT, stage_span

if TYPE_CHECKING:
    from extract.coordination.evidence_plan import EvidencePlan
    from extract.scanning.scope_manifest import ScopeManifest
    from extract.session import ExtractSession

type SourceBuffer = bytes | bytearray | memoryview


def _collect_queries(context: _QueryContext) -> _QueryRows:
    collector = _QueryCollector(
        file_ctx=context.file_ctx,
        data=context.data,
        options=context.options,
    )
    for query_name, query in context.query_pack.queries.items():
        cursor = QueryCursor(
            query,
            match_limit=context.options.query_match_limit,
        )
        if context.options.query_timeout_micros is not None:
            set_timeout = getattr(cursor, "set_timeout_micros", None)
            if callable(set_timeout):
                set_timeout(int(context.options.query_timeout_micros))
        for pattern_index, capture_map in _iter_query_matches(
            cursor,
            root=context.root,
            ranges=context.ranges,
        ):
            collector.record_match(
                query_name=query_name,
                pattern_index=pattern_index,
                capture_map=capture_map,
            )
        if cursor.did_exceed_match_limit:
            collector.stats.match_limit_exceeded = True
    return collector.build()


def extract_ts(
    repo_files: TableLike,
    *,
    options: TreeSitterExtractOptions | None = None,
    context: ExtractExecutionContext | None = None,
) -> ExtractResult[TableLike]:
    """Extract tree-sitter nodes and diagnostics from repo files.

    Parameters
    ----------
    repo_files:
        Repo files table with bytes/text.
    options:
        Extraction options.
    context:
        Shared execution context bundle for extraction.

    Returns:
    -------
    ExtractResult[TableLike]
        Extracted tree-sitter file table.
    """
    normalized_options = normalize_options("tree_sitter", options, TreeSitterExtractOptions)
    normalized_options = _normalize_ts_options(normalized_options)
    normalized_options = _normalize_ts_options(normalized_options)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    plans = extract_ts_plans(
        repo_files,
        options=normalized_options,
        context=exec_context,
    )
    table = cast(
        "TableLike",
        materialize_extract_plan(
            "tree_sitter_files_v1",
            plans["tree_sitter_files"],
            runtime_profile=runtime_profile,
            determinism_tier=determinism_tier,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
    )
    return ExtractResult(table=table, extractor_name="tree_sitter")


def extract_ts_plans(
    repo_files: TableLike,
    *,
    options: TreeSitterExtractOptions | None = None,
    context: ExtractExecutionContext | None = None,
) -> dict[str, DataFusionPlanArtifact]:
    """Extract tree-sitter plans for nested file records.

    Returns:
    -------
    dict[str, DataFusionPlanArtifact]
        Plan bundle keyed by ``tree_sitter_files``.
    """
    normalized_options = normalize_options("tree_sitter", options, TreeSitterExtractOptions)
    normalized_options = _normalize_ts_options(normalized_options)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    rows: list[Row] | None = None
    row_batches: Iterable[Sequence[Mapping[str, object]]] | None = None
    batch_size = _resolve_batch_size(normalized_options)
    request = _TsRowRequest(
        repo_files=repo_files,
        options=normalized_options,
        file_contexts=exec_context.file_contexts,
        scope_manifest=exec_context.scope_manifest,
        runtime_profile=runtime_profile,
    )
    if batch_size is None:
        rows = _collect_ts_rows(request)
    else:
        row_batches = _iter_ts_row_batches(request, batch_size=batch_size)
    evidence_plan = exec_context.evidence_plan
    plan_context = _TreeSitterPlanContext(
        normalize=normalize,
        evidence_plan=evidence_plan,
        session=session,
    )
    return {
        "tree_sitter_files": _build_ts_plan(
            "tree_sitter_files_v1",
            rows,
            row_batches=row_batches,
            plan_context=plan_context,
        ),
    }


@dataclass(frozen=True)
class _TsRowRequest:
    repo_files: TableLike
    options: TreeSitterExtractOptions
    file_contexts: Iterable[FileContext] | None
    scope_manifest: ScopeManifest | None
    runtime_profile: DataFusionRuntimeProfile | None


def _collect_ts_rows(request: _TsRowRequest) -> list[Row]:
    rows: list[Row] = []
    contexts = list(
        iter_worklist_contexts(
            WorklistRequest(
                repo_files=request.repo_files,
                output_table="tree_sitter_files_v1",
                runtime_profile=request.runtime_profile,
                file_contexts=request.file_contexts,
                queue_name=(
                    worklist_queue_name(
                        output_table="tree_sitter_files_v1",
                        repo_id=request.options.repo_id,
                    )
                    if request.options.use_worklist_queue
                    else None
                ),
                scope_manifest=request.scope_manifest,
            )
        )
    )
    if not contexts:
        return rows
    rows.extend(_iter_ts_rows_for_contexts(contexts, options=request.options))
    return rows


def _iter_ts_rows_for_contexts(
    contexts: Sequence[FileContext],
    *,
    options: TreeSitterExtractOptions,
) -> Iterator[Row]:
    if not options.parallel:
        for file_ctx in contexts:
            row = _ts_row_worker(file_ctx, options=options)
            if row is not None:
                yield row
        return
    runner = partial(_ts_row_worker, options=options)
    max_workers = resolve_max_workers(options.max_workers, kind="cpu")
    for row in parallel_map(contexts, runner, max_workers=max_workers):
        if row is not None:
            yield row


def _iter_ts_row_batches(
    request: _TsRowRequest,
    *,
    batch_size: int,
) -> Iterable[Sequence[Mapping[str, object]]]:
    batch: list[Row] = []
    contexts = list(
        iter_worklist_contexts(
            WorklistRequest(
                repo_files=request.repo_files,
                output_table="tree_sitter_files_v1",
                runtime_profile=request.runtime_profile,
                file_contexts=request.file_contexts,
                queue_name=(
                    worklist_queue_name(
                        output_table="tree_sitter_files_v1",
                        repo_id=request.options.repo_id,
                    )
                    if request.options.use_worklist_queue
                    else None
                ),
                scope_manifest=request.scope_manifest,
            )
        )
    )
    if not contexts:
        return
    for row in _iter_ts_rows_for_contexts(contexts, options=request.options):
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _ts_row_worker(file_ctx: FileContext, *, options: TreeSitterExtractOptions) -> Row | None:
    state = _worker_state(options)
    return _extract_ts_file_row(
        file_ctx,
        parser=state.parser,
        cache=state.cache,
        options=options,
        query_pack=state.query_pack,
    )


def _resolve_batch_size(options: TreeSitterExtractOptions) -> int | None:
    if options.batch_size is None:
        return None
    if options.batch_size <= 0:
        msg = "batch_size must be a positive integer."
        raise ValueError(msg)
    return options.batch_size


def _normalize_ts_options(options: TreeSitterExtractOptions) -> TreeSitterExtractOptions:
    ranges = options.included_ranges
    if ranges is None:
        return options
    normalized = tuple((int(start), int(end)) for start, end in ranges)
    if normalized == ranges:
        return options
    return replace(options, included_ranges=normalized)


def _resolve_parse_callback_options(options: TreeSitterExtractOptions) -> bool:
    if options.parse_callback_threshold_bytes is None:
        return False
    if options.parse_callback_threshold_bytes <= 0:
        msg = "parse_callback_threshold_bytes must be a positive integer."
        raise ValueError(msg)
    if options.parse_callback_chunk_size <= 0:
        msg = "parse_callback_chunk_size must be a positive integer."
        raise ValueError(msg)
    return True


def _should_run_queries(options: TreeSitterExtractOptions) -> bool:
    return (
        options.include_captures
        or options.include_defs
        or options.include_calls
        or options.include_imports
        or options.include_docstrings
    )


def _parse_tree(
    context: _ParseContext,
    *,
    options: TreeSitterExtractOptions,
) -> tuple[TreeSitterParseResult, _ParseStats]:
    start = time.monotonic()
    data = context.data
    parser = context.parser
    cache = context.cache
    ranges = None
    if options.included_ranges:
        ranges = _included_ranges(
            data,
            options,
            offsets=LineOffsets.from_bytes(bytes(data)),
        )
    if ranges is not None:
        parser.included_ranges = list(ranges)
    try:
        if (
            cache is not None
            and options.incremental
            and ranges is None
            and not context.use_callback
            and isinstance(data, (bytes, bytearray))
        ):
            source = data if isinstance(data, bytes) else bytes(data)
            parse_result = cache.parse(
                parser=parser,
                key=context.cache_key,
                source=source,
            )
            tree = parse_result.tree
            used_incremental = parse_result.used_incremental
        else:
            if context.use_callback:
                callback = _parse_callback(data, chunk_size=options.parse_callback_chunk_size)
                tree = parser.parse(callback)
            else:
                tree = parser.parse(data)
            used_incremental = False
            parse_result = TreeSitterParseResult(
                tree=tree,
                changed_ranges=(),
                used_incremental=False,
            )
    finally:
        if ranges is not None:
            parser.included_ranges = []
    parse_ms = int((time.monotonic() - start) * 1000)
    parse_timed_out = tree is None
    if parse_timed_out:
        parser.reset()
    return parse_result, _ParseStats(
        parse_ms=parse_ms,
        parse_timed_out=parse_timed_out,
        used_incremental=used_incremental,
    )


def _extract_ts_file_row(
    file_ctx: FileContext,
    *,
    parser: Parser,
    cache: TreeSitterCache | None,
    options: TreeSitterExtractOptions,
    query_pack: TreeSitterQueryPack | None,
) -> Row | None:
    if not file_ctx.file_id or not file_ctx.path:
        return None
    if not _should_parse(file_ctx, options):
        return None
    cache_key = file_ctx.file_id or file_ctx.path
    with _source_buffer(file_ctx, options) as (data, use_callback):
        if data is None:
            return None
        parse_result, parse_stats = _parse_tree(
            _ParseContext(
                parser=parser,
                data=data,
                cache=cache,
                cache_key=cache_key,
                use_callback=use_callback,
            ),
            options=options,
        )
        tree = parse_result.tree if parse_result is not None else None
        if tree is None:
            return _empty_ts_file_row(
                file_ctx=file_ctx,
                options=options,
                query_pack=query_pack,
                parse_stats=parse_stats,
            )
        root = tree.root_node
        node_rows, edge_rows, error_rows, missing_rows, node_stats = _collect_node_rows(
            root,
            file_ctx=file_ctx,
            data=data,
            options=options,
        )
        query_rows = None
        query_ranges = parse_result.changed_ranges if options.incremental else ()
        if query_pack is not None:
            query_rows = _collect_queries(
                _QueryContext(
                    root=root,
                    data=data,
                    file_ctx=file_ctx,
                    options=options,
                    query_pack=query_pack,
                    ranges=query_ranges,
                )
            )
    attrs = _file_attrs(
        _file_ctx=file_ctx,
        query_pack=query_pack,
        parse_stats=parse_stats,
        node_stats=node_stats,
        query_stats=query_rows.stats if query_rows is not None else None,
    )
    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "file_sha256": file_ctx.file_sha256,
        "nodes": node_rows if options.include_nodes else [],
        "edges": edge_rows if options.include_edges else [],
        "errors": error_rows if options.include_errors else [],
        "missing": missing_rows if options.include_missing else [],
        "captures": query_rows.captures if query_rows and options.include_captures else [],
        "defs": query_rows.defs if query_rows and options.include_defs else [],
        "calls": query_rows.calls if query_rows and options.include_calls else [],
        "imports": query_rows.imports if query_rows and options.include_imports else [],
        "docstrings": query_rows.docstrings if query_rows and options.include_docstrings else [],
        "stats": _stats_row(
            node_stats=node_stats,
            parse_stats=parse_stats,
            query_stats=query_rows.stats if query_rows is not None else None,
        )
        if options.include_stats
        else None,
        "attrs": attrs_map(attrs),
    }


def _empty_ts_file_row(
    *,
    file_ctx: FileContext,
    options: TreeSitterExtractOptions,
    query_pack: TreeSitterQueryPack | None,
    parse_stats: _ParseStats,
) -> Row:
    node_stats = _NodeStats()
    attrs = _file_attrs(
        _file_ctx=file_ctx,
        query_pack=query_pack,
        parse_stats=parse_stats,
        node_stats=node_stats,
        query_stats=None,
    )
    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "file_sha256": file_ctx.file_sha256,
        "nodes": [],
        "edges": [],
        "errors": [],
        "missing": [],
        "captures": [],
        "defs": [],
        "calls": [],
        "imports": [],
        "docstrings": [],
        "stats": _stats_row(
            node_stats=node_stats,
            parse_stats=parse_stats,
            query_stats=None,
        )
        if options.include_stats
        else None,
        "attrs": attrs_map(attrs),
    }


def _collect_node_rows(
    root: Node,
    *,
    file_ctx: FileContext,
    data: SourceBuffer,
    options: TreeSitterExtractOptions,
) -> tuple[list[Row], list[Row], list[Row], list[Row], _NodeStats]:
    node_rows: list[Row] = []
    edge_rows: list[Row] = []
    error_rows: list[Row] = []
    missing_rows: list[Row] = []
    node_stats = _NodeStats()
    for node, parent, field_name, child_index in _iter_nodes(root):
        node_stats.node_count += 1
        if node.is_named:
            node_stats.named_count += 1
        if node.is_error:
            node_stats.error_count += 1
        if node.is_missing:
            node_stats.missing_count += 1
        if options.include_nodes:
            node_rows.append(
                _node_entry(
                    node,
                    file_ctx=file_ctx,
                    data=data,
                    options=options,
                    parent=parent,
                )
            )
        if options.include_edges and parent is not None:
            edge_rows.append(
                _edge_entry(
                    file_ctx=file_ctx,
                    parent=parent,
                    child=node,
                    field_name=field_name,
                    child_index=child_index,
                )
            )
        if options.include_errors and node.is_error:
            error_rows.append(_error_entry(node, file_ctx=file_ctx))
        if options.include_missing and node.is_missing:
            missing_rows.append(_missing_entry(node, file_ctx=file_ctx))
    return node_rows, edge_rows, error_rows, missing_rows, node_stats


def _file_attrs(
    *,
    _file_ctx: FileContext,
    query_pack: TreeSitterQueryPack | None,
    parse_stats: _ParseStats,
    node_stats: _NodeStats,
    query_stats: _QueryStats | None,
) -> dict[str, object]:
    attrs: dict[str, object] = {
        "language_name": PY_LANGUAGE.name,
        "language_abi_version": PY_LANGUAGE.abi_version,
        "language_semantic_version": _semantic_version(PY_LANGUAGE.semantic_version),
        "parse_ms": parse_stats.parse_ms,
        "parse_timed_out": parse_stats.parse_timed_out,
        "node_count": node_stats.node_count,
        "named_count": node_stats.named_count,
        "error_count": node_stats.error_count,
        "missing_count": node_stats.missing_count,
        "incremental_used": parse_stats.used_incremental,
    }
    if query_pack is not None:
        attrs.update(query_pack.metadata())
    if query_stats is not None:
        attrs["query_match_count"] = query_stats.match_count
        attrs["query_capture_count"] = query_stats.capture_count
        attrs["query_match_limit_exceeded"] = query_stats.match_limit_exceeded
    return attrs


def _semantic_version(version: object) -> str | None:
    if isinstance(version, tuple) and len(version) == SEMVER_PARTS:
        major, minor, patch = version
        if all(isinstance(part, int) for part in (major, minor, patch)):
            return f"{major}.{minor}.{patch}"
    return None


def _stats_row(
    *,
    node_stats: _NodeStats,
    parse_stats: _ParseStats,
    query_stats: _QueryStats | None,
) -> Row:
    return {
        "node_count": node_stats.node_count,
        "named_count": node_stats.named_count,
        "error_count": node_stats.error_count,
        "missing_count": node_stats.missing_count,
        "parse_ms": parse_stats.parse_ms,
        "parse_timed_out": parse_stats.parse_timed_out,
        "incremental_used": parse_stats.used_incremental,
        "query_match_count": query_stats.match_count if query_stats else None,
        "query_capture_count": query_stats.capture_count if query_stats else None,
        "match_limit_exceeded": query_stats.match_limit_exceeded if query_stats else None,
    }


def _build_ts_plan(
    name: str,
    rows: list[Row] | None,
    *,
    row_batches: Iterable[Sequence[Mapping[str, object]]] | None,
    plan_context: _TreeSitterPlanContext,
) -> DataFusionPlanArtifact:
    plan_options = ExtractPlanOptions(
        normalize=plan_context.normalize,
        evidence_plan=plan_context.evidence_plan,
    )
    if row_batches is not None:
        return extract_plan_from_row_batches(
            name,
            row_batches,
            session=plan_context.session,
            options=plan_options,
        )
    return extract_plan_from_rows(
        name,
        rows or [],
        session=plan_context.session,
        options=plan_options,
    )


@dataclass(frozen=True)
class _TreeSitterPlanContext:
    normalize: ExtractNormalizeOptions
    evidence_plan: EvidencePlan | None
    session: ExtractSession


class _TreeSitterTablesKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    context: ExtractExecutionContext | None
    prefer_reader: bool


class _TreeSitterTablesKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    context: ExtractExecutionContext | None
    prefer_reader: Literal[False]


class _TreeSitterTablesKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    context: ExtractExecutionContext | None
    prefer_reader: Required[Literal[True]]


@overload
def extract_ts_tables(
    **kwargs: Unpack[_TreeSitterTablesKwargsTable],
) -> Mapping[str, TableLike]: ...


@overload
def extract_ts_tables(
    **kwargs: Unpack[_TreeSitterTablesKwargsReader],
) -> Mapping[str, TableLike | RecordBatchReaderLike]: ...


def extract_ts_tables(
    **kwargs: Unpack[_TreeSitterTablesKwargs],
) -> Mapping[str, TableLike | RecordBatchReaderLike]:
    """Extract tree-sitter tables as a name-keyed bundle.

    Parameters
    ----------
    kwargs:
        Keyword-only arguments for extraction (repo_files, options, context, file_contexts,
        evidence_plan, ctx, profile, prefer_reader).

    Returns:
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted tree-sitter outputs keyed by output name.
    """
    with stage_span(
        "extract.tree_sitter_tables",
        stage="extract",
        scope_name=SCOPE_EXTRACT,
        attributes={"codeanatomy.extractor": "tree_sitter"},
    ):
        repo_files = kwargs["repo_files"]
        normalized_options = normalize_options(
            "tree_sitter",
            kwargs.get("options"),
            TreeSitterExtractOptions,
        )
        normalized_options = _normalize_ts_options(normalized_options)
        context = kwargs.get("context")
        if context is None:
            context = ExtractExecutionContext(
                file_contexts=kwargs.get("file_contexts"),
                evidence_plan=kwargs.get("evidence_plan"),
                scope_manifest=kwargs.get("scope_manifest"),
                session=kwargs.get("session"),
                profile=kwargs.get("profile", "default"),
            )
        session = context.ensure_session()
        context = replace(context, session=session)
        runtime_profile = context.ensure_runtime_profile()
        determinism_tier = context.determinism_tier()
        prefer_reader = kwargs.get("prefer_reader", False)
        normalize = ExtractNormalizeOptions(options=normalized_options)
        plans = extract_ts_plans(
            repo_files,
            options=normalized_options,
            context=context,
        )
        return {
            "tree_sitter_files": materialize_extract_plan(
                "tree_sitter_files_v1",
                plans["tree_sitter_files"],
                runtime_profile=runtime_profile,
                determinism_tier=determinism_tier,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    prefer_reader=prefer_reader,
                    apply_post_kernels=True,
                ),
            ),
        }
