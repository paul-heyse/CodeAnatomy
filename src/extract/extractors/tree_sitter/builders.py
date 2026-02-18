"""Tree-sitter extraction adapters using Rust bridge payloads."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from functools import cache, partial
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

import tree_sitter_python
from tree_sitter import LANGUAGE_VERSION, MIN_COMPATIBLE_LANGUAGE_VERSION, Language, Parser

from core_types import RowPermissive as Row
from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.extract.registry import normalize_options
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
from extract.coordination.context import (
    ExtractExecutionContext,
    FileContext,
    attrs_map,
)
from extract.coordination.extraction_runtime_loop import (
    ExtractionRuntimeWorklist,
    iter_runtime_row_batches,
    iter_runtime_rows,
    runtime_worklist_contexts,
)
from extract.coordination.materialization import (
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    extract_plan_from_row_batches,
    extract_plan_from_rows,
    materialize_extract_plan,
)
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.extractors.tree_sitter.queries import TreeSitterQueryPack, compile_query_pack
from extract.extractors.tree_sitter.setup import TreeSitterExtractOptions
from extract.infrastructure.result_types import ExtractResult
from obs.otel import SCOPE_EXTRACT, stage_span

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from extract.coordination.evidence_plan import EvidencePlan
    from extract.scanning.scope_manifest import ScopeManifest
    from extract.session import ExtractSession


PY_LANGUAGE = Language(tree_sitter_python.language())
_MIN_PARTS_FOR_EXTENSION = 2
_MIN_PAIR_ITEMS = 2


@dataclass(frozen=True)
class _TsWorkerState:
    parser: Parser
    query_pack: TreeSitterQueryPack | None


@dataclass(frozen=True)
class _TsRowRequest:
    repo_files: TableLike
    options: TreeSitterExtractOptions
    file_contexts: Iterable[FileContext] | None
    scope_manifest: ScopeManifest | None
    runtime_profile: DataFusionRuntimeProfile | None


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


def _assert_language_abi(lang: Language) -> None:
    if not (MIN_COMPATIBLE_LANGUAGE_VERSION <= lang.abi_version <= LANGUAGE_VERSION):
        msg = f"Tree-sitter ABI mismatch: {lang.abi_version}"
        raise ValueError(msg)


def _parser(options: TreeSitterExtractOptions) -> Parser:
    _assert_language_abi(PY_LANGUAGE)
    parser = Parser(PY_LANGUAGE)
    if options.parser_timeout_micros is not None:
        set_timeout = getattr(parser, "set_timeout_micros", None)
        if callable(set_timeout):
            set_timeout(int(options.parser_timeout_micros))
    return parser


@cache
def _query_pack() -> TreeSitterQueryPack:
    return compile_query_pack(PY_LANGUAGE)


def _should_run_queries(options: TreeSitterExtractOptions) -> bool:
    return (
        options.include_captures
        or options.include_defs
        or options.include_calls
        or options.include_imports
        or options.include_docstrings
    )


@cache
def _worker_state(options: TreeSitterExtractOptions) -> _TsWorkerState:
    parser = _parser(options)
    query_pack = _query_pack() if _should_run_queries(options) else None
    return _TsWorkerState(parser=parser, query_pack=query_pack)


def _normalize_ts_options(options: TreeSitterExtractOptions) -> TreeSitterExtractOptions:
    ranges = options.included_ranges
    if ranges is None:
        return options
    normalized = tuple((int(start), int(end)) for start, end in ranges)
    if normalized == ranges:
        return options
    return replace(options, included_ranges=normalized)


def _resolve_batch_size(options: TreeSitterExtractOptions) -> int | None:
    if options.batch_size is None:
        return None
    if options.batch_size <= 0:
        msg = "batch_size must be a positive integer."
        raise ValueError(msg)
    return options.batch_size


def _should_parse(file_ctx: FileContext, options: TreeSitterExtractOptions) -> bool:
    if not file_ctx.path:
        return False
    if options.extensions is None:
        return True
    suffix = file_ctx.path.rsplit(".", 1)
    ext = f".{suffix[1]}" if len(suffix) == _MIN_PARTS_FOR_EXTENSION else ""
    normalized = tuple(value.lower() for value in options.extensions)
    return ext.lower() in normalized


def _span_from_offsets(start: int, end: int) -> dict[str, object]:
    return {
        "start": None,
        "end": None,
        "end_exclusive": True,
        "col_unit": "byte",
        "byte_span": {
            "byte_start": int(start),
            "byte_len": max(0, int(end) - int(start)),
        },
    }


def _normalize_attrs(value: object) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        return attrs_map(value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        pairs: list[tuple[str, str]] = []
        for entry in value:
            if isinstance(entry, Sequence) and not isinstance(entry, (str, bytes, bytearray)):
                entry_list = list(entry)
                if len(entry_list) >= _MIN_PAIR_ITEMS:
                    pairs.append((str(entry_list[0]), str(entry_list[1])))
        if pairs:
            return pairs
    return []


def _coerce_int(value: object, *, default: int) -> int:
    return value if isinstance(value, int) else default


def _normalize_nested_rows(payload: Row) -> None:
    nodes_value = payload.get("nodes")
    if not isinstance(nodes_value, list):
        payload["nodes"] = list[object]()
        nodes_value = list[object]()
    for node in nodes_value:
        if not isinstance(node, dict):
            continue
        node.setdefault("attrs", [])
        if not isinstance(node.get("attrs"), list):
            node["attrs"] = _normalize_attrs(node.get("attrs"))
        if node.get("span") is None:
            start = _coerce_int(node.get("bstart"), default=0)
            end = _coerce_int(node.get("bend"), default=start)
            node["span"] = _span_from_offsets(start, end)
    for key in ("edges", "errors", "missing", "captures", "defs", "calls", "imports", "docstrings"):
        values = payload.get(key)
        if not isinstance(values, list):
            payload[key] = []
            continue
        for row in values:
            if isinstance(row, dict):
                row.setdefault("attrs", [])
                if not isinstance(row.get("attrs"), list):
                    row["attrs"] = _normalize_attrs(row.get("attrs"))


def _normalize_bridge_row(
    row_payload: Mapping[str, object],
    *,
    file_ctx: FileContext,
    options: TreeSitterExtractOptions,
) -> Row:
    payload: Row = {str(key): value for key, value in row_payload.items()}
    payload.setdefault("repo", options.repo_id)
    payload.setdefault("path", file_ctx.path)
    payload.setdefault("file_id", file_ctx.file_id)
    payload.setdefault("file_sha256", file_ctx.file_sha256)
    payload.setdefault("nodes", [])
    payload.setdefault("edges", [])
    payload.setdefault("errors", [])
    payload.setdefault("missing", [])
    payload.setdefault("captures", [])
    payload.setdefault("defs", [])
    payload.setdefault("calls", [])
    payload.setdefault("imports", [])
    payload.setdefault("docstrings", [])
    payload.setdefault("stats", None)
    payload["attrs"] = _normalize_attrs(payload.get("attrs"))
    _normalize_nested_rows(payload)
    return payload


def _extract_ts_file_row_rust(
    file_ctx: FileContext,
    *,
    options: TreeSitterExtractOptions,
    query_pack: TreeSitterQueryPack | None,
) -> Row | None:
    from datafusion_engine.extensions import datafusion_ext

    bridge = getattr(datafusion_ext, "extract_tree_sitter_batch", None)
    if not callable(bridge):
        return None
    source = file_ctx.text
    if source is None and file_ctx.data is not None:
        source = bytes(file_ctx.data).decode(file_ctx.encoding or "utf-8", errors="replace")
    if source is None or not file_ctx.path:
        return None
    request_payload = {
        "file_id": file_ctx.file_id,
        "file_sha256": file_ctx.file_sha256,
        "repo": options.repo_id,
        "query_pack_version": query_pack.version if query_pack is not None else None,
        "options": {
            "include_nodes": options.include_nodes,
            "include_edges": options.include_edges,
            "include_errors": options.include_errors,
            "include_missing": options.include_missing,
            "include_captures": options.include_captures,
            "include_defs": options.include_defs,
            "include_calls": options.include_calls,
            "include_imports": options.include_imports,
            "include_docstrings": options.include_docstrings,
            "include_stats": options.include_stats,
            "max_text_bytes": options.max_text_bytes,
            "max_docstring_bytes": options.max_docstring_bytes,
        },
    }
    try:
        try:
            payload_like = bridge(source, file_ctx.path, request_payload)
        except TypeError:
            payload_like = bridge(source, file_ctx.path)
    except (RuntimeError, TypeError, ValueError):
        return None
    if payload_like is None:
        return None
    if isinstance(payload_like, Mapping):
        return _normalize_bridge_row(payload_like, file_ctx=file_ctx, options=options)

    from datafusion_engine.arrow.coercion import to_arrow_table

    table = to_arrow_table(payload_like)
    rows = table.to_pylist()
    if rows and isinstance(rows[0], Mapping):
        return _normalize_bridge_row(rows[0], file_ctx=file_ctx, options=options)

    payload = table.to_pydict()
    node_types = payload.get("node_type")
    starts = payload.get("bstart")
    ends = payload.get("bend")
    if (
        not isinstance(node_types, list)
        or not isinstance(starts, list)
        or not isinstance(ends, list)
    ):
        return None

    nodes: list[Row] = []
    for idx, kind in enumerate(node_types):
        bstart = starts[idx] if idx < len(starts) else None
        bend = ends[idx] if idx < len(ends) else None
        if not isinstance(bstart, int) or not isinstance(bend, int):
            continue
        node_id = f"{file_ctx.file_id}:{kind}:{bstart}:{bend}"
        nodes.append(
            {
                "node_id": node_id,
                "node_uid": idx,
                "parent_id": None,
                "kind": str(kind),
                "kind_id": 0,
                "grammar_id": 0,
                "grammar_name": "python",
                "span": _span_from_offsets(bstart, bend),
                "flags": {
                    "is_named": True,
                    "has_error": False,
                    "is_error": False,
                    "is_missing": False,
                    "is_extra": False,
                    "has_changes": False,
                },
                "attrs": [],
            }
        )

    row: Row = {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "file_sha256": file_ctx.file_sha256,
        "nodes": nodes if options.include_nodes else list[object](),
        "edges": list[object](),
        "errors": list[object](),
        "missing": list[object](),
        "captures": list[object](),
        "defs": list[object](),
        "calls": list[object](),
        "imports": list[object](),
        "docstrings": list[object](),
        "stats": (
            {
                "node_count": len(nodes),
                "named_count": len(nodes),
                "error_count": 0,
                "missing_count": 0,
                "parse_ms": 0,
                "parse_timed_out": False,
                "incremental_used": False,
                "query_match_count": 0,
                "query_capture_count": 0,
                "match_limit_exceeded": False,
            }
            if options.include_stats
            else None
        ),
        "attrs": list[object](),
    }
    return _normalize_bridge_row(row, file_ctx=file_ctx, options=options)


def _extract_ts_file_row(
    file_ctx: FileContext,
    *,
    parser: Parser,
    cache: object | None,
    options: TreeSitterExtractOptions,
    query_pack: TreeSitterQueryPack | None,
) -> Row | None:
    _ = (parser, cache)
    if not file_ctx.file_id or not file_ctx.path:
        return None
    if not _should_parse(file_ctx, options):
        return None
    rust_row = _extract_ts_file_row_rust(file_ctx, options=options, query_pack=query_pack)
    if rust_row is None:
        msg = (
            "extract_tree_sitter_batch bridge returned no extraction payload. "
            "Rust tree-sitter extraction bridge is required."
        )
        raise RuntimeError(msg)
    return rust_row


def _iter_ts_rows_for_contexts(
    contexts: Sequence[FileContext],
    *,
    options: TreeSitterExtractOptions,
) -> Iterator[Row]:
    runner = partial(_ts_row_worker, options=options)
    yield from iter_runtime_rows(
        contexts,
        worker=runner,
        parallel=options.parallel,
        max_workers=options.max_workers,
    )


def _collect_ts_rows(request: _TsRowRequest) -> list[Row]:
    contexts = runtime_worklist_contexts(
        ExtractionRuntimeWorklist(
            repo_files=request.repo_files,
            output_table="tree_sitter_files_v1",
            runtime_profile=request.runtime_profile,
            file_contexts=request.file_contexts,
            scope_manifest=request.scope_manifest,
            use_worklist_queue=request.options.use_worklist_queue,
            repo_id=request.options.repo_id,
        )
    )
    if not contexts:
        return []
    return list(_iter_ts_rows_for_contexts(contexts, options=request.options))


def _iter_ts_row_batches(
    request: _TsRowRequest,
    *,
    batch_size: int,
) -> Iterable[Sequence[Mapping[str, object]]]:
    contexts = runtime_worklist_contexts(
        ExtractionRuntimeWorklist(
            repo_files=request.repo_files,
            output_table="tree_sitter_files_v1",
            runtime_profile=request.runtime_profile,
            file_contexts=request.file_contexts,
            scope_manifest=request.scope_manifest,
            use_worklist_queue=request.options.use_worklist_queue,
            repo_id=request.options.repo_id,
        )
    )
    if not contexts:
        return
    yield from iter_runtime_row_batches(
        _iter_ts_rows_for_contexts(contexts, options=request.options),
        batch_size=batch_size,
    )


def _ts_row_worker(file_ctx: FileContext, *, options: TreeSitterExtractOptions) -> Row | None:
    state = _worker_state(options)
    return _extract_ts_file_row(
        file_ctx,
        parser=state.parser,
        cache=None,
        options=options,
        query_pack=state.query_pack,
    )


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


def extract_ts(
    repo_files: TableLike,
    *,
    options: TreeSitterExtractOptions | None = None,
    context: ExtractExecutionContext | None = None,
) -> ExtractResult[TableLike]:
    """Extract tree-sitter rows from repo files via Rust bridge.

    Returns:
    -------
    ExtractResult[TableLike]
        Extracted tree-sitter table payload.
    """
    normalized_options = normalize_options("tree_sitter", options, TreeSitterExtractOptions)
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
        Extracted plan artifacts keyed by output table name.
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

    plan_context = _TreeSitterPlanContext(
        normalize=normalize,
        evidence_plan=exec_context.evidence_plan,
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

    Returns:
    -------
    Mapping[str, TableLike | RecordBatchReaderLike]
        Materialized table bundle keyed by extractor output name.
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
