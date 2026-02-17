"""Runtime extraction/orchestration helpers for AST builders."""

from __future__ import annotations

import ast
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from functools import partial
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.extract.registry import normalize_options
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from extract.coordination.context import (
    ExtractExecutionContext,
    FileContext,
    attrs_map,
    span_dict,
    text_from_file_ctx,
)
from extract.coordination.line_offsets import LineOffsets, line_offsets_from_file_ctx
from extract.coordination.materialization import (
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    extract_plan_from_row_batches,
    extract_plan_from_rows,
    materialize_extract_plan,
)
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.extractors.ast.builders import (
    _cache_key,
    _call_row,
    _def_row,
    _docstring_row,
    _exception_error_row,
    _import_rows,
    _limit_errors,
    _node_name,
    _node_scalar_attrs,
    _node_value_repr,
    _parse_ast_text,
    _span_spec_from_node,
)
from extract.extractors.ast.setup import (
    AstExtractOptions,
    _format_feature_version,
    _resolve_feature_version,
)
from extract.extractors.ast.visitors import (
    AstLimitError,
    _AstWalkAccumulator,
    _AstWalkResult,
)
from extract.infrastructure.cache_utils import (
    CacheSetOptions,
    cache_for_extract,
    cache_get,
    cache_lock,
    cache_set,
    cache_ttl_seconds,
    diskcache_profile_from_ctx,
    stable_cache_key,
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
    from diskcache import Cache, FanoutCache

    from cache.diskcache_factory import DiskCacheProfile
    from extract.coordination.evidence_plan import EvidencePlan
    from extract.scanning.scope_manifest import ScopeManifest
    from extract.session import ExtractSession


def _ast_row_worker(
    file_ctx: FileContext,
    *,
    options: AstExtractOptions,
    cache_profile: DiskCacheProfile | None,
    cache_ttl: float | None,
) -> dict[str, object] | None:
    cache = cache_for_extract(cache_profile) if options.cache_by_sha else None
    return _extract_ast_for_context(
        file_ctx,
        options=options,
        cache=cache,
        cache_ttl=cache_ttl,
    )


def _ast_row_from_walk(
    file_ctx: FileContext,
    *,
    options: AstExtractOptions,
    walk: _AstWalkResult | None,
    errors: list[dict[str, object]],
) -> dict[str, object]:
    parse_manifest = [
        {
            "parse_mode": options.mode,
            "feature_version": _format_feature_version(options.feature_version),
            "optimize": options.optimize,
            "type_comments": options.type_comments,
            "allow_top_level_await": options.allow_top_level_await,
            "dont_inherit": options.dont_inherit,
        }
    ]
    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "file_sha256": file_ctx.file_sha256,
        "nodes": walk.nodes if walk is not None else [],
        "edges": walk.edges if walk is not None else [],
        "errors": errors,
        "docstrings": walk.docstrings if walk is not None else [],
        "imports": walk.imports if walk is not None else [],
        "defs": walk.defs if walk is not None else [],
        "calls": walk.calls if walk is not None else [],
        "type_ignores": walk.type_ignores if walk is not None else [],
        "parse_manifest": parse_manifest,
        "attrs": attrs_map(
            {
                "parse_mode": options.mode,
                "feature_version": options.feature_version,
                "optimize": options.optimize,
                "type_comments": options.type_comments,
                "allow_top_level_await": options.allow_top_level_await,
                "dont_inherit": options.dont_inherit,
                "max_bytes": options.max_bytes,
                "max_nodes": options.max_nodes,
            }
        ),
    }


def _iter_child_items(node: ast.AST) -> list[tuple[ast.AST, str, int]]:
    items: list[tuple[ast.AST, str, int]] = []
    for field, value in ast.iter_fields(node):
        if isinstance(value, ast.AST):
            items.append((value, field, 0))
        elif isinstance(value, list):
            items.extend(
                (item, field, idx) for idx, item in enumerate(value) if isinstance(item, ast.AST)
            )
    return items


def _ast_id_for_node(idx_map: dict[int, int], node: ast.AST) -> int:
    node_id = id(node)
    ast_id = idx_map.get(node_id)
    if ast_id is None:
        ast_id = len(idx_map)
        idx_map[node_id] = ast_id
    return ast_id


@dataclass(frozen=True)
class AstNodeContext:
    """Context for AST node extraction."""

    ast_id: int
    parent_ast_id: int | None
    field_name: str | None
    field_pos: int | None
    line_offsets: LineOffsets | None


def _node_row(node: ast.AST, *, ctx: AstNodeContext) -> dict[str, object]:
    node_attr_values: dict[str, object] = {
        "field_name": ctx.field_name,
        "field_pos": ctx.field_pos,
    }
    node_attr_values.update(_node_scalar_attrs(node))
    return {
        "ast_id": ctx.ast_id,
        "parent_ast_id": ctx.parent_ast_id,
        "kind": type(node).__name__,
        "name": _node_name(node),
        "value": _node_value_repr(node),
        "span": span_dict(_span_spec_from_node(node, line_offsets=ctx.line_offsets)),
        "attrs": attrs_map(node_attr_values),
    }


def _edge_row(
    *,
    parent_ast_id: int | None,
    ast_id: int,
    field_name: str | None,
    field_pos: int | None,
) -> dict[str, object] | None:
    if parent_ast_id is None:
        return None
    return {
        "src": parent_ast_id,
        "dst": ast_id,
        "kind": "CHILD",
        "slot": field_name,
        "idx": field_pos,
        "attrs": attrs_map({}),
    }


def _append_docstring(
    rows: _AstWalkAccumulator,
    node: ast.AST,
    *,
    ast_id: int,
    source: str,
    line_offsets: LineOffsets | None,
) -> None:
    if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
        return
    row = _docstring_row(node, ast_id=ast_id, source=source, line_offsets=line_offsets)
    if row is not None:
        rows.docstrings.append(row)


def _append_def(
    rows: _AstWalkAccumulator,
    node: ast.AST,
    *,
    ctx: AstNodeContext,
    source: str,
) -> None:
    row = _def_row(
        node,
        ast_id=ctx.ast_id,
        parent_ast_id=ctx.parent_ast_id,
        source=source,
        line_offsets=ctx.line_offsets,
    )
    if row is not None:
        rows.defs.append(row)


def _append_call(
    rows: _AstWalkAccumulator,
    node: ast.AST,
    *,
    ast_id: int,
    parent_ast_id: int | None,
    line_offsets: LineOffsets | None,
) -> None:
    row = _call_row(
        node,
        ast_id=ast_id,
        parent_ast_id=parent_ast_id,
        line_offsets=line_offsets,
    )
    if row is not None:
        rows.calls.append(row)


def _append_type_ignore(
    rows: _AstWalkAccumulator,
    node: ast.AST,
    *,
    ast_id: int,
    line_offsets: LineOffsets | None,
) -> None:
    if not isinstance(node, ast.TypeIgnore):
        return
    tag = getattr(node, "tag", None)
    rows.type_ignores.append(
        {
            "ast_id": ast_id,
            "tag": tag if isinstance(tag, str) else None,
            "span": span_dict(_span_spec_from_node(node, line_offsets=line_offsets)),
            "attrs": attrs_map({}),
        }
    )


def _walk_ast(
    root: ast.AST,
    *,
    source: str,
    max_nodes: int | None,
    line_offsets: LineOffsets | None,
) -> _AstWalkResult:
    rows = _AstWalkAccumulator()
    stack: list[tuple[ast.AST, int | None, str | None, int | None]] = [(root, None, None, None)]
    idx_map: dict[int, int] = {}
    node_count = 0

    while stack:
        if max_nodes is not None and node_count >= max_nodes:
            msg = f"AST node limit exceeded: {max_nodes}."
            raise AstLimitError(msg)
        node, parent_idx, field_name, field_pos = stack.pop()
        node_count += 1
        ast_id = _ast_id_for_node(idx_map, node)
        ctx = AstNodeContext(
            ast_id=ast_id,
            parent_ast_id=parent_idx,
            field_name=field_name,
            field_pos=field_pos,
            line_offsets=line_offsets,
        )
        rows.nodes.append(
            _node_row(
                node,
                ctx=ctx,
            )
        )
        edge = _edge_row(
            parent_ast_id=ctx.parent_ast_id,
            ast_id=ctx.ast_id,
            field_name=ctx.field_name,
            field_pos=ctx.field_pos,
        )
        if edge is not None:
            rows.edges.append(edge)
        _append_docstring(
            rows,
            node,
            ast_id=ast_id,
            source=source,
            line_offsets=line_offsets,
        )
        rows.imports.extend(
            _import_rows(
                node,
                ast_id=ast_id,
                parent_ast_id=parent_idx,
                line_offsets=line_offsets,
            )
        )
        _append_def(
            rows,
            node,
            ctx=ctx,
            source=source,
        )
        _append_call(
            rows,
            node,
            ast_id=ast_id,
            parent_ast_id=parent_idx,
            line_offsets=line_offsets,
        )
        _append_type_ignore(rows, node, ast_id=ast_id, line_offsets=line_offsets)

        for child, field, pos in reversed(_iter_child_items(node)):
            stack.append((child, ast_id, field, pos))

    return rows.to_result()


def _parse_and_walk(
    text: str,
    *,
    filename: str,
    options: AstExtractOptions,
    max_nodes: int | None,
    line_offsets: LineOffsets | None,
) -> tuple[_AstWalkResult | None, list[dict[str, object]]]:
    error_rows: list[dict[str, object]] = []
    root, err = _parse_ast_text(
        text,
        filename=filename,
        options=options,
    )
    if err is not None:
        error_rows.append(err)
    if root is None:
        return None, error_rows
    try:
        walk = _walk_ast(
            root,
            source=text,
            max_nodes=max_nodes,
            line_offsets=line_offsets,
        )
    except AstLimitError as exc:
        error_rows.append(_exception_error_row(exc))
        return None, error_rows
    return walk, error_rows


def _extract_ast_for_context(
    file_ctx: FileContext,
    *,
    options: AstExtractOptions,
    cache: Cache | FanoutCache | None = None,
    cache_ttl: float | None = None,
) -> dict[str, object] | None:
    if not file_ctx.file_id or not file_ctx.path:
        return None
    cache_key = _cache_key(file_ctx, options=options)
    use_cache = cache is not None and cache_key is not None
    cache_key_str = stable_cache_key("ast", {"key": cache_key}) if use_cache else None
    if use_cache and cache_key_str is not None:
        cached = cache_get(cache, key=cache_key_str, default=None)
        if isinstance(cached, _AstWalkResult):
            return _ast_row_from_walk(file_ctx, options=options, walk=cached, errors=[])

    def _build_row() -> dict[str, object] | None:
        text = text_from_file_ctx(file_ctx)
        if text is None:
            return None
        line_offsets = line_offsets_from_file_ctx(file_ctx)
        max_nodes, error_rows = _limit_errors(file_ctx, text=text, options=options)
        walk: _AstWalkResult | None = None
        if not error_rows:
            walk, parse_errors = _parse_and_walk(
                text,
                filename=str(file_ctx.path),
                options=options,
                max_nodes=max_nodes,
                line_offsets=line_offsets,
            )
            error_rows.extend(parse_errors)
            if (
                use_cache
                and cache is not None
                and cache_key_str is not None
                and walk is not None
                and not error_rows
            ):
                cache_set(
                    cache,
                    key=cache_key_str,
                    value=walk,
                    options=CacheSetOptions(
                        expire=cache_ttl,
                        tag=options.repo_id,
                    ),
                )
        return _ast_row_from_walk(file_ctx, options=options, walk=walk, errors=error_rows)

    if use_cache and cache_key_str is not None:
        with cache_lock(cache, key=cache_key_str):
            cached = cache_get(cache, key=cache_key_str, default=None)
            if isinstance(cached, _AstWalkResult):
                return _ast_row_from_walk(file_ctx, options=options, walk=cached, errors=[])
            return _build_row()
    return _build_row()


def _extract_ast_for_row(
    row: dict[str, object],
    *,
    options: AstExtractOptions,
) -> dict[str, object] | None:
    file_ctx = FileContext.from_repo_row(row)
    return _extract_ast_for_context(file_ctx, options=options)


def extract_ast(
    repo_files: TableLike,
    options: AstExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> ExtractResult[TableLike]:
    """Extract a minimal AST fact set per file.

    Returns:
    -------
    ExtractResult[TableLike]
        Tables of AST nodes, edges, and errors.
    """
    normalized_options = normalize_options("ast", options, AstExtractOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    plans = extract_ast_plans(
        repo_files,
        options=normalized_options,
        context=exec_context,
    )
    table = cast(
        "TableLike",
        materialize_extract_plan(
            "ast_files_v1",
            plans["ast_files"],
            runtime_profile=runtime_profile,
            determinism_tier=determinism_tier,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
    )
    return ExtractResult(table=table, extractor_name="ast")


def extract_ast_plans(
    repo_files: TableLike,
    options: AstExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> dict[str, DataFusionPlanArtifact]:
    """Extract AST plans for nested file records.

    Returns:
    -------
    dict[str, DataFusionPlanArtifact]
        Plan bundle keyed by ``ast_files``.
    """
    normalized_options = normalize_options("ast", options, AstExtractOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    batch_size = _resolve_batch_size(normalized_options)
    row_batches: Iterable[Sequence[Mapping[str, object]]] | None = None
    rows: list[dict[str, object]] | None = None
    request = _AstRowRequest(
        repo_files=repo_files,
        file_contexts=exec_context.file_contexts,
        scope_manifest=exec_context.scope_manifest,
        options=normalized_options,
        runtime_profile=runtime_profile,
    )
    if batch_size is None:
        rows = _collect_ast_rows(
            repo_files,
            file_contexts=exec_context.file_contexts,
            scope_manifest=exec_context.scope_manifest,
            options=normalized_options,
            runtime_profile=runtime_profile,
        )
    else:
        row_batches = _iter_ast_row_batches(request, batch_size=batch_size)
    evidence_plan = exec_context.evidence_plan
    plan_context = _AstPlanContext(
        normalize=normalize,
        evidence_plan=evidence_plan,
        session=session,
    )
    return {
        "ast_files": _build_ast_plan(
            "ast_files_v1",
            rows,
            row_batches=row_batches,
            plan_context=plan_context,
        ),
    }


@dataclass(frozen=True)
class _AstRowRequest:
    repo_files: TableLike
    file_contexts: Iterable[FileContext] | None
    scope_manifest: ScopeManifest | None
    options: AstExtractOptions
    runtime_profile: DataFusionRuntimeProfile | None


def _collect_ast_rows(
    repo_files: TableLike,
    *,
    file_contexts: Iterable[FileContext] | None,
    scope_manifest: ScopeManifest | None,
    options: AstExtractOptions,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> list[dict[str, object]]:
    request = _AstRowRequest(
        repo_files=repo_files,
        file_contexts=file_contexts,
        scope_manifest=scope_manifest,
        options=options,
        runtime_profile=runtime_profile,
    )
    return list(_iter_ast_rows(request))


def _iter_ast_row_batches(
    request: _AstRowRequest,
    *,
    batch_size: int,
) -> Iterable[list[dict[str, object]]]:
    batch: list[dict[str, object]] = []
    for row in _iter_ast_rows(request):
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _iter_ast_rows(request: _AstRowRequest) -> Iterable[dict[str, object]]:
    contexts = list(
        iter_worklist_contexts(
            WorklistRequest(
                repo_files=request.repo_files,
                output_table="ast_files_v1",
                runtime_profile=request.runtime_profile,
                file_contexts=request.file_contexts,
                queue_name=(
                    worklist_queue_name(
                        output_table="ast_files_v1", repo_id=request.options.repo_id
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
    resolved_options = _resolve_feature_version(request.options, contexts)
    cache_profile = diskcache_profile_from_ctx(request.runtime_profile)
    cache_ttl = cache_ttl_seconds(cache_profile, "extract")
    if not resolved_options.parallel:
        for file_ctx in contexts:
            row = _ast_row_worker(
                file_ctx,
                options=resolved_options,
                cache_profile=cache_profile,
                cache_ttl=cache_ttl,
            )
            if row is not None:
                yield row
        return
    runner = partial(
        _ast_row_worker,
        options=resolved_options,
        cache_profile=cache_profile,
        cache_ttl=cache_ttl,
    )
    max_workers = resolve_max_workers(
        resolved_options.max_workers,
        kind="cpu",
    )
    for row in parallel_map(contexts, runner, max_workers=max_workers):
        if row is not None:
            yield row


def _resolve_batch_size(options: AstExtractOptions) -> int | None:
    if options.batch_size is None:
        return None
    if options.batch_size <= 0:
        msg = "batch_size must be a positive integer."
        raise ValueError(msg)
    return options.batch_size


def _build_ast_plan(
    name: str,
    rows: list[dict[str, object]] | None,
    *,
    row_batches: Iterable[Sequence[Mapping[str, object]]] | None = None,
    plan_context: _AstPlanContext,
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
class _AstPlanContext:
    normalize: ExtractNormalizeOptions
    evidence_plan: EvidencePlan | None
    session: ExtractSession


class _AstTablesKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: AstExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: bool


class _AstTablesKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: AstExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: Literal[False]


class _AstTablesKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: AstExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: Required[Literal[True]]


@overload
def extract_ast_tables(
    **kwargs: Unpack[_AstTablesKwargsTable],
) -> Mapping[str, TableLike]: ...


@overload
def extract_ast_tables(
    **kwargs: Unpack[_AstTablesKwargsReader],
) -> Mapping[str, TableLike | RecordBatchReaderLike]: ...


def extract_ast_tables(
    **kwargs: Unpack[_AstTablesKwargs],
) -> Mapping[str, TableLike | RecordBatchReaderLike]:
    """Extract AST tables as a name-keyed bundle.

    Parameters
    ----------
    kwargs:
        Keyword-only arguments for extraction (repo_files, options, file_contexts, ctx, profile,
        prefer_reader).

    Returns:
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted AST outputs keyed by name.
    """
    with stage_span(
        "extract.ast_tables",
        stage="extract",
        scope_name=SCOPE_EXTRACT,
        attributes={"codeanatomy.extractor": "ast"},
    ):
        repo_files = kwargs["repo_files"]
        normalized_options = normalize_options("ast", kwargs.get("options"), AstExtractOptions)
        file_contexts = kwargs.get("file_contexts")
        evidence_plan = kwargs.get("evidence_plan")
        profile = kwargs.get("profile", "default")
        prefer_reader = kwargs.get("prefer_reader", False)
        exec_context = ExtractExecutionContext(
            file_contexts=file_contexts,
            evidence_plan=evidence_plan,
            scope_manifest=kwargs.get("scope_manifest"),
            session=kwargs.get("session"),
            profile=profile,
        )
        session = exec_context.ensure_session()
        exec_context = replace(exec_context, session=session)
        runtime_profile = exec_context.ensure_runtime_profile()
        determinism_tier = exec_context.determinism_tier()
        normalize = ExtractNormalizeOptions(options=normalized_options)
        plans = extract_ast_plans(
            repo_files,
            options=normalized_options,
            context=exec_context,
        )
        return {
            "ast_files": materialize_extract_plan(
                "ast_files_v1",
                plans["ast_files"],
                runtime_profile=runtime_profile,
                determinism_tier=determinism_tier,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    prefer_reader=prefer_reader,
                    apply_post_kernels=True,
                ),
            ),
        }
