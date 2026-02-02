"""Extract Python symtable data into Arrow tables using shared helpers."""

from __future__ import annotations

import symtable
from collections import defaultdict
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.extract.registry import normalize_options
from datafusion_engine.plan.bundle import DataFusionPlanBundle
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.coordination.line_offsets import LineOffsets
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    FileContext,
    SpanSpec,
    attrs_map,
    bytes_from_file_ctx,
    extract_plan_from_rows,
    file_identity_row,
    materialize_extract_plan,
    span_dict,
    text_from_file_ctx,
)
from extract.infrastructure.cache_utils import (
    CacheSetOptions,
    cache_for_extract,
    cache_get,
    cache_set,
    cache_ttl_seconds,
    diskcache_profile_from_ctx,
    stable_cache_key,
)
from extract.infrastructure.options import RepoOptions, WorkerOptions, WorklistQueueOptions
from extract.infrastructure.parallel import resolve_max_workers
from extract.infrastructure.result_types import ExtractResult
from extract.infrastructure.schema_cache import symtable_files_fingerprint
from extract.infrastructure.worklists import (
    WorklistRequest,
    iter_worklist_contexts,
    worklist_queue_name,
)
from obs.otel.scopes import SCOPE_EXTRACT
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache

    from extract.coordination.evidence_plan import EvidencePlan
    from extract.scanning.scope_manifest import ScopeManifest
    from extract.session import ExtractSession


@dataclass(frozen=True)
class SymtableExtractOptions(RepoOptions, WorklistQueueOptions, WorkerOptions):
    """Configure symtable extraction."""

    compile_type: str = "exec"


@dataclass(frozen=True)
class SymtableContext:
    """Context values shared across symtable rows."""

    file_ctx: FileContext

    @property
    def file_id(self) -> str:
        """Return the file id for this extraction context.

        Returns
        -------
        str
            File id from the file context.
        """
        return self.file_ctx.file_id

    @property
    def path(self) -> str:
        """Return the file path for this extraction context.

        Returns
        -------
        str
            File path from the file context.
        """
        return self.file_ctx.path

    @property
    def file_sha256(self) -> str | None:
        """Return the file sha256 for this extraction context.

        Returns
        -------
        str | None
            File hash from the file context.
        """
        return self.file_ctx.file_sha256

    def identity_row(self) -> dict[str, str | None]:
        """Return the identity row for this context.

        Returns
        -------
        dict[str, str | None]
            File identity columns for row construction.
        """
        return file_identity_row(self.file_ctx)


_SYMTABLE_CACHE_PARTS = 3


def _symtable_cache_key(
    file_ctx: FileContext,
    *,
    compile_type: str,
) -> tuple[str, str, str, str] | None:
    if not file_ctx.file_id or file_ctx.file_sha256 is None:
        return None
    return (file_ctx.file_id, file_ctx.file_sha256, symtable_files_fingerprint(), compile_type)


def _line_offsets(file_ctx: FileContext) -> LineOffsets | None:
    data = bytes_from_file_ctx(file_ctx)
    if data is None:
        return None
    return LineOffsets.from_bytes(data)


def _effective_max_workers(
    options: SymtableExtractOptions,
) -> int:
    max_workers = resolve_max_workers(options.max_workers, kind="cpu")
    return max(1, min(32, max_workers))


def _scope_type_str(tbl: symtable.SymbolTable) -> str:
    return tbl.get_type().name


def _scope_role(scope_type_str: str) -> str:
    return "runtime" if scope_type_str in {"MODULE", "FUNCTION", "CLASS"} else "type_meta"


def _is_meta_scope(scope_type: str | None) -> bool:
    if scope_type is None:
        return False
    return scope_type not in {"MODULE", "FUNCTION", "CLASS"}


def _stable_scope_id(
    *,
    file_id: str,
    qualpath: str,
    lineno: int,
    scope_type: str,
    ordinal: int,
) -> str:
    return f"{file_id}:SCOPE:{qualpath}:{lineno}:{scope_type}:{ordinal}"


def _scope_component(scope_type: str, scope_name: str, ordinal: int) -> str:
    base = "top" if scope_type == "MODULE" else (scope_name or scope_type.lower())
    return f"{base}#{ordinal}" if ordinal > 0 else base


def _join_qualpath(parent: str, component: str) -> str:
    return component if not parent else f"{parent}::{component}"


def _next_ordinal(
    sibling_counts: dict[int | None, dict[tuple[str, str, int], int]],
    *,
    parent_local_id: int | None,
    scope_type: str,
    scope_name: str,
    lineno: int,
) -> int:
    key = (scope_type, scope_name, lineno)
    counts = sibling_counts.setdefault(parent_local_id, {})
    ordinal = counts.get(key, 0)
    counts[key] = ordinal + 1
    return ordinal


def _is_type_parameter_scope(scope_type: str) -> bool:
    return scope_type in {"TYPE_PARAMETERS", "TYPE_VARIABLE"}


def _function_partitions(
    tbl: symtable.SymbolTable, *, scope_type: str
) -> Mapping[str, list[str]] | None:
    if scope_type != "FUNCTION":
        return None
    if not isinstance(tbl, symtable.Function):
        return None
    return {
        "parameters": list(tbl.get_parameters()),
        "locals": list(tbl.get_locals()),
        "globals": list(tbl.get_globals()),
        "nonlocals": list(tbl.get_nonlocals()),
        "frees": list(tbl.get_frees()),
    }


def _class_methods(tbl: symtable.SymbolTable, *, scope_type: str) -> list[str] | None:
    if scope_type != "CLASS":
        return None
    if not isinstance(tbl, symtable.Class):
        return None
    return list(tbl.get_methods())


@dataclass(frozen=True)
class _ScopeContext:
    table_id: int
    scope_id: str
    qualpath: str
    scope_type: str
    scope_type_value: int
    function_partitions: Mapping[str, list[str]] | None
    class_methods: list[str] | None
    parent_table_id: int | None


def _scope_row(
    ctx: SymtableContext,
    *,
    tbl: symtable.SymbolTable,
    scope_ctx: _ScopeContext,
) -> dict[str, object]:
    return {
        **ctx.identity_row(),
        "table_id": scope_ctx.table_id,
        "scope_id": scope_ctx.scope_id,
        "scope_local_id": scope_ctx.table_id,
        "scope_type": scope_ctx.scope_type,
        "scope_type_value": scope_ctx.scope_type_value,
        "scope_name": tbl.get_name(),
        "qualpath": scope_ctx.qualpath,
        "lineno": int(tbl.get_lineno() or 0),
        "is_nested": bool(tbl.is_nested()),
        "is_optimized": bool(tbl.is_optimized()),
        "has_children": bool(tbl.has_children()),
        "scope_role": _scope_role(scope_ctx.scope_type),
        "function_partitions": scope_ctx.function_partitions,
        "class_methods": scope_ctx.class_methods,
    }


def _scope_edge_row(
    ctx: SymtableContext, parent_table_id: int, child_table_id: int
) -> dict[str, object]:
    return {
        **ctx.identity_row(),
        "parent_table_id": parent_table_id,
        "child_table_id": child_table_id,
    }


def _symbol_rows_for_scope(
    ctx: SymtableContext,
    *,
    tbl: symtable.SymbolTable,
    scope_table_id: int,
    scope_id: str,
    scope_type: str,
) -> list[dict[str, object]]:
    symbol_rows: list[dict[str, object]] = []
    identity = ctx.identity_row()

    for sym in tbl.get_symbols():
        namespaces = list(sym.get_namespaces()) if sym.is_namespace() else []
        name = sym.get_name()
        symbol_rows.append(
            {
                "scope_table_id": scope_table_id,
                "scope_id": scope_id,
                **identity,
                "name": name,
                "is_referenced": bool(sym.is_referenced()),
                "is_assigned": bool(sym.is_assigned()),
                "is_imported": bool(sym.is_imported()),
                "is_annotated": bool(sym.is_annotated()),
                "is_parameter": bool(sym.is_parameter()),
                "is_type_parameter": _is_type_parameter_scope(scope_type),
                "is_global": bool(sym.is_global()),
                "is_declared_global": bool(sym.is_declared_global()),
                "is_nonlocal": bool(sym.is_nonlocal()),
                "is_local": bool(sym.is_local()),
                "is_free": bool(sym.is_free()),
                "is_namespace": bool(sym.is_namespace()),
                "namespace_count": len(namespaces),
                "namespace_block_ids": [int(child.get_id()) for child in namespaces],
            }
        )

    return symbol_rows


def _extract_symtable_for_context(
    file_ctx: FileContext,
    *,
    compile_type: str,
    cache: Cache | FanoutCache | None,
    cache_ttl: float | None,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    if not file_ctx.file_id or not file_ctx.path:
        return _empty_symtable_rows()

    cache_key = _symtable_cache_key(file_ctx, compile_type=compile_type)
    use_cache = cache is not None and cache_key is not None
    cache_key_str = stable_cache_key("symtable", {"key": cache_key}) if use_cache else None
    if use_cache and cache_key_str is not None:
        cached = cache_get(cache, key=cache_key_str, default=None)
        cached_rows = _parse_symtable_cache(cached)
        if cached_rows is not None:
            return cached_rows

    def _build_result() -> tuple[
        list[dict[str, object]],
        list[dict[str, object]],
        list[dict[str, object]],
    ]:
        text = text_from_file_ctx(file_ctx)
        if not text:
            return _empty_symtable_rows()
        try:
            top = symtable.symtable(text, file_ctx.path, compile_type)
        except (SyntaxError, TypeError, ValueError):
            return _empty_symtable_rows()
        ctx = SymtableContext(file_ctx=file_ctx)
        return _walk_symtable(top, ctx)

    result = _build_result()
    if use_cache and cache is not None and cache_key_str is not None:
        cache_set(
            cache,
            key=cache_key_str,
            value=result,
            options=CacheSetOptions(
                expire=cache_ttl,
                tag=file_ctx.file_id,
            ),
        )
    return result


def _parse_symtable_cache(
    cached: object,
) -> (
    tuple[
        list[dict[str, object]],
        list[dict[str, object]],
        list[dict[str, object]],
    ]
    | None
):
    if not isinstance(cached, tuple) or len(cached) != _SYMTABLE_CACHE_PARTS:
        return None
    rows: list[list[dict[str, object]]] = []
    for part in cached:
        if not isinstance(part, list):
            return None
        if not all(isinstance(item, dict) for item in part):
            return None
        rows.append(part)
    return (rows[0], rows[1], rows[2])


def _empty_symtable_rows() -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    return [], [], []


def _walk_symtable(
    top: symtable.SymbolTable,
    ctx: SymtableContext,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    scope_rows: list[dict[str, object]] = []
    symbol_rows: list[dict[str, object]] = []
    scope_edge_rows: list[dict[str, object]] = []

    sibling_counts: dict[int | None, dict[tuple[str, str, int], int]] = defaultdict(dict)
    stack: list[tuple[symtable.SymbolTable, symtable.SymbolTable | None, str]] = [(top, None, "")]
    while stack:
        tbl, parent_tbl, parent_qualpath = stack.pop()
        scope_ctx = _build_scope_context(
            tbl,
            parent_tbl=parent_tbl,
            parent_qualpath=parent_qualpath,
            sibling_counts=sibling_counts,
            ctx=ctx,
        )
        scope_rows.append(_scope_row(ctx, tbl=tbl, scope_ctx=scope_ctx))

        if scope_ctx.parent_table_id is not None:
            scope_edge_rows.append(
                _scope_edge_row(ctx, scope_ctx.parent_table_id, scope_ctx.table_id)
            )

        sym_rows = _symbol_rows_for_scope(
            ctx,
            tbl=tbl,
            scope_table_id=scope_ctx.table_id,
            scope_id=scope_ctx.scope_id,
            scope_type=scope_ctx.scope_type,
        )
        symbol_rows.extend(sym_rows)

        stack.extend((child, tbl, scope_ctx.qualpath) for child in _iter_children(tbl))

    return scope_rows, symbol_rows, scope_edge_rows


def _iter_children(tbl: symtable.SymbolTable) -> list[symtable.SymbolTable]:
    return list(tbl.get_children())


def _build_scope_context(
    tbl: symtable.SymbolTable,
    *,
    parent_tbl: symtable.SymbolTable | None,
    parent_qualpath: str,
    sibling_counts: dict[int | None, dict[tuple[str, str, int], int]],
    ctx: SymtableContext,
) -> _ScopeContext:
    scope_type = _scope_type_str(tbl)
    scope_type_value = int(tbl.get_type().value)
    lineno = int(tbl.get_lineno() or 0)
    table_id = int(tbl.get_id())
    parent_local_id = int(parent_tbl.get_id()) if parent_tbl is not None else None
    ordinal = _next_ordinal(
        sibling_counts,
        parent_local_id=parent_local_id,
        scope_type=scope_type,
        scope_name=tbl.get_name(),
        lineno=lineno,
    )
    component = _scope_component(scope_type, tbl.get_name(), ordinal)
    qualpath = _join_qualpath(parent_qualpath, component)
    scope_id = _stable_scope_id(
        file_id=ctx.file_id,
        qualpath=qualpath,
        lineno=lineno,
        scope_type=scope_type,
        ordinal=ordinal,
    )
    return _ScopeContext(
        table_id=table_id,
        scope_id=scope_id,
        qualpath=qualpath,
        scope_type=scope_type,
        scope_type_value=scope_type_value,
        function_partitions=_function_partitions(tbl, scope_type=scope_type),
        class_methods=_class_methods(tbl, scope_type=scope_type),
        parent_table_id=parent_local_id,
    )


def _symbol_entry(row: Mapping[str, object]) -> dict[str, object]:
    namespace_block_ids = row.get("namespace_block_ids")
    block_ids: list[int]
    if isinstance(namespace_block_ids, list):
        block_ids = [int(value) for value in namespace_block_ids]
    else:
        block_ids = []
    namespace_count = row.get("namespace_count")
    count = int(namespace_count) if isinstance(namespace_count, int) else 0
    return {
        "name": row.get("name"),
        "sym_symbol_id": None,
        "flags": {
            "is_referenced": bool(row.get("is_referenced")),
            "is_imported": bool(row.get("is_imported")),
            "is_parameter": bool(row.get("is_parameter")),
            "is_type_parameter": bool(row.get("is_type_parameter")),
            "is_global": bool(row.get("is_global")),
            "is_nonlocal": bool(row.get("is_nonlocal")),
            "is_declared_global": bool(row.get("is_declared_global")),
            "is_local": bool(row.get("is_local")),
            "is_annotated": bool(row.get("is_annotated")),
            "is_free": bool(row.get("is_free")),
            "is_assigned": bool(row.get("is_assigned")),
            "is_namespace": bool(row.get("is_namespace")),
        },
        "namespace_count": count,
        "namespace_block_ids": block_ids,
        "attrs": attrs_map({}),
    }


def _symtable_file_row(
    file_ctx: FileContext,
    *,
    options: SymtableExtractOptions,
    cache: Cache | FanoutCache | None,
    cache_ttl: float | None,
) -> dict[str, object] | None:
    line_offsets = _line_offsets(file_ctx)
    scope_rows, symbol_rows, scope_edge_rows = _extract_symtable_for_context(
        file_ctx,
        compile_type=options.compile_type,
        cache=cache,
        cache_ttl=cache_ttl,
    )
    if not scope_rows and not symbol_rows and not scope_edge_rows:
        return None
    parent_map = _build_parent_map(scope_edge_rows)
    symbols_by_scope = _symbols_by_scope(symbol_rows)
    blocks = _build_scope_blocks(
        scope_rows,
        parent_map=parent_map,
        symbols_by_scope=symbols_by_scope,
        options=options,
        line_offsets=line_offsets,
    )

    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "file_sha256": file_ctx.file_sha256,
        "blocks": blocks,
        "attrs": attrs_map(
            {
                "compile_type": options.compile_type,
            }
        ),
    }


def _build_parent_map(scope_edge_rows: Iterable[Mapping[str, object]]) -> dict[int, int]:
    parent_map: dict[int, int] = {}
    for edge in scope_edge_rows:
        parent_id = edge.get("parent_table_id")
        child_id = edge.get("child_table_id")
        if isinstance(parent_id, int) and isinstance(child_id, int):
            parent_map[child_id] = parent_id
    return parent_map


def _symbols_by_scope(
    symbol_rows: Iterable[Mapping[str, object]],
) -> dict[int, list[dict[str, object]]]:
    mapping: dict[int, list[dict[str, object]]] = {}
    for symbol in symbol_rows:
        scope_id = symbol.get("scope_table_id")
        if not isinstance(scope_id, int):
            continue
        mapping.setdefault(scope_id, []).append(_symbol_entry(symbol))
    return mapping


def _build_scope_blocks(
    scope_rows: Iterable[Mapping[str, object]],
    *,
    parent_map: Mapping[int, int],
    symbols_by_scope: Mapping[int, list[dict[str, object]]],
    options: SymtableExtractOptions,
    line_offsets: LineOffsets | None,
) -> list[dict[str, object]]:
    blocks: list[dict[str, object]] = []
    for scope in scope_rows:
        table_id = scope.get("table_id")
        if not isinstance(table_id, int):
            continue
        block, _scope_type = _scope_block_payload(
            scope,
            table_id=table_id,
            parent_map=parent_map,
            symbols_by_scope=symbols_by_scope,
            options=options,
            line_offsets=line_offsets,
        )
        blocks.append(block)
    return blocks


def _scope_block_payload(
    scope: Mapping[str, object],
    *,
    table_id: int,
    parent_map: Mapping[int, int],
    symbols_by_scope: Mapping[int, list[dict[str, object]]],
    options: SymtableExtractOptions,
    line_offsets: LineOffsets | None,
) -> tuple[dict[str, object], str | None]:
    lineno = scope.get("lineno")
    lineno_int = int(lineno) if isinstance(lineno, int) and lineno > 0 else None
    line0 = lineno_int - 1 if lineno_int is not None else None
    span_hint = _span_hint_from_line(line0, line_offsets=line_offsets)
    scope_type_value = scope.get("scope_type")
    scope_type = scope_type_value if isinstance(scope_type_value, str) else None
    return (
        {
            "block_id": table_id,
            "parent_block_id": parent_map.get(table_id),
            "block_type": scope_type,
            "is_meta_scope": _is_meta_scope(scope_type),
            "name": scope.get("scope_name"),
            "lineno1": lineno_int,
            "span_hint": span_hint,
            "scope_id": scope.get("scope_id"),
            "scope_local_id": scope.get("scope_local_id"),
            "scope_type_value": scope.get("scope_type_value"),
            "qualpath": scope.get("qualpath"),
            "function_partitions": scope.get("function_partitions"),
            "class_methods": scope.get("class_methods"),
            "symbols": symbols_by_scope.get(table_id, []),
            "attrs": attrs_map(
                {
                    "compile_type": options.compile_type,
                    "scope_role": scope.get("scope_role"),
                    "is_nested": scope.get("is_nested"),
                    "is_optimized": scope.get("is_optimized"),
                    "has_children": scope.get("has_children"),
                }
            ),
        },
        scope_type,
    )


def _span_hint_from_line(
    line0: int | None,
    *,
    line_offsets: LineOffsets | None,
) -> dict[str, object] | None:
    if line0 is None:
        return None
    byte_start = None
    if line_offsets is not None:
        byte_start = line_offsets.byte_offset(line0, 0)
    return span_dict(
        SpanSpec(
            start_line0=line0,
            start_col=0,
            end_line0=None,
            end_col=None,
            end_exclusive=True,
            col_unit="utf32",
            byte_start=byte_start,
            byte_len=None,
        )
    )


def _collect_symtable_file_rows(
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None,
    *,
    scope_manifest: ScopeManifest | None,
    options: SymtableExtractOptions,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> list[dict[str, object]]:
    contexts = list(
        iter_worklist_contexts(
            WorklistRequest(
                repo_files=repo_files,
                output_table="symtable_files_v1",
                runtime_profile=runtime_profile,
                file_contexts=file_contexts,
                queue_name=(
                    worklist_queue_name(output_table="symtable_files_v1", repo_id=options.repo_id)
                    if options.use_worklist_queue
                    else None
                ),
                scope_manifest=scope_manifest,
            )
        )
    )
    cache_profile = diskcache_profile_from_ctx(runtime_profile)
    cache = cache_for_extract(cache_profile)
    cache_ttl = cache_ttl_seconds(cache_profile, "extract")
    max_workers = _effective_max_workers(options)
    if max_workers <= 1:
        return [
            row
            for file_ctx in contexts
            if (
                row := _symtable_file_row(
                    file_ctx,
                    options=options,
                    cache=cache,
                    cache_ttl=cache_ttl,
                )
            )
            is not None
        ]

    def _extract_row(file_ctx: FileContext) -> dict[str, object] | None:
        return _symtable_file_row(
            file_ctx,
            options=options,
            cache=cache,
            cache_ttl=cache_ttl,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return [row for row in executor.map(_extract_row, contexts) if row is not None]


def _build_symtable_file_plan(
    rows: list[dict[str, object]],
    *,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None,
    session: ExtractSession,
) -> DataFusionPlanBundle:
    return extract_plan_from_rows(
        "symtable_files_v1",
        rows,
        session=session,
        options=ExtractPlanOptions(
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
    )


def extract_symtable(
    repo_files: TableLike,
    options: SymtableExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> ExtractResult[TableLike]:
    """Extract symbol table artifacts from repository files.

    Returns
    -------
    ExtractResult[TableLike]
        Nested symtable file table.
    """
    normalized_options = normalize_options("symtable", options, SymtableExtractOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(options=normalized_options)

    rows = _collect_symtable_file_rows(
        repo_files,
        exec_context.file_contexts,
        scope_manifest=exec_context.scope_manifest,
        options=normalized_options,
        runtime_profile=runtime_profile,
    )
    plan = _build_symtable_file_plan(
        rows,
        normalize=normalize,
        evidence_plan=exec_context.evidence_plan,
        session=session,
    )
    table = cast(
        "TableLike",
        materialize_extract_plan(
            "symtable_files_v1",
            plan,
            runtime_profile=runtime_profile,
            determinism_tier=determinism_tier,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
    )
    return ExtractResult(table=table, extractor_name="symtable")


def extract_symtable_plans(
    repo_files: TableLike,
    options: SymtableExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> dict[str, DataFusionPlanBundle]:
    """Extract symbol table plans from repository files.

    Returns
    -------
    dict[str, DataFusionPlanBundle]
        Plan bundle keyed by symtable outputs.
    """
    normalized_options = normalize_options("symtable", options, SymtableExtractOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    evidence_plan = exec_context.evidence_plan
    rows = _collect_symtable_file_rows(
        repo_files,
        exec_context.file_contexts,
        scope_manifest=exec_context.scope_manifest,
        options=normalized_options,
        runtime_profile=runtime_profile,
    )
    return {
        "symtable_files": _build_symtable_file_plan(
            rows,
            normalize=normalize,
            evidence_plan=evidence_plan,
            session=session,
        )
    }


class _SymtableTableKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: SymtableExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: bool


class _SymtableTableKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: SymtableExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: Literal[False]


class _SymtableTableKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: SymtableExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: Required[Literal[True]]


@overload
def extract_symtables_table(
    **kwargs: Unpack[_SymtableTableKwargsTable],
) -> TableLike: ...


@overload
def extract_symtables_table(
    **kwargs: Unpack[_SymtableTableKwargsReader],
) -> TableLike | RecordBatchReaderLike: ...


def extract_symtables_table(
    **kwargs: Unpack[_SymtableTableKwargs],
) -> TableLike | RecordBatchReaderLike:
    """Extract symtable data into a single table.

    Parameters
    ----------
    kwargs:
        Keyword-only arguments for extraction (repo_files, options, file_contexts, ctx, profile,
        prefer_reader).

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Symtable extraction output.
    """
    with stage_span(
        "extract.symtable_table",
        stage="extract",
        scope_name=SCOPE_EXTRACT,
        attributes={"codeanatomy.extractor": "symtable"},
    ):
        repo_files = kwargs["repo_files"]
        normalized_options = normalize_options(
            "symtable",
            kwargs.get("options"),
            SymtableExtractOptions,
        )
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
        plans = extract_symtable_plans(
            repo_files,
            options=normalized_options,
            context=exec_context,
        )
        return materialize_extract_plan(
            "symtable_files_v1",
            plans["symtable_files"],
            runtime_profile=runtime_profile,
            determinism_tier=determinism_tier,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        )
