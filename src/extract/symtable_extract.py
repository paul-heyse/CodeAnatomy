"""Extract Python symtable data into Arrow tables using shared helpers."""

from __future__ import annotations

import symtable
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

import ibis
import pyarrow as pa

from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import ArrayLike, RecordBatchReaderLike, TableLike
from arrowdsl.schema.build import table_from_arrays
from arrowdsl.schema.metadata import normalize_dictionaries
from arrowdsl.schema.nested_builders import LargeListViewAccumulator
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    FileContext,
    apply_query_and_project,
    file_identity_row,
    ibis_plan_from_rows,
    iter_contexts,
    materialize_extract_plan,
    text_from_file_ctx,
)
from extract.registry_specs import dataset_row_schema, normalize_options
from extract.schema_ops import ExtractNormalizeOptions
from ibis_engine.plan import IbisPlan
from schema_spec.specs import NestedFieldSpec

if TYPE_CHECKING:
    from extract.evidence_plan import EvidencePlan


@dataclass(frozen=True)
class SymtableExtractOptions:
    """Configure symtable extraction."""

    compile_type: str = "exec"


@dataclass(frozen=True)
class SymtableExtractResult:
    """Hold extracted symtable tables for scopes, symbols, and edges."""

    py_sym_scopes: TableLike
    py_sym_symbols: TableLike
    py_sym_scope_edges: TableLike
    py_sym_namespace_edges: TableLike
    py_sym_function_partitions: TableLike


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


SCOPES_ROW_SCHEMA = dataset_row_schema("py_sym_scopes_v1")
SYMBOLS_ROW_SCHEMA = dataset_row_schema("py_sym_symbols_v1")
SCOPE_EDGES_ROW_SCHEMA = dataset_row_schema("py_sym_scope_edges_v1")
NAMESPACE_EDGES_ROW_SCHEMA = dataset_row_schema("py_sym_namespace_edges_v1")
FUNC_PARTS_ROW_SCHEMA = dataset_row_schema("py_sym_function_partitions_v1")


def _scope_type_str(tbl: symtable.SymbolTable) -> str:
    return str(tbl.get_type()).split(".")[-1]


def _scope_role(scope_type_str: str) -> str:
    return "runtime" if scope_type_str in {"MODULE", "FUNCTION", "CLASS"} else "type_meta"


def _string_list_view_accumulator() -> LargeListViewAccumulator[str | None]:
    return LargeListViewAccumulator()


@dataclass
class _FuncPartsAccumulator:
    scope_table_ids: list[int] = field(default_factory=list)
    file_ids: list[str] = field(default_factory=list)
    paths: list[str] = field(default_factory=list)
    file_sha256s: list[str | None] = field(default_factory=list)
    parameters_acc: LargeListViewAccumulator[str | None] = field(
        default_factory=_string_list_view_accumulator
    )
    locals_acc: LargeListViewAccumulator[str | None] = field(
        default_factory=_string_list_view_accumulator
    )
    globals_acc: LargeListViewAccumulator[str | None] = field(
        default_factory=_string_list_view_accumulator
    )
    nonlocals_acc: LargeListViewAccumulator[str | None] = field(
        default_factory=_string_list_view_accumulator
    )
    frees_acc: LargeListViewAccumulator[str | None] = field(
        default_factory=_string_list_view_accumulator
    )

    def append(self, ctx: SymtableContext, scope_table_id: int, tbl: symtable.SymbolTable) -> None:
        if _scope_type_str(tbl) != "FUNCTION":
            return
        self.scope_table_ids.append(scope_table_id)
        self.file_ids.append(ctx.file_id)
        self.paths.append(ctx.path)
        self.file_sha256s.append(ctx.file_sha256)
        self.parameters_acc.append(_symbol_list(tbl, "get_parameters"))
        self.locals_acc.append(_symbol_list(tbl, "get_locals"))
        self.globals_acc.append(_symbol_list(tbl, "get_globals"))
        self.nonlocals_acc.append(_symbol_list(tbl, "get_nonlocals"))
        self.frees_acc.append(_symbol_list(tbl, "get_frees"))

    def extend(self, other: _FuncPartsAccumulator) -> None:
        self.scope_table_ids.extend(other.scope_table_ids)
        self.file_ids.extend(other.file_ids)
        self.paths.extend(other.paths)
        self.file_sha256s.extend(other.file_sha256s)
        self.parameters_acc.extend_from(other.parameters_acc)
        self.locals_acc.extend_from(other.locals_acc)
        self.globals_acc.extend_from(other.globals_acc)
        self.nonlocals_acc.extend_from(other.nonlocals_acc)
        self.frees_acc.extend_from(other.frees_acc)

    def to_table(self) -> TableLike:
        parameters = FUNC_PARTS_PARAMETERS_SPEC.builder(self)
        locals_ = FUNC_PARTS_LOCALS_SPEC.builder(self)
        globals_ = FUNC_PARTS_GLOBALS_SPEC.builder(self)
        nonlocals = FUNC_PARTS_NONLOCALS_SPEC.builder(self)
        frees = FUNC_PARTS_FREES_SPEC.builder(self)
        columns = {
            "file_id": pa.array(self.file_ids, type=pa.string()),
            "path": pa.array(self.paths, type=pa.string()),
            "file_sha256": pa.array(self.file_sha256s, type=pa.string()),
            "scope_table_id": pa.array(self.scope_table_ids, type=pa.int64()),
            "parameters": parameters,
            "locals": locals_,
            "globals": globals_,
            "nonlocals": nonlocals,
            "frees": frees,
        }
        table = table_from_arrays(
            FUNC_PARTS_ROW_SCHEMA,
            columns=columns,
            num_rows=len(self.scope_table_ids),
        )
        return normalize_dictionaries(table)


def _build_func_parameters(acc: _FuncPartsAccumulator) -> ArrayLike:
    return acc.parameters_acc.build(value_type=pa.string())


def _build_func_locals(acc: _FuncPartsAccumulator) -> ArrayLike:
    return acc.locals_acc.build(value_type=pa.string())


def _build_func_globals(acc: _FuncPartsAccumulator) -> ArrayLike:
    return acc.globals_acc.build(value_type=pa.string())


def _build_func_nonlocals(acc: _FuncPartsAccumulator) -> ArrayLike:
    return acc.nonlocals_acc.build(value_type=pa.string())


def _build_func_frees(acc: _FuncPartsAccumulator) -> ArrayLike:
    return acc.frees_acc.build(value_type=pa.string())


FUNC_PARTS_PARAMETERS_SPEC = NestedFieldSpec(
    name="parameters",
    dtype=pa.large_list(pa.string()),
    builder=_build_func_parameters,
)

FUNC_PARTS_LOCALS_SPEC = NestedFieldSpec(
    name="locals",
    dtype=pa.large_list(pa.string()),
    builder=_build_func_locals,
)

FUNC_PARTS_GLOBALS_SPEC = NestedFieldSpec(
    name="globals",
    dtype=pa.large_list(pa.string()),
    builder=_build_func_globals,
)

FUNC_PARTS_NONLOCALS_SPEC = NestedFieldSpec(
    name="nonlocals",
    dtype=pa.large_list(pa.string()),
    builder=_build_func_nonlocals,
)

FUNC_PARTS_FREES_SPEC = NestedFieldSpec(
    name="frees",
    dtype=pa.large_list(pa.string()),
    builder=_build_func_frees,
)


def _scope_row(ctx: SymtableContext, table_id: int, tbl: symtable.SymbolTable) -> dict[str, object]:
    st_str = _scope_type_str(tbl)
    return {
        **ctx.identity_row(),
        "table_id": table_id,
        "scope_type": st_str,
        "scope_name": tbl.get_name(),
        "lineno": int(tbl.get_lineno() or 0),
        "is_nested": bool(tbl.is_nested()),
        "is_optimized": bool(tbl.is_optimized()),
        "has_children": bool(tbl.has_children()),
        "scope_role": _scope_role(st_str),
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
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    symbol_rows: list[dict[str, object]] = []
    ns_edge_rows: list[dict[str, object]] = []
    identity = ctx.identity_row()

    for sym in tbl.get_symbols():
        name = sym.get_name()
        symbol_rows.append(
            {
                "scope_table_id": scope_table_id,
                **identity,
                "name": name,
                "is_referenced": bool(sym.is_referenced()),
                "is_assigned": bool(sym.is_assigned()),
                "is_imported": bool(sym.is_imported()),
                "is_annotated": bool(sym.is_annotated()),
                "is_parameter": bool(sym.is_parameter()),
                "is_global": bool(sym.is_global()),
                "is_declared_global": bool(sym.is_declared_global()),
                "is_nonlocal": bool(sym.is_nonlocal()),
                "is_local": bool(sym.is_local()),
                "is_free": bool(sym.is_free()),
                "is_namespace": bool(sym.is_namespace()),
            }
        )

        if bool(sym.is_namespace()):
            for nt in _iter_namespaces(sym):
                child_table_id = int(nt.get_id())
                ns_edge_rows.append(
                    {
                        **identity,
                        "scope_table_id": scope_table_id,
                        "symbol_name": name,
                        "child_table_id": child_table_id,
                    }
                )

    return symbol_rows, ns_edge_rows


def _extract_symtable_for_context(
    file_ctx: FileContext,
    *,
    compile_type: str,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    _FuncPartsAccumulator,
]:
    if not file_ctx.file_id or not file_ctx.path:
        return [], [], [], [], _FuncPartsAccumulator()

    text = text_from_file_ctx(file_ctx)
    if not text:
        return [], [], [], [], _FuncPartsAccumulator()

    try:
        top = symtable.symtable(text, file_ctx.path, compile_type)
    except (SyntaxError, TypeError, ValueError):
        return [], [], [], [], _FuncPartsAccumulator()

    ctx = SymtableContext(file_ctx=file_ctx)
    return _walk_symtable(top, ctx)


def _extract_symtable_for_row(
    row: dict[str, object],
    *,
    compile_type: str,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    _FuncPartsAccumulator,
]:
    file_ctx = FileContext.from_repo_row(row)
    return _extract_symtable_for_context(file_ctx, compile_type=compile_type)


def _walk_symtable(
    top: symtable.SymbolTable,
    ctx: SymtableContext,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    _FuncPartsAccumulator,
]:
    scope_rows: list[dict[str, object]] = []
    symbol_rows: list[dict[str, object]] = []
    scope_edge_rows: list[dict[str, object]] = []
    ns_edge_rows: list[dict[str, object]] = []
    func_parts = _FuncPartsAccumulator()

    stack: list[tuple[symtable.SymbolTable, symtable.SymbolTable | None]] = [(top, None)]
    while stack:
        tbl, parent_tbl = stack.pop()
        table_id = int(tbl.get_id())
        scope_rows.append(_scope_row(ctx, table_id, tbl))

        if parent_tbl is not None:
            parent_table_id = int(parent_tbl.get_id())
            scope_edge_rows.append(_scope_edge_row(ctx, parent_table_id, table_id))

        sym_rows, ns_rows = _symbol_rows_for_scope(
            ctx,
            tbl=tbl,
            scope_table_id=table_id,
        )
        symbol_rows.extend(sym_rows)
        ns_edge_rows.extend(ns_rows)

        func_parts.append(ctx, table_id, tbl)

        stack.extend((child, tbl) for child in _iter_children(tbl))

    return scope_rows, symbol_rows, scope_edge_rows, ns_edge_rows, func_parts


def _symbol_list(tbl: symtable.SymbolTable, name: str) -> list[str]:
    fn = getattr(tbl, name, None)
    if callable(fn):
        values = cast("Sequence[str]", fn())
        return list(values)
    return []


def _iter_children(tbl: symtable.SymbolTable) -> list[symtable.SymbolTable]:
    return list(tbl.get_children())


def _iter_namespaces(sym: symtable.Symbol) -> list[symtable.SymbolTable]:
    return list(sym.get_namespaces())


@dataclass(frozen=True)
class _SymtableRows:
    scope_rows: list[dict[str, object]]
    symbol_rows: list[dict[str, object]]
    scope_edge_rows: list[dict[str, object]]
    ns_edge_rows: list[dict[str, object]]
    func_parts_acc: _FuncPartsAccumulator


def _collect_symtable_rows(
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None,
    *,
    compile_type: str,
) -> _SymtableRows:
    rows = _SymtableRows(
        scope_rows=[],
        symbol_rows=[],
        scope_edge_rows=[],
        ns_edge_rows=[],
        func_parts_acc=_FuncPartsAccumulator(),
    )

    for file_ctx in iter_contexts(repo_files, file_contexts):
        (
            file_scope_rows,
            file_symbol_rows,
            file_scope_edge_rows,
            file_ns_edge_rows,
            file_func_parts_acc,
        ) = _extract_symtable_for_context(
            file_ctx,
            compile_type=compile_type,
        )
        rows.scope_rows.extend(file_scope_rows)
        rows.symbol_rows.extend(file_symbol_rows)
        rows.scope_edge_rows.extend(file_scope_edge_rows)
        rows.ns_edge_rows.extend(file_ns_edge_rows)
        rows.func_parts_acc.extend(file_func_parts_acc)

    return rows


def extract_symtable(
    repo_files: TableLike,
    options: SymtableExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> SymtableExtractResult:
    """Extract symbol table artifacts from repository files.

    Returns
    -------
    SymtableExtractResult
        Tables for scopes, symbols, and namespace edges.
    """
    normalized_options = normalize_options("symtable", options, SymtableExtractOptions)
    exec_context = context or ExtractExecutionContext()
    ctx = exec_context.ensure_ctx()
    normalize = ExtractNormalizeOptions(options=normalized_options)

    rows = _collect_symtable_rows(
        repo_files,
        exec_context.file_contexts,
        compile_type=normalized_options.compile_type,
    )
    plans = _build_symtable_plans(
        rows,
        normalize=normalize,
        evidence_plan=exec_context.evidence_plan,
    )
    return SymtableExtractResult(
        py_sym_scopes=materialize_extract_plan(
            "py_sym_scopes_v1",
            plans["py_sym_scopes"],
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
        py_sym_symbols=materialize_extract_plan(
            "py_sym_symbols_v1",
            plans["py_sym_symbols"],
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
        py_sym_scope_edges=materialize_extract_plan(
            "py_sym_scope_edges_v1",
            plans["py_sym_scope_edges"],
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
        py_sym_namespace_edges=materialize_extract_plan(
            "py_sym_namespace_edges_v1",
            plans["py_sym_namespace_edges"],
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
        py_sym_function_partitions=materialize_extract_plan(
            "py_sym_function_partitions_v1",
            plans["py_sym_function_partitions"],
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
    )


def extract_symtable_plans(
    repo_files: TableLike,
    options: SymtableExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> dict[str, IbisPlan]:
    """Extract symbol table plans from repository files.

    Returns
    -------
    dict[str, IbisPlan]
        Ibis plan bundle keyed by symtable outputs.
    """
    normalized_options = normalize_options("symtable", options, SymtableExtractOptions)
    exec_context = context or ExtractExecutionContext()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    evidence_plan = exec_context.evidence_plan
    rows = _collect_symtable_rows(
        repo_files,
        exec_context.file_contexts,
        compile_type=normalized_options.compile_type,
    )
    return _build_symtable_plans(rows, normalize=normalize, evidence_plan=evidence_plan)


def _build_scopes(
    scope_rows: list[dict[str, object]],
    *,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None = None,
) -> tuple[IbisPlan, IbisPlan]:
    raw_plan = ibis_plan_from_rows(
        "py_sym_scopes_v1",
        scope_rows,
        row_schema=SCOPES_ROW_SCHEMA,
    )
    scopes_plan = apply_query_and_project(
        "py_sym_scopes_v1",
        raw_plan.expr,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )
    scopes_table = scopes_plan.expr
    scope_keys = scopes_table.select(
        scopes_table["file_id"],
        scopes_table["table_id"],
        scopes_table["scope_id"],
    )
    scope_key_plan = IbisPlan(expr=scope_keys, ordering=scopes_plan.ordering)
    return scopes_plan, scope_key_plan


def _build_symbols(
    symbol_rows: list[dict[str, object]],
    *,
    scope_key_plan: IbisPlan,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None = None,
) -> IbisPlan:
    raw_plan = ibis_plan_from_rows(
        "py_sym_symbols_v1",
        symbol_rows,
        row_schema=SYMBOLS_ROW_SCHEMA,
    )
    symbols_table = raw_plan.expr
    scope_keys = scope_key_plan.expr
    joined = symbols_table.left_join(
        scope_keys,
        predicates=[
            symbols_table["file_id"] == scope_keys["file_id"],
            symbols_table["scope_table_id"] == scope_keys["table_id"],
        ],
    )
    selected = joined.select(
        [symbols_table[col] for col in symbols_table.columns] + [scope_keys["scope_id"]]
    )
    return apply_query_and_project(
        "py_sym_symbols_v1",
        selected,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )


def _build_scope_edges(
    scope_edge_rows: list[dict[str, object]],
    *,
    scope_key_plan: IbisPlan,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None = None,
) -> IbisPlan:
    raw_plan = ibis_plan_from_rows(
        "py_sym_scope_edges_v1",
        scope_edge_rows,
        row_schema=SCOPE_EDGES_ROW_SCHEMA,
    )
    edges_table = raw_plan.expr
    scope_keys_parent = scope_key_plan.expr
    parent_join = edges_table.left_join(
        scope_keys_parent,
        predicates=[
            edges_table["file_id"] == scope_keys_parent["file_id"],
            edges_table["parent_table_id"] == scope_keys_parent["table_id"],
        ],
    )
    parent_selected = parent_join.select(
        [edges_table[col] for col in edges_table.columns]
        + [scope_keys_parent["scope_id"].name("parent_scope_id")]
    )
    scope_keys_child = scope_key_plan.expr.view()
    child_join = parent_selected.left_join(
        scope_keys_child,
        predicates=[
            parent_selected["file_id"] == scope_keys_child["file_id"],
            parent_selected["child_table_id"] == scope_keys_child["table_id"],
        ],
    )
    child_selected = child_join.select(
        [parent_selected[col] for col in parent_selected.columns]
        + [scope_keys_child["scope_id"].name("child_scope_id")]
    )
    return apply_query_and_project(
        "py_sym_scope_edges_v1",
        child_selected,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )


def _build_namespace_edges(
    ns_edge_rows: list[dict[str, object]],
    *,
    scope_key_plan: IbisPlan,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None = None,
) -> IbisPlan:
    raw_plan = ibis_plan_from_rows(
        "py_sym_namespace_edges_v1",
        ns_edge_rows,
        row_schema=NAMESPACE_EDGES_ROW_SCHEMA,
    )
    ns_table = raw_plan.expr
    scope_keys = scope_key_plan.expr
    scope_join = ns_table.left_join(
        scope_keys,
        predicates=[
            ns_table["file_id"] == scope_keys["file_id"],
            ns_table["scope_table_id"] == scope_keys["table_id"],
        ],
    )
    scope_selected = scope_join.select(
        [ns_table[col] for col in ns_table.columns] + [scope_keys["scope_id"]]
    )
    child_keys = scope_key_plan.expr.view()
    child_join = scope_selected.left_join(
        child_keys,
        predicates=[
            scope_selected["file_id"] == child_keys["file_id"],
            scope_selected["child_table_id"] == child_keys["table_id"],
        ],
    )
    child_selected = child_join.select(
        [scope_selected[col] for col in scope_selected.columns]
        + [child_keys["scope_id"].name("child_scope_id")]
    )
    return apply_query_and_project(
        "py_sym_namespace_edges_v1",
        child_selected,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )


def _build_func_parts(
    func_parts_acc: _FuncPartsAccumulator,
    *,
    scope_key_plan: IbisPlan,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None = None,
) -> IbisPlan:
    func_parts_table = func_parts_acc.to_table()
    if func_parts_table.num_rows == 0 or not {
        "file_id",
        "scope_table_id",
    } <= set(func_parts_table.column_names):
        raw_plan = ibis_plan_from_rows(
            "py_sym_function_partitions_v1",
            [],
            row_schema=FUNC_PARTS_ROW_SCHEMA,
        )
    else:
        raw_plan = IbisPlan(expr=ibis.memtable(func_parts_table), ordering=scope_key_plan.ordering)
    func_table = raw_plan.expr
    scope_keys = scope_key_plan.expr
    joined = func_table.left_join(
        scope_keys,
        predicates=[
            func_table["file_id"] == scope_keys["file_id"],
            func_table["scope_table_id"] == scope_keys["table_id"],
        ],
    )
    selected = joined.select(
        [func_table[col] for col in func_table.columns] + [scope_keys["scope_id"]]
    )
    return apply_query_and_project(
        "py_sym_function_partitions_v1",
        selected,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )


def _build_symtable_plans(
    rows: _SymtableRows,
    *,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None = None,
) -> dict[str, IbisPlan]:
    scopes_plan, scope_key_plan = _build_scopes(
        rows.scope_rows,
        normalize=normalize,
        evidence_plan=evidence_plan,
    )
    symbols_plan = _build_symbols(
        rows.symbol_rows,
        scope_key_plan=scope_key_plan,
        normalize=normalize,
        evidence_plan=evidence_plan,
    )
    scope_edges_plan = _build_scope_edges(
        rows.scope_edge_rows,
        scope_key_plan=scope_key_plan,
        normalize=normalize,
        evidence_plan=evidence_plan,
    )
    ns_edges_plan = _build_namespace_edges(
        rows.ns_edge_rows,
        scope_key_plan=scope_key_plan,
        normalize=normalize,
        evidence_plan=evidence_plan,
    )
    func_parts_plan = _build_func_parts(
        rows.func_parts_acc,
        scope_key_plan=scope_key_plan,
        normalize=normalize,
        evidence_plan=evidence_plan,
    )

    return {
        "py_sym_scopes": scopes_plan,
        "py_sym_symbols": symbols_plan,
        "py_sym_scope_edges": scope_edges_plan,
        "py_sym_namespace_edges": ns_edges_plan,
        "py_sym_function_partitions": func_parts_plan,
    }


class _SymtableTableKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: SymtableExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    prefer_reader: bool


class _SymtableTableKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: SymtableExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    prefer_reader: Literal[False]


class _SymtableTableKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: SymtableExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
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
    repo_files = kwargs["repo_files"]
    normalized_options = normalize_options(
        "symtable",
        kwargs.get("options"),
        SymtableExtractOptions,
    )
    file_contexts = kwargs.get("file_contexts")
    evidence_plan = kwargs.get("evidence_plan")
    profile = kwargs.get("profile", "default")
    exec_ctx = kwargs.get("ctx") or execution_context_factory(profile)
    prefer_reader = kwargs.get("prefer_reader", False)
    normalize = ExtractNormalizeOptions(options=normalized_options)
    plans = extract_symtable_plans(
        repo_files,
        options=normalized_options,
        context=ExtractExecutionContext(
            file_contexts=file_contexts,
            evidence_plan=evidence_plan,
            ctx=exec_ctx,
            profile=profile,
        ),
    )
    return materialize_extract_plan(
        "py_sym_scopes_v1",
        plans["py_sym_scopes"],
        ctx=exec_ctx,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            prefer_reader=prefer_reader,
            apply_post_kernels=True,
        ),
    )
