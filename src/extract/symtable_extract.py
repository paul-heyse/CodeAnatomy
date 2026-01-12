"""Extract Python symtable data into Arrow tables using shared helpers."""

from __future__ import annotations

import symtable
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Literal, cast, overload

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext, RuntimeProfile
from arrowdsl.core.ids import HashSpec
from arrowdsl.core.interop import ArrayLike, RecordBatchReaderLike, TableLike, pc
from arrowdsl.plan.plan import Plan
from arrowdsl.schema.schema import empty_table
from extract.common import file_identity_row, iter_contexts, text_from_file_ctx
from extract.file_context import FileContext
from extract.hashing import apply_hash_projection
from extract.join_helpers import JoinConfig, left_join
from extract.nested_lists import ListAccumulator
from extract.spec_helpers import register_dataset
from extract.tables import (
    align_plan,
    apply_query_spec,
    finalize_plan_bundle,
    materialize_plan,
    plan_from_rows,
    query_for_schema,
    rename_plan_columns,
)
from schema_spec.specs import ArrowFieldSpec, NestedFieldSpec, file_identity_bundle

SCHEMA_VERSION = 1


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


SCOPES_SPEC = register_dataset(
    name="py_sym_scopes_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=[
        ArrowFieldSpec(name="scope_id", dtype=pa.string()),
        ArrowFieldSpec(name="table_id", dtype=pa.int64()),
        ArrowFieldSpec(name="scope_type", dtype=pa.string()),
        ArrowFieldSpec(name="scope_name", dtype=pa.string()),
        ArrowFieldSpec(name="lineno", dtype=pa.int32()),
        ArrowFieldSpec(name="is_nested", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_optimized", dtype=pa.bool_()),
        ArrowFieldSpec(name="has_children", dtype=pa.bool_()),
        ArrowFieldSpec(name="scope_role", dtype=pa.string()),
    ],
)

SYMBOLS_SPEC = register_dataset(
    name="py_sym_symbols_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=[
        ArrowFieldSpec(name="symbol_row_id", dtype=pa.string()),
        ArrowFieldSpec(name="scope_id", dtype=pa.string()),
        ArrowFieldSpec(name="name", dtype=pa.string()),
        ArrowFieldSpec(name="is_referenced", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_assigned", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_imported", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_annotated", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_parameter", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_global", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_declared_global", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_nonlocal", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_local", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_free", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_namespace", dtype=pa.bool_()),
    ],
)

SCOPE_EDGES_SPEC = register_dataset(
    name="py_sym_scope_edges_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=[
        ArrowFieldSpec(name="edge_id", dtype=pa.string()),
        ArrowFieldSpec(name="parent_scope_id", dtype=pa.string()),
        ArrowFieldSpec(name="child_scope_id", dtype=pa.string()),
    ],
)

NAMESPACE_EDGES_SPEC = register_dataset(
    name="py_sym_namespace_edges_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=[
        ArrowFieldSpec(name="edge_id", dtype=pa.string()),
        ArrowFieldSpec(name="scope_id", dtype=pa.string()),
        ArrowFieldSpec(name="symbol_row_id", dtype=pa.string()),
        ArrowFieldSpec(name="child_scope_id", dtype=pa.string()),
    ],
)

FUNC_PARTS_SPEC = register_dataset(
    name="py_sym_function_partitions_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=[
        ArrowFieldSpec(name="scope_id", dtype=pa.string()),
        ArrowFieldSpec(name="parameters", dtype=pa.list_(pa.string())),
        ArrowFieldSpec(name="locals", dtype=pa.list_(pa.string())),
        ArrowFieldSpec(name="globals", dtype=pa.list_(pa.string())),
        ArrowFieldSpec(name="nonlocals", dtype=pa.list_(pa.string())),
        ArrowFieldSpec(name="frees", dtype=pa.list_(pa.string())),
    ],
)

SCOPES_SCHEMA = SCOPES_SPEC.table_spec.to_arrow_schema()
SYMBOLS_SCHEMA = SYMBOLS_SPEC.table_spec.to_arrow_schema()
SCOPE_EDGES_SCHEMA = SCOPE_EDGES_SPEC.table_spec.to_arrow_schema()
NAMESPACE_EDGES_SCHEMA = NAMESPACE_EDGES_SPEC.table_spec.to_arrow_schema()
FUNC_PARTS_SCHEMA = FUNC_PARTS_SPEC.table_spec.to_arrow_schema()

SYMBOL_ROWS_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.string()),
        pa.field("path", pa.string()),
        pa.field("file_sha256", pa.string()),
        pa.field("scope_table_id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("is_referenced", pa.bool_()),
        pa.field("is_assigned", pa.bool_()),
        pa.field("is_imported", pa.bool_()),
        pa.field("is_annotated", pa.bool_()),
        pa.field("is_parameter", pa.bool_()),
        pa.field("is_global", pa.bool_()),
        pa.field("is_declared_global", pa.bool_()),
        pa.field("is_nonlocal", pa.bool_()),
        pa.field("is_local", pa.bool_()),
        pa.field("is_free", pa.bool_()),
        pa.field("is_namespace", pa.bool_()),
    ]
)

SCOPE_EDGE_ROWS_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.string()),
        pa.field("path", pa.string()),
        pa.field("file_sha256", pa.string()),
        pa.field("parent_table_id", pa.int64()),
        pa.field("child_table_id", pa.int64()),
    ]
)

NAMESPACE_EDGE_ROWS_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.string()),
        pa.field("path", pa.string()),
        pa.field("file_sha256", pa.string()),
        pa.field("scope_table_id", pa.int64()),
        pa.field("symbol_name", pa.string()),
        pa.field("child_table_id", pa.int64()),
    ]
)

SCOPES_QUERY = query_for_schema(SCOPES_SCHEMA)
SYMBOLS_QUERY = query_for_schema(SYMBOLS_SCHEMA)
SCOPE_EDGES_QUERY = query_for_schema(SCOPE_EDGES_SCHEMA)
NAMESPACE_EDGES_QUERY = query_for_schema(NAMESPACE_EDGES_SCHEMA)
FUNC_PARTS_QUERY = query_for_schema(FUNC_PARTS_SCHEMA)


def _scope_type_str(tbl: symtable.SymbolTable) -> str:
    return str(tbl.get_type()).split(".")[-1]


def _scope_role(scope_type_str: str) -> str:
    return "runtime" if scope_type_str in {"MODULE", "FUNCTION", "CLASS"} else "type_meta"


@dataclass
class _FuncPartsAccumulator:
    scope_table_ids: list[int] = field(default_factory=list)
    file_ids: list[str] = field(default_factory=list)
    paths: list[str] = field(default_factory=list)
    file_sha256s: list[str | None] = field(default_factory=list)
    parameters_acc: ListAccumulator[str | None] = field(default_factory=ListAccumulator)
    locals_acc: ListAccumulator[str | None] = field(default_factory=ListAccumulator)
    globals_acc: ListAccumulator[str | None] = field(default_factory=ListAccumulator)
    nonlocals_acc: ListAccumulator[str | None] = field(default_factory=ListAccumulator)
    frees_acc: ListAccumulator[str | None] = field(default_factory=ListAccumulator)

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
        return pa.Table.from_arrays(
            [
                pa.array(self.file_ids, type=pa.string()),
                pa.array(self.paths, type=pa.string()),
                pa.array(self.file_sha256s, type=pa.string()),
                pa.array(self.scope_table_ids, type=pa.int64()),
                parameters,
                locals_,
                globals_,
                nonlocals,
                frees,
            ],
            names=[
                "file_id",
                "path",
                "file_sha256",
                "scope_table_id",
                "parameters",
                "locals",
                "globals",
                "nonlocals",
                "frees",
            ],
        )


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
    dtype=pa.list_(pa.string()),
    builder=_build_func_parameters,
)

FUNC_PARTS_LOCALS_SPEC = NestedFieldSpec(
    name="locals",
    dtype=pa.list_(pa.string()),
    builder=_build_func_locals,
)

FUNC_PARTS_GLOBALS_SPEC = NestedFieldSpec(
    name="globals",
    dtype=pa.list_(pa.string()),
    builder=_build_func_globals,
)

FUNC_PARTS_NONLOCALS_SPEC = NestedFieldSpec(
    name="nonlocals",
    dtype=pa.list_(pa.string()),
    builder=_build_func_nonlocals,
)

FUNC_PARTS_FREES_SPEC = NestedFieldSpec(
    name="frees",
    dtype=pa.list_(pa.string()),
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


def extract_symtable(
    repo_files: TableLike,
    options: SymtableExtractOptions | None = None,
    *,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
) -> SymtableExtractResult:
    """Extract symbol table artifacts from repository files.

    Returns
    -------
    SymtableExtractResult
        Tables for scopes, symbols, and namespace edges.
    """
    options = options or SymtableExtractOptions()
    ctx = ctx or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))

    scope_rows: list[dict[str, object]] = []
    symbol_rows: list[dict[str, object]] = []
    scope_edge_rows: list[dict[str, object]] = []
    ns_edge_rows: list[dict[str, object]] = []
    func_parts_acc = _FuncPartsAccumulator()

    for file_ctx in iter_contexts(repo_files, file_contexts):
        (
            file_scope_rows,
            file_symbol_rows,
            file_scope_edge_rows,
            file_ns_edge_rows,
            file_func_parts_acc,
        ) = _extract_symtable_for_context(file_ctx, compile_type=options.compile_type)
        scope_rows.extend(file_scope_rows)
        symbol_rows.extend(file_symbol_rows)
        scope_edge_rows.extend(file_scope_edge_rows)
        ns_edge_rows.extend(file_ns_edge_rows)
        func_parts_acc.extend(file_func_parts_acc)

    rows = _SymtableRows(
        scope_rows=scope_rows,
        symbol_rows=symbol_rows,
        scope_edge_rows=scope_edge_rows,
        ns_edge_rows=ns_edge_rows,
        func_parts_acc=func_parts_acc,
    )
    return _build_symtable_result(
        rows,
        ctx=ctx,
    )


def extract_symtable_plans(
    repo_files: TableLike,
    options: SymtableExtractOptions | None = None,
    *,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
) -> dict[str, Plan]:
    """Extract symbol table plans from repository files.

    Returns
    -------
    dict[str, Plan]
        Plan bundle keyed by symtable outputs.
    """
    options = options or SymtableExtractOptions()
    ctx = ctx or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))

    scope_rows: list[dict[str, object]] = []
    symbol_rows: list[dict[str, object]] = []
    scope_edge_rows: list[dict[str, object]] = []
    ns_edge_rows: list[dict[str, object]] = []
    func_parts_acc = _FuncPartsAccumulator()

    for file_ctx in iter_contexts(repo_files, file_contexts):
        (
            file_scope_rows,
            file_symbol_rows,
            file_scope_edge_rows,
            file_ns_edge_rows,
            file_func_parts_acc,
        ) = _extract_symtable_for_context(file_ctx, compile_type=options.compile_type)
        scope_rows.extend(file_scope_rows)
        symbol_rows.extend(file_symbol_rows)
        scope_edge_rows.extend(file_scope_edge_rows)
        ns_edge_rows.extend(file_ns_edge_rows)
        func_parts_acc.extend(file_func_parts_acc)

    rows = _SymtableRows(
        scope_rows=scope_rows,
        symbol_rows=symbol_rows,
        scope_edge_rows=scope_edge_rows,
        ns_edge_rows=ns_edge_rows,
        func_parts_acc=func_parts_acc,
    )
    return _build_symtable_plans(rows, ctx=ctx)


def _build_scopes(
    scope_rows: list[dict[str, object]],
    *,
    ctx: ExecutionContext,
) -> tuple[Plan, Plan]:
    scopes_plan = plan_from_rows(scope_rows, schema=SCOPES_SCHEMA, label="sym_scopes")
    scopes_plan = apply_hash_projection(
        scopes_plan,
        specs=(
            HashSpec(
                prefix="sym_scope",
                cols=("file_id", "table_id", "scope_type", "scope_name", "lineno"),
                out_col="scope_id",
            ),
        ),
        available=SCOPES_SCHEMA.names,
        required={"scope_id": ("file_id", "table_id", "scope_type", "scope_name", "lineno")},
        ctx=ctx,
    )
    scope_key_plan = scopes_plan.project(
        [pc.field("file_id"), pc.field("table_id"), pc.field("scope_id")],
        ["file_id", "table_id", "scope_id"],
        ctx=ctx,
    )
    scopes_plan = apply_query_spec(scopes_plan, spec=SCOPES_QUERY, ctx=ctx)
    scopes_plan = align_plan(
        scopes_plan,
        schema=SCOPES_SCHEMA,
        available=SCOPES_SCHEMA.names,
        ctx=ctx,
    )
    return scopes_plan, scope_key_plan


def _build_symbols(
    symbol_rows: list[dict[str, object]],
    *,
    scope_key_plan: Plan,
    ctx: ExecutionContext,
) -> Plan:
    if not symbol_rows:
        return Plan.table_source(empty_table(SYMBOLS_SCHEMA))
    symbols_plan = plan_from_rows(symbol_rows, schema=SYMBOL_ROWS_SCHEMA, label="sym_symbols_raw")
    symbols_cols = list(SYMBOL_ROWS_SCHEMA.names)
    if {"file_id", "scope_table_id"} <= set(symbols_cols):
        join_config = JoinConfig.from_sequences(
            left_keys=("file_id", "scope_table_id"),
            right_keys=("file_id", "table_id"),
            left_output=tuple(SYMBOL_ROWS_SCHEMA.names),
            right_output=("scope_id",),
        )
        symbols_plan = left_join(
            symbols_plan,
            scope_key_plan,
            config=join_config,
            use_threads=ctx.use_threads,
            ctx=ctx,
        )
        symbols_cols = list(join_config.left_output) + list(join_config.right_output)
    symbols_plan = apply_hash_projection(
        symbols_plan,
        specs=(
            HashSpec(
                prefix="sym_symbol",
                cols=("scope_id", "name"),
                out_col="symbol_row_id",
            ),
        ),
        available=symbols_cols,
        required={"symbol_row_id": ("scope_id", "name")},
        ctx=ctx,
    )
    symbols_plan = apply_query_spec(symbols_plan, spec=SYMBOLS_QUERY, ctx=ctx)
    return align_plan(
        symbols_plan,
        schema=SYMBOLS_SCHEMA,
        available=SYMBOLS_SCHEMA.names,
        ctx=ctx,
    )


def _build_scope_edges(
    scope_edge_rows: list[dict[str, object]],
    *,
    scope_key_plan: Plan,
    ctx: ExecutionContext,
) -> Plan:
    if not scope_edge_rows:
        return Plan.table_source(empty_table(SCOPE_EDGES_SCHEMA))
    scope_edges_plan = plan_from_rows(
        scope_edge_rows,
        schema=SCOPE_EDGE_ROWS_SCHEMA,
        label="sym_scope_edges_raw",
    )
    join_parent = JoinConfig.from_sequences(
        left_keys=("file_id", "parent_table_id"),
        right_keys=("file_id", "table_id"),
        left_output=tuple(SCOPE_EDGE_ROWS_SCHEMA.names),
        right_output=("scope_id",),
    )
    scope_edges_plan = left_join(
        scope_edges_plan,
        scope_key_plan,
        config=join_parent,
        use_threads=ctx.use_threads,
        ctx=ctx,
    )
    parent_cols = list(join_parent.left_output) + list(join_parent.right_output)
    scope_edges_plan = rename_plan_columns(
        scope_edges_plan,
        columns=parent_cols,
        rename={"scope_id": "parent_scope_id"},
        ctx=ctx,
    )
    rename_parent_cols = ["parent_scope_id" if name == "scope_id" else name for name in parent_cols]
    join_child = JoinConfig.from_sequences(
        left_keys=("file_id", "child_table_id"),
        right_keys=("file_id", "table_id"),
        left_output=tuple(rename_parent_cols),
        right_output=("scope_id",),
    )
    scope_edges_plan = left_join(
        scope_edges_plan,
        scope_key_plan,
        config=join_child,
        use_threads=ctx.use_threads,
        ctx=ctx,
    )
    child_cols = list(join_child.left_output) + list(join_child.right_output)
    scope_edges_plan = rename_plan_columns(
        scope_edges_plan,
        columns=child_cols,
        rename={"scope_id": "child_scope_id"},
        ctx=ctx,
    )
    rename_child_cols = ["child_scope_id" if name == "scope_id" else name for name in child_cols]
    scope_edges_plan = apply_hash_projection(
        scope_edges_plan,
        specs=(
            HashSpec(
                prefix="sym_scope_edge",
                cols=("parent_scope_id", "child_scope_id"),
                out_col="edge_id",
            ),
        ),
        available=rename_child_cols,
        required={"edge_id": ("parent_scope_id", "child_scope_id")},
        ctx=ctx,
    )
    scope_edges_plan = apply_query_spec(scope_edges_plan, spec=SCOPE_EDGES_QUERY, ctx=ctx)
    return align_plan(
        scope_edges_plan,
        schema=SCOPE_EDGES_SCHEMA,
        available=SCOPE_EDGES_SCHEMA.names,
        ctx=ctx,
    )


def _build_namespace_edges(
    ns_edge_rows: list[dict[str, object]],
    *,
    scope_key_plan: Plan,
    ctx: ExecutionContext,
) -> Plan:
    if not ns_edge_rows:
        return Plan.table_source(empty_table(NAMESPACE_EDGES_SCHEMA))
    ns_edges_plan = plan_from_rows(
        ns_edge_rows,
        schema=NAMESPACE_EDGE_ROWS_SCHEMA,
        label="sym_namespace_edges_raw",
    )
    join_scope = JoinConfig.from_sequences(
        left_keys=("file_id", "scope_table_id"),
        right_keys=("file_id", "table_id"),
        left_output=tuple(NAMESPACE_EDGE_ROWS_SCHEMA.names),
        right_output=("scope_id",),
    )
    ns_edges_plan = left_join(
        ns_edges_plan,
        scope_key_plan,
        config=join_scope,
        use_threads=ctx.use_threads,
        ctx=ctx,
    )
    scope_cols = list(join_scope.left_output) + list(join_scope.right_output)
    join_child = JoinConfig.from_sequences(
        left_keys=("file_id", "child_table_id"),
        right_keys=("file_id", "table_id"),
        left_output=tuple(scope_cols),
        right_output=("scope_id",),
        output_suffix_for_right="_child",
    )
    ns_edges_plan = left_join(
        ns_edges_plan,
        scope_key_plan,
        config=join_child,
        use_threads=ctx.use_threads,
        ctx=ctx,
    )
    child_cols = list(join_child.left_output) + [
        f"{name}{join_child.output_suffix_for_right}" for name in join_child.right_output
    ]
    ns_edges_plan = rename_plan_columns(
        ns_edges_plan,
        columns=child_cols,
        rename={"scope_id_child": "child_scope_id"},
        ctx=ctx,
    )
    rename_child_cols = [
        "child_scope_id" if name == "scope_id_child" else name for name in child_cols
    ]
    ns_edges_plan = apply_hash_projection(
        ns_edges_plan,
        specs=(
            HashSpec(
                prefix="sym_symbol",
                cols=("scope_id", "symbol_name"),
                out_col="symbol_row_id",
            ),
            HashSpec(
                prefix="sym_ns_edge",
                cols=("symbol_row_id", "child_scope_id"),
                out_col="edge_id",
            ),
        ),
        available=rename_child_cols,
        required={
            "symbol_row_id": ("scope_id", "symbol_name"),
            "edge_id": ("symbol_row_id", "child_scope_id"),
        },
        ctx=ctx,
    )
    ns_edges_plan = apply_query_spec(ns_edges_plan, spec=NAMESPACE_EDGES_QUERY, ctx=ctx)
    return align_plan(
        ns_edges_plan,
        schema=NAMESPACE_EDGES_SCHEMA,
        available=NAMESPACE_EDGES_SCHEMA.names,
        ctx=ctx,
    )


def _build_func_parts(
    func_parts_acc: _FuncPartsAccumulator,
    *,
    scope_key_plan: Plan,
    ctx: ExecutionContext,
) -> Plan:
    func_parts_table = func_parts_acc.to_table()
    if func_parts_table.num_rows == 0:
        return Plan.table_source(empty_table(FUNC_PARTS_SCHEMA))
    if not {"file_id", "scope_table_id"} <= set(func_parts_table.column_names):
        return Plan.table_source(empty_table(FUNC_PARTS_SCHEMA))
    func_parts_plan = Plan.table_source(func_parts_table)
    join_config = JoinConfig.from_sequences(
        left_keys=("file_id", "scope_table_id"),
        right_keys=("file_id", "table_id"),
        left_output=tuple(func_parts_table.column_names),
        right_output=("scope_id",),
    )
    func_parts_plan = left_join(
        func_parts_plan,
        scope_key_plan,
        config=join_config,
        use_threads=ctx.use_threads,
        ctx=ctx,
    )
    func_parts_plan = apply_query_spec(func_parts_plan, spec=FUNC_PARTS_QUERY, ctx=ctx)
    return align_plan(
        func_parts_plan,
        schema=FUNC_PARTS_SCHEMA,
        available=FUNC_PARTS_SCHEMA.names,
        ctx=ctx,
    )


def _build_symtable_result(
    rows: _SymtableRows,
    *,
    ctx: ExecutionContext,
) -> SymtableExtractResult:
    plans = _build_symtable_plans(rows, ctx=ctx)
    return SymtableExtractResult(
        py_sym_scopes=materialize_plan(plans["py_sym_scopes"], ctx=ctx),
        py_sym_symbols=materialize_plan(plans["py_sym_symbols"], ctx=ctx),
        py_sym_scope_edges=materialize_plan(plans["py_sym_scope_edges"], ctx=ctx),
        py_sym_namespace_edges=materialize_plan(plans["py_sym_namespace_edges"], ctx=ctx),
        py_sym_function_partitions=materialize_plan(plans["py_sym_function_partitions"], ctx=ctx),
    )


def _build_symtable_plans(
    rows: _SymtableRows,
    *,
    ctx: ExecutionContext,
) -> dict[str, Plan]:
    if not rows.scope_rows:
        return {
            "py_sym_scopes": Plan.table_source(empty_table(SCOPES_SCHEMA)),
            "py_sym_symbols": Plan.table_source(empty_table(SYMBOLS_SCHEMA)),
            "py_sym_scope_edges": Plan.table_source(empty_table(SCOPE_EDGES_SCHEMA)),
            "py_sym_namespace_edges": Plan.table_source(empty_table(NAMESPACE_EDGES_SCHEMA)),
            "py_sym_function_partitions": Plan.table_source(empty_table(FUNC_PARTS_SCHEMA)),
        }

    scopes_plan, scope_key_plan = _build_scopes(rows.scope_rows, ctx=ctx)
    symbols_plan = _build_symbols(rows.symbol_rows, scope_key_plan=scope_key_plan, ctx=ctx)
    scope_edges_plan = _build_scope_edges(
        rows.scope_edge_rows,
        scope_key_plan=scope_key_plan,
        ctx=ctx,
    )
    ns_edges_plan = _build_namespace_edges(
        rows.ns_edge_rows,
        scope_key_plan=scope_key_plan,
        ctx=ctx,
    )
    func_parts_plan = _build_func_parts(
        rows.func_parts_acc,
        scope_key_plan=scope_key_plan,
        ctx=ctx,
    )

    return {
        "py_sym_scopes": scopes_plan,
        "py_sym_symbols": symbols_plan,
        "py_sym_scope_edges": scope_edges_plan,
        "py_sym_namespace_edges": ns_edges_plan,
        "py_sym_function_partitions": func_parts_plan,
    }


@overload
def extract_symtables_table(
    *,
    repo_root: str | None,
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
    prefer_reader: Literal[False] = False,
) -> TableLike: ...


@overload
def extract_symtables_table(
    *,
    repo_root: str | None,
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
    prefer_reader: Literal[True],
) -> TableLike | RecordBatchReaderLike: ...


def extract_symtables_table(
    *,
    repo_root: str | None,
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
    prefer_reader: bool = False,
) -> TableLike | RecordBatchReaderLike:
    """Extract symtable data into a single table.

    Parameters
    ----------
    repo_root:
        Optional repository root (unused).
    repo_files:
        Repo files table.
    file_contexts:
        Optional pre-built file contexts for extraction.
    ctx:
        Execution context for plan execution.

    prefer_reader:
        When True, return a streaming reader when possible.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Symtable extraction output.
    """
    _ = repo_root
    exec_ctx = ctx or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))
    plans = extract_symtable_plans(repo_files, file_contexts=file_contexts, ctx=exec_ctx)
    return finalize_plan_bundle(
        {"py_sym_scopes": plans["py_sym_scopes"]},
        ctx=exec_ctx,
        prefer_reader=prefer_reader,
    )["py_sym_scopes"]
