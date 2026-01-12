"""Extract Python symtable data into Arrow tables."""

from __future__ import annotations

import symtable
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import cast

import arrowdsl.pyarrow_core as pa
from arrowdsl.column_ops import set_or_append_column
from arrowdsl.compute import pc
from arrowdsl.empty import empty_table
from arrowdsl.id_specs import HashSpec
from arrowdsl.ids import hash_column_values
from arrowdsl.kernels import apply_join
from arrowdsl.pyarrow_protocols import TableLike
from arrowdsl.schema_ops import SchemaTransform
from arrowdsl.specs import JoinSpec
from extract.file_context import FileContext, iter_file_contexts
from schema_spec.core import ArrowFieldSpec
from schema_spec.factories import make_table_spec
from schema_spec.fields import file_identity_bundle
from schema_spec.registry import GLOBAL_SCHEMA_REGISTRY

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


SCOPES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
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
)

SYMBOLS_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
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
)

SCOPE_EDGES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
        name="py_sym_scope_edges_v1",
        version=SCHEMA_VERSION,
        bundles=(file_identity_bundle(),),
        fields=[
            ArrowFieldSpec(name="edge_id", dtype=pa.string()),
            ArrowFieldSpec(name="parent_scope_id", dtype=pa.string()),
            ArrowFieldSpec(name="child_scope_id", dtype=pa.string()),
        ],
    )
)

NAMESPACE_EDGES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
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
)

FUNC_PARTS_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
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
)

SCOPES_SCHEMA = SCOPES_SPEC.to_arrow_schema()
SYMBOLS_SCHEMA = SYMBOLS_SPEC.to_arrow_schema()
SCOPE_EDGES_SCHEMA = SCOPE_EDGES_SPEC.to_arrow_schema()
NAMESPACE_EDGES_SCHEMA = NAMESPACE_EDGES_SPEC.to_arrow_schema()
FUNC_PARTS_SCHEMA = FUNC_PARTS_SPEC.to_arrow_schema()


def _scope_type_str(tbl: symtable.SymbolTable) -> str:
    return str(tbl.get_type()).split(".")[-1]


def _scope_role(scope_type_str: str) -> str:
    return "runtime" if scope_type_str in {"MODULE", "FUNCTION", "CLASS"} else "type_meta"


def _valid_mask(table: TableLike, cols: Sequence[str]) -> object:
    mask = pc.is_valid(table[cols[0]])
    for col in cols[1:]:
        mask = pc.and_(mask, pc.is_valid(table[col]))
    return mask


def _apply_hash_column(
    table: TableLike,
    *,
    spec: HashSpec,
    required: Sequence[str] | None = None,
) -> TableLike:
    hashed = hash_column_values(table, spec=spec)
    out_col = spec.out_col or f"{spec.prefix}_id"
    if required:
        mask = _valid_mask(table, required)
        hashed = pc.if_else(mask, hashed, pa.scalar(None, type=hashed.type))
    return set_or_append_column(table, out_col, hashed)


def _rename_column(table: TableLike, old: str, new: str) -> TableLike:
    names = [new if name == old else name for name in table.column_names]
    return table.rename_columns(names)


def _scope_row(ctx: SymtableContext, table_id: int, tbl: symtable.SymbolTable) -> dict[str, object]:
    st_str = _scope_type_str(tbl)
    return {
        "file_id": ctx.file_id,
        "path": ctx.path,
        "file_sha256": ctx.file_sha256,
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
        "file_id": ctx.file_id,
        "path": ctx.path,
        "file_sha256": ctx.file_sha256,
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

    for sym in tbl.get_symbols():
        name = sym.get_name()
        symbol_rows.append(
            {
                "scope_table_id": scope_table_id,
                "file_id": ctx.file_id,
                "path": ctx.path,
                "file_sha256": ctx.file_sha256,
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
                        "file_id": ctx.file_id,
                        "path": ctx.path,
                        "file_sha256": ctx.file_sha256,
                        "scope_table_id": scope_table_id,
                        "symbol_name": name,
                        "child_table_id": child_table_id,
                    }
                )

    return symbol_rows, ns_edge_rows


def _func_parts_row(
    ctx: SymtableContext, scope_table_id: int, tbl: symtable.SymbolTable
) -> dict[str, object] | None:
    if _scope_type_str(tbl) != "FUNCTION":
        return None
    return {
        "scope_table_id": scope_table_id,
        "file_id": ctx.file_id,
        "path": ctx.path,
        "file_sha256": ctx.file_sha256,
        "parameters": _symbol_list(tbl, "get_parameters"),
        "locals": _symbol_list(tbl, "get_locals"),
        "globals": _symbol_list(tbl, "get_globals"),
        "nonlocals": _symbol_list(tbl, "get_nonlocals"),
        "frees": _symbol_list(tbl, "get_frees"),
    }


def _repo_text(file_ctx: FileContext) -> str | None:
    if file_ctx.text:
        return file_ctx.text
    if file_ctx.data is None:
        return None
    encoding = file_ctx.encoding or "utf-8"
    try:
        return file_ctx.data.decode(encoding, errors="replace")
    except UnicodeError:
        return None


def _extract_symtable_for_context(
    file_ctx: FileContext,
    *,
    compile_type: str,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    if not file_ctx.file_id or not file_ctx.path:
        return [], [], [], [], []

    text = _repo_text(file_ctx)
    if not text:
        return [], [], [], [], []

    try:
        top = symtable.symtable(text, file_ctx.path, compile_type)
    except (SyntaxError, TypeError, ValueError):
        return [], [], [], [], []

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
    list[dict[str, object]],
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
    list[dict[str, object]],
]:
    scope_rows: list[dict[str, object]] = []
    symbol_rows: list[dict[str, object]] = []
    scope_edge_rows: list[dict[str, object]] = []
    ns_edge_rows: list[dict[str, object]] = []
    func_parts_rows: list[dict[str, object]] = []

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

        parts_row = _func_parts_row(ctx, table_id, tbl)
        if parts_row is not None:
            func_parts_rows.append(parts_row)

        stack.extend((child, tbl) for child in _iter_children(tbl))

    return scope_rows, symbol_rows, scope_edge_rows, ns_edge_rows, func_parts_rows


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


def extract_symtable(
    repo_files: TableLike,
    options: SymtableExtractOptions | None = None,
    *,
    file_contexts: Iterable[FileContext] | None = None,
) -> SymtableExtractResult:
    """Extract symbol table artifacts from repository files.

    Returns
    -------
    SymtableExtractResult
        Tables for scopes, symbols, and namespace edges.
    """
    options = options or SymtableExtractOptions()

    scope_rows: list[dict[str, object]] = []
    symbol_rows: list[dict[str, object]] = []
    scope_edge_rows: list[dict[str, object]] = []
    ns_edge_rows: list[dict[str, object]] = []
    func_parts_rows: list[dict[str, object]] = []

    contexts = file_contexts if file_contexts is not None else iter_file_contexts(repo_files)
    for file_ctx in contexts:
        (
            file_scope_rows,
            file_symbol_rows,
            file_scope_edge_rows,
            file_ns_edge_rows,
            file_func_parts_rows,
        ) = _extract_symtable_for_context(file_ctx, compile_type=options.compile_type)
        scope_rows.extend(file_scope_rows)
        symbol_rows.extend(file_symbol_rows)
        scope_edge_rows.extend(file_scope_edge_rows)
        ns_edge_rows.extend(file_ns_edge_rows)
        func_parts_rows.extend(file_func_parts_rows)

    return _build_symtable_result(
        scope_rows=scope_rows,
        symbol_rows=symbol_rows,
        scope_edge_rows=scope_edge_rows,
        ns_edge_rows=ns_edge_rows,
        func_parts_rows=func_parts_rows,
    )


def _build_symtable_result(
    *,
    scope_rows: list[dict[str, object]],
    symbol_rows: list[dict[str, object]],
    scope_edge_rows: list[dict[str, object]],
    ns_edge_rows: list[dict[str, object]],
    func_parts_rows: list[dict[str, object]],
) -> SymtableExtractResult:
    if not scope_rows:
        return SymtableExtractResult(
            py_sym_scopes=empty_table(SCOPES_SCHEMA),
            py_sym_symbols=empty_table(SYMBOLS_SCHEMA),
            py_sym_scope_edges=empty_table(SCOPE_EDGES_SCHEMA),
            py_sym_namespace_edges=empty_table(NAMESPACE_EDGES_SCHEMA),
            py_sym_function_partitions=empty_table(FUNC_PARTS_SCHEMA),
        )

    scopes_raw = pa.Table.from_pylist(scope_rows)
    scopes_raw = _apply_hash_column(
        scopes_raw,
        spec=HashSpec(
            prefix="sym_scope",
            cols=("file_id", "table_id", "scope_type", "scope_name", "lineno"),
            out_col="scope_id",
        ),
        required=("file_id", "table_id", "scope_type", "scope_name", "lineno"),
    )
    scopes = SchemaTransform(schema=SCOPES_SCHEMA).apply(scopes_raw)
    scope_key = scopes.select(["file_id", "table_id", "scope_id"])

    symbols_raw = pa.Table.from_pylist(symbol_rows) if symbol_rows else empty_table(SYMBOLS_SCHEMA)
    if symbols_raw.num_rows > 0 and {"file_id", "scope_table_id"} <= set(symbols_raw.column_names):
        symbols_raw = apply_join(
            symbols_raw,
            scope_key,
            spec=JoinSpec(
                join_type="left outer",
                left_keys=("file_id", "scope_table_id"),
                right_keys=("file_id", "table_id"),
                left_output=tuple(symbols_raw.column_names),
                right_output=("scope_id",),
            ),
            use_threads=True,
        )
        symbols_raw = _apply_hash_column(
            symbols_raw,
            spec=HashSpec(
                prefix="sym_symbol",
                cols=("scope_id", "name"),
                out_col="symbol_row_id",
            ),
            required=("scope_id", "name"),
        )
    symbols = SchemaTransform(schema=SYMBOLS_SCHEMA).apply(symbols_raw)

    scope_edges_raw = (
        pa.Table.from_pylist(scope_edge_rows)
        if scope_edge_rows
        else empty_table(SCOPE_EDGES_SCHEMA)
    )
    if scope_edges_raw.num_rows > 0:
        scope_edges_raw = apply_join(
            scope_edges_raw,
            scope_key,
            spec=JoinSpec(
                join_type="left outer",
                left_keys=("file_id", "parent_table_id"),
                right_keys=("file_id", "table_id"),
                left_output=tuple(scope_edges_raw.column_names),
                right_output=("scope_id",),
            ),
            use_threads=True,
        )
        scope_edges_raw = _rename_column(scope_edges_raw, "scope_id", "parent_scope_id")
        scope_edges_raw = apply_join(
            scope_edges_raw,
            scope_key,
            spec=JoinSpec(
                join_type="left outer",
                left_keys=("file_id", "child_table_id"),
                right_keys=("file_id", "table_id"),
                left_output=tuple(scope_edges_raw.column_names),
                right_output=("scope_id",),
            ),
            use_threads=True,
        )
        scope_edges_raw = _rename_column(scope_edges_raw, "scope_id", "child_scope_id")
        scope_edges_raw = _apply_hash_column(
            scope_edges_raw,
            spec=HashSpec(
                prefix="sym_scope_edge",
                cols=("parent_scope_id", "child_scope_id"),
                out_col="edge_id",
            ),
            required=("parent_scope_id", "child_scope_id"),
        )
    scope_edges = SchemaTransform(schema=SCOPE_EDGES_SCHEMA).apply(scope_edges_raw)

    ns_edges_raw = (
        pa.Table.from_pylist(ns_edge_rows) if ns_edge_rows else empty_table(NAMESPACE_EDGES_SCHEMA)
    )
    if ns_edges_raw.num_rows > 0:
        ns_edges_raw = apply_join(
            ns_edges_raw,
            scope_key,
            spec=JoinSpec(
                join_type="left outer",
                left_keys=("file_id", "scope_table_id"),
                right_keys=("file_id", "table_id"),
                left_output=tuple(ns_edges_raw.column_names),
                right_output=("scope_id",),
            ),
            use_threads=True,
        )
        ns_edges_raw = apply_join(
            ns_edges_raw,
            scope_key,
            spec=JoinSpec(
                join_type="left outer",
                left_keys=("file_id", "child_table_id"),
                right_keys=("file_id", "table_id"),
                left_output=tuple(ns_edges_raw.column_names),
                right_output=("scope_id",),
                output_suffix_for_right="_child",
            ),
            use_threads=True,
        )
        ns_edges_raw = _rename_column(ns_edges_raw, "scope_id_child", "child_scope_id")
        ns_edges_raw = _apply_hash_column(
            ns_edges_raw,
            spec=HashSpec(
                prefix="sym_symbol",
                cols=("scope_id", "symbol_name"),
                out_col="symbol_row_id",
            ),
            required=("scope_id", "symbol_name"),
        )
        ns_edges_raw = _apply_hash_column(
            ns_edges_raw,
            spec=HashSpec(
                prefix="sym_ns_edge",
                cols=("symbol_row_id", "child_scope_id"),
                out_col="edge_id",
            ),
            required=("symbol_row_id", "child_scope_id"),
        )
    ns_edges = SchemaTransform(schema=NAMESPACE_EDGES_SCHEMA).apply(ns_edges_raw)

    func_parts_raw = (
        pa.Table.from_pylist(func_parts_rows) if func_parts_rows else empty_table(FUNC_PARTS_SCHEMA)
    )
    if func_parts_raw.num_rows > 0 and {"file_id", "scope_table_id"} <= set(
        func_parts_raw.column_names
    ):
        func_parts_raw = apply_join(
            func_parts_raw,
            scope_key,
            spec=JoinSpec(
                join_type="left outer",
                left_keys=("file_id", "scope_table_id"),
                right_keys=("file_id", "table_id"),
                left_output=tuple(func_parts_raw.column_names),
                right_output=("scope_id",),
            ),
            use_threads=True,
        )
    func_parts = SchemaTransform(schema=FUNC_PARTS_SCHEMA).apply(func_parts_raw)

    return SymtableExtractResult(
        py_sym_scopes=scopes,
        py_sym_symbols=symbols,
        py_sym_scope_edges=scope_edges,
        py_sym_namespace_edges=ns_edges,
        py_sym_function_partitions=func_parts,
    )


def extract_symtables_table(
    *,
    repo_root: str | None,
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: object | None = None,
) -> TableLike:
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
        Execution context (unused).

    Returns
    -------
    pyarrow.Table
        Symtable extraction table.
    """
    _ = (repo_root, ctx)
    result = extract_symtable(repo_files, file_contexts=file_contexts)
    return result.py_sym_scopes
