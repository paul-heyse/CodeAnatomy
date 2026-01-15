"""Extract Python symtable data into Arrow tables using shared helpers."""

from __future__ import annotations

import symtable
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

import pyarrow as pa

from arrowdsl.compute.expr_core import MaskedHashExprSpec
from arrowdsl.core.context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import ArrayLike, RecordBatchReaderLike, TableLike, pc
from arrowdsl.plan.joins import join_config_for_output, left_join
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import materialize_plan, run_plan_bundle
from arrowdsl.schema.build import table_from_arrays
from arrowdsl.schema.metadata import normalize_dictionaries
from arrowdsl.schema.nested_builders import LargeListViewAccumulator
from arrowdsl.schema.schema import SchemaMetadataSpec, empty_table
from extract.helpers import (
    ExtractExecutionContext,
    FileContext,
    file_identity_row,
    iter_contexts,
    project_columns,
    text_from_file_ctx,
)
from extract.plan_helpers import apply_query_and_normalize, plan_from_rows
from extract.registry_ids import hash_spec
from extract.registry_specs import (
    dataset_row_schema,
    dataset_schema,
    normalize_options,
)
from extract.schema_ops import metadata_spec_for_dataset
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


SYM_SCOPE_ID_SPEC = hash_spec("sym_scope_id")
SYM_SYMBOL_ROW_ID_SPEC = hash_spec("sym_symbol_row_id")
SYM_SCOPE_EDGE_ID_SPEC = hash_spec("sym_scope_edge_id")
SYM_NS_SYMBOL_ROW_ID_SPEC = hash_spec("sym_ns_symbol_row_id")
SYM_NS_EDGE_ID_SPEC = hash_spec("sym_ns_edge_id")

SCOPES_SCHEMA = dataset_schema("py_sym_scopes_v1")
SYMBOLS_SCHEMA = dataset_schema("py_sym_symbols_v1")
SCOPE_EDGES_SCHEMA = dataset_schema("py_sym_scope_edges_v1")
NAMESPACE_EDGES_SCHEMA = dataset_schema("py_sym_namespace_edges_v1")
FUNC_PARTS_SCHEMA = dataset_schema("py_sym_function_partitions_v1")

SCOPES_ROW_SCHEMA = dataset_row_schema("py_sym_scopes_v1")
SYMBOLS_ROW_SCHEMA = dataset_row_schema("py_sym_symbols_v1")
SCOPE_EDGES_ROW_SCHEMA = dataset_row_schema("py_sym_scope_edges_v1")
NAMESPACE_EDGES_ROW_SCHEMA = dataset_row_schema("py_sym_namespace_edges_v1")
FUNC_PARTS_ROW_SCHEMA = dataset_row_schema("py_sym_function_partitions_v1")


def _symtable_metadata_specs(
    options: SymtableExtractOptions,
) -> dict[str, SchemaMetadataSpec]:
    return {
        "py_sym_scopes": metadata_spec_for_dataset("py_sym_scopes_v1", options=options),
        "py_sym_symbols": metadata_spec_for_dataset("py_sym_symbols_v1", options=options),
        "py_sym_scope_edges": metadata_spec_for_dataset("py_sym_scope_edges_v1", options=options),
        "py_sym_namespace_edges": metadata_spec_for_dataset(
            "py_sym_namespace_edges_v1", options=options
        ),
        "py_sym_function_partitions": metadata_spec_for_dataset(
            "py_sym_function_partitions_v1", options=options
        ),
    }


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
    dtype=pa.large_list_view(pa.string()),
    builder=_build_func_parameters,
)

FUNC_PARTS_LOCALS_SPEC = NestedFieldSpec(
    name="locals",
    dtype=pa.large_list_view(pa.string()),
    builder=_build_func_locals,
)

FUNC_PARTS_GLOBALS_SPEC = NestedFieldSpec(
    name="globals",
    dtype=pa.large_list_view(pa.string()),
    builder=_build_func_globals,
)

FUNC_PARTS_NONLOCALS_SPEC = NestedFieldSpec(
    name="nonlocals",
    dtype=pa.large_list_view(pa.string()),
    builder=_build_func_nonlocals,
)

FUNC_PARTS_FREES_SPEC = NestedFieldSpec(
    name="frees",
    dtype=pa.large_list_view(pa.string()),
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
    metadata_specs = _symtable_metadata_specs(normalized_options)

    rows = _collect_symtable_rows(
        repo_files,
        exec_context.file_contexts,
        compile_type=normalized_options.compile_type,
    )
    return _build_symtable_result(
        rows,
        ctx=ctx,
        metadata_specs=metadata_specs,
        evidence_plan=exec_context.evidence_plan,
    )


def extract_symtable_plans(
    repo_files: TableLike,
    options: SymtableExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> dict[str, Plan]:
    """Extract symbol table plans from repository files.

    Returns
    -------
    dict[str, Plan]
        Plan bundle keyed by symtable outputs.
    """
    normalized_options = normalize_options("symtable", options, SymtableExtractOptions)
    exec_context = context or ExtractExecutionContext()
    ctx = exec_context.ensure_ctx()
    evidence_plan = exec_context.evidence_plan
    rows = _collect_symtable_rows(
        repo_files,
        exec_context.file_contexts,
        compile_type=normalized_options.compile_type,
    )
    return _build_symtable_plans(rows, ctx=ctx, evidence_plan=evidence_plan)


def _build_scopes(
    scope_rows: list[dict[str, object]],
    *,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> tuple[Plan, Plan]:
    scopes_plan = plan_from_rows(scope_rows, schema=SCOPES_ROW_SCHEMA, label="sym_scopes")
    scopes_base = [name for name in SCOPES_ROW_SCHEMA.names if name != "scope_id"]
    scopes_plan = project_columns(
        scopes_plan,
        base=scopes_base,
        extras=[
            (
                MaskedHashExprSpec(
                    spec=SYM_SCOPE_ID_SPEC,
                    required=("file_id", "table_id", "scope_type", "scope_name", "lineno"),
                ).to_expression(),
                "scope_id",
            )
        ],
        ctx=ctx,
    )
    scope_key_plan = scopes_plan.project(
        [pc.field("file_id"), pc.field("table_id"), pc.field("scope_id")],
        ["file_id", "table_id", "scope_id"],
        ctx=ctx,
    )
    scopes_plan = apply_query_and_normalize(
        "py_sym_scopes_v1",
        scopes_plan,
        ctx=ctx,
        evidence_plan=evidence_plan,
    )
    return scopes_plan, scope_key_plan


def _build_symbols(
    symbol_rows: list[dict[str, object]],
    *,
    scope_key_plan: Plan,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> Plan:
    if not symbol_rows:
        return Plan.table_source(empty_table(SYMBOLS_SCHEMA))
    symbols_plan = plan_from_rows(symbol_rows, schema=SYMBOLS_ROW_SCHEMA, label="sym_symbols_raw")
    symbols_cols = list(SYMBOLS_ROW_SCHEMA.names)
    join_config = join_config_for_output(
        left_columns=SYMBOLS_ROW_SCHEMA.names,
        right_columns=scope_key_plan.schema(ctx=ctx).names,
        key_pairs=(("file_id", "file_id"), ("scope_table_id", "table_id")),
        right_output=("scope_id",),
    )
    if join_config is not None:
        symbols_plan = left_join(
            symbols_plan,
            scope_key_plan,
            config=join_config,
            use_threads=ctx.use_threads,
            ctx=ctx,
        )
        symbols_cols = list(join_config.left_output) + list(join_config.right_output)
    symbols_base = [name for name in symbols_cols if name != "symbol_row_id"]
    symbols_plan = project_columns(
        symbols_plan,
        base=symbols_base,
        extras=[
            (
                MaskedHashExprSpec(
                    spec=SYM_SYMBOL_ROW_ID_SPEC,
                    required=("scope_id", "name"),
                ).to_expression(),
                "symbol_row_id",
            )
        ],
        ctx=ctx,
    )
    return apply_query_and_normalize(
        "py_sym_symbols_v1",
        symbols_plan,
        ctx=ctx,
        evidence_plan=evidence_plan,
    )


def _build_scope_edges(
    scope_edge_rows: list[dict[str, object]],
    *,
    scope_key_plan: Plan,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> Plan:
    if not scope_edge_rows:
        return Plan.table_source(empty_table(SCOPE_EDGES_SCHEMA))
    scope_edges_plan = plan_from_rows(
        scope_edge_rows,
        schema=SCOPE_EDGES_ROW_SCHEMA,
        label="sym_scope_edges_raw",
    )
    join_parent = join_config_for_output(
        left_columns=SCOPE_EDGES_ROW_SCHEMA.names,
        right_columns=scope_key_plan.schema(ctx=ctx).names,
        key_pairs=(("file_id", "file_id"), ("parent_table_id", "table_id")),
        right_output=("scope_id",),
    )
    if join_parent is None:
        msg = "Scope edge rows missing required join keys."
        raise ValueError(msg)
    scope_edges_plan = left_join(
        scope_edges_plan,
        scope_key_plan,
        config=join_parent,
        use_threads=ctx.use_threads,
        ctx=ctx,
    )
    parent_cols = list(join_parent.left_output) + list(join_parent.right_output)
    scope_edges_plan = project_columns(
        scope_edges_plan,
        base=parent_cols,
        rename={"scope_id": "parent_scope_id"},
        ctx=ctx,
    )
    rename_parent_cols = ["parent_scope_id" if name == "scope_id" else name for name in parent_cols]
    join_child = join_config_for_output(
        left_columns=rename_parent_cols,
        right_columns=scope_key_plan.schema(ctx=ctx).names,
        key_pairs=(("file_id", "file_id"), ("child_table_id", "table_id")),
        right_output=("scope_id",),
    )
    if join_child is None:
        msg = "Scope edge rows missing required child join keys."
        raise ValueError(msg)
    scope_edges_plan = left_join(
        scope_edges_plan,
        scope_key_plan,
        config=join_child,
        use_threads=ctx.use_threads,
        ctx=ctx,
    )
    child_cols = list(join_child.left_output) + list(join_child.right_output)
    scope_edges_plan = project_columns(
        scope_edges_plan,
        base=child_cols,
        rename={"scope_id": "child_scope_id"},
        ctx=ctx,
    )
    rename_child_cols = ["child_scope_id" if name == "scope_id" else name for name in child_cols]
    scope_edges_base = [name for name in rename_child_cols if name != "edge_id"]
    scope_edges_plan = project_columns(
        scope_edges_plan,
        base=scope_edges_base,
        extras=[
            (
                MaskedHashExprSpec(
                    spec=SYM_SCOPE_EDGE_ID_SPEC,
                    required=("parent_scope_id", "child_scope_id"),
                ).to_expression(),
                "edge_id",
            )
        ],
        ctx=ctx,
    )
    return apply_query_and_normalize(
        "py_sym_scope_edges_v1",
        scope_edges_plan,
        ctx=ctx,
        evidence_plan=evidence_plan,
    )


def _build_namespace_edges(
    ns_edge_rows: list[dict[str, object]],
    *,
    scope_key_plan: Plan,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> Plan:
    if not ns_edge_rows:
        return Plan.table_source(empty_table(NAMESPACE_EDGES_SCHEMA))
    ns_edges_plan = plan_from_rows(
        ns_edge_rows,
        schema=NAMESPACE_EDGES_ROW_SCHEMA,
        label="sym_namespace_edges_raw",
    )
    join_scope = join_config_for_output(
        left_columns=NAMESPACE_EDGES_ROW_SCHEMA.names,
        right_columns=scope_key_plan.schema(ctx=ctx).names,
        key_pairs=(("file_id", "file_id"), ("scope_table_id", "table_id")),
        right_output=("scope_id",),
    )
    if join_scope is None:
        msg = "Namespace edge rows missing required scope join keys."
        raise ValueError(msg)
    ns_edges_plan = left_join(
        ns_edges_plan,
        scope_key_plan,
        config=join_scope,
        use_threads=ctx.use_threads,
        ctx=ctx,
    )
    scope_cols = list(join_scope.left_output) + list(join_scope.right_output)
    join_child = join_config_for_output(
        left_columns=scope_cols,
        right_columns=scope_key_plan.schema(ctx=ctx).names,
        key_pairs=(("file_id", "file_id"), ("child_table_id", "table_id")),
        right_output=("scope_id",),
        output_suffix_for_right="_child",
    )
    if join_child is None:
        msg = "Namespace edge rows missing required child join keys."
        raise ValueError(msg)
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
    ns_edges_plan = project_columns(
        ns_edges_plan,
        base=child_cols,
        rename={"scope_id_child": "child_scope_id"},
        ctx=ctx,
    )
    rename_child_cols = [
        "child_scope_id" if name == "scope_id_child" else name for name in child_cols
    ]
    ns_base = [name for name in rename_child_cols if name != "symbol_row_id"]
    ns_edges_plan = project_columns(
        ns_edges_plan,
        base=ns_base,
        extras=[
            (
                MaskedHashExprSpec(
                    spec=SYM_NS_SYMBOL_ROW_ID_SPEC,
                    required=("scope_id", "symbol_name"),
                ).to_expression(),
                "symbol_row_id",
            )
        ],
        ctx=ctx,
    )
    return apply_query_and_normalize(
        "py_sym_namespace_edges_v1",
        ns_edges_plan,
        ctx=ctx,
        evidence_plan=evidence_plan,
    )


def _build_func_parts(
    func_parts_acc: _FuncPartsAccumulator,
    *,
    scope_key_plan: Plan,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> Plan:
    func_parts_table = func_parts_acc.to_table()
    if func_parts_table.num_rows == 0:
        return Plan.table_source(empty_table(FUNC_PARTS_SCHEMA))
    if not {"file_id", "scope_table_id"} <= set(func_parts_table.column_names):
        return Plan.table_source(empty_table(FUNC_PARTS_SCHEMA))
    func_parts_plan = Plan.table_source(func_parts_table)
    join_config = join_config_for_output(
        left_columns=func_parts_table.column_names,
        right_columns=scope_key_plan.schema(ctx=ctx).names,
        key_pairs=(("file_id", "file_id"), ("scope_table_id", "table_id")),
        right_output=("scope_id",),
    )
    if join_config is None:
        msg = "Func parts rows missing required join keys."
        raise ValueError(msg)
    func_parts_plan = left_join(
        func_parts_plan,
        scope_key_plan,
        config=join_config,
        use_threads=ctx.use_threads,
        ctx=ctx,
    )
    return apply_query_and_normalize(
        "py_sym_function_partitions_v1",
        func_parts_plan,
        ctx=ctx,
        evidence_plan=evidence_plan,
    )


def _build_symtable_result(
    rows: _SymtableRows,
    *,
    ctx: ExecutionContext,
    metadata_specs: Mapping[str, SchemaMetadataSpec],
    evidence_plan: EvidencePlan | None = None,
) -> SymtableExtractResult:
    plans = _build_symtable_plans(rows, ctx=ctx, evidence_plan=evidence_plan)
    return SymtableExtractResult(
        py_sym_scopes=materialize_plan(
            plans["py_sym_scopes"],
            ctx=ctx,
            metadata_spec=metadata_specs["py_sym_scopes"],
            attach_ordering_metadata=True,
        ),
        py_sym_symbols=materialize_plan(
            plans["py_sym_symbols"],
            ctx=ctx,
            metadata_spec=metadata_specs["py_sym_symbols"],
            attach_ordering_metadata=True,
        ),
        py_sym_scope_edges=materialize_plan(
            plans["py_sym_scope_edges"],
            ctx=ctx,
            metadata_spec=metadata_specs["py_sym_scope_edges"],
            attach_ordering_metadata=True,
        ),
        py_sym_namespace_edges=materialize_plan(
            plans["py_sym_namespace_edges"],
            ctx=ctx,
            metadata_spec=metadata_specs["py_sym_namespace_edges"],
            attach_ordering_metadata=True,
        ),
        py_sym_function_partitions=materialize_plan(
            plans["py_sym_function_partitions"],
            ctx=ctx,
            metadata_spec=metadata_specs["py_sym_function_partitions"],
            attach_ordering_metadata=True,
        ),
    )


def _build_symtable_plans(
    rows: _SymtableRows,
    *,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> dict[str, Plan]:
    if not rows.scope_rows:
        return {
            "py_sym_scopes": Plan.table_source(empty_table(SCOPES_SCHEMA)),
            "py_sym_symbols": Plan.table_source(empty_table(SYMBOLS_SCHEMA)),
            "py_sym_scope_edges": Plan.table_source(empty_table(SCOPE_EDGES_SCHEMA)),
            "py_sym_namespace_edges": Plan.table_source(empty_table(NAMESPACE_EDGES_SCHEMA)),
            "py_sym_function_partitions": Plan.table_source(empty_table(FUNC_PARTS_SCHEMA)),
        }

    scopes_plan, scope_key_plan = _build_scopes(
        rows.scope_rows,
        ctx=ctx,
        evidence_plan=evidence_plan,
    )
    symbols_plan = _build_symbols(
        rows.symbol_rows,
        scope_key_plan=scope_key_plan,
        ctx=ctx,
        evidence_plan=evidence_plan,
    )
    scope_edges_plan = _build_scope_edges(
        rows.scope_edge_rows,
        scope_key_plan=scope_key_plan,
        ctx=ctx,
        evidence_plan=evidence_plan,
    )
    ns_edges_plan = _build_namespace_edges(
        rows.ns_edge_rows,
        scope_key_plan=scope_key_plan,
        ctx=ctx,
        evidence_plan=evidence_plan,
    )
    func_parts_plan = _build_func_parts(
        rows.func_parts_acc,
        scope_key_plan=scope_key_plan,
        ctx=ctx,
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
    metadata_specs = _symtable_metadata_specs(normalized_options)
    return run_plan_bundle(
        {"py_sym_scopes": plans["py_sym_scopes"]},
        ctx=exec_ctx,
        prefer_reader=prefer_reader,
        metadata_specs={"py_sym_scopes": metadata_specs["py_sym_scopes"]},
        attach_ordering_metadata=True,
    )["py_sym_scopes"]
