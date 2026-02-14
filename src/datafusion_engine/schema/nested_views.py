"""Nested view specification and registration helpers.

Nested views are DataFusion views that project struct fields from
extraction tables. They are registered during session setup to provide
flat access to nested data. This module owns the ``NESTED_DATASET_INDEX``
mapping and all accessor/builder functions that operate on it.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Literal, TypedDict

import pyarrow as pa
from datafusion import SessionContext, col

from datafusion_engine.schema.extraction_schemas import (
    _field_from_container,
    extract_schema_for,
)
from schema_spec.view_specs import ViewRuntimeSpec
from utils.registry_protocol import ImmutableRegistry, MappingRegistryAdapter

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame
    from datafusion.expr import Expr


_LOGGER = logging.getLogger(__name__)


class NestedDatasetSpec(TypedDict):
    """Specification for a nested dataset within a root extraction table."""

    root: str
    path: str
    role: Literal["intrinsic", "derived"]
    context: dict[str, str]


NESTED_DATASET_INDEX: ImmutableRegistry[str, NestedDatasetSpec] = ImmutableRegistry.from_dict(
    {
        "cst_nodes": {
            "root": "libcst_files_v1",
            "path": "nodes",
            "role": "intrinsic",
            "context": {},
        },
        "cst_edges": {
            "root": "libcst_files_v1",
            "path": "edges",
            "role": "intrinsic",
            "context": {},
        },
        "cst_parse_manifest": {
            "root": "libcst_files_v1",
            "path": "parse_manifest",
            "role": "intrinsic",
            "context": {},
        },
        "cst_parse_errors": {
            "root": "libcst_files_v1",
            "path": "parse_errors",
            "role": "derived",
            "context": {},
        },
        "cst_refs": {
            "root": "libcst_files_v1",
            "path": "refs",
            "role": "derived",
            "context": {},
        },
        "cst_imports": {
            "root": "libcst_files_v1",
            "path": "imports",
            "role": "derived",
            "context": {},
        },
        "cst_callsites": {
            "root": "libcst_files_v1",
            "path": "callsites",
            "role": "derived",
            "context": {},
        },
        "cst_defs": {
            "root": "libcst_files_v1",
            "path": "defs",
            "role": "derived",
            "context": {},
        },
        "cst_type_exprs": {
            "root": "libcst_files_v1",
            "path": "type_exprs",
            "role": "derived",
            "context": {},
        },
        "cst_docstrings": {
            "root": "libcst_files_v1",
            "path": "docstrings",
            "role": "derived",
            "context": {},
        },
        "cst_decorators": {
            "root": "libcst_files_v1",
            "path": "decorators",
            "role": "derived",
            "context": {},
        },
        "cst_call_args": {
            "root": "libcst_files_v1",
            "path": "call_args",
            "role": "derived",
            "context": {},
        },
        "ast_nodes": {
            "root": "ast_files_v1",
            "path": "nodes",
            "role": "derived",
            "context": {},
        },
        "ast_edges": {
            "root": "ast_files_v1",
            "path": "edges",
            "role": "derived",
            "context": {},
        },
        "ast_errors": {
            "root": "ast_files_v1",
            "path": "errors",
            "role": "derived",
            "context": {},
        },
        "ast_docstrings": {
            "root": "ast_files_v1",
            "path": "docstrings",
            "role": "derived",
            "context": {},
        },
        "ast_imports": {
            "root": "ast_files_v1",
            "path": "imports",
            "role": "derived",
            "context": {},
        },
        "ast_defs": {
            "root": "ast_files_v1",
            "path": "defs",
            "role": "derived",
            "context": {},
        },
        "ast_calls": {
            "root": "ast_files_v1",
            "path": "calls",
            "role": "derived",
            "context": {},
        },
        "ast_type_ignores": {
            "root": "ast_files_v1",
            "path": "type_ignores",
            "role": "derived",
            "context": {},
        },
        "ts_nodes": {
            "root": "tree_sitter_files_v1",
            "path": "nodes",
            "role": "derived",
            "context": {},
        },
        "ts_edges": {
            "root": "tree_sitter_files_v1",
            "path": "edges",
            "role": "intrinsic",
            "context": {},
        },
        "ts_errors": {
            "root": "tree_sitter_files_v1",
            "path": "errors",
            "role": "derived",
            "context": {},
        },
        "ts_missing": {
            "root": "tree_sitter_files_v1",
            "path": "missing",
            "role": "derived",
            "context": {},
        },
        "ts_captures": {
            "root": "tree_sitter_files_v1",
            "path": "captures",
            "role": "derived",
            "context": {},
        },
        "ts_defs": {
            "root": "tree_sitter_files_v1",
            "path": "defs",
            "role": "derived",
            "context": {},
        },
        "ts_calls": {
            "root": "tree_sitter_files_v1",
            "path": "calls",
            "role": "derived",
            "context": {},
        },
        "ts_imports": {
            "root": "tree_sitter_files_v1",
            "path": "imports",
            "role": "derived",
            "context": {},
        },
        "ts_docstrings": {
            "root": "tree_sitter_files_v1",
            "path": "docstrings",
            "role": "derived",
            "context": {},
        },
        "ts_stats": {
            "root": "tree_sitter_files_v1",
            "path": "stats",
            "role": "intrinsic",
            "context": {},
        },
        "symtable_scopes": {
            "root": "symtable_files_v1",
            "path": "blocks",
            "role": "derived",
            "context": {},
        },
        "symtable_symbols": {
            "root": "symtable_files_v1",
            "path": "blocks.symbols",
            "role": "derived",
            "context": {"block_id": "blocks.block_id", "scope_id": "blocks.scope_id"},
        },
        "symtable_scope_edges": {
            "root": "symtable_files_v1",
            "path": "blocks",
            "role": "derived",
            "context": {},
        },
        "py_bc_code_units": {
            "root": "bytecode_files_v1",
            "path": "code_objects",
            "role": "derived",
            "context": {},
        },
        "py_bc_line_table": {
            "root": "bytecode_files_v1",
            "path": "code_objects.line_table",
            "role": "derived",
            "context": {
                "code_unit_qualpath": "code_objects.qualname",
                "code_unit_name": "code_objects.name",
                "code_unit_firstlineno": "code_objects.firstlineno1",
            },
        },
        "py_bc_instructions": {
            "root": "bytecode_files_v1",
            "path": "code_objects.instructions",
            "role": "derived",
            "context": {
                "code_unit_qualpath": "code_objects.qualname",
                "code_unit_name": "code_objects.name",
                "code_unit_firstlineno": "code_objects.firstlineno1",
            },
        },
        "py_bc_cache_entries": {
            "root": "bytecode_files_v1",
            "path": "code_objects.instructions.cache_info",
            "role": "derived",
            "context": {
                "code_unit_qualpath": "code_objects.qualname",
                "code_unit_name": "code_objects.name",
                "code_unit_firstlineno": "code_objects.firstlineno1",
                "instr_index": "code_objects.instructions.instr_index",
                "offset": "code_objects.instructions.offset",
            },
        },
        "py_bc_consts": {
            "root": "bytecode_files_v1",
            "path": "code_objects.consts",
            "role": "derived",
            "context": {
                "code_unit_qualpath": "code_objects.qualname",
                "code_unit_name": "code_objects.name",
                "code_unit_firstlineno": "code_objects.firstlineno1",
            },
        },
        "py_bc_blocks": {
            "root": "bytecode_files_v1",
            "path": "code_objects.blocks",
            "role": "derived",
            "context": {
                "code_unit_qualpath": "code_objects.qualname",
                "code_unit_name": "code_objects.name",
                "code_unit_firstlineno": "code_objects.firstlineno1",
            },
        },
        "py_bc_cfg_edges": {
            "root": "bytecode_files_v1",
            "path": "code_objects.cfg_edges",
            "role": "derived",
            "context": {
                "code_unit_qualpath": "code_objects.qualname",
                "code_unit_name": "code_objects.name",
                "code_unit_firstlineno": "code_objects.firstlineno1",
            },
        },
        "py_bc_dfg_edges": {
            "root": "bytecode_files_v1",
            "path": "code_objects.dfg_edges",
            "role": "derived",
            "context": {
                "code_unit_qualpath": "code_objects.qualname",
                "code_unit_name": "code_objects.name",
                "code_unit_firstlineno": "code_objects.firstlineno1",
            },
        },
        "bytecode_exception_table": {
            "root": "bytecode_files_v1",
            "path": "code_objects.exception_table",
            "role": "derived",
            "context": {
                "code_unit_qualpath": "code_objects.qualname",
                "code_unit_name": "code_objects.name",
                "code_unit_firstlineno": "code_objects.firstlineno1",
            },
        },
        "bytecode_errors": {
            "root": "bytecode_files_v1",
            "path": "errors",
            "role": "derived",
            "context": {},
        },
        "scip_metadata": {
            "root": "scip_index_v1",
            "path": "metadata",
            "role": "intrinsic",
            "context": {},
        },
        "scip_index_stats": {
            "root": "scip_index_v1",
            "path": "index_stats",
            "role": "intrinsic",
            "context": {},
        },
        "scip_documents": {
            "root": "scip_index_v1",
            "path": "documents",
            "role": "intrinsic",
            "context": {},
        },
        "scip_document_texts": {
            "root": "scip_index_v1",
            "path": "documents",
            "role": "derived",
            "context": {},
        },
        "scip_document_symbols": {
            "root": "scip_index_v1",
            "path": "documents.symbols",
            "role": "derived",
            "context": {
                "document_id": "documents.document_id",
                "path": "documents.path",
            },
        },
        "scip_occurrences": {
            "root": "scip_index_v1",
            "path": "documents.occurrences",
            "role": "derived",
            "context": {
                "document_id": "documents.document_id",
                "path": "documents.path",
            },
        },
        "scip_diagnostics": {
            "root": "scip_index_v1",
            "path": "documents.diagnostics",
            "role": "derived",
            "context": {
                "document_id": "documents.document_id",
                "path": "documents.path",
            },
        },
        "scip_symbol_information": {
            "root": "scip_index_v1",
            "path": "symbol_information",
            "role": "intrinsic",
            "context": {},
        },
        "scip_external_symbol_information": {
            "root": "scip_index_v1",
            "path": "external_symbol_information",
            "role": "intrinsic",
            "context": {},
        },
        "scip_symbol_relationships": {
            "root": "scip_index_v1",
            "path": "symbol_relationships",
            "role": "intrinsic",
            "context": {},
        },
        "scip_signature_occurrences": {
            "root": "scip_index_v1",
            "path": "signature_occurrences",
            "role": "intrinsic",
            "context": {},
        },
    }
)

ROOT_IDENTITY_FIELDS: ImmutableRegistry[str, tuple[str, ...]] = ImmutableRegistry.from_dict(
    {
        "ast_files_v1": ("file_id", "path"),
        "bytecode_files_v1": ("file_id", "path"),
        "libcst_files_v1": ("file_id", "path"),
        "symtable_files_v1": ("file_id", "path"),
        "tree_sitter_files_v1": ("file_id", "path"),
        "scip_index_v1": ("index_id", "index_path"),
    }
)


# ---------------------------------------------------------------------------
# Nested dataset accessor functions
# ---------------------------------------------------------------------------


def nested_dataset_registry() -> MappingRegistryAdapter[str, NestedDatasetSpec]:
    """Return a registry adapter for nested dataset specs.

    Returns:
    -------
    MappingRegistryAdapter[str, NestedDatasetSpec]
        Read-only registry adapter for nested dataset specs.
    """
    return MappingRegistryAdapter.from_mapping(
        _mapping_from_registry(NESTED_DATASET_INDEX),
        read_only=True,
    )


def root_identity_registry() -> MappingRegistryAdapter[str, tuple[str, ...]]:
    """Return a registry adapter for root identity field specs.

    Returns:
    -------
    MappingRegistryAdapter[str, tuple[str, ...]]
        Read-only registry adapter for root identity field specs.
    """
    return MappingRegistryAdapter.from_mapping(
        _mapping_from_registry(ROOT_IDENTITY_FIELDS),
        read_only=True,
    )


def _mapping_from_registry[T](registry: ImmutableRegistry[str, T]) -> dict[str, T]:
    return {key: value for key in registry if (value := registry.get(key)) is not None}


def extract_nested_dataset_names() -> tuple[str, ...]:
    """Return extract-derived nested dataset names in sorted order.

    Returns:
    -------
    tuple[str, ...]
        Sorted extract nested dataset name tuple.
    """
    return tuple(sorted(NESTED_DATASET_INDEX))


def extract_nested_schema_names() -> tuple[str, ...]:
    """Return intrinsic extract nested dataset names in sorted order.

    Returns:
    -------
    tuple[str, ...]
        Sorted intrinsic extract nested dataset name tuple.
    """
    names: list[str] = []
    for name in NESTED_DATASET_INDEX:
        spec = NESTED_DATASET_INDEX.get(name)
        if spec is None:
            continue
        if spec["role"] == "intrinsic":
            names.append(name)
    return tuple(sorted(names))


def extract_nested_spec_for(name: str) -> NestedDatasetSpec:
    """Return the extract nested dataset spec for a name.

    Args:
        name: Description.

    Raises:
        KeyError: If the operation cannot be completed.
    """
    spec = NESTED_DATASET_INDEX.get(name)
    if spec is None:
        msg = f"Unknown extract nested dataset: {name!r}."
        raise KeyError(msg)
    return spec


def extract_datasets_for_path(root: str, path: str) -> tuple[str, ...]:
    """Return extract nested dataset names for a root/path pair.

    Returns:
    -------
    tuple[str, ...]
        Sorted extract nested dataset name tuple for the path.
    """
    names: list[str] = []
    for name in NESTED_DATASET_INDEX:
        spec = NESTED_DATASET_INDEX.get(name)
        if spec is None:
            continue
        if spec["root"] == root and spec["path"] == path:
            names.append(name)
    return tuple(sorted(names))


def extract_nested_path_for(name: str) -> tuple[str, str]:
    """Return the root schema name and path for an extract nested dataset.

    Returns:
    -------
    tuple[str, str]
        Root schema name and nested path.
    """
    spec = extract_nested_spec_for(name)
    return spec["root"], spec["path"]


def extract_nested_context_for(name: str) -> Mapping[str, str]:
    """Return the context fields for an extract nested dataset.

    Returns:
    -------
    Mapping[str, str]
        Mapping of output column name to nested context path.
    """
    return extract_nested_spec_for(name)["context"]


def extract_nested_role_for(name: str) -> Literal["intrinsic", "derived"]:
    """Return the nested role for an extract nested dataset.

    Returns:
    -------
    Literal["intrinsic", "derived"]
        Dataset role label.
    """
    return extract_nested_spec_for(name)["role"]


def is_extract_nested_dataset(name: str) -> bool:
    """Return whether a name is a known extract nested dataset.

    Returns:
    -------
    bool
        ``True`` when the dataset name is registered.
    """
    return name in NESTED_DATASET_INDEX


def is_extract_intrinsic_nested_dataset(name: str) -> bool:
    """Return whether an extract nested dataset is intrinsic.

    Returns:
    -------
    bool
        ``True`` when the dataset is intrinsic.
    """
    spec = NESTED_DATASET_INDEX.get(name)
    return spec is not None and spec["role"] == "intrinsic"


# Backward-compatible aliases
def nested_dataset_names() -> tuple[str, ...]:
    """Return extract-derived nested dataset names in sorted order.

    Returns:
    -------
    tuple[str, ...]
        Sorted extract nested dataset name tuple.
    """
    return extract_nested_dataset_names()


def nested_schema_names() -> tuple[str, ...]:
    """Return intrinsic extract nested dataset names in sorted order.

    Returns:
    -------
    tuple[str, ...]
        Sorted intrinsic extract nested dataset name tuple.
    """
    return extract_nested_schema_names()


def nested_spec_for(name: str) -> NestedDatasetSpec:
    """Return the extract nested dataset spec for a name.

    Returns:
    -------
    NestedDatasetSpec
        Nested dataset specification mapping.
    """
    return extract_nested_spec_for(name)


def datasets_for_path(root: str, path: str) -> tuple[str, ...]:
    """Return extract nested dataset names registered for a root/path pair.

    Returns:
    -------
    tuple[str, ...]
        Sorted extract nested dataset name tuple for the path.
    """
    return extract_datasets_for_path(root, path)


def nested_path_for(name: str) -> tuple[str, str]:
    """Return the root schema name and path for an extract nested dataset.

    Returns:
    -------
    tuple[str, str]
        Root schema name and nested path.
    """
    return extract_nested_path_for(name)


def nested_context_for(name: str) -> Mapping[str, str]:
    """Return the context fields for an extract nested dataset.

    Returns:
    -------
    Mapping[str, str]
        Mapping of output column name to nested context path.
    """
    return extract_nested_context_for(name)


def nested_role_for(name: str) -> Literal["intrinsic", "derived"]:
    """Return the nested role for an extract nested dataset.

    Returns:
    -------
    Literal["intrinsic", "derived"]
        Dataset role label.
    """
    return extract_nested_role_for(name)


def is_nested_dataset(name: str) -> bool:
    """Return whether a name is a known extract nested dataset.

    Returns:
    -------
    bool
        ``True`` when the dataset name is registered.
    """
    return is_extract_nested_dataset(name)


def is_intrinsic_nested_dataset(name: str) -> bool:
    """Return whether an extract nested dataset is intrinsic.

    Returns:
    -------
    bool
        ``True`` when the dataset is intrinsic.
    """
    return is_extract_intrinsic_nested_dataset(name)


# ---------------------------------------------------------------------------
# Struct path resolution
# ---------------------------------------------------------------------------


def _is_list_type(dtype: pa.DataType) -> bool:
    return pa.types.is_list(dtype) or pa.types.is_large_list(dtype)


def struct_for_path(schema: pa.Schema, path: str) -> pa.StructType:
    """Resolve the struct type for a nested path.

    Args:
        schema: Root schema to traverse.
        path: Dot-delimited nested field path.

    Returns:
        pa.StructType: Result.

    Raises:
        TypeError: If a non-struct field is traversed before path resolution completes.
        ValueError: If `path` is empty.
    """
    if not path:
        msg = "Nested schema path cannot be empty."
        raise ValueError(msg)
    current: pa.Schema | pa.StructType = schema
    for step in path.split("."):
        field = _field_from_container(current, step)
        dtype = field.type
        if _is_list_type(dtype):
            dtype = dtype.value_type
        if not pa.types.is_struct(dtype):
            msg = f"Nested path {path!r} does not resolve to a struct at {step!r}."
            raise TypeError(msg)
        current = dtype
    if isinstance(current, pa.StructType):
        return current
    msg = f"Nested path {path!r} did not resolve to a struct."
    raise TypeError(msg)


# ---------------------------------------------------------------------------
# Context field resolution
# ---------------------------------------------------------------------------


def _context_fields_for(name: str, root_schema: pa.Schema) -> tuple[pa.Field, ...]:
    context = nested_context_for(name)
    if not context:
        return ()
    fields: list[pa.Field] = []
    for alias, path in context.items():
        prefix, sep, field_name = path.rpartition(".")
        if not sep:
            field = root_schema.field(field_name)
        else:
            struct_t = struct_for_path(root_schema, prefix)
            field = struct_t.field(field_name)
        fields.append(pa.field(alias, field.type, field.nullable, metadata=field.metadata))
    return tuple(fields)


def identity_fields_for(
    name: str,
    root_schema: pa.Schema,
    row_struct: pa.StructType,
) -> tuple[str, ...]:
    """Return identity column names for a nested dataset.

    Returns:
    -------
    tuple[str, ...]
        Identity column names for the dataset.
    """
    root, _ = nested_path_for(name)
    identities = ROOT_IDENTITY_FIELDS.get(root) or ()
    row_fields = {field.name for field in row_struct}
    resolved: list[str] = []
    for field_name in identities:
        if field_name in row_fields:
            continue
        root_schema.field(field_name)
        resolved.append(field_name)
    return tuple(resolved)


# ---------------------------------------------------------------------------
# Nested view runtime helpers (DataFrame construction)
# ---------------------------------------------------------------------------


def _resolve_root_schema(ctx: SessionContext, root: str) -> pa.Schema:
    try:
        df = ctx.table(root)
    except (KeyError, RuntimeError, TypeError, ValueError):
        return extract_schema_for(root)
    resolved = df.schema()
    if isinstance(resolved, pa.Schema):
        return resolved
    to_arrow = getattr(resolved, "to_arrow", None)
    if callable(to_arrow):
        arrow_schema = to_arrow()
        if isinstance(arrow_schema, pa.Schema):
            return arrow_schema
    msg = f"Unable to resolve root schema for {root!r}."
    raise TypeError(msg)


def _append_expr_selection(
    selections: list[Expr],
    selected_names: set[str],
    *,
    name: str,
    expr: Expr,
) -> None:
    if name in selected_names:
        return
    selections.append(expr.alias(name))
    selected_names.add(name)


def _context_expr_expr(
    *,
    prefix_exprs: Mapping[str, Expr],
    ctx_path: str,
    dataset_name: str,
) -> Expr:
    prefix, sep, field_name = ctx_path.rpartition(".")
    if not sep:
        return col(field_name)
    prefix_expr = prefix_exprs.get(prefix)
    if prefix_expr is None:
        msg = f"Nested context path {ctx_path!r} not resolved for {dataset_name!r}."
        raise KeyError(msg)
    return prefix_expr[field_name]


def _resolve_nested_path(
    df: DataFrame,
    root_schema: pa.Schema,
    *,
    path: str,
) -> tuple[DataFrame, pa.StructType, Expr | None, dict[str, Expr]]:
    current_struct: pa.Schema | pa.StructType = root_schema
    current_expr: Expr | None = None
    prefix_exprs: dict[str, Expr] = {}
    parts = path.split(".") if path else []
    for idx, step in enumerate(parts):
        field = _field_from_container(current_struct, step)
        dtype = field.type
        prefix = ".".join(parts[: idx + 1])
        if _is_list_type(dtype):
            list_expr = col(step) if current_expr is None else current_expr[step]
            alias = f"n{idx}"
            df = df.with_column(alias, list_expr)
            df = df.unnest_columns(alias, preserve_nulls=False)
            current_expr = col(alias)
            value_type = dtype.value_type
            if not pa.types.is_struct(value_type):
                msg = f"Nested path {path!r} does not resolve to a struct at {step!r}."
                raise TypeError(msg)
            current_struct = value_type
            prefix_exprs[prefix] = current_expr
            continue
        if not pa.types.is_struct(dtype):
            msg = f"Nested path {path!r} does not resolve to a struct at {step!r}."
            raise TypeError(msg)
        current_expr = col(step) if current_expr is None else current_expr[step]
        current_struct = dtype
        prefix_exprs[prefix] = current_expr
    if not isinstance(current_struct, pa.StructType):
        msg = f"Nested path {path!r} did not resolve to a struct."
        raise TypeError(msg)
    return df, current_struct, current_expr, prefix_exprs


def _build_nested_selections(
    *,
    name: str,
    root_schema: pa.Schema,
    current_struct: pa.StructType,
    current_expr: Expr | None,
    prefix_exprs: Mapping[str, Expr],
) -> list[Expr]:
    identity_cols = identity_fields_for(name, root_schema, current_struct)
    context_fields = nested_context_for(name)
    selections: list[Expr] = []
    selected_names: set[str] = set()
    for field_name in identity_cols:
        _append_expr_selection(
            selections,
            selected_names,
            name=field_name,
            expr=col(field_name),
        )
    for alias, ctx_path in context_fields.items():
        expr = _context_expr_expr(
            prefix_exprs=prefix_exprs,
            ctx_path=ctx_path,
            dataset_name=name,
        )
        _append_expr_selection(selections, selected_names, name=alias, expr=expr)
    for field in current_struct:
        expr = col(field.name) if current_expr is None else current_expr[field.name]
        _append_expr_selection(selections, selected_names, name=field.name, expr=expr)
    return selections


def nested_base_df(
    ctx: SessionContext,
    name: str,
    *,
    table: str | None = None,
) -> DataFrame:
    """Return a DataFrame for a nested dataset path.

    Parameters
    ----------
    ctx:
        DataFusion session context.
    name:
        Nested dataset name.
    table:
        Optional base table name to use instead of the root schema name.

    Returns:
    -------
    DataFrame
        DataFrame projecting the nested dataset rows.

    """
    root, path = nested_path_for(name)
    root_schema = _resolve_root_schema(ctx, root)
    resolved_table = table or root
    df = ctx.table(resolved_table)
    df, current_struct, current_expr, prefix_exprs = _resolve_nested_path(
        df,
        root_schema,
        path=path,
    )
    selections = _build_nested_selections(
        name=name,
        root_schema=root_schema,
        current_struct=current_struct,
        current_expr=current_expr,
        prefix_exprs=prefix_exprs,
    )
    return df.select(*selections)


def nested_view_spec(
    ctx: SessionContext,
    name: str,
    *,
    table: str | None = None,
) -> ViewRuntimeSpec:
    """Return a runtime view spec for a nested dataset.

    Returns:
    -------
    ViewRuntimeSpec
        Runtime view specification derived from the registered base table.
    """
    _ = ctx

    def builder(runtime_ctx: SessionContext) -> DataFrame:
        return nested_base_df(runtime_ctx, name=name, table=table)

    return ViewRuntimeSpec(
        name=name,
        builder=builder,
        schema=None,
    )


def nested_view_specs(
    ctx: SessionContext,
    *,
    table: str | None = None,
) -> tuple[ViewRuntimeSpec, ...]:
    """Return runtime view specs for all nested datasets.

    Returns:
    -------
    tuple[ViewRuntimeSpec, ...]
        Runtime view specifications for nested datasets.
    """
    if table is not None and not ctx.table_exist(table):
        return ()
    specs: list[ViewRuntimeSpec] = []
    for name in nested_dataset_names():
        root, _path = nested_path_for(name)
        resolved_table = table or root
        if not ctx.table_exist(resolved_table):
            continue
        specs.append(nested_view_spec(ctx, name, table=table))
    return tuple(specs)


def validate_nested_types(ctx: SessionContext, name: str) -> None:
    """Validate nested dataset types using DataFusion arrow_typeof."""
    if name not in NESTED_DATASET_INDEX:
        return
    try:
        df = ctx.table(name)
    except (RuntimeError, TypeError, ValueError, pa.ArrowInvalid) as exc:
        _LOGGER.warning("Nested type validation skipped for %s: %s", name, exc)
        return
    expected: pa.Schema
    try:
        expected = extract_schema_for(name)
    except KeyError as exc:
        _LOGGER.warning(
            "Nested type validation skipped for %s: no derived schema authority (%s)",
            name,
            exc,
        )
        return
    from datafusion_engine.arrow.coercion import storage_type

    actual = df.schema()
    if not actual:
        return
    actual_fields = {field.name: field for field in actual}
    missing = [field.name for field in expected if field.name not in actual_fields]
    mismatched = [
        (
            field.name,
            storage_type(actual_fields[field.name].type),
            storage_type(field.type),
        )
        for field in expected
        if field.name in actual_fields
        and storage_type(actual_fields[field.name].type) != storage_type(field.type)
    ]
    if missing or mismatched:
        _LOGGER.warning(
            "Nested type validation mismatch for %s: missing=%s mismatched=%s",
            name,
            missing,
            mismatched,
        )
