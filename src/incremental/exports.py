"""Exported definition index builder for incremental impact analysis."""

from __future__ import annotations

from typing import cast

import pyarrow as pa

from arrowdsl.core.ids import prefixed_hash_id
from arrowdsl.core.interop import TableLike, pc
from arrowdsl.schema.build import table_from_arrays
from arrowdsl.schema.schema import align_table
from incremental.registry_specs import dataset_schema


def _column_or_null(table: pa.Table, name: str, dtype: pa.DataType) -> pa.Array:
    if name in table.column_names:
        return cast("pa.Array", table[name])
    return pa.nulls(table.num_rows, type=dtype)


def build_exported_defs_index(
    cst_defs_norm: TableLike,
    *,
    rel_def_symbol: TableLike | None = None,
) -> pa.Table:
    """Build the exported definitions index for top-level defs.

    Returns
    -------
    pa.Table
        Exported definitions table with qname and optional symbol bindings.
    """
    schema = dataset_schema("dim_exported_defs_v1")
    base = _build_exported_defs_base(cast("pa.Table", cst_defs_norm), schema)
    if base is None:
        return _empty_exported_defs(schema)
    if rel_def_symbol is None:
        return base
    return _attach_def_symbols(base, rel_def_symbol=rel_def_symbol, schema=schema)


def _build_exported_defs_base(
    table: pa.Table,
    schema: pa.Schema,
) -> pa.Table | None:
    if table.num_rows == 0 or "qnames" not in table.column_names:
        return None
    top = (
        table.filter(pc.is_null(table["container_def_id"]))
        if "container_def_id" in table.column_names
        else table
    )
    if top.num_rows == 0:
        return None
    qnames = top["qnames"]
    if len(qnames) == 0:
        return None
    parent_idx = pc.list_parent_indices(qnames)
    flat = pc.list_flatten(qnames)
    if len(flat) == 0:
        return None

    def_id = pc.take(_column_or_null(top, "def_id", pa.string()), parent_idx)
    file_id = pc.take(_column_or_null(top, "file_id", pa.string()), parent_idx)
    path = pc.take(_column_or_null(top, "path", pa.string()), parent_idx)
    name = pc.take(_column_or_null(top, "name", pa.string()), parent_idx)
    def_kind = _resolve_def_kind(top, parent_idx)
    qname = pc.cast(pc.struct_field(flat, "name"), pa.string())
    qname_source = pc.cast(pc.struct_field(flat, "source"), pa.string())
    qname_id = prefixed_hash_id([qname], prefix="qname")

    return table_from_arrays(
        schema,
        columns={
            "file_id": file_id,
            "path": path,
            "def_id": def_id,
            "def_kind_norm": def_kind,
            "name": name,
            "qname_id": qname_id,
            "qname": qname,
            "qname_source": qname_source,
            "symbol": pa.nulls(len(qname), type=pa.string()),
            "symbol_roles": pa.nulls(len(qname), type=pa.int32()),
        },
        num_rows=len(qname),
    )


def _attach_def_symbols(
    base: pa.Table,
    *,
    rel_def_symbol: TableLike,
    schema: pa.Schema,
) -> pa.Table:
    rel_table = cast("pa.Table", rel_def_symbol)
    if rel_table.num_rows == 0 or "def_id" not in rel_table.column_names:
        return base
    rel_subset = rel_table.select(["def_id", "path", "symbol", "symbol_roles"])
    base_trimmed = base
    if "symbol" in base.column_names and "symbol_roles" in base.column_names:
        base_trimmed = base.drop(["symbol", "symbol_roles"])
    joined = base_trimmed.join(rel_subset, keys=["def_id", "path"], join_type="left outer")
    return align_table(joined, schema=schema, safe_cast=True)


def _resolve_def_kind(top: pa.Table, parent_idx: pa.Array) -> pa.Array:
    def_kind = _column_or_null(top, "def_kind_norm", pa.string())
    if def_kind.null_count == top.num_rows:
        def_kind = _column_or_null(top, "kind", pa.string())
    return pc.take(def_kind, parent_idx)


def _empty_exported_defs(schema: pa.Schema) -> pa.Table:
    return table_from_arrays(schema, columns={}, num_rows=0)


__all__ = ["build_exported_defs_index"]
