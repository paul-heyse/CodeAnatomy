"""Normalize type expressions into type nodes and edges."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.core.interop import ArrayLike, TableLike, pc
from arrowdsl.schema.arrays import const_array
from arrowdsl.schema.schema import empty_table
from normalize.arrow_utils import column_or_null, compute_array, trimmed_non_empty_utf8
from normalize.ids import masked_prefixed_hash, prefixed_hash64
from normalize.pipeline import canonical_sort
from normalize.schemas import TYPE_EXPRS_NORM_SCHEMA, TYPE_NODES_SCHEMA


def _base_type_exprs_table(cst_type_exprs: TableLike) -> TableLike:
    return pa.Table.from_arrays(
        [
            column_or_null(cst_type_exprs, "file_id", pa.string()),
            column_or_null(cst_type_exprs, "path", pa.string()),
            column_or_null(cst_type_exprs, "bstart", pa.int64()),
            column_or_null(cst_type_exprs, "bend", pa.int64()),
            column_or_null(cst_type_exprs, "owner_def_id", pa.string()),
            column_or_null(cst_type_exprs, "param_name", pa.string()),
            column_or_null(cst_type_exprs, "expr_kind", pa.string()),
            column_or_null(cst_type_exprs, "expr_role", pa.string()),
            column_or_null(cst_type_exprs, "expr_text", pa.string()),
        ],
        names=[
            "file_id",
            "path",
            "bstart",
            "bend",
            "owner_def_id",
            "param_name",
            "expr_kind",
            "expr_role",
            "expr_text",
        ],
    )


def _dedupe_type_nodes(table: TableLike) -> TableLike:
    if table.num_rows == 0:
        return empty_table(TYPE_NODES_SCHEMA)
    non_keys = [col for col in table.column_names if col != "type_id"]
    aggs = [(col, "first") for col in non_keys]
    out = table.group_by(["type_id"], use_threads=False).aggregate(aggs)
    rename_map = {f"{col}_first": col for col in non_keys}
    new_names = [rename_map.get(name, name) for name in out.schema.names]
    out = out.rename_columns(new_names)
    return canonical_sort(out, sort_keys=[("type_id", "ascending")])


def _build_type_nodes(
    type_id: ArrayLike,
    type_repr: ArrayLike,
    *,
    type_form: str,
    origin: str,
) -> TableLike:
    n = len(type_id)
    if n == 0:
        return empty_table(TYPE_NODES_SCHEMA)
    return pa.Table.from_arrays(
        [
            type_id,
            type_repr,
            const_array(n, type_form, dtype=pa.string()),
            const_array(n, origin, dtype=pa.string()),
        ],
        schema=TYPE_NODES_SCHEMA,
    )


def normalize_type_exprs(cst_type_exprs: TableLike) -> TableLike:
    """Normalize type expression rows into join-ready tables.

    Parameters
    ----------
    cst_type_exprs:
        CST type expression rows captured during extraction.

    Returns
    -------
    TableLike
        Normalized type expressions table with type ids.
    """
    if cst_type_exprs.num_rows == 0:
        return empty_table(TYPE_EXPRS_NORM_SCHEMA)

    base = _base_type_exprs_table(cst_type_exprs)
    trimmed, mask = trimmed_non_empty_utf8(base["expr_text"])
    filtered = base.filter(mask)
    if filtered.num_rows == 0:
        return empty_table(TYPE_EXPRS_NORM_SCHEMA)

    type_repr = compute_array("filter", [trimmed, mask])
    path_arr = filtered["path"]
    bstart_arr = filtered["bstart"]
    bend_arr = filtered["bend"]
    type_expr_id = masked_prefixed_hash(
        "cst_type_expr",
        [path_arr, bstart_arr, bend_arr],
        required=[path_arr, bstart_arr, bend_arr],
    )
    type_id = prefixed_hash64("type", [type_repr])

    return pa.Table.from_arrays(
        [
            filtered["file_id"],
            path_arr,
            bstart_arr,
            bend_arr,
            type_expr_id,
            filtered["owner_def_id"],
            filtered["param_name"],
            filtered["expr_kind"],
            filtered["expr_role"],
            filtered["expr_text"],
            type_repr,
            type_id,
        ],
        schema=TYPE_EXPRS_NORM_SCHEMA,
    )


def normalize_types(
    type_exprs_norm: TableLike,
    scip_symbol_information: TableLike | None = None,
) -> TableLike:
    """Build a type node table from normalized type expressions.

    Parameters
    ----------
    type_exprs_norm:
        Normalized type expression table.
    scip_symbol_information:
        Optional SCIP symbol information table with type details.

    Returns
    -------
    TableLike
        Normalized type node table.
    """
    scip_table = _types_from_scip(scip_symbol_information)
    if scip_table is not None:
        return scip_table
    return _types_from_type_exprs(type_exprs_norm)


def _types_from_scip(scip_symbol_information: TableLike | None) -> TableLike | None:
    if scip_symbol_information is None or scip_symbol_information.num_rows == 0:
        return None
    if "type_repr" not in scip_symbol_information.column_names:
        return None

    type_repr_raw = column_or_null(scip_symbol_information, "type_repr", pa.string())
    trimmed, mask = trimmed_non_empty_utf8(type_repr_raw)
    type_repr = compute_array("filter", [trimmed, mask])
    if len(type_repr) == 0:
        return None

    type_id = prefixed_hash64("type", [type_repr])
    table = _build_type_nodes(type_id, type_repr, type_form="scip", origin="inferred")
    return _dedupe_type_nodes(table)


def _types_from_type_exprs(type_exprs_norm: TableLike) -> TableLike:
    if type_exprs_norm.num_rows == 0:
        return empty_table(TYPE_NODES_SCHEMA)
    if (
        "type_id" not in type_exprs_norm.column_names
        or "type_repr" not in type_exprs_norm.column_names
    ):
        return empty_table(TYPE_NODES_SCHEMA)

    type_id_raw = column_or_null(type_exprs_norm, "type_id", pa.string())
    type_repr_raw = column_or_null(type_exprs_norm, "type_repr", pa.string())
    trimmed, mask = trimmed_non_empty_utf8(type_repr_raw)
    mask = pc.and_(mask, pc.is_valid(type_id_raw))
    type_id = compute_array("filter", [type_id_raw, mask])
    type_repr = compute_array("filter", [trimmed, mask])
    if len(type_id) == 0:
        return empty_table(TYPE_NODES_SCHEMA)

    table = _build_type_nodes(type_id, type_repr, type_form="annotation", origin="annotation")
    return _dedupe_type_nodes(table)
