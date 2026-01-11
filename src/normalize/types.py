"""Normalize type expressions into type nodes and edges."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.empty import empty_table
from normalize.ids import stable_id

SCHEMA_VERSION = 1


TYPE_EXPRS_NORM_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("type_expr_id", pa.string()),
        ("owner_def_id", pa.string()),
        ("param_name", pa.string()),
        ("expr_kind", pa.string()),
        ("expr_role", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
        ("expr_text", pa.string()),
        ("type_repr", pa.string()),
        ("type_id", pa.string()),
    ]
)

TYPE_NODES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("type_id", pa.string()),
        ("type_repr", pa.string()),
        ("type_form", pa.string()),
        ("origin", pa.string()),
    ]
)


def normalize_type_exprs(cst_type_exprs: pa.Table) -> pa.Table:
    """Normalize type expression rows into join-ready tables.

    Parameters
    ----------
    cst_type_exprs:
        CST type expression rows captured during extraction.

    Returns
    -------
    pa.Table
        Normalized type expressions table with type ids.
    """
    if cst_type_exprs.num_rows == 0:
        return empty_table(TYPE_EXPRS_NORM_SCHEMA)

    out_rows: list[dict[str, object]] = []
    for row in cst_type_exprs.to_pylist():
        expr_text = row.get("expr_text")
        if not isinstance(expr_text, str):
            continue
        type_repr = expr_text.strip()
        if not type_repr:
            continue

        type_expr_id = row.get("type_expr_id")
        if not isinstance(type_expr_id, str):
            path = row.get("path")
            bstart = row.get("bstart")
            bend = row.get("bend")
            type_expr_id = stable_id(
                "type_expr",
                str(path),
                str(bstart),
                str(bend),
            )

        out_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "type_expr_id": type_expr_id,
                "owner_def_id": row.get("owner_def_id"),
                "param_name": row.get("param_name"),
                "expr_kind": row.get("expr_kind"),
                "expr_role": row.get("expr_role"),
                "file_id": row.get("file_id"),
                "path": row.get("path"),
                "bstart": row.get("bstart"),
                "bend": row.get("bend"),
                "expr_text": expr_text,
                "type_repr": type_repr,
                "type_id": stable_id("type", type_repr),
            }
        )

    if not out_rows:
        return empty_table(TYPE_EXPRS_NORM_SCHEMA)
    return pa.Table.from_pylist(out_rows, schema=TYPE_EXPRS_NORM_SCHEMA)


def normalize_types(
    type_exprs_norm: pa.Table,
    scip_symbol_information: pa.Table | None = None,
) -> pa.Table:
    """Build a type node table from normalized type expressions.

    Parameters
    ----------
    type_exprs_norm:
        Normalized type expression table.
    scip_symbol_information:
        Optional SCIP symbol information table with type details.

    Returns
    -------
    pa.Table
        Normalized type node table.
    """
    scip_table = _types_from_scip(scip_symbol_information)
    if scip_table is not None:
        return scip_table
    return _types_from_type_exprs(type_exprs_norm)


def _types_from_scip(scip_symbol_information: pa.Table | None) -> pa.Table | None:
    if scip_symbol_information is None or scip_symbol_information.num_rows == 0:
        return None
    if "type_repr" not in scip_symbol_information.column_names:
        return None
    out_rows: list[dict[str, object]] = []
    for row in scip_symbol_information.to_pylist():
        type_repr = row.get("type_repr")
        if not isinstance(type_repr, str) or not type_repr:
            continue
        type_id = stable_id("type", type_repr)
        out_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "type_id": type_id,
                "type_repr": type_repr,
                "type_form": "scip",
                "origin": "inferred",
            }
        )
    if not out_rows:
        return None
    return pa.Table.from_pylist(out_rows, schema=TYPE_NODES_SCHEMA)


def _types_from_type_exprs(type_exprs_norm: pa.Table) -> pa.Table:
    if type_exprs_norm.num_rows == 0:
        return empty_table(TYPE_NODES_SCHEMA)
    seen: dict[str, dict[str, object]] = {}
    for row in type_exprs_norm.to_pylist():
        type_id = row.get("type_id")
        type_repr = row.get("type_repr")
        if not isinstance(type_id, str) or not isinstance(type_repr, str):
            continue
        if type_id in seen:
            continue
        seen[type_id] = {
            "schema_version": SCHEMA_VERSION,
            "type_id": type_id,
            "type_repr": type_repr,
            "type_form": "annotation",
            "origin": "annotation",
        }
    if not seen:
        return empty_table(TYPE_NODES_SCHEMA)
    return pa.Table.from_pylist(list(seen.values()), schema=TYPE_NODES_SCHEMA)
