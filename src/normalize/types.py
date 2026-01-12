"""Normalize type expressions into type nodes and edges."""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa

from arrowdsl.core.ids import iter_array_values, iter_arrays, prefixed_hash_id
from arrowdsl.core.interop import ArrayLike, TableLike, pc
from arrowdsl.schema.arrays import const_array
from arrowdsl.schema.schema import empty_table
from schema_spec.specs import ArrowFieldSpec, file_identity_bundle, span_bundle
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY, make_dataset_spec, make_table_spec

SCHEMA_VERSION = 1


TYPE_EXPRS_NORM_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=make_table_spec(
            name="type_exprs_norm_v1",
            version=SCHEMA_VERSION,
            bundles=(file_identity_bundle(include_sha256=False), span_bundle()),
            fields=[
                ArrowFieldSpec(name="type_expr_id", dtype=pa.string()),
                ArrowFieldSpec(name="owner_def_id", dtype=pa.string()),
                ArrowFieldSpec(name="param_name", dtype=pa.string()),
                ArrowFieldSpec(name="expr_kind", dtype=pa.string()),
                ArrowFieldSpec(name="expr_role", dtype=pa.string()),
                ArrowFieldSpec(name="expr_text", dtype=pa.string()),
                ArrowFieldSpec(name="type_repr", dtype=pa.string()),
                ArrowFieldSpec(name="type_id", dtype=pa.string()),
            ],
        )
    )
)

TYPE_NODES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=make_table_spec(
            name="type_nodes_v1",
            version=SCHEMA_VERSION,
            bundles=(),
            fields=[
                ArrowFieldSpec(name="type_id", dtype=pa.string()),
                ArrowFieldSpec(name="type_repr", dtype=pa.string()),
                ArrowFieldSpec(name="type_form", dtype=pa.string()),
                ArrowFieldSpec(name="origin", dtype=pa.string()),
            ],
        )
    )
)

TYPE_EXPRS_NORM_SCHEMA = TYPE_EXPRS_NORM_SPEC.table_spec.to_arrow_schema()
TYPE_NODES_SCHEMA = TYPE_NODES_SPEC.table_spec.to_arrow_schema()


def _prefixed_hash64(
    prefix: str,
    arrays: list[ArrayLike],
) -> ArrayLike:
    return prefixed_hash_id(arrays, prefix=prefix)


@dataclass
class _TypeExprBuffers:
    owner_def_ids: list[object | None] = field(default_factory=list)
    param_names: list[object | None] = field(default_factory=list)
    expr_kinds: list[object | None] = field(default_factory=list)
    expr_roles: list[object | None] = field(default_factory=list)
    file_ids: list[object | None] = field(default_factory=list)
    paths: list[object | None] = field(default_factory=list)
    bstarts: list[object | None] = field(default_factory=list)
    bends: list[object | None] = field(default_factory=list)
    expr_texts: list[str] = field(default_factory=list)
    type_reprs: list[str] = field(default_factory=list)

    def append(self, row: _TypeExprRow) -> None:
        self.owner_def_ids.append(row.owner_def_id)
        self.param_names.append(row.param_name)
        self.expr_kinds.append(row.expr_kind)
        self.expr_roles.append(row.expr_role)
        self.file_ids.append(row.file_id)
        self.paths.append(row.path)
        self.bstarts.append(row.bstart)
        self.bends.append(row.bend)
        self.expr_texts.append(row.expr_text)
        self.type_reprs.append(row.type_repr)


@dataclass(frozen=True)
class _TypeExprRow:
    owner_def_id: object | None
    param_name: object | None
    expr_kind: object | None
    expr_role: object | None
    file_id: object | None
    path: object | None
    bstart: object | None
    bend: object | None
    expr_text: str
    type_repr: str


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

    buffers = _TypeExprBuffers()
    arrays = [
        cst_type_exprs["expr_text"],
        cst_type_exprs["owner_def_id"] if "owner_def_id" in cst_type_exprs.column_names else None,
        cst_type_exprs["param_name"] if "param_name" in cst_type_exprs.column_names else None,
        cst_type_exprs["expr_kind"] if "expr_kind" in cst_type_exprs.column_names else None,
        cst_type_exprs["expr_role"] if "expr_role" in cst_type_exprs.column_names else None,
        cst_type_exprs["file_id"] if "file_id" in cst_type_exprs.column_names else None,
        cst_type_exprs["path"] if "path" in cst_type_exprs.column_names else None,
        cst_type_exprs["bstart"] if "bstart" in cst_type_exprs.column_names else None,
        cst_type_exprs["bend"] if "bend" in cst_type_exprs.column_names else None,
    ]
    missing = [arr is None for arr in arrays]
    arrays = [
        arr if arr is not None else pa.nulls(cst_type_exprs.num_rows, type=pa.string())
        for arr in arrays
    ]

    for (
        expr_text,
        owner_def_id,
        param_name,
        expr_kind,
        expr_role,
        file_id,
        path,
        bstart,
        bend,
    ) in iter_arrays(arrays):
        if not isinstance(expr_text, str):
            continue
        type_repr = expr_text.strip()
        if not type_repr:
            continue
        buffers.append(
            _TypeExprRow(
                owner_def_id=None if missing[1] else owner_def_id,
                param_name=None if missing[2] else param_name,
                expr_kind=None if missing[3] else expr_kind,
                expr_role=None if missing[4] else expr_role,
                file_id=None if missing[5] else file_id,
                path=None if missing[6] else path,
                bstart=None if missing[7] else bstart,
                bend=None if missing[8] else bend,
                expr_text=expr_text,
                type_repr=type_repr,
            )
        )

    if not buffers.type_reprs:
        return empty_table(TYPE_EXPRS_NORM_SCHEMA)

    path_arr = pa.array(buffers.paths, type=pa.string())
    bstart_arr = pa.array(buffers.bstarts, type=pa.int64())
    bend_arr = pa.array(buffers.bends, type=pa.int64())
    type_expr_prefixed = _prefixed_hash64("cst_type_expr", [path_arr, bstart_arr, bend_arr])
    valid = pc.and_(pc.is_valid(path_arr), pc.and_(pc.is_valid(bstart_arr), pc.is_valid(bend_arr)))
    type_expr_id = pc.if_else(valid, type_expr_prefixed, pa.scalar(None, type=pa.string()))

    type_repr_arr = pa.array(buffers.type_reprs, type=pa.string())
    type_id = _prefixed_hash64("type", [type_repr_arr])

    return pa.Table.from_arrays(
        [
            pa.array(buffers.file_ids, type=pa.string()),
            path_arr,
            bstart_arr,
            bend_arr,
            type_expr_id,
            pa.array(buffers.owner_def_ids, type=pa.string()),
            pa.array(buffers.param_names, type=pa.string()),
            pa.array(buffers.expr_kinds, type=pa.string()),
            pa.array(buffers.expr_roles, type=pa.string()),
            pa.array(buffers.expr_texts, type=pa.string()),
            type_repr_arr,
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
    type_reprs: list[str] = []
    seen: set[str] = set()
    for value in iter_array_values(scip_symbol_information["type_repr"]):
        if not isinstance(value, str):
            continue
        raw = value.strip()
        if not raw or raw in seen:
            continue
        seen.add(raw)
        type_reprs.append(raw)
    if not type_reprs:
        return None

    type_repr_arr = pa.array(type_reprs, type=pa.string())
    type_id = _prefixed_hash64("type", [type_repr_arr])
    n = len(type_reprs)
    return pa.Table.from_arrays(
        [
            type_id,
            type_repr_arr,
            const_array(n, "scip", dtype=pa.string()),
            const_array(n, "inferred", dtype=pa.string()),
        ],
        schema=TYPE_NODES_SCHEMA,
    )


def _types_from_type_exprs(type_exprs_norm: TableLike) -> TableLike:
    if type_exprs_norm.num_rows == 0:
        return empty_table(TYPE_NODES_SCHEMA)
    if (
        "type_id" not in type_exprs_norm.column_names
        or "type_repr" not in type_exprs_norm.column_names
    ):
        return empty_table(TYPE_NODES_SCHEMA)

    type_ids: list[str] = []
    type_reprs: list[str] = []
    seen: set[str] = set()
    arrays = [type_exprs_norm["type_id"], type_exprs_norm["type_repr"]]
    for type_id, type_repr in iter_arrays(arrays):
        if not isinstance(type_id, str) or not isinstance(type_repr, str):
            continue
        if type_id in seen:
            continue
        seen.add(type_id)
        type_ids.append(type_id)
        type_reprs.append(type_repr)

    if not type_ids:
        return empty_table(TYPE_NODES_SCHEMA)

    n = len(type_ids)
    return pa.Table.from_arrays(
        [
            pa.array(type_ids, type=pa.string()),
            pa.array(type_reprs, type=pa.string()),
            const_array(n, "annotation", dtype=pa.string()),
            const_array(n, "annotation", dtype=pa.string()),
        ],
        schema=TYPE_NODES_SCHEMA,
    )
