"""Generic CPG builder engines driven by specs."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.compute import pc
from arrowdsl.ids import hash64_from_arrays
from arrowdsl.iter import iter_arrays
from cpg.kinds import EntityKind
from cpg.specs import (
    EdgeEmitSpec,
    EdgePlanSpec,
    NodeEmitSpec,
    NodePlanSpec,
    PropOptions,
    PropTableSpec,
)

type PropValue = object | None
type PropRow = dict[str, object]


def _const_str(n: int, value: str) -> pa.Array:
    return pa.array([value] * n, type=pa.string())


def _const_i32(n: int, value: int) -> pa.Array:
    return pa.array([int(value)] * n, type=pa.int32())


def _const_f32(n: int, value: float) -> pa.Array:
    return pa.array([float(value)] * n, type=pa.float32())


def _row_id(value: object | None) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, int) and not isinstance(value, bool) and value:
        return str(value)
    return None


def _first_non_null(row: Mapping[str, object], cols: Sequence[str]) -> object | None:
    for col in cols:
        value = row.get(col)
        if value is not None:
            return value
    return None


def _pick_first(table: pa.Table, cols: Sequence[str], *, default_type: pa.DataType) -> pa.Array:
    for col in cols:
        if col in table.column_names:
            return table[col]
    return pa.nulls(table.num_rows, type=default_type)


def _resolve_string_col(rel: pa.Table, col: str, *, default_value: str) -> pa.Array:
    arr = _pick_first(rel, [col], default_type=pa.string())
    if arr.null_count == rel.num_rows:
        return _const_str(rel.num_rows, default_value)
    return arr


def _resolve_float_col(rel: pa.Table, col: str, *, default_value: float) -> pa.Array:
    arr = _pick_first(rel, [col], default_type=pa.float32())
    if arr.null_count == rel.num_rows:
        return _const_f32(rel.num_rows, default_value)
    return arr


@dataclass(frozen=True)
class EdgeIdArrays:
    """Column arrays required to compute edge IDs."""

    src: pa.Array | pa.ChunkedArray
    dst: pa.Array | pa.ChunkedArray
    path: pa.Array | pa.ChunkedArray
    bstart: pa.Array | pa.ChunkedArray
    bend: pa.Array | pa.ChunkedArray


def _edge_id_array(
    *,
    edge_kind: str,
    inputs: EdgeIdArrays,
) -> pa.Array | pa.ChunkedArray:
    n = len(inputs.src)
    kind_arr = _const_str(n, edge_kind)
    base_hash = hash64_from_arrays([kind_arr, inputs.src, inputs.dst], prefix="edge")
    full_hash = hash64_from_arrays(
        [kind_arr, inputs.src, inputs.dst, inputs.path, inputs.bstart, inputs.bend],
        prefix="edge",
    )
    has_span = pc.and_(
        pc.is_valid(inputs.path),
        pc.and_(pc.is_valid(inputs.bstart), pc.is_valid(inputs.bend)),
    )
    chosen = pc.if_else(has_span, full_hash, base_hash)
    chosen_str = pc.cast(chosen, pa.string())
    edge_id = pc.binary_join_element_wise(pa.scalar("edge"), chosen_str, ":")
    valid = pc.and_(pc.is_valid(inputs.src), pc.is_valid(inputs.dst))
    return pc.if_else(valid, edge_id, pa.scalar(None, type=pa.string()))


def emit_edges_from_relation(
    rel: pa.Table,
    *,
    spec: EdgeEmitSpec,
    schema_version: int,
    edge_schema: pa.Schema,
) -> pa.Table:
    """Emit edge rows from a relation table.

    Parameters
    ----------
    rel:
        Relation table containing source/target columns.
    spec:
        Emission spec describing source/target column candidates.
    schema_version:
        Schema version to write.
    edge_schema:
        Target edge schema.

    Returns
    -------
    pa.Table
        Emitted edge table.
    """
    if rel.num_rows == 0:
        return pa.Table.from_arrays(
            [pa.array([], type=f.type) for f in edge_schema],
            schema=edge_schema,
        )

    n = rel.num_rows
    src = _pick_first(rel, spec.src_cols, default_type=pa.string())
    dst = _pick_first(rel, spec.dst_cols, default_type=pa.string())
    path = _pick_first(rel, spec.path_cols, default_type=pa.string())
    bstart = _pick_first(rel, spec.bstart_cols, default_type=pa.int64())
    bend = _pick_first(rel, spec.bend_cols, default_type=pa.int64())
    edge_ids = _edge_id_array(
        edge_kind=spec.edge_kind.value,
        inputs=EdgeIdArrays(src=src, dst=dst, path=path, bstart=bstart, bend=bend),
    )

    default_score = 1.0 if spec.origin == "scip" else 0.5
    origin = _resolve_string_col(rel, "origin", default_value=spec.origin)
    origin = pc.fill_null(origin, replacement=spec.origin)

    out = pa.Table.from_arrays(
        [
            _const_i32(n, schema_version),
            edge_ids,
            _const_str(n, spec.edge_kind.value),
            src,
            dst,
            path,
            bstart,
            bend,
            origin,
            _resolve_string_col(
                rel, "resolution_method", default_value=spec.default_resolution_method
            ),
            _resolve_float_col(rel, "confidence", default_value=default_score),
            _resolve_float_col(rel, "score", default_value=default_score),
            _pick_first(rel, ["symbol_roles"], default_type=pa.int32()),
            _pick_first(rel, ["qname_source"], default_type=pa.string()),
            _pick_first(rel, ["ambiguity_group_id"], default_type=pa.string()),
            _pick_first(rel, ["rule_name"], default_type=pa.string()),
            _pick_first(rel, ["rule_priority"], default_type=pa.int32()),
        ],
        schema=edge_schema,
    )
    mask = pc.fill_null(pc.is_valid(edge_ids), replacement=False)
    return out.filter(mask)


class EdgeBuilder:
    """Build edges from declarative specs."""

    def __init__(
        self,
        *,
        emitters: Sequence[EdgePlanSpec],
        schema_version: int,
        edge_schema: pa.Schema,
    ) -> None:
        self._emitters = tuple(emitters)
        self._schema_version = schema_version
        self._edge_schema = edge_schema

    def build(
        self,
        *,
        tables: Mapping[str, pa.Table],
        options: object,
    ) -> list[pa.Table]:
        """Build edge tables from configured emitters.

        Returns
        -------
        list[pa.Table]
            Edge table parts ready for concatenation.

        Raises
        ------
        ValueError
            If the configured option flag is missing on the options object.
        """
        parts: list[pa.Table] = []
        for emitter in self._emitters:
            enabled = getattr(options, emitter.option_flag, None)
            if enabled is None:
                msg = f"Unknown option flag: {emitter.option_flag}"
                raise ValueError(msg)
            if not enabled:
                continue
            rel = emitter.relation_getter(tables)
            if rel is None or rel.num_rows == 0:
                continue
            if emitter.filter_fn is not None:
                mask = emitter.filter_fn(rel)
                mask = pc.fill_null(mask, replacement=False)
                rel = rel.filter(mask)
                if rel.num_rows == 0:
                    continue
            parts.append(
                emit_edges_from_relation(
                    rel,
                    spec=emitter.emit,
                    schema_version=self._schema_version,
                    edge_schema=self._edge_schema,
                )
            )
        return parts


def emit_nodes_from_table(
    table: pa.Table,
    *,
    spec: NodeEmitSpec,
    schema_version: int,
    node_schema: pa.Schema,
) -> pa.Table:
    """Emit node rows from a table.

    Returns
    -------
    pa.Table
        Node table conforming to the configured schema.
    """
    if table.num_rows == 0:
        return pa.Table.from_arrays(
            [pa.array([], type=f.type) for f in node_schema],
            schema=node_schema,
        )

    n = table.num_rows
    node_id = _pick_first(table, spec.id_cols, default_type=pa.string())
    path = _pick_first(table, spec.path_cols, default_type=pa.string())
    bstart = _pick_first(table, spec.bstart_cols, default_type=pa.int64())
    bend = _pick_first(table, spec.bend_cols, default_type=pa.int64())
    file_id = _pick_first(table, spec.file_id_cols, default_type=pa.string())

    return pa.Table.from_arrays(
        [
            _const_i32(n, schema_version),
            node_id,
            _const_str(n, spec.node_kind.value),
            path,
            bstart,
            bend,
            file_id,
        ],
        schema=node_schema,
    )


class NodeBuilder:
    """Build nodes from declarative specs."""

    def __init__(
        self,
        *,
        emitters: Sequence[NodePlanSpec],
        schema_version: int,
        node_schema: pa.Schema,
    ) -> None:
        self._emitters = tuple(emitters)
        self._schema_version = schema_version
        self._node_schema = node_schema

    def build(
        self,
        *,
        tables: Mapping[str, pa.Table],
        options: object,
    ) -> list[pa.Table]:
        """Build node tables from configured emitters.

        Returns
        -------
        list[pa.Table]
            Node table parts ready for concatenation.

        Raises
        ------
        ValueError
            If the configured option flag is missing on the options object.
        """
        parts: list[pa.Table] = []
        for emitter in self._emitters:
            enabled = getattr(options, emitter.option_flag, None)
            if enabled is None:
                msg = f"Unknown option flag: {emitter.option_flag}"
                raise ValueError(msg)
            if not enabled:
                continue
            table = emitter.table_getter(tables)
            if table is None or table.num_rows == 0:
                continue
            if emitter.preprocessor is not None:
                table = emitter.preprocessor(table)
                if table.num_rows == 0:
                    continue
            parts.append(
                emit_nodes_from_table(
                    table,
                    spec=emitter.emit,
                    schema_version=self._schema_version,
                    node_schema=self._node_schema,
                )
            )
        return parts


@dataclass(frozen=True)
class PropPayload:
    """Payload for emitting a single property row."""

    entity_kind: EntityKind
    entity_id: str
    key: str
    value: PropValue


def _prop_row(schema_version: int, payload: PropPayload) -> PropRow:
    rec: PropRow = {
        "schema_version": schema_version,
        "entity_kind": payload.entity_kind.value,
        "entity_id": str(payload.entity_id),
        "prop_key": str(payload.key),
        "value_str": None,
        "value_int": None,
        "value_float": None,
        "value_bool": None,
        "value_json": None,
    }

    value = payload.value
    if value is None:
        return rec

    if isinstance(value, bool):
        rec["value_bool"] = bool(value)
    elif isinstance(value, int) and not isinstance(value, bool):
        rec["value_int"] = int(value)
    elif isinstance(value, float):
        rec["value_float"] = float(value)
    elif isinstance(value, str):
        rec["value_str"] = value
    else:
        try:
            rec["value_json"] = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        except (TypeError, ValueError):
            rec["value_json"] = json.dumps(str(value), ensure_ascii=False)
    return rec


class PropBuilder:
    """Build property rows from table specs."""

    def __init__(
        self,
        *,
        table_specs: Sequence[PropTableSpec],
        schema_version: int,
        prop_schema: pa.Schema,
    ) -> None:
        self._table_specs = tuple(table_specs)
        self._schema_version = schema_version
        self._prop_schema = prop_schema

    def build(
        self,
        *,
        tables: Mapping[str, pa.Table],
        options: PropOptions,
    ) -> pa.Table:
        """Build property table from configured specs.

        Returns
        -------
        pa.Table
            Property table.
        """
        rows: list[PropRow] = []
        for spec in self._table_specs:
            table = self._table_for_spec(spec, tables=tables, options=options)
            if table is None:
                continue
            self._emit_props_for_table(
                rows,
                spec=spec,
                table=table,
                options=options,
            )
        return pa.Table.from_pylist(rows, schema=self._prop_schema)

    @staticmethod
    def _table_for_spec(
        spec: PropTableSpec,
        *,
        tables: Mapping[str, pa.Table],
        options: PropOptions,
    ) -> pa.Table | None:
        enabled = getattr(options, spec.option_flag, None)
        if enabled is None:
            msg = f"Unknown option flag: {spec.option_flag}"
            raise ValueError(msg)
        if not enabled:
            return None
        if spec.include_if is not None and not spec.include_if(options):
            return None
        table = spec.table_getter(tables)
        if table is None or table.num_rows == 0:
            return None
        return table

    def _emit_props_for_table(
        self,
        rows: list[PropRow],
        *,
        spec: PropTableSpec,
        table: pa.Table,
        options: PropOptions,
    ) -> None:
        cols = self._collect_columns(spec)
        arrays = [
            table[col] if col in table.column_names else pa.nulls(table.num_rows) for col in cols
        ]
        for values in iter_arrays(arrays):
            row = dict(zip(cols, values, strict=True))
            entity_id_val = _first_non_null(row, spec.id_cols)
            entity_id = _row_id(entity_id_val)
            if entity_id is None:
                continue
            self._emit_props_for_row(
                rows,
                spec=spec,
                row=row,
                entity_id=entity_id,
                options=options,
            )

    def _emit_props_for_row(
        self,
        rows: list[PropRow],
        *,
        spec: PropTableSpec,
        row: Mapping[str, object],
        entity_id: str,
        options: PropOptions,
    ) -> None:
        if spec.node_kind is not None:
            rows.append(
                _prop_row(
                    self._schema_version,
                    PropPayload(
                        entity_kind=spec.entity_kind,
                        entity_id=entity_id,
                        key="node_kind",
                        value=spec.node_kind.value,
                    ),
                )
            )
        for field in spec.fields:
            if field.include_if is not None and not field.include_if(options):
                continue
            value = field.value_from(row)
            if value is None and field.skip_if_none:
                continue
            rows.append(
                _prop_row(
                    self._schema_version,
                    PropPayload(
                        entity_kind=spec.entity_kind,
                        entity_id=entity_id,
                        key=field.prop_key,
                        value=value,
                    ),
                )
            )

    @staticmethod
    def _collect_columns(spec: PropTableSpec) -> list[str]:
        cols = list(spec.id_cols)
        for field in spec.fields:
            if field.source_col is not None and field.source_col not in cols:
                cols.append(field.source_col)
        return cols
