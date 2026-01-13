"""Generic CPG builder engines driven by specs."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.compute.kernels import ChunkPolicy
from arrowdsl.compute.predicates import FilterSpec, IsNull, Not
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import iter_arrays, prefixed_hash_id
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    SchemaLike,
    TableLike,
    pc,
)
from arrowdsl.plan.plan import PlanSpec
from arrowdsl.schema.arrays import const_array
from arrowdsl.schema.builders import (
    maybe_dictionary,
    pick_first,
    resolve_float_col,
    resolve_string_col,
    table_from_schema,
)
from arrowdsl.schema.tables import table_from_arrays
from cpg.catalog import PlanCatalog, PlanSource, resolve_plan_source
from cpg.kinds import EntityKind
from cpg.plan_helpers import ensure_plan
from cpg.specs import (
    EdgeEmitSpec,
    EdgePlanSpec,
    NodeEmitSpec,
    NodePlanSpec,
    PropOptions,
    PropTableSpec,
    resolve_edge_filter,
    resolve_preprocessor,
    resolve_prop_include,
)

type PropValue = object | None
type PropRow = dict[str, object]


def _materialize_source(source: PlanSource, *, ctx: ExecutionContext) -> TableLike:
    plan = ensure_plan(source, ctx=ctx)
    return PlanSpec.from_plan(plan).to_table(ctx=ctx)


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


@dataclass(frozen=True)
class EdgeIdArrays:
    """Column arrays required to compute edge IDs."""

    src: ArrayLike | ChunkedArrayLike
    dst: ArrayLike | ChunkedArrayLike
    path: ArrayLike | ChunkedArrayLike
    bstart: ArrayLike | ChunkedArrayLike
    bend: ArrayLike | ChunkedArrayLike


def _edge_id_array(
    *,
    edge_kind: str,
    inputs: EdgeIdArrays,
) -> ArrayLike | ChunkedArrayLike:
    n = len(inputs.src)
    kind_arr = const_array(n, edge_kind, dtype=pa.string())
    base_id = prefixed_hash_id([kind_arr, inputs.src, inputs.dst], prefix="edge")
    full_id = prefixed_hash_id(
        [kind_arr, inputs.src, inputs.dst, inputs.path, inputs.bstart, inputs.bend],
        prefix="edge",
    )
    has_span = pc.and_(
        pc.is_valid(inputs.path),
        pc.and_(pc.is_valid(inputs.bstart), pc.is_valid(inputs.bend)),
    )
    edge_id = pc.if_else(has_span, full_id, base_id)
    valid = pc.and_(pc.is_valid(inputs.src), pc.is_valid(inputs.dst))
    return pc.if_else(valid, edge_id, pa.scalar(None, type=pa.string()))


def _edge_columns_from_relation(
    rel: TableLike,
    *,
    spec: EdgeEmitSpec,
    schema_version: int,
    edge_schema: SchemaLike,
) -> tuple[int, dict[str, ArrayLike | ChunkedArrayLike]]:
    n = rel.num_rows
    src = pick_first(rel, spec.src_cols, default_type=pa.string())
    dst = pick_first(rel, spec.dst_cols, default_type=pa.string())
    path = pick_first(rel, spec.path_cols, default_type=pa.string())
    bstart = pick_first(rel, spec.bstart_cols, default_type=pa.int64())
    bend = pick_first(rel, spec.bend_cols, default_type=pa.int64())
    edge_ids = _edge_id_array(
        edge_kind=spec.edge_kind.value,
        inputs=EdgeIdArrays(src=src, dst=dst, path=path, bstart=bstart, bend=bend),
    )

    default_score = 1.0 if spec.origin == "scip" else 0.5
    origin = resolve_string_col(rel, "origin", default_value=spec.origin)
    origin = maybe_dictionary(origin, edge_schema.field("origin").type)

    edge_kind = const_array(n, spec.edge_kind.value, dtype=pa.string())
    edge_kind = maybe_dictionary(edge_kind, edge_schema.field("edge_kind").type)
    resolution = resolve_string_col(
        rel,
        "resolution_method",
        default_value=spec.default_resolution_method,
    )
    resolution = maybe_dictionary(resolution, edge_schema.field("resolution_method").type)
    qname_source = pick_first(rel, ["qname_source"], default_type=pa.string())
    qname_source = maybe_dictionary(qname_source, edge_schema.field("qname_source").type)
    rule_name = pick_first(rel, ["rule_name"], default_type=pa.string())
    rule_name = maybe_dictionary(rule_name, edge_schema.field("rule_name").type)

    columns: dict[str, ArrayLike | ChunkedArrayLike] = {
        "edge_id": edge_ids,
        "edge_kind": edge_kind,
        "src_node_id": src,
        "dst_node_id": dst,
        "path": path,
        "bstart": bstart,
        "bend": bend,
        "origin": origin,
        "resolution_method": resolution,
        "confidence": resolve_float_col(rel, "confidence", default_value=default_score),
        "score": resolve_float_col(rel, "score", default_value=default_score),
        "symbol_roles": pick_first(rel, ["symbol_roles"], default_type=pa.int32()),
        "qname_source": qname_source,
        "ambiguity_group_id": pick_first(rel, ["ambiguity_group_id"], default_type=pa.string()),
        "rule_name": rule_name,
        "rule_priority": pick_first(rel, ["rule_priority"], default_type=pa.int32()),
    }
    if "schema_version" in edge_schema.names:
        columns["schema_version"] = const_array(n, schema_version, dtype=pa.int32())
    return n, columns


def emit_edges_from_relation(
    rel: TableLike,
    *,
    spec: EdgeEmitSpec,
    schema_version: int,
    edge_schema: SchemaLike,
) -> TableLike:
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
    TableLike
        Emitted edge table.
    """
    if rel.num_rows == 0:
        return table_from_arrays(edge_schema, columns={}, num_rows=0)

    n, columns = _edge_columns_from_relation(
        rel,
        spec=spec,
        schema_version=schema_version,
        edge_schema=edge_schema,
    )
    out = table_from_schema(edge_schema, columns=columns, num_rows=n)
    predicate = Not(IsNull("edge_id"))
    filtered = FilterSpec(predicate).apply_kernel(out)
    return ChunkPolicy().apply(filtered)


class EdgeBuilder:
    """Build edges from declarative specs."""

    def __init__(
        self,
        *,
        emitters: Sequence[EdgePlanSpec],
        schema_version: int,
        edge_schema: SchemaLike,
    ) -> None:
        self._emitters = tuple(emitters)
        self._schema_version = schema_version
        self._edge_schema = edge_schema

    def build(
        self,
        *,
        tables: Mapping[str, PlanSource],
        options: object,
        ctx: ExecutionContext,
    ) -> list[TableLike]:
        """Build edge tables from configured emitters.

        Returns
        -------
        list[TableLike]
            Edge table parts ready for concatenation.

        Raises
        ------
        ValueError
            If the configured option flag is missing on the options object.
        """
        catalog = PlanCatalog()
        catalog.extend(tables)
        parts: list[TableLike] = []
        for emitter in self._emitters:
            enabled = getattr(options, emitter.option_flag, None)
            if enabled is None:
                msg = f"Unknown option flag: {emitter.option_flag}"
                raise ValueError(msg)
            if not enabled:
                continue
            source = resolve_plan_source(catalog, emitter.relation_ref, ctx=ctx)
            if source is None:
                continue
            plan = ensure_plan(source, label=emitter.name, ctx=ctx)
            rel = _materialize_source(plan, ctx=ctx)
            if rel.num_rows == 0:
                continue
            filter_fn = resolve_edge_filter(emitter.filter_id)
            if filter_fn is not None:
                rel = filter_fn(rel)
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
    table: TableLike,
    *,
    spec: NodeEmitSpec,
    schema_version: int,
    node_schema: SchemaLike,
) -> TableLike:
    """Emit node rows from a table.

    Returns
    -------
    TableLike
        Node table conforming to the configured schema.
    """
    if table.num_rows == 0:
        return table_from_arrays(node_schema, columns={}, num_rows=0)

    n = table.num_rows
    node_id = pick_first(table, spec.id_cols, default_type=pa.string())
    path = pick_first(table, spec.path_cols, default_type=pa.string())
    bstart = pick_first(table, spec.bstart_cols, default_type=pa.int64())
    bend = pick_first(table, spec.bend_cols, default_type=pa.int64())
    file_id = pick_first(table, spec.file_id_cols, default_type=pa.string())

    node_kind = const_array(n, spec.node_kind.value, dtype=pa.string())
    node_kind = maybe_dictionary(node_kind, node_schema.field("node_kind").type)

    columns: dict[str, ArrayLike | ChunkedArrayLike] = {
        "node_id": node_id,
        "node_kind": node_kind,
        "path": path,
        "bstart": bstart,
        "bend": bend,
        "file_id": file_id,
    }
    if "schema_version" in node_schema.names:
        columns["schema_version"] = const_array(n, schema_version, dtype=pa.int32())
    out = table_from_schema(node_schema, columns=columns, num_rows=n)
    return ChunkPolicy().apply(out)


class NodeBuilder:
    """Build nodes from declarative specs."""

    def __init__(
        self,
        *,
        emitters: Sequence[NodePlanSpec],
        schema_version: int,
        node_schema: SchemaLike,
    ) -> None:
        self._emitters = tuple(emitters)
        self._schema_version = schema_version
        self._node_schema = node_schema

    def build(
        self,
        *,
        tables: Mapping[str, PlanSource],
        options: object,
        ctx: ExecutionContext,
    ) -> list[TableLike]:
        """Build node tables from configured emitters.

        Returns
        -------
        list[TableLike]
            Node table parts ready for concatenation.

        Raises
        ------
        ValueError
            If the configured option flag is missing on the options object.
        """
        catalog = PlanCatalog()
        catalog.extend(tables)
        parts: list[TableLike] = []
        for emitter in self._emitters:
            enabled = getattr(options, emitter.option_flag, None)
            if enabled is None:
                msg = f"Unknown option flag: {emitter.option_flag}"
                raise ValueError(msg)
            if not enabled:
                continue
            source = resolve_plan_source(catalog, emitter.table_ref, ctx=ctx)
            if source is None:
                continue
            plan = ensure_plan(source, label=emitter.name, ctx=ctx)
            preprocessor = resolve_preprocessor(emitter.preprocessor_id)
            if preprocessor is not None:
                plan = preprocessor(plan)
            table = _materialize_source(plan, ctx=ctx)
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


def _prop_row(
    *,
    schema_version: int,
    include_schema_version: bool,
    payload: PropPayload,
) -> PropRow:
    rec: PropRow = {
        "entity_kind": payload.entity_kind.value,
        "entity_id": str(payload.entity_id),
        "prop_key": str(payload.key),
        "value_str": None,
        "value_int": None,
        "value_float": None,
        "value_bool": None,
        "value_json": None,
    }

    if include_schema_version:
        rec["schema_version"] = schema_version

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
        prop_schema: SchemaLike,
    ) -> None:
        self._table_specs = tuple(table_specs)
        self._schema_version = schema_version
        self._prop_schema = prop_schema
        self._include_schema_version = "schema_version" in prop_schema.names

    def build(
        self,
        *,
        tables: Mapping[str, PlanSource],
        options: PropOptions,
        ctx: ExecutionContext,
    ) -> TableLike:
        """Build property table from configured specs.

        Returns
        -------
        TableLike
            Property table.
        """
        catalog = PlanCatalog()
        catalog.extend(tables)
        rows: list[PropRow] = []
        for spec in self._table_specs:
            table = self._table_for_spec(spec, catalog=catalog, options=options, ctx=ctx)
            if table is None:
                continue
            self._emit_props_for_table(
                rows,
                spec=spec,
                table=table,
                options=options,
            )
        out = pa.Table.from_pylist(rows, schema=self._prop_schema)
        return ChunkPolicy().apply(out)

    @staticmethod
    def _table_for_spec(
        spec: PropTableSpec,
        *,
        catalog: PlanCatalog,
        options: PropOptions,
        ctx: ExecutionContext,
    ) -> TableLike | None:
        enabled = getattr(options, spec.option_flag, None)
        if enabled is None:
            msg = f"Unknown option flag: {spec.option_flag}"
            raise ValueError(msg)
        if not enabled:
            return None
        include_if = resolve_prop_include(spec.include_if_id)
        if include_if is not None and not include_if(options):
            return None
        source = resolve_plan_source(catalog, spec.table_ref, ctx=ctx)
        if source is None:
            return None
        table = _materialize_source(source, ctx=ctx)
        if table.num_rows == 0:
            return None
        return table

    def _emit_props_for_table(
        self,
        rows: list[PropRow],
        *,
        spec: PropTableSpec,
        table: TableLike,
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
                    schema_version=self._schema_version,
                    include_schema_version=self._include_schema_version,
                    payload=PropPayload(
                        entity_kind=spec.entity_kind,
                        entity_id=entity_id,
                        key="node_kind",
                        value=spec.node_kind.value,
                    ),
                )
            )
        for field in spec.fields:
            include_if = resolve_prop_include(field.include_if_id)
            if include_if is not None and not include_if(options):
                continue
            value = field.value_from(row)
            if value is None and field.skip_if_none:
                continue
            rows.append(
                _prop_row(
                    schema_version=self._schema_version,
                    include_schema_version=self._include_schema_version,
                    payload=PropPayload(
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
