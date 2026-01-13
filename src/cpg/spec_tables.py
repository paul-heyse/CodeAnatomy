"""Arrow spec tables for CPG specs."""

from __future__ import annotations

import base64
import json
from collections.abc import Sequence
from typing import Any

import pyarrow as pa

from arrowdsl.core.interop import ScalarLike
from cpg.kinds import EdgeKind, EntityKind, NodeKind
from cpg.specs import (
    EdgeEmitSpec,
    EdgePlanSpec,
    NodeEmitSpec,
    NodePlanSpec,
    PropFieldSpec,
    PropTableSpec,
)

NODE_EMIT_STRUCT = pa.struct(
    [
        pa.field("node_kind", pa.string(), nullable=False),
        pa.field("id_cols", pa.list_(pa.string()), nullable=False),
        pa.field("path_cols", pa.list_(pa.string()), nullable=False),
        pa.field("bstart_cols", pa.list_(pa.string()), nullable=False),
        pa.field("bend_cols", pa.list_(pa.string()), nullable=False),
        pa.field("file_id_cols", pa.list_(pa.string()), nullable=False),
    ]
)

EDGE_EMIT_STRUCT = pa.struct(
    [
        pa.field("edge_kind", pa.string(), nullable=False),
        pa.field("src_cols", pa.list_(pa.string()), nullable=False),
        pa.field("dst_cols", pa.list_(pa.string()), nullable=False),
        pa.field("path_cols", pa.list_(pa.string()), nullable=False),
        pa.field("bstart_cols", pa.list_(pa.string()), nullable=False),
        pa.field("bend_cols", pa.list_(pa.string()), nullable=False),
        pa.field("origin", pa.string(), nullable=False),
        pa.field("default_resolution_method", pa.string(), nullable=False),
    ]
)

PROP_FIELD_STRUCT = pa.struct(
    [
        pa.field("prop_key", pa.string(), nullable=False),
        pa.field("source_col", pa.string(), nullable=True),
        pa.field("literal_json", pa.string(), nullable=True),
        pa.field("transform_id", pa.string(), nullable=True),
        pa.field("include_if_id", pa.string(), nullable=True),
        pa.field("skip_if_none", pa.bool_(), nullable=False),
        pa.field("value_type", pa.string(), nullable=True),
    ]
)

NODE_PLAN_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("option_flag", pa.string(), nullable=False),
        pa.field("table_ref", pa.string(), nullable=False),
        pa.field("emit", NODE_EMIT_STRUCT, nullable=False),
        pa.field("preprocessor_id", pa.string(), nullable=True),
    ],
    metadata={b"spec_kind": b"cpg_node_specs"},
)

EDGE_PLAN_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("option_flag", pa.string(), nullable=False),
        pa.field("relation_ref", pa.string(), nullable=False),
        pa.field("emit", EDGE_EMIT_STRUCT, nullable=False),
        pa.field("filter_id", pa.string(), nullable=True),
    ],
    metadata={b"spec_kind": b"cpg_edge_specs"},
)

PROP_TABLE_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("option_flag", pa.string(), nullable=False),
        pa.field("table_ref", pa.string(), nullable=False),
        pa.field("entity_kind", pa.string(), nullable=False),
        pa.field("id_cols", pa.list_(pa.string()), nullable=False),
        pa.field("node_kind", pa.string(), nullable=True),
        pa.field("fields", pa.list_(PROP_FIELD_STRUCT), nullable=True),
        pa.field("include_if_id", pa.string(), nullable=True),
    ],
    metadata={b"spec_kind": b"cpg_prop_specs"},
)


def _encode_literal(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, ScalarLike):
        value = value.as_py()
    if isinstance(value, bytes):
        payload = {"type": "bytes", "value": base64.b64encode(value).decode("ascii")}
        return json.dumps(payload, ensure_ascii=True)
    payload = {"type": "json", "value": value}
    return json.dumps(payload, ensure_ascii=True)


def _decode_literal(payload: str | None) -> object | None:
    if payload is None:
        return None
    data = json.loads(payload)
    if isinstance(data, dict) and data.get("type") == "bytes":
        value = data.get("value", "")
        if not isinstance(value, str):
            msg = "Encoded bytes literal must contain a base64 string."
            raise ValueError(msg)
        return base64.b64decode(value.encode("ascii"))
    if isinstance(data, dict) and "value" in data:
        return data["value"]
    return data


def _node_emit_row(spec: NodeEmitSpec) -> dict[str, object]:
    return {
        "node_kind": spec.node_kind.value,
        "id_cols": list(spec.id_cols),
        "path_cols": list(spec.path_cols),
        "bstart_cols": list(spec.bstart_cols),
        "bend_cols": list(spec.bend_cols),
        "file_id_cols": list(spec.file_id_cols),
    }


def _edge_emit_row(spec: EdgeEmitSpec) -> dict[str, object]:
    return {
        "edge_kind": spec.edge_kind.value,
        "src_cols": list(spec.src_cols),
        "dst_cols": list(spec.dst_cols),
        "path_cols": list(spec.path_cols),
        "bstart_cols": list(spec.bstart_cols),
        "bend_cols": list(spec.bend_cols),
        "origin": spec.origin,
        "default_resolution_method": spec.default_resolution_method,
    }


def _prop_field_row(spec: PropFieldSpec) -> dict[str, object]:
    return {
        "prop_key": spec.prop_key,
        "source_col": spec.source_col,
        "literal_json": _encode_literal(spec.literal),
        "transform_id": spec.transform_id,
        "include_if_id": spec.include_if_id,
        "skip_if_none": spec.skip_if_none,
        "value_type": spec.value_type,
    }


def node_plan_table(specs: Sequence[NodePlanSpec]) -> pa.Table:
    """Build a node plan spec table.

    Returns
    -------
    pa.Table
        Arrow table of node plan specs.
    """
    rows = [
        {
            "name": spec.name,
            "option_flag": spec.option_flag,
            "table_ref": spec.table_ref,
            "emit": _node_emit_row(spec.emit),
            "preprocessor_id": spec.preprocessor_id,
        }
        for spec in specs
    ]
    return pa.Table.from_pylist(rows, schema=NODE_PLAN_SCHEMA)


def edge_plan_table(specs: Sequence[EdgePlanSpec]) -> pa.Table:
    """Build an edge plan spec table.

    Returns
    -------
    pa.Table
        Arrow table of edge plan specs.
    """
    rows = [
        {
            "name": spec.name,
            "option_flag": spec.option_flag,
            "relation_ref": spec.relation_ref,
            "emit": _edge_emit_row(spec.emit),
            "filter_id": spec.filter_id,
        }
        for spec in specs
    ]
    return pa.Table.from_pylist(rows, schema=EDGE_PLAN_SCHEMA)


def prop_table_table(specs: Sequence[PropTableSpec]) -> pa.Table:
    """Build a property table spec table.

    Returns
    -------
    pa.Table
        Arrow table of property table specs.
    """
    rows = [
        {
            "name": spec.name,
            "option_flag": spec.option_flag,
            "table_ref": spec.table_ref,
            "entity_kind": spec.entity_kind.value,
            "id_cols": list(spec.id_cols),
            "node_kind": spec.node_kind.value if spec.node_kind is not None else None,
            "fields": [_prop_field_row(field) for field in spec.fields] or None,
            "include_if_id": spec.include_if_id,
        }
        for spec in specs
    ]
    return pa.Table.from_pylist(rows, schema=PROP_TABLE_SCHEMA)


def _node_emit_from_row(payload: dict[str, Any]) -> NodeEmitSpec:
    return NodeEmitSpec(
        node_kind=NodeKind(str(payload["node_kind"])),
        id_cols=tuple(payload.get("id_cols") or ()),
        path_cols=tuple(payload.get("path_cols") or ()),
        bstart_cols=tuple(payload.get("bstart_cols") or ()),
        bend_cols=tuple(payload.get("bend_cols") or ()),
        file_id_cols=tuple(payload.get("file_id_cols") or ()),
    )


def _edge_emit_from_row(payload: dict[str, Any]) -> EdgeEmitSpec:
    return EdgeEmitSpec(
        edge_kind=EdgeKind(str(payload["edge_kind"])),
        src_cols=tuple(payload.get("src_cols") or ()),
        dst_cols=tuple(payload.get("dst_cols") or ()),
        path_cols=tuple(payload.get("path_cols") or ()),
        bstart_cols=tuple(payload.get("bstart_cols") or ()),
        bend_cols=tuple(payload.get("bend_cols") or ()),
        origin=str(payload.get("origin", "")),
        default_resolution_method=str(payload.get("default_resolution_method", "")),
    )


def _prop_field_from_row(payload: dict[str, Any]) -> PropFieldSpec:
    return PropFieldSpec(
        prop_key=str(payload["prop_key"]),
        source_col=payload.get("source_col"),
        literal=_decode_literal(payload.get("literal_json")),
        transform_id=payload.get("transform_id"),
        include_if_id=payload.get("include_if_id"),
        skip_if_none=bool(payload.get("skip_if_none")),
        value_type=payload.get("value_type"),
    )


def node_plan_specs_from_table(table: pa.Table) -> tuple[NodePlanSpec, ...]:
    """Compile NodePlanSpec entries from a spec table.

    Returns
    -------
    tuple[NodePlanSpec, ...]
        Node plan specs parsed from the table.
    """
    specs = [
        NodePlanSpec(
            name=str(row["name"]),
            option_flag=str(row["option_flag"]),
            table_ref=str(row["table_ref"]),
            emit=_node_emit_from_row(row["emit"]),
            preprocessor_id=row.get("preprocessor_id"),
        )
        for row in table.to_pylist()
    ]
    return tuple(specs)


def edge_plan_specs_from_table(table: pa.Table) -> tuple[EdgePlanSpec, ...]:
    """Compile EdgePlanSpec entries from a spec table.

    Returns
    -------
    tuple[EdgePlanSpec, ...]
        Edge plan specs parsed from the table.
    """
    specs = [
        EdgePlanSpec(
            name=str(row["name"]),
            option_flag=str(row["option_flag"]),
            relation_ref=str(row["relation_ref"]),
            emit=_edge_emit_from_row(row["emit"]),
            filter_id=row.get("filter_id"),
        )
        for row in table.to_pylist()
    ]
    return tuple(specs)


def prop_table_specs_from_table(table: pa.Table) -> tuple[PropTableSpec, ...]:
    """Compile PropTableSpec entries from a spec table.

    Returns
    -------
    tuple[PropTableSpec, ...]
        Property table specs parsed from the table.
    """
    specs: list[PropTableSpec] = []
    for row in table.to_pylist():
        fields = tuple(_prop_field_from_row(item) for item in row.get("fields") or ())
        node_kind = row.get("node_kind")
        specs.append(
            PropTableSpec(
                name=str(row["name"]),
                option_flag=str(row["option_flag"]),
                table_ref=str(row["table_ref"]),
                entity_kind=EntityKind(str(row["entity_kind"])),
                id_cols=tuple(row.get("id_cols") or ()),
                node_kind=NodeKind(str(node_kind)) if node_kind is not None else None,
                fields=fields,
                include_if_id=row.get("include_if_id"),
            )
        )
    return tuple(specs)


__all__ = [
    "EDGE_EMIT_STRUCT",
    "EDGE_PLAN_SCHEMA",
    "NODE_EMIT_STRUCT",
    "NODE_PLAN_SCHEMA",
    "PROP_FIELD_STRUCT",
    "PROP_TABLE_SCHEMA",
    "edge_plan_specs_from_table",
    "edge_plan_table",
    "node_plan_specs_from_table",
    "node_plan_table",
    "prop_table_specs_from_table",
    "prop_table_table",
]
