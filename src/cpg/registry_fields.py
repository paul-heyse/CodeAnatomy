"""Field catalog for programmatic CPG schema definitions."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa

from schema_spec.specs import DICT_STRING, ArrowFieldSpec

ENCODING_METADATA_KEY = "encoding"
ENCODING_DICTIONARY = "dictionary"


def _spec(
    name: str,
    dtype: pa.DataType,
    *,
    nullable: bool = True,
    metadata: Mapping[str, str] | None = None,
) -> ArrowFieldSpec:
    return ArrowFieldSpec(
        name=name,
        dtype=dtype,
        nullable=nullable,
        metadata=dict(metadata or {}),
    )


def _dict_spec(name: str, *, nullable: bool = True) -> ArrowFieldSpec:
    return _spec(
        name,
        DICT_STRING,
        nullable=nullable,
        metadata={ENCODING_METADATA_KEY: ENCODING_DICTIONARY},
    )


_FIELD_CATALOG: dict[str, ArrowFieldSpec] = {
    "node_id": _spec("node_id", pa.string(), nullable=False),
    "node_kind": _dict_spec("node_kind", nullable=False),
    "file_id": _spec("file_id", pa.string(), nullable=False),
    "edge_id": _spec("edge_id", pa.string(), nullable=False),
    "edge_kind": _dict_spec("edge_kind", nullable=False),
    "src_node_id": _spec("src_node_id", pa.string(), nullable=False),
    "dst_node_id": _spec("dst_node_id", pa.string(), nullable=False),
    "path": _spec("path", pa.string()),
    "edge_owner_file_id": _spec("edge_owner_file_id", pa.string()),
    "origin": _dict_spec("origin"),
    "resolution_method": _dict_spec("resolution_method"),
    "confidence": _spec("confidence", pa.float32()),
    "score": _spec("score", pa.float32()),
    "symbol_roles": _spec("symbol_roles", pa.int32()),
    "qname_source": _dict_spec("qname_source"),
    "ambiguity_group_id": _spec("ambiguity_group_id", pa.string()),
    "rule_name": _dict_spec("rule_name"),
    "rule_priority": _spec("rule_priority", pa.int32()),
    "entity_kind": _spec("entity_kind", pa.string(), nullable=False),
    "entity_id": _spec("entity_id", pa.string(), nullable=False),
    "prop_key": _spec("prop_key", pa.string(), nullable=False),
    "value_str": _spec("value_str", pa.string()),
    "value_int": _spec("value_int", pa.int64()),
    "value_float": _spec("value_float", pa.float64()),
    "value_bool": _spec("value_bool", pa.bool_()),
    "value_json": _spec("value_json", pa.string()),
}


def field(name: str) -> ArrowFieldSpec:
    """Return the ArrowFieldSpec for a catalog name.

    Returns
    -------
    ArrowFieldSpec
        Field specification for the requested name.
    """
    return _FIELD_CATALOG[name]


__all__ = ["field"]
