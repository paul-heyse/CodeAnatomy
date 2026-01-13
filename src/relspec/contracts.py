"""Canonical relationship output schema and helpers."""

from __future__ import annotations

from functools import cache

import pyarrow as pa

from arrowdsl.core.interop import SchemaLike
from schema_spec.system import (
    GLOBAL_SCHEMA_REGISTRY,
    Contract,
    DatasetSpec,
    dataset_spec_from_schema,
    register_dataset_spec,
)

RELATION_OUTPUT_NAME = "relation_output_v1"

RELATION_OUTPUT_SCHEMA = pa.schema(
    [
        pa.field("src", pa.string(), nullable=True),
        pa.field("dst", pa.string(), nullable=True),
        pa.field("path", pa.string(), nullable=True),
        pa.field("bstart", pa.int64(), nullable=True),
        pa.field("bend", pa.int64(), nullable=True),
        pa.field("origin", pa.string(), nullable=True),
        pa.field("resolution_method", pa.string(), nullable=True),
        pa.field("confidence", pa.float32(), nullable=True),
        pa.field("score", pa.float32(), nullable=True),
        pa.field("symbol_roles", pa.int32(), nullable=True),
        pa.field("qname_source", pa.string(), nullable=True),
        pa.field("ambiguity_group_id", pa.string(), nullable=True),
        pa.field("rule_name", pa.string(), nullable=True),
        pa.field("rule_priority", pa.int32(), nullable=True),
    ],
    metadata={b"spec_kind": b"relation_output"},
)


@cache
def relation_output_spec() -> DatasetSpec:
    """Return the DatasetSpec for canonical relationship outputs.

    Returns
    -------
    DatasetSpec
        Dataset specification for relationship outputs.
    """
    existing = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(RELATION_OUTPUT_NAME)
    if existing is not None:
        return existing
    spec = dataset_spec_from_schema(RELATION_OUTPUT_NAME, RELATION_OUTPUT_SCHEMA, version=1)
    return register_dataset_spec(spec)


def relation_output_schema() -> SchemaLike:
    """Return the Arrow schema for canonical relationship outputs.

    Returns
    -------
    SchemaLike
        Arrow schema for relationship outputs.
    """
    return relation_output_spec().schema()


def relation_output_contract() -> Contract:
    """Return the Contract for canonical relationship outputs.

    Returns
    -------
    Contract
        Runtime contract for relationship outputs.
    """
    return relation_output_spec().contract()


__all__ = [
    "RELATION_OUTPUT_NAME",
    "RELATION_OUTPUT_SCHEMA",
    "relation_output_contract",
    "relation_output_schema",
    "relation_output_spec",
]
