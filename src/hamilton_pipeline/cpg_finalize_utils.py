"""Shared helpers for finalizing CPG outputs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pyarrow as pa

from datafusion_engine.arrow_interop import TableLike as ArrowTableLike
from datafusion_engine.arrow_schema.build import empty_table
from datafusion_engine.arrow_schema.semantic_types import (
    apply_semantic_types,
    edge_id_metadata,
    node_id_metadata,
    span_id_metadata,
)
from datafusion_engine.finalize import Contract, FinalizeOptions, normalize_only
from engine.runtime_profile import RuntimeProfileSpec
from relspec.runtime_artifacts import TableLike


def finalize_cpg_table(
    table: TableLike,
    *,
    name: str,
    runtime_profile_spec: RuntimeProfileSpec,
) -> TableLike:
    """Finalize a CPG table with schema normalization and semantic metadata.

    Returns
    -------
    TableLike
        Finalized table with semantic metadata attached.
    """
    schema_names = getattr(table.schema, "names", [])
    if not schema_names:
        return empty_table(table.schema)
    contract = Contract(name=name, schema=table.schema)
    normalized = normalize_only(
        cast("ArrowTableLike", table),
        contract=contract,
        options=FinalizeOptions(
            runtime_profile=runtime_profile_spec.datafusion,
            determinism_tier=runtime_profile_spec.determinism_tier,
        ),
    )
    field_metadata = _semantic_field_metadata_for_cpg(name)
    updated = _apply_field_metadata(normalized, field_metadata=field_metadata)
    return _apply_semantic_schema(updated)


def _semantic_field_metadata_for_cpg(name: str) -> dict[str, dict[bytes, bytes]]:
    if name == "cpg_nodes_v1":
        return {"node_id": node_id_metadata()}
    if name == "cpg_edges_v1":
        return {
            "edge_id": edge_id_metadata(),
            "src_node_id": node_id_metadata(),
            "dst_node_id": node_id_metadata(),
            "span_id": span_id_metadata(),
        }
    return {}


def _apply_field_metadata(
    table: TableLike,
    *,
    field_metadata: Mapping[str, Mapping[bytes, bytes]],
) -> TableLike:
    if not field_metadata:
        return table
    if not isinstance(table, pa.Table):
        return table
    table_value = cast("pa.Table", table)
    schema = table_value.schema
    fields: list[pa.Field] = []
    for field in schema:
        metadata = dict(field.metadata or {})
        extra = field_metadata.get(field.name)
        if extra:
            metadata.update(extra)
        fields.append(
            pa.field(
                field.name,
                field.type,
                nullable=field.nullable,
                metadata=metadata or None,
            )
        )
    updated_schema = pa.schema(fields, metadata=schema.metadata)
    try:
        return table_value.cast(updated_schema, safe=False)
    except (pa.ArrowInvalid, TypeError, ValueError):
        return table_value


def _apply_semantic_schema(table: TableLike) -> TableLike:
    if not isinstance(table, pa.Table):
        return table
    table_value = cast("pa.Table", table)
    schema = apply_semantic_types(table_value.schema)
    try:
        return table_value.cast(schema, safe=False)
    except (pa.ArrowInvalid, TypeError, ValueError):
        return table_value


__all__ = ["finalize_cpg_table"]
