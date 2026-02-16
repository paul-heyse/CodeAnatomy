"""Spec builders for semantic dataset rows.

This module provides functions to build DatasetSpec and input schema
from SemanticDatasetRow definitions, following the normalize layer
dataset_builders pattern.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import pyarrow as pa

from arrow_utils.core.ordering import OrderingLevel
from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.arrow.metadata import (
    SchemaMetadataSpec,
    merge_metadata_specs,
    ordering_metadata_spec,
)
from datafusion_engine.arrow.semantic import SPAN_STORAGE
from schema_spec.arrow_types import arrow_type_from_pyarrow
from schema_spec.dataset_spec import (
    DatasetSpec,
    DeltaCdfPolicy,
    DeltaMaintenancePolicy,
    TableSpecConstraints,
    ValidationPolicySpec,
    make_contract_spec,
    make_dataset_spec,
    make_table_spec,
)
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import (
    FieldBundle,
    file_identity_bundle,
    span_bundle,
)
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy

if TYPE_CHECKING:
    from schema_spec.specs import TableSchemaSpec
    from semantics.catalog.dataset_rows import SemanticDatasetRow


def partition_spec_from_row(
    row: SemanticDatasetRow,
    table_spec: TableSchemaSpec,
) -> tuple[tuple[str, pa.DataType], ...]:
    """Convert partition column names to (name, dtype) pairs.

    Parameters
    ----------
    row
        Semantic dataset row definition.
    table_spec
        Table schema specification for type lookup.

    Returns:
    -------
    tuple[tuple[str, pa.DataType], ...]
        Partition column specifications as (name, dtype) pairs.
    """
    if not row.partition_cols:
        return ()
    schema = table_spec.to_arrow_schema()
    field_types: dict[str, pa.DataType] = {field.name: field.type for field in schema}
    return tuple((col, field_types.get(col, pa.string())) for col in row.partition_cols)


def write_policy_from_row(row: SemanticDatasetRow) -> DeltaWritePolicy:
    """Build write policy including merge keys and partitions.

    Uses merge_keys for z-ordering when available, falling back to join_keys.
    Partition columns from the row are applied to the write policy.

    Parameters
    ----------
    row
        Semantic dataset row definition.

    Returns:
    -------
    DeltaWritePolicy
        Write policy configured from row metadata.
    """
    zorder_cols = row.merge_keys if row.merge_keys else tuple(row.join_keys)
    bloom_columns = tuple(field_name for field_name in row.fields if field_name.endswith("_id"))
    stats_columns = tuple(dict.fromkeys((*row.join_keys, *bloom_columns))) or None
    return DeltaWritePolicy(
        target_file_size=128 * 1024 * 1024,
        partition_by=row.partition_cols,
        zorder_by=zorder_cols,
        stats_policy="explicit" if stats_columns else "auto",
        stats_columns=stats_columns,
        enable_features=("change_data_feed", "column_mapping", "v2_checkpoints"),
    )


_SEMANTIC_BUNDLE_CATALOG: dict[str, FieldBundle] = {
    "file_identity": file_identity_bundle(include_sha256=False),
    "span": FieldBundle(
        name="span",
        fields=(FieldSpec(name="span", dtype=arrow_type_from_pyarrow(SPAN_STORAGE)),),
    ),
}


def _bundle(name: str) -> FieldBundle:
    """Return a semantic field bundle by name.

    Args:
        name: Description.

    Raises:
        KeyError: If the operation cannot be completed.
    """
    if name not in _SEMANTIC_BUNDLE_CATALOG:
        if name == "file_identity":
            return file_identity_bundle(include_sha256=False)
        if name == "span":
            return span_bundle()
        msg = f"Unknown semantic bundle: {name!r}"
        raise KeyError(msg)
    return _SEMANTIC_BUNDLE_CATALOG[name]


def _bundles_for_row(row: SemanticDatasetRow) -> tuple[FieldBundle, ...]:
    """Return field bundles for a semantic dataset row.

    Returns:
    -------
    tuple[FieldBundle, ...]
        Bundles referenced by the row.
    """
    return tuple(_bundle(name) for name in row.bundles)


def _edge_field_specs(row: SemanticDatasetRow) -> list[FieldSpec] | None:
    if row.name not in {"cpg_edges_by_src", "cpg_edges_by_dst"}:
        return None
    edge_peer = "dst_node_id" if row.name == "cpg_edges_by_src" else "src_node_id"
    edge_struct = pa.struct(
        [
            pa.field("edge_id", pa.string()),
            pa.field("edge_kind", pa.string()),
            pa.field(edge_peer, pa.string()),
            pa.field("path", pa.string()),
            pa.field("bstart", pa.int64()),
            pa.field("bend", pa.int64()),
            pa.field("origin", pa.string()),
            pa.field("resolution_method", pa.string()),
            pa.field("confidence", pa.float64()),
            pa.field("score", pa.float64()),
            pa.field("symbol_roles", pa.int32()),
            pa.field("qname_source", pa.string()),
            pa.field("ambiguity_group_id", pa.string()),
        ]
    )
    return [
        FieldSpec(name=row.fields[0], dtype=arrow_type_from_pyarrow(pa.string())),
        FieldSpec(name="edges", dtype=arrow_type_from_pyarrow(pa.list_(edge_struct))),
    ]


def _props_map_field_specs(row: SemanticDatasetRow) -> list[FieldSpec] | None:
    if row.name != "cpg_props_map":
        return None
    value_struct = pa.struct(
        [
            pa.field("value_type", pa.string()),
            pa.field("value_string", pa.string()),
            pa.field("value_int", pa.int64()),
            pa.field("value_float", pa.float64()),
            pa.field("value_bool", pa.bool_()),
            pa.field("value_json", pa.string()),
        ]
    )
    prop_struct = pa.struct(
        [
            pa.field("prop_key", pa.string()),
            pa.field("value", value_struct),
        ]
    )
    return [
        FieldSpec(name="entity_kind", dtype=arrow_type_from_pyarrow(pa.string())),
        FieldSpec(name="entity_id", dtype=arrow_type_from_pyarrow(pa.string())),
        FieldSpec(name="node_kind", dtype=arrow_type_from_pyarrow(pa.string())),
        FieldSpec(name="props", dtype=arrow_type_from_pyarrow(pa.list_(prop_struct))),
    ]


def _scip_field_specs(row: SemanticDatasetRow) -> list[FieldSpec] | None:
    if row.template != "semantic_scip":
        return None
    type_map = {
        "symbol": pa.string(),
        "symbol_roles": pa.int32(),
        "bstart": pa.int64(),
        "bend": pa.int64(),
        "is_definition": pa.bool_(),
        "is_read": pa.bool_(),
        "is_import": pa.bool_(),
        "is_write": pa.bool_(),
        "is_generated": pa.bool_(),
        "is_test": pa.bool_(),
        "is_forward_definition": pa.bool_(),
    }
    return [
        FieldSpec(
            name=field_name,
            dtype=arrow_type_from_pyarrow(type_map.get(field_name, pa.string())),
        )
        for field_name in row.fields
    ]


def _relationship_field_specs(row: SemanticDatasetRow) -> list[FieldSpec] | None:
    if row.template != "semantic_relationship":
        return None
    type_map = {
        "entity_id": pa.string(),
        "symbol": pa.string(),
        "path": pa.string(),
        "edge_owner_file_id": pa.string(),
        "bstart": pa.int64(),
        "bend": pa.int64(),
        "origin": pa.string(),
        "confidence": pa.float64(),
        "score": pa.float64(),
        "provider": pa.string(),
        "rule_name": pa.string(),
        "ambiguity_group_id": pa.string(),
    }
    specs: list[FieldSpec] = []
    for field_name in row.fields:
        if field_name.startswith("hard_"):
            dtype = pa.bool_()
        elif field_name.startswith("feat_"):
            dtype = pa.float64()
        else:
            dtype = type_map.get(field_name, pa.string())
        specs.append(FieldSpec(name=field_name, dtype=arrow_type_from_pyarrow(dtype)))
    return specs


def _semantic_normalize_field_specs(row: SemanticDatasetRow) -> list[FieldSpec] | None:
    if row.template != "semantic_normalize":
        return None
    from datafusion_engine.extract.registry import dataset_schema

    schema = dataset_schema(row.source_dataset or row.name)
    field_types: dict[str, pa.DataType] = {field.name: field.type for field in schema}
    derived_types = {
        "bstart": pa.int64(),
        "bend": pa.int64(),
        "span": SPAN_STORAGE,
    }
    return [
        FieldSpec(
            name=field_name,
            dtype=arrow_type_from_pyarrow(
                field_types.get(field_name, derived_types.get(field_name, pa.string()))
            ),
        )
        for field_name in row.fields
    ]


def _input_field_specs(row: SemanticDatasetRow) -> list[FieldSpec] | None:
    if row.role != "input":
        return None
    from datafusion_engine.extract.registry import dataset_schema

    try:
        schema = dataset_schema(row.source_dataset or row.name)
    except KeyError:
        schema = None
    if schema is None:
        return None
    return [
        FieldSpec(
            name=field.name,
            dtype=arrow_type_from_pyarrow(field.type),
            nullable=field.nullable,
        )
        for field in schema
    ]


def _field_specs_for_row(row: SemanticDatasetRow) -> list[FieldSpec]:
    """Return field specs for a semantic dataset row.

    Returns:
    -------
    list[FieldSpec]
        Field specifications for the row schema.
    """
    for builder in (
        _edge_field_specs,
        _props_map_field_specs,
        _scip_field_specs,
        _relationship_field_specs,
        _semantic_normalize_field_specs,
        _input_field_specs,
    ):
        specs = builder(row)
        if specs is not None:
            return specs
    return [
        FieldSpec(name=field_name, dtype=arrow_type_from_pyarrow(pa.string()))
        for field_name in row.fields
    ]


def _semantic_metadata_spec(
    dataset_name: str,
    *,
    stage: str = "semantic",
    extra: Mapping[bytes, bytes] | None = None,
) -> SchemaMetadataSpec:
    meta: dict[bytes, bytes] = {
        b"semantic_stage": stage.encode("utf-8"),
        b"semantic_dataset": dataset_name.encode("utf-8"),
    }
    if extra:
        meta.update(extra)
    return SchemaMetadataSpec(schema_metadata=meta)


def _build_metadata_spec(row: SemanticDatasetRow) -> SchemaMetadataSpec:
    base = _semantic_metadata_spec(row.name, stage=row.category, extra=row.metadata_extra)
    keys = row.merge_keys or row.join_keys
    ordering = None
    if keys:
        ordering_keys = tuple((key, "ascending") for key in keys)
        ordering = ordering_metadata_spec(OrderingLevel.EXPLICIT, keys=ordering_keys)
    return merge_metadata_specs(base, ordering)


def _semantic_cdf_policy(row: SemanticDatasetRow) -> DeltaCdfPolicy | None:
    if not row.supports_cdf:
        return None
    return DeltaCdfPolicy(required=True, allow_out_of_range=False)


def _effective_materialization(row: SemanticDatasetRow) -> str | None:
    if row.materialization is not None:
        return row.materialization
    if row.role == "input":
        return None
    return "delta"


def _semantic_maintenance_policy(row: SemanticDatasetRow) -> DeltaMaintenancePolicy | None:
    if _effective_materialization(row) != "delta":
        return None
    zorder_cols = row.merge_keys if row.merge_keys else tuple(row.join_keys)
    zorder_when = "after_partition_complete" if zorder_cols else "never"
    return DeltaMaintenancePolicy(
        optimize_on_write=False,
        z_order_cols=zorder_cols,
        z_order_when=zorder_when,
        vacuum_on_write=False,
        enable_v2_checkpoints=True,
        enable_log_compaction=True,
    )


def _semantic_write_policy(row: SemanticDatasetRow) -> DeltaWritePolicy | None:
    if _effective_materialization(row) != "delta":
        return None
    return write_policy_from_row(row)


def _build_table_spec(row: SemanticDatasetRow) -> TableSchemaSpec:
    constraints = TableSpecConstraints(
        required_non_null=row.merge_keys or row.join_keys,
        key_fields=row.merge_keys or row.join_keys,
    )
    return make_table_spec(
        name=row.name,
        version=row.version,
        bundles=_bundles_for_row(row),
        fields=_field_specs_for_row(row),
        constraints=constraints,
    )


_SEMANTIC_VALIDATION_POLICY = ValidationPolicySpec(enabled=True, lazy=True, sample=1000)


def build_input_schema(row: SemanticDatasetRow) -> SchemaLike:
    """Build the input schema for a semantic dataset row.

    Returns:
    -------
    SchemaLike
        Schema describing the row inputs.
    """
    if row.role == "input":
        from datafusion_engine.extract.registry import dataset_schema

        return dataset_schema(row.source_dataset or row.name)
    table_spec = _build_table_spec(row)
    return table_spec.to_arrow_schema()


def build_dataset_spec(row: SemanticDatasetRow) -> DatasetSpec:
    """Build the DatasetSpec for a semantic dataset row.

    Returns:
    -------
    DatasetSpec
        Registered dataset specification.
    """
    table_spec = _build_table_spec(row)
    contract_spec = make_contract_spec(table_spec=table_spec)
    materialization = _effective_materialization(row)
    return make_dataset_spec(
        table_spec=table_spec,
        contract_spec=contract_spec,
        delta_cdf_policy=_semantic_cdf_policy(row),
        delta_maintenance_policy=_semantic_maintenance_policy(row),
        delta_write_policy=_semantic_write_policy(row),
        delta_schema_policy=DeltaSchemaPolicy(schema_mode="merge", column_mapping_mode="name")
        if materialization == "delta"
        else None,
        delta_feature_gate=None,
        metadata_spec=_build_metadata_spec(row),
        dataframe_validation=_SEMANTIC_VALIDATION_POLICY,
    )


__all__ = [
    "build_dataset_spec",
    "build_input_schema",
    "partition_spec_from_row",
    "write_policy_from_row",
]
