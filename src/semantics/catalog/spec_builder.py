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
from datafusion_engine.delta.protocol import DeltaFeatureGate
from schema_spec.field_spec import FieldSpec
from schema_spec.registration import DatasetRegistration, register_dataset
from schema_spec.specs import (
    FieldBundle,
    file_identity_bundle,
    span_bundle,
)
from schema_spec.system import (
    DatasetSpec,
    DeltaCdfPolicy,
    DeltaMaintenancePolicy,
    make_table_spec,
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

    Returns
    -------
    tuple[tuple[str, pa.DataType], ...]
        Partition column specifications as (name, dtype) pairs.
    """
    import pyarrow as pa

    if not row.partition_cols:
        return ()
    schema = table_spec.to_arrow_schema()
    field_types: dict[str, pa.DataType] = {}
    for field in schema:
        field_types[field.name] = field.type
    return tuple((col, field_types.get(col, pa.string())) for col in row.partition_cols)


def write_policy_from_row(row: SemanticDatasetRow) -> DeltaWritePolicy:
    """Build write policy including merge keys and partitions.

    Uses merge_keys for z-ordering when available, falling back to join_keys.
    Partition columns from the row are applied to the write policy.

    Parameters
    ----------
    row
        Semantic dataset row definition.

    Returns
    -------
    DeltaWritePolicy
        Write policy configured from row metadata.
    """
    # Determine z-order columns: prefer merge_keys, then join_keys
    zorder_cols = row.merge_keys if row.merge_keys else tuple(row.join_keys)

    # Collect bloom filter candidates from fields ending with _id
    bloom_columns = tuple(field_name for field_name in row.fields if field_name.endswith("_id"))

    # Stats columns combine join keys and id fields
    stats_columns = tuple(dict.fromkeys((*row.join_keys, *bloom_columns))) or None

    return DeltaWritePolicy(
        target_file_size=128 * 1024 * 1024,
        partition_by=row.partition_cols,
        zorder_by=zorder_cols,
        stats_policy="explicit" if stats_columns else "auto",
        stats_columns=stats_columns,
        enable_features=("change_data_feed", "column_mapping", "v2_checkpoints"),
    )


# Bundle catalog for semantic datasets - mirrors normalize.dataset_bundles
_SEMANTIC_BUNDLE_CATALOG: dict[str, FieldBundle] = {
    "file_identity": file_identity_bundle(include_sha256=False),
    "span": FieldBundle(
        name="span",
        fields=(FieldSpec(name="span", dtype=SPAN_STORAGE),),
    ),
}


def _bundle(name: str) -> FieldBundle:
    """Return a semantic field bundle by name.

    Parameters
    ----------
    name
        Bundle name to retrieve.

    Returns
    -------
    FieldBundle
        Bundle specification.

    Raises
    ------
    KeyError
        Raised when the bundle name is not recognized.
    """
    if name not in _SEMANTIC_BUNDLE_CATALOG:
        # Fall back to standard bundles
        if name == "file_identity":
            return file_identity_bundle(include_sha256=False)
        if name == "span":
            return span_bundle()
        msg = f"Unknown semantic bundle: {name!r}"
        raise KeyError(msg)
    return _SEMANTIC_BUNDLE_CATALOG[name]


def _bundles_for_row(row: SemanticDatasetRow) -> tuple[FieldBundle, ...]:
    """Return field bundles for a semantic dataset row.

    Parameters
    ----------
    row
        Semantic dataset row definition.

    Returns
    -------
    tuple[FieldBundle, ...]
        Bundle specifications for the row.
    """
    return tuple(_bundle(name) for name in row.bundles)


def _field_specs_for_row(row: SemanticDatasetRow) -> list[FieldSpec]:
    """Return field specs for a semantic dataset row.

    Parameters
    ----------
    row
        Semantic dataset row definition.

    Returns
    -------
    list[FieldSpec]
        Field specifications derived from the row fields.
    """
    # Default to string type for semantic fields
    # More sophisticated type inference can be added later
    return [FieldSpec(name=field_name, dtype=pa.string()) for field_name in row.fields]


def _semantic_metadata_spec(
    dataset_name: str,
    *,
    stage: str = "semantic",
    extra: Mapping[bytes, bytes] | None = None,
) -> SchemaMetadataSpec:
    """Return schema metadata for semantic datasets.

    Parameters
    ----------
    dataset_name
        Name of the semantic dataset.
    stage
        Processing stage name for metadata.
    extra
        Additional metadata entries.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for semantic datasets.
    """
    meta: dict[bytes, bytes] = {
        b"semantic_stage": stage.encode("utf-8"),
        b"semantic_dataset": dataset_name.encode("utf-8"),
    }
    if extra:
        meta.update(extra)
    return SchemaMetadataSpec(schema_metadata=meta)


def _build_metadata_spec(row: SemanticDatasetRow) -> SchemaMetadataSpec:
    """Build the metadata spec for a semantic dataset row.

    Parameters
    ----------
    row
        Semantic dataset row definition.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for the dataset schema.
    """
    stage = "semantic" if row.category == "semantic" else row.category
    extra: dict[bytes, bytes] = dict(row.metadata_extra) if row.metadata_extra else {}
    metadata = _semantic_metadata_spec(row.name, stage=stage, extra=extra)

    if not row.join_keys:
        return metadata

    ordering = ordering_metadata_spec(
        OrderingLevel.IMPLICIT,
        keys=tuple((name, "ascending") for name in row.join_keys),
    )
    return merge_metadata_specs(metadata, ordering)


def _semantic_cdf_policy(row: SemanticDatasetRow) -> DeltaCdfPolicy:
    """Return CDF policy for a semantic dataset row.

    Parameters
    ----------
    row
        Semantic dataset row definition.

    Returns
    -------
    DeltaCdfPolicy
        CDF policy configured based on row settings.
    """
    return DeltaCdfPolicy(required=row.supports_cdf, allow_out_of_range=False)


def _semantic_maintenance_policy(row: SemanticDatasetRow) -> DeltaMaintenancePolicy:
    """Return maintenance policy for a semantic dataset row.

    Parameters
    ----------
    row
        Semantic dataset row definition.

    Returns
    -------
    DeltaMaintenancePolicy
        Maintenance policy configured based on row settings.
    """
    z_order_cols = tuple(row.join_keys) if row.join_keys else ()
    z_order_when = "after_partition_complete" if z_order_cols else "never"
    return DeltaMaintenancePolicy(
        optimize_on_write=True,
        optimize_target_size=256 * 1024 * 1024,
        z_order_cols=z_order_cols,
        z_order_when=z_order_when,
        vacuum_on_write=False,
        enable_deletion_vectors=True,
        enable_v2_checkpoints=True,
        enable_log_compaction=True,
    )


def _semantic_write_policy(row: SemanticDatasetRow) -> DeltaWritePolicy:
    """Return write policy for a semantic dataset row.

    Uses merge_keys for z-ordering when available, falling back to join_keys.
    Partition columns from the row are applied to the write policy.

    Parameters
    ----------
    row
        Semantic dataset row definition.

    Returns
    -------
    DeltaWritePolicy
        Write policy configured based on row settings.
    """
    # Delegate to write_policy_from_row for consistent behavior
    return write_policy_from_row(row)


def _semantic_schema_policy() -> DeltaSchemaPolicy:
    """Return schema policy for semantic datasets.

    Returns
    -------
    DeltaSchemaPolicy
        Schema policy for semantic datasets.
    """
    return DeltaSchemaPolicy(schema_mode="merge", column_mapping_mode="name")


def _semantic_feature_gate() -> DeltaFeatureGate:
    """Return feature gate for semantic datasets.

    Returns
    -------
    DeltaFeatureGate
        Feature gate for semantic datasets.
    """
    return DeltaFeatureGate(
        required_writer_features=("change_data_feed", "column_mapping", "v2_checkpoints"),
        required_reader_features=(),
    )


def build_input_schema(row: SemanticDatasetRow) -> SchemaLike:
    """Build the input schema for a semantic dataset row.

    The input schema represents the schema expected from upstream sources
    before any transformations or derived fields are applied.

    Parameters
    ----------
    row
        Semantic dataset row definition.

    Returns
    -------
    SchemaLike
        Input schema for plan sources.
    """
    bundles = _bundles_for_row(row)
    field_specs = _field_specs_for_row(row)

    return make_table_spec(
        name=f"{row.name}_input",
        version=row.version,
        bundles=bundles,
        fields=tuple(field_specs),
    ).to_arrow_schema()


def build_dataset_spec(row: SemanticDatasetRow) -> DatasetSpec:
    """Build a DatasetSpec from a SemanticDatasetRow.

    Constructs a complete DatasetSpec including query, contract, metadata,
    and Delta Lake policies derived from the semantic dataset row definition.

    Parameters
    ----------
    row
        Semantic dataset row definition.

    Returns
    -------
    DatasetSpec
        Dataset specification for the semantic dataset.
    """
    bundles = _bundles_for_row(row)
    field_specs = _field_specs_for_row(row)

    table_spec = make_table_spec(
        name=row.name,
        version=row.version,
        bundles=bundles,
        fields=field_specs,
    )

    metadata_spec = _build_metadata_spec(row)
    registration = DatasetRegistration(
        contract_spec=None,
        metadata_spec=metadata_spec,
        delta_cdf_policy=_semantic_cdf_policy(row),
        delta_maintenance_policy=_semantic_maintenance_policy(row),
        delta_write_policy=_semantic_write_policy(row),
        delta_schema_policy=_semantic_schema_policy(),
        delta_feature_gate=_semantic_feature_gate(),
    )

    return register_dataset(table_spec=table_spec, registration=registration)


__all__ = [
    "build_dataset_spec",
    "build_input_schema",
    "partition_spec_from_row",
    "write_policy_from_row",
]
