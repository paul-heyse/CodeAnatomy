"""Incremental dataset schema registration helpers."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.schema.registry import registered_table_names
from datafusion_engine.session.runtime import DataFusionRuntimeProfile, dataset_schema_from_context
from schema_spec.system import (
    DatasetSpec,
    DeltaMaintenancePolicy,
    dataset_spec_from_schema,
    dataset_spec_with_delta_maintenance,
)


def incremental_dataset_specs() -> tuple[DatasetSpec, ...]:
    """Return incremental dataset specs derived from DataFusion.

    Returns:
    -------
    tuple[object, ...]
        Dataset specs for incremental datasets.
    """
    ctx = DataFusionRuntimeProfile().session_runtime().ctx
    specs: list[DatasetSpec] = []
    for name in sorted(registered_table_names(ctx)):
        try:
            schema = dataset_schema_from_context(name)
        except KeyError:
            continue
        if not _is_incremental_schema(schema):
            continue
        spec = dataset_spec_from_schema(name, schema)
        maintenance = _incremental_maintenance_policy(spec)
        spec = dataset_spec_with_delta_maintenance(spec, maintenance)
        specs.append(spec)
    return tuple(specs)


def _is_incremental_schema(schema: SchemaLike) -> bool:
    resolved = schema if isinstance(schema, pa.Schema) else pa.schema(schema)
    metadata = resolved.metadata or {}
    return metadata.get(b"incremental_stage") == b"incremental"


def _incremental_maintenance_policy(spec: DatasetSpec) -> DeltaMaintenancePolicy | None:
    from schema_spec.system import dataset_spec_schema

    schema = dataset_spec_schema(spec)
    resolved = schema if isinstance(schema, pa.Schema) else pa.schema(schema)
    candidates = ("file_id", "path", "node_id", "edge_id", "span_id")
    z_order_cols = tuple(name for name in candidates if name in resolved.names)
    if not z_order_cols:
        return None
    return DeltaMaintenancePolicy(
        optimize_on_write=True,
        optimize_target_size=256 * 1024 * 1024,
        z_order_cols=z_order_cols,
        z_order_when="after_partition_complete",
        vacuum_on_write=False,
        enable_deletion_vectors=True,
        enable_v2_checkpoints=True,
        enable_log_compaction=True,
    )


__all__ = ["incremental_dataset_specs"]
