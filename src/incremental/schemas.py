"""Incremental dataset schema registration helpers."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.core.interop import SchemaLike
from datafusion_engine.runtime import DataFusionRuntimeProfile, dataset_schema_from_context
from datafusion_engine.schema_registry import registered_table_names
from schema_spec.system import DatasetSpec, dataset_spec_from_schema


def incremental_dataset_specs() -> tuple[DatasetSpec, ...]:
    """Return incremental dataset specs derived from DataFusion.

    Returns
    -------
    tuple[object, ...]
        Dataset specs for incremental datasets.
    """
    ctx = DataFusionRuntimeProfile().session_context()
    specs: list[DatasetSpec] = []
    for name in sorted(registered_table_names(ctx)):
        try:
            schema = dataset_schema_from_context(name)
        except KeyError:
            continue
        if not _is_incremental_schema(schema):
            continue
        specs.append(dataset_spec_from_schema(name, schema))
    return tuple(specs)


def _is_incremental_schema(schema: SchemaLike) -> bool:
    resolved = schema if isinstance(schema, pa.Schema) else pa.schema(schema)
    metadata = resolved.metadata or {}
    return metadata.get(b"incremental_stage") == b"incremental"


__all__ = ["incremental_dataset_specs"]
