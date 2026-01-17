"""Incremental dataset schema registration helpers."""

from __future__ import annotations

from incremental.registry_rows import DATASET_ROWS
from incremental.registry_specs import dataset_spec
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY, SchemaRegistry


def register_incremental_specs(registry: SchemaRegistry) -> SchemaRegistry:
    """Register incremental dataset specs into the provided registry.

    Returns
    -------
    SchemaRegistry
        Registry with incremental specs added.
    """
    for row in DATASET_ROWS:
        registry.register_dataset(dataset_spec(row.name))
    return registry


INCREMENTAL_SCHEMA_REGISTRY = register_incremental_specs(GLOBAL_SCHEMA_REGISTRY)


__all__ = ["INCREMENTAL_SCHEMA_REGISTRY", "register_incremental_specs"]
