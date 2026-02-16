"""Schema-oriented helpers for dataset registration."""

from __future__ import annotations

from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.dataset.registry import DatasetLocation, resolve_dataset_schema


def resolved_registration_schema(location: DatasetLocation) -> SchemaLike | None:
    """Return resolved schema for registration planning."""
    return resolve_dataset_schema(location)


__all__ = ["resolved_registration_schema"]
