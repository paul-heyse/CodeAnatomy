"""Incremental dataset schema registration helpers."""

from __future__ import annotations

from incremental.registry_rows import DATASET_ROWS
from incremental.registry_specs import dataset_spec
from schema_spec.system import DatasetSpec


def incremental_dataset_specs() -> tuple[DatasetSpec, ...]:
    """Return incremental dataset specs derived from DataFusion.

    Returns
    -------
    tuple[object, ...]
        Dataset specs for incremental datasets.
    """
    return tuple(dataset_spec(row.name) for row in DATASET_ROWS)


__all__ = ["incremental_dataset_specs"]
