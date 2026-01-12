"""Helpers for registering extractor datasets."""

from __future__ import annotations

from collections.abc import Sequence

from schema_spec.specs import ArrowFieldSpec, FieldBundle
from schema_spec.system import (
    GLOBAL_SCHEMA_REGISTRY,
    DatasetSpec,
    make_dataset_spec,
    make_table_spec,
)


def register_dataset(
    *,
    name: str,
    version: int,
    fields: Sequence[ArrowFieldSpec],
    bundles: Sequence[FieldBundle] = (),
) -> DatasetSpec:
    """Register a dataset spec with the global schema registry.

    Returns
    -------
    DatasetSpec
        Registered dataset specification.
    """
    return GLOBAL_SCHEMA_REGISTRY.register_dataset(
        make_dataset_spec(
            table_spec=make_table_spec(
                name=name,
                version=version,
                bundles=tuple(bundles),
                fields=list(fields),
            )
        )
    )
