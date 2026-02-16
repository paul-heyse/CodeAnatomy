"""Schema discovery exports."""

from __future__ import annotations

from pathlib import Path
from typing import Unpack

from core_types import PathLike
from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.delta.service_protocol import DeltaServicePort
from schema_spec.dataset_spec import (
    DatasetOpenSpec,
    DatasetSpec,
    DatasetSpecKwargs,
    TableSchemaSpec,
)
from schema_spec.dataset_spec import (
    dataset_spec_from_dataset as _dataset_spec_from_dataset,
)
from schema_spec.dataset_spec import (
    dataset_spec_from_path as _dataset_spec_from_path,
)
from schema_spec.dataset_spec import (
    dataset_spec_from_schema as _dataset_spec_from_schema,
)
from schema_spec.dataset_spec import (
    make_dataset_spec as _make_dataset_spec,
)


def dataset_spec_from_schema(
    *,
    name: str,
    schema: SchemaLike,
    version: int | None = None,
) -> DatasetSpec:
    """Build dataset spec from a concrete Arrow schema.

    Returns:
        DatasetSpec: Dataset spec derived from the provided schema.
    """
    return _dataset_spec_from_schema(
        name=name,
        schema=schema,
        version=version,
    )


def dataset_spec_from_dataset(
    *,
    name: str,
    dataset: object,
    version: int | None = None,
) -> DatasetSpec:
    """Build dataset spec from an in-memory dataset payload.

    Returns:
        DatasetSpec: Dataset spec derived from the provided dataset.
    """
    return _dataset_spec_from_dataset(
        name=name,
        dataset=dataset,
        version=version,
    )


def dataset_spec_from_path(
    *,
    name: str,
    path: PathLike,
    options: DatasetOpenSpec | None = None,
    version: int | None = None,
    delta_service: DeltaServicePort | None = None,
) -> DatasetSpec:
    """Build dataset spec by inspecting files at path.

    Returns:
        DatasetSpec: Dataset spec resolved from the given path.
    """
    raw = str(path)
    normalized = raw if "://" in raw else str(Path(raw).expanduser())
    return _dataset_spec_from_path(
        name=name,
        path=normalized,
        options=options,
        version=version,
        delta_service=delta_service,
    )


def make_dataset_spec(
    *,
    table_spec: TableSchemaSpec,
    **kwargs: Unpack[DatasetSpecKwargs],
) -> DatasetSpec:
    """Build dataset spec from explicit structural inputs.

    Returns:
        DatasetSpec: Dataset spec assembled from explicit fields.
    """
    return _make_dataset_spec(table_spec=table_spec, **kwargs)


__all__ = [
    "dataset_spec_from_dataset",
    "dataset_spec_from_path",
    "dataset_spec_from_schema",
    "make_dataset_spec",
]
