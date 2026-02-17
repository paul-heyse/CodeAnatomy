"""Schema discovery exports."""

from __future__ import annotations

from pathlib import Path
from typing import Unpack

import pyarrow.dataset as ds

from core_types import PathLike
from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.delta.service_protocol import DeltaServicePort
from schema_spec.dataset_spec import (
    DatasetOpenSpec,
    DatasetSpec,
    DatasetSpecKwargs,
    DeltaSchemaRequest,
    TableSchemaSpec,
)
from schema_spec.metadata_spec import metadata_spec_from_schema
from schema_spec.table_spec import delta_constraints_from_table_spec


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
    from schema_spec.dataset_spec import make_dataset_spec, resolve_schema_evolution_spec

    table_spec = TableSchemaSpec.from_schema(name, schema, version=version)
    evolution_spec = resolve_schema_evolution_spec(name)
    return make_dataset_spec(
        table_spec=table_spec,
        metadata_spec=metadata_spec_from_schema(schema),
        evolution_spec=evolution_spec,
        delta_constraints=delta_constraints_from_table_spec(table_spec),
    )


def dataset_spec_from_dataset(
    *,
    name: str,
    dataset: ds.Dataset,
    version: int | None = None,
) -> DatasetSpec:
    """Build dataset spec from an in-memory dataset payload.

    Returns:
        DatasetSpec: Dataset spec derived from the provided dataset.
    """
    return dataset_spec_from_schema(name=name, schema=dataset.schema, version=version)


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

    Raises:
        ValueError: If Delta schema resolution is requested without service support
            or when Delta schema cannot be resolved.
    """
    options = options or DatasetOpenSpec()
    raw = str(path)
    normalized = raw if "://" in raw else str(Path(raw).expanduser())
    if options.dataset_format == "delta":
        if delta_service is None:
            msg = "dataset_spec_from_path(..., dataset_format='delta') requires delta_service."
            raise ValueError(msg)
        schema = delta_service.table_schema(
            DeltaSchemaRequest(
                path=normalized,
                storage_options=options.storage_options or None,
                log_storage_options=options.delta_log_storage_options or None,
                version=options.delta_version,
                timestamp=options.delta_timestamp,
            )
        )
        if schema is None:
            msg = f"Delta schema unavailable for dataset at {normalized!r}."
            raise ValueError(msg)
        return dataset_spec_from_schema(
            name=name,
            schema=schema,
            version=version,
        )
    dataset = options.open(normalized)
    return dataset_spec_from_dataset(name=name, dataset=dataset, version=version)


def make_dataset_spec(
    *,
    table_spec: TableSchemaSpec,
    **kwargs: Unpack[DatasetSpecKwargs],
) -> DatasetSpec:
    """Build dataset spec from explicit structural inputs.

    Returns:
        DatasetSpec: Dataset spec assembled from explicit fields.
    """
    from schema_spec.dataset_spec import make_dataset_spec as _make_dataset_spec

    return _make_dataset_spec(table_spec=table_spec, **kwargs)


__all__ = [
    "dataset_spec_from_dataset",
    "dataset_spec_from_path",
    "dataset_spec_from_schema",
    "make_dataset_spec",
]
