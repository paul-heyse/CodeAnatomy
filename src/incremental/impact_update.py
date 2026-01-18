"""Persist incremental impact diagnostics to the state store."""

from __future__ import annotations

from arrowdsl.core.interop import TableLike
from arrowdsl.io.delta import (
    DeltaWriteOptions,
    coerce_delta_table,
    write_dataset_delta,
)
from arrowdsl.schema.metadata import encoding_policy_from_schema
from incremental.registry_specs import dataset_schema
from incremental.state_store import StateStore

_IMPACTED_CALLERS_DATASET = "inc_impacted_callers_v1"
_IMPACTED_IMPORTERS_DATASET = "inc_impacted_importers_v1"
_IMPACTED_FILES_DATASET = "inc_impacted_files_v2"


def write_impacted_callers(
    impacted_callers: TableLike,
    *,
    state_store: StateStore,
) -> dict[str, str]:
    """Write impacted callers diagnostics to the state store.

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset path.
    """
    schema = dataset_schema(_IMPACTED_CALLERS_DATASET)
    data = coerce_delta_table(
        impacted_callers,
        schema=schema,
        encoding_policy=encoding_policy_from_schema(schema),
    )
    result = write_dataset_delta(
        data,
        str(state_store.dataset_dir(_IMPACTED_CALLERS_DATASET)),
        options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
    )
    return {_IMPACTED_CALLERS_DATASET: result.path}


def write_impacted_importers(
    impacted_importers: TableLike,
    *,
    state_store: StateStore,
) -> dict[str, str]:
    """Write impacted importers diagnostics to the state store.

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset path.
    """
    schema = dataset_schema(_IMPACTED_IMPORTERS_DATASET)
    data = coerce_delta_table(
        impacted_importers,
        schema=schema,
        encoding_policy=encoding_policy_from_schema(schema),
    )
    result = write_dataset_delta(
        data,
        str(state_store.dataset_dir(_IMPACTED_IMPORTERS_DATASET)),
        options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
    )
    return {_IMPACTED_IMPORTERS_DATASET: result.path}


def write_impacted_files(
    impacted_files: TableLike,
    *,
    state_store: StateStore,
) -> dict[str, str]:
    """Write impacted file diagnostics to the state store.

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset path.
    """
    schema = dataset_schema(_IMPACTED_FILES_DATASET)
    data = coerce_delta_table(
        impacted_files,
        schema=schema,
        encoding_policy=encoding_policy_from_schema(schema),
    )
    result = write_dataset_delta(
        data,
        str(state_store.dataset_dir(_IMPACTED_FILES_DATASET)),
        options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
    )
    return {_IMPACTED_FILES_DATASET: result.path}


__all__ = ["write_impacted_callers", "write_impacted_files", "write_impacted_importers"]
