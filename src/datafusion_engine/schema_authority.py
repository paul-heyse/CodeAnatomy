"""Provide DataFusion-backed dataset schema accessors."""

from __future__ import annotations

from arrowdsl.core.interop import SchemaLike
from datafusion_engine.runtime import DataFusionRuntimeProfile
from schema_spec.system import DatasetSpec, dataset_spec_from_schema


def dataset_schema_from_context(name: str) -> SchemaLike:
    """Return the dataset schema from the DataFusion SessionContext.

    Parameters
    ----------
    name : str
        Dataset name registered in the DataFusion SessionContext.

    Returns
    -------
    SchemaLike
        Arrow schema fetched from DataFusion.

    Raises
    ------
    KeyError
        Raised when the dataset is not registered in the SessionContext.
    """
    ctx = DataFusionRuntimeProfile().session_context()
    try:
        return ctx.table(name).schema()
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Dataset schema not registered in DataFusion: {name!r}."
        raise KeyError(msg) from exc


def dataset_spec_from_context(name: str) -> DatasetSpec:
    """Return a DatasetSpec derived from the DataFusion schema.

    Parameters
    ----------
    name : str
        Dataset name registered in the DataFusion SessionContext.

    Returns
    -------
    DatasetSpec
        DatasetSpec derived from the DataFusion schema.
    """
    schema = dataset_schema_from_context(name)
    return dataset_spec_from_schema(name, schema)


__all__ = ["dataset_schema_from_context", "dataset_spec_from_context"]
