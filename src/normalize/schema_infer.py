"""Infer and align Arrow schemas across extraction tables."""

from __future__ import annotations

from dataclasses import dataclass

from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.schema.schema import SchemaTransform
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.schema_registry import is_nested_dataset, nested_schema_for


@dataclass(frozen=True)
class SchemaInferOptions:
    """Schema inference and alignment policy.

    promote_options="permissive" is the key setting that makes this system robust to
    missing columns, type drift, and partial extraction results.
    """

    promote_options: str = "permissive"
    keep_extra_columns: bool = False
    safe_cast: bool = False  # False is usually what you want for “accept ambiguity” systems.


def infer_schema_or_registry(
    name: str,
) -> SchemaLike:
    """Resolve a schema from the DataFusion SessionContext.

    Parameters
    ----------
    name : str
        Dataset name registered in the SessionContext.

    Returns
    -------
    SchemaLike
        DataFusion schema for the dataset.
    """
    if is_nested_dataset(name):
        return nested_schema_for(name, allow_derived=True)
    ctx = DataFusionRuntimeProfile().session_context()
    try:
        return ctx.table(name).schema()
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Dataset schema not registered in DataFusion: {name!r}."
        raise KeyError(msg) from exc


def align_table_to_schema(
    table: TableLike,
    schema: SchemaLike,
    *,
    opts: SchemaInferOptions | None = None,
) -> TableLike:
    """Align a table to a target schema.

    Aligns the table by:
      - adding missing columns as nulls
      - casting existing columns to the target types (safe_cast configurable)
      - optionally dropping extra columns
      - ensuring the output column order matches schema

    This is a foundational building block for “inference-driven, schema-flexible” pipelines.

    Returns
    -------
    TableLike
        Table aligned to the provided schema.
    """
    opts = opts or SchemaInferOptions()
    transform = SchemaTransform(
        schema=schema,
        safe_cast=opts.safe_cast,
        keep_extra_columns=opts.keep_extra_columns,
        on_error="keep",
    )
    return transform.apply(table)
