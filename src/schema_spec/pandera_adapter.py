"""Pandera adapters for Arrow-centric schemas."""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal, cast

import pandera.polars as pa_pl
import polars as pl
import pyarrow.types as patypes
from polars.datatypes.classes import DataTypeClass

import arrowdsl.pyarrow_core as pa
from arrowdsl.pyarrow_protocols import DataTypeLike, FieldLike, SchemaLike, TableLike
from schema_spec.core import TableSchemaSpec

_ARROW_TO_POLARS: tuple[tuple[Callable[[DataTypeLike], bool], DataTypeClass], ...] = (
    (patypes.is_string, pl.Utf8),
    (patypes.is_boolean, pl.Boolean),
    (patypes.is_int8, pl.Int8),
    (patypes.is_int16, pl.Int16),
    (patypes.is_int32, pl.Int32),
    (patypes.is_int64, pl.Int64),
    (patypes.is_uint8, pl.UInt8),
    (patypes.is_uint16, pl.UInt16),
    (patypes.is_uint32, pl.UInt32),
    (patypes.is_uint64, pl.UInt64),
    (patypes.is_float32, pl.Float32),
    (patypes.is_float64, pl.Float64),
    (patypes.is_binary, pl.Binary),
)

_POLARS_TO_ARROW: dict[DataTypeClass, DataTypeLike] = {
    pl.Utf8: pa.string(),
    pl.Boolean: pa.bool_(),
    pl.Int8: pa.int8(),
    pl.Int16: pa.int16(),
    pl.Int32: pa.int32(),
    pl.Int64: pa.int64(),
    pl.UInt8: pa.uint8(),
    pl.UInt16: pa.uint16(),
    pl.UInt32: pa.uint32(),
    pl.UInt64: pa.uint64(),
    pl.Float32: pa.float32(),
    pl.Float64: pa.float64(),
    pl.Binary: pa.binary(),
}


def _arrow_to_polars_dtype(dtype: DataTypeLike) -> DataTypeClass:
    for predicate, polars_dtype in _ARROW_TO_POLARS:
        if predicate(dtype):
            return polars_dtype
    if patypes.is_list(dtype):
        return pl.List
    return pl.Object


def _polars_dtype_class(dtype: pl.DataType | DataTypeClass) -> DataTypeClass:
    if isinstance(dtype, type):
        return cast("DataTypeClass", dtype)
    return cast("DataTypeClass", type(dtype))


def _polars_to_arrow_dtype(dtype: pl.DataType | DataTypeClass) -> DataTypeLike:
    if isinstance(dtype, pl.List):
        inner = cast("pl.DataType", dtype.inner)
        return pa.list_(_polars_to_arrow_dtype(inner))
    dtype_cls = _polars_dtype_class(dtype)
    mapped = _POLARS_TO_ARROW.get(dtype_cls)
    if mapped is not None:
        return mapped
    if dtype_cls is pl.List:
        return pa.list_(pa.binary())
    return pa.binary()


def table_spec_to_pandera(
    spec: TableSchemaSpec,
    *,
    strict: bool | Literal["filter"] = "filter",
    coerce: bool = False,
) -> pa_pl.DataFrameSchema:
    """Convert a TableSchemaSpec into a Pandera schema.

    Returns
    -------
    pandera.polars.DataFrameSchema
        Pandera schema suitable for validation.
    """
    columns = {
        field.name: pa_pl.Column(
            dtype=_arrow_to_polars_dtype(field.dtype),
            nullable=field.nullable,
            required=True,
        )
        for field in spec.fields
    }
    return pa_pl.DataFrameSchema(columns=columns, strict=strict, coerce=coerce)


def pandera_schema_to_arrow(schema: pa_pl.DataFrameSchema) -> SchemaLike:
    """Convert a Pandera schema into an Arrow schema.

    Returns
    -------
    pyarrow.Schema
        Arrow schema converted from Pandera.
    """
    fields: list[FieldLike] = []
    for name, column in schema.columns.items():
        arrow_dtype = _polars_to_arrow_dtype(column.dtype)
        fields.append(pa.field(name, arrow_dtype, nullable=column.nullable))
    return pa.schema(fields)


def validate_arrow_table(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    lazy: bool = True,
    strict: bool | Literal["filter"] = "filter",
    coerce: bool = False,
) -> TableLike:
    """Validate an Arrow table with Pandera.

    Returns
    -------
    pyarrow.Table
        Validated table in Arrow form.

    Raises
    ------
    TypeError
        If Arrow input or validation does not yield a polars DataFrame.
    """
    df = pl.from_arrow(table)
    if not isinstance(df, pl.DataFrame):
        msg = "Expected a polars DataFrame from Arrow input."
        raise TypeError(msg)
    schema = table_spec_to_pandera(spec, strict=strict, coerce=coerce)
    validated = schema.validate(df, lazy=lazy)
    if isinstance(validated, pl.LazyFrame):
        validated = validated.collect()
    if not isinstance(validated, pl.DataFrame):
        msg = "Expected a polars DataFrame after validation."
        raise TypeError(msg)
    return validated.to_arrow()
