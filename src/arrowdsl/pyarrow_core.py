"""Typed wrapper for core pyarrow module APIs."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import cast

import pyarrow as pa

from arrowdsl.pyarrow_protocols import (
    ArrayLike,
    ChunkedArrayLike,
    DataTypeLike,
    FieldLike,
    ListArrayLike,
    NativeFileLike,
    RecordBatchReaderLike,
    ScalarLike,
    SchemaLike,
    StructArrayLike,
    TableLike,
)

Array: type[ArrayLike] = cast("type[ArrayLike]", pa.Array)
ChunkedArray: type[ChunkedArrayLike] = cast("type[ChunkedArrayLike]", pa.ChunkedArray)
Scalar: type[ScalarLike] = cast("type[ScalarLike]", pa.Scalar)
DataType: type[DataTypeLike] = cast("type[DataTypeLike]", pa.DataType)
Field: type[FieldLike] = cast("type[FieldLike]", pa.Field)
Schema: type[SchemaLike] = cast("type[SchemaLike]", pa.Schema)
RecordBatchReader: type[RecordBatchReaderLike] = cast(
    "type[RecordBatchReaderLike]", pa.RecordBatchReader
)
NativeFile: type[NativeFileLike] = cast("type[NativeFileLike]", pa.NativeFile)
Table: type[TableLike] = cast("type[TableLike]", pa.Table)
ListArray: type[ListArrayLike] = cast("type[ListArrayLike]", pa.ListArray)
StructArray: type[StructArrayLike] = cast("type[StructArrayLike]", pa.StructArray)

ArrowInvalid: type[Exception] = cast("type[Exception]", pa.ArrowInvalid)
ArrowTypeError: type[Exception] = cast("type[Exception]", pa.ArrowTypeError)

array: Callable[..., ArrayLike] = pa.array
scalar: Callable[..., ScalarLike] = pa.scalar
table: Callable[..., TableLike] = pa.table
schema: Callable[[Sequence[FieldLike | tuple[str, DataTypeLike]]], SchemaLike] = pa.schema
field: Callable[..., FieldLike] = pa.field
nulls: Callable[..., ArrayLike] = pa.nulls
concat_tables: Callable[..., TableLike] = pa.concat_tables
unify_schemas: Callable[..., SchemaLike] = pa.unify_schemas
set_cpu_count: Callable[[int], None] = pa.set_cpu_count
set_io_thread_count: Callable[[int], None] = pa.set_io_thread_count
OSFile: Callable[..., NativeFileLike] = pa.OSFile
memory_map: Callable[..., NativeFileLike] = pa.memory_map
from_numpy_dtype: Callable[[object], DataTypeLike] = pa.from_numpy_dtype

string: Callable[[], DataTypeLike] = pa.string
int8: Callable[[], DataTypeLike] = pa.int8
int16: Callable[[], DataTypeLike] = pa.int16
int32: Callable[[], DataTypeLike] = pa.int32
int64: Callable[[], DataTypeLike] = pa.int64
uint8: Callable[[], DataTypeLike] = pa.uint8
uint16: Callable[[], DataTypeLike] = pa.uint16
uint32: Callable[[], DataTypeLike] = pa.uint32
uint64: Callable[[], DataTypeLike] = pa.uint64
float32: Callable[[], DataTypeLike] = pa.float32
float64: Callable[[], DataTypeLike] = pa.float64
bool_: Callable[[], DataTypeLike] = pa.bool_
binary: Callable[[], DataTypeLike] = pa.binary
list_: Callable[..., DataTypeLike] = pa.list_
struct: Callable[..., DataTypeLike] = pa.struct

__all__ = [
    "Array",
    "ArrowInvalid",
    "ArrowTypeError",
    "ChunkedArray",
    "DataType",
    "Field",
    "ListArray",
    "NativeFile",
    "OSFile",
    "RecordBatchReader",
    "Scalar",
    "Schema",
    "StructArray",
    "Table",
    "array",
    "binary",
    "bool_",
    "concat_tables",
    "field",
    "float32",
    "float64",
    "from_numpy_dtype",
    "int8",
    "int16",
    "int32",
    "int64",
    "list_",
    "memory_map",
    "nulls",
    "scalar",
    "schema",
    "set_cpu_count",
    "set_io_thread_count",
    "string",
    "struct",
    "table",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "unify_schemas",
]
