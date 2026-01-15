"""Arrow interop shims and protocol definitions."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from types import TracebackType
from typing import Protocol, Self, cast, runtime_checkable

import pyarrow as pa
import pyarrow.compute as _pc
from pyarrow import acero as _acero


@runtime_checkable
class DataTypeLike(Protocol):
    """Protocol for pyarrow.DataType."""


@runtime_checkable
class FieldLike(Protocol):
    """Protocol for pyarrow.Field."""

    name: str
    type: DataTypeLike
    metadata: Mapping[bytes, bytes] | None
    nullable: bool

    def with_metadata(self, metadata: Mapping[bytes, bytes]) -> FieldLike:
        """Return a field with updated metadata."""
        ...

    def flatten(self) -> list[FieldLike]:
        """Return flattened child fields for struct types."""
        ...


@runtime_checkable
class ArrayLike(Protocol):
    """Protocol for pyarrow.Array behavior we rely on."""

    null_count: int
    type: DataTypeLike

    def field(self, index: int | str) -> ArrayLike:
        """Return a field from a struct-like array."""
        ...

    def to_pylist(self) -> list[object]:
        """Return the array as a Python list."""
        ...

    def __iter__(self) -> Iterator[object]:
        """Iterate over array elements."""
        ...

    def __len__(self) -> int:
        """Return array length."""
        ...


@runtime_checkable
class ChunkedArrayLike(ArrayLike, Protocol):
    """Protocol for pyarrow.ChunkedArray behavior we rely on."""

    def combine_chunks(self) -> ArrayLike:
        """Combine chunks into a single Array-like object."""
        ...


@runtime_checkable
class ScalarLike(Protocol):
    """Protocol for pyarrow.Scalar behavior we rely on."""

    type: DataTypeLike

    def as_py(self) -> object:
        """Return the scalar as a Python value."""
        ...


@runtime_checkable
class SchemaLike(Protocol):
    """Protocol for pyarrow.Schema behavior we rely on."""

    names: list[str]
    metadata: Mapping[bytes, bytes] | None

    def with_metadata(self, metadata: Mapping[bytes, bytes]) -> SchemaLike:
        """Return a schema with updated metadata."""
        ...

    def field(self, name_or_index: str | int) -> FieldLike:
        """Return a field by name or index."""
        ...

    def get_field_index(self, name: str) -> int:
        """Return the index for a field name."""
        ...

    def __iter__(self) -> Iterator[FieldLike]:
        """Iterate over fields."""
        ...


@runtime_checkable
class RecordBatchReaderLike(Protocol):
    """Protocol for pyarrow.RecordBatchReader behavior we rely on."""

    schema: SchemaLike

    @classmethod
    def __subclasshook__(cls, subclass: type, /) -> bool:
        """Return True when a subclass satisfies the reader protocol shape.

        Returns
        -------
        bool
            True when the subclass advertises the expected reader attributes.
        """
        if cls is not RecordBatchReaderLike:
            return NotImplemented
        required = ("schema", "read_all")
        return all(hasattr(subclass, name) for name in required)

    def read_all(self) -> TableLike:
        """Read all batches into a table."""
        ...


@runtime_checkable
class NativeFileLike(Protocol):
    """Protocol for pyarrow.NativeFile behavior we rely on."""

    def __enter__(self) -> Self:
        """Enter the file context."""
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit the file context."""
        ...


@runtime_checkable
class TableGroupByLike(Protocol):
    """Protocol for pyarrow.TableGroupBy behavior we rely on."""

    def aggregate(self, aggs: Sequence[tuple[str, str]]) -> TableLike:
        """Aggregate the grouped table."""
        ...


@runtime_checkable
class TableLike(Protocol):
    """Protocol for pyarrow.Table behavior we rely on."""

    num_rows: int
    column_names: list[str]
    schema: SchemaLike

    @classmethod
    def __subclasshook__(cls, subclass: type, /) -> bool:
        """Return True when a subclass satisfies the table protocol shape.

        Returns
        -------
        bool
            True when the subclass advertises the expected table attributes.
        """
        if cls is not TableLike:
            return NotImplemented
        required = (
            "schema",
            "column_names",
            "num_rows",
            "to_pylist",
            "to_pydict",
            "filter",
            "take",
            "select",
        )
        return all(hasattr(subclass, name) for name in required)

    @classmethod
    def from_pylist(
        cls, data: Sequence[Mapping[str, object]], schema: SchemaLike | None = None
    ) -> TableLike:
        """Build a table from row-wise dicts."""
        ...

    @classmethod
    def from_arrays(
        cls,
        arrays: Sequence[ArrayLike | ChunkedArrayLike],
        schema: SchemaLike | None = None,
        names: Sequence[str] | None = None,
    ) -> TableLike:
        """Build a table from arrays."""
        ...

    def append_column(self, name: str, data: ArrayLike | ChunkedArrayLike) -> TableLike:
        """Return a table with an appended column."""
        ...

    def drop(self, columns: Sequence[str]) -> TableLike:
        """Return a table without the specified columns."""
        ...

    def rename_columns(self, names: Sequence[str]) -> TableLike:
        """Return a table with renamed columns."""
        ...

    def to_pylist(self) -> list[dict[str, object]]:
        """Return the table as row-wise dicts."""
        ...

    def to_pydict(self) -> dict[str, list[object]]:
        """Return the table as column-wise Python lists."""
        ...

    def filter(self, mask: ArrayLike) -> TableLike:
        """Filter rows by a boolean mask."""
        ...

    def take(self, indices: ArrayLike) -> TableLike:
        """Take rows by integer indices."""
        ...

    def group_by(self, keys: Sequence[str], *, use_threads: bool = True) -> TableGroupByLike:
        """Group rows by the given keys."""
        ...

    def join(self, right: TableLike, keys: Sequence[str], **kwargs: object) -> TableLike:
        """Join with another table."""
        ...

    def join_asof(self, right: TableLike, on: str, **kwargs: object) -> TableLike:
        """Join with another table using as-of semantics."""
        ...

    def select(self, names: Sequence[str]) -> TableLike:
        """Select columns by name."""
        ...

    def set_column(self, i: int, name: str, data: ArrayLike | ChunkedArrayLike) -> TableLike:
        """Replace a column by index."""
        ...

    def cast(self, target_schema: SchemaLike, *, safe: bool | None = None) -> TableLike:
        """Cast the table to the target schema."""
        ...

    def unify_dictionaries(self) -> TableLike:
        """Unify dictionary-encoded columns."""
        ...

    def combine_chunks(self) -> TableLike:
        """Combine chunked columns into contiguous chunks."""
        ...

    def to_reader(self) -> RecordBatchReaderLike:
        """Return a batch reader over the table."""
        ...

    def __getitem__(self, key: str) -> ChunkedArrayLike:
        """Return a column by name."""
        ...


@runtime_checkable
class StructArrayLike(ArrayLike, Protocol):
    """Protocol for pyarrow.StructArray behavior we rely on."""

    @classmethod
    def from_arrays(
        cls,
        arrays: Sequence[ArrayLike | ChunkedArrayLike],
        *,
        names: Sequence[str],
        mask: ArrayLike | None = None,
    ) -> StructArrayLike:
        """Build a StructArray from arrays."""
        ...


@runtime_checkable
class ListArrayLike(ArrayLike, Protocol):
    """Protocol for pyarrow.ListArray behavior we rely on."""

    @classmethod
    def from_arrays(
        cls,
        offsets: ArrayLike | ChunkedArrayLike,
        values: ArrayLike | ChunkedArrayLike,
    ) -> ListArrayLike:
        """Build a ListArray from offsets and values."""
        ...


@runtime_checkable
class ComputeExpression(Protocol):
    """Protocol for pyarrow.compute.Expression behavior we rely on."""

    def __and__(self, other: ComputeExpression) -> ComputeExpression:
        """Return a logical AND expression."""
        ...

    def __or__(self, other: ComputeExpression) -> ComputeExpression:
        """Return a logical OR expression."""
        ...

    def isin(self, values: Sequence[object]) -> ComputeExpression:
        """Return an inclusion expression."""
        ...

    def is_null(self) -> ComputeExpression:
        """Return a null-check expression."""
        ...

    def is_valid(self) -> ComputeExpression:
        """Return a validity-check expression."""
        ...

    def cast(
        self,
        target_type: DataTypeLike | None = None,
        *,
        safe: bool | None = None,
        options: object | None = None,
    ) -> ComputeExpression:
        """Return a casted expression."""
        ...

    def _call(
        self,
        function_name: str,
        arguments: Sequence[ComputeExpression],
        options: object | None = None,
    ) -> ComputeExpression:
        """Call a compute function and return the derived expression."""
        ...


class UdfContext(Protocol):
    """Protocol for pyarrow.compute.UdfContext."""


@runtime_checkable
class DeclarationLike(Protocol):
    """Protocol for pyarrow.acero.Declaration behavior we rely on."""

    def to_table(self, *, use_threads: bool | None = None) -> TableLike:
        """Execute the declaration and return a materialized table."""
        ...

    def to_reader(self, *, use_threads: bool | None = None) -> RecordBatchReaderLike:
        """Execute the declaration and return a streaming reader."""
        ...


type ComputeOperand = ComputeExpression | ArrayLike | ChunkedArrayLike
type ScalarOperand = str | bytes | ScalarLike | ComputeExpression


def ensure_expression(value: object) -> ComputeExpression:
    """Return a compute expression after a runtime guard.

    Returns
    -------
    ComputeExpression
        The validated compute expression.

    Raises
    ------
    TypeError
        If the value is not a compute expression.
    """
    if isinstance(value, ComputeExpression):
        return value
    msg = "Expected a compute expression."
    raise TypeError(msg)


class ComputeModule(Protocol):
    """Protocol for the subset of pyarrow.compute used in this repo."""

    field: Callable[[str], ComputeExpression]
    scalar: Callable[[object], ComputeExpression]
    cast: Callable[..., ArrayLike]
    coalesce: Callable[..., ArrayLike]
    is_null: Callable[[ComputeOperand], ArrayLike]
    is_valid: Callable[[ComputeOperand], ArrayLike]
    and_: Callable[
        [
            ComputeOperand,
            ComputeOperand,
        ],
        ArrayLike,
    ]
    or_: Callable[
        [
            ComputeOperand,
            ComputeOperand,
        ],
        ArrayLike,
    ]
    if_else: Callable[..., ArrayLike]
    fill_null: Callable[..., ArrayLike]
    equal: Callable[..., ArrayLike]
    not_equal: Callable[..., ArrayLike]
    less: Callable[..., ArrayLike]
    less_equal: Callable[..., ArrayLike]
    greater: Callable[..., ArrayLike]
    greater_equal: Callable[..., ArrayLike]
    bit_wise_and: Callable[
        [ArrayLike | ChunkedArrayLike | ScalarLike, ArrayLike | ChunkedArrayLike | ScalarLike],
        ArrayLike,
    ]
    starts_with: Callable[[ComputeOperand, ScalarOperand], ArrayLike]
    ends_with: Callable[[ComputeOperand, ScalarOperand], ArrayLike]
    match_substring: Callable[[ComputeOperand, ScalarOperand], ArrayLike]
    match_substring_regex: Callable[[ComputeOperand, ScalarOperand], ArrayLike]
    utf8_trim: Callable[[ComputeOperand], ArrayLike]
    match_substring_regex: Callable[[ComputeOperand, ScalarOperand], ArrayLike]
    is_in: Callable[..., ArrayLike]
    drop_null: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    invert: Callable[[ComputeOperand], ArrayLike]
    fill_null: Callable[..., ArrayLike]
    value_counts: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    sort_indices: Callable[..., ArrayLike]
    list_parent_indices: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    list_flatten: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    take: Callable[[ArrayLike | ChunkedArrayLike, ArrayLike], ArrayLike]
    make_struct: Callable[..., ArrayLike]
    struct_field: Callable[..., ArrayLike]
    binary_join_element_wise: Callable[..., ArrayLike]
    dictionary_encode: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    unique: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    call_function: Callable[..., ArrayLike | ChunkedArrayLike | ScalarLike]
    get_function: Callable[[str], object]
    register_scalar_function: Callable[..., None]


class AceroModule(Protocol):
    """Protocol for the subset of pyarrow.acero used in this repo."""

    Declaration: Callable[..., DeclarationLike]
    ScanNodeOptions: Callable[..., object]
    FilterNodeOptions: Callable[..., object]
    ProjectNodeOptions: Callable[..., object]
    TableSourceNodeOptions: Callable[..., object]
    OrderByNodeOptions: Callable[..., object]
    AggregateNodeOptions: Callable[..., object]
    HashJoinNodeOptions: Callable[..., object]


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
LargeListArray: type[ListArrayLike] = cast("type[ListArrayLike]", pa.LargeListArray)
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
dictionary: Callable[[DataTypeLike, DataTypeLike], DataTypeLike] = pa.dictionary
list_: Callable[..., DataTypeLike] = pa.list_
list_view: Callable[..., DataTypeLike] = pa.list_view
large_list_view: Callable[..., DataTypeLike] = pa.large_list_view
struct: Callable[..., DataTypeLike] = pa.struct

pc = cast("ComputeModule", _pc)
acero = cast("AceroModule", _acero)

__all__ = [
    "AceroModule",
    "Array",
    "ArrayLike",
    "ArrowInvalid",
    "ArrowTypeError",
    "ChunkedArray",
    "ChunkedArrayLike",
    "ComputeExpression",
    "ComputeModule",
    "DataType",
    "DataTypeLike",
    "DeclarationLike",
    "Field",
    "FieldLike",
    "LargeListArray",
    "ListArray",
    "ListArrayLike",
    "NativeFile",
    "NativeFileLike",
    "RecordBatchReader",
    "RecordBatchReaderLike",
    "Scalar",
    "ScalarLike",
    "Schema",
    "SchemaLike",
    "StructArray",
    "StructArrayLike",
    "Table",
    "TableGroupByLike",
    "TableLike",
    "UdfContext",
    "acero",
    "array",
    "binary",
    "bool_",
    "concat_tables",
    "dictionary",
    "ensure_expression",
    "field",
    "float32",
    "float64",
    "from_numpy_dtype",
    "int8",
    "int16",
    "int32",
    "int64",
    "large_list_view",
    "list_",
    "list_view",
    "memory_map",
    "nulls",
    "pc",
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
