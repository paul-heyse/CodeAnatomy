"""Arrow interop shims and protocol definitions."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import suppress
from types import TracebackType
from typing import Protocol, Self, cast, runtime_checkable

import arro3.core as ac
import pyarrow as pa
import pyarrow.compute as _pc

logger = logging.getLogger(__name__)


@runtime_checkable
class DataTypeLike(Protocol):
    """Protocol for pyarrow.DataType."""


def ensure_arrow_dtype(dtype: DataTypeLike | object) -> pa.DataType:
    """Return a concrete pyarrow.DataType for known spec types.

    Args:
        dtype: Description.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    if isinstance(dtype, pa.DataType):
        logger.debug("Arrow dtype already concrete: %s", dtype)
        return dtype
    try:
        from schema_spec.arrow_types import ArrowTypeBase as _ArrowTypeBase
        from schema_spec.arrow_types import arrow_type_to_pyarrow
    except ImportError as exc:
        msg = "Arrow type conversion requires schema_spec.arrow_types."
        raise TypeError(msg) from exc
    if isinstance(dtype, _ArrowTypeBase):
        resolved = arrow_type_to_pyarrow(dtype)
        logger.debug("Converted schema-spec dtype %s to %s", dtype, resolved)
        return resolved
    msg = f"Expected pyarrow.DataType, got {type(dtype)!r}."
    logger.warning("Failed dtype coercion: %s", msg)
    raise TypeError(msg)


def coerce_arrow_schema(value: object) -> pa.Schema | None:
    """Return a ``pyarrow.Schema`` when the input can be coerced.

    Parameters
    ----------
    value
        Schema-like value to coerce.

    Returns:
    -------
    pa.Schema | None
        Coerced schema, or ``None`` when conversion is not possible.
    """
    for candidate in _schema_coercion_candidates(value):
        resolved = _coerce_schema_candidate(candidate)
        if resolved is not None:
            logger.debug("Coerced Arrow schema from %s", type(candidate).__name__)
            return resolved
    logger.debug("Could not coerce Arrow schema from %s", type(value).__name__)
    return None


def arrow_schema_from_dfschema(schema: object) -> pa.Schema | None:
    """Return an Arrow schema from a DataFusion schema-like object."""
    return coerce_arrow_schema(schema)


def arrow_schema_from_df(df: object) -> pa.Schema:
    """Extract an Arrow schema from a DataFrame-like object.

    Returns:
    -------
    pa.Schema
        Coerced Arrow schema from the DataFrame schema surface.

    Raises:
        TypeError: If the DataFrame schema cannot be coerced to a ``pyarrow.Schema``.
    """
    schema_attr = getattr(df, "schema", None)
    schema_value = schema_attr() if callable(schema_attr) else schema_attr
    resolved = arrow_schema_from_dfschema(schema_value)
    if resolved is not None:
        return resolved
    msg = "Failed to resolve DataFusion schema."
    logger.warning("DataFrame schema coercion failed for %s", type(df).__name__)
    raise TypeError(msg)


def _schema_coercion_candidates(value: object) -> tuple[object, ...]:
    candidates: list[object] = [value]
    for method_name in ("to_arrow", "to_pyarrow"):
        method = getattr(value, method_name, None)
        if not callable(method):
            continue
        with suppress(TypeError, ValueError):
            candidates.append(method())
    return tuple(candidates)


def _coerce_schema_candidate(value: object) -> pa.Schema | None:
    if isinstance(value, pa.Schema):
        return value
    with suppress(TypeError, ValueError):
        candidate = pa.schema(value)
        if isinstance(candidate, pa.Schema):
            return candidate
    return None


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

        Returns:
        -------
        bool
            True when the subclass advertises the expected reader attributes.
        """
        if cls is not RecordBatchReaderLike:
            return NotImplemented
        required = ("schema", "read_all", "__iter__")
        return all(hasattr(subclass, name) for name in required)

    def read_all(self) -> TableLike:
        """Read all batches into a table."""
        ...

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        """Iterate over record batches."""
        ...

    def arrow_c_stream(
        self,
        requested_schema: pa.Schema | None = None,
    ) -> object:
        """Export record batches via Arrow C stream."""
        ...

    __arrow_c_stream__ = arrow_c_stream


@runtime_checkable
class ArrowStreamExportable(Protocol):
    """Protocol for objects exposing the Arrow C stream export surface."""

    def arrow_c_stream(
        self,
        requested_schema: pa.Schema | None = None,
    ) -> object:
        """Return an Arrow C stream capsule."""
        ...

    __arrow_c_stream__ = arrow_c_stream


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

        Returns:
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
            "to_pydict",
            "filter",
            "take",
            "select",
        )
        return all(hasattr(subclass, name) for name in required)

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

    def to_pydict(self) -> dict[str, list[object]]:
        """Return the table as column-wise Python lists."""
        ...

    def to_batches(self) -> Sequence[pa.RecordBatch]:
        """Return the table as a sequence of record batches."""
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


type ComputeOperand = ComputeExpression | ArrayLike | ChunkedArrayLike
type ScalarOperand = str | bytes | ScalarLike | ComputeExpression


def ensure_expression(value: object) -> ComputeExpression:
    """Return a compute expression after a runtime guard.

    Args:
        value: Description.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    if isinstance(value, ComputeExpression):
        return value
    msg = "Expected a compute expression."
    raise TypeError(msg)


def call_expression_function(
    function_name: str,
    arguments: Sequence[ComputeExpression],
    options: object | None = None,
) -> ComputeExpression:
    """Return a compute expression for a registered function call.

    Args:
        function_name: Description.
        arguments: Description.
        options: Description.

    Raises:
        AttributeError: If the operation cannot be completed.
        ValueError: If the operation cannot be completed.
    """
    if not arguments:
        msg = "call_expression_function requires at least one argument."
        raise ValueError(msg)
    call = cast(
        "Callable[[str, Sequence[ComputeExpression], object | None], ComputeExpression]",
        getattr(arguments[0], "_call", None),
    )
    if call is None:
        msg = "ComputeExpression does not support function calls."
        raise AttributeError(msg)
    return call(function_name, arguments, options)


def table_from_dataframe_protocol(obj: object) -> TableLike:
    """Return a pyarrow.Table from the dataframe interchange protocol.

    Args:
        obj: Description.

    Raises:
        RuntimeError: If the operation cannot be completed.
        TypeError: If the operation cannot be completed.
    """
    interchange = getattr(pa, "interchange", None)
    if interchange is None:
        msg = "pyarrow.interchange is unavailable."
        raise RuntimeError(msg)
    if not hasattr(obj, "__dataframe__"):
        msg = "Object does not implement the dataframe interchange protocol."
        raise TypeError(msg)
    return interchange.from_dataframe(obj)


def reader_from_arrow_stream(
    obj: object,
    *,
    requested_schema: pa.Schema | None = None,
) -> RecordBatchReaderLike:
    """Return a RecordBatchReader from an Arrow C stream provider.

    Args:
        obj: Description.
        requested_schema: Description.

    Raises:
        TypeError: If the operation cannot be completed.
        ValueError: If the operation cannot be completed.
    """
    stream_provider = getattr(obj, "__arrow_c_stream__", None)
    if not callable(stream_provider):
        msg = "Object does not expose __arrow_c_stream__."
        raise TypeError(msg)
    if requested_schema is not None:
        try:
            capsule = stream_provider(requested_schema=requested_schema)
        except TypeError as exc:
            msg = "Schema negotiation is not supported"
            raise ValueError(msg) from exc
        importer = getattr(pa.RecordBatchReader, "_import_from_c", None)
        if not callable(importer):
            msg = "Schema negotiation is not supported"
            raise ValueError(msg)
        return cast("RecordBatchReaderLike", importer(capsule))
    return cast("RecordBatchReaderLike", pa.RecordBatchReader.from_stream(obj))


def table_from_arrow_c_array(
    obj: object,
    *,
    name: str = "value",
    requested_schema: pa.Schema | None = None,
) -> TableLike:
    """Return a table from an Arrow C array provider.

    Args:
        obj: Description.
        name: Description.
        requested_schema: Description.

    Raises:
        TypeError: If the operation cannot be completed.
        ValueError: If the operation cannot be completed.
    """
    array_provider = getattr(obj, "__arrow_c_array__", None)
    if not callable(array_provider):
        msg = "Object does not expose __arrow_c_array__."
        raise TypeError(msg)
    if requested_schema is not None:
        try:
            capsule = array_provider(requested_schema=requested_schema)
        except TypeError:
            capsule = array_provider()
    else:
        capsule = array_provider()
    schema_capsule = None
    if isinstance(capsule, tuple) and len(capsule) == ARROW_C_ARRAY_TUPLE_LEN:
        schema_capsule, array_capsule = capsule
    else:
        array_capsule = capsule
        schema_provider = getattr(obj, "__arrow_c_schema__", None)
        if callable(schema_provider):
            schema_capsule = schema_provider()
    if schema_capsule is None:
        msg = "Arrow C array providers must supply a schema capsule."
        raise ValueError(msg)
    # NOTE: Arrow C array providers are single-consumption objects.
    array = pa.array(ac.Array.from_arrow_pycapsule(schema_capsule, array_capsule))
    return pa.table({name: array})


def coerce_table_like(
    obj: object,
    *,
    requested_schema: pa.Schema | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Coerce Arrow-like inputs into table or reader representations.

    Args:
        obj: Arrow-like object to coerce.
        requested_schema: Optional schema passed to C stream/array providers.

    Returns:
        TableLike | RecordBatchReaderLike: Result.

    Raises:
        TypeError: If the input cannot be coerced into a table-like object.
    """
    if isinstance(obj, (TableLike, RecordBatchReaderLike)):
        return obj
    if hasattr(obj, "__arrow_c_stream__"):
        return reader_from_arrow_stream(obj, requested_schema=requested_schema)
    if hasattr(obj, "__arrow_c_array__"):
        return table_from_arrow_c_array(obj, requested_schema=requested_schema)
    if hasattr(obj, "__dataframe__"):
        return table_from_dataframe_protocol(obj)
    msg = "Unsupported Arrow-like input; provide a Table or RecordBatchReader."
    raise TypeError(msg)


def as_reader(
    obj: object,
    *,
    requested_schema: pa.Schema | None = None,
) -> RecordBatchReaderLike:
    """Return a RecordBatchReader for Arrow table/stream-compatible inputs.

    Args:
        obj: Description.
        requested_schema: Description.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    if isinstance(obj, RecordBatchReaderLike):
        return obj
    if isinstance(obj, TableLike):
        reader = obj.to_reader()
        if isinstance(reader, RecordBatchReaderLike):
            return reader
        msg = "Table-like object returned a non-reader result from to_reader()."
        raise TypeError(msg)
    if isinstance(obj, ArrowStreamExportable) or hasattr(obj, "__arrow_c_stream__"):
        return reader_from_arrow_stream(obj, requested_schema=requested_schema)
    to_reader = getattr(obj, "to_reader", None)
    if callable(to_reader):
        reader = to_reader()
        if isinstance(reader, RecordBatchReaderLike):
            return reader
    msg = "Object is not reader-adaptable; expected table, reader, or Arrow stream exporter."
    raise TypeError(msg)


def concat_readers(readers: Sequence[RecordBatchReaderLike]) -> RecordBatchReaderLike:
    """Return a RecordBatchReader that concatenates reader batches in order.

    Args:
        readers: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    if not readers:
        msg = "concat_readers requires at least one reader."
        raise ValueError(msg)
    first = cast("pa.RecordBatchReader", readers[0])
    schema = first.schema

    def _batches() -> Iterator[pa.RecordBatch]:
        for reader in readers:
            resolved = cast("pa.RecordBatchReader", reader)
            if resolved.schema != schema:
                msg = "RecordBatchReader schema mismatch in concat_readers."
                raise ValueError(msg)
            yield from resolved

    return cast("RecordBatchReaderLike", pa.RecordBatchReader.from_batches(schema, _batches()))


def empty_table_for_schema(schema: pa.Schema) -> pa.Table:
    """Return an empty table preserving schema metadata and complex types.

    Returns:
    -------
    pyarrow.Table
        Empty table with the provided schema.
    """
    return pa.Table.from_batches([], schema=schema)


class ComputeModule(Protocol):
    """Protocol for the subset of pyarrow.compute used in this repo."""

    list_functions: Callable[[], Sequence[str]]
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
    bit_wise_and: Callable[[ComputeOperand | ScalarLike, ComputeOperand | ScalarLike], ArrayLike]
    starts_with: Callable[[ComputeOperand, ScalarOperand], ArrayLike]
    ends_with: Callable[[ComputeOperand, ScalarOperand], ArrayLike]
    match_substring: Callable[[ComputeOperand, ScalarOperand], ArrayLike]
    match_substring_regex: Callable[[ComputeOperand, ScalarOperand], ArrayLike]
    split_pattern: Callable[..., ArrayLike]
    list_element: Callable[..., ArrayLike]
    utf8_trim: Callable[[ComputeOperand], ArrayLike]
    utf8_trim_whitespace: Callable[[ComputeOperand], ArrayLike]
    utf8_length: Callable[[ComputeOperand], ArrayLike]
    utf8_upper: Callable[[ComputeOperand], ArrayLike]
    is_in: Callable[..., ArrayLike]
    case_when: Callable[..., ArrayLike]
    filter: Callable[[ComputeOperand, ComputeOperand], ArrayLike]
    drop_null: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    invert: Callable[[ComputeOperand], ArrayLike]
    value_counts: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    sort_indices: Callable[..., ArrayLike]
    list_parent_indices: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    list_flatten: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    list_value_length: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    cumulative_sum: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    subtract: Callable[[ComputeOperand, ComputeOperand], ArrayLike]
    indices_nonzero: Callable[[ComputeOperand], ArrayLike]
    take: Callable[[ArrayLike | ChunkedArrayLike, ArrayLike], ArrayLike]
    any: Callable[[ComputeOperand], ScalarLike]
    make_struct: Callable[..., ArrayLike]
    struct_field: Callable[..., ArrayLike]
    binary_join_element_wise: Callable[..., ArrayLike]
    dictionary_encode: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    unique: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    call_function: Callable[..., ArrayLike | ChunkedArrayLike | ScalarLike]
    get_function: Callable[[str], object]
    register_scalar_function: Callable[..., None]
    SetLookupOptions: Callable[..., object]


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

__all__ = [
    "Array",
    "ArrayLike",
    "ArrowInvalid",
    "ArrowStreamExportable",
    "ArrowTypeError",
    "ChunkedArray",
    "ChunkedArrayLike",
    "ComputeExpression",
    "ComputeModule",
    "DataType",
    "DataTypeLike",
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
    "array",
    "arrow_schema_from_df",
    "arrow_schema_from_dfschema",
    "as_reader",
    "binary",
    "bool_",
    "call_expression_function",
    "coerce_arrow_schema",
    "coerce_table_like",
    "concat_readers",
    "concat_tables",
    "dictionary",
    "empty_table_for_schema",
    "ensure_arrow_dtype",
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
    "reader_from_arrow_stream",
    "scalar",
    "schema",
    "set_cpu_count",
    "set_io_thread_count",
    "string",
    "struct",
    "table",
    "table_from_arrow_c_array",
    "table_from_dataframe_protocol",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "unify_schemas",
]
ARROW_C_ARRAY_TUPLE_LEN = 2
