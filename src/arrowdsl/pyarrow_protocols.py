"""Local protocols for PyArrow compute and acero APIs."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from types import TracebackType
from typing import Protocol, Self, runtime_checkable


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

    def select(self, names: Sequence[str]) -> TableLike:
        """Select columns by name."""
        ...

    def set_column(self, i: int, name: str, data: ArrayLike | ChunkedArrayLike) -> TableLike:
        """Replace a column by index."""
        ...

    def cast(self, target_schema: SchemaLike, *, safe: bool | None = None) -> TableLike:
        """Cast the table to the target schema."""
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

    def __hash__(self) -> int:
        """Return a hash for the expression."""
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


class UdfContext(Protocol):
    """Protocol for pyarrow.compute.UdfContext."""


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


@runtime_checkable
class DeclarationLike(Protocol):
    """Protocol for pyarrow.acero.Declaration behavior we rely on."""

    def to_table(self, *, use_threads: bool | None = None) -> TableLike:
        """Execute the declaration and return a materialized table."""
        ...

    def to_reader(self, *, use_threads: bool | None = None) -> RecordBatchReaderLike:
        """Execute the declaration and return a streaming reader."""
        ...


class ComputeModule(Protocol):
    """Protocol for the subset of pyarrow.compute used in this repo."""

    field: Callable[[str], ComputeExpression]
    scalar: Callable[[object], ComputeExpression]
    cast: Callable[..., ArrayLike]
    coalesce: Callable[..., ArrayLike]
    is_null: Callable[[ComputeExpression | ArrayLike | ChunkedArrayLike], ArrayLike]
    is_valid: Callable[[ComputeExpression | ArrayLike | ChunkedArrayLike], ArrayLike]
    and_: Callable[
        [
            ComputeExpression | ArrayLike | ChunkedArrayLike,
            ComputeExpression | ArrayLike | ChunkedArrayLike,
        ],
        ArrayLike,
    ]
    or_: Callable[
        [
            ComputeExpression | ArrayLike | ChunkedArrayLike,
            ComputeExpression | ArrayLike | ChunkedArrayLike,
        ],
        ArrayLike,
    ]
    if_else: Callable[..., ArrayLike]
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
    starts_with: Callable[[ArrayLike | ChunkedArrayLike, str], ArrayLike]
    is_in: Callable[..., ArrayLike]
    drop_null: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    invert: Callable[[ComputeExpression | ArrayLike | ChunkedArrayLike], ArrayLike]
    fill_null: Callable[..., ArrayLike]
    value_counts: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    sort_indices: Callable[..., ArrayLike]
    list_parent_indices: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    list_flatten: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    take: Callable[[ArrayLike | ChunkedArrayLike, ArrayLike], ArrayLike]
    binary_join_element_wise: Callable[..., ArrayLike]
    dictionary_encode: Callable[[ArrayLike | ChunkedArrayLike], ArrayLike]
    call_function: Callable[..., ArrayLike | ChunkedArrayLike | ScalarLike]
    get_function: Callable[[str], object]
    register_scalar_function: Callable[..., None]
