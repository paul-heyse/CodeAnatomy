"""Local protocols for PyArrow compute and acero APIs."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Protocol, runtime_checkable

import pyarrow as pa


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

    def to_table(self, *, use_threads: bool | None = None) -> pa.Table:
        """Execute the declaration and return a materialized table."""
        ...

    def to_reader(self, *, use_threads: bool | None = None) -> pa.RecordBatchReader:
        """Execute the declaration and return a streaming reader."""
        ...


class ComputeModule(Protocol):
    """Protocol for the subset of pyarrow.compute used in this repo."""

    field: Callable[[str], ComputeExpression]
    scalar: Callable[[object], ComputeExpression]
    cast: Callable[..., pa.Array]
    coalesce: Callable[..., pa.Array]
    is_null: Callable[[ComputeExpression | pa.Array | pa.ChunkedArray], pa.Array]
    is_valid: Callable[[ComputeExpression | pa.Array | pa.ChunkedArray], pa.Array]
    and_: Callable[
        [ComputeExpression | pa.Array | pa.ChunkedArray, ComputeExpression | pa.Array | pa.ChunkedArray],
        pa.Array,
    ]
    or_: Callable[
        [ComputeExpression | pa.Array | pa.ChunkedArray, ComputeExpression | pa.Array | pa.ChunkedArray],
        pa.Array,
    ]
    if_else: Callable[..., pa.Array]
    equal: Callable[..., pa.Array]
    not_equal: Callable[..., pa.Array]
    less: Callable[..., pa.Array]
    less_equal: Callable[..., pa.Array]
    greater: Callable[..., pa.Array]
    greater_equal: Callable[..., pa.Array]
    bit_wise_and: Callable[[pa.Array | pa.ChunkedArray | pa.Scalar, pa.Array | pa.ChunkedArray | pa.Scalar], pa.Array]
    starts_with: Callable[[pa.Array | pa.ChunkedArray, str], pa.Array]
    is_in: Callable[..., pa.Array]
    drop_null: Callable[[pa.Array | pa.ChunkedArray], pa.Array]
    invert: Callable[[ComputeExpression | pa.Array | pa.ChunkedArray], pa.Array]
    fill_null: Callable[..., pa.Array]
    value_counts: Callable[[pa.Array | pa.ChunkedArray], pa.Array]
    sort_indices: Callable[..., pa.Array]
    list_parent_indices: Callable[[pa.Array | pa.ChunkedArray], pa.Array]
    list_flatten: Callable[[pa.Array | pa.ChunkedArray], pa.Array]
    take: Callable[[pa.Array | pa.ChunkedArray, pa.Array], pa.Array]
    binary_join_element_wise: Callable[..., pa.Array]
    call_function: Callable[..., pa.Array | pa.ChunkedArray | pa.Scalar]
    get_function: Callable[[str], object]
    register_scalar_function: Callable[..., None]
