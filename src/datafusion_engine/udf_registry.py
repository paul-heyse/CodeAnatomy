"""Shared DataFusion UDF registry helpers."""

from __future__ import annotations

import warnings
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from itertools import repeat
from typing import TYPE_CHECKING, Literal, Protocol, TypeVar, cast
from weakref import WeakSet

import pyarrow as pa
import pyarrow.dataset as ds
from datafusion import SessionContext, udaf, udf, udtf, udwf
from datafusion.catalog import Table
from datafusion.user_defined import (
    Accumulator,
    AggregateUDF,
    ScalarUDF,
    TableFunction,
    WindowEvaluator,
    WindowUDF,
)

if TYPE_CHECKING:
    from datafusion.user_defined import ScalarUDFExportable
else:

    class ScalarUDFExportable(Protocol):
        """Protocol for DataFusion PyCapsule exportable UDFs."""

        __datafusion_scalar_udf__: object


from arrowdsl.core.array_iter import iter_array_values
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike
from datafusion_engine.builtin_function_map import is_builtin_function
from datafusion_engine.hash_utils import hash64_from_text, hash128_from_text
from datafusion_engine.udf_catalog import (
    UdfCatalog,
    UdfPerformancePolicy,
    UdfTierPolicy,
    check_udf_allowed,
    create_default_catalog,
    create_strict_catalog,
)

_KERNEL_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_AGGREGATE_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_WINDOW_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_TABLE_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_PYCAPSULE_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_PYCAPSULE_UDF_SPECS: dict[str, DataFusionPycapsuleUdfEntry] = {}

DataFusionUdfKind = Literal["scalar", "aggregate", "window", "table"]
UdfTier = Literal["builtin", "pyarrow", "pandas", "python"]
UDF_TIER_PRIORITY: tuple[UdfTier, ...] = ("builtin", "pyarrow", "pandas", "python")

T_Udf = TypeVar("T_Udf", ScalarUDF, AggregateUDF, WindowUDF, TableFunction)

ENC_UTF8 = 1
ENC_UTF16 = 2
ENC_UTF32 = 3


def _pycapsule_entries() -> tuple[DataFusionPycapsuleUdfEntry, ...]:
    return tuple(_PYCAPSULE_UDF_SPECS[name] for name in sorted(_PYCAPSULE_UDF_SPECS))


@dataclass(frozen=True)
class DataFusionUdfSpec:
    """Specification for a DataFusion UDF entry."""

    func_id: str
    engine_name: str
    kind: DataFusionUdfKind
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    state_type: pa.DataType | None = None
    volatility: str = "stable"
    arg_names: tuple[str, ...] | None = None
    catalog: str | None = None
    database: str | None = None
    capsule_id: str | None = None
    udf_tier: UdfTier = "python"
    rewrite_tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate UDF tier values.

        Raises
        ------
        ValueError
            Raised when the tier is not supported.
        """
        if self.udf_tier not in UDF_TIER_PRIORITY:
            msg = f"Unsupported UDF tier: {self.udf_tier!r}."
            raise ValueError(msg)


@dataclass(frozen=True)
class DataFusionUdfSnapshot:
    """Snapshot of UDF registrations for diagnostics."""

    scalar: tuple[str, ...] = ()
    aggregate: tuple[str, ...] = ()
    window: tuple[str, ...] = ()
    table: tuple[str, ...] = ()
    capsule_udfs: tuple[DataFusionUdfCapsuleEntry, ...] = ()

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for diagnostics.

        Returns
        -------
        dict[str, object]
            JSON-ready payload with UDF names.
        """
        return {
            "scalar": list(self.scalar),
            "aggregate": list(self.aggregate),
            "window": list(self.window),
            "table": list(self.table),
            "pycapsule_udfs": [entry.payload() for entry in self.capsule_udfs],
        }


@dataclass(frozen=True)
class DataFusionUdfCapsuleEntry:
    """PyCapsule-backed UDF entry for diagnostics."""

    name: str
    kind: DataFusionUdfKind
    capsule_id: str
    udf_tier: UdfTier

    def payload(self) -> Mapping[str, object]:
        """Return a JSON-ready payload for the entry.

        Returns
        -------
        Mapping[str, object]
            JSON-ready UDF capsule payload.
        """
        return {
            "name": self.name,
            "kind": self.kind,
            "capsule_id": self.capsule_id,
            "udf_tier": self.udf_tier,
        }


@dataclass(frozen=True)
class DataFusionPycapsuleUdfEntry:
    """Registration metadata for PyCapsule-backed UDFs."""

    spec: DataFusionUdfSpec
    capsule: ScalarUDFExportable

    def capsule_id(self) -> str:
        """Return the capsule identifier for diagnostics.

        Returns
        -------
        str
            Capsule identifier.
        """
        return self.spec.capsule_id or udf_capsule_id(self.capsule)


def _stable_hash64(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    if isinstance(values, pa.Scalar):
        value = values.as_py()
        if value is None:
            return pa.scalar(None, type=pa.int64())
        hashed = hash64_from_text(str(value))
        return pa.scalar(hashed, type=pa.int64())
    out = [
        hash64_from_text(str(value)) if value is not None else None
        for value in iter_array_values(values)
    ]
    return pa.array(out, type=pa.int64())


def _stable_hash128(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    if isinstance(values, pa.Scalar):
        value = values.as_py()
        if value is None:
            return pa.scalar(None, type=pa.string())
        hashed = hash128_from_text(str(value))
        return pa.scalar(hashed, type=pa.string())
    out = [
        hash128_from_text(str(value)) if value is not None else None
        for value in iter_array_values(values)
    ]
    return pa.array(out, type=pa.string())


def _prefixed_hash64(
    prefix_values: pa.Array | pa.ChunkedArray | pa.Scalar,
    value_values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    if isinstance(prefix_values, pa.Scalar) and isinstance(value_values, pa.Scalar):
        prefix = prefix_values.as_py()
        value = value_values.as_py()
        if prefix is None or value is None:
            return pa.scalar(None, type=pa.string())
        hashed = hash64_from_text(str(value))
        return pa.scalar(f"{prefix}:{hashed}", type=pa.string())
    length = len(prefix_values) if not isinstance(prefix_values, pa.Scalar) else len(value_values)
    prefix_iter = (
        repeat(prefix_values.as_py(), length)
        if isinstance(prefix_values, pa.Scalar)
        else iter_array_values(prefix_values)
    )
    value_iter = (
        repeat(value_values.as_py(), length)
        if isinstance(value_values, pa.Scalar)
        else iter_array_values(value_values)
    )
    out: list[str | None] = []
    for prefix, value in zip(prefix_iter, value_iter, strict=True):
        if prefix is None or value is None:
            out.append(None)
            continue
        hashed = hash64_from_text(str(value))
        out.append(f"{prefix}:{hashed}")
    return pa.array(out, type=pa.string())


def _stable_id(
    prefix_values: pa.Array | pa.ChunkedArray | pa.Scalar,
    value_values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    if isinstance(prefix_values, pa.Scalar) and isinstance(value_values, pa.Scalar):
        prefix = prefix_values.as_py()
        value = value_values.as_py()
        if prefix is None or value is None:
            return pa.scalar(None, type=pa.string())
        hashed = hash128_from_text(str(value))
        return pa.scalar(f"{prefix}:{hashed}", type=pa.string())
    length = len(prefix_values) if not isinstance(prefix_values, pa.Scalar) else len(value_values)
    prefix_iter = (
        repeat(prefix_values.as_py(), length)
        if isinstance(prefix_values, pa.Scalar)
        else iter_array_values(prefix_values)
    )
    value_iter = (
        repeat(value_values.as_py(), length)
        if isinstance(value_values, pa.Scalar)
        else iter_array_values(value_values)
    )
    out: list[str | None] = []
    for prefix, value in zip(prefix_iter, value_iter, strict=True):
        if prefix is None or value is None:
            out.append(None)
            continue
        hashed = hash128_from_text(str(value))
        out.append(f"{prefix}:{hashed}")
    return pa.array(out, type=pa.string())


class _ListUniqueAccumulator(Accumulator):
    """Aggregate unique string values into a list."""

    def __init__(self) -> None:
        self._values: list[str] = []
        self._seen: set[str] = set()

    def update(self, *values: object) -> None:
        """Update the accumulator with a batch of values."""
        if not values or not isinstance(values[0], (ArrayLike, ChunkedArrayLike)):
            return
        array = values[0]
        for value in iter_array_values(array):
            if value is None:
                continue
            text = str(value)
            if text in self._seen:
                continue
            self._seen.add(text)
            self._values.append(text)

    def merge(self, states: list[pa.Array]) -> None:
        """Merge partial accumulator states."""
        if not states:
            return
        for entry in iter_array_values(states[0]):
            if entry is None:
                continue
            if isinstance(entry, (ArrayLike, ChunkedArrayLike)):
                entry_values = iter_array_values(entry)
            elif isinstance(entry, (list, tuple)):
                entry_values = entry
            else:
                continue
            for value in entry_values:
                text = str(value)
                if text in self._seen:
                    continue
                self._seen.add(text)
                self._values.append(text)

    def state(self) -> list[pa.Scalar]:
        """Return the intermediate state for this accumulator.

        Returns
        -------
        list[pyarrow.Scalar]
            Scalar list with current accumulator values.
        """
        return [pa.scalar(self._values, type=pa.list_(pa.string()))]

    def evaluate(self) -> pa.Scalar:
        """Return the final aggregated list.

        Returns
        -------
        pyarrow.Scalar
            Scalar list containing the aggregated values.
        """
        return pa.scalar(self._values, type=pa.list_(pa.string()))


class _FirstValueAccumulator(Accumulator):
    """Aggregate first non-null value in group."""

    def __init__(self) -> None:
        self._first_value: object = None
        self._has_value: bool = False

    def update(self, *values: object) -> None:
        """Update the accumulator with a batch of values."""
        if (
            self._has_value
            or not values
            or not isinstance(values[0], (ArrayLike, ChunkedArrayLike))
        ):
            return
        array = values[0]
        for value in iter_array_values(array):
            if value is not None:
                self._first_value = value
                self._has_value = True
                break

    def merge(self, states: list[pa.Array]) -> None:
        """Merge partial accumulator states."""
        if self._has_value or not states:
            return
        for entry in iter_array_values(states[0]):
            if entry is not None:
                self._first_value = entry
                self._has_value = True
                break

    def state(self) -> list[pa.Scalar]:
        """Return the intermediate state for this accumulator.

        Returns
        -------
        list[pyarrow.Scalar]
            Scalar with current first value.
        """
        return [pa.scalar(self._first_value)]

    def evaluate(self) -> pa.Scalar:
        """Return the final first value.

        Returns
        -------
        pyarrow.Scalar
            First non-null value in the group.
        """
        return pa.scalar(self._first_value)


class _LastValueAccumulator(Accumulator):
    """Aggregate last non-null value in group."""

    def __init__(self) -> None:
        self._last_value: object = None

    def update(self, *values: object) -> None:
        """Update the accumulator with a batch of values."""
        if not values or not isinstance(values[0], (ArrayLike, ChunkedArrayLike)):
            return
        array = values[0]
        for value in iter_array_values(array):
            if value is not None:
                self._last_value = value

    def merge(self, states: list[pa.Array]) -> None:
        """Merge partial accumulator states."""
        if not states:
            return
        for entry in iter_array_values(states[0]):
            if entry is not None:
                self._last_value = entry

    def state(self) -> list[pa.Scalar]:
        """Return the intermediate state for this accumulator.

        Returns
        -------
        list[pyarrow.Scalar]
            Scalar with current last value.
        """
        return [pa.scalar(self._last_value)]

    def evaluate(self) -> pa.Scalar:
        """Return the final last value.

        Returns
        -------
        pyarrow.Scalar
            Last non-null value in the group.
        """
        return pa.scalar(self._last_value)


class _CountDistinctAccumulator(Accumulator):
    """Count distinct non-null values in group."""

    def __init__(self) -> None:
        self._seen: set[object] = set()

    def update(self, *values: object) -> None:
        """Update the accumulator with a batch of values."""
        if not values or not isinstance(values[0], (ArrayLike, ChunkedArrayLike)):
            return
        array = values[0]
        for value in iter_array_values(array):
            if value is not None:
                # For hashability, convert unhashable types to string representation
                try:
                    self._seen.add(value)
                except TypeError:
                    self._seen.add(str(value))

    def merge(self, states: list[pa.Array]) -> None:
        """Merge partial accumulator states."""
        if not states:
            return
        for value in _iter_distinct_merge_values(states[0]):
            try:
                self._seen.add(value)
            except TypeError:
                self._seen.add(str(value))

    def state(self) -> list[pa.Scalar]:
        """Return the intermediate state for this accumulator.

        Returns
        -------
        list[pyarrow.Scalar]
            Scalar list with current seen values.
        """
        return [pa.scalar(list(self._seen))]

    def evaluate(self) -> pa.Scalar:
        """Return the count of distinct values.

        Returns
        -------
        pyarrow.Scalar
            Count of distinct non-null values in the group.
        """
        return pa.scalar(len(self._seen), type=pa.int64())


class _StringAggAccumulator(Accumulator):
    """Concatenate strings with separator."""

    def __init__(self) -> None:
        self._values: list[str] = []
        self._separator: str = ","

    def update(self, *values: object) -> None:
        """Update the accumulator with a batch of values."""
        if not values or not isinstance(values[0], (ArrayLike, ChunkedArrayLike)):
            return
        array = values[0]
        # Second argument is separator if provided
        if len(values) > 1 and isinstance(values[1], (ArrayLike, ChunkedArrayLike)):
            sep_array = values[1]
            for sep in iter_array_values(sep_array):
                if sep is not None:
                    self._separator = str(sep)
                    break
        for value in iter_array_values(array):
            if value is not None:
                self._values.append(str(value))

    def merge(self, states: list[pa.Array]) -> None:
        """Merge partial accumulator states."""
        if not states:
            return
        for entry in iter_array_values(states[0]):
            if entry is None:
                continue
            if isinstance(entry, (ArrayLike, ChunkedArrayLike)):
                for value in iter_array_values(entry):
                    if value is not None:
                        self._values.append(str(value))
            elif isinstance(entry, (list, tuple)):
                for value in entry:
                    if value is not None:
                        self._values.append(str(value))

    def state(self) -> list[pa.Scalar]:
        """Return the intermediate state for this accumulator.

        Returns
        -------
        list[pyarrow.Scalar]
            Scalar list with current values.
        """
        return [pa.scalar(self._values, type=pa.list_(pa.string()))]

    def evaluate(self) -> pa.Scalar:
        """Return the concatenated string.

        Returns
        -------
        pyarrow.Scalar
            Concatenated string with separator.
        """
        return pa.scalar(self._separator.join(self._values), type=pa.string())


class _RowIndexEvaluator(WindowEvaluator):
    """Window evaluator that returns 1-based row indices."""

    @staticmethod
    def evaluate_all(values: list[pa.Array], num_rows: int) -> pa.Array:
        """Return 1-based indices for every row in the partition.

        Returns
        -------
        pyarrow.Array
            1-based row indices for the window partition.
        """
        _ = values
        return pa.array(range(1, num_rows + 1), type=pa.int64())


class _RunningCountEvaluator(WindowEvaluator):
    """Window evaluator that returns running count within partition."""

    @staticmethod
    def evaluate_all(values: list[pa.Array], num_rows: int) -> pa.Array:
        """Return running count for every row in the partition.

        Returns
        -------
        pyarrow.Array
            Running count from 1 to num_rows.
        """
        _ = values
        return pa.array(range(1, num_rows + 1), type=pa.int64())


class _RunningTotalEvaluator(WindowEvaluator):
    """Window evaluator that returns running sum within partition."""

    @staticmethod
    def evaluate_all(values: list[pa.Array], num_rows: int) -> pa.Array:
        """Return running sum for every row in the partition.

        Parameters
        ----------
        values:
            List of input arrays (first array contains values to sum).
        num_rows:
            Number of rows in the partition (unused).

        Returns
        -------
        pyarrow.Array
            Running sum values.
        """
        _ = num_rows
        if not values or len(values[0]) == 0:
            return pa.array([], type=pa.float64())

        input_array = values[0]
        running_sum = 0.0
        result: list[float | None] = []

        for value in iter_array_values(input_array):
            if value is None:
                result.append(None)
            else:
                # Safely convert to float - handle various numeric types
                try:
                    if isinstance(value, (int, float, str)):
                        numeric_value = float(value)
                    else:
                        # For any other type, try to get its numeric value
                        numeric_value = float(str(value))
                    running_sum += numeric_value
                    result.append(running_sum)
                except (TypeError, ValueError):
                    result.append(None)

        return pa.array(result, type=pa.float64())


class _RowNumberEvaluator(WindowEvaluator):
    """Window evaluator that returns row number within partition."""

    @staticmethod
    def evaluate_all(values: list[pa.Array], num_rows: int) -> pa.Array:
        """Return row numbers for every row in the partition.

        Returns
        -------
        pyarrow.Array
            Row numbers from 1 to num_rows.
        """
        _ = values
        return pa.array(range(1, num_rows + 1), type=pa.int64())


class _LagEvaluator(WindowEvaluator):
    """Window evaluator that accesses previous row value."""

    @staticmethod
    def evaluate_all(values: list[pa.Array], num_rows: int) -> pa.Array:
        """Return lagged values for every row in the partition.

        Parameters
        ----------
        values:
            List of input arrays (first array contains values to lag).
        num_rows:
            Number of rows in the partition (unused).

        Returns
        -------
        pyarrow.Array
            Lagged values (first row is null by default).
        """
        _ = num_rows
        if not values or len(values[0]) == 0:
            return pa.array([], type=pa.null())

        input_array = values[0]
        result: list[object] = [None]  # First row has no previous value

        for i in range(len(input_array) - 1):
            value = input_array[i].as_py() if hasattr(input_array[i], "as_py") else input_array[i]
            result.append(value)

        return pa.array(result, type=input_array.type)


class _LeadEvaluator(WindowEvaluator):
    """Window evaluator that accesses next row value."""

    @staticmethod
    def evaluate_all(values: list[pa.Array], num_rows: int) -> pa.Array:
        """Return lead values for every row in the partition.

        Parameters
        ----------
        values:
            List of input arrays (first array contains values to lead).
        num_rows:
            Number of rows in the partition (unused).

        Returns
        -------
        pyarrow.Array
            Lead values (last row is null by default).
        """
        _ = num_rows
        if not values or len(values[0]) == 0:
            return pa.array([], type=pa.null())

        input_array = values[0]
        result: list[object] = []

        for i in range(1, len(input_array)):
            value = input_array[i].as_py() if hasattr(input_array[i], "as_py") else input_array[i]
            result.append(value)

        result.append(None)  # Last row has no next value

        return pa.array(result, type=input_array.type)


def _literal_int(expr: object, *, label: str) -> int:
    """Return a literal integer value from a DataFusion expression.

    Raises
    ------
    TypeError
        Raised when the expression does not resolve to an integer.

    Returns
    -------
    int
        Literal integer value.
    """
    to_python = getattr(expr, "python_value", None)
    value = to_python() if callable(to_python) else expr
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"range_table {label} literal must be an int."
        raise TypeError(msg)
    return value


def _iter_distinct_merge_values(states: pa.Array) -> Iterable[object]:
    for entry in iter_array_values(states):
        if entry is None:
            continue
        if isinstance(entry, (ArrayLike, ChunkedArrayLike)):
            for value in iter_array_values(entry):
                if value is not None:
                    yield value
            continue
        if isinstance(entry, (list, tuple, set)):
            for value in entry:
                if value is not None:
                    yield value


RANGE_TABLE_ARG_COUNT: int = 2


def _range_table_udtf(*values: object) -> Table:
    """Return a table of integer values between start and end.

    Raises
    ------
    ValueError
        Raised when the UDTF receives an unexpected number of arguments.

    Returns
    -------
    datafusion.Table
        Table of integer values between start and end.
    """
    if len(values) != RANGE_TABLE_ARG_COUNT:
        msg = "range_table expects exactly two literal arguments."
        raise ValueError(msg)
    start_value = _literal_int(values[0], label="start")
    end_value = _literal_int(values[1], label="end")
    if end_value < start_value:
        range_values: list[int] = []
    else:
        range_values = list(range(start_value, end_value + 1))
    table = pa.table({"value": range_values})
    return Table(ds.dataset(table))


def stable_hash64_values(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    """Return stable hash64 values for Arrow inputs.

    Returns
    -------
    pa.Array | pa.Scalar
        Stable hash64 values.
    """
    return _stable_hash64(values)


def stable_hash128_values(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    """Return stable hash128 values for Arrow inputs.

    Returns
    -------
    pa.Array | pa.Scalar
        Stable hash128 values.
    """
    return _stable_hash128(values)


def load_udf_from_capsule(capsule: ScalarUDFExportable) -> ScalarUDF:
    """Load a DataFusion UDF from a PyCapsule value.

    Returns
    -------
    datafusion.user_defined.ScalarUDF
        UDF loaded from the capsule.
    """
    return ScalarUDF.from_pycapsule(capsule)


def udf_capsule_id(capsule: object) -> str:
    """Return a stable identifier for a PyCapsule UDF.

    Returns
    -------
    str
        Capsule identifier string.
    """
    return repr(capsule)


def _coerce_int(value: object | None) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        return int(raw) if raw.isdigit() else None
    return None


def _code_unit_offset_to_py_index(line: str, offset: int, position_encoding: int) -> int:
    if position_encoding == ENC_UTF32:
        return offset
    if position_encoding == ENC_UTF8:
        encoded = line.encode("utf-8")
        byte_off = min(offset, len(encoded))
        return len(encoded[:byte_off].decode("utf-8", errors="strict"))
    if position_encoding == ENC_UTF16:
        encoded = line.encode("utf-16-le")
        byte_off = min(offset * 2, len(encoded))
        return len(encoded[:byte_off].decode("utf-16-le", errors="strict"))
    return min(offset, len(line))


def _normalize_col_unit(value: object | None) -> str:
    if isinstance(value, int):
        return _col_unit_from_int(value)
    if isinstance(value, str):
        return _col_unit_from_text(value)
    return "utf32"


def _col_unit_from_int(value: int) -> str:
    encoding_map: dict[int, str] = {
        ENC_UTF8: "utf8",
        ENC_UTF16: "utf16",
        ENC_UTF32: "utf32",
    }
    return encoding_map.get(value, "utf32")


def _col_unit_from_text(value: str) -> str:
    text = value.strip().lower()
    if text.isdigit():
        return _col_unit_from_int(int(text))
    if "byte" in text:
        return "byte"
    if "utf8" in text:
        return "utf8"
    if "utf16" in text:
        return "utf16"
    if "utf32" in text:
        return "utf32"
    return "utf32"


def _encoding_from_unit(unit: str) -> int:
    if unit == "utf8":
        return ENC_UTF8
    if unit == "utf16":
        return ENC_UTF16
    return ENC_UTF32


def _clamp_offset(offset: int, limit: int) -> int:
    return max(0, min(offset, limit))


def _col_to_byte(
    line_values: pa.Array | pa.ChunkedArray | pa.Scalar,
    offset_values: pa.Array | pa.ChunkedArray | pa.Scalar,
    encoding_values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    if isinstance(line_values, pa.Scalar):
        line = line_values.as_py()
        offset_value = offset_values.as_py() if isinstance(offset_values, pa.Scalar) else None
        offset = _coerce_int(offset_value)
        unit = _normalize_col_unit(
            encoding_values.as_py() if isinstance(encoding_values, pa.Scalar) else None
        )
        if not isinstance(line, str) or offset is None:
            return pa.scalar(None, type=pa.int64())
        if unit == "byte":
            byte_len = len(line.encode("utf-8"))
            return pa.scalar(_clamp_offset(offset, byte_len), type=pa.int64())
        enc = _encoding_from_unit(unit)
        py_index = _code_unit_offset_to_py_index(line, offset, enc)
        py_index = _clamp_offset(py_index, len(line))
        return pa.scalar(len(line[:py_index].encode("utf-8")), type=pa.int64())

    length = len(line_values)
    line_iter = iter_array_values(line_values)
    offset_iter = (
        repeat(offset_values.as_py(), length)
        if isinstance(offset_values, pa.Scalar)
        else iter_array_values(offset_values)
    )
    encoding_iter = (
        repeat(encoding_values.as_py(), length)
        if isinstance(encoding_values, pa.Scalar)
        else iter_array_values(encoding_values)
    )
    out: list[int | None] = []
    for line_value, offset_value, encoding_value in zip(
        line_iter,
        offset_iter,
        encoding_iter,
        strict=True,
    ):
        if not isinstance(line_value, str):
            out.append(None)
            continue
        offset = _coerce_int(offset_value)
        unit = _normalize_col_unit(encoding_value)
        if offset is None:
            out.append(None)
            continue
        if unit == "byte":
            byte_len = len(line_value.encode("utf-8"))
            out.append(_clamp_offset(offset, byte_len))
            continue
        enc = _encoding_from_unit(unit)
        py_index = _code_unit_offset_to_py_index(line_value, offset, enc)
        py_index = _clamp_offset(py_index, len(line_value))
        out.append(len(line_value[:py_index].encode("utf-8")))
    return pa.array(out, type=pa.int64())


_STABLE_HASH64_UDF = udf(
    _stable_hash64,
    [pa.string()],
    pa.int64(),
    "stable",
    "stable_hash64",
)
_STABLE_HASH128_UDF = udf(
    _stable_hash128,
    [pa.string()],
    pa.string(),
    "stable",
    "stable_hash128",
)
_COL_TO_BYTE_UDF = udf(
    _col_to_byte,
    [pa.string(), pa.int64(), pa.string()],
    pa.int64(),
    "stable",
    "col_to_byte",
)
_PREFIXED_HASH64_UDF = udf(
    _prefixed_hash64,
    [pa.string(), pa.string()],
    pa.string(),
    "stable",
    "prefixed_hash64",
)
_STABLE_ID_UDF = udf(
    _stable_id,
    [pa.string(), pa.string()],
    pa.string(),
    "stable",
    "stable_id",
)

_LIST_UNIQUE_UDAF = udaf(
    _ListUniqueAccumulator,
    [pa.string()],
    pa.list_(pa.string()),
    [pa.list_(pa.string())],
    "stable",
    "list_unique",
)
_FIRST_VALUE_UDAF = udaf(
    _FirstValueAccumulator,
    [pa.string()],
    pa.string(),
    [pa.string()],
    "stable",
    "first_value_agg",
)
_LAST_VALUE_UDAF = udaf(
    _LastValueAccumulator,
    [pa.string()],
    pa.string(),
    [pa.string()],
    "stable",
    "last_value_agg",
)
_COUNT_DISTINCT_UDAF = udaf(
    _CountDistinctAccumulator,
    [pa.string()],
    pa.int64(),
    [pa.list_(pa.string())],
    "stable",
    "count_distinct_agg",
)
_STRING_AGG_UDAF = udaf(
    _StringAggAccumulator,
    [pa.string(), pa.string()],
    pa.string(),
    [pa.list_(pa.string())],
    "stable",
    "string_agg",
)
_ROW_INDEX_UDWF = udwf(
    _RowIndexEvaluator,
    [pa.int64()],
    pa.int64(),
    "stable",
    "row_index",
)
_RUNNING_COUNT_UDWF = udwf(
    _RunningCountEvaluator,
    [pa.int64()],
    pa.int64(),
    "stable",
    "running_count",
)
_RUNNING_TOTAL_UDWF = udwf(
    _RunningTotalEvaluator,
    [pa.float64()],
    pa.float64(),
    "stable",
    "running_total",
)
_ROW_NUMBER_UDWF = udwf(
    _RowNumberEvaluator,
    [pa.int64()],
    pa.int64(),
    "stable",
    "row_number_window",
)
_LAG_UDWF = udwf(
    _LagEvaluator,
    [pa.string()],
    pa.string(),
    "stable",
    "lag_window",
)
_LEAD_UDWF = udwf(
    _LeadEvaluator,
    [pa.string()],
    pa.string(),
    "stable",
    "lead_window",
)
_RANGE_TABLE_UDTF = udtf(cast("Callable[[], Table]", _range_table_udtf), name="range_table")

_SCALAR_UDF_SPECS: tuple[tuple[DataFusionUdfSpec, ScalarUDF], ...] = (
    (
        DataFusionUdfSpec(
            func_id="stable_hash64",
            engine_name="stable_hash64",
            kind="scalar",
            input_types=(pa.string(),),
            return_type=pa.int64(),
            arg_names=("value",),
            rewrite_tags=("hash",),
        ),
        _STABLE_HASH64_UDF,
    ),
    (
        DataFusionUdfSpec(
            func_id="stable_hash128",
            engine_name="stable_hash128",
            kind="scalar",
            input_types=(pa.string(),),
            return_type=pa.string(),
            arg_names=("value",),
            rewrite_tags=("hash",),
        ),
        _STABLE_HASH128_UDF,
    ),
    (
        DataFusionUdfSpec(
            func_id="prefixed_hash64",
            engine_name="prefixed_hash64",
            kind="scalar",
            input_types=(pa.string(), pa.string()),
            return_type=pa.string(),
            arg_names=("prefix", "value"),
            rewrite_tags=("hash",),
        ),
        _PREFIXED_HASH64_UDF,
    ),
    (
        DataFusionUdfSpec(
            func_id="stable_id",
            engine_name="stable_id",
            kind="scalar",
            input_types=(pa.string(), pa.string()),
            return_type=pa.string(),
            arg_names=("prefix", "value"),
            rewrite_tags=("hash",),
        ),
        _STABLE_ID_UDF,
    ),
    (
        DataFusionUdfSpec(
            func_id="col_to_byte",
            engine_name="col_to_byte",
            kind="scalar",
            input_types=(pa.string(), pa.int64(), pa.string()),
            return_type=pa.int64(),
            arg_names=("line_text", "col", "col_unit"),
            rewrite_tags=("position_encoding",),
        ),
        _COL_TO_BYTE_UDF,
    ),
)

_AGGREGATE_UDF_SPECS: tuple[tuple[DataFusionUdfSpec, AggregateUDF], ...] = (
    (
        DataFusionUdfSpec(
            func_id="list_unique",
            engine_name="list_unique",
            kind="aggregate",
            input_types=(pa.string(),),
            return_type=pa.list_(pa.string()),
            state_type=pa.list_(pa.string()),
            arg_names=("value",),
            rewrite_tags=("list",),
        ),
        _LIST_UNIQUE_UDAF,
    ),
    (
        DataFusionUdfSpec(
            func_id="first_value_agg",
            engine_name="first_value_agg",
            kind="aggregate",
            input_types=(pa.string(),),
            return_type=pa.string(),
            state_type=pa.string(),
            arg_names=("value",),
            rewrite_tags=("aggregate",),
        ),
        _FIRST_VALUE_UDAF,
    ),
    (
        DataFusionUdfSpec(
            func_id="last_value_agg",
            engine_name="last_value_agg",
            kind="aggregate",
            input_types=(pa.string(),),
            return_type=pa.string(),
            state_type=pa.string(),
            arg_names=("value",),
            rewrite_tags=("aggregate",),
        ),
        _LAST_VALUE_UDAF,
    ),
    (
        DataFusionUdfSpec(
            func_id="count_distinct_agg",
            engine_name="count_distinct_agg",
            kind="aggregate",
            input_types=(pa.string(),),
            return_type=pa.int64(),
            state_type=pa.list_(pa.string()),
            arg_names=("value",),
            rewrite_tags=("aggregate",),
        ),
        _COUNT_DISTINCT_UDAF,
    ),
    (
        DataFusionUdfSpec(
            func_id="string_agg",
            engine_name="string_agg",
            kind="aggregate",
            input_types=(pa.string(), pa.string()),
            return_type=pa.string(),
            state_type=pa.list_(pa.string()),
            arg_names=("value", "separator"),
            rewrite_tags=("aggregate", "string"),
        ),
        _STRING_AGG_UDAF,
    ),
)

_WINDOW_UDF_SPECS: tuple[tuple[DataFusionUdfSpec, WindowUDF], ...] = (
    (
        DataFusionUdfSpec(
            func_id="row_index",
            engine_name="row_index",
            kind="window",
            input_types=(pa.int64(),),
            return_type=pa.int64(),
            arg_names=("value",),
            rewrite_tags=("window",),
        ),
        _ROW_INDEX_UDWF,
    ),
    (
        DataFusionUdfSpec(
            func_id="running_count",
            engine_name="running_count",
            kind="window",
            input_types=(pa.int64(),),
            return_type=pa.int64(),
            arg_names=("value",),
            rewrite_tags=("window",),
        ),
        _RUNNING_COUNT_UDWF,
    ),
    (
        DataFusionUdfSpec(
            func_id="running_total",
            engine_name="running_total",
            kind="window",
            input_types=(pa.float64(),),
            return_type=pa.float64(),
            arg_names=("value",),
            rewrite_tags=("window",),
        ),
        _RUNNING_TOTAL_UDWF,
    ),
    (
        DataFusionUdfSpec(
            func_id="row_number_window",
            engine_name="row_number_window",
            kind="window",
            input_types=(pa.int64(),),
            return_type=pa.int64(),
            arg_names=("value",),
            rewrite_tags=("window",),
        ),
        _ROW_NUMBER_UDWF,
    ),
    (
        DataFusionUdfSpec(
            func_id="lag_window",
            engine_name="lag_window",
            kind="window",
            input_types=(pa.string(),),
            return_type=pa.string(),
            arg_names=("value",),
            rewrite_tags=("window",),
        ),
        _LAG_UDWF,
    ),
    (
        DataFusionUdfSpec(
            func_id="lead_window",
            engine_name="lead_window",
            kind="window",
            input_types=(pa.string(),),
            return_type=pa.string(),
            arg_names=("value",),
            rewrite_tags=("window",),
        ),
        _LEAD_UDWF,
    ),
)

_TABLE_UDF_SPECS: tuple[tuple[DataFusionUdfSpec, TableFunction], ...] = (
    (
        DataFusionUdfSpec(
            func_id="range_table",
            engine_name="range_table",
            kind="table",
            input_types=(pa.int64(), pa.int64()),
            return_type=pa.struct([pa.field("value", pa.int64())]),
            arg_names=("start", "end"),
            rewrite_tags=("table",),
        ),
        _RANGE_TABLE_UDTF,
    ),
)

DATAFUSION_UDF_SPECS: tuple[DataFusionUdfSpec, ...] = tuple(
    spec
    for spec, _ in (_SCALAR_UDF_SPECS + _AGGREGATE_UDF_SPECS + _WINDOW_UDF_SPECS + _TABLE_UDF_SPECS)
)


def datafusion_scalar_udf_map() -> dict[str, ScalarUDF]:
    """Return a mapping of scalar UDF names to implementations.

    Returns
    -------
    dict[str, datafusion.user_defined.ScalarUDF]
        Mapping of engine UDF names to ScalarUDF instances.
    """
    return {spec.engine_name: udf_impl for spec, udf_impl in _SCALAR_UDF_SPECS}


def datafusion_udf_specs() -> tuple[DataFusionUdfSpec, ...]:
    """Return the canonical DataFusion UDF specs.

    Returns
    -------
    tuple[DataFusionUdfSpec, ...]
        Canonical DataFusion UDF specifications.
    """
    pycapsule_specs = tuple(entry.spec for entry in _pycapsule_entries())
    return DATAFUSION_UDF_SPECS + pycapsule_specs


def register_pycapsule_udf_spec(
    spec: DataFusionUdfSpec,
    *,
    capsule: ScalarUDFExportable,
) -> DataFusionUdfSpec:
    """Register a PyCapsule-backed UDF spec for diagnostics and registries.

    Returns
    -------
    DataFusionUdfSpec
        Spec augmented with the capsule identifier.
    """
    capsule_id = spec.capsule_id or udf_capsule_id(capsule)
    updated = spec if spec.capsule_id == capsule_id else replace(spec, capsule_id=capsule_id)
    _PYCAPSULE_UDF_SPECS[updated.engine_name] = DataFusionPycapsuleUdfEntry(
        spec=updated,
        capsule=capsule,
    )
    return updated


def _register_scalar_udfs(ctx: SessionContext) -> tuple[str, ...]:
    if ctx not in _KERNEL_UDF_CONTEXTS:
        for _, udf_impl in _SCALAR_UDF_SPECS:
            ctx.register_udf(udf_impl)
        _KERNEL_UDF_CONTEXTS.add(ctx)
    return tuple(spec.engine_name for spec, _ in _SCALAR_UDF_SPECS)


def _register_pycapsule_udfs(ctx: SessionContext) -> tuple[DataFusionUdfCapsuleEntry, ...]:
    if ctx not in _PYCAPSULE_UDF_CONTEXTS:
        for entry in _pycapsule_entries():
            try:
                ctx.register_udf(load_udf_from_capsule(entry.capsule))
            except (RuntimeError, TypeError, ValueError):
                continue
        _PYCAPSULE_UDF_CONTEXTS.add(ctx)
    return tuple(
        DataFusionUdfCapsuleEntry(
            name=entry.spec.engine_name,
            kind=entry.spec.kind,
            capsule_id=entry.capsule_id(),
            udf_tier=entry.spec.udf_tier,
        )
        for entry in _pycapsule_entries()
    )


def _register_aggregate_udfs(_ctx: SessionContext) -> tuple[str, ...]:
    if _ctx not in _AGGREGATE_UDF_CONTEXTS:
        for _, udf_impl in _AGGREGATE_UDF_SPECS:
            _ctx.register_udaf(udf_impl)
        _AGGREGATE_UDF_CONTEXTS.add(_ctx)
    return tuple(spec.engine_name for spec, _ in _AGGREGATE_UDF_SPECS)


def _register_window_udfs(_ctx: SessionContext) -> tuple[str, ...]:
    if _ctx not in _WINDOW_UDF_CONTEXTS:
        for _, udf_impl in _WINDOW_UDF_SPECS:
            _ctx.register_udwf(udf_impl)
        _WINDOW_UDF_CONTEXTS.add(_ctx)
    return tuple(spec.engine_name for spec, _ in _WINDOW_UDF_SPECS)


def _register_table_udfs(_ctx: SessionContext) -> tuple[str, ...]:
    if _ctx not in _TABLE_UDF_CONTEXTS:
        for _, udf_impl in _TABLE_UDF_SPECS:
            _ctx.register_udtf(udf_impl)
        _TABLE_UDF_CONTEXTS.add(_ctx)
    return tuple(spec.engine_name for spec, _ in _TABLE_UDF_SPECS)


def _register_kernel_udfs(ctx: SessionContext) -> None:
    _ = _register_scalar_udfs(ctx)
    _ = _register_aggregate_udfs(ctx)
    _ = _register_window_udfs(ctx)
    _ = _register_table_udfs(ctx)


def register_datafusion_udfs(ctx: SessionContext) -> DataFusionUdfSnapshot:
    """Register shared DataFusion UDFs in the provided session context.

    Returns
    -------
    DataFusionUdfSnapshot
        Snapshot of registered UDFs.
    """
    capsule_udfs = _register_pycapsule_udfs(ctx)
    scalar = _register_scalar_udfs(ctx)
    aggregate = _register_aggregate_udfs(ctx)
    window = _register_window_udfs(ctx)
    table = _register_table_udfs(ctx)
    return DataFusionUdfSnapshot(
        scalar=scalar,
        aggregate=aggregate,
        window=window,
        table=table,
        capsule_udfs=capsule_udfs,
    )


def register_extended_udfs(ctx: SessionContext) -> None:
    """Register extended UDFs (UDAF/UDWF) in the provided session context.

    This function registers all extended aggregate and window functions beyond
    the core UDF set. It does not check performance policies - use
    register_datafusion_udfs_with_policy for policy-aware registration.

    Parameters
    ----------
    ctx:
        DataFusion SessionContext to register UDFs in.
    """
    # Extended UDFs are already included in the standard registration functions
    # This is a convenience alias for explicit extended UDF registration
    _register_aggregate_udfs(ctx)
    _register_window_udfs(ctx)


def register_datafusion_udfs_with_policy(
    ctx: SessionContext,
    policy: UdfPerformancePolicy,
) -> list[str]:
    """Register DataFusion UDFs respecting performance policy restrictions.

    This function registers UDFs while respecting the provided performance policy,
    skipping Python UDFs if not allowed and emitting warnings for slow UDFs when
    configured.

    Parameters
    ----------
    ctx:
        DataFusion SessionContext to register UDFs in.
    policy:
        Performance policy to apply during registration.

    Returns
    -------
    list[str]
        List of registered UDF names.
    """
    registered: list[str] = []

    # Always register PyCapsule UDFs (they are native Rust implementations)
    _register_pycapsule_udfs(ctx)

    registered.extend(
        _register_udf_specs(
            specs=_SCALAR_UDF_SPECS,
            policy=policy,
            register_fn=ctx.register_udf,
            kind_label="UDF",
        )
    )
    registered.extend(
        _register_udf_specs(
            specs=_AGGREGATE_UDF_SPECS,
            policy=policy,
            register_fn=ctx.register_udaf,
            kind_label="UDAF",
        )
    )
    registered.extend(
        _register_udf_specs(
            specs=_WINDOW_UDF_SPECS,
            policy=policy,
            register_fn=ctx.register_udwf,
            kind_label="UDWF",
        )
    )
    registered.extend(
        _register_udf_specs(
            specs=_TABLE_UDF_SPECS,
            policy=policy,
            register_fn=ctx.register_udtf,
            kind_label="UDTF",
        )
    )

    return registered


def create_udf_catalog_from_specs(
    specs: tuple[DataFusionUdfSpec, ...] | None = None,
    *,
    tier_policy: UdfTierPolicy | None = None,
) -> UdfCatalog:
    """Create a UDF catalog from DataFusion UDF specifications.

    Parameters
    ----------
    specs:
        Optional tuple of UDF specs. If None, uses all registered specs.
    tier_policy:
        Optional tier policy for the catalog.

    Returns
    -------
    UdfCatalog
        Catalog initialized with the provided specs and policy.
    """
    resolved_specs = specs or datafusion_udf_specs()
    spec_map = {spec.func_id: spec for spec in resolved_specs}
    if tier_policy:
        return UdfCatalog(tier_policy=tier_policy, udf_specs=spec_map)
    return create_default_catalog(udf_specs=spec_map)


def validate_udf_performance(
    func_id: str,
    spec: DataFusionUdfSpec,
    *,
    policy: UdfPerformancePolicy,
) -> tuple[bool, str | None]:
    """Validate that a UDF meets performance policy requirements.

    Parameters
    ----------
    func_id:
        Function identifier to validate.
    spec:
        UDF specification to validate.
    policy:
        Performance policy to apply.

    Returns
    -------
    tuple[bool, str | None]
        Tuple of (is_valid, reason). If invalid, reason contains explanation.
    """
    # Check if this is actually a builtin (shouldn't be in custom UDF specs)
    if is_builtin_function(func_id):
        return True, None

    # Validate tier permissions
    return policy.is_udf_allowed(func_id, spec.udf_tier)


def _register_udf_specs(
    *,
    specs: tuple[tuple[DataFusionUdfSpec, T_Udf], ...],
    policy: UdfPerformancePolicy,
    register_fn: Callable[[T_Udf], None],
    kind_label: str,
) -> list[str]:
    registered: list[str] = []
    for spec, udf_impl in specs:
        allowed, reason = validate_udf_performance(spec.func_id, spec, policy=policy)
        if not allowed:
            _warn_skipped_udf(policy, kind_label=kind_label, func_id=spec.func_id, reason=reason)
            continue
        _warn_python_udf(policy, kind_label=kind_label, func_id=spec.func_id, tier=spec.udf_tier)
        register_fn(udf_impl)
        registered.append(spec.engine_name)
    return registered


def _warn_skipped_udf(
    policy: UdfPerformancePolicy,
    *,
    kind_label: str,
    func_id: str,
    reason: str | None,
) -> None:
    if not policy.warn_on_python_udfs:
        return
    warnings.warn(f"Skipping {kind_label} {func_id}: {reason}", stacklevel=2)


def _warn_python_udf(
    policy: UdfPerformancePolicy,
    *,
    kind_label: str,
    func_id: str,
    tier: UdfTier,
) -> None:
    if not policy.warn_on_python_udfs or tier != "python":
        return
    warnings.warn(
        f"Registering Python {kind_label} {func_id} (consider using builtin alternative)",
        stacklevel=2,
    )


def get_default_udf_catalog() -> UdfCatalog:
    """Get a default UDF catalog with all registered DataFusion UDFs.

    Returns
    -------
    UdfCatalog
        Default catalog with standard tier policy.
    """
    return create_udf_catalog_from_specs()


def get_strict_udf_catalog() -> UdfCatalog:
    """Get a strict UDF catalog that prefers builtins only.

    Returns
    -------
    UdfCatalog
        Catalog with strict builtin-only policy.
    """
    specs = datafusion_udf_specs()
    spec_map = {spec.func_id: spec for spec in specs}
    return create_strict_catalog(udf_specs=spec_map)


__all__ = [
    "DATAFUSION_UDF_SPECS",
    "DataFusionPycapsuleUdfEntry",
    "DataFusionUdfCapsuleEntry",
    "DataFusionUdfSnapshot",
    "DataFusionUdfSpec",
    "UdfCatalog",
    "UdfPerformancePolicy",
    "UdfTier",
    "UdfTierPolicy",
    "_register_kernel_udfs",
    "check_udf_allowed",
    "create_default_catalog",
    "create_udf_catalog_from_specs",
    "datafusion_scalar_udf_map",
    "datafusion_udf_specs",
    "get_default_udf_catalog",
    "get_strict_udf_catalog",
    "is_builtin_function",
    "load_udf_from_capsule",
    "register_datafusion_udfs",
    "register_datafusion_udfs_with_policy",
    "register_extended_udfs",
    "register_pycapsule_udf_spec",
    "stable_hash64_values",
    "stable_hash128_values",
    "udf_capsule_id",
    "validate_udf_performance",
]
