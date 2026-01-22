"""Shared DataFusion UDF registry helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from itertools import repeat
from typing import TYPE_CHECKING, Literal, Protocol, cast
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
from datafusion_engine.hash_utils import hash64_from_text, hash128_from_text

_KERNEL_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_AGGREGATE_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_WINDOW_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_TABLE_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_PYCAPSULE_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_PYCAPSULE_UDF_SPECS: dict[str, DataFusionPycapsuleUdfEntry] = {}

DataFusionUdfKind = Literal["scalar", "aggregate", "window", "table"]
UdfTier = Literal["builtin", "pyarrow", "pandas", "python"]
UDF_TIER_PRIORITY: tuple[UdfTier, ...] = ("builtin", "pyarrow", "pandas", "python")

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
_ROW_INDEX_UDWF = udwf(
    _RowIndexEvaluator,
    [pa.int64()],
    pa.int64(),
    "stable",
    "row_index",
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


__all__ = [
    "DATAFUSION_UDF_SPECS",
    "DataFusionPycapsuleUdfEntry",
    "DataFusionUdfCapsuleEntry",
    "DataFusionUdfSnapshot",
    "DataFusionUdfSpec",
    "UdfTier",
    "_register_kernel_udfs",
    "datafusion_scalar_udf_map",
    "datafusion_udf_specs",
    "load_udf_from_capsule",
    "register_datafusion_udfs",
    "register_pycapsule_udf_spec",
    "stable_hash64_values",
    "stable_hash128_values",
    "udf_capsule_id",
]
