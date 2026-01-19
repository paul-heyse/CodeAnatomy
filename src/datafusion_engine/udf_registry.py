"""Shared DataFusion UDF registry helpers."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, replace
from itertools import repeat
from typing import TYPE_CHECKING, Literal, Protocol
from weakref import WeakSet

import pyarrow as pa
from datafusion import SessionContext, udf
from datafusion.user_defined import ScalarUDF

if TYPE_CHECKING:
    from datafusion.user_defined import ScalarUDFExportable
else:

    class ScalarUDFExportable(Protocol):
        """Protocol for DataFusion PyCapsule exportable UDFs."""

        __datafusion_scalar_udf__: object


from arrowdsl.compute.position_encoding import (
    ENC_UTF8,
    ENC_UTF16,
    ENC_UTF32,
    normalize_position_encoding,
)
from arrowdsl.core.ids import hash64_from_text, iter_array_values
from arrowdsl.core.interop import pc

_NUMERIC_REGEX = r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$"
_KERNEL_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_PYCAPSULE_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_PYCAPSULE_UDF_SPECS: dict[str, DataFusionPycapsuleUdfEntry] = {}

DataFusionUdfKind = Literal["scalar", "aggregate", "window", "table"]
UdfTier = Literal["builtin", "pyarrow", "pandas", "python"]
UDF_TIER_PRIORITY: tuple[UdfTier, ...] = ("builtin", "pyarrow", "pandas", "python")


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


def _normalize_span(values: pa.Array | pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
    text = pc.utf8_trim_whitespace(pc.cast(values, pa.string(), safe=False))
    mask = pc.match_substring_regex(text, _NUMERIC_REGEX)
    mask = pc.fill_null(mask, fill_value=False)
    sanitized = pc.if_else(mask, text, pa.scalar(None, type=pa.string()))
    numeric = pc.cast(sanitized, pa.float64(), safe=False)
    return pc.cast(numeric, pa.int64(), safe=False)


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


def _hash128_text(value: str) -> str:
    return hashlib.blake2b(value.encode("utf-8"), digest_size=16).hexdigest()


def _stable_hash128(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    if isinstance(values, pa.Scalar):
        value = values.as_py()
        if value is None:
            return pa.scalar(None, type=pa.string())
        return pa.scalar(_hash128_text(str(value)), type=pa.string())
    out = [
        _hash128_text(str(value)) if value is not None else None
        for value in iter_array_values(values)
    ]
    return pa.array(out, type=pa.string())


def _position_encoding_norm(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    if isinstance(values, pa.Scalar):
        value = normalize_position_encoding(values.as_py())
        return pa.scalar(value, type=pa.int32())
    out = [normalize_position_encoding(value) for value in iter_array_values(values)]
    return pa.array(out, type=pa.int32())


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


_NORMALIZE_SPAN_UDF = udf(
    _normalize_span,
    [pa.string()],
    pa.int64(),
    "stable",
    "normalize_span",
)
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
_POSITION_ENCODING_NORM_UDF = udf(
    _position_encoding_norm,
    [pa.string()],
    pa.int32(),
    "stable",
    "position_encoding_norm",
)
_COL_TO_BYTE_UDF = udf(
    _col_to_byte,
    [pa.string(), pa.int64(), pa.string()],
    pa.int64(),
    "stable",
    "col_to_byte",
)

_SCALAR_UDF_SPECS: tuple[tuple[DataFusionUdfSpec, ScalarUDF], ...] = (
    (
        DataFusionUdfSpec(
            func_id="normalize_span",
            engine_name="normalize_span",
            kind="scalar",
            input_types=(pa.string(),),
            return_type=pa.int64(),
            arg_names=("value",),
            rewrite_tags=("normalize_span",),
        ),
        _NORMALIZE_SPAN_UDF,
    ),
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
            func_id="position_encoding_norm",
            engine_name="position_encoding_norm",
            kind="scalar",
            input_types=(pa.string(),),
            return_type=pa.int32(),
            arg_names=("value",),
            rewrite_tags=("position_encoding",),
        ),
        _POSITION_ENCODING_NORM_UDF,
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

DATAFUSION_UDF_SPECS: tuple[DataFusionUdfSpec, ...] = tuple(spec for spec, _ in _SCALAR_UDF_SPECS)


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
    return ()


def _register_window_udfs(_ctx: SessionContext) -> tuple[str, ...]:
    return ()


def _register_table_udfs(_ctx: SessionContext) -> tuple[str, ...]:
    return ()


def _register_kernel_udfs(ctx: SessionContext) -> None:
    _ = _register_scalar_udfs(ctx)


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
    "_NORMALIZE_SPAN_UDF",
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
    "udf_capsule_id",
]
