"""Value coercion utilities with tolerant and strict variants."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

import pyarrow as pa

from datafusion_engine.arrow_interop import (
    RecordBatchReader,
    RecordBatchReaderLike,
    TableLike,
)


class CoercionError(ValueError):
    """Raised when strict coercion fails."""

    def __init__(self, value: object, target_type: str, reason: str | None = None) -> None:
        self.value = value
        self.target_type = target_type
        message = f"Cannot coerce {type(value).__name__} to {target_type}"
        if reason:
            message = f"{message}: {reason}"
        super().__init__(message)


def coerce_int(value: object) -> int | None:
    """Coerce value to int, returning None for unconvertible values.

    Returns
    -------
    int | None
        Coerced integer or None if conversion fails.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return int(stripped)
            except ValueError:
                return None
    return None


def coerce_float(value: object) -> float | None:
    """Coerce value to float, returning None for unconvertible values.

    Returns
    -------
    float | None
        Coerced float or None if conversion fails.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return float(stripped)
            except ValueError:
                return None
    return None


def coerce_bool(value: object) -> bool | None:
    """Coerce value to bool, returning None for unconvertible values.

    Returns
    -------
    bool | None
        Coerced bool or None if conversion fails.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        lower = value.strip().lower()
        if lower in {"true", "1", "yes", "on"}:
            return True
        if lower in {"false", "0", "no", "off"}:
            return False
    return None


def coerce_str(value: object) -> str | None:
    """Coerce value to string, returning None for None.

    Returns
    -------
    str | None
        Coerced string or None if value is None.
    """
    if value is None:
        return None
    return str(value)


def coerce_str_list(value: object) -> list[str]:
    """Coerce value to list of non-empty strings.

    Returns
    -------
    list[str]
        List of non-empty strings.
    """
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [str(item) for item in value if str(item).strip()]
    return []


def coerce_str_tuple(value: object) -> tuple[str, ...]:
    """Coerce value to tuple of non-empty strings.

    Returns
    -------
    tuple[str, ...]
        Tuple of non-empty strings.
    """
    return tuple(coerce_str_list(value))


def coerce_mapping_list(value: object) -> Sequence[Mapping[str, object]] | None:
    """Coerce value to sequence of mappings.

    Returns
    -------
    Sequence[Mapping[str, object]] | None
        Sequence of mappings or None if conversion fails.
    """
    if value is None:
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [dict(entry) for entry in value if isinstance(entry, Mapping)]
    return None


def coerce_to_recordbatch_reader(value: object) -> RecordBatchReaderLike | None:
    """Coerce value to a RecordBatchReader.

    Parameters
    ----------
    value
        Value to coerce. Accepts:
        - RecordBatchReader (returned as-is)
        - Table (converted to reader)
        - RecordBatch (wrapped in reader)
        - Sequence[RecordBatch] (wrapped in reader)

    Returns
    -------
    RecordBatchReaderLike | None
        RecordBatchReader or None if coercion fails.
    """
    if value is None:
        return None
    if isinstance(value, RecordBatchReader):
        return value
    if isinstance(value, TableLike):
        return cast(
            "RecordBatchReaderLike",
            pa.RecordBatchReader.from_batches(value.schema, value.to_batches()),
        )
    if isinstance(value, pa.RecordBatch):
        record_batch = cast("pa.RecordBatch", value)
        return cast(
            "RecordBatchReaderLike",
            pa.RecordBatchReader.from_batches(record_batch.schema, [record_batch]),
        )
    if isinstance(value, Sequence):
        batches = [batch for batch in value if isinstance(batch, pa.RecordBatch)]
        if batches:
            return cast(
                "RecordBatchReaderLike",
                pa.RecordBatchReader.from_batches(batches[0].schema, batches),
            )
    return None


def raise_for_int(value: object, *, context: str = "") -> int:
    """Coerce value to int, raising CoercionError on failure.

    Returns
    -------
    int
        Coerced integer value.

    Raises
    ------
    CoercionError
        Raised when value cannot be coerced to int.
    """
    result = coerce_int(value)
    if result is None:
        raise CoercionError(value, "int", context or None)
    return result


def raise_for_float(value: object, *, context: str = "") -> float:
    """Coerce value to float, raising CoercionError on failure.

    Returns
    -------
    float
        Coerced float value.

    Raises
    ------
    CoercionError
        Raised when value cannot be coerced to float.
    """
    result = coerce_float(value)
    if result is None:
        raise CoercionError(value, "float", context or None)
    return result


def raise_for_bool(value: object, *, context: str = "") -> bool:
    """Coerce value to bool, raising CoercionError on failure.

    Returns
    -------
    bool
        Coerced bool value.

    Raises
    ------
    CoercionError
        Raised when value cannot be coerced to bool.
    """
    result = coerce_bool(value)
    if result is None:
        raise CoercionError(value, "bool", context or None)
    return result


def raise_for_str(value: object, *, context: str = "") -> str:
    """Coerce value to str, raising CoercionError when value is None.

    Returns
    -------
    str
        Coerced string value.

    Raises
    ------
    CoercionError
        Raised when value is None.
    """
    if value is None:
        raise CoercionError(value, "str", context or "value is None")
    return str(value)


__all__ = [
    "CoercionError",
    "coerce_bool",
    "coerce_float",
    "coerce_int",
    "coerce_mapping_list",
    "coerce_str",
    "coerce_str_list",
    "coerce_str_tuple",
    "coerce_to_recordbatch_reader",
    "raise_for_bool",
    "raise_for_float",
    "raise_for_int",
    "raise_for_str",
]
