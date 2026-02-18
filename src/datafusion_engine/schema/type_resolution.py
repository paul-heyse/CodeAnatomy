"""Canonical Arrow type mapping helpers."""

from __future__ import annotations

from typing import cast

import pyarrow as pa

from datafusion_engine.arrow.interop import DataTypeLike
from schema_spec.arrow_types import ArrowTypeBase, ArrowTypeSpec, arrow_type_to_pyarrow

TYPE_HINT_TO_ARROW: dict[str, pa.DataType] = {
    "str": pa.string(),
    "string": pa.string(),
    "int": pa.int64(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float": pa.float64(),
    "float64": pa.float64(),
    "bool": pa.bool_(),
    "bytes": pa.binary(),
    "map<string,string>": pa.map_(pa.string(), pa.string()),
}

_ARROW_TO_SQL_PAIRS: tuple[tuple[pa.DataType, str], ...] = (
    (pa.int64(), "Int64"),
    (pa.int32(), "Int32"),
    (pa.string(), "Utf8"),
    (pa.float64(), "Float64"),
    (pa.bool_(), "Boolean"),
)


def resolve_arrow_type(hint: str) -> pa.DataType:
    """Resolve a type-hint string to a concrete Arrow type.

    Returns:
    -------
    pyarrow.DataType
        Canonical Arrow data type mapped from the hint.

    Raises:
        ValueError: If the type hint is unsupported.
    """
    normalized = hint.strip().lower().replace(" ", "")
    resolved = TYPE_HINT_TO_ARROW.get(normalized)
    if resolved is not None:
        return resolved
    msg = f"Unsupported type hint: {hint!r}."
    raise ValueError(msg)


def arrow_type_to_sql(arrow_type: pa.DataType | ArrowTypeBase) -> str:
    """Convert an Arrow type to a SQL-like string label.

    Returns:
    -------
    str
        SQL-ish type label for the provided Arrow type.
    """
    if isinstance(arrow_type, ArrowTypeBase):
        arrow_type = arrow_type_to_pyarrow(arrow_type)
    for candidate, sql_type in _ARROW_TO_SQL_PAIRS:
        if arrow_type == candidate:
            return sql_type
    if pa.types.is_dictionary(arrow_type):
        dict_type = cast("pa.DictionaryType", arrow_type)
        return arrow_type_to_sql(dict_type.value_type)
    storage_type = getattr(arrow_type, "storage_type", None)
    if isinstance(storage_type, pa.DataType) and storage_type is not arrow_type:
        return arrow_type_to_sql(storage_type)
    return str(arrow_type)


def ensure_arrow_dtype(dtype: DataTypeLike | ArrowTypeSpec) -> pa.DataType:
    """Normalize schema-spec/native dtype variants to ``pyarrow.DataType``.

    Returns:
    -------
    pyarrow.DataType
        Normalized Arrow data type.

    Raises:
        TypeError: If the value cannot be normalized into a ``pyarrow.DataType``.
    """
    if isinstance(dtype, pa.DataType):
        return dtype
    if isinstance(dtype, ArrowTypeBase):
        return arrow_type_to_pyarrow(dtype)
    msg = f"Expected pyarrow.DataType, got {type(dtype)!r}."
    raise TypeError(msg)


__all__ = [
    "TYPE_HINT_TO_ARROW",
    "arrow_type_to_sql",
    "ensure_arrow_dtype",
    "resolve_arrow_type",
]
