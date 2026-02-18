"""Shared DDL type aliases used by dataset and UDF registration surfaces."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pyarrow.types as patypes

_DDL_TYPE_ALIASES: dict[str, str] = {
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INT",
    "int64": "BIGINT",
    "uint8": "TINYINT",
    "uint16": "SMALLINT",
    "uint32": "INT",
    "uint64": "BIGINT",
    "float32": "FLOAT",
    "float64": "DOUBLE",
    "utf8": "VARCHAR",
    "large_utf8": "VARCHAR",
    "large_string": "VARCHAR",
    "string": "VARCHAR",
    "null": "VARCHAR",
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
}

DDL_TYPE_ALIASES: Mapping[str, str] = _DDL_TYPE_ALIASES

DDL_COMPLEX_TYPE_TOKENS: tuple[str, ...] = (
    "struct<",
    "list<",
    "large_list<",
    "fixed_size_list<",
    "map<",
    "dictionary<",
    "binary",
    "large_binary",
    "fixed_size_binary",
    "union<",
)


def ddl_type_alias(dtype_name: str) -> str | None:
    """Resolve a canonical SQL alias for a DataFusion/Arrow type name.

    Returns:
    -------
    str | None
        Canonical SQL alias when recognized, otherwise ``None``.
    """
    normalized = dtype_name.strip()
    if not normalized:
        return None
    alias = _DDL_TYPE_ALIASES.get(normalized)
    if alias is not None:
        return alias
    return _DDL_TYPE_ALIASES.get(normalized.lower())


def ddl_type_name_from_arrow(dtype: pa.DataType) -> str | None:
    """Resolve canonical DDL type name from a PyArrow dtype when possible.

    Returns:
    -------
    str | None
        Canonical DDL name when known, otherwise ``None``.
    """
    if patypes.is_dictionary(dtype):
        return ddl_type_name(dtype.value_type)
    if patypes.is_decimal(dtype):
        return f"DECIMAL({dtype.precision},{dtype.scale})"
    if patypes.is_timestamp(dtype):
        return "TIMESTAMP"
    if patypes.is_date(dtype):
        return "DATE"
    if patypes.is_time(dtype):
        return "TIME"
    varchar_checks = (
        patypes.is_struct,
        patypes.is_list,
        patypes.is_large_list,
        patypes.is_fixed_size_list,
        patypes.is_map,
        patypes.is_union,
        patypes.is_binary,
        patypes.is_large_binary,
        patypes.is_fixed_size_binary,
    )
    if any(check(dtype) for check in varchar_checks):
        return "VARCHAR"
    return None


def ddl_type_name_from_string(dtype_name: str) -> str:
    """Resolve canonical DDL type name from a normalized type string.

    Returns:
    -------
    str
        Canonicalized DDL type name.
    """
    normalized = dtype_name.strip().lower()
    if normalized.startswith("timestamp"):
        return "TIMESTAMP"
    if normalized.startswith("date"):
        return "DATE"
    if normalized.startswith("time"):
        return "TIME"
    if any(token in normalized for token in DDL_COMPLEX_TYPE_TOKENS) or normalized in {
        "any",
        "variant",
        "json",
    }:
        return "VARCHAR"
    alias = ddl_type_alias(normalized)
    return alias or normalized.upper()


def ddl_type_name(dtype: object) -> str:
    """Resolve canonical DDL type name from Arrow dtype or free-form type value.

    Returns:
    -------
    str
        Canonicalized DDL type name.
    """
    if isinstance(dtype, pa.DataType):
        resolved = ddl_type_name_from_arrow(dtype)
        if resolved is not None:
            return resolved
    return ddl_type_name_from_string(str(dtype))


__all__ = [
    "DDL_COMPLEX_TYPE_TOKENS",
    "DDL_TYPE_ALIASES",
    "ddl_type_alias",
    "ddl_type_name",
    "ddl_type_name_from_arrow",
    "ddl_type_name_from_string",
]
