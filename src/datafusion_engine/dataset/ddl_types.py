"""Shared DDL type aliases used by dataset and UDF registration surfaces."""

from __future__ import annotations

from collections.abc import Mapping

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


__all__ = [
    "DDL_COMPLEX_TYPE_TOKENS",
    "DDL_TYPE_ALIASES",
    "ddl_type_alias",
]
