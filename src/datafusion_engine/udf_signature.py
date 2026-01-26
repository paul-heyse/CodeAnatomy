"""Helpers for parsing Rust UDF signature snapshots."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

import pyarrow as pa

_SIMPLE_TYPES: Mapping[str, pa.DataType] = {
    "null": pa.null(),
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "string": pa.string(),
    "utf8": pa.string(),
    "largeutf8": pa.large_string(),
    "utf8view": pa.string(),
}
_DECIMAL_PARTS: int = 2
_MAP_PARTS: int = 2


def parse_type_signature(value: str) -> pa.DataType:
    """Parse a Rust signature type into a PyArrow datatype.

    Parameters
    ----------
    value:
        Type signature string (e.g., ``string``, ``list<int64>``).

    Returns
    -------
    pyarrow.DataType
        Parsed PyArrow datatype.

    Raises
    ------
    ValueError
        Raised when the signature cannot be parsed.
    """
    text = _normalize_type(value)
    simple = _SIMPLE_TYPES.get(text)
    if simple is not None:
        return simple
    parsed = _parse_container_type(text)
    if parsed is not None:
        return parsed
    parsed = _parse_decimal(text)
    if parsed is not None:
        return parsed
    msg = f"Unsupported type signature: {value!r}."
    raise ValueError(msg)


def signature_inputs(
    snapshot: Mapping[str, object], name: str
) -> tuple[tuple[pa.DataType, ...], ...]:
    """Return parsed input signatures for a function name.

    Returns
    -------
    tuple[tuple[pyarrow.DataType, ...], ...]
        Parsed input signatures.
    """
    raw = snapshot.get("signature_inputs")
    if not isinstance(raw, Mapping):
        return ()
    entries = raw.get(name)
    if not isinstance(entries, Iterable) or isinstance(entries, (str, bytes)):
        return ()
    parsed: list[tuple[pa.DataType, ...]] = []
    for row in entries:
        if not isinstance(row, Iterable) or isinstance(row, (str, bytes)):
            continue
        values = [str(item) for item in row if item is not None]
        parsed.append(tuple(parse_type_signature(value) for value in values))
    return tuple(parsed)


def signature_returns(
    snapshot: Mapping[str, object], name: str
) -> tuple[pa.DataType, ...]:
    """Return parsed return types for a function name.

    Returns
    -------
    tuple[pyarrow.DataType, ...]
        Parsed return types.
    """
    raw = snapshot.get("return_types")
    if not isinstance(raw, Mapping):
        return ()
    entries = raw.get(name)
    if not isinstance(entries, Iterable) or isinstance(entries, (str, bytes)):
        return ()
    values = [str(item) for item in entries if item is not None]
    return tuple(parse_type_signature(value) for value in values)


def custom_udf_names(snapshot: Mapping[str, object]) -> tuple[str, ...]:
    """Return the list of custom Rust UDF names from a registry snapshot.

    Returns
    -------
    tuple[str, ...]
        Custom Rust UDF names.
    """
    raw = snapshot.get("custom_udfs")
    if isinstance(raw, Iterable) and not isinstance(raw, (str, bytes)):
        names = [str(value) for value in raw if value is not None]
        if names:
            return tuple(names)
    names: list[str] = []
    for key in ("scalar", "aggregate", "window", "table"):
        values = snapshot.get(key)
        if isinstance(values, Iterable) and not isinstance(values, (str, bytes)):
            names.extend(str(value) for value in values if value is not None)
    return tuple(names)


def _split_top_level(value: str, delimiter: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(value):
        if char == "<":
            depth += 1
        elif char == ">":
            depth = max(depth - 1, 0)
        elif char == delimiter and depth == 0:
            parts.append(value[start:index].strip())
            start = index + 1
    tail = value[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _normalize_type(value: str) -> str:
    text = value.strip().lower()
    if not text:
        msg = "Empty type signature."
        raise ValueError(msg)
    return text


def _parse_container_type(text: str) -> pa.DataType | None:
    if text.startswith("list<") and text.endswith(">"):
        inner = parse_type_signature(text[5:-1])
        return pa.list_(inner)
    if text.startswith("struct<") and text.endswith(">"):
        return _parse_struct(text[7:-1].strip())
    if text.startswith("map<") and text.endswith(">"):
        return _parse_map(text[4:-1])
    return None


def _parse_struct(body: str) -> pa.DataType:
    if not body:
        return pa.struct([])
    fields: list[pa.Field] = []
    for item in _split_top_level(body, ","):
        name, _, type_str = item.partition(":")
        if not name or not type_str:
            msg = f"Invalid struct field signature: {item!r}."
            raise ValueError(msg)
        fields.append(pa.field(name.strip(), parse_type_signature(type_str)))
    return pa.struct(fields)


def _parse_map(body: str) -> pa.DataType:
    parts = _split_top_level(body, ",")
    if len(parts) != _MAP_PARTS:
        msg = f"Invalid map signature: {body!r}."
        raise ValueError(msg)
    key_type = parse_type_signature(parts[0])
    item_type = parse_type_signature(parts[1])
    return pa.map_(key_type, item_type)


def _parse_decimal(text: str) -> pa.DataType | None:
    if not (text.startswith("decimal(") and text.endswith(")")):
        return None
    precision_text = text[8:-1]
    parts = [part.strip() for part in precision_text.split(",") if part.strip()]
    if len(parts) != _DECIMAL_PARTS:
        msg = f"Invalid decimal signature: {text!r}."
        raise ValueError(msg)
    precision = int(parts[0])
    scale = int(parts[1])
    return pa.decimal128(precision, scale)


__all__ = [
    "custom_udf_names",
    "parse_type_signature",
    "signature_inputs",
    "signature_returns",
]
