"""Shared msgspec utility helpers for CQ contracts."""

from __future__ import annotations

from typing import cast

import msgspec
from msgspec import inspect, structs


def struct_field_names(struct_type: type) -> tuple[str, ...]:
    """Return declared field names for schema-aware validation/rendering.

    Parameters
    ----------
    struct_type : type
        The msgspec Struct type to inspect.

    Returns:
    -------
    tuple[str, ...]
        Ordered tuple of field names.
    """
    return tuple(field.name for field in structs.fields(cast("msgspec.Struct", struct_type)))


def union_schema_summary(types: tuple[type[msgspec.Struct], ...]) -> dict[str, object]:
    """Build lightweight runtime schema summary for debug/telemetry surfaces.

    Parameters
    ----------
    types : tuple[type[msgspec.Struct], ...]
        Struct types to summarize.

    Returns:
    -------
    dict[str, object]
        Summary with variant_count and variant names.
    """
    infos = inspect.multi_type_info(list(types))
    return {"variant_count": len(infos), "variants": [type(info).__name__ for info in infos]}


def decode_raw_json_blob(data: bytes) -> msgspec.Raw:
    """Defer full decoding for pass-through payload sections.

    Parameters
    ----------
    data : bytes
        Raw JSON bytes.

    Returns:
    -------
    msgspec.Raw
        Opaque raw JSON token for deferred processing.
    """
    return msgspec.json.decode(data, type=msgspec.Raw)


__all__ = [
    "decode_raw_json_blob",
    "struct_field_names",
    "union_schema_summary",
]
