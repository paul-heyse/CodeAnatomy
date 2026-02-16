"""Cache helpers for schema introspection."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import cast

from serde_msgspec import to_builtins
from utils.hashing import hash_msgpack_canonical

type SchemaMapping = dict[str, dict[str, dict[str, dict[str, str]]]]


def _normalize_schema_map_payload(value: object) -> dict[str, dict[str, str]]:
    if not isinstance(value, Mapping):
        msg = "schema_map() must return a mapping payload."
        raise TypeError(msg)
    normalized: dict[str, dict[str, str]] = {}
    for table_name, columns in value.items():
        if not isinstance(columns, Mapping):
            continue
        normalized[str(table_name)] = {str(key): str(val) for key, val in columns.items()}
    return normalized


def schema_map_fingerprint_from_mapping(mapping: SchemaMapping) -> str:
    """Compute schema-map fingerprint from a prebuilt mapping.

    Returns:
    -------
    str
        Deterministic fingerprint for the schema mapping payload.
    """
    return hash_msgpack_canonical(to_builtins(mapping))


def schema_map_fingerprint(introspector: object) -> str:
    """Compute fingerprint for schema map payload.

    Returns:
    -------
    str
        Deterministic fingerprint for the introspector schema map.

    Raises:
        TypeError: If ``introspector`` does not expose a callable ``schema_map``.
    """
    schema_map = getattr(introspector, "schema_map", None)
    if not callable(schema_map):
        msg = "schema_map_fingerprint requires an introspector with schema_map()."
        raise TypeError(msg)
    mapping = _normalize_schema_map_payload(schema_map())
    wrapped: SchemaMapping = {"default": {"default": mapping}}
    return schema_map_fingerprint_from_mapping(wrapped)


def catalogs_snapshot(introspector: object) -> list[dict[str, object]]:
    """Return cached catalog snapshot payload.

    Returns:
    -------
    list[dict[str, object]]
        De-duplicated catalog rows.

    Raises:
        TypeError: If ``introspector`` does not expose a callable ``schemata_snapshot``.
    """
    snapshot = getattr(introspector, "schemata_snapshot", None)
    if not callable(snapshot):
        msg = "catalogs_snapshot requires an introspector with schemata_snapshot()."
        raise TypeError(msg)
    raw_rows = snapshot()
    if not isinstance(raw_rows, Iterable):
        msg = "schemata_snapshot() must return an iterable payload."
        raise TypeError(msg)
    seen: set[str] = set()
    rows: list[dict[str, object]] = []
    for row in cast("Iterable[object]", raw_rows):
        if not isinstance(row, Mapping):
            continue
        name = row.get("catalog_name")
        if name is None:
            continue
        catalog = str(name)
        if catalog in seen:
            continue
        seen.add(catalog)
        rows.append({"catalog_name": catalog})
    return rows


__all__ = ["catalogs_snapshot", "schema_map_fingerprint", "schema_map_fingerprint_from_mapping"]
