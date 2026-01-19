"""Shared helpers for registry metadata payloads."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from registry_common.arrow_payloads import ipc_table, payload_ipc_bytes

if TYPE_CHECKING:
    from arrowdsl.core.expr_types import ScalarValue
    from arrowdsl.core.interop import ScalarLike

METADATA_PAYLOAD_VERSION: int = 1

_MAP_ENTRY_TYPE = pa.struct(
    [
        pa.field("key", pa.string()),
        pa.field("value", pa.string()),
    ]
)
_MAP_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("entries", pa.list_(_MAP_ENTRY_TYPE)),
    ]
)
_LIST_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("entries", pa.list_(pa.string())),
    ]
)
_SCALAR_ENTRY_TYPE = pa.struct(
    [
        pa.field("key", pa.string()),
        pa.field("value_kind", pa.string()),
        pa.field("value_bool", pa.bool_()),
        pa.field("value_int", pa.int64()),
        pa.field("value_float", pa.float64()),
        pa.field("value_string", pa.string()),
        pa.field("value_binary", pa.binary()),
    ]
)
_SCALAR_MAP_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("entries", pa.list_(_SCALAR_ENTRY_TYPE)),
    ]
)


@dataclass(frozen=True)
class EvidenceMetadataSpec:
    """Specification for common evidence metadata fields."""

    evidence_family: str
    coordinate_system: str
    ambiguity_policy: str
    superior_rank: int
    span_coord_policy: bytes | None = None
    streaming_safe: bool | None = None
    pipeline_breaker: bool | None = None


def evidence_metadata(
    *,
    spec: EvidenceMetadataSpec,
    extra: Mapping[bytes, bytes] | None = None,
) -> dict[bytes, bytes]:
    """Return evidence metadata with common keys applied.

    Returns
    -------
    dict[bytes, bytes]
        Metadata payload with evidence keys applied.
    """
    meta: dict[bytes, bytes] = {
        b"evidence_family": spec.evidence_family.encode("utf-8"),
        b"coordinate_system": spec.coordinate_system.encode("utf-8"),
        b"ambiguity_policy": spec.ambiguity_policy.encode("utf-8"),
        b"superior_rank": str(spec.superior_rank).encode("utf-8"),
    }
    if spec.span_coord_policy is not None:
        meta[b"span_coord_policy"] = spec.span_coord_policy
    if spec.streaming_safe is not None:
        meta[b"streaming_safe"] = str(spec.streaming_safe).lower().encode("utf-8")
    if spec.pipeline_breaker is not None:
        meta[b"pipeline_breaker"] = str(spec.pipeline_breaker).lower().encode("utf-8")
    if extra:
        meta.update(extra)
    return meta


def metadata_map_bytes(entries: Mapping[str, str]) -> bytes:
    """Encode string mapping metadata as IPC bytes.

    Parameters
    ----------
    entries:
        Mapping of string keys to string values.

    Returns
    -------
    bytes
        IPC bytes for the mapping payload.
    """
    payload = {
        "version": METADATA_PAYLOAD_VERSION,
        "entries": [
            {"key": str(key), "value": str(value)}
            for key, value in sorted(entries.items(), key=lambda item: str(item[0]))
        ],
    }
    return payload_ipc_bytes(payload, _MAP_SCHEMA)


def metadata_list_bytes(entries: Sequence[str]) -> bytes:
    """Encode string list metadata as IPC bytes.

    Parameters
    ----------
    entries:
        Sequence of string values.

    Returns
    -------
    bytes
        IPC bytes for the list payload.
    """
    payload = {"version": METADATA_PAYLOAD_VERSION, "entries": [str(item) for item in entries]}
    return payload_ipc_bytes(payload, _LIST_SCHEMA)


def metadata_scalar_map_bytes(entries: Mapping[str, ScalarValue]) -> bytes:
    """Encode scalar mapping metadata as IPC bytes.

    Parameters
    ----------
    entries:
        Mapping of scalar values keyed by string.

    Returns
    -------
    bytes
        IPC bytes for the scalar mapping payload.
    """
    payload = {
        "version": METADATA_PAYLOAD_VERSION,
        "entries": [
            _scalar_entry(str(key), value)
            for key, value in sorted(entries.items(), key=lambda item: str(item[0]))
        ],
    }
    return payload_ipc_bytes(payload, _SCALAR_MAP_SCHEMA)


def decode_metadata_map(payload: bytes) -> dict[str, str]:
    """Decode IPC metadata bytes into a string mapping.

    Parameters
    ----------
    payload:
        IPC payload bytes.

    Returns
    -------
    dict[str, str]
        Decoded string mapping.
    """
    row = _single_payload_row(payload)
    entries = _entries_list(row)
    results: dict[str, str] = {}
    for entry in entries:
        mapping = _entry_mapping(entry)
        results[str(mapping["key"])] = str(mapping["value"])
    return results


def decode_metadata_list(payload: bytes) -> list[str]:
    """Decode IPC metadata bytes into a list of strings.

    Parameters
    ----------
    payload:
        IPC payload bytes.

    Returns
    -------
    list[str]
        Decoded list of strings.
    """
    row = _single_payload_row(payload)
    return [str(item) for item in _entries_list(row)]


def decode_metadata_scalar_map(payload: bytes) -> dict[str, ScalarValue]:
    """Decode IPC metadata bytes into a scalar mapping.

    Parameters
    ----------
    payload:
        IPC payload bytes.

    Returns
    -------
    dict[str, ScalarValue]
        Decoded scalar mapping.
    """
    row = _single_payload_row(payload)
    entries = _entries_list(row)
    results: dict[str, ScalarValue] = {}
    for entry in entries:
        mapping = _entry_mapping(entry)
        key = str(mapping["key"])
        results[key] = _scalar_value(mapping)
    return results


def _single_payload_row(payload: bytes) -> dict[str, object]:
    table = ipc_table(payload)
    if table.num_rows != 1:
        msg = "Metadata payload must contain exactly one row."
        raise ValueError(msg)
    row = cast("dict[str, object]", table.to_pylist()[0])
    version = row.get("version")
    if version != METADATA_PAYLOAD_VERSION:
        msg = f"Unsupported metadata payload version: {version!r}."
        raise ValueError(msg)
    return row


def _scalar_entry(key: str, value: ScalarValue) -> dict[str, object]:
    normalized = _normalize_scalar(value)
    entry: dict[str, object] = {
        "key": key,
        "value_kind": "null",
        "value_bool": None,
        "value_int": None,
        "value_float": None,
        "value_string": None,
        "value_binary": None,
    }
    if normalized is None:
        return entry
    if isinstance(normalized, bool):
        entry["value_kind"] = "bool"
        entry["value_bool"] = normalized
        return entry
    if isinstance(normalized, int):
        entry["value_kind"] = "int64"
        entry["value_int"] = normalized
        return entry
    if isinstance(normalized, float):
        entry["value_kind"] = "float64"
        entry["value_float"] = normalized
        return entry
    if isinstance(normalized, bytes):
        entry["value_kind"] = "binary"
        entry["value_binary"] = normalized
        return entry
    entry["value_kind"] = "string"
    entry["value_string"] = str(normalized)
    return entry


def _entries_list(row: Mapping[str, object]) -> list[object]:
    entries = row.get("entries")
    if entries is None:
        return []
    if isinstance(entries, list):
        return entries
    msg = "Metadata payload entries must be a list."
    raise TypeError(msg)


def _entry_mapping(entry: object) -> Mapping[str, object]:
    if isinstance(entry, Mapping):
        return entry
    msg = "Metadata payload entry must be a mapping."
    raise TypeError(msg)


def _scalar_value(entry: Mapping[str, object]) -> ScalarValue:
    kind = entry.get("value_kind")
    if kind == "bool":
        return cast("bool | None", entry.get("value_bool"))
    if kind == "int64":
        return cast("int | None", entry.get("value_int"))
    if kind == "float64":
        return cast("float | None", entry.get("value_float"))
    if kind == "binary":
        return cast("bytes | None", entry.get("value_binary"))
    if kind == "string":
        return cast("str | None", entry.get("value_string"))
    return None


def _normalize_scalar(value: ScalarValue) -> object | None:
    if value is None:
        return None
    if _is_scalar_like(value):
        return cast("ScalarLike", value).as_py()
    return value


def _is_scalar_like(value: object) -> bool:
    return callable(getattr(value, "as_py", None))


__all__ = [
    "EvidenceMetadataSpec",
    "decode_metadata_list",
    "decode_metadata_map",
    "decode_metadata_scalar_map",
    "evidence_metadata",
    "metadata_list_bytes",
    "metadata_map_bytes",
    "metadata_scalar_map_bytes",
]
