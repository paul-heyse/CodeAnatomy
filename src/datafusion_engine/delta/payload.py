"""Shared helpers for Delta payload normalization."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from datafusion_engine.delta.specs import DeltaCdfOptionsSpec, DeltaCommitOptionsSpec
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from serde_msgspec import dumps_msgpack, to_builtins

if TYPE_CHECKING:
    from datafusion_engine.generated.delta_types import DeltaCommitOptions
    from storage.deltalake.delta import DeltaCdfOptions


def schema_ipc_payload(schema: object | None) -> bytes | None:
    """Serialize an Arrow schema to IPC bytes when supported.

    Args:
        schema: Description.

    Returns:
        bytes | None: Result.

    Raises:
        DataFusionEngineError: If the operation cannot be completed.
    """
    if schema is None:
        return None
    serialize = getattr(schema, "serialize", None)
    if not callable(serialize):
        msg = "Delta scan schema does not support serialize()."
        raise DataFusionEngineError(msg, kind=ErrorKind.ARROW)
    buffer = serialize()
    to_bytes = getattr(buffer, "to_pybytes", None)
    if not callable(to_bytes):
        msg = "Delta scan schema serialize() did not return a compatible buffer."
        raise DataFusionEngineError(msg, kind=ErrorKind.ARROW)
    payload = to_bytes()
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, bytearray):
        return bytes(payload)
    if isinstance(payload, memoryview):
        return payload.tobytes()
    msg = "Delta scan schema serialize() returned unsupported buffer type."
    raise DataFusionEngineError(msg, kind=ErrorKind.ARROW)


def commit_payload(
    options: DeltaCommitOptions | DeltaCommitOptionsSpec | None,
) -> tuple[
    list[tuple[str, str]] | None,
    str | None,
    int | None,
    int | None,
    int | None,
    bool | None,
]:
    """Convert commit options into Rust extension payload values.

    Returns:
    -------
    tuple[list[tuple[str, str]] | None, str | None, int | None, int | None, int | None, bool | None]
        Commit metadata and idempotent transaction fields for Rust calls.
    """
    if options is None:
        return None, None, None, None, None, None
    metadata_items = sorted((str(key), str(value)) for key, value in options.metadata.items())
    metadata_payload = metadata_items or None
    app_id: str | None = None
    app_version: int | None = None
    app_last_updated: int | None = None
    if options.app_transaction is not None:
        app_id = options.app_transaction.app_id
        app_version = options.app_transaction.version
        app_last_updated = options.app_transaction.last_updated
    return (
        metadata_payload,
        app_id,
        app_version,
        app_last_updated,
        options.max_retries,
        options.create_checkpoint,
    )


def cdf_options_payload(
    options: DeltaCdfOptions | DeltaCdfOptionsSpec | None,
) -> dict[str, object]:
    """Return a normalized payload for CDF options.

    Returns:
    -------
    dict[str, object]
        Payload describing the requested CDF window.
    """
    if options is None:
        return {
            "starting_version": None,
            "ending_version": None,
            "starting_timestamp": None,
            "ending_timestamp": None,
            "allow_out_of_range": False,
        }
    return {
        "starting_version": options.starting_version,
        "ending_version": options.ending_version,
        "starting_timestamp": options.starting_timestamp,
        "ending_timestamp": options.ending_timestamp,
        "allow_out_of_range": options.allow_out_of_range,
    }


def msgpack_payload(value: object) -> bytes:
    """Encode a value as msgpack for diagnostic payloads.

    Returns:
    -------
    bytes
        MessagePack-encoded payload.
    """
    return dumps_msgpack(to_builtins(value, str_keys=True))


def msgpack_or_none(value: object | None) -> bytes | None:
    """Return MessagePack payload when a value is provided.

    Returns:
    -------
    bytes | None
        MessagePack payload when value is present, otherwise None.
    """
    if value is None:
        return None
    return msgpack_payload(value)


def string_list(value: object) -> list[str]:
    """Normalize a value into a list of strings for payloads.

    Returns:
    -------
    list[str]
        Normalized string list.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, memoryview)):
        return [str(item) for item in value]
    return [str(value)]


def string_map(value: object) -> dict[str, str]:
    """Normalize a mapping payload into a string-keyed dict.

    Returns:
    -------
    dict[str, str]
        Normalized string mapping.
    """
    if isinstance(value, Mapping):
        return {str(key): str(item) for key, item in value.items()}
    return {}


def settings_bool(settings: Mapping[str, str], key: str) -> bool | None:
    """Parse a settings value as a boolean when possible.

    Returns:
    -------
    bool | None
        Parsed boolean value when available.
    """
    value = settings.get(key)
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y", "on"}:
        return True
    if normalized in {"false", "0", "no", "n", "off"}:
        return False
    return None


__all__ = [
    "cdf_options_payload",
    "commit_payload",
    "msgpack_or_none",
    "msgpack_payload",
    "schema_ipc_payload",
    "settings_bool",
    "string_list",
    "string_map",
]
