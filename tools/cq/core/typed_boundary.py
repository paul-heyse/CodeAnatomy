"""Shared typed boundary conversion helpers and error taxonomy."""

from __future__ import annotations

from typing import cast

import msgspec


class BoundaryDecodeError(RuntimeError):
    """Raised when payload conversion fails at CQ boundaries."""


def _raise_boundary_error(exc: Exception) -> BoundaryDecodeError:
    return BoundaryDecodeError(str(exc))


def convert_strict[T](
    payload: object,
    *,
    type_: type[T] | object,
    from_attributes: bool = False,
) -> T:
    """Convert payload with strict msgspec semantics.

    Returns:
        Decoded payload typed as ``T``.

    Raises:
        BoundaryDecodeError: If conversion fails.
    """
    try:
        return cast(
            "T",
            msgspec.convert(
                payload,
                type=type_,
                strict=True,
                from_attributes=from_attributes,
            ),
        )
    except (msgspec.ValidationError, msgspec.DecodeError, TypeError, ValueError) as exc:
        raise _raise_boundary_error(exc) from exc


def convert_lax[T](
    payload: object,
    *,
    type_: type[T] | object,
    from_attributes: bool = False,
) -> T:
    """Convert payload with lax msgspec semantics.

    Returns:
        Decoded payload typed as ``T``.

    Raises:
        BoundaryDecodeError: If conversion fails.
    """
    try:
        return cast(
            "T",
            msgspec.convert(
                payload,
                type=type_,
                strict=False,
                from_attributes=from_attributes,
            ),
        )
    except (msgspec.ValidationError, msgspec.DecodeError, TypeError, ValueError) as exc:
        raise _raise_boundary_error(exc) from exc


def decode_json_strict[T](payload: bytes | str, *, type_: type[T] | object) -> T:
    """Decode JSON payload with strict schema validation.

    Returns:
        Decoded payload typed as ``T``.

    Raises:
        BoundaryDecodeError: If decoding fails.
    """
    try:
        raw = payload.encode("utf-8") if isinstance(payload, str) else payload
        return cast("T", msgspec.json.decode(raw, type=type_, strict=True))
    except (msgspec.ValidationError, msgspec.DecodeError, TypeError, ValueError) as exc:
        raise _raise_boundary_error(exc) from exc


def decode_toml_strict[T](payload: bytes | str, *, type_: type[T] | object) -> T:
    """Decode TOML payload with strict schema validation.

    Returns:
        Decoded payload typed as ``T``.

    Raises:
        BoundaryDecodeError: If decoding fails.
    """
    try:
        raw = payload.encode("utf-8") if isinstance(payload, str) else payload
        return cast("T", msgspec.toml.decode(raw, type=type_, strict=True))
    except (msgspec.ValidationError, msgspec.DecodeError, TypeError, ValueError) as exc:
        raise _raise_boundary_error(exc) from exc


def decode_yaml_strict[T](payload: bytes | str, *, type_: type[T] | object) -> T:
    """Decode YAML payload with strict schema validation.

    Returns:
        Decoded payload typed as ``T``.

    Raises:
        BoundaryDecodeError: If decoding fails.
    """
    try:
        raw = payload.encode("utf-8") if isinstance(payload, str) else payload
        return cast("T", msgspec.yaml.decode(raw, type=type_, strict=True))
    except (msgspec.ValidationError, msgspec.DecodeError, TypeError, ValueError) as exc:
        raise _raise_boundary_error(exc) from exc


__all__ = [
    "BoundaryDecodeError",
    "convert_lax",
    "convert_strict",
    "decode_json_strict",
    "decode_toml_strict",
    "decode_yaml_strict",
]
