"""Shared msgspec inspection helpers."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Literal, cast

import msgspec

_NOT_HANDLED = object()
_RUNTIME_ADDRESS_RE = re.compile(r" at 0x[0-9A-Fa-f]+")

type MappingMode = Literal["recurse", "stringify"]


def _is_msgspec_inspect(obj: object) -> bool:
    return obj.__class__.__module__ == "msgspec.inspect"


def _handle_sentinel(obj: object) -> object:
    if obj is msgspec.NODEFAULT:
        return "NODEFAULT"
    if obj is ...:
        return "..."
    return _NOT_HANDLED


def _handle_type_like(obj: object) -> object:
    if isinstance(obj, type):
        return f"{obj.__module__}.{obj.__qualname__}"
    if callable(obj) and hasattr(obj, "__qualname__"):
        return f"{obj.__module__}.{obj.__qualname__}"
    return _NOT_HANDLED


def _normalize_repr(text: str) -> str:
    """Remove runtime-specific address fragments from repr-like strings.

    Returns:
    -------
    str
        Normalized string representation without runtime addresses.
    """
    return _RUNTIME_ADDRESS_RE.sub("", text)


def stable_stringify(
    obj: object,
    *,
    fallback: Callable[[object], object] | None = None,
) -> str:
    """Stringify values while removing nondeterministic runtime addresses.

    Returns:
    -------
    str
        Stable string representation with address fragments removed.
    """
    type_like = _handle_type_like(obj)
    if isinstance(type_like, str):
        return type_like
    rendered = str(obj) if fallback is None or fallback is stable_stringify else fallback(obj)
    if not isinstance(rendered, str):
        rendered = str(rendered)
    return _normalize_repr(rendered)


def _handle_struct(
    obj: object,
    *,
    str_keys: bool,
    mapping_mode: MappingMode,
    fallback: Callable[[object], object] | None,
    seen: set[int],
) -> object:
    fields = getattr(obj, "__struct_fields__", None)
    if not (_is_msgspec_inspect(obj) or fields is not None):
        return _NOT_HANDLED
    if fields is None:
        return _NOT_HANDLED
    fields = cast("Sequence[str]", fields)
    obj_id = id(obj)
    seen.add(obj_id)
    payload = {
        field: inspect_to_builtins(
            getattr(obj, field),
            str_keys=str_keys,
            mapping_mode=mapping_mode,
            fallback=fallback,
            _seen=seen,
        )
        for field in fields
    }
    seen.remove(obj_id)
    return payload


def _handle_mapping(
    obj: object,
    *,
    str_keys: bool,
    mapping_mode: MappingMode,
    fallback: Callable[[object], object] | None,
    seen: set[int],
) -> object:
    if not isinstance(obj, Mapping):
        return _NOT_HANDLED
    if mapping_mode == "stringify":
        return stable_stringify(obj, fallback=fallback)
    obj_id = id(obj)
    seen.add(obj_id)
    items = obj.items()
    if str_keys:
        items = [(str(key), value) for key, value in items]
    sorted_items = sorted(items, key=lambda item: str(item[0]))
    payload = {
        key: inspect_to_builtins(
            value,
            str_keys=str_keys,
            mapping_mode=mapping_mode,
            fallback=fallback,
            _seen=seen,
        )
        for key, value in sorted_items
    }
    seen.remove(obj_id)
    return payload


def _handle_sequence(
    obj: object,
    *,
    str_keys: bool,
    mapping_mode: MappingMode,
    fallback: Callable[[object], object] | None,
    seen: set[int],
) -> object:
    context = (str_keys, mapping_mode, fallback, seen)
    if isinstance(obj, list):
        return _sequence_payload(
            obj,
            factory=list,
            context=context,
        )
    if isinstance(obj, tuple):
        return _sequence_payload(
            obj,
            factory=tuple,
            context=context,
        )
    return _NOT_HANDLED


def _sequence_payload(
    items: Sequence[object],
    *,
    factory: Callable[[Sequence[object]], object],
    context: tuple[bool, MappingMode, Callable[[object], object] | None, set[int]],
) -> object:
    str_keys, mapping_mode, fallback, seen = context
    if mapping_mode == "stringify":
        return stable_stringify(items, fallback=fallback)
    obj_id = id(items)
    seen.add(obj_id)
    payload = [
        inspect_to_builtins(
            item,
            str_keys=str_keys,
            mapping_mode=mapping_mode,
            fallback=fallback,
            _seen=seen,
        )
        for item in items
    ]
    seen.remove(obj_id)
    return factory(payload)


def _handle_path(obj: object) -> object:
    if isinstance(obj, Path):
        return str(obj)
    return _NOT_HANDLED


def _handle_raw_bytes(obj: object) -> object:
    if isinstance(obj, msgspec.Raw):
        return bytes(obj).hex()
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return bytes(obj).hex()
    return _NOT_HANDLED


def inspect_to_builtins(
    obj: object,
    *,
    str_keys: bool = False,
    mapping_mode: MappingMode = "recurse",
    fallback: Callable[[object], object] | None = None,
    _seen: set[int] | None = None,
) -> object:
    """Convert msgspec inspection objects into builtin-friendly payloads.

    Returns:
    -------
    object
        Builtin-friendly representation of the inspection payload.
    """
    seen = _seen if _seen is not None else set()
    obj_id = id(obj)
    if obj_id in seen:
        return "..."
    for handler in (
        _handle_sentinel,
        _handle_type_like,
        lambda value: _handle_struct(
            value,
            str_keys=str_keys,
            mapping_mode=mapping_mode,
            fallback=fallback,
            seen=seen,
        ),
        lambda value: _handle_mapping(
            value,
            str_keys=str_keys,
            mapping_mode=mapping_mode,
            fallback=fallback,
            seen=seen,
        ),
        lambda value: _handle_sequence(
            value,
            str_keys=str_keys,
            mapping_mode=mapping_mode,
            fallback=fallback,
            seen=seen,
        ),
        _handle_path,
        _handle_raw_bytes,
    ):
        result = handler(obj)
        if result is not _NOT_HANDLED:
            return result
    if fallback is None:
        return obj
    return stable_stringify(obj, fallback=fallback)


__all__ = ["inspect_to_builtins", "stable_stringify"]
