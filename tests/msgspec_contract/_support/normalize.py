"""Normalization helpers for msgspec contract tests."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence

import msgspec

from serde_msgspec import validation_error_payload

_VALIDATION_RE = re.compile(r"^(?P<summary>.*?)(?:\s+-\s+at\s+`(?P<path>[^`]+)`)?$")


def normalize_exception(exc: Exception) -> dict[str, str]:
    """Convert an exception into a snapshot-friendly dict.

    Returns
    -------
    dict[str, str]
        Normalized exception payload.
    """
    if isinstance(exc, msgspec.ValidationError):
        return validation_error_payload(exc)
    message = str(exc).strip()
    match = _VALIDATION_RE.match(message)

    output: dict[str, str] = {"type": exc.__class__.__name__}
    if match:
        summary = (match.group("summary") or "").strip()
        if summary:
            output["summary"] = summary
        path = match.group("path")
        if path:
            output["path"] = path
        return output

    output["summary"] = message
    return output


def normalize_jsonable(value: object) -> object:
    """Convert values into JSON-friendly structures.

    Returns
    -------
    object
        Normalized JSON-compatible value.
    """
    if isinstance(value, Mapping):
        return {str(key): normalize_jsonable(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        return [normalize_jsonable(item) for item in sorted(value, key=str)]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [normalize_jsonable(item) for item in value]
    return value
