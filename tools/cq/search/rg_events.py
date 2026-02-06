"""Typed ripgrep JSON event decoding."""

from __future__ import annotations

from typing import Any

import msgspec


class RgEvent(msgspec.Struct, frozen=True):
    """Minimal ripgrep JSON event."""

    type: str
    data: dict[str, Any] | None = None


_RG_DECODER = msgspec.json.Decoder(type=RgEvent)


def decode_rg_event(line: str | bytes) -> RgEvent | None:
    """Decode a ripgrep JSON event line.

    Returns:
    -------
    RgEvent | None
        Decoded event, or None when decoding fails.
    """
    try:
        if isinstance(line, str):
            line = line.encode("utf-8")
        return _RG_DECODER.decode(line)
    except (msgspec.DecodeError, msgspec.ValidationError):
        return None


__all__ = [
    "RgEvent",
    "decode_rg_event",
]
