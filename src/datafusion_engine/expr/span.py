"""Span normalization helpers shared across DataFusion and normalize."""

from __future__ import annotations

ENC_UTF8: int = 1
ENC_UTF16: int = 2
ENC_UTF32: int = 3
DEFAULT_POSITION_ENCODING: int = ENC_UTF32


def normalize_col_unit_value(value: object | None) -> str:
    """Normalize a col-unit value to one of: byte/utf8/utf16/utf32.

    Returns
    -------
    str
        Normalized col-unit string.
    """
    if isinstance(value, int):
        return col_unit_from_int(value)
    if isinstance(value, str):
        return col_unit_from_text(value)
    return "utf32"


def col_unit_from_int(value: int) -> str:
    """Return a col-unit string for a numeric encoding.

    Returns
    -------
    str
        Normalized col-unit string.
    """
    encoding_map: dict[int, str] = {
        ENC_UTF8: "utf8",
        ENC_UTF16: "utf16",
        ENC_UTF32: "utf32",
    }
    return encoding_map.get(value, "utf32")


def col_unit_from_text(value: str) -> str:
    """Return a col-unit string for a text encoding description.

    Returns
    -------
    str
        Normalized col-unit string.
    """
    text = value.strip().lower()
    if text.isdigit():
        return col_unit_from_int(int(text))
    if "byte" in text:
        return "byte"
    if "utf8" in text:
        return "utf8"
    if "utf16" in text:
        return "utf16"
    if "utf32" in text:
        return "utf32"
    return "utf32"


def encoding_from_unit(unit: str) -> int:
    """Return encoding enum for a normalized col-unit string.

    Returns
    -------
    int
        Encoding enum value.
    """
    if unit == "utf8":
        return ENC_UTF8
    if unit == "utf16":
        return ENC_UTF16
    return ENC_UTF32


__all__ = [
    "DEFAULT_POSITION_ENCODING",
    "ENC_UTF8",
    "ENC_UTF16",
    "ENC_UTF32",
    "col_unit_from_int",
    "col_unit_from_text",
    "encoding_from_unit",
    "normalize_col_unit_value",
]
