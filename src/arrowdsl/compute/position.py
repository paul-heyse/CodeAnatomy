"""Position encoding helpers for span normalization."""

from __future__ import annotations

ENC_UTF8 = 1
ENC_UTF16 = 2
ENC_UTF32 = 3
DEFAULT_POSITION_ENCODING = ENC_UTF32
VALID_POSITION_ENCODINGS: frozenset[int] = frozenset((ENC_UTF8, ENC_UTF16, ENC_UTF32))


def normalize_position_encoding(value: object | None) -> int:
    """Normalize position encoding values to SCIP enum integers.

    Returns
    -------
    int
        Normalized encoding enum value.
    """
    encoding = DEFAULT_POSITION_ENCODING
    if value is None:
        return encoding
    if isinstance(value, int):
        return value if value in VALID_POSITION_ENCODINGS else encoding
    if isinstance(value, str):
        text = value.strip().upper()
        if text.isdigit():
            value_int = int(text)
            return value_int if value_int in VALID_POSITION_ENCODINGS else encoding
        if "UTF8" in text:
            encoding = ENC_UTF8
        elif "UTF16" in text:
            encoding = ENC_UTF16
        elif "UTF32" in text:
            encoding = ENC_UTF32
    return encoding


__all__ = [
    "DEFAULT_POSITION_ENCODING",
    "ENC_UTF8",
    "ENC_UTF16",
    "ENC_UTF32",
    "normalize_position_encoding",
]
