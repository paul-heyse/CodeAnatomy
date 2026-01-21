"""Shared text index primitives for normalization."""

from __future__ import annotations

ENC_UTF8 = 1
ENC_UTF16 = 2
ENC_UTF32 = 3
DEFAULT_POSITION_ENCODING = ENC_UTF32

__all__ = [
    "DEFAULT_POSITION_ENCODING",
    "ENC_UTF8",
    "ENC_UTF16",
    "ENC_UTF32",
]
