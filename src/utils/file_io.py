"""File I/O utilities with consistent encoding handling."""

from __future__ import annotations

import io
import json
import tokenize
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import msgspec


def read_text(path: Path, *, encoding: str = "utf-8") -> str:
    """Read text file with consistent encoding.

    Parameters
    ----------
    path
        Path to the file.
    encoding
        Text encoding.

    Returns
    -------
    str
        File contents.
    """
    return path.read_text(encoding=encoding)


def read_toml(path: Path) -> Mapping[str, object]:
    """Read and parse a TOML file.

    Parameters
    ----------
    path
        Path to the TOML file.

    Returns
    -------
    Mapping[str, object]
        Parsed TOML content.

    Raises
    ------
    TypeError
        Raised when the TOML content is not a mapping.
    """
    payload = msgspec.toml.decode(path.read_text(encoding="utf-8"), type=object, strict=True)
    if not isinstance(payload, dict):
        msg = f"Expected TOML mapping in {path}, got {type(payload).__name__}."
        raise TypeError(msg)
    return payload


def read_json(path: Path) -> Any:
    """Read and parse a JSON file.

    Parameters
    ----------
    path
        Path to the JSON file.

    Returns
    -------
    Any
        Parsed JSON content.
    """
    return json.loads(path.read_text(encoding="utf-8"))


def read_pyproject_toml(path: Path) -> Mapping[str, object]:
    """Read and parse a pyproject.toml file.

    Parameters
    ----------
    path
        Path to the pyproject.toml file. If a directory is provided,
        looks for pyproject.toml in that directory.

    Returns
    -------
    Mapping[str, object]
        Parsed pyproject.toml content.
    """
    if path.is_dir():
        path /= "pyproject.toml"
    return read_toml(path)


def detect_encoding(data: bytes, *, default: str = "utf-8") -> str:
    """Detect file encoding using tokenize rules."""
    try:
        encoding, _ = tokenize.detect_encoding(io.BytesIO(data).readline)
    except (LookupError, SyntaxError):
        return default
    return encoding


def decode_bytes(data: bytes, *, default: str = "utf-8") -> tuple[str, str | None]:
    """Detect encoding and decode bytes best-effort."""
    encoding = detect_encoding(data, default=default)
    try:
        text = data.decode(encoding, errors="replace")
    except (LookupError, UnicodeError):
        return encoding, None
    return encoding, text


__all__ = [
    "decode_bytes",
    "detect_encoding",
    "read_json",
    "read_pyproject_toml",
    "read_text",
    "read_toml",
]
