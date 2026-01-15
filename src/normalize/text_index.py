"""Shared text index primitives for normalization."""

from __future__ import annotations

from dataclasses import dataclass

from arrowdsl.compute.position_encoding import (
    DEFAULT_POSITION_ENCODING,
    ENC_UTF8,
    ENC_UTF16,
    ENC_UTF32,
    normalize_position_encoding,
)


@dataclass(frozen=True)
class FileTextIndex:
    """Per-file text index for byte-span conversion."""

    file_id: str
    path: str
    file_sha256: str | None
    encoding: str | None
    text: str
    lines: list[str]
    line_start_utf8: list[int]


@dataclass(frozen=True)
class RepoTextIndex:
    """Repo-wide indices for fast lookup."""

    by_file_id: dict[str, FileTextIndex]
    by_path: dict[str, FileTextIndex]


def row_value_int(value: object | None) -> int | None:
    """Normalize numeric-like values into ints.

    Returns
    -------
    int | None
        Normalized integer value or ``None``.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        return int(raw) if raw.isdigit() else None
    return None


def file_index(
    repo_index: RepoTextIndex,
    file_id: object | None,
    path: object | None,
) -> FileTextIndex | None:
    """Resolve a file index by file_id or path.

    Returns
    -------
    FileTextIndex | None
        Matching file index or ``None``.
    """
    if isinstance(file_id, str):
        return repo_index.by_file_id.get(file_id)
    if isinstance(path, str):
        return repo_index.by_path.get(path)
    return None


__all__ = [
    "DEFAULT_POSITION_ENCODING",
    "ENC_UTF8",
    "ENC_UTF16",
    "ENC_UTF32",
    "FileTextIndex",
    "RepoTextIndex",
    "file_index",
    "normalize_position_encoding",
    "row_value_int",
]
