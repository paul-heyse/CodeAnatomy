"""Shared file context utilities for extractors."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class FileContext:
    """Canonical file identity and payload context for extractors."""

    file_id: str
    path: str
    file_sha256: str | None
    encoding: str | None = None
    text: str | None = None
    data: bytes | None = None

    @classmethod
    def from_repo_row(cls, row: Mapping[str, object]) -> FileContext:
        """Build a FileContext from a repo_files row.

        Parameters
        ----------
        row:
            Row mapping from repo_files output.

        Returns
        -------
        FileContext
            Parsed file context.
        """
        file_id_raw = row.get("file_id")
        path_raw = row.get("path")
        sha_raw = row.get("file_sha256")
        encoding_raw = row.get("encoding")
        text_raw = row.get("text")
        data_raw = row.get("bytes")

        file_id = file_id_raw if isinstance(file_id_raw, str) else ""
        path = path_raw if isinstance(path_raw, str) else ""
        file_sha256 = sha_raw if isinstance(sha_raw, str) else None
        encoding = encoding_raw if isinstance(encoding_raw, str) else None
        text = text_raw if isinstance(text_raw, str) else None
        data = bytes(data_raw) if isinstance(data_raw, (bytes, bytearray, memoryview)) else None

        return cls(
            file_id=file_id,
            path=path,
            file_sha256=file_sha256,
            encoding=encoding,
            text=text,
            data=data,
        )
