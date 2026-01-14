"""Build per-file line index tables from repo files."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.core.ids import iter_arrays
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import column_or_null, table_from_arrays
from extract.registry_specs import dataset_schema


def build_line_index_table(
    repo_files: TableLike,
    *,
    ctx: object | None = None,
) -> TableLike:
    """Build a file line index table from repo files.

    Returns
    -------
    TableLike
        Table of line offsets and text rows keyed by file_id + line_no.
    """
    _ = ctx
    file_ids: list[str] = []
    paths: list[str] = []
    line_nos: list[int] = []
    line_start_bytes: list[int] = []
    line_end_bytes: list[int] = []
    line_texts: list[str] = []
    newline_kinds: list[str] = []

    arrays = [
        column_or_null(repo_files, "file_id", pa.string()),
        column_or_null(repo_files, "path", pa.string()),
        column_or_null(repo_files, "encoding", pa.string()),
        column_or_null(repo_files, "text", pa.string()),
        column_or_null(repo_files, "bytes", pa.binary()),
    ]
    for file_id, path, encoding, text_value, bytes_value in iter_arrays(arrays):
        if not isinstance(file_id, str) or not isinstance(path, str):
            continue
        text = _decode_repo_text(text_value, bytes_value, encoding)
        if text is None:
            continue
        offset = 0
        for line_no, line in enumerate(text.splitlines(keepends=True)):
            line_bytes = line.encode("utf-8")
            line_start = offset
            offset += len(line_bytes)
            file_ids.append(file_id)
            paths.append(path)
            line_nos.append(line_no)
            line_start_bytes.append(line_start)
            line_end_bytes.append(offset)
            line_texts.append(line)
            newline_kinds.append(_newline_kind(line))

    schema = dataset_schema("file_line_index_v1")
    columns = {
        "file_id": pa.array(file_ids, type=pa.string()),
        "path": pa.array(paths, type=pa.string()),
        "line_no": pa.array(line_nos, type=pa.int32()),
        "line_start_byte": pa.array(line_start_bytes, type=pa.int64()),
        "line_end_byte": pa.array(line_end_bytes, type=pa.int64()),
        "line_text": pa.array(line_texts, type=pa.string()),
        "newline_kind": pa.array(newline_kinds, type=pa.string()),
    }
    return table_from_arrays(schema, columns=columns, num_rows=len(line_nos))


def _decode_repo_text(
    text_value: object | None,
    bytes_value: object | None,
    encoding_value: object | None,
) -> str | None:
    if isinstance(text_value, str) and text_value:
        return text_value
    if isinstance(bytes_value, (bytes, bytearray, memoryview)):
        encoding = encoding_value if isinstance(encoding_value, str) else "utf-8"
        try:
            return bytes(bytes_value).decode(encoding, errors="replace")
        except (LookupError, UnicodeError):
            return bytes(bytes_value).decode("utf-8", errors="replace")
    return None


def _newline_kind(line: str) -> str:
    if line.endswith("\r\n"):
        return "CRLF"
    if line.endswith("\n"):
        return "LF"
    if line.endswith("\r"):
        return "CR"
    return "NONE"


__all__ = ["build_line_index_table"]
