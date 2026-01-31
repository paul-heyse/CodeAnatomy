"""File index extraction subpackage.

This subpackage provides extractors for file-level index tables
such as line indices needed for byte offset conversion.
"""

from __future__ import annotations

from extract.extractors.file_index.line_index import (
    FILE_LINE_INDEX_SCHEMA,
    LineIndexOptions,
    extract_file_line_index,
    extract_line_index_rows,
    file_line_index_query,
    scan_file_line_index,
    scan_file_line_index_plan,
)

__all__ = [
    "FILE_LINE_INDEX_SCHEMA",
    "LineIndexOptions",
    "extract_file_line_index",
    "extract_line_index_rows",
    "file_line_index_query",
    "scan_file_line_index",
    "scan_file_line_index_plan",
]
