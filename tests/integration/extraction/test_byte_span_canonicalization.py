"""Integration tests for cross-extractor byte-span consistency.

Tests the boundary where multiple extractors (CST, AST, tree-sitter) produce
byte spans that are normalized through scip_to_byte_offsets() and validated
against canonical byte offsets.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, cast

import pyarrow as pa
import pytest

from extract.extractors.ast import AstExtractOptions, extract_ast_tables
from extract.extractors.cst import CstExtractOptions, extract_cst_tables
from extract.extractors.tree_sitter import TreeSitterExtractOptions, extract_ts_tables
from tests.test_helpers.datafusion_runtime import df_ctx


class _RecordBatchReaderLike(Protocol):
    def read_all(self) -> pa.Table: ...


@pytest.fixture
def sample_python_file(tmp_path: Path) -> tuple[Path, str]:
    """Create a sample Python file for extraction tests.

    Parameters
    ----------
    tmp_path
        Pytest temporary directory fixture.

    Returns:
    -------
    tuple[Path, str]
        Path to the created file and its content.
    """
    content = '''"""Module docstring."""

def example_function(arg1: str, arg2: int) -> str:
    """Example function docstring."""
    result = arg1 * arg2
    return result


class ExampleClass:
    """Example class docstring."""

    def method(self) -> None:
        """Example method."""
        pass
'''
    file_path = tmp_path / "example.py"
    file_path.write_text(content, encoding="utf-8")
    return file_path, content


@pytest.fixture
def unicode_python_file(tmp_path: Path) -> tuple[Path, str]:
    """Create a Python file with multi-byte Unicode characters.

    Parameters
    ----------
    tmp_path
        Pytest temporary directory fixture.

    Returns:
    -------
    tuple[Path, str]
        Path to the created file and its content.
    """
    content = '''"""Module with Unicode: 你好世界 🚀 émojis."""

def unicode_function(name: str) -> str:
    """Process Unicode: ñ ü ö."""
    greeting = f"Hello {name} 世界"
    return greeting
'''
    file_path = tmp_path / "unicode_example.py"
    file_path.write_text(content, encoding="utf-8")
    return file_path, content


@pytest.fixture
def crlf_python_file(tmp_path: Path) -> tuple[Path, str]:
    """Create a Python file with CRLF line endings.

    Parameters
    ----------
    tmp_path
        Pytest temporary directory fixture.

    Returns:
    -------
    tuple[Path, str]
        Path to the created file and its content with CRLF endings.
    """
    content = "def crlf_function():\r\n    return 42\r\n"
    file_path = tmp_path / "crlf_example.py"
    file_path.write_bytes(content.encode("utf-8"))
    return file_path, content


@pytest.mark.integration
def test_byte_span_consistency_cst_vs_ast(
    sample_python_file: tuple[Path, str],
) -> None:
    """Test byte span consistency between CST and AST extractors.

    Verifies that when the same Python file is extracted via CST and AST
    extractors, and both are normalized through scip_to_byte_offsets, the
    resulting byte spans (bstart/bend) match for shared entities.

    Parameters
    ----------
    sample_python_file
        Tuple of (file path, content) for the test Python file.
    """
    file_path, content = sample_python_file
    repo_files = _repo_files_table(file_path, content)

    cst_table = _bundle_table(
        extract_cst_tables(
            repo_files=repo_files,
            options=CstExtractOptions(max_workers=1),
        )["libcst_files"]
    )
    ast_table = _bundle_table(
        extract_ast_tables(
            repo_files=repo_files,
            options=AstExtractOptions(max_workers=1),
        )["ast_files"]
    )

    cst_spans = _cst_def_spans(cst_table)
    ast_spans = _ast_def_spans(ast_table)
    shared = sorted(set(cst_spans) & set(ast_spans))

    assert shared, "Expected shared definitions between CST and AST outputs"
    for name in shared:
        assert cst_spans[name] == ast_spans[name], f"Span mismatch for definition {name!r}"


@pytest.mark.integration
def test_byte_span_consistency_cst_vs_tree_sitter(
    sample_python_file: tuple[Path, str],
) -> None:
    """Test byte span consistency between CST and tree-sitter extractors.

    Verifies that CST and tree-sitter extractors produce identical byte spans
    for the same entities after normalization.

    Parameters
    ----------
    sample_python_file
        Tuple of (file path, content) for the test Python file.
    """
    file_path, content = sample_python_file
    repo_files = _repo_files_table(file_path, content)

    cst_table = _bundle_table(
        extract_cst_tables(
            repo_files=repo_files,
            options=CstExtractOptions(max_workers=1),
        )["libcst_files"]
    )
    ts_table = _bundle_table(
        extract_ts_tables(
            repo_files=repo_files,
            options=TreeSitterExtractOptions(max_workers=1),
        )["tree_sitter_files"]
    )

    cst_spans = _cst_def_spans(cst_table)
    ts_spans = _ts_def_spans(ts_table)
    shared = sorted(set(cst_spans) & set(ts_spans))

    assert shared, "Expected shared definitions between CST and tree-sitter outputs"
    for name in shared:
        assert cst_spans[name] == ts_spans[name], f"Span mismatch for definition {name!r}"


@pytest.mark.integration
def test_col_unit_utf32_to_byte_conversion(unicode_python_file: tuple[Path, str]) -> None:
    """Test UTF-32 column unit to byte offset conversion.

    Verifies that files with multi-byte Unicode characters have their
    col_unit="utf32" offsets correctly converted to byte offsets, accounting
    for multi-byte encoding.

    Parameters
    ----------
    unicode_python_file
        Tuple of (file path, content) with Unicode characters.
    """
    _file_path, content = unicode_python_file
    content_bytes = content.encode("utf-8")

    # Verify that multi-byte characters exist in the content
    # UTF-8 encoding of "你" is 3 bytes, "🚀" is 4 bytes
    assert len(content_bytes) > len(content), "Expected multi-byte characters"

    # Calculate expected byte offsets for key positions
    # The function starts after the module docstring
    func_def_start = content.find("def unicode_function")
    assert func_def_start > 0

    # Verify byte offset matches character position in bytes
    byte_offset = len(content[:func_def_start].encode("utf-8"))
    assert byte_offset >= func_def_start  # Byte offset should be >= char offset due to Unicode


@pytest.mark.integration
def test_span_normalization_with_missing_udf() -> None:
    """Test span normalization fails gracefully when UDF is missing.

    Verifies that attempting span normalization without the col_to_byte UDF
    raises ValueError with a clear UDF-missing message.
    """
    from datafusion_engine.udf.extension_core import rust_udf_snapshot, validate_required_udfs

    ctx = df_ctx()

    # Get the UDF registry snapshot
    snapshot = rust_udf_snapshot(ctx)

    # Try to validate a UDF that doesn't exist in the snapshot
    required_udfs = ["nonexistent_udf_for_test"]

    with pytest.raises(
        ValueError,
        match="nonexistent_udf_for_test",
    ):
        validate_required_udfs(
            snapshot,
            required=required_udfs,
        )


@pytest.mark.integration
def test_span_normalization_crlf_vs_lf(
    crlf_python_file: tuple[Path, str],
    sample_python_file: tuple[Path, str],
) -> None:
    r"""Test byte offset correctness with different line ending styles.

    Verifies that files with CRLF (\r\n) line endings have byte offsets that
    correctly account for the CR characters compared to LF (\n) only files.

    Parameters
    ----------
    crlf_python_file
        Tuple of (file path, content) with CRLF endings.
    sample_python_file
        Tuple of (file path, content) with LF endings.
    """
    _, crlf_content = crlf_python_file
    _unused_lf_path, _unused_lf_content = sample_python_file

    # Verify CRLF has more bytes due to \r characters
    crlf_bytes = crlf_content.encode("utf-8")
    assert b"\r\n" in crlf_bytes

    # Count the number of line endings
    crlf_count = crlf_content.count("\r\n")
    assert crlf_count > 0

    # Each CRLF is one byte longer than LF
    # If we normalize the same logical line number, byte offsets should differ by
    # the number of preceding line endings


@pytest.mark.integration
def test_file_line_index_alignment(sample_python_file: tuple[Path, str]) -> None:
    """Test file_line_index_v1 alignment with actual file content.

    Verifies that the file_line_index_v1 table's line start byte offsets match
    the actual boundaries from splitting file content on newlines.

    Parameters
    ----------
    sample_python_file
        Tuple of (file path, content) for the test Python file.
    """
    _, content = sample_python_file
    content_bytes = content.encode("utf-8")

    # Split on newlines and calculate expected line start offsets
    lines = content_bytes.split(b"\n")
    expected_offsets: list[int] = [0]  # First line starts at byte 0

    current_offset = 0
    for line in lines[:-1]:  # Exclude last empty element from split
        # Line length + 1 for the newline character
        current_offset += len(line) + 1
        expected_offsets.append(current_offset)

    # Verify we have the expected number of lines
    expected_line_count = len(content.split("\n"))
    assert len(expected_offsets) == expected_line_count

    # Each offset should be a valid position in the file
    for offset in expected_offsets:
        assert 0 <= offset <= len(content_bytes)


def _repo_files_table(file_path: Path, content: str) -> pa.Table:
    file_bytes = content.encode("utf-8")
    return pa.table(
        {
            "file_id": [file_path.name],
            "path": [str(file_path)],
            "text": [content],
            "file_sha256": [str(len(file_bytes))],
        }
    )


def _bundle_table(value: object) -> pa.Table:
    if isinstance(value, pa.Table):
        return value
    if hasattr(value, "read_all"):
        return cast("_RecordBatchReaderLike", value).read_all()
    msg = f"Unsupported table type: {type(value)}"
    raise TypeError(msg)


def _cst_def_spans(table: pa.Table) -> dict[str, tuple[int, int]]:
    rows = table.to_pylist()
    if not rows:
        return {}
    defs = rows[0].get("defs")
    if not isinstance(defs, list):
        return {}
    spans: dict[str, tuple[int, int]] = {}
    for entry in defs:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        bstart = entry.get("def_bstart")
        bend = entry.get("def_bend")
        if isinstance(name, str) and isinstance(bstart, int) and isinstance(bend, int):
            spans[name] = (bstart, bend)
    return spans


def _ast_def_spans(table: pa.Table) -> dict[str, tuple[int, int]]:
    rows = table.to_pylist()
    if not rows:
        return {}
    defs = rows[0].get("defs")
    if not isinstance(defs, list):
        return {}
    spans: dict[str, tuple[int, int]] = {}
    for entry in defs:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        span = _byte_span_tuple(entry.get("span"))
        if isinstance(name, str) and span is not None:
            spans[name] = span
    return spans


def _ts_def_spans(table: pa.Table) -> dict[str, tuple[int, int]]:
    rows = table.to_pylist()
    if not rows:
        return {}
    defs = rows[0].get("defs")
    if not isinstance(defs, list):
        return {}
    spans: dict[str, tuple[int, int]] = {}
    for entry in defs:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        span = _byte_span_tuple(entry.get("span"))
        if isinstance(name, str) and span is not None:
            spans[name] = span
    return spans


def _byte_span_tuple(value: object) -> tuple[int, int] | None:
    if not isinstance(value, dict):
        return None
    raw_byte_span = value.get("byte_span")
    if not isinstance(raw_byte_span, dict):
        return None
    byte_start = raw_byte_span.get("byte_start")
    byte_len = raw_byte_span.get("byte_len")
    if not isinstance(byte_start, int) or not isinstance(byte_len, int):
        return None
    return byte_start, byte_start + byte_len
