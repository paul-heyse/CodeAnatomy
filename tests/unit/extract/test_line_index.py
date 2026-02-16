"""Unit tests for line index extraction."""

from __future__ import annotations

import pyarrow as pa

from extract.extractors.file_index.line_index import (
    FILE_LINE_INDEX_SCHEMA,
    _LineIndexConfig,
    extract_line_index_rows,
)

ONE_LINE = 1
TWO_LINES = 2
THREE_LINES = 3
HELLO_WORLD_BYTES = 11
HELLO_WORLD_LF_BYTES = 12
HELLO_WORLD_CRLF_BYTES = 13
FIRST_LF_END = 9
SECOND_LF_END = 18
THIRD_LF_END = 28
TRUNCATED_LINE_BYTES = 37
THIRD_LINE_NO = 2
SECOND_EMPTY_LINE_END = 2


class TestExtractLineIndexRows:
    """Test extract_line_index_rows function."""

    @staticmethod
    def test_single_line_no_newline() -> None:
        """Test extraction of single line without trailing newline."""
        content = b"hello world"
        config = _LineIndexConfig(file_id="f1", path="test.py")
        rows = list(extract_line_index_rows(content, config=config))

        assert len(rows) == ONE_LINE
        row = rows[0]
        assert row["file_id"] == "f1"
        assert row["path"] == "test.py"
        assert row["line_no"] == 0
        assert row["line_start_byte"] == 0
        assert row["line_end_byte"] == HELLO_WORLD_BYTES
        assert row["line_text"] == "hello world"
        assert row["newline_kind"] == "none"

    @staticmethod
    def test_single_line_with_lf() -> None:
        """Test extraction of single line with LF newline."""
        content = b"hello world\n"
        config = _LineIndexConfig(file_id="f1", path="test.py")
        rows = list(extract_line_index_rows(content, config=config))

        assert len(rows) == ONE_LINE
        row = rows[0]
        assert row["line_no"] == 0
        assert row["line_start_byte"] == 0
        assert row["line_end_byte"] == HELLO_WORLD_LF_BYTES  # includes newline
        assert row["line_text"] == "hello world"  # excludes newline
        assert row["newline_kind"] == "lf"

    @staticmethod
    def test_single_line_with_crlf() -> None:
        """Test extraction of single line with CRLF newline."""
        content = b"hello world\r\n"
        config = _LineIndexConfig(file_id="f1", path="test.py")
        rows = list(extract_line_index_rows(content, config=config))

        assert len(rows) == ONE_LINE
        row = rows[0]
        assert row["line_no"] == 0
        assert row["line_start_byte"] == 0
        assert row["line_end_byte"] == HELLO_WORLD_CRLF_BYTES  # includes \r\n
        assert row["line_text"] == "hello world"  # excludes \r\n
        assert row["newline_kind"] == "crlf"

    @staticmethod
    def test_multiple_lines_lf() -> None:
        """Test extraction of multiple lines with LF newlines."""
        content = b"line one\nline two\nline three"
        config = _LineIndexConfig(file_id="f1", path="test.py")
        rows = list(extract_line_index_rows(content, config=config))

        assert len(rows) == THREE_LINES

        assert rows[0]["line_no"] == 0
        assert rows[0]["line_start_byte"] == 0
        assert rows[0]["line_end_byte"] == FIRST_LF_END
        assert rows[0]["line_text"] == "line one"
        assert rows[0]["newline_kind"] == "lf"

        assert rows[1]["line_no"] == 1
        assert rows[1]["line_start_byte"] == FIRST_LF_END
        assert rows[1]["line_end_byte"] == SECOND_LF_END
        assert rows[1]["line_text"] == "line two"
        assert rows[1]["newline_kind"] == "lf"

        assert rows[2]["line_no"] == THIRD_LINE_NO
        assert rows[2]["line_start_byte"] == SECOND_LF_END
        assert rows[2]["line_end_byte"] == THIRD_LF_END
        assert rows[2]["line_text"] == "line three"
        assert rows[2]["newline_kind"] == "none"

    @staticmethod
    def test_empty_content() -> None:
        """Test extraction of empty content produces no rows."""
        content = b""
        config = _LineIndexConfig(file_id="f1", path="test.py")
        rows = list(extract_line_index_rows(content, config=config))

        assert len(rows) == 0

    @staticmethod
    def test_empty_lines() -> None:
        """Test extraction preserves empty lines."""
        content = b"\n\n"
        config = _LineIndexConfig(file_id="f1", path="test.py")
        rows = list(extract_line_index_rows(content, config=config))

        assert len(rows) == TWO_LINES

        assert rows[0]["line_no"] == 0
        assert rows[0]["line_start_byte"] == 0
        assert rows[0]["line_end_byte"] == 1
        assert not rows[0]["line_text"]
        assert rows[0]["newline_kind"] == "lf"

        assert rows[1]["line_no"] == 1
        assert rows[1]["line_start_byte"] == 1
        assert rows[1]["line_end_byte"] == SECOND_EMPTY_LINE_END
        assert not rows[1]["line_text"]
        assert rows[1]["newline_kind"] == "lf"

    @staticmethod
    def test_include_text_false() -> None:
        """Test extraction with include_text=False."""
        content = b"hello world\n"
        config = _LineIndexConfig(file_id="f1", path="test.py", include_text=False)
        rows = list(extract_line_index_rows(content, config=config))

        assert len(rows) == ONE_LINE
        assert rows[0]["line_text"] is None

    @staticmethod
    def test_max_line_text_bytes_truncation() -> None:
        """Test extraction truncates long lines."""
        content = b"hello world this is a very long line\n"
        config = _LineIndexConfig(file_id="f1", path="test.py", max_line_text_bytes=5)
        rows = list(extract_line_index_rows(content, config=config))

        assert len(rows) == ONE_LINE
        assert rows[0]["line_text"] == "hello"
        # Line end byte still includes full line
        assert rows[0]["line_end_byte"] == TRUNCATED_LINE_BYTES

    @staticmethod
    def test_utf8_content() -> None:
        """Test extraction of UTF-8 content."""
        content = "hello \u4e16\u754c\n".encode()  # hello world in Chinese
        config = _LineIndexConfig(file_id="f1", path="test.py")
        rows = list(extract_line_index_rows(content, config=config))

        assert len(rows) == ONE_LINE
        assert rows[0]["line_text"] == "hello \u4e16\u754c"
        # UTF-8: hello=5, space=1, 2 Chinese chars=6 bytes, newline=1 = 13 total
        assert rows[0]["line_end_byte"] == HELLO_WORLD_CRLF_BYTES

    @staticmethod
    def test_mixed_newlines() -> None:
        """Test extraction handles mixed newline types."""
        content = b"line one\r\nline two\nline three\r\n"
        config = _LineIndexConfig(file_id="f1", path="test.py")
        rows = list(extract_line_index_rows(content, config=config))

        assert len(rows) == THREE_LINES

        assert rows[0]["newline_kind"] == "crlf"
        assert rows[1]["newline_kind"] == "lf"
        assert rows[2]["newline_kind"] == "crlf"

    @staticmethod
    def test_byte_offsets_are_contiguous() -> None:
        """Test that byte offsets form a contiguous range."""
        content = b"line one\nline two\nline three\n"
        config = _LineIndexConfig(file_id="f1", path="test.py")
        rows = list(extract_line_index_rows(content, config=config))

        for i in range(1, len(rows)):
            # Each line should start where the previous ended
            assert rows[i]["line_start_byte"] == rows[i - 1]["line_end_byte"]

        # Last line should end at content length
        assert rows[-1]["line_end_byte"] == len(content)


class TestFileLineIndexSchema:
    """Test FILE_LINE_INDEX_SCHEMA constant."""

    @staticmethod
    def test_schema_fields() -> None:
        """Test schema has expected fields."""
        expected_names = [
            "file_id",
            "path",
            "line_no",
            "line_start_byte",
            "line_end_byte",
            "line_text",
            "newline_kind",
        ]
        assert FILE_LINE_INDEX_SCHEMA.names == expected_names

    @staticmethod
    def test_schema_types() -> None:
        """Test schema has expected types."""
        assert FILE_LINE_INDEX_SCHEMA.field("file_id").type == pa.string()
        assert FILE_LINE_INDEX_SCHEMA.field("path").type == pa.string()
        assert FILE_LINE_INDEX_SCHEMA.field("line_no").type == pa.int64()
        assert FILE_LINE_INDEX_SCHEMA.field("line_start_byte").type == pa.int64()
        assert FILE_LINE_INDEX_SCHEMA.field("line_end_byte").type == pa.int64()
        assert FILE_LINE_INDEX_SCHEMA.field("line_text").type == pa.string()
        assert FILE_LINE_INDEX_SCHEMA.field("newline_kind").type == pa.string()

    @staticmethod
    def test_line_text_nullable() -> None:
        """Test line_text is nullable."""
        assert FILE_LINE_INDEX_SCHEMA.field("line_text").nullable is True

    @staticmethod
    def test_required_fields_not_nullable() -> None:
        """Test required fields are not nullable."""
        assert FILE_LINE_INDEX_SCHEMA.field("file_id").nullable is False
        assert FILE_LINE_INDEX_SCHEMA.field("path").nullable is False
        assert FILE_LINE_INDEX_SCHEMA.field("line_no").nullable is False
        assert FILE_LINE_INDEX_SCHEMA.field("line_start_byte").nullable is False
        assert FILE_LINE_INDEX_SCHEMA.field("line_end_byte").nullable is False
        assert FILE_LINE_INDEX_SCHEMA.field("newline_kind").nullable is False
