"""Unit tests for the extraction row builder module."""

from __future__ import annotations

from typing import cast

import pyarrow as pa
import pytest

from extract.row_builder import (
    ExtractionBatchBuilder,
    ExtractionRowBuilder,
    SchemaTemplateOptions,
    SpanTemplateSpec,
    extraction_schema_template,
    make_attrs_list,
    make_span_dict,
    make_span_spec_dict,
)


class TestExtractionRowBuilder:
    """Test suite for ExtractionRowBuilder."""

    @staticmethod
    def test_init_direct() -> None:
        """Verify direct initialization with explicit fields."""
        builder = ExtractionRowBuilder(
            file_id="test-file-123",
            path="src/example.py",
            file_sha256="abc123def456",
            repo_id="my-repo",
        )
        assert builder.file_id == "test-file-123"
        assert builder.path == "src/example.py"
        assert builder.file_sha256 == "abc123def456"
        assert builder.repo_id == "my-repo"

    @staticmethod
    def test_add_identity() -> None:
        """Verify add_identity returns correct columns."""
        builder = ExtractionRowBuilder(
            file_id="id1",
            path="test.py",
            file_sha256="hash1",
        )
        identity = builder.add_identity()
        assert identity == {
            "file_id": "id1",
            "path": "test.py",
            "file_sha256": "hash1",
        }

    @staticmethod
    def test_add_repo_identity() -> None:
        """Verify add_repo_identity includes repo column."""
        builder = ExtractionRowBuilder(
            file_id="id2",
            path="test2.py",
            file_sha256="hash2",
            repo_id="my-repo",
        )
        identity = builder.add_repo_identity()
        assert identity == {
            "repo": "my-repo",
            "file_id": "id2",
            "path": "test2.py",
            "file_sha256": "hash2",
        }

    @staticmethod
    def test_add_span() -> None:
        """Verify add_span returns correct byte offsets."""
        builder = ExtractionRowBuilder(file_id="x", path="y")
        span = builder.add_span(100, 250)
        assert span == {"bstart": 100, "bend": 250}

    @staticmethod
    def test_add_span_spec() -> None:
        """Verify add_span_spec returns structured span dict."""
        builder = ExtractionRowBuilder(file_id="x", path="y")
        spec = SpanTemplateSpec(
            start_line0=10,
            start_col=5,
            end_line0=15,
            end_col=20,
            byte_start=100,
            byte_len=50,
        )
        result = builder.add_span_spec(spec)
        assert result is not None
        assert result["start"] == {"line0": 10, "col": 5}
        assert result["end"] == {"line0": 15, "col": 20}
        assert result["byte_span"] == {"byte_start": 100, "byte_len": 50}

    @staticmethod
    def test_add_attrs() -> None:
        """Verify add_attrs returns map entries."""
        builder = ExtractionRowBuilder(file_id="x", path="y")
        attrs = builder.add_attrs({"key1": "value1", "key2": 42})
        assert ("key1", "value1") in attrs
        assert ("key2", "42") in attrs

    @staticmethod
    def test_build_row() -> None:
        """Verify build_row merges identity with custom fields."""
        builder = ExtractionRowBuilder(
            file_id="f1",
            path="p1.py",
            file_sha256="h1",
        )
        row = builder.build_row(kind="FunctionDef", name="my_func")
        assert row["file_id"] == "f1"
        assert row["path"] == "p1.py"
        assert row["file_sha256"] == "h1"
        assert row["kind"] == "FunctionDef"
        assert row["name"] == "my_func"
        assert "repo" not in row

    @staticmethod
    def test_build_row_with_repo() -> None:
        """Verify build_row includes repo when requested."""
        builder = ExtractionRowBuilder(
            file_id="f2",
            path="p2.py",
            repo_id="r1",
        )
        row = builder.build_row(include_repo=True, kind="ClassDef")
        assert row["repo"] == "r1"
        assert row["file_id"] == "f2"
        assert row["kind"] == "ClassDef"

    @staticmethod
    def test_build_row_with_span() -> None:
        """Verify build_row_with_span includes byte offsets."""
        builder = ExtractionRowBuilder(
            file_id="f3",
            path="p3.py",
        )
        bstart = 50
        bend = 100
        row = builder.build_row_with_span(bstart, bend, kind="Import")
        assert row["bstart"] == bstart
        assert row["bend"] == bend
        assert row["kind"] == "Import"


class TestModuleFunctions:
    """Test suite for module-level functions."""

    @staticmethod
    def test_make_span_dict() -> None:
        """Verify make_span_dict returns correct structure."""
        result = make_span_dict(10, 20)
        assert result == {"bstart": 10, "bend": 20}

    @staticmethod
    def test_make_span_spec_dict() -> None:
        """Verify make_span_spec_dict with full specification."""
        spec = SpanTemplateSpec(
            start_line0=0,
            start_col=0,
            end_line0=5,
            end_col=10,
            end_exclusive=True,
            col_unit="byte",
        )
        result = make_span_spec_dict(spec)
        assert result is not None
        assert result["col_unit"] == "byte"
        assert result["end_exclusive"] is True

    @staticmethod
    def test_make_span_spec_dict_empty() -> None:
        """Verify make_span_spec_dict returns dict with defaults."""
        spec = SpanTemplateSpec()
        result = make_span_spec_dict(spec)
        # With default end_exclusive=True, it should still produce a dict
        assert result is not None

    @staticmethod
    def test_make_attrs_list() -> None:
        """Verify make_attrs_list filters None values."""
        result = make_attrs_list({"a": "1", "b": None, "c": "3"})
        keys = [k for k, _ in result]
        assert "a" in keys
        assert "c" in keys
        assert "b" not in keys

    @staticmethod
    def test_make_attrs_list_empty() -> None:
        """Verify make_attrs_list with no input."""
        result = make_attrs_list(None)
        assert result == []


class TestExtractionBatchBuilder:
    """Test suite for ExtractionBatchBuilder."""

    @pytest.fixture
    def simple_schema(self) -> pa.Schema:
        """Return a simple test schema for batch builder tests.

        Returns:
        -------
        pa.Schema
            A schema with file_id, path, kind, and name columns.
        """
        _ = self
        return pa.schema(
            [
                ("file_id", pa.utf8()),
                ("path", pa.utf8()),
                ("kind", pa.utf8()),
                ("name", pa.utf8()),
            ]
        )

    @staticmethod
    def test_init_with_pyarrow_schema(simple_schema: pa.Schema) -> None:
        """Verify initialization with pyarrow Schema."""
        builder = ExtractionBatchBuilder(simple_schema)
        assert builder.schema == simple_schema
        assert builder.num_rows == 0

    @staticmethod
    def test_add_row(simple_schema: pa.Schema) -> None:
        """Verify add_row increments row count."""
        builder = ExtractionBatchBuilder(simple_schema)
        builder.add_row({"file_id": "f1", "path": "p1", "kind": "k1", "name": "n1"})
        assert builder.num_rows == 1

    @staticmethod
    def test_add_rows(simple_schema: pa.Schema) -> None:
        """Verify add_rows adds multiple rows."""
        builder = ExtractionBatchBuilder(simple_schema)
        builder.add_rows(
            [
                {"file_id": "f1", "path": "p1", "kind": "k1", "name": "n1"},
                {"file_id": "f2", "path": "p2", "kind": "k2", "name": "n2"},
            ]
        )
        expected_rows = 2
        assert builder.num_rows == expected_rows

    @staticmethod
    def test_extend(simple_schema: pa.Schema) -> None:
        """Verify extend is an alias for add_rows."""
        builder = ExtractionBatchBuilder(simple_schema)
        builder.extend(
            [
                {"file_id": "f1", "path": "p1", "kind": "k1", "name": "n1"},
            ]
        )
        assert builder.num_rows == 1

    @staticmethod
    def test_clear(simple_schema: pa.Schema) -> None:
        """Verify clear removes all rows."""
        builder = ExtractionBatchBuilder(simple_schema)
        builder.add_row({"file_id": "f1", "path": "p1", "kind": "k1", "name": "n1"})
        builder.clear()
        assert builder.num_rows == 0

    @staticmethod
    def test_build(simple_schema: pa.Schema) -> None:
        """Verify build returns correct RecordBatch."""
        builder = ExtractionBatchBuilder(simple_schema)
        builder.add_row({"file_id": "f1", "path": "p1", "kind": "k1", "name": "n1"})
        batch = builder.build()
        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows == 1
        assert batch.schema == simple_schema

    @staticmethod
    def test_build_table(simple_schema: pa.Schema) -> None:
        """Verify build_table returns correct Table."""
        builder = ExtractionBatchBuilder(simple_schema)
        builder.add_row({"file_id": "f1", "path": "p1", "kind": "k1", "name": "n1"})
        table = builder.build_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 1

    @staticmethod
    def test_build_and_clear(simple_schema: pa.Schema) -> None:
        """Verify build_and_clear returns batch and clears rows."""
        builder = ExtractionBatchBuilder(simple_schema)
        builder.add_row({"file_id": "f1", "path": "p1", "kind": "k1", "name": "n1"})
        batch = builder.build_and_clear()
        assert batch.num_rows == 1
        assert builder.num_rows == 0

    @staticmethod
    def test_init_invalid_schema_type() -> None:
        """Verify TypeError for invalid schema."""
        with pytest.raises(TypeError, match=r"must be pa\.Schema"):
            ExtractionBatchBuilder(cast("pa.Schema", "not a schema"))


class TestSchemaTemplateOptions:
    """Test suite for SchemaTemplateOptions."""

    @staticmethod
    def test_default_options() -> None:
        """Verify default options include standard identity fields."""
        opts = SchemaTemplateOptions()
        fields = extraction_schema_template(opts)
        names = [name for name, _ in fields]
        assert "file_id" in names
        assert "path" in names
        assert "file_sha256" in names
        assert "repo" not in names
        assert "bstart" not in names

    @staticmethod
    def test_with_spans() -> None:
        """Verify span fields when requested."""
        opts = SchemaTemplateOptions(include_bstart=True, include_bend=True)
        fields = extraction_schema_template(opts)
        names = [name for name, _ in fields]
        assert "bstart" in names
        assert "bend" in names

    @staticmethod
    def test_with_repo() -> None:
        """Verify repo field when requested."""
        opts = SchemaTemplateOptions(include_repo=True)
        fields = extraction_schema_template(opts)
        names = [name for name, _ in fields]
        assert "repo" in names

    @staticmethod
    def test_minimal_schema() -> None:
        """Verify minimal schema with everything disabled."""
        opts = SchemaTemplateOptions(
            include_file_id=False,
            include_path=False,
            include_sha256=False,
        )
        fields = extraction_schema_template(opts)
        assert fields == []

    @staticmethod
    def test_schema_field_types() -> None:
        """Verify correct Arrow types for fields."""
        opts = SchemaTemplateOptions(include_bstart=True, include_bend=True)
        fields = extraction_schema_template(opts)
        type_map = dict(fields)
        assert type_map["file_id"] == pa.utf8()
        assert type_map["bstart"] == pa.int64()
        assert type_map["bend"] == pa.int64()
