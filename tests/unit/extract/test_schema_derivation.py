"""Unit tests for extraction schema derivation."""

from __future__ import annotations

import pyarrow as pa
import pytest

from extract.schema_derivation import (
    EXTRACTION_SCHEMA_TEMPLATES,
    DerivationOptions,
    ExtractionSchemaBuilder,
    build_schema_from_template,
    extraction_schema_to_arrow,
    validate_extraction_schema,
)
from schema_spec.arrow_type_coercion import coerce_arrow_type
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import FieldBundle


class TestExtractionSchemaBuilder:
    """Tests for ExtractionSchemaBuilder."""

    def test_builder_basic_construction(self) -> None:
        """Test basic builder construction."""
        builder = ExtractionSchemaBuilder("test_dataset", version=1)
        schema = builder.build()
        assert schema.name == "test_dataset"
        assert schema.version == 1
        assert len(schema.fields) == 0

    def test_builder_with_file_identity(self) -> None:
        """Test builder with file identity fields."""
        schema = (
            ExtractionSchemaBuilder("test_dataset").with_file_identity(include_sha256=True).build()
        )
        field_names = {f.name for f in schema.fields}
        assert "file_id" in field_names
        assert "path" in field_names
        assert "file_sha256" in field_names
        assert "file_id" in schema.required_non_null
        assert "path" in schema.required_non_null

    def test_builder_with_byte_span(self) -> None:
        """Test builder with byte span fields."""
        schema = ExtractionSchemaBuilder("test_dataset").with_span(style="byte").build()
        field_names = {f.name for f in schema.fields}
        assert "bstart" in field_names
        assert "bend" in field_names

    def test_builder_with_prefixed_span(self) -> None:
        """Test builder with prefixed span fields."""
        schema = (
            ExtractionSchemaBuilder("test_dataset").with_span(prefix="def", style="byte").build()
        )
        field_names = {f.name for f in schema.fields}
        assert "def_bstart" in field_names
        assert "def_bend" in field_names

    def test_builder_with_line_col_span(self) -> None:
        """Test builder with line/col span fields."""
        schema = ExtractionSchemaBuilder("test_dataset").with_span(style="line_col").build()
        field_names = {f.name for f in schema.fields}
        assert "start_line" in field_names
        assert "start_col" in field_names
        assert "end_line" in field_names
        assert "end_col" in field_names

    def test_builder_with_structured_span(self) -> None:
        """Test builder with structured span field."""
        schema = ExtractionSchemaBuilder("test_dataset").with_span(style="structured").build()
        field_names = {f.name for f in schema.fields}
        assert "span" in field_names

    def test_builder_with_invalid_span_style(self) -> None:
        """Test builder raises on invalid span style."""
        with pytest.raises(ValueError, match="Unsupported span style"):
            ExtractionSchemaBuilder("test_dataset").with_span(style="invalid")

    def test_builder_with_evidence_metadata(self) -> None:
        """Test builder with evidence metadata fields."""
        schema = ExtractionSchemaBuilder("test_dataset").with_evidence_metadata().build()
        field_names = {f.name for f in schema.fields}
        assert "task_name" in field_names
        assert "confidence" in field_names
        assert "score" in field_names

    def test_builder_with_custom_field(self) -> None:
        """Test builder with custom fields."""
        schema = (
            ExtractionSchemaBuilder("test_dataset")
            .with_field("custom_col", pa.string())
            .with_field("custom_int", pa.int32(), nullable=False)
            .build()
        )
        field_names = {f.name for f in schema.fields}
        assert "custom_col" in field_names
        assert "custom_int" in field_names

    def test_builder_with_field_spec(self) -> None:
        """Test builder with FieldSpec objects."""
        custom_spec = FieldSpec(
            name="custom_field", dtype=coerce_arrow_type(pa.float64())
        )
        schema = ExtractionSchemaBuilder("test_dataset").with_fields(custom_spec).build()
        field_names = {f.name for f in schema.fields}
        assert "custom_field" in field_names

    def test_builder_with_custom_bundle(self) -> None:
        """Test builder with custom field bundle."""
        bundle = FieldBundle(
            name="custom_bundle",
            fields=(
                FieldSpec(name="bundle_field1", dtype=coerce_arrow_type(pa.string())),
                FieldSpec(name="bundle_field2", dtype=coerce_arrow_type(pa.int64())),
            ),
        )
        schema = ExtractionSchemaBuilder("test_dataset").with_bundle(bundle).build()
        field_names = {f.name for f in schema.fields}
        assert "bundle_field1" in field_names
        assert "bundle_field2" in field_names

    def test_builder_field_deduplication(self) -> None:
        """Test that duplicate field names are deduplicated."""
        schema = (
            ExtractionSchemaBuilder("test_dataset")
            .with_file_identity()
            .with_field("file_id", pa.string())  # Duplicate
            .build()
        )
        # Count occurrences of file_id
        file_id_count = sum(1 for f in schema.fields if f.name == "file_id")
        assert file_id_count == 1

    def test_builder_with_key_fields(self) -> None:
        """Test builder with key fields."""
        schema = (
            ExtractionSchemaBuilder("test_dataset")
            .with_file_identity()
            .with_key_fields("file_id", "path")
            .build()
        )
        assert "file_id" in schema.key_fields
        assert "path" in schema.key_fields

    def test_builder_with_required_non_null(self) -> None:
        """Test builder with required non-null fields."""
        schema = (
            ExtractionSchemaBuilder("test_dataset")
            .with_field("important", pa.string())
            .with_required_non_null("important")
            .build()
        )
        assert "important" in schema.required_non_null

    def test_builder_chaining(self) -> None:
        """Test that all builder methods return self for chaining."""
        schema = (
            ExtractionSchemaBuilder("test_dataset", version=2)
            .with_file_identity()
            .with_span(style="byte")
            .with_evidence_metadata()
            .with_field("custom", pa.string())
            .with_required_non_null("custom")
            .with_key_fields("file_id")
            .build()
        )
        assert schema.name == "test_dataset"
        assert schema.version == 2
        assert len(schema.fields) > 0


class TestExtractionSchemaTemplates:
    """Tests for extraction schema templates."""

    def test_templates_exist(self) -> None:
        """Test that expected templates are defined."""
        expected = {"ast", "cst", "bytecode", "symtable", "scip", "tree_sitter", "repo_scan"}
        assert expected.issubset(set(EXTRACTION_SCHEMA_TEMPLATES.keys()))

    def test_build_ast_template(self) -> None:
        """Test building from AST template."""
        schema = build_schema_from_template("ast", "py_ast_nodes_v1")
        field_names = {f.name for f in schema.fields}
        # Should have file identity
        assert "file_id" in field_names
        assert "path" in field_names
        # Should have byte span
        assert "bstart" in field_names
        assert "bend" in field_names
        # Should have AST-specific fields
        assert "node_type" in field_names
        assert "node_id" in field_names

    def test_build_cst_template(self) -> None:
        """Test building from CST template."""
        schema = build_schema_from_template("cst", "py_cst_nodes_v1")
        field_names = {f.name for f in schema.fields}
        assert "node_type" in field_names
        assert "node_id" in field_names
        assert "qualified_name" in field_names

    def test_build_bytecode_template(self) -> None:
        """Test building from bytecode template."""
        schema = build_schema_from_template("bytecode", "py_bytecode_v1")
        field_names = {f.name for f in schema.fields}
        # Should have file identity
        assert "file_id" in field_names
        # Should NOT have span (bytecode doesn't use spans)
        assert "bstart" not in field_names
        # Should have bytecode-specific fields
        assert "code_unit_id" in field_names
        assert "opname" in field_names

    def test_build_scip_template(self) -> None:
        """Test building from SCIP template."""
        schema = build_schema_from_template("scip", "py_scip_symbols_v1")
        field_names = {f.name for f in schema.fields}
        # Should have line/col span (via hook)
        assert "start_line" in field_names
        assert "start_col" in field_names
        # Should have SCIP-specific fields
        assert "symbol" in field_names
        assert "document_id" in field_names

    def test_build_template_with_extra_fields(self) -> None:
        """Test building template with extra fields."""
        extra = [FieldSpec(name="extra_col", dtype=coerce_arrow_type(pa.binary()))]
        schema = build_schema_from_template("ast", "custom_ast", extra_fields=extra)
        field_names = {f.name for f in schema.fields}
        assert "extra_col" in field_names

    def test_build_template_with_version(self) -> None:
        """Test building template with specific version."""
        schema = build_schema_from_template("ast", "py_ast_v2", version=2)
        assert schema.version == 2

    def test_build_unknown_template_raises(self) -> None:
        """Test that unknown template raises KeyError."""
        with pytest.raises(KeyError, match="Unknown extraction template"):
            build_schema_from_template("unknown_template", "test")


class TestDerivationOptions:
    """Tests for DerivationOptions."""

    def test_default_options(self) -> None:
        """Test default derivation options."""
        options = DerivationOptions()
        assert options.version == 1
        assert options.include_file_identity is True
        assert options.include_span is True
        assert options.include_evidence_metadata is False
        assert options.span_style == "byte"

    def test_custom_options(self) -> None:
        """Test custom derivation options."""
        options = DerivationOptions(
            version=2,
            include_file_identity=False,
            include_span=True,
            include_evidence_metadata=True,
            span_style="line_col",
        )
        assert options.version == 2
        assert options.include_file_identity is False
        assert options.include_evidence_metadata is True
        assert options.span_style == "line_col"


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_extraction_schema_to_arrow(self) -> None:
        """Test converting schema spec to Arrow schema."""
        schema_spec = (
            ExtractionSchemaBuilder("test")
            .with_file_identity()
            .with_field("custom", pa.string())
            .build()
        )
        arrow_schema = extraction_schema_to_arrow(schema_spec)
        assert isinstance(arrow_schema, pa.Schema)
        assert "file_id" in arrow_schema.names
        assert "custom" in arrow_schema.names

    def test_validate_extraction_schema_valid(self) -> None:
        """Test validation of valid schema."""
        schema_spec = (
            ExtractionSchemaBuilder("test").with_file_identity().with_span(style="byte").build()
        )
        is_valid, errors = validate_extraction_schema(
            schema_spec,
            require_file_identity=True,
            require_span=True,
        )
        assert is_valid is True
        assert len(errors) == 0

    def test_validate_extraction_schema_missing_identity(self) -> None:
        """Test validation catches missing file identity."""
        schema_spec = ExtractionSchemaBuilder("test").with_span(style="byte").build()
        is_valid, errors = validate_extraction_schema(
            schema_spec,
            require_file_identity=True,
        )
        assert is_valid is False
        assert any("file identity" in err.lower() for err in errors)

    def test_validate_extraction_schema_missing_span(self) -> None:
        """Test validation catches missing span."""
        schema_spec = ExtractionSchemaBuilder("test").with_file_identity().build()
        is_valid, errors = validate_extraction_schema(
            schema_spec,
            require_file_identity=True,
            require_span=True,
        )
        assert is_valid is False
        assert any("span" in err.lower() for err in errors)

    def test_validate_extraction_schema_no_requirements(self) -> None:
        """Test validation with no requirements passes."""
        schema_spec = ExtractionSchemaBuilder("test").build()
        is_valid, errors = validate_extraction_schema(
            schema_spec,
            require_file_identity=False,
            require_span=False,
        )
        assert is_valid is True
        assert len(errors) == 0
