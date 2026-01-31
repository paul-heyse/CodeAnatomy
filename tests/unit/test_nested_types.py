"""Unit tests for schema_spec.nested_types module."""

from __future__ import annotations

from typing import cast

import pyarrow as pa

from schema_spec.nested_types import (
    NestedTypeBuilder,
    attrs_map_type,
    byte_span_struct_builder,
    byte_span_struct_type,
    int32_list_type,
    int64_list_type,
    line_col_struct_builder,
    line_col_struct_type,
    list_of,
    list_of_structs,
    map_of,
    nested_field,
    span_struct_builder,
    span_struct_type,
    string_list_type,
)


class TestNestedTypeBuilder:
    """Test NestedTypeBuilder fluent builder."""

    def test_add_scalar_field(self) -> None:
        """Builder adds scalar fields correctly."""
        builder = NestedTypeBuilder()
        result = builder.add_field("name", pa.string()).add_field("age", pa.int32())

        assert result is builder  # Fluent return
        assert len(builder.fields) == 2
        assert builder.fields[0].name == "name"
        assert builder.fields[0].type == pa.string()
        assert builder.fields[1].name == "age"
        assert builder.fields[1].type == pa.int32()

    def test_add_field_nullable(self) -> None:
        """Builder respects nullable parameter."""
        builder = NestedTypeBuilder()
        builder.add_field("required", pa.string(), nullable=False)
        builder.add_field("optional", pa.string(), nullable=True)

        assert builder.fields[0].nullable is False
        assert builder.fields[1].nullable is True

    def test_add_field_with_metadata(self) -> None:
        """Builder attaches field metadata."""
        metadata = {b"key": b"value"}
        builder = NestedTypeBuilder()
        builder.add_field("field", pa.string(), metadata=metadata)

        assert builder.fields[0].metadata == metadata

    def test_add_struct_from_builder(self) -> None:
        """Builder nests struct from another builder."""
        inner = NestedTypeBuilder().add_field("x", pa.int32()).add_field("y", pa.int32())
        outer = NestedTypeBuilder().add_struct("point", inner)

        struct_type = outer.build_struct()
        assert pa.types.is_struct(struct_type)
        point_field = struct_type.field("point")
        assert pa.types.is_struct(point_field.type)
        assert point_field.type.field("x").type == pa.int32()

    def test_add_struct_type_directly(self) -> None:
        """Builder accepts pre-built struct types."""
        point_type = pa.struct([("x", pa.int32()), ("y", pa.int32())])
        builder = NestedTypeBuilder().add_struct_type("point", point_type)

        struct_type = builder.build_struct()
        assert struct_type.field("point").type == point_type

    def test_add_list_field(self) -> None:
        """Builder creates list fields."""
        builder = NestedTypeBuilder().add_list("items", pa.string())

        struct_type = builder.build_struct()
        items_field = struct_type.field("items")
        assert pa.types.is_list(items_field.type)
        assert items_field.type.value_type == pa.string()

    def test_add_large_list_field(self) -> None:
        """Builder creates large list fields when large=True."""
        builder = NestedTypeBuilder().add_list("items", pa.string(), large=True)

        struct_type = builder.build_struct()
        items_field = struct_type.field("items")
        assert pa.types.is_large_list(items_field.type)

    def test_add_map_field(self) -> None:
        """Builder creates map fields."""
        builder = NestedTypeBuilder().add_map("attrs", pa.string(), pa.string())

        struct_type = builder.build_struct()
        attrs_field = struct_type.field("attrs")
        assert pa.types.is_map(attrs_field.type)

    def test_add_list_of_structs(self) -> None:
        """Builder creates list of struct fields."""
        inner = NestedTypeBuilder().add_field("name", pa.string())
        builder = NestedTypeBuilder().add_list_of_structs("items", inner)

        struct_type = builder.build_struct()
        items_field = struct_type.field("items")
        assert pa.types.is_list(items_field.type)
        assert pa.types.is_struct(items_field.type.value_type)

    def test_build_struct(self) -> None:
        """Builder produces correct struct type."""
        builder = (
            NestedTypeBuilder()
            .add_field("id", pa.int64(), nullable=False)
            .add_field("name", pa.string())
        )

        struct_type = builder.build_struct()
        assert isinstance(struct_type, pa.StructType)
        assert len(struct_type) == 2
        assert struct_type.field("id").nullable is False
        assert struct_type.field("name").nullable is True

    def test_build_schema(self) -> None:
        """Builder produces correct Arrow schema."""
        builder = NestedTypeBuilder().add_field("id", pa.int64())

        schema_like = builder.build_schema()
        schema = cast("pa.Schema", schema_like)
        assert isinstance(schema, pa.Schema)
        assert len(schema) == 1
        assert schema.field("id").type == pa.int64()

    def test_build_schema_with_metadata(self) -> None:
        """Builder attaches schema-level metadata."""
        builder = NestedTypeBuilder().add_field("id", pa.int64())
        metadata = {b"version": b"1"}

        schema = builder.build_schema(metadata=metadata)
        assert schema.metadata == metadata

    def test_build_field(self) -> None:
        """Builder produces nested field."""
        builder = NestedTypeBuilder().add_field("x", pa.int32()).add_field("y", pa.int32())

        field = builder.build_field("point")
        assert field.name == "point"
        assert pa.types.is_struct(field.type)


class TestHelperFunctions:
    """Test helper functions."""

    def test_nested_field(self) -> None:
        """nested_field creates struct field from builder."""
        builder = NestedTypeBuilder().add_field("value", pa.float64())
        field = nested_field("data", builder)

        assert field.name == "data"
        assert pa.types.is_struct(field.type)

    def test_list_of(self) -> None:
        """list_of creates list type."""
        list_type = list_of(pa.string())
        assert pa.types.is_list(list_type)

        large_type = list_of(pa.string(), large=True)
        assert pa.types.is_large_list(large_type)

    def test_list_of_structs(self) -> None:
        """list_of_structs creates list<struct<...>> type."""
        builder = NestedTypeBuilder().add_field("id", pa.int64())
        list_type_like = list_of_structs(builder)
        list_type = cast("pa.ListType", list_type_like)

        assert pa.types.is_list(list_type)
        assert pa.types.is_struct(list_type.value_type)

    def test_map_of(self) -> None:
        """map_of creates map type."""
        map_type = map_of(pa.string(), pa.int32())
        assert pa.types.is_map(map_type)


class TestCommonTemplates:
    """Test pre-defined nested type templates."""

    def test_line_col_struct_type(self) -> None:
        """line_col_struct_type matches expected structure."""
        struct_type = line_col_struct_type()

        assert pa.types.is_struct(struct_type)
        assert struct_type.field("line0").type == pa.int32()
        assert struct_type.field("col").type == pa.int32()

    def test_byte_span_struct_type(self) -> None:
        """byte_span_struct_type matches BYTE_SPAN_STORAGE."""
        from datafusion_engine.arrow.semantic import BYTE_SPAN_STORAGE

        struct_type = byte_span_struct_type()
        assert struct_type == BYTE_SPAN_STORAGE

    def test_span_struct_type(self) -> None:
        """span_struct_type matches SPAN_STORAGE."""
        from datafusion_engine.arrow.semantic import SPAN_STORAGE

        struct_type = span_struct_type()
        assert struct_type == SPAN_STORAGE

    def test_attrs_map_type(self) -> None:
        """attrs_map_type matches ATTRS_T."""
        from datafusion_engine.schema.registry import ATTRS_T

        map_type = attrs_map_type()
        assert map_type == ATTRS_T

    def test_string_list_type(self) -> None:
        """string_list_type creates list<string>."""
        list_type_like = string_list_type()
        list_type = cast("pa.ListType", list_type_like)
        assert pa.types.is_list(list_type)
        assert list_type.value_type == pa.string()

        large_type = string_list_type(large=True)
        assert pa.types.is_large_list(large_type)

    def test_int32_list_type(self) -> None:
        """int32_list_type creates list<int32>."""
        list_type_like = int32_list_type()
        list_type = cast("pa.ListType", list_type_like)
        assert pa.types.is_list(list_type)
        assert list_type.value_type == pa.int32()

    def test_int64_list_type(self) -> None:
        """int64_list_type creates list<int64>."""
        list_type_like = int64_list_type()
        list_type = cast("pa.ListType", list_type_like)
        assert pa.types.is_list(list_type)
        assert list_type.value_type == pa.int64()


class TestBuilderTemplates:
    """Test builder template functions."""

    def test_line_col_struct_builder(self) -> None:
        """line_col_struct_builder returns configured builder."""
        builder = line_col_struct_builder()
        assert len(builder.fields) == 2
        assert builder.fields[0].name == "line0"
        assert builder.fields[1].name == "col"

    def test_byte_span_struct_builder(self) -> None:
        """byte_span_struct_builder returns configured builder."""
        builder = byte_span_struct_builder()
        assert len(builder.fields) == 2
        assert builder.fields[0].name == "byte_start"
        assert builder.fields[1].name == "byte_len"

    def test_span_struct_builder(self) -> None:
        """span_struct_builder returns configured builder."""
        builder = span_struct_builder()
        assert len(builder.fields) == 5
        field_names = [f.name for f in builder.fields]
        assert "start" in field_names
        assert "end" in field_names
        assert "byte_span" in field_names


class TestSchemaSpecIntegration:
    """Test integration with schema_spec package."""

    def test_import_from_schema_spec(self) -> None:
        """Nested type builders are accessible via schema_spec."""
        from schema_spec import (
            NestedTypeBuilder,
            attrs_map_type,
            byte_span_struct_type,
            span_struct_type,
        )

        # Verify imports work and return correct types
        builder = NestedTypeBuilder()
        assert isinstance(builder, NestedTypeBuilder)

        assert pa.types.is_struct(byte_span_struct_type())
        assert pa.types.is_struct(span_struct_type())
        assert pa.types.is_map(attrs_map_type())
