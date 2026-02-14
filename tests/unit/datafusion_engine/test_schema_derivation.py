"""Parity tests: derived schemas vs. canonical registry schemas.

Gate 2 of Phase H (Schema Derivation) -- verify that schemas derived from
``ExtractMetadata`` field descriptors match the schemas resolved by
``datafusion_engine.schema.extract_schema_for``.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.extract.metadata import extract_metadata_by_name
from datafusion_engine.schema import extract_schema_for
from datafusion_engine.schema.derivation import derive_extract_schema

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _type_category(pa_type: pa.DataType) -> str:
    """Classify a PyArrow type into a broad category for parity checks.

    Returns:
        Category label such as ``"list"``, ``"struct"``, ``"map"``, etc.
    """
    checks: dict[str, bool] = {
        "list": pa.types.is_list(pa_type) or pa.types.is_large_list(pa_type),
        "struct": pa.types.is_struct(pa_type),
        "map": pa.types.is_map(pa_type),
        "string": pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type),
        "integer": pa.types.is_integer(pa_type),
        "boolean": pa.types.is_boolean(pa_type),
        "float": pa.types.is_floating(pa_type),
    }
    return next((label for label, matched in checks.items() if matched), "other")


# The 7 base extract datasets that have both metadata and static schemas.
_BASE_DATASET_NAMES: tuple[str, ...] = (
    "repo_files_v1",
    "libcst_files_v1",
    "ast_files_v1",
    "symtable_files_v1",
    "tree_sitter_files_v1",
    "bytecode_files_v1",
    "scip_index_v1",
)


# ---------------------------------------------------------------------------
# Gate 2c: field_types and nested_shapes are populated
# ---------------------------------------------------------------------------


class TestMetadataEnrichment:
    """Verify that field_types and nested_shapes are present after template expansion."""

    @pytest.mark.parametrize("dataset_name", _BASE_DATASET_NAMES)
    def test_field_types_populated(self, dataset_name: str) -> None:
        """Verify field_types mapping is non-empty for base datasets."""
        metadata = extract_metadata_by_name()[dataset_name]
        assert metadata.field_types, f"{dataset_name}: field_types is empty"

    @pytest.mark.parametrize("dataset_name", _BASE_DATASET_NAMES)
    def test_fields_populated(self, dataset_name: str) -> None:
        """Verify fields tuple is non-empty for base datasets."""
        metadata = extract_metadata_by_name()[dataset_name]
        assert metadata.fields, f"{dataset_name}: fields is empty"

    @pytest.mark.parametrize("dataset_name", _BASE_DATASET_NAMES)
    def test_field_types_cover_fields(self, dataset_name: str) -> None:
        """Verify every field name has a corresponding field_type entry."""
        metadata = extract_metadata_by_name()[dataset_name]
        missing = set(metadata.fields) - set(metadata.field_types)
        assert not missing, f"{dataset_name}: fields without field_types: {sorted(missing)}"

    @pytest.mark.parametrize(
        "dataset_name",
        [
            "ast_files_v1",
            "libcst_files_v1",
            "bytecode_files_v1",
            "symtable_files_v1",
            "tree_sitter_files_v1",
            "scip_index_v1",
        ],
    )
    def test_nested_shapes_populated(self, dataset_name: str) -> None:
        """Verify nested_shapes is non-empty for datasets with nested columns."""
        metadata = extract_metadata_by_name()[dataset_name]
        assert metadata.nested_shapes, f"{dataset_name}: nested_shapes is empty"

    def test_repo_files_no_nested_shapes(self) -> None:
        """Verify repo_files_v1 has empty nested_shapes (all scalar columns)."""
        metadata = extract_metadata_by_name()["repo_files_v1"]
        assert not metadata.nested_shapes


# ---------------------------------------------------------------------------
# Gate 2c: derivation parity
# ---------------------------------------------------------------------------


class TestSchemaDerivationParity:
    """Compare derived schemas against canonical registry schemas."""

    @pytest.mark.parametrize("dataset_name", _BASE_DATASET_NAMES)
    def test_field_names_match(self, dataset_name: str) -> None:
        """Verify derived schema field names match canonical schema field names."""
        metadata = extract_metadata_by_name()[dataset_name]
        static_schema = extract_schema_for(dataset_name)

        derived_schema = derive_extract_schema(metadata)

        assert list(derived_schema.names) == list(static_schema.names), (
            f"{dataset_name}: field name mismatch.\n"
            f"  Derived: {derived_schema.names}\n"
            f"  Static:  {static_schema.names}"
        )

    @pytest.mark.parametrize("dataset_name", _BASE_DATASET_NAMES)
    def test_field_count_match(self, dataset_name: str) -> None:
        """Verify derived schema has same number of fields as canonical schema."""
        metadata = extract_metadata_by_name()[dataset_name]
        static_schema = extract_schema_for(dataset_name)

        derived_schema = derive_extract_schema(metadata)

        assert len(derived_schema) == len(static_schema), (
            f"{dataset_name}: field count mismatch. "
            f"Derived={len(derived_schema)}, Static={len(static_schema)}"
        )

    @pytest.mark.parametrize("dataset_name", _BASE_DATASET_NAMES)
    def test_type_categories_match(self, dataset_name: str) -> None:
        """Verify each field's top-level type category matches."""
        metadata = extract_metadata_by_name()[dataset_name]
        static_schema = extract_schema_for(dataset_name)

        derived_schema = derive_extract_schema(metadata)

        # Only compare fields that exist in both schemas.
        for i, derived_field in enumerate(derived_schema):
            if i >= len(static_schema):
                break
            static_field = static_schema.field(i)
            derived_cat = _type_category(derived_field.type)
            static_cat = _type_category(static_field.type)
            assert derived_cat == static_cat, (
                f"{dataset_name}.{derived_field.name}: "
                f"type category mismatch. "
                f"Derived={derived_cat} ({derived_field.type}), "
                f"Static={static_cat} ({static_field.type})"
            )


# ---------------------------------------------------------------------------
# Gate 2c: nested_shapes parity
# ---------------------------------------------------------------------------


class TestNestedShapesParity:
    """Verify nested_shapes child field names match static schema struct fields."""

    @pytest.mark.parametrize("dataset_name", _BASE_DATASET_NAMES)
    def test_nested_shapes_field_names(self, dataset_name: str) -> None:
        """Verify nested shape child names match struct field names in schema."""
        metadata = extract_metadata_by_name()[dataset_name]
        static_schema = extract_schema_for(dataset_name)

        for col_name, child_names in metadata.nested_shapes.items():
            if col_name not in static_schema.names:
                continue
            static_field = static_schema.field(col_name)
            # Get the struct type (unwrapping list if needed).
            if pa.types.is_list(static_field.type):
                struct_type = static_field.type.value_type
            else:
                struct_type = static_field.type

            if not pa.types.is_struct(struct_type):
                continue

            static_child_names = [struct_type.field(j).name for j in range(struct_type.num_fields)]
            assert list(child_names) == static_child_names, (
                f"{dataset_name}.{col_name}: nested shape mismatch.\n"
                f"  Metadata: {list(child_names)}\n"
                f"  Schema:   {static_child_names}"
            )
