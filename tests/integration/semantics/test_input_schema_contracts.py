"""Integration tests for semantic input schema validation contracts.

Tests the boundary where extraction outputs are validated through
validate_semantic_inputs() and validate_semantic_input_columns() before
being passed to SemanticCompiler.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from semantics.validation import (
    ColumnValidationSpec,
    SemanticInputValidationError,
    validate_semantic_input_columns,
)
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.datafusion_runtime import df_ctx


@pytest.fixture
def runtime_profile() -> DataFusionRuntimeProfile:
    """Provide a DataFusion runtime profile for tests.

    Returns:
    -------
    DataFusionRuntimeProfile
        Runtime profile configured for tests.
    """
    return DataFusionRuntimeProfile()


@pytest.mark.integration
def test_wrong_column_type_error_message_quality() -> None:
    """Validation error payloads should include table and missing column details."""
    ctx = df_ctx()
    register_arrow_table(
        ctx,
        name="cst_refs",
        value=pa.table(
            {
                "file_id": ["file-1"],
                "path": ["src/example.py"],
                # Deliberately wrong type to document current coercion gap.
                "bstart": ["0"],
            }
        ),
    )

    validation = validate_semantic_input_columns(
        ctx,
        input_mapping={"cst_refs": "cst_refs"},
        specs=(
            ColumnValidationSpec(
                canonical_name="cst_refs",
                required_columns=("file_id", "path", "bstart", "bend"),
            ),
        ),
    )
    assert not validation.valid
    assert validation.missing_columns["cst_refs"] == ("bend",)

    error = SemanticInputValidationError(validation)
    message = str(error)
    assert "cst_refs" in message
    assert "bend" in message


@pytest.mark.integration
def test_missing_required_typed_column_cross_module_error() -> None:
    """Missing-column payload should preserve canonical->physical table mapping."""
    ctx = df_ctx()
    register_arrow_table(
        ctx,
        name="module_a_cst_refs",
        value=pa.table(
            {
                "file_id": ["file-1"],
                "bstart": [0],
                "bend": [5],
            }
        ),
    )

    validation = validate_semantic_input_columns(
        ctx,
        input_mapping={"cst_refs": "module_a_cst_refs"},
        specs=(
            ColumnValidationSpec(
                canonical_name="cst_refs",
                required_columns=("file_id", "path", "bstart", "bend"),
            ),
        ),
    )
    assert not validation.valid
    assert validation.resolved_tables["cst_refs"] == "module_a_cst_refs"
    assert validation.missing_columns["module_a_cst_refs"] == ("path",)

    error = SemanticInputValidationError(validation)
    message = str(error)
    assert "module_a_cst_refs" in message
    assert "path" in message


@pytest.mark.integration
def test_extra_columns_accepted_silently() -> None:
    """Test that extra columns beyond spec are accepted silently.

    Verifies that when a table is registered with extra columns beyond what's
    specified in the schema, validation passes without rejection.
    """
    ctx = df_ctx()

    # Create a table with extra columns
    table_with_extras = pa.table(
        {
            "file_id": ["file1", "file2"],
            "path": ["src/file1.py", "src/file2.py"],
            "required_column": [10, 20],
            "extra_column_1": ["extra1", "extra2"],
            "extra_column_2": [100, 200],
        }
    )

    # Register the table - this should succeed
    df = register_arrow_table(ctx, name="table_with_extras", value=table_with_extras)

    # Verify table was registered successfully
    assert df is not None

    # Verify we can query the table
    result = ctx.sql("SELECT * FROM table_with_extras").collect()
    assert len(result) > 0

    # Verify all columns are present (including extras)
    schema = ctx.table("table_with_extras").schema()
    field_names = [field.name for field in schema]
    assert "file_id" in field_names
    assert "required_column" in field_names
    assert "extra_column_1" in field_names
    assert "extra_column_2" in field_names


@pytest.mark.integration
def test_schema_validation_basic_types() -> None:
    """Test basic type validation for common schema patterns.

    Verifies that basic Arrow type validation works for common patterns
    like string, int64, and struct types.
    """
    # Create tables with various common types
    basic_types_table = pa.table(
        {
            "str_col": pa.array(["a", "b"], type=pa.string()),
            "int_col": pa.array([1, 2], type=pa.int64()),
            "float_col": pa.array([1.5, 2.5], type=pa.float64()),
            "bool_col": pa.array([True, False], type=pa.bool_()),
        }
    )

    ctx = df_ctx()
    df = register_arrow_table(ctx, name="basic_types", value=basic_types_table)

    # Verify registration succeeded
    assert df is not None

    # Verify schema types match expectations
    schema = ctx.table("basic_types").schema()
    field_dict = {field.name: field.type for field in schema}

    assert field_dict["str_col"] == pa.string()
    assert field_dict["int_col"] == pa.int64()
    assert field_dict["float_col"] == pa.float64()
    assert field_dict["bool_col"] == pa.bool_()


@pytest.mark.integration
def test_nested_struct_schema_validation() -> None:
    """Test schema validation with nested struct types.

    Verifies that nested struct types are properly validated and preserved
    through registration.
    """
    # Create table with nested struct
    nested_data = pa.table(
        {
            "id": [1, 2],
            "nested": pa.array(
                [
                    {"x": 10, "y": 20},
                    {"x": 30, "y": 40},
                ],
                type=pa.struct([("x", pa.int64()), ("y", pa.int64())]),
            ),
        }
    )

    ctx = df_ctx()
    df = register_arrow_table(ctx, name="nested_struct", value=nested_data)

    # Verify registration succeeded
    assert df is not None

    # Verify nested struct schema preserved
    schema = ctx.table("nested_struct").schema()
    nested_field = next(f for f in schema if f.name == "nested")

    # Should be a struct type with x and y fields
    assert pa.types.is_struct(nested_field.type)
