"""Schema alignment, validation, and array-building utilities."""

from arrowdsl.schema.build import (
    empty_table,
    list_view_type,
    map_type,
    rows_to_table,
    struct_type,
    table_from_arrays,
    table_from_rows,
    table_from_schema,
)
from arrowdsl.schema.metadata import (
    encoding_policy_from_fields,
    encoding_policy_from_schema,
    encoding_policy_from_spec,
)
from arrowdsl.schema.policy import SchemaPolicy
from arrowdsl.schema.schema import (
    best_fit_type,
    infer_schema_from_tables,
    missing_key_fields,
    required_field_names,
    required_non_null_mask,
)
from arrowdsl.schema.validation import (
    ArrowValidationOptions,
    ValidationReport,
    duplicate_key_rows,
    duplicate_key_rows_plan,
    invalid_rows_plan,
    validate_table,
)

__all__ = [
    "ArrowValidationOptions",
    "SchemaPolicy",
    "ValidationReport",
    "best_fit_type",
    "duplicate_key_rows",
    "duplicate_key_rows_plan",
    "empty_table",
    "encoding_policy_from_fields",
    "encoding_policy_from_schema",
    "encoding_policy_from_spec",
    "infer_schema_from_tables",
    "invalid_rows_plan",
    "list_view_type",
    "map_type",
    "missing_key_fields",
    "required_field_names",
    "required_non_null_mask",
    "rows_to_table",
    "struct_type",
    "table_from_arrays",
    "table_from_rows",
    "table_from_schema",
    "validate_table",
]
