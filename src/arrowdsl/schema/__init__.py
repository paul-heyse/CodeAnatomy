"""Schema alignment, validation, and array-building utilities."""

from arrowdsl.schema.arrays import list_view_type, map_type, struct_type
from arrowdsl.schema.constraints import (
    missing_key_fields,
    required_field_names,
    required_non_null_mask,
)
from arrowdsl.schema.encoding import encoding_policy_from_fields, encoding_policy_from_spec
from arrowdsl.schema.infer import best_fit_type, infer_schema_from_tables
from arrowdsl.schema.policy import SchemaPolicy
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
    "encoding_policy_from_fields",
    "encoding_policy_from_spec",
    "infer_schema_from_tables",
    "invalid_rows_plan",
    "list_view_type",
    "map_type",
    "missing_key_fields",
    "required_field_names",
    "required_non_null_mask",
    "struct_type",
    "validate_table",
]
