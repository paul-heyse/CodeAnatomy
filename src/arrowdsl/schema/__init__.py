"""Schema alignment, validation, and array-building utilities."""

from arrowdsl.schema.infer import best_fit_type, infer_schema_from_tables
from arrowdsl.schema.validation import (
    ArrowValidationOptions,
    ValidationReport,
    duplicate_key_rows,
    invalid_rows_plan,
    required_non_null_mask,
    validate_table,
)

__all__ = [
    "ArrowValidationOptions",
    "ValidationReport",
    "best_fit_type",
    "duplicate_key_rows",
    "infer_schema_from_tables",
    "invalid_rows_plan",
    "required_non_null_mask",
    "validate_table",
]
