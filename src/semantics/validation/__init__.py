"""Validation utilities for semantic pipeline inputs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from semantics.validation.catalog_validation import (
    SEMANTIC_INPUT_COLUMN_SPECS,
    ColumnValidationSpec,
    SemanticInputValidationResult,
    validate_semantic_input_columns,
)

if TYPE_CHECKING:
    from datafusion import SessionContext


class SemanticInputValidationError(ValueError):
    """Raised when semantic input validation fails."""

    def __init__(self, validation: SemanticInputValidationResult) -> None:
        self.validation = validation
        missing_tables = ", ".join(sorted(validation.missing_tables))
        missing_columns = {
            table: sorted(columns) for table, columns in validation.missing_columns.items()
        }
        msg = (
            "Semantic input validation failed. "
            f"Missing tables: {missing_tables or 'none'}. "
            f"Missing columns: {missing_columns or 'none'}."
        )
        super().__init__(msg)


def require_semantic_inputs(
    ctx: SessionContext,
    *,
    input_mapping: dict[str, str],
) -> SemanticInputValidationResult:
    """Validate semantic inputs and raise when invalid.

    Parameters
    ----------
    ctx
        DataFusion session context.
    input_mapping
        Mapping of canonical input names to registered table names.

    Returns
    -------
    SemanticInputValidationResult
        Validation result when inputs are valid.

    Raises
    ------
    SemanticInputValidationError
        Raised when semantic inputs are missing or invalid.
    """
    validation = validate_semantic_input_columns(ctx, input_mapping=input_mapping)
    if not validation.valid:
        raise SemanticInputValidationError(validation)
    return validation


__all__ = [
    "SEMANTIC_INPUT_COLUMN_SPECS",
    "ColumnValidationSpec",
    "SemanticInputValidationError",
    "SemanticInputValidationResult",
    "require_semantic_inputs",
    "validate_semantic_input_columns",
]
