"""Unified validation violation types."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum


class ViolationType(Enum):
    """Types of validation violations."""

    MISSING_TABLE = "missing_table"
    MISSING_COLUMN = "missing_column"
    EXTRA_COLUMN = "extra_column"
    TYPE_MISMATCH = "type_mismatch"
    NULLABILITY_MISMATCH = "nullability_mismatch"
    METADATA_MISMATCH = "metadata_mismatch"
    NULL_VIOLATION = "null_violation"
    MISSING_KEY_FIELD = "missing_key_field"
    DUPLICATE_KEYS = "duplicate_keys"


@dataclass(frozen=True)
class ValidationViolation:
    """Single validation violation unified across validation contexts."""

    violation_type: ViolationType
    table_name: str | None = None
    column_name: str | None = None
    expected: str | None = None
    actual: str | None = None
    count: int | None = None

    def __str__(self) -> str:
        """Return a human-readable violation message.

        Returns
        -------
        str
            Human-readable violation message.
        """
        formatter = _VIOLATION_FORMATTERS.get(self.violation_type)
        if formatter is None:
            return _default_violation_message(self)
        return formatter(self)


def _default_violation_message(violation: ValidationViolation) -> str:
    return f"Validation violation: {violation.violation_type.value}"


def _format_missing_table(violation: ValidationViolation) -> str:
    if violation.table_name:
        return f"Table '{violation.table_name}' not found"
    return "Table not found"


def _format_metadata_mismatch(violation: ValidationViolation) -> str:
    table = violation.table_name or "<unknown>"
    return (
        f"Metadata mismatch for '{table}': key={violation.column_name!r} "
        f"expected={violation.expected!r} actual={violation.actual!r}"
    )


def _format_missing_column(violation: ValidationViolation) -> str:
    if violation.table_name and violation.column_name:
        return f"Column '{violation.table_name}.{violation.column_name}' not found"
    if violation.column_name:
        return f"Missing column '{violation.column_name}'"
    return "Missing column"


def _format_extra_column(violation: ValidationViolation) -> str:
    if violation.table_name and violation.column_name:
        return f"Unexpected column '{violation.table_name}.{violation.column_name}'"
    if violation.column_name:
        return f"Unexpected column '{violation.column_name}'"
    return "Unexpected column"


def _format_type_mismatch(violation: ValidationViolation) -> str:
    if violation.table_name and violation.column_name:
        return (
            f"Type mismatch for '{violation.table_name}.{violation.column_name}': "
            f"expected {violation.expected}, got {violation.actual}"
        )
    if violation.column_name:
        return (
            f"Type mismatch for '{violation.column_name}': "
            f"expected {violation.expected}, got {violation.actual}"
        )
    return "Type mismatch"


def _format_nullability_mismatch(violation: ValidationViolation) -> str:
    if violation.table_name and violation.column_name:
        return (
            f"Nullability mismatch for '{violation.table_name}.{violation.column_name}': "
            f"expected {violation.expected}, got {violation.actual}"
        )
    if violation.column_name:
        return (
            f"Nullability mismatch for '{violation.column_name}': "
            f"expected {violation.expected}, got {violation.actual}"
        )
    return "Nullability mismatch"


def _format_null_violation(violation: ValidationViolation) -> str:
    if violation.column_name:
        return f"Null value in non-nullable column '{violation.column_name}'"
    return "Null value in non-nullable column"


def _format_missing_key_field(violation: ValidationViolation) -> str:
    if violation.column_name:
        return f"Missing key field '{violation.column_name}'"
    return "Missing key field"


def _format_duplicate_keys(violation: ValidationViolation) -> str:
    if violation.column_name:
        return f"Duplicate keys for '{violation.column_name}'"
    return "Duplicate keys"


_VIOLATION_FORMATTERS: dict[ViolationType, Callable[[ValidationViolation], str]] = {
    ViolationType.MISSING_TABLE: _format_missing_table,
    ViolationType.METADATA_MISMATCH: _format_metadata_mismatch,
    ViolationType.MISSING_COLUMN: _format_missing_column,
    ViolationType.EXTRA_COLUMN: _format_extra_column,
    ViolationType.TYPE_MISMATCH: _format_type_mismatch,
    ViolationType.NULLABILITY_MISMATCH: _format_nullability_mismatch,
    ViolationType.NULL_VIOLATION: _format_null_violation,
    ViolationType.MISSING_KEY_FIELD: _format_missing_key_field,
    ViolationType.DUPLICATE_KEYS: _format_duplicate_keys,
}


__all__ = ["ValidationViolation", "ViolationType"]
