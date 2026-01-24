"""Hamilton data-quality validators for pipeline outputs."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from hamilton.data_quality import base as dq_base


class TableSchemaValidator(dq_base.DataValidator):
    """Validate that a table-like object matches an expected schema."""

    def __init__(self, *, expected_columns: Sequence[str], importance: str = "warn") -> None:
        super().__init__(importance)
        self._expected_columns = tuple(expected_columns)

    def applies_to(self, datatype: type[type]) -> bool:
        """Return whether the validator applies to the datatype.

        Returns
        -------
        bool
            ``True`` for any datatype.
        """
        _ = self._expected_columns
        _ = datatype
        return True

    def description(self) -> str:
        """Return a human-readable validator description.

        Returns
        -------
        str
            Validator description.
        """
        return (
            "Validates that table-like outputs include the expected columns "
            f"({len(self._expected_columns)} columns)."
        )

    @classmethod
    def name(cls) -> str:
        """Return the validator name.

        Returns
        -------
        str
            Validator name.
        """
        return "table_schema_validator"

    def validate(self, dataset: Any) -> dq_base.ValidationResult:
        """Validate that the dataset contains the expected columns.

        Returns
        -------
        ValidationResult
            Validation result payload.
        """
        schema = getattr(dataset, "schema", None)
        names = getattr(schema, "names", None)
        if not names:
            return dq_base.ValidationResult(
                passes=False,
                message="Table schema is missing or empty.",
                diagnostics={"expected_columns": list(self._expected_columns)},
            )
        missing = [name for name in self._expected_columns if name not in names]
        if missing:
            return dq_base.ValidationResult(
                passes=False,
                message="Table schema missing expected columns.",
                diagnostics={
                    "missing_columns": missing,
                    "expected_columns": list(self._expected_columns),
                    "available_columns": list(names),
                },
            )
        return dq_base.ValidationResult(
            passes=True,
            message="Table schema validation passed.",
            diagnostics={"expected_columns": list(self._expected_columns)},
        )


class NonEmptyTableValidator(dq_base.DataValidator):
    """Warn when a table-like output is empty."""

    def __init__(self, *, importance: str = "warn") -> None:
        super().__init__(importance)

    def applies_to(self, datatype: type[type]) -> bool:
        """Return whether the validator applies to the datatype.

        Returns
        -------
        bool
            ``True`` for any datatype.
        """
        _ = self.importance
        _ = datatype
        return True

    def description(self) -> str:
        """Return a human-readable validator description.

        Returns
        -------
        str
            Validator description.
        """
        return f"Validates that table-like outputs are non-empty ({self.importance})."

    @classmethod
    def name(cls) -> str:
        """Return the validator name.

        Returns
        -------
        str
            Validator name.
        """
        return "non_empty_table_validator"

    def validate(self, dataset: Any) -> dq_base.ValidationResult:
        """Validate that the dataset has at least one row.

        Returns
        -------
        ValidationResult
            Validation result payload.
        """
        value = getattr(dataset, "num_rows", None)
        rows = value if isinstance(value, int) and not isinstance(value, bool) else None
        if rows is None:
            return dq_base.ValidationResult(
                passes=True,
                message="Table row count unavailable; skipping non-empty check.",
                diagnostics={"importance": str(self.importance)},
            )
        if rows <= 0:
            return dq_base.ValidationResult(
                passes=False,
                message="Table is empty.",
                diagnostics={"rows": rows, "importance": str(self.importance)},
            )
        return dq_base.ValidationResult(
            passes=True,
            message="Table has rows.",
            diagnostics={"rows": rows, "importance": str(self.importance)},
        )


__all__ = ["NonEmptyTableValidator", "TableSchemaValidator"]
