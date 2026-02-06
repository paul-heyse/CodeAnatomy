"""Hamilton data-quality validators for pipeline outputs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from hamilton.data_quality import base as dq_base

from datafusion_engine.arrow.coercion import to_arrow_table
from datafusion_engine.schema.contracts import SchemaContract
from utils.validation import find_missing

_SCHEMA_CONTRACTS: dict[str, SchemaContract] = {}


def set_schema_contracts(contracts: Mapping[str, object]) -> None:
    """Replace the global schema contract registry.

    Parameters
    ----------
    contracts
        Mapping of dataset names to schema contracts (or other values).
    """
    _SCHEMA_CONTRACTS.clear()
    for name, contract in contracts.items():
        if isinstance(contract, SchemaContract):
            _SCHEMA_CONTRACTS[str(name)] = contract


def _schema_contract_for(name: str) -> SchemaContract | None:
    return _SCHEMA_CONTRACTS.get(name)


class TableSchemaValidator(dq_base.DataValidator):
    """Validate that a table-like object matches an expected schema."""

    def __init__(self, *, expected_columns: Sequence[str], importance: str = "warn") -> None:
        """__init__."""
        super().__init__(importance)
        self._expected_columns = tuple(expected_columns)

    def applies_to(self, datatype: type[type]) -> bool:
        """Return whether the validator applies to the datatype.

        Returns:
        -------
        bool
            ``True`` for any datatype.
        """
        _ = self._expected_columns
        _ = datatype
        return True

    def description(self) -> str:
        """Return a human-readable validator description.

        Returns:
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

        Returns:
        -------
        str
            Validator name.
        """
        return "table_schema_validator"

    def validate(self, dataset: Any) -> dq_base.ValidationResult:
        """Validate that the dataset contains the expected columns.

        Returns:
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
        missing = find_missing(self._expected_columns, names)
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


class SchemaContractValidator(dq_base.DataValidator):
    """Validate a table-like output against a registered schema contract."""

    def __init__(self, *, dataset_name: str, importance: str = "warn") -> None:
        """__init__."""
        super().__init__(importance)
        self._dataset_name = dataset_name

    def applies_to(self, datatype: type[type]) -> bool:
        """Return whether the validator applies to the datatype.

        Returns:
        -------
        bool
            ``True`` for any datatype.
        """
        return bool(self._dataset_name) and isinstance(datatype, type)

    def description(self) -> str:
        """Return a human-readable validator description.

        Returns:
        -------
        str
            Validator description.
        """
        return f"Validates schema contract for {self._dataset_name}."

    @classmethod
    def name(cls) -> str:
        """Return the validator name.

        Returns:
        -------
        str
            Validator name.
        """
        return "schema_contract_validator"

    def validate(self, dataset: Any) -> dq_base.ValidationResult:
        """Validate the dataset against its schema contract.

        Returns:
        -------
        ValidationResult
            Validation result payload.
        """
        contract = _schema_contract_for(self._dataset_name)
        if contract is None:
            return dq_base.ValidationResult(
                passes=True,
                message="Schema contract not registered; skipping validation.",
                diagnostics={"dataset_name": self._dataset_name},
            )
        try:
            table = to_arrow_table(dataset)
        except (TypeError, ValueError) as exc:
            return dq_base.ValidationResult(
                passes=False,
                message="Failed to coerce dataset to Arrow table.",
                diagnostics={"dataset_name": self._dataset_name, "error": str(exc)},
            )
        violations = contract.validate_against_schema(table.schema)
        if violations:
            return dq_base.ValidationResult(
                passes=False,
                message="Schema contract validation failed.",
                diagnostics={
                    "dataset_name": self._dataset_name,
                    "violations": [str(item) for item in violations],
                },
            )
        return dq_base.ValidationResult(
            passes=True,
            message="Schema contract validation passed.",
            diagnostics={"dataset_name": self._dataset_name},
        )


class NonEmptyTableValidator(dq_base.DataValidator):
    """Warn when a table-like output is empty."""

    def __init__(self, *, importance: str = "warn") -> None:
        """__init__."""
        super().__init__(importance)

    def applies_to(self, datatype: type[type]) -> bool:
        """Return whether the validator applies to the datatype.

        Returns:
        -------
        bool
            ``True`` for any datatype.
        """
        _ = self.importance
        _ = datatype
        return True

    def description(self) -> str:
        """Return a human-readable validator description.

        Returns:
        -------
        str
            Validator description.
        """
        return f"Validates that table-like outputs are non-empty ({self.importance})."

    @classmethod
    def name(cls) -> str:
        """Return the validator name.

        Returns:
        -------
        str
            Validator name.
        """
        return "non_empty_table_validator"

    def validate(self, dataset: Any) -> dq_base.ValidationResult:
        """Validate that the dataset has at least one row.

        Returns:
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


__all__ = [
    "NonEmptyTableValidator",
    "SchemaContractValidator",
    "TableSchemaValidator",
    "set_schema_contracts",
]
