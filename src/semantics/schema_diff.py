"""Schema diff utilities for semantic dataset evolution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from schema_spec.specs import TableSchemaSpec
from schema_spec.system import ContractSpec

if TYPE_CHECKING:
    from schema_spec.field_spec import FieldSpec


@dataclass(frozen=True)
class FieldTypeChange:
    """Type change for a schema field."""

    name: str
    before: str
    after: str


@dataclass(frozen=True)
class FieldNullabilityChange:
    """Nullability change for a schema field."""

    name: str
    before: bool
    after: bool

    @property
    def tightened(self) -> bool:
        """Return True when nullability tightened (nullable -> non-nullable)."""
        return self.before and not self.after


@dataclass(frozen=True)
class SchemaDiff:
    """Schema diff summary for migration enforcement."""

    added: tuple[str, ...]
    removed: tuple[str, ...]
    type_changes: tuple[FieldTypeChange, ...]
    nullability_changes: tuple[FieldNullabilityChange, ...]
    required_non_null_added: tuple[str, ...]
    required_non_null_removed: tuple[str, ...]
    key_fields_added: tuple[str, ...]
    key_fields_removed: tuple[str, ...]

    @property
    def is_breaking(self) -> bool:
        """Return True when the diff is breaking."""
        return bool(
            self.removed
            or self.type_changes
            or any(change.tightened for change in self.nullability_changes)
            or self.required_non_null_added
            or self.key_fields_added
            or self.key_fields_removed
        )

    def summary_lines(self) -> tuple[str, ...]:
        """Return human-readable diff summary lines.

        Returns:
        -------
        tuple[str, ...]
            Summary lines describing schema changes.
        """
        lines: list[str] = []
        if self.added:
            lines.append(f"added_fields={self.added}")
        if self.removed:
            lines.append(f"removed_fields={self.removed}")
        if self.type_changes:
            lines.append(
                "type_changes=("
                + ", ".join(
                    f"{change.name}:{change.before}->{change.after}" for change in self.type_changes
                )
                + ")"
            )
        if self.nullability_changes:
            lines.append(
                "nullability_changes=("
                + ", ".join(
                    f"{change.name}:{change.before}->{change.after}"
                    for change in self.nullability_changes
                )
                + ")"
            )
        if self.required_non_null_added:
            lines.append(f"required_non_null_added={self.required_non_null_added}")
        if self.required_non_null_removed:
            lines.append(f"required_non_null_removed={self.required_non_null_removed}")
        if self.key_fields_added:
            lines.append(f"key_fields_added={self.key_fields_added}")
        if self.key_fields_removed:
            lines.append(f"key_fields_removed={self.key_fields_removed}")
        return tuple(lines)


def diff_contract_specs(source: ContractSpec, target: ContractSpec) -> SchemaDiff:
    """Return a SchemaDiff for two contract specs.

    Returns:
    -------
    SchemaDiff
        Diff summary for the contract specs.
    """
    return diff_table_schema(source.table_schema, target.table_schema)


def diff_table_schema(source: TableSchemaSpec, target: TableSchemaSpec) -> SchemaDiff:
    """Return a SchemaDiff between two table schema specs.

    Returns:
    -------
    SchemaDiff
        Diff summary for the table schemas.
    """
    source_fields = _field_index(source.fields)
    target_fields = _field_index(target.fields)
    source_names = set(source_fields)
    target_names = set(target_fields)

    added = tuple(sorted(target_names - source_names))
    removed = tuple(sorted(source_names - target_names))

    type_changes: list[FieldTypeChange] = []
    nullability_changes: list[FieldNullabilityChange] = []
    for name in sorted(source_names & target_names):
        src = source_fields[name]
        tgt = target_fields[name]
        if src.dtype != tgt.dtype:
            type_changes.append(
                FieldTypeChange(
                    name=name,
                    before=str(src.dtype),
                    after=str(tgt.dtype),
                )
            )
        if src.nullable != tgt.nullable:
            nullability_changes.append(
                FieldNullabilityChange(name=name, before=src.nullable, after=tgt.nullable)
            )

    required_non_null_added, required_non_null_removed = _diff_tuple_sets(
        source.required_non_null,
        target.required_non_null,
    )
    key_fields_added, key_fields_removed = _diff_tuple_sets(
        source.key_fields,
        target.key_fields,
    )

    return SchemaDiff(
        added=added,
        removed=removed,
        type_changes=tuple(type_changes),
        nullability_changes=tuple(nullability_changes),
        required_non_null_added=required_non_null_added,
        required_non_null_removed=required_non_null_removed,
        key_fields_added=key_fields_added,
        key_fields_removed=key_fields_removed,
    )


def _field_index(fields: list[FieldSpec]) -> dict[str, FieldSpec]:
    return {field.name: field for field in fields}


def _diff_tuple_sets(
    source: tuple[str, ...],
    target: tuple[str, ...],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    source_set = set(source)
    target_set = set(target)
    added = tuple(sorted(target_set - source_set))
    removed = tuple(sorted(source_set - target_set))
    return added, removed


__all__ = [
    "FieldNullabilityChange",
    "FieldTypeChange",
    "SchemaDiff",
    "diff_contract_specs",
    "diff_table_schema",
]
