"""Constraint helpers and contracts for schema governance."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Literal

import msgspec

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from schema_spec.specs import TableSchemaSpec


ConstraintType = Literal["pk", "not_null", "check", "unique"]


def normalize_column_names(values: Iterable[str] | None) -> tuple[str, ...]:
    """Return normalized column names in stable order."""
    if values is None:
        return ()
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        name = str(value).strip()
        if not name or name in seen:
            continue
        ordered.append(name)
        seen.add(name)
    return tuple(ordered)


def merge_constraint_expressions(*parts: Iterable[str]) -> tuple[str, ...]:
    """Return normalized constraint expressions from multiple sources."""
    merged: list[str] = []
    seen: set[str] = set()
    for part in parts:
        for entry in part:
            normalized = str(entry).strip()
            if not normalized or normalized in seen:
                continue
            merged.append(normalized)
            seen.add(normalized)
    return tuple(merged)


class ConstraintSpec(msgspec.Struct, frozen=True):
    """Constraint specification for governance policies."""

    constraint_type: ConstraintType
    column: str | None = None
    columns: tuple[str, ...] | None = None
    expression: str | None = None

    def normalized_columns(self) -> tuple[str, ...]:
        """Return normalized column names from the constraint spec."""
        if self.columns:
            return normalize_column_names(self.columns)
        if self.column:
            return normalize_column_names((self.column,))
        return ()

    def normalized_expression(self) -> str | None:
        """Return a normalized expression string, if present."""
        if self.expression is None:
            return None
        normalized = self.expression.strip()
        return normalized or None


class TableConstraints(msgspec.Struct, frozen=True):
    """Governance constraints for a table."""

    primary_key: tuple[str, ...] | None = None
    not_null: tuple[str, ...] | None = None
    checks: tuple[ConstraintSpec, ...] | None = None
    unique: tuple[tuple[str, ...], ...] | None = None

    def required_non_null(self) -> tuple[str, ...]:
        """Return normalized non-null column names."""
        return normalize_column_names((*(self.not_null or ()), *(self.primary_key or ())))

    def check_expressions(self) -> tuple[str, ...]:
        """Return normalized check expressions."""
        expressions: list[str] = []
        for check in self.checks or ():
            if check.constraint_type != "check":
                continue
            normalized = check.normalized_expression()
            if normalized:
                expressions.append(normalized)
        return merge_constraint_expressions(expressions)


def table_constraints_from_spec(
    spec: TableSchemaSpec,
    *,
    checks: Iterable[str] = (),
    unique: Iterable[Iterable[str]] = (),
) -> TableConstraints:
    """Build a TableConstraints payload from a schema spec.

    Returns:
        TableConstraints: Normalized constraints derived from schema inputs.
    """
    primary_key = normalize_column_names(spec.key_fields)
    not_null = normalize_column_names(spec.required_non_null)
    check_specs = tuple(
        ConstraintSpec(constraint_type="check", expression=expression)
        for expression in merge_constraint_expressions(checks)
    )
    unique_specs: list[tuple[str, ...]] = []
    for entry in unique:
        normalized = normalize_column_names(entry)
        if normalized:
            unique_specs.append(normalized)
    return TableConstraints(
        primary_key=primary_key or None,
        not_null=not_null or None,
        checks=check_specs or None,
        unique=tuple(unique_specs) or None,
    )


def constraint_key_fields(constraints: TableConstraints) -> tuple[str, ...]:
    """Return key fields derived from constraint metadata."""
    if constraints.primary_key:
        return constraints.primary_key
    if constraints.unique:
        return constraints.unique[0]
    return ()


def delta_check_constraints(constraints: TableConstraints) -> tuple[str, ...]:
    """Return Delta CHECK constraint expressions for a constraint bundle."""
    not_null_exprs = tuple(f"{name} IS NOT NULL" for name in constraints.required_non_null())
    return merge_constraint_expressions(not_null_exprs, constraints.check_expressions())


def table_constraint_definitions(constraints: TableConstraints) -> tuple[str, ...]:
    """Return DDL-style constraint definitions for metadata snapshots."""
    definitions: list[str] = []
    primary_key = normalize_column_names(constraints.primary_key)
    if primary_key:
        definitions.append(f"PRIMARY KEY ({', '.join(primary_key)})")
    for unique_cols in constraints.unique or ():
        normalized = normalize_column_names(unique_cols)
        if normalized:
            definitions.append(f"UNIQUE ({', '.join(normalized)})")
    definitions.extend(delta_check_constraints(constraints))
    return merge_constraint_expressions(definitions)


def table_constraints_from_location(
    location: DatasetLocation | None,
    *,
    extra_checks: Iterable[str] = (),
) -> TableConstraints:
    """Resolve TableConstraints for a dataset location.

    Returns:
        TableConstraints: Constraints resolved from location/spec metadata.
    """
    if location is None:
        return TableConstraints(
            checks=tuple(
                ConstraintSpec(constraint_type="check", expression=expression)
                for expression in merge_constraint_expressions(extra_checks)
            )
            or None,
        )
    dataset_spec = location.dataset_spec
    resolved = location.resolved
    table_spec = resolved.table_spec or (
        dataset_spec.table_spec if dataset_spec is not None else None
    )
    resolved_checks: tuple[str, ...]
    if resolved.delta_constraints:
        resolved_checks = merge_constraint_expressions(resolved.delta_constraints, extra_checks)
    elif dataset_spec is not None:
        from schema_spec.dataset_spec import dataset_spec_delta_constraints

        resolved_checks = merge_constraint_expressions(
            dataset_spec_delta_constraints(dataset_spec),
            extra_checks,
        )
    else:
        resolved_checks = merge_constraint_expressions(extra_checks)
    if table_spec is None:
        return TableConstraints(
            checks=tuple(
                ConstraintSpec(constraint_type="check", expression=expression)
                for expression in resolved_checks
            )
            or None,
        )
    return table_constraints_from_spec(table_spec, checks=resolved_checks)


def delta_constraints_for_location(
    location: DatasetLocation | None,
    *,
    extra_checks: Iterable[str] = (),
) -> tuple[str, ...]:
    """Return Delta CHECK constraints resolved from a dataset location."""
    return delta_check_constraints(
        table_constraints_from_location(location, extra_checks=extra_checks)
    )


__all__ = [
    "ConstraintSpec",
    "ConstraintType",
    "TableConstraints",
    "constraint_key_fields",
    "delta_check_constraints",
    "delta_constraints_for_location",
    "merge_constraint_expressions",
    "normalize_column_names",
    "table_constraint_definitions",
    "table_constraints_from_location",
    "table_constraints_from_spec",
]
