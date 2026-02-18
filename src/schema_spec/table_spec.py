"""Table/column schema contracts and helpers."""

from __future__ import annotations

from collections.abc import Iterable

from datafusion_engine.arrow.interop import SchemaLike
from schema_spec.dataset_contracts import ContractRow, SortKeySpec, TableSchemaContract
from schema_spec.dataset_spec import TableSpecConstraints
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import FieldBundle, TableSchemaSpec


def _merge_names(*parts: Iterable[str]) -> tuple[str, ...]:
    merged: list[str] = []
    seen: set[str] = set()
    for part in parts:
        for name in part:
            if name in seen:
                continue
            merged.append(name)
            seen.add(name)
    return tuple(merged)


def _merge_fields(
    bundles: Iterable[FieldBundle],
    fields: Iterable[FieldSpec],
) -> list[FieldSpec]:
    merged: list[FieldSpec] = []
    for bundle in bundles:
        merged.extend(bundle.fields)
    merged.extend(fields)
    return merged


def make_table_spec(
    name: str,
    *,
    version: int | None,
    bundles: Iterable[FieldBundle],
    fields: Iterable[FieldSpec],
    constraints: TableSpecConstraints | None = None,
) -> TableSchemaSpec:
    """Create a TableSchemaSpec from field bundles and explicit fields.

    Returns:
        TableSchemaSpec: Composed table schema specification.
    """
    if constraints is None:
        constraints = TableSpecConstraints()
    bundle_required = (bundle.required_non_null for bundle in bundles)
    bundle_keys = (bundle.key_fields for bundle in bundles)
    return TableSchemaSpec(
        name=name,
        version=version,
        fields=_merge_fields(bundles, fields),
        required_non_null=_merge_names(*bundle_required, constraints.required_non_null),
        key_fields=_merge_names(*bundle_keys, constraints.key_fields),
    )


def table_spec_from_schema(
    name: str,
    schema: SchemaLike,
    *,
    version: int | None = None,
) -> TableSchemaSpec:
    """Create a TableSchemaSpec from an Arrow schema.

    Returns:
        TableSchemaSpec: Table schema specification derived from the Arrow schema.
    """
    return TableSchemaSpec.from_schema(name, schema, version=version)


def delta_constraints_from_table_spec(table_spec: TableSchemaSpec) -> tuple[str, ...]:
    """Return default Delta constraints derived from schema constraints."""
    required = _merge_names(table_spec.required_non_null, table_spec.key_fields)
    if not required:
        return ()
    required_set = set(required)
    ordered = [field.name for field in table_spec.fields if field.name in required_set]
    return tuple(f"{name} IS NOT NULL" for name in ordered)


__all__ = [
    "ContractRow",
    "SortKeySpec",
    "TableSchemaContract",
    "TableSchemaSpec",
    "delta_constraints_from_table_spec",
    "make_table_spec",
    "table_spec_from_schema",
]
