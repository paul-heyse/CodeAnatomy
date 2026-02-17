"""Schema divergence contracts and comparison helpers."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True)
class SchemaDivergence:
    """Result of comparing a spec schema against a plan-derived schema."""

    has_divergence: bool
    added_columns: tuple[str, ...]
    removed_columns: tuple[str, ...]
    type_mismatches: tuple[tuple[str, str, str], ...]


def compute_schema_divergence(
    spec_schema: pa.Schema,
    plan_schema: pa.Schema,
) -> SchemaDivergence:
    """Compare a spec-declared schema against a plan-derived schema.

    Returns:
        SchemaDivergence: Added/removed/type-mismatch summary between schemas.
    """
    spec_names = {field.name for field in spec_schema}
    plan_names = {field.name for field in plan_schema}

    added = tuple(sorted(plan_names - spec_names))
    removed = tuple(sorted(spec_names - plan_names))

    type_mismatches: list[tuple[str, str, str]] = []
    for name in sorted(spec_names & plan_names):
        spec_field = spec_schema.field(name)
        plan_field = plan_schema.field(name)
        spec_type = str(spec_field.type)
        plan_type = str(plan_field.type)
        if spec_type != plan_type:
            type_mismatches.append((name, spec_type, plan_type))

    has_divergence = bool(added or removed or type_mismatches)
    return SchemaDivergence(
        has_divergence=has_divergence,
        added_columns=added,
        removed_columns=removed,
        type_mismatches=tuple(type_mismatches),
    )


__all__ = ["SchemaDivergence", "compute_schema_divergence"]
