"""Constraint compilation helpers for Arrow schemas."""

from __future__ import annotations

from collections.abc import Sequence

import arrowdsl.core.interop as pa
from arrowdsl.core.interop import ComputeExpression, ensure_expression, pc
from schema_spec.specs import TableSchemaSpec


def required_field_names(spec: TableSchemaSpec) -> tuple[str, ...]:
    """Return required field names (explicit or non-nullable).

    Returns
    -------
    tuple[str, ...]
        Required field names.
    """
    required = set(spec.required_non_null)
    return tuple(
        field.name for field in spec.fields if field.name in required or not field.nullable
    )


def required_non_null_mask(
    required: Sequence[str],
    *,
    available: set[str],
) -> ComputeExpression:
    """Return a plan-lane mask for required non-null violations.

    Returns
    -------
    ComputeExpression
        Boolean expression for invalid rows.
    """
    exprs = [
        ensure_expression(pc.invert(pc.is_valid(pc.field(name))))
        for name in required
        if name in available
    ]
    if not exprs:
        return ensure_expression(pc.scalar(pa.scalar(value=False)))
    return ensure_expression(pc.or_(*exprs))


def missing_key_fields(keys: Sequence[str], *, missing_cols: Sequence[str]) -> tuple[str, ...]:
    """Return key fields missing from the available columns.

    Returns
    -------
    tuple[str, ...]
        Missing key field names.
    """
    missing = set(missing_cols)
    return tuple(key for key in keys if key in missing)


__all__ = [
    "missing_key_fields",
    "required_field_names",
    "required_non_null_mask",
]
