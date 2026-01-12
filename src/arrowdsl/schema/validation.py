"""Arrow-native schema validation helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, cast

import arrowdsl.core.interop as pa
from arrowdsl.core.context import ExecutionContext, RuntimeProfile, SchemaValidationPolicy
from arrowdsl.core.interop import (
    ArrayLike,
    ComputeExpression,
    ScalarLike,
    TableLike,
    ensure_expression,
    pc,
)
from arrowdsl.plan.plan import Plan
from arrowdsl.schema.schema import AlignmentInfo, SchemaTransform
from schema_spec.specs import TableSchemaSpec


@dataclass(frozen=True)
class ArrowValidationOptions:
    """Options for Arrow-native table validation."""

    strict: bool | Literal["filter"] = "filter"
    coerce: bool = False
    max_errors: int | None = None
    emit_invalid_rows: bool = True
    emit_error_table: bool = True

    @classmethod
    def from_policy(cls, policy: SchemaValidationPolicy) -> ArrowValidationOptions:
        """Create options from a schema validation policy.

        Returns
        -------
        ArrowValidationOptions
            Options derived from the policy.
        """
        return cls(
            strict=policy.strict,
            coerce=policy.coerce,
            emit_invalid_rows=policy.strict == "filter",
        )


@dataclass(frozen=True)
class ValidationReport:
    """Validation output and metadata."""

    valid: bool
    validated: TableLike
    errors: TableLike
    stats: TableLike
    invalid_rows: TableLike | None


@dataclass(frozen=True)
class _ValidationErrorEntry:
    code: str
    column: str | None
    count: int


def _ensure_execution_context(ctx: ExecutionContext | None) -> ExecutionContext:
    if ctx is not None:
        return ctx
    return ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))


def _required_fields(spec: TableSchemaSpec) -> tuple[str, ...]:
    required = set(spec.required_non_null)
    return tuple(
        field.name for field in spec.fields if field.name in required or not field.nullable
    )


def _align_for_validation(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions,
) -> tuple[TableLike, AlignmentInfo]:
    keep_extra = options.strict is False
    safe_cast = not options.coerce
    on_error = "unsafe" if options.coerce else "keep"
    transform = SchemaTransform(
        schema=spec.to_arrow_schema(),
        safe_cast=safe_cast,
        keep_extra_columns=keep_extra,
        on_error=on_error,
    )
    return transform.apply_with_info(table)


def _combine_masks(masks: Sequence[ArrayLike]) -> ArrayLike | None:
    if not masks:
        return None
    combined = masks[0]
    for mask in masks[1:]:
        combined = pc.or_(combined, mask)
    return combined


def _count_mask(mask: ArrayLike) -> int:
    total = pc.call_function("sum", [pc.cast(mask, pa.int64())])
    value = cast("int | float | bool | None", cast("ScalarLike", total).as_py())
    if value is None:
        return 0
    return int(value)


def _duplicate_row_count(table: TableLike, keys: Sequence[str]) -> int:
    if not keys:
        return 0
    grouped = table.group_by(list(keys)).aggregate([(keys[0], "count")])
    count_col = f"{keys[0]}_count"
    dupes = grouped.filter(pc.greater(grouped[count_col], pa.scalar(1)))
    if dupes.num_rows == 0:
        return 0
    total = pc.call_function("sum", [dupes[count_col]])
    value = cast("int | float | bool | None", cast("ScalarLike", total).as_py())
    if value is None:
        return 0
    return int(value)


def _missing_column_errors(info: AlignmentInfo, row_count: int) -> list[_ValidationErrorEntry]:
    return [
        _ValidationErrorEntry("missing_column", name, row_count) for name in info["missing_cols"]
    ]


def _extra_column_errors(info: AlignmentInfo, row_count: int) -> list[_ValidationErrorEntry]:
    return [_ValidationErrorEntry("extra_column", name, row_count) for name in info["dropped_cols"]]


def _type_mismatch_errors(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    row_count: int,
) -> list[_ValidationErrorEntry]:
    return [
        _ValidationErrorEntry("type_mismatch", field.name, row_count)
        for field in spec.fields
        if field.name in table.column_names and table.schema.field(field.name).type != field.dtype
    ]


def _coerce_type_mismatch_errors(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
) -> list[_ValidationErrorEntry]:
    entries: list[_ValidationErrorEntry] = []
    for field in spec.fields:
        if field.name not in table.column_names:
            continue
        if table.schema.field(field.name).type == field.dtype:
            continue
        col = table[field.name]
        casted = pc.cast(col, field.dtype, safe=False)
        valid = pc.is_valid(col)
        cast_valid = pc.is_valid(casted)
        failures = pc.and_(valid, pc.invert(cast_valid))
        count = _count_mask(failures)
        if count:
            entries.append(_ValidationErrorEntry("type_mismatch", field.name, count))
    return entries


def _null_violation_results(
    aligned: TableLike,
    *,
    required_fields: Sequence[str],
    missing_cols: Sequence[str],
) -> tuple[list[_ValidationErrorEntry], list[ArrayLike]]:
    required_present = [name for name in required_fields if name not in missing_cols]
    masks = [pc.invert(pc.is_valid(aligned[name])) for name in required_present]
    entries = [
        _ValidationErrorEntry("null_violation", name, count)
        for name, mask in zip(required_present, masks, strict=True)
        if (count := _count_mask(mask))
    ]
    return entries, masks


def _row_filter_results(
    aligned: TableLike,
    *,
    masks: Sequence[ArrayLike],
    options: ArrowValidationOptions,
) -> tuple[TableLike, TableLike | None, int]:
    invalid_mask = _combine_masks(masks)
    if invalid_mask is None:
        return aligned, None, 0
    invalid_row_count = _count_mask(invalid_mask)
    invalid_rows = None
    if options.emit_invalid_rows or options.strict == "filter":
        invalid_rows = aligned.filter(invalid_mask)
    validated = aligned
    if options.strict == "filter":
        validated = aligned.filter(pc.invert(invalid_mask))
    return validated, invalid_rows, invalid_row_count


def _key_field_errors(
    aligned: TableLike,
    *,
    key_fields: Sequence[str],
    missing_cols: Sequence[str],
    row_count: int,
) -> list[_ValidationErrorEntry]:
    if not key_fields:
        return []
    missing_keys = [key for key in key_fields if key in missing_cols]
    entries = [_ValidationErrorEntry("missing_key_field", key, row_count) for key in missing_keys]
    if not missing_keys:
        dup_count = _duplicate_row_count(aligned, key_fields)
        if dup_count:
            key_label = ",".join(key_fields)
            entries.append(_ValidationErrorEntry("duplicate_keys", key_label, dup_count))
    return entries


def _error_table(entries: Sequence[_ValidationErrorEntry]) -> TableLike:
    codes = [entry.code for entry in entries]
    columns = [entry.column for entry in entries]
    counts = [entry.count for entry in entries]
    return pa.table(
        {
            "error_code": pa.array(codes, type=pa.string()),
            "error_column": pa.array(columns, type=pa.string()),
            "error_count": pa.array(counts, type=pa.int64()),
        }
    )


def _stats_table(
    *,
    valid: bool,
    total_errors: int,
    total_error_rows: int,
    total_invalid_rows: int,
) -> TableLike:
    return pa.table(
        {
            "valid": pa.array([valid], type=pa.bool_()),
            "total_errors": pa.array([total_errors], type=pa.int64()),
            "total_error_rows": pa.array([total_error_rows], type=pa.int64()),
            "total_invalid_rows": pa.array([total_invalid_rows], type=pa.int64()),
        }
    )


def _empty_error_table() -> TableLike:
    return pa.table(
        {
            "error_code": pa.array([], type=pa.string()),
            "error_column": pa.array([], type=pa.string()),
            "error_count": pa.array([], type=pa.int64()),
        }
    )


def required_non_null_mask(
    spec: TableSchemaSpec,
    *,
    available: set[str] | None = None,
) -> ComputeExpression:
    """Return a plan-lane mask for required non-null violations.

    Returns
    -------
    ComputeExpression
        Boolean expression for invalid rows.
    """
    required = _required_fields(spec)
    if available is not None:
        required = tuple(name for name in required if name in available)
    exprs = [ensure_expression(pc.invert(pc.is_valid(pc.field(name)))) for name in required]
    if not exprs:
        return ensure_expression(pc.scalar(pa.scalar(value=False)))
    return ensure_expression(pc.or_(*exprs))


def invalid_rows_plan(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    ctx: ExecutionContext,
) -> Plan:
    """Return a plan that filters rows failing required non-null checks.

    Returns
    -------
    Plan
        Plan yielding invalid rows.
    """
    available = set(table.column_names)
    mask = required_non_null_mask(spec, available=available)
    return Plan.table_source(table, label=f"{spec.name}_validate").filter(mask, ctx=ctx)


def duplicate_key_rows(
    table: TableLike,
    *,
    keys: Sequence[str],
    ctx: ExecutionContext,
) -> TableLike:
    """Return rows grouped by keys with duplicate counts.

    Returns
    -------
    TableLike
        Table containing duplicate key counts.
    """
    if not keys:
        return pa.table({})
    missing = [key for key in keys if key not in table.column_names]
    if missing:
        return pa.table({})
    count_col = f"{keys[0]}_count"
    plan = Plan.table_source(table, label="validate_keys").aggregate(
        group_keys=keys,
        aggs=[(keys[0], "count")],
        ctx=ctx,
    )
    dupes = plan.filter(
        ensure_expression(pc.greater(pc.field(count_col), pc.scalar(1))),
        ctx=ctx,
    )
    return dupes.to_table(ctx=ctx)


def _build_validation_report(
    *,
    validated: TableLike,
    invalid_rows: TableLike | None,
    invalid_row_count: int,
    error_entries: Sequence[_ValidationErrorEntry],
    options: ArrowValidationOptions,
) -> ValidationReport:
    total_error_rows = sum(entry.count for entry in error_entries)
    total_errors = len(error_entries)
    valid = total_errors == 0 and invalid_row_count == 0
    errors = _error_table(error_entries) if options.emit_error_table else _empty_error_table()
    stats = _stats_table(
        valid=valid,
        total_errors=total_errors,
        total_error_rows=total_error_rows,
        total_invalid_rows=invalid_row_count,
    )
    return ValidationReport(
        valid=valid,
        validated=validated,
        errors=errors,
        stats=stats,
        invalid_rows=invalid_rows,
    )


def validate_table(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions | None = None,
    ctx: ExecutionContext | None = None,
) -> ValidationReport:
    """Validate an Arrow table against a schema spec.

    Returns
    -------
    ValidationReport
        Validation report with invalid rows and stats.
    """
    ctx = _ensure_execution_context(ctx)
    options = options or ArrowValidationOptions.from_policy(ctx.schema_validation)
    aligned, info = _align_for_validation(table, spec=spec, options=options)

    row_count = int(table.num_rows)
    error_entries: list[_ValidationErrorEntry] = []
    error_entries.extend(_missing_column_errors(info, row_count))
    if options.strict is True:
        error_entries.extend(_extra_column_errors(info, row_count))
    if options.coerce:
        error_entries.extend(_coerce_type_mismatch_errors(table, spec=spec))
    else:
        error_entries.extend(
            _type_mismatch_errors(table, spec=spec, row_count=row_count),
        )

    null_entries, masks = _null_violation_results(
        aligned,
        required_fields=_required_fields(spec),
        missing_cols=info["missing_cols"],
    )
    error_entries.extend(null_entries)
    validated, invalid_rows, invalid_row_count = _row_filter_results(
        aligned,
        masks=masks,
        options=options,
    )
    error_entries.extend(
        _key_field_errors(
            aligned,
            key_fields=spec.key_fields,
            missing_cols=info["missing_cols"],
            row_count=row_count,
        ),
    )

    if options.max_errors is not None:
        error_entries = error_entries[: options.max_errors]

    return _build_validation_report(
        validated=validated,
        invalid_rows=invalid_rows,
        invalid_row_count=invalid_row_count,
        error_entries=error_entries,
        options=options,
    )


__all__ = [
    "ArrowValidationOptions",
    "ValidationReport",
    "duplicate_key_rows",
    "invalid_rows_plan",
    "required_non_null_mask",
    "validate_table",
]
