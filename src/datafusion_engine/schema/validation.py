"""DataFusion-backed schema validation helpers."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as f
from pyarrow import Table as ArrowTable

from core_types import NonNegativeInt
from datafusion_engine.arrow.coercion import to_arrow_table
from datafusion_engine.arrow.interop import (
    DataTypeLike,
    SchemaLike,
    TableLike,
    empty_table_for_schema,
)
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.expr.cast import safe_cast
from datafusion_engine.schema.alignment import AlignmentInfo, CastErrorPolicy, align_to_schema
from datafusion_engine.schema.introspection_core import table_constraint_rows
from datafusion_engine.schema.spec_protocol import ArrowFieldSpec, TableSchemaSpec
from datafusion_engine.session.helpers import deregister_table, register_temp_table
from datafusion_engine.sql.options import sql_options_for_profile
from serde_msgspec import StructBaseStrict
from validation.violations import ValidationViolation, ViolationType

if TYPE_CHECKING:
    from datafusion import SQLOptions
    from datafusion.dataframe import DataFrame
    from datafusion.expr import Expr

    from datafusion_engine.schema.contracts import SchemaContract, TableConstraints
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class _SchemaContractAdapter:
    contract: SchemaContract

    @property
    def name(self) -> str:
        return self.contract.table_name

    @property
    def fields(self) -> Sequence[ArrowFieldSpec]:
        return self.contract.columns

    @property
    def key_fields(self) -> Sequence[str]:
        return ()

    @property
    def required_non_null(self) -> Sequence[str]:
        return tuple(field.name for field in self.contract.columns if not field.nullable)

    def to_arrow_schema(self) -> SchemaLike:
        return self.contract.to_arrow_schema()


def _constraints_from_spec(spec: TableSchemaSpec) -> TableConstraints:
    from datafusion_engine.schema.contracts import TableConstraints

    primary_key = tuple(spec.key_fields)
    not_null = tuple(spec.required_non_null)
    return TableConstraints(
        primary_key=primary_key or None,
        not_null=not_null or None,
    )


class ArrowValidationOptions(StructBaseStrict, frozen=True):
    """Options for DataFusion-backed table validation."""

    strict: bool | Literal["filter"] = "filter"
    coerce: bool = False
    max_errors: NonNegativeInt | None = None
    emit_invalid_rows: bool = True
    emit_error_table: bool = True


@dataclass(frozen=True)
class SchemaValidationReport:
    """Validation output and metadata."""

    valid: bool
    validated: TableLike
    errors: TableLike
    stats: TableLike
    invalid_rows: TableLike | None


@dataclass(frozen=True)
class _ValidationContext:
    aligned_name: str
    original_name: str
    aligned_table: ArrowTable
    original_table: ArrowTable
    spec: TableSchemaSpec
    info: AlignmentInfo
    row_count: int


def _session_context(runtime_profile: DataFusionRuntimeProfile | None) -> SessionContext:
    if runtime_profile is not None:
        return runtime_profile.session_runtime().ctx
    return SessionContext()


def _expr_table(df: DataFrame) -> ArrowTable:
    return df.to_arrow_table()


def _datafusion_type_name(dtype: pa.DataType) -> str:
    """Return the DataFusion SQL type name for an Arrow dtype.

    Returns:
    -------
    str
        DataFusion SQL type name for the dtype.
    """
    from datafusion_engine.session.runtime_dataset_io import (
        _datafusion_type_name as runtime_type_name,
    )

    return runtime_type_name(dtype)


def _or_expressions(expressions: Sequence[Expr]) -> Expr | None:
    if not expressions:
        return None
    if len(expressions) == 1:
        return expressions[0]
    combined = expressions[0]
    for expr in expressions[1:]:
        combined |= expr
    return combined


def _constraint_key_fields(rows: Sequence[Mapping[str, object]]) -> list[str]:
    constraints: dict[tuple[str, str], list[tuple[int, str]]] = {}
    for row in rows:
        constraint_type = row.get("constraint_type")
        if not isinstance(constraint_type, str):
            continue
        constraint_kind = constraint_type.upper()
        if constraint_kind not in {"PRIMARY KEY", "UNIQUE"}:
            continue
        constraint_name = row.get("constraint_name")
        column_name = row.get("column_name")
        if not isinstance(constraint_name, str) or not constraint_name:
            continue
        if not isinstance(column_name, str) or not column_name:
            continue
        ordinal = row.get("ordinal_position")
        position = int(ordinal) if isinstance(ordinal, (int, float)) else 0
        constraints.setdefault((constraint_kind, constraint_name), []).append(
            (position, column_name)
        )
    if not constraints:
        return []
    for kind in ("PRIMARY KEY", "UNIQUE"):
        candidates = {key: values for key, values in constraints.items() if key[0] == kind}
        if not candidates:
            continue
        _, values = sorted(candidates.items(), key=lambda item: item[0][1])[0]
        return [name for _, name in sorted(values, key=lambda item: item[0])]
    return []


def _resolve_key_fields(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    session: SessionContext,
    spec: TableSchemaSpec,
    sql_options: SQLOptions,
) -> Sequence[str]:
    from datafusion_engine.schema.contracts import constraint_key_fields

    constraints = _constraints_from_spec(spec)
    default_keys = constraint_key_fields(constraints) or spec.key_fields
    if runtime_profile is None or not runtime_profile.catalog.enable_information_schema:
        return default_keys
    try:
        rows = table_constraint_rows(
            session,
            table_name=spec.name,
            sql_options=sql_options,
        )
    except (RuntimeError, TypeError, ValueError):
        return default_keys
    resolved = _constraint_key_fields(rows)
    return resolved or default_keys


def _count_rows(
    ctx: SessionContext,
    *,
    table_name: str,
    where: Expr | None = None,
    sql_options: SQLOptions,
) -> int:
    _ = sql_options
    df = ctx.table(table_name)
    if where is not None:
        df = df.filter(where)
    table = _expr_table(df.aggregate([], [f.sum(lit(1)).alias("count")]))
    value = table["count"][0].as_py() if table.num_rows else 0
    return int(value or 0)


def _align_for_validation(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions,
) -> tuple[TableLike, AlignmentInfo]:
    keep_extra = options.strict is False
    safe_cast = not options.coerce
    on_error: CastErrorPolicy = "unsafe" if options.coerce else "raise"
    aligned, info = align_to_schema(
        table,
        schema=spec.to_arrow_schema(),
        safe_cast=safe_cast,
        keep_extra_columns=keep_extra,
        on_error=on_error,
    )
    return aligned, info


def _cast_failure_count(
    ctx: SessionContext,
    *,
    table_name: str,
    column: str,
    dtype: DataTypeLike,
    sql_options: SQLOptions,
) -> int:
    _ = sql_options
    col_expr = col(column)
    cast_expr = safe_cast(col_expr, dtype)
    condition = col_expr.is_not_null() & cast_expr.is_null()
    failures_expr = f.sum(f.when(condition, lit(1)).otherwise(lit(0))).alias("failures")
    try:
        table = _expr_table(ctx.table(table_name).aggregate([], [failures_expr]))
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"DataFusion cast check failed for column {column!r}: {exc}."
        raise DataFusionEngineError(msg, kind=ErrorKind.DATAFUSION) from exc
    value = table["failures"][0].as_py() if table.num_rows else 0
    return int(value or 0)


def _duplicate_row_count(
    ctx: SessionContext,
    *,
    table_name: str,
    keys: Sequence[str],
    sql_options: SQLOptions,
) -> int:
    if not keys:
        return 0
    _ = sql_options
    df = ctx.table(table_name)
    group_cols = [col(name) for name in keys]
    dupe_count_expr = f.sum(lit(1)).alias("dupe_count")
    grouped = df.aggregate(group_cols, [dupe_count_expr])
    filtered = grouped.filter(col("dupe_count") > lit(1))
    table = _expr_table(filtered.aggregate([], [f.sum(col("dupe_count")).alias("dupes")]))
    value = table["dupes"][0].as_py() if table.num_rows else 0
    return int(value or 0)


def _missing_column_errors(info: AlignmentInfo, row_count: int) -> list[ValidationViolation]:
    return [
        ValidationViolation(
            violation_type=ViolationType.MISSING_COLUMN,
            column_name=name,
            count=row_count,
        )
        for name in info["missing_cols"]
    ]


def _extra_column_errors(info: AlignmentInfo, row_count: int) -> list[ValidationViolation]:
    return [
        ValidationViolation(
            violation_type=ViolationType.EXTRA_COLUMN,
            column_name=name,
            count=row_count,
        )
        for name in info["dropped_cols"]
    ]


def _type_mismatch_errors(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    row_count: int,
) -> list[ValidationViolation]:
    return [
        ValidationViolation(
            violation_type=ViolationType.TYPE_MISMATCH,
            column_name=field.name,
            count=row_count,
        )
        for field in spec.fields
        if field.name in table.column_names and table.schema.field(field.name).type != field.dtype
    ]


def _cast_type_mismatch_errors(
    ctx: SessionContext,
    *,
    table_name: str,
    table: TableLike,
    spec: TableSchemaSpec,
    sql_options: SQLOptions,
) -> list[ValidationViolation]:
    entries: list[ValidationViolation] = []
    for field in spec.fields:
        if field.name not in table.column_names:
            continue
        if table.schema.field(field.name).type == field.dtype:
            continue
        count = _cast_failure_count(
            ctx,
            table_name=table_name,
            column=field.name,
            dtype=field.dtype,
            sql_options=sql_options,
        )
        if count:
            entries.append(
                ValidationViolation(
                    violation_type=ViolationType.TYPE_MISMATCH,
                    column_name=field.name,
                    count=count,
                )
            )
    return entries


def _null_violation_results(
    ctx: SessionContext,
    *,
    table_name: str,
    required_fields: Sequence[str],
    missing_cols: Sequence[str],
    sql_options: SQLOptions,
) -> tuple[list[ValidationViolation], Expr | None]:
    required_present = [name for name in required_fields if name not in missing_cols]
    if not required_present:
        return [], None
    _ = sql_options
    selections: list[Expr] = []
    alias_by_field: dict[str, str] = {}
    null_checks: list[Expr] = []
    for idx, name in enumerate(required_present):
        alias = f"null_{idx}"
        alias_by_field[name] = alias
        null_check = col(name).is_null()
        null_checks.append(null_check)
        case_expr = f.when(null_check, lit(1)).otherwise(lit(0))
        selections.append(f.sum(case_expr).alias(alias))
    table = _expr_table(ctx.table(table_name).aggregate([], selections))
    entries: list[ValidationViolation] = []
    for name, alias in alias_by_field.items():
        value = table[alias][0].as_py() if table.num_rows else 0
        count = int(value or 0)
        if count:
            entries.append(
                ValidationViolation(
                    violation_type=ViolationType.NULL_VIOLATION,
                    column_name=name,
                    count=count,
                )
            )
    invalid_expr = _or_expressions(null_checks)
    return entries, invalid_expr


@dataclass(frozen=True)
class _RowFilterInputs:
    ctx: SessionContext
    table_name: str
    aligned: TableLike
    invalid_expr: Expr | None
    options: ArrowValidationOptions
    sql_options: SQLOptions


def _row_filter_results(
    inputs: _RowFilterInputs,
) -> tuple[TableLike, TableLike | None, int]:
    if inputs.invalid_expr is None:
        return inputs.aligned, None, 0
    invalid_rows = None
    if inputs.options.emit_invalid_rows or inputs.options.strict == "filter":
        invalid_rows = _expr_table(inputs.ctx.table(inputs.table_name).filter(inputs.invalid_expr))
    invalid_row_count = _count_rows(
        inputs.ctx,
        table_name=inputs.table_name,
        where=inputs.invalid_expr,
        sql_options=inputs.sql_options,
    )
    validated = inputs.aligned
    if inputs.options.strict == "filter":
        validated = _expr_table(inputs.ctx.table(inputs.table_name).filter(~inputs.invalid_expr))
    return validated, invalid_rows, invalid_row_count


@dataclass(frozen=True)
class _KeyFieldInputs:
    ctx: SessionContext
    table_name: str
    key_fields: Sequence[str]
    missing_cols: Sequence[str]
    row_count: int
    sql_options: SQLOptions


def _key_field_errors(
    inputs: _KeyFieldInputs,
) -> list[ValidationViolation]:
    if not inputs.key_fields:
        return []
    missing_keys = missing_key_fields(inputs.key_fields, missing_cols=inputs.missing_cols)
    entries = [
        ValidationViolation(
            violation_type=ViolationType.MISSING_KEY_FIELD,
            column_name=key,
            count=inputs.row_count,
        )
        for key in missing_keys
    ]
    if not missing_keys:
        dup_count = _duplicate_row_count(
            inputs.ctx,
            table_name=inputs.table_name,
            keys=inputs.key_fields,
            sql_options=inputs.sql_options,
        )
        if dup_count:
            key_label = ",".join(inputs.key_fields)
            entries.append(
                ValidationViolation(
                    violation_type=ViolationType.DUPLICATE_KEYS,
                    column_name=key_label,
                    count=dup_count,
                )
            )
    return entries


def _error_table(entries: Sequence[ValidationViolation]) -> TableLike:
    codes = [entry.violation_type.value for entry in entries]
    columns = [entry.column_name for entry in entries]
    counts = [entry.count or 0 for entry in entries]
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
    schema = pa.schema(
        [
            pa.field("error_code", pa.string()),
            pa.field("error_column", pa.string()),
            pa.field("error_count", pa.int64()),
        ]
    )
    return empty_table_for_schema(schema)


def _build_validation_report(
    *,
    validated: TableLike,
    invalid_rows: TableLike | None,
    invalid_row_count: int,
    error_entries: Sequence[ValidationViolation],
    options: ArrowValidationOptions,
) -> SchemaValidationReport:
    total_error_rows = sum(entry.count or 0 for entry in error_entries)
    total_errors = len(error_entries)
    valid = total_errors == 0 and invalid_row_count == 0
    errors = _error_table(error_entries) if options.emit_error_table else _empty_error_table()
    stats = _stats_table(
        valid=valid,
        total_errors=total_errors,
        total_error_rows=total_error_rows,
        total_invalid_rows=invalid_row_count,
    )
    return SchemaValidationReport(
        valid=valid,
        validated=validated,
        errors=errors,
        stats=stats,
        invalid_rows=invalid_rows,
    )


def _collect_validation_results(
    session: SessionContext,
    *,
    context: _ValidationContext,
    options: ArrowValidationOptions,
    key_fields: Sequence[str],
    sql_options: SQLOptions,
) -> tuple[list[ValidationViolation], TableLike, TableLike | None, int]:
    error_entries: list[ValidationViolation] = []
    error_entries.extend(_missing_column_errors(context.info, context.row_count))
    if options.strict is True:
        error_entries.extend(_extra_column_errors(context.info, context.row_count))
    if options.coerce:
        error_entries.extend(
            _cast_type_mismatch_errors(
                session,
                table_name=context.original_name,
                table=context.original_table,
                spec=context.spec,
                sql_options=sql_options,
            )
        )
    else:
        error_entries.extend(
            _type_mismatch_errors(
                context.original_table,
                spec=context.spec,
                row_count=context.row_count,
            ),
        )

    null_entries, invalid_expr = _null_violation_results(
        session,
        table_name=context.aligned_name,
        required_fields=required_field_names(context.spec),
        missing_cols=context.info["missing_cols"],
        sql_options=sql_options,
    )
    error_entries.extend(null_entries)
    validated, invalid_rows, invalid_row_count = _row_filter_results(
        _RowFilterInputs(
            ctx=session,
            table_name=context.aligned_name,
            aligned=context.aligned_table,
            invalid_expr=invalid_expr,
            options=options,
            sql_options=sql_options,
        )
    )
    error_entries.extend(
        _key_field_errors(
            _KeyFieldInputs(
                ctx=session,
                table_name=context.aligned_name,
                key_fields=key_fields,
                missing_cols=context.info["missing_cols"],
                row_count=context.row_count,
                sql_options=sql_options,
            )
        ),
    )
    return error_entries, validated, invalid_rows, invalid_row_count


def _prepare_validation_context(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    table: TableLike,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions,
) -> tuple[SessionContext, _ValidationContext, str, str]:
    session = _session_context(runtime_profile)
    resolved_table = to_arrow_table(table)
    row_count = int(resolved_table.num_rows)
    aligned, info = _align_for_validation(resolved_table, spec=spec, options=options)
    aligned_table = cast("ArrowTable", aligned)
    aligned_name = register_temp_table(session, aligned_table, prefix="schema_validate")
    original_name = register_temp_table(session, resolved_table, prefix="schema_validate_src")
    return (
        session,
        _ValidationContext(
            aligned_name=aligned_name,
            original_name=original_name,
            aligned_table=aligned_table,
            original_table=resolved_table,
            spec=spec,
            info=info,
            row_count=row_count,
        ),
        aligned_name,
        original_name,
    )


def validate_table(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> SchemaValidationReport:
    """Validate an Arrow table against a schema spec.

    Returns:
    -------
    SchemaValidationReport
        Validation report with invalid rows and stats.
    """
    options = options or ArrowValidationOptions()
    session, context, aligned_name, original_name = _prepare_validation_context(
        runtime_profile,
        table=table,
        spec=spec,
        options=options,
    )
    _LOGGER.debug(
        "schema.validate_table.start",
        extra={
            "table_name": spec.name,
            "row_count": context.row_count,
            "strict_mode": options.strict,
            "coerce": options.coerce,
        },
    )
    sql_options = sql_options_for_profile(runtime_profile)
    key_fields = _resolve_key_fields(
        runtime_profile,
        session=session,
        spec=spec,
        sql_options=sql_options,
    )
    try:
        error_entries, validated, invalid_rows, invalid_row_count = _collect_validation_results(
            session,
            context=context,
            options=options,
            key_fields=key_fields,
            sql_options=sql_options,
        )
    finally:
        deregister_table(session, aligned_name)
        deregister_table(session, original_name)

    if options.max_errors is not None:
        error_entries = error_entries[: options.max_errors]

    _LOGGER.debug(
        "schema.validate_table.complete",
        extra={
            "table_name": spec.name,
            "row_count": context.row_count,
            "invalid_row_count": invalid_row_count,
            "error_count": len(error_entries),
        },
    )
    return _build_validation_report(
        validated=validated,
        invalid_rows=invalid_rows,
        invalid_row_count=invalid_row_count,
        error_entries=error_entries,
        options=options,
    )


def validate_schema_contract(
    table: TableLike,
    *,
    contract: SchemaContract,
    options: ArrowValidationOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> SchemaValidationReport:
    """Validate a table against a SchemaContract definition.

    Returns:
    -------
    SchemaValidationReport
        Validation report with invalid rows and stats.
    """
    adapter = _SchemaContractAdapter(contract=contract)
    return validate_table(
        table,
        spec=adapter,
        options=options,
        runtime_profile=runtime_profile,
    )


def required_field_names(spec: TableSchemaSpec) -> tuple[str, ...]:
    """Return required field names (explicit or non-nullable).

    Returns:
    -------
    tuple[str, ...]
        Required field names.
    """
    required = set(_constraints_from_spec(spec).required_non_null())
    return tuple(
        field.name for field in spec.fields if field.name in required or not field.nullable
    )


def missing_key_fields(keys: Sequence[str], *, missing_cols: Sequence[str]) -> tuple[str, ...]:
    """Return key fields missing from the available columns.

    Returns:
    -------
    tuple[str, ...]
        Missing key field names.
    """
    missing = set(missing_cols)
    return tuple(key for key in keys if key in missing)


__all__ = [
    "ArrowValidationOptions",
    "SchemaValidationReport",
    "missing_key_fields",
    "required_field_names",
    "validate_schema_contract",
    "validate_table",
]
