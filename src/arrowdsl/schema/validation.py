"""Arrow-native schema validation helpers."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import Literal, Protocol, cast

import pyarrow as pa
from datafusion import SessionContext
from pyarrow import Table as ArrowTable

import arrowdsl.core.interop as arrow_pa
from arrowdsl.core.execution_context import (
    ExecutionContext,
    SchemaValidationPolicy,
    execution_context_factory,
)
from arrowdsl.core.interop import (
    DataTypeLike,
    SchemaLike,
    TableLike,
    coerce_table_like,
)
from arrowdsl.schema.encoding_policy import EncodingPolicy
from arrowdsl.schema.policy import SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import (
    AlignmentInfo,
    missing_key_fields,
    required_field_names,
    required_non_null_mask,
)


class _ArrowFieldSpec(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def dtype(self) -> DataTypeLike: ...

    @property
    def nullable(self) -> bool: ...

    @property
    def metadata(self) -> Mapping[str, str]: ...

    @property
    def encoding(self) -> str | None: ...


class _TableSchemaSpec(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def fields(self) -> Sequence[_ArrowFieldSpec]: ...

    @property
    def key_fields(self) -> Sequence[str]: ...

    @property
    def required_non_null(self) -> Sequence[str]: ...

    def to_arrow_schema(self) -> SchemaLike: ...


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


@dataclass(frozen=True)
class _ValidationContext:
    aligned_name: str
    original_name: str
    aligned_table: ArrowTable
    original_table: ArrowTable
    spec: _TableSchemaSpec
    info: AlignmentInfo
    row_count: int


def _ensure_execution_context(ctx: ExecutionContext | None) -> ExecutionContext:
    if ctx is not None:
        return ctx
    return execution_context_factory("default")


def _session_context(ctx: ExecutionContext) -> SessionContext:
    runtime = ctx.runtime.datafusion
    if runtime is None:
        msg = "DataFusion runtime is required for schema validation."
        raise ValueError(msg)
    return runtime.session_context()


@lru_cache(maxsize=128)
def _datafusion_type_name(dtype: DataTypeLike) -> str:
    ctx = SessionContext()
    table = pa.Table.from_arrays(
        [pa.array([None], type=dtype)],
        names=["value"],
    )
    batches = list(table.to_batches())
    ctx.register_record_batches("t", batches)
    result = ctx.sql("SELECT arrow_typeof(value) AS dtype FROM t").to_arrow_table()
    value = result["dtype"][0].as_py()
    if not isinstance(value, str):
        msg = "Failed to resolve DataFusion type name."
        raise TypeError(msg)
    return value


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _register_temp_table(ctx: SessionContext, table: TableLike, *, prefix: str) -> str:
    name = f"__{prefix}_{uuid.uuid4().hex}"
    resolved = coerce_table_like(table)
    if isinstance(resolved, arrow_pa.RecordBatchReader):
        resolved_table = pa.Table.from_batches(list(resolved))
    else:
        resolved_table = cast("ArrowTable", resolved)
    batches = list(resolved_table.to_batches())
    ctx.register_record_batches(name, batches)
    return name


def _deregister_table(ctx: SessionContext, *, name: str) -> None:
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            deregister(name)


def _count_rows(ctx: SessionContext, table_name: str, *, where: str | None = None) -> int:
    clause = f" WHERE {where}" if where else ""
    table = ctx.sql(f"SELECT COUNT(*) AS count FROM {table_name}{clause}").to_arrow_table()
    value = table["count"][0].as_py() if table.num_rows else 0
    return int(value or 0)


def _align_for_validation(
    table: TableLike,
    *,
    spec: _TableSchemaSpec,
    options: ArrowValidationOptions,
) -> tuple[TableLike, AlignmentInfo]:
    keep_extra = options.strict is False
    safe_cast = not options.coerce
    on_error = "unsafe" if options.coerce else "raise"
    policy = schema_policy_factory(
        spec,
        ctx=execution_context_factory("default"),
        options=SchemaPolicyOptions(
            schema=spec.to_arrow_schema(),
            encoding=EncodingPolicy(dictionary_cols=frozenset()),
            safe_cast=safe_cast,
            keep_extra_columns=keep_extra,
            on_error=on_error,
        ),
    )
    return policy.apply_with_info(table)


def _cast_failure_count(
    ctx: SessionContext,
    *,
    table_name: str,
    column: str,
    dtype: DataTypeLike,
) -> int:
    dtype_name = _datafusion_type_name(dtype)
    col_name = _sql_identifier(column)
    sql = (
        "SELECT SUM(CASE WHEN "
        f"{col_name} IS NOT NULL AND arrow_cast({col_name}, '{dtype_name}') IS NULL "
        "THEN 1 ELSE 0 END) AS failures "
        f"FROM {table_name}"
    )
    try:
        table = ctx.sql(sql).to_arrow_table()
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"DataFusion cast check failed for column {column!r}: {exc}."
        raise ValueError(msg) from exc
    value = table["failures"][0].as_py() if table.num_rows else 0
    return int(value or 0)


def _duplicate_row_count(
    ctx: SessionContext,
    *,
    table_name: str,
    keys: Sequence[str],
) -> int:
    if not keys:
        return 0
    key_expr = ", ".join(_sql_identifier(name) for name in keys)
    sql = (
        "SELECT SUM(dupe_count) AS dupes FROM ("
        f"SELECT COUNT(*) AS dupe_count FROM {table_name} "
        f"GROUP BY {key_expr} HAVING COUNT(*) > 1)"
    )
    table = ctx.sql(sql).to_arrow_table()
    value = table["dupes"][0].as_py() if table.num_rows else 0
    return int(value or 0)


def _missing_column_errors(info: AlignmentInfo, row_count: int) -> list[_ValidationErrorEntry]:
    return [
        _ValidationErrorEntry("missing_column", name, row_count) for name in info["missing_cols"]
    ]


def _extra_column_errors(info: AlignmentInfo, row_count: int) -> list[_ValidationErrorEntry]:
    return [_ValidationErrorEntry("extra_column", name, row_count) for name in info["dropped_cols"]]


def _type_mismatch_errors(
    table: TableLike,
    *,
    spec: _TableSchemaSpec,
    row_count: int,
) -> list[_ValidationErrorEntry]:
    return [
        _ValidationErrorEntry("type_mismatch", field.name, row_count)
        for field in spec.fields
        if field.name in table.column_names and table.schema.field(field.name).type != field.dtype
    ]


def _coerce_type_mismatch_errors(
    ctx: SessionContext,
    *,
    table_name: str,
    table: TableLike,
    spec: _TableSchemaSpec,
) -> list[_ValidationErrorEntry]:
    entries: list[_ValidationErrorEntry] = []
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
        )
        if count:
            entries.append(_ValidationErrorEntry("type_mismatch", field.name, count))
    return entries


def _null_violation_results(
    ctx: SessionContext,
    *,
    table_name: str,
    required_fields: Sequence[str],
    missing_cols: Sequence[str],
) -> tuple[list[_ValidationErrorEntry], str | None]:
    required_present = [name for name in required_fields if name not in missing_cols]
    if not required_present:
        return [], None
    selections: list[str] = []
    alias_by_field: dict[str, str] = {}
    for idx, name in enumerate(required_present):
        alias = f"null_{idx}"
        alias_by_field[name] = alias
        col_name = _sql_identifier(name)
        selections.append(f"SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) AS {alias}")
    sql = f"SELECT {', '.join(selections)} FROM {table_name}"
    table = ctx.sql(sql).to_arrow_table()
    entries: list[_ValidationErrorEntry] = []
    for name, alias in alias_by_field.items():
        value = table[alias][0].as_py() if table.num_rows else 0
        count = int(value or 0)
        if count:
            entries.append(_ValidationErrorEntry("null_violation", name, count))
    invalid_expr = " OR ".join(f"{_sql_identifier(name)} IS NULL" for name in required_present)
    return entries, invalid_expr


def _row_filter_results(
    ctx: SessionContext,
    *,
    table_name: str,
    aligned: TableLike,
    invalid_expr: str | None,
    options: ArrowValidationOptions,
) -> tuple[TableLike, TableLike | None, int]:
    if invalid_expr is None:
        return aligned, None, 0
    invalid_rows = None
    if options.emit_invalid_rows or options.strict == "filter":
        invalid_rows = ctx.sql(f"SELECT * FROM {table_name} WHERE {invalid_expr}").to_arrow_table()
    invalid_row_count = _count_rows(ctx, table_name, where=invalid_expr)
    validated = aligned
    if options.strict == "filter":
        validated = ctx.sql(
            f"SELECT * FROM {table_name} WHERE NOT ({invalid_expr})"
        ).to_arrow_table()
    return validated, invalid_rows, invalid_row_count


def _key_field_errors(
    ctx: SessionContext,
    *,
    table_name: str,
    key_fields: Sequence[str],
    missing_cols: Sequence[str],
    row_count: int,
) -> list[_ValidationErrorEntry]:
    if not key_fields:
        return []
    missing_keys = missing_key_fields(key_fields, missing_cols=missing_cols)
    entries = [_ValidationErrorEntry("missing_key_field", key, row_count) for key in missing_keys]
    if not missing_keys:
        dup_count = _duplicate_row_count(ctx, table_name=table_name, keys=key_fields)
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


def _collect_validation_results(
    session: SessionContext,
    *,
    context: _ValidationContext,
    options: ArrowValidationOptions,
) -> tuple[list[_ValidationErrorEntry], TableLike, TableLike | None, int]:
    error_entries: list[_ValidationErrorEntry] = []
    error_entries.extend(_missing_column_errors(context.info, context.row_count))
    if options.strict is True:
        error_entries.extend(_extra_column_errors(context.info, context.row_count))
    if options.coerce:
        error_entries.extend(
            _coerce_type_mismatch_errors(
                session,
                table_name=context.original_name,
                table=context.original_table,
                spec=context.spec,
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
    )
    error_entries.extend(null_entries)
    validated, invalid_rows, invalid_row_count = _row_filter_results(
        session,
        table_name=context.aligned_name,
        aligned=context.aligned_table,
        invalid_expr=invalid_expr,
        options=options,
    )
    error_entries.extend(
        _key_field_errors(
            session,
            table_name=context.aligned_name,
            key_fields=context.spec.key_fields,
            missing_cols=context.info["missing_cols"],
            row_count=context.row_count,
        ),
    )
    return error_entries, validated, invalid_rows, invalid_row_count


def _prepare_validation_context(
    ctx: ExecutionContext,
    *,
    table: TableLike,
    spec: _TableSchemaSpec,
    options: ArrowValidationOptions,
) -> tuple[SessionContext, _ValidationContext, str, str]:
    session = _session_context(ctx)
    resolved = coerce_table_like(table)
    if isinstance(resolved, arrow_pa.RecordBatchReader):
        resolved_table = pa.Table.from_batches(list(resolved))
    else:
        resolved_table = cast("ArrowTable", resolved)
    row_count = int(resolved_table.num_rows)
    aligned, info = _align_for_validation(resolved_table, spec=spec, options=options)
    aligned_table = cast("ArrowTable", aligned)
    aligned_name = _register_temp_table(session, aligned_table, prefix="schema_validate")
    original_name = _register_temp_table(session, resolved_table, prefix="schema_validate_src")
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
    spec: _TableSchemaSpec,
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
    session, context, aligned_name, original_name = _prepare_validation_context(
        ctx,
        table=table,
        spec=spec,
        options=options,
    )
    try:
        error_entries, validated, invalid_rows, invalid_row_count = _collect_validation_results(
            session,
            context=context,
            options=options,
        )
    finally:
        _deregister_table(session, name=aligned_name)
        _deregister_table(session, name=original_name)

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
    "required_non_null_mask",
    "validate_table",
]
