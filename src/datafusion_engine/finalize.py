"""Finalize gate for schema alignment, invariants, and deterministic output."""

from __future__ import annotations

import contextlib
import importlib
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Protocol, cast

import pyarrow as pa
import pyarrow.types as patypes
from datafusion import SessionContext, SQLOptions, col
from datafusion import functions as f

from arrowdsl.core.array_iter import iter_array_values
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import (
    ArrayLike,
    DataTypeLike,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    coerce_table_like,
)
from arrowdsl.core.plan_ops import DedupeSpec, SortKey
from arrowdsl.core.schema_constants import PROVENANCE_COLS
from arrowdsl.schema.build import ColumnDefaultsSpec, ConstExpr
from arrowdsl.schema.chunking import ChunkPolicy
from arrowdsl.schema.encoding_policy import EncodingPolicy
from arrowdsl.schema.policy import SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import AlignmentInfo, SchemaMetadataSpec, align_table
from arrowdsl.schema.validation import ArrowValidationOptions
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.kernels import canonical_sort_if_canonical, dedupe_kernel
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.sql_options import sql_options_for_profile
from schema_spec.specs import TableSchemaSpec

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame

    from arrowdsl.schema.policy import SchemaPolicy


type InvariantFn = Callable[[TableLike], tuple[ArrayLike, str]]


class _ValidateArrowTable(Protocol):
    def __call__(
        self,
        table: TableLike,
        *,
        spec: TableSchemaSpec,
        options: ArrowValidationOptions,
    ) -> TableLike: ...


def _table_spec_from_schema(
    name: str,
    schema: SchemaLike,
    *,
    version: int | None = None,
) -> TableSchemaSpec:
    return TableSchemaSpec.from_schema(name, schema, version=version)


def _validate_arrow_table(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions,
) -> TableLike:
    module = importlib.import_module("schema_spec.system")
    validate_fn = cast("_ValidateArrowTable", module.validate_arrow_table)
    return validate_fn(table, spec=spec, options=options)


def _execute_sql_df(
    ctx: SessionContext,
    sql: str,
    *,
    sql_options: SQLOptions | None = None,
    allow_statements: bool = False,
) -> DataFrame:
    from sqlglot.errors import ParseError

    from datafusion_engine.compile_options import DataFusionCompileOptions, DataFusionSqlPolicy
    from datafusion_engine.execution_facade import DataFusionExecutionFacade
    from sqlglot_tools.optimizer import parse_sql_strict, register_datafusion_dialect

    resolved_sql_options = sql_options or sql_options_for_profile(None)
    if allow_statements:
        resolved_sql_options = resolved_sql_options.with_allow_statements(allow=True)

    def _sql_ingest(_payload: Mapping[str, object]) -> None:
        return None

    options = DataFusionCompileOptions(
        sql_options=resolved_sql_options,
        sql_policy=DataFusionSqlPolicy(allow_statements=allow_statements),
        sql_ingest_hook=_sql_ingest,
    )
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=None)
    try:
        register_datafusion_dialect()
        expr = parse_sql_strict(sql, dialect=options.dialect)
    except (ParseError, TypeError, ValueError) as exc:
        msg = "Finalize SQL parse failed."
        raise ValueError(msg) from exc
    plan = facade.compile(expr, options=options)
    result = facade.execute(plan)
    if result.dataframe is None:
        msg = "Finalize SQL execution did not return a DataFusion DataFrame."
        raise ValueError(msg)
    return result.dataframe


def _execute_sql_table(
    ctx: SessionContext,
    sql: str,
    *,
    sql_options: SQLOptions | None = None,
    allow_statements: bool = False,
) -> pa.Table:
    return _execute_sql_df(
        ctx,
        sql,
        sql_options=sql_options,
        allow_statements=allow_statements,
    ).to_arrow_table()


@dataclass(frozen=True)
class Contract:
    """Output contract: schema, invariants, and determinism policy."""

    name: str
    schema: SchemaLike
    schema_spec: TableSchemaSpec | None = None

    key_fields: tuple[str, ...] = ()
    required_non_null: tuple[str, ...] = ()
    invariants: tuple[InvariantFn, ...] = ()
    constraints: tuple[str, ...] = ()

    dedupe: DedupeSpec | None = None
    canonical_sort: tuple[SortKey, ...] = ()

    version: int | None = None

    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None
    validation: ArrowValidationOptions | None = None

    def with_versioned_schema(self) -> SchemaLike:
        """Return the schema with contract metadata attached.

        Returns
        -------
        SchemaLike
            Schema with contract metadata attached.
        """
        meta = {b"contract_name": str(self.name).encode("utf-8")}
        if self.version is not None:
            meta[b"contract_version"] = str(self.version).encode("utf-8")
        return SchemaMetadataSpec(schema_metadata=meta).apply(self.schema)

    def available_fields(self) -> tuple[str, ...]:
        """Return all visible field names (columns + virtual fields).

        Returns
        -------
        tuple[str, ...]
            Visible field names.
        """
        cols = tuple(self.schema.names)
        return cols + tuple(self.virtual_fields)


@dataclass(frozen=True)
class FinalizeResult:
    """Finalize output bundles.

    Parameters
    ----------
    good:
        Finalized table that passed invariants.
    errors:
        Error rows grouped by ``row_id`` with ``error_detail`` list<struct>.
    stats:
        Error statistics table.
    alignment:
        Schema alignment summary table.
    """

    good: TableLike
    errors: TableLike
    stats: TableLike
    alignment: TableLike


def _list_str_array(values: list[list[str]]) -> ArrayLike:
    """Build a list<str> array from nested string lists.

    Parameters
    ----------
    values:
        Nested string values to convert.

    Returns
    -------
    ArrayLike
        Array of list<string> values.
    """
    return pa.array(values, type=pa.list_(pa.string()))


def _provenance_columns(table: TableLike, schema: SchemaLike) -> list[str]:
    cols = [col for col in PROVENANCE_COLS if col in table.column_names]
    return [col for col in cols if col not in schema.names]


def _relax_nullable_schema(schema: SchemaLike) -> pa.Schema:
    fields = [
        pa.field(field.name, field.type, nullable=True, metadata=field.metadata) for field in schema
    ]
    return pa.schema(fields, metadata=schema.metadata)


def _relax_nullable_table(table: TableLike) -> TableLike:
    relaxed_schema = _relax_nullable_schema(table.schema)
    arrays = [table[name] for name in table.column_names]
    return pa.Table.from_arrays(arrays, schema=relaxed_schema)


@dataclass(frozen=True)
class InvariantResult:
    """Invariant evaluation with metadata for error details."""

    mask: ArrayLike
    code: str
    message: str | None
    column: str | None
    severity: Literal["ERROR", "WARNING", "INFO"]
    source: str


ERROR_DETAIL_FIELDS: tuple[tuple[str, DataTypeLike], ...] = (
    ("error_code", pa.string()),
    ("error_message", pa.string()),
    ("error_column", pa.string()),
    ("error_severity", pa.string()),
    ("error_task_name", pa.string()),
    ("error_task_priority", pa.int32()),
    ("error_source", pa.string()),
)

ERROR_DETAIL_STRUCT = pa.struct([(name, dtype) for name, dtype in ERROR_DETAIL_FIELDS])
ERROR_DETAIL_LIST_DTYPE = pa.list_(ERROR_DETAIL_STRUCT)


@dataclass(frozen=True)
class ErrorArtifactSpec:
    """Specification for error artifacts emitted by finalize."""

    detail_fields: tuple[tuple[str, DataTypeLike], ...]

    def append_detail_columns(self, table: TableLike, result: InvariantResult) -> TableLike:
        """Append error detail columns for a single invariant result.

        Returns
        -------
        TableLike
            Table with error detail columns appended.
        """
        message = result.message or result.code
        error_values: dict[str, object] = {
            "error_code": result.code,
            "error_message": message,
            "error_column": result.column,
            "error_severity": result.severity,
            "error_task_name": None,
            "error_task_priority": None,
            "error_source": result.source,
        }
        defaults = tuple(
            (name, ConstExpr(value=error_values.get(name), dtype=dtype))
            for name, dtype in self.detail_fields
        )
        return ColumnDefaultsSpec(defaults=defaults).apply(table)

    def build_error_table(
        self,
        table: TableLike,
        results: Sequence[InvariantResult],
    ) -> TableLike:
        """Build the error table for failed invariants.

        Returns
        -------
        TableLike
            Error rows with detail columns.
        """
        error_parts: list[TableLike] = []
        for result in results:
            bad_rows = table.filter(result.mask)
            if bad_rows.num_rows == 0:
                continue
            bad_rows = self.append_detail_columns(bad_rows, result)
            error_parts.append(bad_rows)

        if error_parts:
            return pa.concat_tables(error_parts, promote_options="default")

        empty_arrays = [pa.array([], type=schema_field.type) for schema_field in table.schema]
        empty_arrays.extend(pa.array([], type=field_type) for _, field_type in self.detail_fields)
        names = [*list(table.schema.names), *[name for name, _ in self.detail_fields]]
        return pa.Table.from_arrays(empty_arrays, names=names)

    @staticmethod
    def build_stats_table(errors: TableLike) -> TableLike:
        """Build the error statistics table.

        Returns
        -------
        TableLike
            Per-error-code counts.
        """
        if errors.num_rows == 0:
            return pa.Table.from_arrays(
                [pa.array([], type=pa.string()), pa.array([], type=pa.int64())],
                names=["error_code", "count"],
            )

        return _error_code_counts_table(errors)


ERROR_ARTIFACT_SPEC = ErrorArtifactSpec(detail_fields=ERROR_DETAIL_FIELDS)


@dataclass(frozen=True)
class FinalizeOptions:
    """Configuration overrides for finalize behavior."""

    error_spec: ErrorArtifactSpec = ERROR_ARTIFACT_SPEC
    schema_policy: SchemaPolicy | None = None
    chunk_policy: ChunkPolicy | None = None
    skip_canonical_sort: bool = False


def _required_non_null_results(
    table: TableLike,
    cols: Sequence[str],
    *,
    ctx: ExecutionContext,
) -> tuple[list[InvariantResult], ArrayLike]:
    """Return invariant results and combined mask for required non-null checks.

    Returns
    -------
    tuple[list[InvariantResult], ArrayLike]
        Invariant results and combined bad-row mask.

    Raises
    ------
    TypeError
        Raised when DataFusion is required but unavailable.
    """
    if not cols:
        return [], pa.array([False] * table.num_rows, type=pa.bool_())
    df_ctx = _datafusion_context(ctx)
    if df_ctx is None:
        msg = "DataFusion SessionContext required for required_non_null checks."
        raise TypeError(msg)
    table_name, resolved = _register_temp_table(df_ctx, table, prefix="finalize_required")
    try:
        mask_exprs: list[str] = []
        combined_parts: list[str] = []
        for column_name in cols:
            if column_name in resolved.column_names:
                expr = f"{_sql_identifier(column_name)} IS NULL"
            else:
                expr = "TRUE"
            mask_expr = f"COALESCE({expr}, FALSE)"
            mask_exprs.append(f"{mask_expr} AS {_sql_identifier(column_name)}")
            combined_parts.append(mask_expr)
        combined_expr = " OR ".join(combined_parts) if combined_parts else "FALSE"
        sql = (
            f"SELECT {', '.join(mask_exprs)}, {combined_expr} AS bad_any "
            f"FROM {_sql_identifier(table_name)}"
        )
        result = _execute_sql_table(
            df_ctx,
            sql,
            sql_options=sql_options_for_profile(None),
        )
    finally:
        deregister = getattr(df_ctx, "deregister_table", None)
        if callable(deregister):
            deregister(table_name)
    results = [
        InvariantResult(
            mask=result[column_name],
            code="REQUIRED_NON_NULL",
            message=f"{column_name} is required.",
            column=column_name,
            severity="ERROR",
            source="required_non_null",
        )
        for column_name in cols
    ]
    return results, result["bad_any"]


def _collect_invariant_results(
    table: TableLike,
    contract: Contract,
    *,
    ctx: ExecutionContext,
) -> tuple[list[InvariantResult], ArrayLike]:
    """Collect invariant results and the combined bad-row mask.

    Returns
    -------
    tuple[list[InvariantResult], ArrayLike]
        Invariant results and combined bad-row mask.

    Raises
    ------
    TypeError
        Raised when DataFusion is required but unavailable.
    """
    results, required_bad_any = _required_non_null_results(
        table,
        contract.required_non_null,
        ctx=ctx,
    )
    masks: list[ArrayLike] = [required_bad_any]
    for inv in contract.invariants:
        bad_mask, code = inv(table)
        results.append(
            InvariantResult(
                mask=bad_mask,
                code=code,
                message=code,
                column=None,
                severity="ERROR",
                source="invariant",
            )
        )
        masks.append(bad_mask)
    df_ctx = _datafusion_context(ctx)
    if df_ctx is None:
        msg = "DataFusion SessionContext required to combine invariant masks."
        raise TypeError(msg)
    bad_any = _combine_masks_df(df_ctx, masks, table.num_rows)
    return results, bad_any


def _combine_masks_df(
    ctx: SessionContext,
    masks: Sequence[ArrayLike],
    length: int,
) -> ArrayLike:
    if not masks:
        return pa.array([False] * length, type=pa.bool_())
    columns = {f"mask_{idx}": mask for idx, mask in enumerate(masks)}
    table = pa.Table.from_pydict(columns)
    table_name, _ = _register_temp_table(ctx, table, prefix="finalize_masks")
    try:
        parts = [f"COALESCE(mask_{idx}, FALSE)" for idx in range(len(masks))]
        combined_expr = " OR ".join(parts) if parts else "FALSE"
        sql = f"SELECT {combined_expr} AS bad_any FROM {_sql_identifier(table_name)}"
        result = _execute_sql_table(
            ctx,
            sql,
            sql_options=sql_options_for_profile(None),
        )
    finally:
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            deregister(table_name)
    return result["bad_any"]


def _filter_good_rows(
    table: TableLike,
    bad_any: ArrayLike,
    *,
    ctx: ExecutionContext,
) -> TableLike:
    if table.num_rows == 0:
        return table
    df_ctx = _datafusion_context(ctx)
    if df_ctx is None:
        msg = "DataFusion SessionContext required to filter invariant failures."
        raise TypeError(msg)
    masked = table.append_column("_bad_any", bad_any)
    table_name, resolved = _register_temp_table(df_ctx, masked, prefix="finalize_good")
    try:
        columns = [name for name in resolved.schema.names if name != "_bad_any"]
        select_cols = ", ".join(_sql_identifier(name) for name in columns)
        sql = f"SELECT {select_cols} FROM {_sql_identifier(table_name)} WHERE NOT _bad_any"
        return _execute_sql_table(
            df_ctx,
            sql,
            sql_options=sql_options_for_profile(None),
        )
    finally:
        deregister = getattr(df_ctx, "deregister_table", None)
        if callable(deregister):
            deregister(table_name)


def _build_error_table(
    table: TableLike,
    results: Sequence[InvariantResult],
    *,
    error_spec: ErrorArtifactSpec,
) -> TableLike:
    """Build the error table for failed invariants.

    Returns
    -------
    TableLike
        Error table with detail columns.
    """
    return error_spec.build_error_table(table, results)


def _build_stats_table(errors: TableLike, *, error_spec: ErrorArtifactSpec) -> TableLike:
    """Build the error statistics table.

    Returns
    -------
    TableLike
        Error statistics table.
    """
    return error_spec.build_stats_table(errors)


def _raise_on_errors_if_strict(
    errors: TableLike,
    *,
    ctx: ExecutionContext,
    contract: Contract,
) -> None:
    if ctx.mode != "strict" or errors.num_rows == 0:
        return
    counts = _error_code_counts_table(errors)
    pairs = [
        (value, count)
        for value, count in zip(
            iter_array_values(counts["error_code"]),
            iter_array_values(counts["count"]),
            strict=True,
        )
    ]
    msg = f"Finalize(strict) failed for contract={contract.name!r}: errors={pairs}"
    raise ValueError(msg)


def _error_code_counts_table(errors: TableLike) -> pa.Table:
    from datafusion_engine.arrow_ingest import datafusion_from_arrow
    from datafusion_engine.runtime import DataFusionRuntimeProfile

    ctx = DataFusionRuntimeProfile().ephemeral_context()
    datafusion_from_arrow(ctx, name="errors", value=errors)
    sql = "SELECT error_code, COUNT(*) AS count FROM errors GROUP BY error_code"
    table = _execute_sql_table(
        ctx,
        sql,
        sql_options=sql_options_for_profile(None),
    )
    return cast("pa.Table", table)


def _maybe_validate_with_arrow(
    table: TableLike,
    *,
    contract: Contract,
    ctx: ExecutionContext,
    schema_policy: SchemaPolicy | None = None,
) -> TableLike:
    if not ctx.schema_validation.enabled:
        return table
    if contract.schema_spec is None:
        return table
    options = None
    if schema_policy is not None and schema_policy.validation is not None:
        options = schema_policy.validation
    if options is None:
        options = contract.validation
    if options is None:
        options = ArrowValidationOptions.from_policy(ctx.schema_validation)
    return _validate_arrow_table(table, spec=contract.schema_spec, options=options)


def _error_detail_key_cols(contract: Contract, schema: SchemaLike) -> list[str]:
    return list(contract.key_fields) if contract.key_fields else list(schema.names)


def _empty_error_details_table(
    *,
    contract: Contract,
    schema: SchemaLike,
    errors_schema: SchemaLike,
    provenance: Sequence[str],
) -> TableLike:
    key_cols = _error_detail_key_cols(contract, schema)
    fields = [("row_id", pa.int64())]
    fields.extend((col, schema.field(col).type) for col in key_cols if col in schema.names)
    fields.extend((col, errors_schema.field(col).type) for col in provenance)
    fields.append(("error_detail", ERROR_DETAIL_LIST_DTYPE))
    empty_schema = pa.schema(fields)
    return pa.Table.from_arrays(
        [pa.array([], type=schema_field.type) for schema_field in empty_schema],
        schema=empty_schema,
    )


_HASH_JOIN_SEPARATOR = "\x1f"
_HASH_NULL_SENTINEL = "__NULL__"


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _coalesce_cast(name: str) -> str:
    return f"coalesce(CAST({_sql_identifier(name)} AS STRING), {_sql_literal(_HASH_NULL_SENTINEL)})"


def _row_id_sql(prefix: str, cols: Sequence[str]) -> str:
    parts = [_sql_literal(prefix)]
    parts.extend(_coalesce_cast(name) for name in cols)
    delimiter = _sql_literal(_HASH_JOIN_SEPARATOR)
    joined = f"concat_ws({delimiter}, {', '.join(parts)})"
    return f"stable_hash64({joined})"


def _row_id_for_errors(
    errors: TableLike,
    *,
    ctx: ExecutionContext,
    contract: Contract,
    key_cols: Sequence[str],
) -> ArrayLike:
    if key_cols:
        df_ctx = _datafusion_context(ctx)
        if df_ctx is None:
            msg = "DataFusion SessionContext required to compute error row ids."
            raise TypeError(msg)
        resolved = coerce_table_like(errors)
        if isinstance(resolved, pa.RecordBatchReader):
            reader = cast("RecordBatchReaderLike", resolved)
            resolved_table = pa.Table.from_batches(list(reader))
        else:
            resolved_table = cast("pa.Table", resolved)
        table_name = f"_finalize_errors_{uuid.uuid4().hex}"
        adapter = DataFusionIOAdapter(ctx=df_ctx, profile=None)
        adapter.register_record_batches(table_name, [resolved_table.to_batches()])
        try:
            prefix = f"{contract.name}:row"
            row_sql = _row_id_sql(prefix, key_cols)
            sql = f"SELECT {row_sql} AS row_id FROM {_sql_identifier(table_name)}"
            result = _execute_sql_table(
                df_ctx,
                sql,
                sql_options=sql_options_for_profile(None),
            )
        finally:
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table(table_name)
        return result["row_id"]
    return pa.array(range(errors.num_rows), type=pa.int64())


def _aggregate_error_detail_lists(
    errors: TableLike,
    *,
    ctx: ExecutionContext,
    group_cols: Sequence[str],
    detail_field_names: Sequence[str],
) -> TableLike:
    group_table = errors.select(list(group_cols) + list(detail_field_names))
    df_ctx = _datafusion_context(ctx)
    if df_ctx is None:
        msg = "DataFusion SessionContext required to aggregate error details."
        raise TypeError(msg)
    if not _supports_error_detail_aggregation(df_ctx):
        msg = "DataFusion array_agg/named_struct required for error aggregation."
        raise TypeError(msg)
    aggregated_table = _aggregate_error_detail_lists_df(
        df_ctx,
        group_table,
        group_cols=group_cols,
        detail_field_names=detail_field_names,
    )
    return cast("TableLike", aggregated_table)


def _aggregate_error_details(
    errors: TableLike,
    *,
    ctx: ExecutionContext,
    contract: Contract,
    schema: SchemaLike,
    provenance_cols: Sequence[str],
) -> TableLike:
    provenance = [col for col in provenance_cols if col in errors.column_names]
    if errors.num_rows == 0:
        return _empty_error_details_table(
            contract=contract,
            schema=schema,
            errors_schema=errors.schema,
            provenance=provenance,
        )
    key_cols = _error_detail_key_cols(contract, schema)
    row_id = _row_id_for_errors(errors, ctx=ctx, contract=contract, key_cols=key_cols)
    errors = errors.append_column("row_id", row_id)
    group_cols = ["row_id"] + [col for col in key_cols if col in errors.column_names] + provenance
    detail_field_names = [name for name, _ in ERROR_DETAIL_FIELDS]
    return _aggregate_error_detail_lists(
        errors,
        ctx=ctx,
        group_cols=group_cols,
        detail_field_names=detail_field_names,
    )


def _datafusion_context(ctx: ExecutionContext) -> SessionContext | None:
    profile = ctx.runtime.datafusion
    if profile is None:
        return None
    try:
        return profile.session_context()
    except (RuntimeError, TypeError, ValueError):
        return None


def _register_temp_table(
    ctx: SessionContext,
    table: TableLike,
    *,
    prefix: str,
) -> tuple[str, pa.Table]:
    resolved = coerce_table_like(table)
    if isinstance(resolved, pa.RecordBatchReader):
        reader = cast("RecordBatchReaderLike", resolved)
        resolved_table = pa.Table.from_batches(list(reader))
    else:
        resolved_table = cast("pa.Table", resolved)
    table_name = f"_{prefix}_{uuid.uuid4().hex}"
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_record_batches(table_name, [resolved_table.to_batches()])
    return table_name, resolved_table


def _arrow_type_name(ctx: SessionContext, dtype: pa.DataType) -> str:
    temp_name = f"_dtype_{uuid.uuid4().hex}"
    table = pa.table({"value": pa.array([None], type=dtype)})
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_record_batches(temp_name, [table.to_batches()])
    try:
        sql = f"SELECT arrow_typeof(value) AS dtype FROM {temp_name}"
        result = _execute_sql_table(
            ctx,
            sql,
            sql_options=sql_options_for_profile(None),
        )
        value = result["dtype"][0].as_py()
    finally:
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(temp_name)
    if not isinstance(value, str):
        msg = "Failed to resolve DataFusion type name."
        raise TypeError(msg)
    return value


def _supports_error_detail_aggregation(ctx: SessionContext) -> bool:
    from datafusion_engine.runtime import sql_options_for_profile

    try:
        names = SchemaIntrospector(ctx, sql_options=sql_options_for_profile(None)).function_names()
    except (RuntimeError, TypeError, ValueError):
        return False
    normalized = {name.lower() for name in names}
    return "array_agg" in normalized and "named_struct" in normalized


def _aggregate_error_detail_lists_df(
    ctx: SessionContext,
    table: pa.Table,
    *,
    group_cols: Sequence[str],
    detail_field_names: Sequence[str],
) -> pa.Table:
    table_name, resolved = _register_temp_table(ctx, table, prefix="finalize_errors")
    try:
        selections: list[str] = []
        for field in resolved.schema:
            name = field.name
            if patypes.is_dictionary(field.type):
                dict_type = cast("pa.DictionaryType", field.type)
                value_name = _arrow_type_name(ctx, dict_type.value_type)
                source = _sql_identifier(name)
                alias = _sql_identifier(name)
                selections.append(f"arrow_cast({source}, '{value_name}') AS {alias}")
            else:
                selections.append(_sql_identifier(name))
        select_sql = f"SELECT {', '.join(selections)} FROM {_sql_identifier(table_name)}"
        df = _execute_sql_df(
            ctx,
            select_sql,
            sql_options=sql_options_for_profile(None),
        )
        struct_expr = f.named_struct([(name, col(name)) for name in detail_field_names])
        aggregated = df.aggregate(
            group_by=list(group_cols),
            aggs=[f.array_agg(struct_expr).alias("error_detail")],
        )
        return aggregated.to_arrow_table()
    finally:
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            deregister(table_name)


def _build_alignment_table(
    contract: Contract,
    info: AlignmentInfo,
    *,
    good_rows: int,
    error_rows: int,
) -> TableLike:
    """Build the schema alignment summary table.

    Parameters
    ----------
    contract:
        Contract that produced the output.
    info:
        Alignment metadata.
    good_rows:
        Number of good rows.
    error_rows:
        Number of error rows.

    Returns
    -------
    pyarrow.Table
        Alignment summary table.
    """
    return pa.table(
        {
            "contract": [contract.name],
            "input_rows": [info["input_rows"]],
            "good_rows": [good_rows],
            "error_rows": [error_rows],
            "missing_cols": _list_str_array([list(info["missing_cols"])]),
            "dropped_cols": _list_str_array([list(info["dropped_cols"])]),
            "casted_cols": _list_str_array([list(info["casted_cols"])]),
        }
    )


def finalize(
    table: TableLike,
    *,
    contract: Contract,
    ctx: ExecutionContext,
    options: FinalizeOptions | None = None,
) -> FinalizeResult:
    """Finalize a table at the contract boundary.

    Parameters
    ----------
    table:
        Input table.
    contract:
        Output contract.
    ctx:
        Execution context for finalize behavior.
    options:
        Optional finalize options for error specs, transforms, and chunk policies.

    Returns
    -------
    FinalizeResult
        Finalized table bundle.
    """
    options = options or FinalizeOptions()
    schema_policy = _resolve_schema_policy(contract, ctx=ctx, schema_policy=options.schema_policy)
    schema = schema_policy.resolved_schema()
    aligned, align_info = schema_policy.apply_with_info(table)
    if schema_policy.encoding is None:
        aligned = (options.chunk_policy or ChunkPolicy()).apply(aligned)
    aligned = _maybe_validate_with_arrow(
        aligned,
        contract=contract,
        ctx=ctx,
        schema_policy=schema_policy,
    )
    provenance_cols: list[str] = _provenance_columns(aligned, schema) if ctx.provenance else []

    results, bad_any = _collect_invariant_results(aligned, contract, ctx=ctx)

    good = _filter_good_rows(aligned, bad_any, ctx=ctx)

    raw_errors = _build_error_table(aligned, results, error_spec=options.error_spec)
    errors = _aggregate_error_details(
        raw_errors,
        ctx=ctx,
        contract=contract,
        schema=schema,
        provenance_cols=provenance_cols,
    )
    errors = _relax_nullable_table(errors)

    _raise_on_errors_if_strict(raw_errors, ctx=ctx, contract=contract)

    if contract.dedupe is not None:
        good = dedupe_kernel(good, spec=contract.dedupe, _ctx=ctx)

    if not options.skip_canonical_sort:
        good = canonical_sort_if_canonical(good, sort_keys=contract.canonical_sort, ctx=ctx)

    keep_extras = bool(ctx.provenance and provenance_cols)
    good = align_table(
        good,
        schema=schema,
        safe_cast=schema_policy.safe_cast,
        keep_extra_columns=keep_extras,
        on_error=schema_policy.on_error,
    )

    if keep_extras:
        keep = list(schema.names) + [col for col in provenance_cols if col in good.column_names]
        good = good.select(keep)
    elif good.column_names != schema.names:
        good = good.select(schema.names)
    return FinalizeResult(
        good=good,
        errors=errors,
        stats=_build_stats_table(raw_errors, error_spec=options.error_spec),
        alignment=_build_alignment_table(
            contract, align_info, good_rows=good.num_rows, error_rows=errors.num_rows
        ),
    )


def normalize_only(
    table: TableLike,
    *,
    contract: Contract,
    ctx: ExecutionContext,
    options: FinalizeOptions | None = None,
) -> TableLike:
    """Normalize a table using finalize schema policy without invariants.

    Returns
    -------
    TableLike
        Normalized (aligned/encoded) table.
    """
    options = options or FinalizeOptions()
    schema_policy = _resolve_schema_policy(contract, ctx=ctx, schema_policy=options.schema_policy)
    schema = schema_policy.resolved_schema()
    aligned, _ = schema_policy.apply_with_info(table)
    if schema_policy.encoding is None:
        aligned = (options.chunk_policy or ChunkPolicy()).apply(aligned)
    if not schema_policy.keep_extra_columns and aligned.column_names != schema.names:
        aligned = aligned.select(schema.names)
    return aligned


def _resolve_schema_policy(
    contract: Contract,
    *,
    ctx: ExecutionContext,
    schema_policy: SchemaPolicy | None,
) -> SchemaPolicy:
    if schema_policy is not None:
        return schema_policy
    schema_spec = contract.schema_spec
    if schema_spec is None:
        schema_spec = _table_spec_from_schema(
            contract.name,
            contract.schema,
            version=contract.version,
        )
        policy_options = SchemaPolicyOptions(
            schema=contract.with_versioned_schema(),
            encoding=EncodingPolicy(dictionary_cols=frozenset()),
            validation=contract.validation,
        )
    else:
        policy_options = SchemaPolicyOptions(
            schema=contract.with_versioned_schema(),
            validation=contract.validation,
        )
    return schema_policy_factory(schema_spec, ctx=ctx, options=policy_options)


@dataclass(frozen=True)
class FinalizeContext:
    """Reusable finalize configuration for a contract."""

    contract: Contract
    error_spec: ErrorArtifactSpec = ERROR_ARTIFACT_SPEC
    schema_policy: SchemaPolicy | None = None
    chunk_policy: ChunkPolicy = field(default_factory=ChunkPolicy)
    skip_canonical_sort: bool = False

    def run(self, table: TableLike, ctx: ExecutionContext) -> FinalizeResult:
        """Finalize a table using the stored contract and context.

        Returns
        -------
        FinalizeResult
            Finalized table bundle.
        """
        options = FinalizeOptions(
            error_spec=self.error_spec,
            schema_policy=self.schema_policy,
            chunk_policy=self.chunk_policy,
            skip_canonical_sort=self.skip_canonical_sort,
        )
        return finalize(
            table,
            contract=self.contract,
            ctx=ctx,
            options=options,
        )


__all__ = [
    "ERROR_ARTIFACT_SPEC",
    "Contract",
    "ErrorArtifactSpec",
    "FinalizeContext",
    "FinalizeOptions",
    "FinalizeResult",
    "InvariantFn",
    "InvariantResult",
    "finalize",
    "normalize_only",
]
