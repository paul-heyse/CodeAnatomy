"""Invariant and error-artifact helpers for schema finalization."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol, cast

import pyarrow as pa
import pyarrow.types as patypes
from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.expr import Expr

from arrow_utils.core.array_iter import iter_array_values
from datafusion_engine.arrow.build import ColumnDefaultsSpec, ConstExpr
from datafusion_engine.arrow.coercion import to_arrow_table
from datafusion_engine.arrow.interop import (
    ArrayLike,
    DataTypeLike,
    SchemaLike,
    TableLike,
    empty_table_for_schema,
)
from datafusion_engine.expr.cast import safe_cast
from datafusion_engine.schema.introspection_core import SchemaIntrospector
from datafusion_engine.session.helpers import deregister_table, register_temp_table, temp_table
from datafusion_engine.udf.expr import udf_expr

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


class _ContractLike(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def required_non_null(self) -> tuple[str, ...]: ...

    @property
    def key_fields(self) -> tuple[str, ...]: ...

    @property
    def invariants(self) -> tuple[Callable[[TableLike], tuple[ArrayLike, str]], ...]: ...


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

        Returns:
            TableLike: Table with appended detail columns.
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

        Returns:
            TableLike: Error table containing rows that violated invariants.
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

        base_fields = list(table.schema)
        detail_fields = [pa.field(name, dtype) for name, dtype in self.detail_fields]
        empty_schema = pa.schema([*base_fields, *detail_fields])
        return empty_table_for_schema(empty_schema)

    @staticmethod
    def build_stats_table(errors: TableLike) -> TableLike:
        """Build per-error-code summary rows.

        Returns:
            TableLike: Summary rows grouped by `error_code`.
        """
        if errors.num_rows == 0:
            return pa.Table.from_arrays(
                [pa.array([], type=pa.string()), pa.array([], type=pa.int64())],
                names=["error_code", "count"],
            )
        return _error_code_counts_table(errors)


ERROR_ARTIFACT_SPEC = ErrorArtifactSpec(detail_fields=ERROR_DETAIL_FIELDS)


def _required_non_null_results(
    table: TableLike,
    cols: Sequence[str],
) -> tuple[list[InvariantResult], ArrayLike]:
    """Return invariant results and combined mask for required non-null checks."""
    if not cols:
        return [], pa.array([False] * table.num_rows, type=pa.bool_())
    resolved_table = to_arrow_table(table)
    masks: list[ArrayLike] = []
    for column_name in cols:
        if column_name in resolved_table.column_names:
            raw_mask = _compute_is_null(resolved_table[column_name])
            mask = _fill_null(raw_mask, fill_value=False)
        else:
            mask = pa.array([True] * resolved_table.num_rows, type=pa.bool_())
        masks.append(mask)
    combined = _combine_or(masks[0], masks[1]) if len(masks) > 1 else masks[0]
    for mask in masks[2:]:
        combined = _combine_or(combined, mask)
    results = [
        InvariantResult(
            mask=masks[idx],
            code="REQUIRED_NON_NULL",
            message=f"{column_name} is required.",
            column=column_name,
            severity="ERROR",
            source="required_non_null",
        )
        for idx, column_name in enumerate(cols)
    ]
    return results, combined


def collect_invariant_results(
    table: TableLike,
    contract: _ContractLike,
) -> tuple[list[InvariantResult], ArrayLike]:
    """Collect invariant results and the combined bad-row mask.

    Returns:
        tuple[list[InvariantResult], ArrayLike]: Individual invariant results and combined bad mask.
    """
    results, required_bad_any = _required_non_null_results(
        table,
        contract.required_non_null,
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
    bad_any = _combine_masks_df(masks, table.num_rows)
    return results, bad_any


def _combine_masks_df(
    masks: Sequence[ArrayLike],
    length: int,
) -> ArrayLike:
    if not masks:
        return pa.array([False] * length, type=pa.bool_())
    combined = _fill_null(masks[0], fill_value=False)
    for mask in masks[1:]:
        combined = _combine_or(combined, _fill_null(mask, fill_value=False))
    return combined


def filter_good_rows(
    table: TableLike,
    bad_any: ArrayLike,
) -> TableLike:
    """Filter rows that violated any invariant.

    Returns:
        TableLike: Rows that passed all invariant checks.
    """
    if table.num_rows == 0:
        return table
    resolved_table = to_arrow_table(table)
    filtered_mask = _invert_mask(_fill_null(bad_any, fill_value=False))
    return resolved_table.filter(filtered_mask)


def build_error_table(
    table: TableLike,
    results: Sequence[InvariantResult],
    *,
    error_spec: ErrorArtifactSpec,
) -> TableLike:
    """Build the error table for failed invariants.

    Returns:
        TableLike: Error rows with attached detail fields.
    """
    return error_spec.build_error_table(table, results)


def build_stats_table(errors: TableLike, *, error_spec: ErrorArtifactSpec) -> TableLike:
    """Build the error statistics table.

    Returns:
        TableLike: Summary table of error counts by code.
    """
    return error_spec.build_stats_table(errors)


def raise_on_errors_if_strict(
    errors: TableLike,
    *,
    mode: Literal["strict", "tolerant"],
    contract: _ContractLike,
) -> None:
    """Raise when strict mode receives one or more error rows.

    Raises:
        ValueError: If strict mode is enabled and one or more error rows exist.
    """
    if mode != "strict" or errors.num_rows == 0:
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
    table = to_arrow_table(errors)
    counts = _value_counts(table["error_code"])
    return pa.table(
        {
            "error_code": counts.field("values"),
            "count": counts.field("counts"),
        }
    )


def _error_detail_key_cols(contract: _ContractLike, schema: SchemaLike) -> list[str]:
    return list(contract.key_fields) if contract.key_fields else list(schema.names)


def _empty_error_details_table(
    *,
    contract: _ContractLike,
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
    return empty_table_for_schema(empty_schema)


_HASH_JOIN_SEPARATOR = "\x1f"
_HASH_NULL_SENTINEL = "__NULL__"


def _compute_is_null(values: ArrayLike) -> ArrayLike:
    is_null = getattr(values, "is_null", None)
    if callable(is_null):
        return cast("ArrayLike", is_null())
    return pa.array([value is None for value in iter_array_values(values)], type=pa.bool_())


def _combine_or(left: ArrayLike, right: ArrayLike) -> ArrayLike:
    combined = [
        bool(lval) or bool(rval)
        for lval, rval in zip(
            iter_array_values(left),
            iter_array_values(right),
            strict=True,
        )
    ]
    return pa.array(combined, type=pa.bool_())


def _invert_mask(values: ArrayLike) -> ArrayLike:
    inverted = [not bool(value) for value in iter_array_values(values)]
    return pa.array(inverted, type=pa.bool_())


def _value_counts(values: ArrayLike) -> ArrayLike:
    value_list = list(iter_array_values(values))
    counts = Counter(value_list)
    unique_values = list(counts.keys())
    count_values = [counts[value] for value in unique_values]
    value_type = values.type
    values_array = pa.array(unique_values, type=value_type)
    counts_array = pa.array(count_values, type=pa.int64())
    return pa.StructArray.from_arrays(
        [values_array, counts_array],
        fields=[
            pa.field("values", values_array.type),
            pa.field("counts", pa.int64()),
        ],
    )


def _fill_null(values: ArrayLike, *, fill_value: bool) -> ArrayLike:
    filled = [fill_value if value is None else bool(value) for value in iter_array_values(values)]
    return pa.array(filled, type=pa.bool_())


def _row_id_for_errors(
    errors: TableLike,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    contract: _ContractLike,
    key_cols: Sequence[str],
) -> ArrayLike:
    if key_cols:
        df_ctx = _datafusion_context(runtime_profile)
        if df_ctx is None:
            msg = "DataFusion SessionContext required to compute error row ids."
            raise TypeError(msg)
        resolved_table = to_arrow_table(errors)
        with temp_table(df_ctx, resolved_table, prefix="_finalize_errors_") as table_name:
            prefix = f"{contract.name}:row"
            df = df_ctx.table(table_name)
            parts = [lit(prefix)]
            for name in key_cols:
                if name in resolved_table.column_names:
                    expr = safe_cast(col(name), "Utf8")
                    expr = f.coalesce(expr, lit(_HASH_NULL_SENTINEL))
                else:
                    expr = lit(_HASH_NULL_SENTINEL)
                parts.append(expr)
            concat_expr = f.concat_ws(_HASH_JOIN_SEPARATOR, *parts)
            result = df.select(
                udf_expr("stable_hash64", concat_expr).alias("row_id")
            ).to_arrow_table()
        return result["row_id"]
    return pa.array(range(errors.num_rows), type=pa.int64())


def _aggregate_error_detail_lists(
    errors: TableLike,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    group_cols: Sequence[str],
    detail_field_names: Sequence[str],
) -> TableLike:
    group_table = errors.select(list(group_cols) + list(detail_field_names))
    df_ctx = _datafusion_context(runtime_profile)
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


def aggregate_error_details(
    errors: TableLike,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    contract: _ContractLike,
    schema: SchemaLike,
    provenance_cols: Sequence[str],
) -> TableLike:
    """Aggregate per-row error details into one ``error_detail`` list column.

    Returns:
        TableLike: Aggregated error rows keyed by row identity.
    """
    provenance = [col for col in provenance_cols if col in errors.column_names]
    if errors.num_rows == 0:
        return _empty_error_details_table(
            contract=contract,
            schema=schema,
            errors_schema=errors.schema,
            provenance=provenance,
        )
    key_cols = _error_detail_key_cols(contract, schema)
    row_id = _row_id_for_errors(
        errors, runtime_profile=runtime_profile, contract=contract, key_cols=key_cols
    )
    errors = errors.append_column("row_id", row_id)
    group_cols = ["row_id"] + [col for col in key_cols if col in errors.column_names] + provenance
    detail_field_names = [name for name, _ in ERROR_DETAIL_FIELDS]
    return _aggregate_error_detail_lists(
        errors,
        runtime_profile=runtime_profile,
        group_cols=group_cols,
        detail_field_names=detail_field_names,
    )


def _datafusion_context(runtime_profile: DataFusionRuntimeProfile | None) -> SessionContext | None:
    if runtime_profile is None:
        return None
    try:
        return runtime_profile.session_context()
    except (RuntimeError, TypeError, ValueError):
        return None


def _supports_error_detail_aggregation(ctx: SessionContext) -> bool:
    from datafusion_engine.sql.options import sql_options_for_profile

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
    resolved = to_arrow_table(table)
    table_name = register_temp_table(ctx, resolved, prefix="finalize_errors")
    try:
        df = ctx.table(table_name)
        exprs: list[Expr] = []
        for field in resolved.schema:
            name = field.name
            expr = col(name)
            if patypes.is_dictionary(field.type):
                dict_type = cast("pa.DictionaryType", field.type)
                expr = safe_cast(expr, dict_type.value_type)
            exprs.append(expr.alias(name))
        df = df.select(*exprs)
        struct_expr = f.named_struct([(name, col(name)) for name in detail_field_names])
        aggregated = df.aggregate(
            group_by=list(group_cols),
            aggs=[f.array_agg(struct_expr).alias("error_detail")],
        )
        return aggregated.to_arrow_table()
    finally:
        deregister_table(ctx, table_name)


__all__ = [
    "ERROR_ARTIFACT_SPEC",
    "ErrorArtifactSpec",
    "InvariantResult",
    "aggregate_error_details",
    "build_error_table",
    "build_stats_table",
    "collect_invariant_results",
    "filter_good_rows",
    "raise_on_errors_if_strict",
]
