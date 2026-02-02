"""Finalize gate for schema alignment, invariants, and deterministic output."""

from __future__ import annotations

import importlib
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Protocol, cast

import pyarrow as pa
import pyarrow.types as patypes
from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.expr import Expr

from arrow_utils.core.array_iter import iter_array_values
from arrow_utils.core.schema_constants import PROVENANCE_COLS
from core_types import DeterminismTier
from datafusion_engine.arrow.build import ColumnDefaultsSpec, ConstExpr
from datafusion_engine.arrow.chunking import ChunkPolicy
from datafusion_engine.arrow.coercion import to_arrow_table
from datafusion_engine.arrow.encoding import EncodingPolicy
from datafusion_engine.arrow.interop import ArrayLike, DataTypeLike, SchemaLike, TableLike
from datafusion_engine.arrow.metadata import SchemaMetadataSpec
from datafusion_engine.expr.cast import safe_cast
from datafusion_engine.kernels import (
    DedupeSpec,
    SortKey,
    canonical_sort_if_canonical,
    dedupe_kernel,
)
from datafusion_engine.schema.alignment import AlignmentInfo, align_table
from datafusion_engine.schema.introspection import SchemaIntrospector
from datafusion_engine.schema.policy import SchemaPolicyOptions, schema_policy_factory
from datafusion_engine.schema.validation import ArrowValidationOptions
from datafusion_engine.session.helpers import deregister_table, register_temp_table, temp_table
from datafusion_engine.udf.shims import stable_hash64
from schema_spec.specs import TableSchemaSpec

if TYPE_CHECKING:
    from datafusion_engine.schema.policy import SchemaPolicy
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


type InvariantFn = Callable[[TableLike], tuple[ArrayLike, str]]


class _ValidateArrowTable(Protocol):
    def __call__(
        self,
        table: TableLike,
        *,
        spec: TableSchemaSpec,
        options: ArrowValidationOptions,
        runtime_profile: DataFusionRuntimeProfile | None,
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
    runtime_profile: DataFusionRuntimeProfile | None,
) -> TableLike:
    module = importlib.import_module("schema_spec.system")
    validate_fn = cast("_ValidateArrowTable", module.validate_arrow_table)
    return validate_fn(table, spec=spec, options=options, runtime_profile=runtime_profile)


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
    runtime_profile: DataFusionRuntimeProfile | None = None
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT
    mode: Literal["strict", "tolerant"] = "tolerant"
    provenance: bool = False
    schema_validation: ArrowValidationOptions | None = None


def _required_non_null_results(
    table: TableLike,
    cols: Sequence[str],
) -> tuple[list[InvariantResult], ArrayLike]:
    """Return invariant results and combined mask for required non-null checks.

    Returns
    -------
    tuple[list[InvariantResult], ArrayLike]
        Invariant results and combined bad-row mask.
    """
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


def _collect_invariant_results(
    table: TableLike,
    contract: Contract,
) -> tuple[list[InvariantResult], ArrayLike]:
    """Collect invariant results and the combined bad-row mask.

    Returns
    -------
    tuple[list[InvariantResult], ArrayLike]
        Invariant results and combined bad-row mask.
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


def _filter_good_rows(
    table: TableLike,
    bad_any: ArrayLike,
) -> TableLike:
    if table.num_rows == 0:
        return table
    resolved_table = to_arrow_table(table)
    filtered_mask = _invert_mask(_fill_null(bad_any, fill_value=False))
    return resolved_table.filter(filtered_mask)


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
    mode: Literal["strict", "tolerant"],
    contract: Contract,
) -> None:
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


def _maybe_validate_with_arrow(
    table: TableLike,
    *,
    contract: Contract,
    schema_policy: SchemaPolicy | None = None,
    schema_validation: ArrowValidationOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> TableLike:
    if contract.schema_spec is None:
        return table
    options = None
    if schema_policy is not None and schema_policy.validation is not None:
        options = schema_policy.validation
    if options is None:
        options = contract.validation
    if options is None:
        options = schema_validation
    if options is None:
        return table
    return _validate_arrow_table(
        table,
        spec=contract.schema_spec,
        options=options,
        runtime_profile=runtime_profile,
    )


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
    contract: Contract,
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
            result = df.select(stable_hash64(concat_expr).alias("row_id")).to_arrow_table()
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


def _aggregate_error_details(
    errors: TableLike,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
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
    from datafusion_engine.session.runtime import sql_options_for_profile

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
    options: FinalizeOptions | None = None,
) -> FinalizeResult:
    """Finalize a table at the contract boundary.

    Parameters
    ----------
    table:
        Input table.
    contract:
        Output contract.
    options:
        Optional finalize options for error specs, transforms, and chunk policies.

    Returns
    -------
    FinalizeResult
        Finalized table bundle.
    """
    options = options or FinalizeOptions()
    schema_policy = _resolve_schema_policy(contract, schema_policy=options.schema_policy)
    schema = schema_policy.resolved_schema()
    aligned, align_info = schema_policy.apply_with_info(table)
    if schema_policy.encoding is None:
        aligned = (options.chunk_policy or ChunkPolicy()).apply(aligned)
    aligned = _maybe_validate_with_arrow(
        aligned,
        contract=contract,
        schema_policy=schema_policy,
        schema_validation=options.schema_validation,
        runtime_profile=options.runtime_profile,
    )
    provenance_cols: list[str] = _provenance_columns(aligned, schema) if options.provenance else []

    results, bad_any = _collect_invariant_results(aligned, contract)

    good = _filter_good_rows(aligned, bad_any)

    raw_errors = _build_error_table(aligned, results, error_spec=options.error_spec)
    errors = _aggregate_error_details(
        raw_errors,
        runtime_profile=options.runtime_profile,
        contract=contract,
        schema=schema,
        provenance_cols=provenance_cols,
    )
    errors = _relax_nullable_table(errors)

    _raise_on_errors_if_strict(raw_errors, mode=options.mode, contract=contract)

    if contract.dedupe is not None:
        good = dedupe_kernel(
            good,
            spec=contract.dedupe,
            runtime_profile=options.runtime_profile,
        )

    if not options.skip_canonical_sort:
        good = canonical_sort_if_canonical(
            good,
            sort_keys=contract.canonical_sort,
            determinism_tier=options.determinism_tier,
        )

    keep_extras = bool(options.provenance and provenance_cols)
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
    options: FinalizeOptions | None = None,
) -> TableLike:
    """Normalize a table using finalize schema policy without invariants.

    Returns
    -------
    TableLike
        Normalized (aligned/encoded) table.
    """
    options = options or FinalizeOptions()
    schema_policy = _resolve_schema_policy(contract, schema_policy=options.schema_policy)
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
    return schema_policy_factory(schema_spec, options=policy_options)


@dataclass(frozen=True)
class FinalizeContext:
    """Reusable finalize configuration for a contract."""

    contract: Contract
    error_spec: ErrorArtifactSpec = ERROR_ARTIFACT_SPEC
    schema_policy: SchemaPolicy | None = None
    chunk_policy: ChunkPolicy = field(default_factory=ChunkPolicy)
    skip_canonical_sort: bool = False

    def run(
        self,
        table: TableLike,
        *,
        request: FinalizeRunRequest,
    ) -> FinalizeResult:
        """Finalize a table using the stored contract and options.

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
            runtime_profile=request.runtime_profile,
            determinism_tier=request.determinism_tier,
            mode=request.mode,
            provenance=request.provenance,
            schema_validation=request.schema_validation,
        )
        return finalize(
            table,
            contract=self.contract,
            options=options,
        )


@dataclass(frozen=True)
class FinalizeRunRequest:
    """Inputs required to run a finalize pass."""

    runtime_profile: DataFusionRuntimeProfile | None
    determinism_tier: DeterminismTier
    mode: Literal["strict", "tolerant"] = "tolerant"
    provenance: bool = False
    schema_validation: ArrowValidationOptions | None = None


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
