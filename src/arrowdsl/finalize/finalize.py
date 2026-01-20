"""Finalize gate for schema alignment, invariants, and deterministic output."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Protocol, cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.array_iter import iter_array_values
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.ids import HashSpec, hash_column_values
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    DataTypeLike,
    ListArrayLike,
    SchemaLike,
    TableLike,
)
from arrowdsl.core.plan_ops import DedupeSpec, SortKey
from arrowdsl.core.schema_constants import PROVENANCE_COLS
from arrowdsl.schema.build import (
    ColumnDefaultsSpec,
    ConstExpr,
    build_list,
    build_struct,
    const_array,
)
from arrowdsl.schema.chunking import ChunkPolicy
from arrowdsl.schema.encoding_policy import EncodingPolicy
from arrowdsl.schema.policy import SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import AlignmentInfo, SchemaMetadataSpec, align_table
from arrowdsl.schema.validation import ArrowValidationOptions
from datafusion_engine.compute_ops import (
    cast_values,
    cumulative_sum,
    dictionary_encode,
    fill_null,
    invert,
    is_null,
    list_flatten,
    list_value_length,
    or_,
    value_counts,
)
from datafusion_engine.kernels import canonical_sort_if_canonical, dedupe_kernel

if TYPE_CHECKING:
    from arrowdsl.schema.policy import SchemaPolicy
    from schema_spec.specs import TableSchemaSpec


type InvariantFn = Callable[[TableLike], tuple[ArrayLike, str]]


class _TableSpecFromSchema(Protocol):
    def __call__(
        self,
        name: str,
        schema: SchemaLike,
        *,
        version: int | None = None,
    ) -> TableSchemaSpec: ...


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
    module = importlib.import_module("schema_spec.system")
    table_spec_fn = cast("_TableSpecFromSchema", module.table_spec_from_schema)
    return table_spec_fn(name, schema, version=version)


def _validate_arrow_table(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions,
) -> TableLike:
    module = importlib.import_module("schema_spec.system")
    validate_fn = cast("_ValidateArrowTable", module.validate_arrow_table)
    return validate_fn(table, spec=spec, options=options)


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
    ("error_rule_name", pa.string()),
    ("error_rule_priority", pa.int32()),
    ("error_source", pa.string()),
)

ERROR_DETAIL_STRUCT = pa.struct([(name, dtype) for name, dtype in ERROR_DETAIL_FIELDS])
ERROR_DETAIL_LIST_DTYPE = pa.list_(ERROR_DETAIL_STRUCT)


def _build_error_detail_list(errors: TableLike) -> ArrayLike:
    detail_fields: dict[str, ArrayLike] = {}
    for name, _ in ERROR_DETAIL_FIELDS:
        column = errors[name]
        if isinstance(column, pa.ChunkedArray):
            column = column.combine_chunks()
        detail_fields[name] = column
    detail = build_struct(detail_fields, struct_type=ERROR_DETAIL_STRUCT)
    offsets = pa.array(range(errors.num_rows + 1), type=pa.int32())
    return build_list(offsets, detail)


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
            "error_rule_name": None,
            "error_rule_priority": None,
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

        vc = value_counts(errors["error_code"])
        return pa.Table.from_arrays(
            [vc.field("values"), vc.field("counts")],
            names=["error_code", "count"],
        )


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
) -> list[InvariantResult]:
    """Return invariant results for required non-null checks.

    Parameters
    ----------
    table:
        Input table.
    cols:
        Columns that must be non-null.

    Returns
    -------
    list[InvariantResult]
        Invariant results for required non-null checks.
    """
    results: list[InvariantResult] = []
    for col in cols:
        mask = fill_null(is_null(table[col]), fill_value=False)
        results.append(
            InvariantResult(
                mask=mask,
                code="REQUIRED_NON_NULL",
                message=f"{col} is required.",
                column=col,
                severity="ERROR",
                source="required_non_null",
            )
        )
    return results


def _collect_invariant_results(
    table: TableLike,
    contract: Contract,
) -> list[InvariantResult]:
    """Collect invariant results and error metadata.

    Parameters
    ----------
    table:
        Input table.
    contract:
        Contract containing invariants.

    Returns
    -------
    list[InvariantResult]
        Invariant results.
    """
    results: list[InvariantResult] = []
    results.extend(_required_non_null_results(table, contract.required_non_null))
    for inv in contract.invariants:
        bad_mask, code = inv(table)
        results.append(
            InvariantResult(
                mask=fill_null(bad_mask, fill_value=False),
                code=code,
                message=code,
                column=None,
                severity="ERROR",
                source="invariant",
            )
        )
    return results


def _combine_masks(masks: Sequence[ArrayLike], length: int) -> ArrayLike:
    """Combine multiple masks into a single mask.

    Parameters
    ----------
    masks:
        Sequence of boolean masks.
    length:
        Length of the output mask.

    Returns
    -------
    pyarrow.Array
        Combined boolean mask.
    """
    if not masks:
        return const_array(length, value=False, dtype=pa.bool_())
    combined = masks[0]
    for mask in masks[1:]:
        combined = or_(combined, mask)
    return fill_null(combined, fill_value=False)


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
    vc = value_counts(errors["error_code"])
    pairs = [
        (value, count)
        for value, count in zip(
            iter_array_values(vc.field("values")),
            iter_array_values(vc.field("counts")),
            strict=True,
        )
    ]
    msg = f"Finalize(strict) failed for contract={contract.name!r}: errors={pairs}"
    raise ValueError(msg)


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


def _row_id_for_errors(
    errors: TableLike,
    *,
    contract: Contract,
    key_cols: Sequence[str],
) -> ArrayLike:
    if key_cols:
        spec = HashSpec(
            prefix=f"{contract.name}:row",
            cols=tuple(key_cols),
            as_string=False,
            missing="null",
        )
        return hash_column_values(errors, spec=spec)
    return pa.array(range(errors.num_rows), type=pa.int64())


def _aggregate_error_detail_lists(
    errors: TableLike,
    *,
    group_cols: Sequence[str],
    detail_field_names: Sequence[str],
) -> TableLike:
    aggregates = [(name, "list") for name in detail_field_names]
    group_table = errors.select(list(group_cols) + list(detail_field_names))
    group_table, dict_cols = _decode_dictionary_columns(group_table)
    aggregated = group_table.group_by(list(group_cols), use_threads=True).aggregate(aggregates)
    list_columns = {name: aggregated[f"{name}_list"] for name in detail_field_names}
    error_detail = _build_error_detail_from_lists(list_columns)
    list_cols = [f"{name}_list" for name in detail_field_names]
    aggregated = aggregated.append_column("error_detail", error_detail).drop(list_cols)
    if dict_cols:
        aggregated = _reencode_dictionary_columns(aggregated, dict_cols)
    return aggregated


def _aggregate_error_details(
    errors: TableLike,
    *,
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
    row_id = _row_id_for_errors(errors, contract=contract, key_cols=key_cols)
    errors = errors.append_column("row_id", row_id)
    group_cols = ["row_id"] + [col for col in key_cols if col in errors.column_names] + provenance
    detail_field_names = [name for name, _ in ERROR_DETAIL_FIELDS]
    return _aggregate_error_detail_lists(
        errors,
        group_cols=group_cols,
        detail_field_names=detail_field_names,
    )


def _build_error_detail_from_lists(
    list_columns: dict[str, ArrayLike | ChunkedArrayLike],
) -> ArrayLike:
    first = _combine_list_array(next(iter(list_columns.values())))
    offsets = _list_offsets(first)
    struct_fields: dict[str, ArrayLike] = {}
    for name, list_col in list_columns.items():
        list_array = _combine_list_array(list_col)
        values = list_flatten(list_array)
        struct_fields[name] = values
    detail_struct = build_struct(struct_fields, struct_type=ERROR_DETAIL_STRUCT)
    return _build_list_from_offsets(offsets, detail_struct, template=first)


def _build_list_from_offsets(
    offsets: ArrayLike | ChunkedArrayLike,
    values: ArrayLike | ChunkedArrayLike,
    *,
    template: ListArrayLike,
) -> ListArrayLike:
    if isinstance(template, pa.LargeListArray):
        return pa.LargeListArray.from_arrays(offsets, values)
    return build_list(offsets, values)


def _list_offsets(list_array: ListArrayLike) -> ArrayLike:
    if isinstance(list_array, pa.ListArray):
        return cast("pa.ListArray", list_array).offsets
    if isinstance(list_array, pa.LargeListArray):
        return cast("pa.LargeListArray", list_array).offsets
    msg = "Unsupported list array type for error detail aggregation."
    raise TypeError(msg)


def _is_list_like_type(dtype: DataTypeLike) -> bool:
    return bool(
        patypes.is_list(dtype)
        or patypes.is_large_list(dtype)
        or patypes.is_list_view(dtype)
        or patypes.is_large_list_view(dtype)
        or patypes.is_fixed_size_list(dtype)
    )


def _list_view_to_list_array(
    values: pa.ListViewArray | pa.LargeListViewArray,
) -> ListArrayLike:
    lengths = list_value_length(values)
    lengths = fill_null(lengths, fill_value=0)
    if isinstance(values, pa.LargeListViewArray):
        if lengths.type != pa.int64():
            lengths = cast_values(lengths, pa.int64())
    elif lengths.type != pa.int32():
        lengths = cast_values(lengths, pa.int32())
    offsets = cumulative_sum(lengths)
    zero = pa.array([0], type=offsets.type)
    offsets = pa.concat_arrays([zero, offsets])
    flat = list_flatten(values)
    mask = is_null(values)
    if isinstance(values, pa.LargeListViewArray):
        return cast("ListArrayLike", pa.LargeListArray.from_arrays(offsets, flat, mask=mask))
    return cast("ListArrayLike", pa.ListArray.from_arrays(offsets, flat, mask=mask))


def _combine_list_array(values: ArrayLike | ChunkedArrayLike) -> ListArrayLike:
    combined = values.combine_chunks() if isinstance(values, ChunkedArrayLike) else values
    if isinstance(combined, pa.ListArray):
        return cast("ListArrayLike", combined)
    if isinstance(combined, pa.LargeListArray):
        return cast("ListArrayLike", combined)
    if isinstance(combined, pa.ListViewArray):
        return _list_view_to_list_array(combined)
    if isinstance(combined, pa.LargeListViewArray):
        return _list_view_to_list_array(combined)
    combined_type = getattr(combined, "type", None)
    if combined_type is not None and _is_list_like_type(combined_type):
        value_type = getattr(combined_type, "value_type", None)
        if value_type is not None:
            list_type = (
                pa.large_list(cast("pa.DataType", value_type))
                if patypes.is_large_list(combined_type) or patypes.is_large_list_view(combined_type)
                else pa.list_(cast("pa.DataType", value_type))
            )
            casted = cast_values(combined, list_type)
            if isinstance(casted, (pa.ListArray, pa.LargeListArray)):
                return cast("ListArrayLike", casted)
    msg = "Expected list array for error detail aggregation."
    raise TypeError(msg)


def _decode_dictionary_columns(
    table: TableLike,
) -> tuple[pa.Table, dict[str, pa.DictionaryType]]:
    dict_cols: dict[str, pa.DictionaryType] = {}
    columns: list[ChunkedArrayLike] = []
    names: list[str] = []
    for schema_field in table.schema:
        col = table[schema_field.name]
        if patypes.is_dictionary(schema_field.type):
            dict_type = cast("pa.DictionaryType", schema_field.type)
            dict_cols[schema_field.name] = dict_type
            col = cast("ChunkedArrayLike", cast_values(col, dict_type.value_type))
        columns.append(col)
        names.append(schema_field.name)
    if not dict_cols:
        return cast("pa.Table", table), {}
    return pa.table(columns, names=names), dict_cols


def _reencode_dictionary_columns(
    table: pa.Table,
    dict_cols: dict[str, pa.DictionaryType],
) -> pa.Table:
    if not dict_cols:
        return table
    columns: list[ChunkedArrayLike] = []
    names: list[str] = []
    for name in table.schema.names:
        col = table[name]
        dict_type = dict_cols.get(name)
        if dict_type is not None:
            encoded = dictionary_encode(col)
            col = cast("ChunkedArrayLike", cast_values(encoded, dict_type))
        columns.append(col)
        names.append(name)
    return pa.table(columns, names=names)


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
    schema_policy = options.schema_policy
    if schema_policy is None:
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
        schema_policy = schema_policy_factory(schema_spec, ctx=ctx, options=policy_options)
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

    results = _collect_invariant_results(aligned, contract)
    bad_any = _combine_masks([result.mask for result in results], aligned.num_rows)

    good = aligned.filter(invert(bad_any))

    raw_errors = _build_error_table(aligned, results, error_spec=options.error_spec)
    errors = _aggregate_error_details(
        raw_errors,
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
]
