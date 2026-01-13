"""Finalize gate for schema alignment, invariants, and deterministic output."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

from arrowdsl.compute.kernels import ChunkPolicy, apply_dedupe, canonical_sort_if_canonical
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import HashSpec, hash_column_values, iter_array_values
from arrowdsl.core.interop import ArrayLike, DataTypeLike, SchemaLike, TableLike, pc
from arrowdsl.plan.ops import DedupeSpec, SortKey
from arrowdsl.schema.build import (
    ColumnDefaultsSpec,
    ConstExpr,
    build_list,
    build_struct,
    const_array,
)
from arrowdsl.schema.policy import SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import AlignmentInfo, EncodingPolicy, SchemaMetadataSpec
from arrowdsl.schema.validation import ArrowValidationOptions
from schema_spec.specs import PROVENANCE_COLS, NestedFieldSpec
from schema_spec.system import table_spec_from_schema, validate_arrow_table

if TYPE_CHECKING:
    from arrowdsl.schema.policy import SchemaPolicy
    from schema_spec.specs import TableSchemaSpec


type InvariantFn = Callable[[TableLike], tuple[ArrayLike, str]]


@dataclass(frozen=True)
class Contract:
    """Output contract: schema, invariants, and determinism policy."""

    name: str
    schema: SchemaLike
    schema_spec: TableSchemaSpec | None = None

    key_fields: tuple[str, ...] = ()
    required_non_null: tuple[str, ...] = ()
    invariants: tuple[InvariantFn, ...] = ()

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


def _build_error_detail_list(errors: TableLike) -> ArrayLike:
    detail = build_struct(
        {
            "code": errors["error_code"],
            "message": errors["error_message"],
            "column": errors["error_column"],
            "severity": errors["error_severity"],
            "rule_name": errors["error_rule_name"],
            "rule_priority": errors["error_rule_priority"],
            "source": errors["error_source"],
        }
    )
    offsets = pa.array(range(errors.num_rows + 1), type=pa.int32())
    return build_list(offsets, detail)


ERROR_DETAIL_SPEC = NestedFieldSpec(
    name="error_detail",
    dtype=pa.list_(ERROR_DETAIL_STRUCT),
    builder=_build_error_detail_list,
)


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
            return pa.concat_tables(error_parts, promote=True)

        empty_arrays = [pa.array([], type=field.type) for field in table.schema]
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

        vc = pc.value_counts(errors["error_code"])
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
        mask = pc.fill_null(pc.is_null(table[col]), fill_value=False)
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
                mask=pc.fill_null(bad_mask, fill_value=False),
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
        combined = pc.or_(combined, mask)
    return pc.fill_null(combined, fill_value=False)


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
    vc = pc.value_counts(errors["error_code"])
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
    return validate_arrow_table(table, spec=contract.schema_spec, options=options)


def _aggregate_error_details(
    errors: TableLike,
    *,
    contract: Contract,
    schema: SchemaLike,
    provenance_cols: Sequence[str],
) -> TableLike:
    provenance = [col for col in provenance_cols if col in errors.column_names]
    if errors.num_rows == 0:
        fields = [("row_id", pa.int64())]
        key_cols = list(contract.key_fields) if contract.key_fields else list(schema.names)
        fields.extend((col, schema.field(col).type) for col in key_cols if col in schema.names)
        fields.extend((col, errors.schema.field(col).type) for col in provenance)
        fields.append(("error_detail", ERROR_DETAIL_SPEC.dtype))
        empty_schema = pa.schema(fields)
        return pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in empty_schema],
            schema=empty_schema,
        )

    key_cols = list(contract.key_fields) if contract.key_fields else list(schema.names)
    if key_cols:
        spec = HashSpec(
            prefix=f"{contract.name}:row",
            cols=tuple(key_cols),
            as_string=False,
            missing="null",
        )
        row_id = hash_column_values(errors, spec=spec)
    else:
        row_id = pa.array(range(errors.num_rows), type=pa.int64())
    errors = errors.append_column("row_id", row_id)
    detail_list = ERROR_DETAIL_SPEC.builder(errors)
    errors = errors.append_column("error_detail_list", detail_list)
    group_cols = ["row_id"] + [col for col in key_cols if col in errors.column_names] + provenance
    aggregated = errors.group_by(group_cols, use_threads=True).aggregate(
        [("error_detail_list", "list")]
    )
    list_col = "error_detail_list_list"
    if list_col not in aggregated.column_names:
        list_col = "error_detail_list"
    flattened = pc.list_flatten(aggregated[list_col])
    return aggregated.append_column("error_detail", flattened).drop([list_col])


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
            schema_spec = table_spec_from_schema(
                contract.name,
                contract.schema,
                version=contract.version,
            )
            policy_options = SchemaPolicyOptions(
                schema=contract.with_versioned_schema(),
                encoding=EncodingPolicy(specs=()),
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
    provenance_cols = _provenance_columns(aligned, schema) if ctx.provenance else []

    results = _collect_invariant_results(aligned, contract)
    bad_any = _combine_masks([result.mask for result in results], aligned.num_rows)

    good = aligned.filter(pc.invert(bad_any))

    raw_errors = _build_error_table(aligned, results, error_spec=options.error_spec)
    errors = _aggregate_error_details(
        raw_errors,
        contract=contract,
        schema=schema,
        provenance_cols=provenance_cols,
    )

    _raise_on_errors_if_strict(raw_errors, ctx=ctx, contract=contract)

    if contract.dedupe is not None:
        good = apply_dedupe(good, spec=contract.dedupe, _ctx=ctx)

    if not options.skip_canonical_sort:
        good = canonical_sort_if_canonical(good, sort_keys=contract.canonical_sort, ctx=ctx)

    if ctx.provenance and provenance_cols:
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
