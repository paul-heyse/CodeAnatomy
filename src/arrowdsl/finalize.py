"""Finalize gate for schema alignment, invariants, and deterministic output."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import arrowdsl.pyarrow_core as pa
from arrowdsl.column_ops import ColumnDefaultsSpec, ConstExpr, const_array
from arrowdsl.compute import pc
from arrowdsl.contracts import Contract
from arrowdsl.id_specs import HashSpec
from arrowdsl.ids import hash_column_values
from arrowdsl.iter import iter_array_values
from arrowdsl.kernels import apply_dedupe, canonical_sort_if_canonical
from arrowdsl.nested import build_list_array, build_struct_array
from arrowdsl.pyarrow_protocols import ArrayLike, DataTypeLike, SchemaLike, TableLike
from arrowdsl.runtime import ExecutionContext
from arrowdsl.schema import AlignmentInfo
from arrowdsl.schema_ops import SchemaTransform
from schema_spec.pandera_adapter import PanderaValidationOptions, validate_arrow_table


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


def _build_error_table(table: TableLike, results: Sequence[InvariantResult]) -> TableLike:
    """Build the error table for failed invariants.

    Returns
    -------
    TableLike
        Error table with detail columns.
    """
    return ERROR_ARTIFACT_SPEC.build_error_table(table, results)


def _build_stats_table(errors: TableLike) -> TableLike:
    """Build the error statistics table.

    Returns
    -------
    TableLike
        Error statistics table.
    """
    return ERROR_ARTIFACT_SPEC.build_stats_table(errors)


def _maybe_validate_with_pandera(
    table: TableLike,
    *,
    contract: Contract,
    ctx: ExecutionContext,
) -> TableLike:
    policy = ctx.schema_validation
    if not policy.enabled:
        return table
    if contract.schema_spec is None:
        return table
    return validate_arrow_table(
        table,
        spec=contract.schema_spec,
        options=PanderaValidationOptions(
            lazy=policy.lazy,
            strict=policy.strict,
            coerce=policy.coerce,
        ),
    )


def _aggregate_error_details(
    errors: TableLike,
    *,
    contract: Contract,
    schema: SchemaLike,
) -> TableLike:
    if errors.num_rows == 0:
        detail_struct = pa.struct(
            [
                ("code", pa.string()),
                ("message", pa.string()),
                ("column", pa.string()),
                ("severity", pa.string()),
                ("rule_name", pa.string()),
                ("rule_priority", pa.int32()),
                ("source", pa.string()),
            ]
        )
        fields = [("row_id", pa.int64())]
        key_cols = list(contract.key_fields) if contract.key_fields else list(schema.names)
        fields.extend((col, schema.field(col).type) for col in key_cols if col in schema.names)
        fields.append(("error_detail", pa.list_(detail_struct)))
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
    detail = build_struct_array(
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
    detail_list = build_list_array(offsets, detail)
    errors = errors.append_column("error_detail_list", detail_list)
    group_cols = ["row_id"] + [col for col in key_cols if col in errors.column_names]
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


def finalize(table: TableLike, *, contract: Contract, ctx: ExecutionContext) -> FinalizeResult:
    """Finalize a table at the contract boundary.

    Parameters
    ----------
    table:
        Input table.
    contract:
        Output contract.
    ctx:
        Execution context for finalize behavior.

    Returns
    -------
    FinalizeResult
        Finalized table bundle.

    Raises
    ------
    ValueError
        Raised when ``ctx.mode`` is ``"strict"`` and errors are present.
    """
    schema = contract.with_versioned_schema()
    transform = SchemaTransform(
        schema=schema,
        safe_cast=ctx.safe_cast,
        on_error="unsafe" if ctx.safe_cast else "raise",
    )
    aligned, align_info = transform.apply_with_info(table)
    aligned = _maybe_validate_with_pandera(aligned, contract=contract, ctx=ctx)

    results = _collect_invariant_results(aligned, contract)
    bad_any = _combine_masks([result.mask for result in results], aligned.num_rows)

    good_mask = pc.invert(bad_any)
    good = aligned.filter(good_mask)

    raw_errors = _build_error_table(aligned, results)
    errors = _aggregate_error_details(raw_errors, contract=contract, schema=schema)

    if ctx.mode == "strict" and raw_errors.num_rows > 0:
        vc = pc.value_counts(raw_errors["error_code"])
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

    if contract.dedupe is not None:
        good = apply_dedupe(good, spec=contract.dedupe, _ctx=ctx)

    good = canonical_sort_if_canonical(good, sort_keys=contract.canonical_sort, ctx=ctx)

    if good.column_names != schema.names:
        good = good.select(schema.names)

    stats = _build_stats_table(raw_errors)
    alignment = _build_alignment_table(
        contract, align_info, good_rows=good.num_rows, error_rows=errors.num_rows
    )

    return FinalizeResult(good=good, errors=errors, stats=stats, alignment=alignment)
