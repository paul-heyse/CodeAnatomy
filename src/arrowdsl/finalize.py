"""Finalize gate for schema alignment, invariants, and deterministic output."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc

from arrowdsl.contracts import Contract
from arrowdsl.ids import hash64_from_columns
from arrowdsl.iter import iter_array_values
from arrowdsl.kernels import apply_dedupe, canonical_sort_if_canonical
from arrowdsl.runtime import ExecutionContext
from arrowdsl.schema import AlignmentInfo, align_to_schema
from schema_spec.pandera_adapter import validate_arrow_table


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

    good: pa.Table
    errors: pa.Table
    stats: pa.Table
    alignment: pa.Table


def _list_str_array(values: list[list[str]]) -> pa.Array:
    return pa.array(values, type=pa.list_(pa.string()))


@dataclass(frozen=True)
class InvariantResult:
    """Invariant evaluation with metadata for error details."""

    mask: pa.Array
    code: str
    message: str | None
    column: str | None
    severity: Literal["ERROR", "WARNING", "INFO"]
    source: str


ERROR_DETAIL_FIELDS: tuple[tuple[str, pa.DataType], ...] = (
    ("error_code", pa.string()),
    ("error_message", pa.string()),
    ("error_column", pa.string()),
    ("error_severity", pa.string()),
    ("error_rule_name", pa.string()),
    ("error_rule_priority", pa.int32()),
    ("error_source", pa.string()),
)


def _required_non_null_results(
    table: pa.Table,
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
        mask = pc.fill_null(pc.is_null(table[col]), replacement=False)
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
    table: pa.Table,
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
                mask=pc.fill_null(bad_mask, replacement=False),
                code=code,
                message=code,
                column=None,
                severity="ERROR",
                source="invariant",
            )
        )
    return results


def _combine_masks(masks: Sequence[pa.Array], length: int) -> pa.Array:
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
        return pa.array([False] * length, type=pa.bool_())
    combined = masks[0]
    for mask in masks[1:]:
        combined = pc.or_(combined, mask)
    return pc.fill_null(combined, replacement=False)


def _build_error_table(table: pa.Table, results: Sequence[InvariantResult]) -> pa.Table:
    """Build the error table for failed invariants.

    Parameters
    ----------
    table:
        Input table.
    results:
        Invariant results with metadata.

    Returns
    -------
    pyarrow.Table
        Error table with detail columns.
    """
    error_parts: list[pa.Table] = []
    for result in results:
        bad_rows = table.filter(result.mask)
        if bad_rows.num_rows == 0:
            continue
        bad_rows = _append_error_detail_columns(bad_rows, result)
        error_parts.append(bad_rows)

    if error_parts:
        return pa.concat_tables(error_parts, promote=True)

    empty_arrays = [pa.array([], type=field.type) for field in table.schema]
    empty_arrays.extend(pa.array([], type=field_type) for _, field_type in ERROR_DETAIL_FIELDS)
    names = [*list(table.schema.names), *[name for name, _ in ERROR_DETAIL_FIELDS]]
    return pa.Table.from_arrays(empty_arrays, names=names)


def _append_error_detail_columns(table: pa.Table, result: InvariantResult) -> pa.Table:
    n = table.num_rows
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
    for name, dtype in ERROR_DETAIL_FIELDS:
        value = error_values.get(name)
        table = table.append_column(name, pa.array([value] * n, type=dtype))
    return table


def _build_stats_table(errors: pa.Table) -> pa.Table:
    """Build the error statistics table.

    Parameters
    ----------
    errors:
        Error table containing ``error_code`` column.

    Returns
    -------
    pyarrow.Table
        Error statistics table.
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


def _maybe_validate_with_pandera(
    table: pa.Table,
    *,
    contract: Contract,
    ctx: ExecutionContext,
) -> pa.Table:
    policy = ctx.schema_validation
    if not policy.enabled:
        return table
    if contract.schema_spec is None:
        return table
    return validate_arrow_table(
        table,
        spec=contract.schema_spec,
        lazy=policy.lazy,
        strict=policy.strict,
        coerce=policy.coerce,
    )


def _aggregate_error_details(
    errors: pa.Table,
    *,
    contract: Contract,
    schema: pa.Schema,
) -> pa.Table:
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
        row_id = hash64_from_columns(
            errors,
            cols=key_cols,
            prefix=f"{contract.name}:row",
            missing="null",
        )
    else:
        row_id = pa.array(range(errors.num_rows), type=pa.int64())
    errors = errors.append_column("row_id", row_id)
    detail = pa.StructArray.from_arrays(
        [
            errors["error_code"],
            errors["error_message"],
            errors["error_column"],
            errors["error_severity"],
            errors["error_rule_name"],
            errors["error_rule_priority"],
            errors["error_source"],
        ],
        names=["code", "message", "column", "severity", "rule_name", "rule_priority", "source"],
    )
    errors = errors.append_column("error_detail", detail)
    group_cols = ["row_id"] + [col for col in key_cols if col in errors.column_names]
    aggregated = errors.group_by(group_cols).aggregate([("error_detail", "list")])
    if "error_detail_list" in aggregated.column_names:
        aggregated = aggregated.rename_columns(
            [
                "error_detail" if name == "error_detail_list" else name
                for name in aggregated.column_names
            ]
        )
    return aggregated


def _build_alignment_table(
    contract: Contract,
    info: AlignmentInfo,
    *,
    good_rows: int,
    error_rows: int,
) -> pa.Table:
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


def finalize(table: pa.Table, *, contract: Contract, ctx: ExecutionContext) -> FinalizeResult:
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
    on_error: Literal["unsafe", "raise"] = "unsafe" if ctx.safe_cast else "raise"
    aligned, align_info = align_to_schema(
        table,
        schema=schema,
        safe_cast=ctx.safe_cast,
        on_error=on_error,
    )
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
