"""Finalize gate for schema alignment, invariants, and deterministic output."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TypedDict

import pyarrow as pa
import pyarrow.compute as pc

from arrowdsl.contracts import Contract
from arrowdsl.kernels import apply_dedupe, canonical_sort_if_canonical
from arrowdsl.runtime import ExecutionContext


class AlignmentInfo(TypedDict):
    """Alignment metadata for schema casting and column selection."""

    input_cols: list[str]
    input_rows: int
    missing_cols: list[str]
    dropped_cols: list[str]
    casted_cols: list[str]
    output_rows: int


@dataclass(frozen=True)
class FinalizeResult:
    """Finalize output bundles.

    Parameters
    ----------
    good:
        Finalized table that passed invariants.
    errors:
        Error rows with an ``error_code`` column.
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


def _align_to_schema(
    table: pa.Table, *, schema: pa.Schema, safe_cast: bool
) -> tuple[pa.Table, AlignmentInfo]:
    """Align and cast a table to a target schema.

    Parameters
    ----------
    table:
        Input table.
    schema:
        Target schema.
    safe_cast:
        When ``True``, allow safe casts only.

    Returns
    -------
    tuple[pyarrow.Table, AlignmentInfo]
        Aligned table and alignment metadata.

    Raises
    ------
    pyarrow.ArrowInvalid
        Raised when casting fails and safe_cast is disabled.
    pyarrow.ArrowTypeError
        Raised when casting fails and safe_cast is disabled.
    """
    info: AlignmentInfo = {
        "input_cols": list(table.column_names),
        "input_rows": int(table.num_rows),
        "missing_cols": [],
        "dropped_cols": [],
        "casted_cols": [],
        "output_rows": 0,
    }

    target_names = [field.name for field in schema]
    missing = [name for name in target_names if name not in table.column_names]
    extra = [name for name in table.column_names if name not in target_names]

    arrays: list[pa.Array | pa.ChunkedArray] = []
    for field in schema:
        if field.name in table.column_names:
            col = table[field.name]
            if col.type != field.type:
                try:
                    col = pc.cast(col, field.type, safe=safe_cast)
                except (pa.ArrowInvalid, pa.ArrowTypeError):
                    if safe_cast:
                        col = pc.cast(col, field.type, safe=False)
                    else:
                        raise
                info["casted_cols"].append(field.name)
            arrays.append(col)
        else:
            arrays.append(pa.nulls(table.num_rows, type=field.type))

    aligned = pa.Table.from_arrays(arrays, schema=schema)
    info["missing_cols"] = missing
    info["dropped_cols"] = extra
    info["output_rows"] = int(aligned.num_rows)
    return aligned, info


def _required_non_null_mask(table: pa.Table, cols: Sequence[str]) -> pa.Array | None:
    """Return a mask for required non-null violations.

    Parameters
    ----------
    table:
        Input table.
    cols:
        Columns that must be non-null.

    Returns
    -------
    pyarrow.Array | None
        Boolean mask where ``True`` indicates a violation.
    """
    if not cols:
        return None
    bad: pa.Array | None = None
    for col in cols:
        mask = pc.is_null(table[col])
        bad = mask if bad is None else pc.or_(bad, mask)
    if bad is None:
        return None
    return pc.fill_null(bad, replacement=False)


def _collect_masks(table: pa.Table, contract: Contract) -> list[tuple[pa.Array, str]]:
    """Collect invariant masks and error codes.

    Parameters
    ----------
    table:
        Input table.
    contract:
        Contract containing invariants.

    Returns
    -------
    list[tuple[pyarrow.Array, str]]
        Mask/code pairs.
    """
    masks: list[tuple[pa.Array, str]] = []
    nn_mask = _required_non_null_mask(table, contract.required_non_null)
    if nn_mask is not None:
        masks.append((nn_mask, "REQUIRED_NON_NULL"))
    for inv in contract.invariants:
        bad_mask, code = inv(table)
        masks.append((pc.fill_null(bad_mask, replacement=False), code))
    return masks


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


def _build_error_table(table: pa.Table, masks: Sequence[tuple[pa.Array, str]]) -> pa.Table:
    """Build the error table for failed invariants.

    Parameters
    ----------
    table:
        Input table.
    masks:
        Mask/code pairs.

    Returns
    -------
    pyarrow.Table
        Error table with ``error_code`` column.
    """
    error_parts: list[pa.Table] = []
    for mask, code in masks:
        bad_rows = table.filter(mask)
        if bad_rows.num_rows == 0:
            continue
        bad_rows = bad_rows.append_column(
            "error_code",
            pa.array([code] * bad_rows.num_rows, type=pa.string()),
        )
        error_parts.append(bad_rows)

    if error_parts:
        return pa.concat_tables(error_parts, promote=True)

    empty_arrays = [pa.array([], type=field.type) for field in table.schema]
    empty_arrays.append(pa.array([], type=pa.string()))
    return pa.Table.from_arrays(empty_arrays, names=[*list(table.schema.names), "error_code"])


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
    aligned, align_info = _align_to_schema(table, schema=schema, safe_cast=ctx.safe_cast)

    masks = _collect_masks(aligned, contract)
    bad_any = _combine_masks([mask for mask, _ in masks], aligned.num_rows)

    good_mask = pc.invert(bad_any)
    good = aligned.filter(good_mask)

    errors = _build_error_table(aligned, masks)

    if ctx.mode == "strict" and errors.num_rows > 0:
        vc = pc.value_counts(errors["error_code"])
        pairs = list(
            zip(vc.field("values").to_pylist(), vc.field("counts").to_pylist(), strict=True)
        )
        msg = f"Finalize(strict) failed for contract={contract.name!r}: errors={pairs}"
        raise ValueError(msg)

    if contract.dedupe is not None:
        good = apply_dedupe(good, spec=contract.dedupe, _ctx=ctx)

    good = canonical_sort_if_canonical(good, sort_keys=contract.canonical_sort, ctx=ctx)

    if good.column_names != schema.names:
        good = good.select(schema.names)

    stats = _build_stats_table(errors)
    alignment = _build_alignment_table(
        contract, align_info, good_rows=good.num_rows, error_rows=errors.num_rows
    )

    return FinalizeResult(good=good, errors=errors, stats=stats, alignment=alignment)
