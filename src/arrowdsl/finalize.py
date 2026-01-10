from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple, TYPE_CHECKING

from .contracts import Contract
from .kernels import apply_dedupe, canonical_sort_if_canonical
from .runtime import ExecutionContext

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow as pa


@dataclass(frozen=True)
class FinalizeResult:
    good: "pa.Table"
    errors: "pa.Table"
    stats: "pa.Table"
    alignment: "pa.Table"


def _list_str_array(xss: List[List[str]]) -> "pa.Array":
    import pyarrow as pa

    return pa.array(xss, type=pa.list_(pa.string()))


def _align_to_schema(table: "pa.Table", *, schema: "pa.Schema", safe_cast: bool) -> Tuple["pa.Table", Dict[str, object]]:
    """Align/cast/reorder table to match schema."""
    import pyarrow as pa
    import pyarrow.compute as pc

    info: Dict[str, object] = {}
    info["input_cols"] = list(table.column_names)
    info["input_rows"] = int(table.num_rows)

    target_names = [f.name for f in schema]
    missing = [name for name in target_names if name not in table.column_names]
    extra = [name for name in table.column_names if name not in target_names]
    casted: List[str] = []

    arrays: List[pa.Array | pa.ChunkedArray] = []
    for field in schema:
        if field.name in table.column_names:
            col = table[field.name]
            if col.type != field.type:
                try:
                    col = pc.cast(col, field.type, safe=safe_cast)
                except Exception:
                    if safe_cast:
                        col = pc.cast(col, field.type, safe=False)
                    else:
                        raise
                casted.append(field.name)
            arrays.append(col)
        else:
            arrays.append(pa.nulls(table.num_rows, type=field.type))

    aligned = pa.Table.from_arrays(arrays, schema=schema)
    info["missing_cols"] = missing
    info["dropped_cols"] = extra
    info["casted_cols"] = casted
    info["output_rows"] = int(aligned.num_rows)
    return aligned, info


def _required_non_null_mask(table: "pa.Table", cols: Sequence[str]) -> Optional["pa.Array"]:
    """Return a per-row boolean mask for required non-null violations."""
    import pyarrow.compute as pc

    if not cols:
        return None
    bad: Optional["pa.Array"] = None
    for c in cols:
        m = pc.is_null(table[c])
        bad = m if bad is None else (bad | m)
    assert bad is not None
    return pc.fill_null(bad, False)


def finalize(table: "pa.Table", *, contract: Contract, ctx: ExecutionContext) -> FinalizeResult:
    """Single correctness boundary: schema alignment + invariants + error tables + determinism."""
    import pyarrow as pa
    import pyarrow.compute as pc

    schema = contract.with_versioned_schema()
    aligned, align_info = _align_to_schema(table, schema=schema, safe_cast=ctx.safe_cast)

    error_parts: List[pa.Table] = []
    masks: List[Tuple[pa.Array, str]] = []

    nn_mask = _required_non_null_mask(aligned, contract.required_non_null)
    if nn_mask is not None:
        masks.append((nn_mask, "REQUIRED_NON_NULL"))

    for inv in contract.invariants:
        bad_mask, code = inv(aligned)
        bad_mask = pc.fill_null(bad_mask, False)
        masks.append((bad_mask, code))

    if masks:
        bad_any = masks[0][0]
        for m, _ in masks[1:]:
            bad_any = bad_any | m
        bad_any = pc.fill_null(bad_any, False)
    else:
        bad_any = pa.array([False] * aligned.num_rows, type=pa.bool_())

    good_mask = pc.invert(bad_any)
    good = aligned.filter(good_mask)

    for m, code in masks:
        bad_rows = aligned.filter(m)
        if bad_rows.num_rows == 0:
            continue
        bad_rows = bad_rows.append_column("error_code", pa.array([code] * bad_rows.num_rows, type=pa.string()))
        error_parts.append(bad_rows)

    if error_parts:
        errors = pa.concat_tables(error_parts, promote=True)
    else:
        errors = pa.Table.from_arrays(
            [pa.array([], type=f.type) for f in aligned.schema] + [pa.array([], type=pa.string())],
            names=list(aligned.schema.names) + ["error_code"],
        )

    if ctx.mode == "strict" and errors.num_rows > 0:
        vc = pc.value_counts(errors["error_code"])
        values = vc.field("values")
        counts = vc.field("counts")
        pairs = list(zip(values.to_pylist(), counts.to_pylist()))
        raise ValueError(f"Finalize(strict) failed for contract={contract.name!r}: errors={pairs}")

    if contract.dedupe is not None:
        good = apply_dedupe(good, spec=contract.dedupe, ctx=ctx)

    good = canonical_sort_if_canonical(good, sort_keys=contract.canonical_sort, ctx=ctx)

    if good.column_names != schema.names:
        good = good.select(schema.names)

    if errors.num_rows:
        vc = pc.value_counts(errors["error_code"])
        stats = pa.Table.from_arrays([vc.field("values"), vc.field("counts")], names=["error_code", "count"])
    else:
        stats = pa.Table.from_arrays(
            [pa.array([], type=pa.string()), pa.array([], type=pa.int64())], names=["error_code", "count"]
        )

    alignment = pa.table(
        {
            "contract": [contract.name],
            "input_rows": [align_info.get("input_rows", 0)],
            "good_rows": [good.num_rows],
            "error_rows": [errors.num_rows],
            "missing_cols": _list_str_array([list(align_info.get("missing_cols", []))]),
            "dropped_cols": _list_str_array([list(align_info.get("dropped_cols", []))]),
            "casted_cols": _list_str_array([list(align_info.get("casted_cols", []))]),
        }
    )

    return FinalizeResult(good=good, errors=errors, stats=stats, alignment=alignment)
