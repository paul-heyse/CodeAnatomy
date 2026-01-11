"""Derive bytecode def/use events and reaching-def edges."""

from __future__ import annotations

from collections.abc import Iterator, Sequence

import pyarrow as pa
import pyarrow.compute as pc

from arrowdsl.columns import coalesce_string
from arrowdsl.empty import empty_table
from arrowdsl.ids import hash64_from_arrays

SCHEMA_VERSION = 1

DEF_USE_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("event_id", pa.string()),
        ("instr_id", pa.string()),
        ("code_unit_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("kind", pa.string()),
        ("symbol", pa.string()),
        ("opname", pa.string()),
        ("offset", pa.int32()),
    ]
)

REACHES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("edge_id", pa.string()),
        ("code_unit_id", pa.string()),
        ("def_event_id", pa.string()),
        ("use_event_id", pa.string()),
        ("symbol", pa.string()),
        ("path", pa.string()),
        ("file_id", pa.string()),
    ]
)

USE_PREFIXES = ("LOAD_",)
DEF_PREFIXES = ("STORE_", "DELETE_")
DEF_OPS = {"IMPORT_NAME", "IMPORT_FROM"}

type ArrayLike = pa.Array | pa.ChunkedArray


def _iter_array_values(array: ArrayLike) -> Iterator[object | None]:
    for value in array:
        if isinstance(value, pa.Scalar):
            yield value.as_py()
        else:
            yield value


def _iter_arrays(arrays: Sequence[ArrayLike]) -> Iterator[tuple[object | None, ...]]:
    iters = [_iter_array_values(array) for array in arrays]
    yield from zip(*iters, strict=True)


def _prefixed_hash64(prefix: str, arrays: list[ArrayLike]) -> ArrayLike:
    hashed = hash64_from_arrays(arrays, prefix=prefix)
    hashed_str = pc.cast(hashed, pa.string())
    return pc.binary_join_element_wise(pa.scalar(prefix), hashed_str, ":")


def _column_or_null(
    table: pa.Table,
    col: str,
    dtype: pa.DataType,
) -> pa.Array | pa.ChunkedArray:
    if col in table.column_names:
        return table[col]
    return pa.nulls(table.num_rows, type=dtype)


def build_def_use_events(py_bc_instructions: pa.Table) -> pa.Table:
    """Build def/use events from bytecode instruction rows.

    Parameters
    ----------
    py_bc_instructions:
        Bytecode instruction table with opname and argval data.

    Returns
    -------
    pa.Table
        Def/use events table.
    """
    if py_bc_instructions.num_rows == 0:
        return empty_table(DEF_USE_SCHEMA)
    if "opname" not in py_bc_instructions.column_names:
        return empty_table(DEF_USE_SCHEMA)

    table = py_bc_instructions
    symbol_cols = [col for col in ("argval_str", "argrepr") if col in table.column_names]
    if not symbol_cols:
        return empty_table(DEF_USE_SCHEMA)
    table = coalesce_string(table, symbol_cols, out_col="symbol")

    opname = pc.cast(table["opname"], pa.string())
    def_ops = pa.array(sorted(DEF_OPS), type=pa.string())
    is_def = pc.or_(
        pc.is_in(opname, value_set=def_ops),
        pc.or_(
            pc.starts_with(opname, DEF_PREFIXES[0]),
            pc.starts_with(opname, DEF_PREFIXES[1]),
        ),
    )
    is_use = pc.starts_with(opname, USE_PREFIXES[0])
    none_str = pa.scalar(None, type=pa.string())
    kind = pc.if_else(
        is_def,
        pa.scalar("def"),
        pc.if_else(is_use, pa.scalar("use"), none_str),
    )
    if "kind" in table.column_names:
        idx = table.schema.get_field_index("kind")
        table = table.set_column(idx, "kind", kind)
    else:
        table = table.append_column("kind", kind)

    mask = pc.and_(pc.is_valid(table["symbol"]), pc.is_valid(table["kind"]))
    mask = pc.fill_null(mask, replacement=False)
    table = table.filter(mask)
    if table.num_rows == 0:
        return empty_table(DEF_USE_SCHEMA)

    event_inputs = [
        _column_or_null(table, "code_unit_id", pa.string()),
        _column_or_null(table, "instr_id", pa.string()),
        table["kind"],
        table["symbol"],
    ]
    event_hash = hash64_from_arrays(event_inputs, prefix="df_event")
    event_hash_str = pc.cast(event_hash, pa.string())
    event_id = pc.binary_join_element_wise(pa.scalar("df_event"), event_hash_str, ":")

    return pa.Table.from_arrays(
        [
            pa.array([SCHEMA_VERSION] * table.num_rows, type=pa.int32()),
            event_id,
            _column_or_null(table, "instr_id", pa.string()),
            _column_or_null(table, "code_unit_id", pa.string()),
            _column_or_null(table, "file_id", pa.string()),
            _column_or_null(table, "path", pa.string()),
            table["kind"],
            table["symbol"],
            _column_or_null(table, "opname", pa.string()),
            _column_or_null(table, "offset", pa.int32()),
        ],
        schema=DEF_USE_SCHEMA,
    )


def run_reaching_defs(def_use_events: pa.Table) -> pa.Table:
    """Compute a best-effort reaching-defs edge table.

    This is a conservative, symbol-matching approximation that joins definitions to uses
    within the same code unit. It is deterministic and safe for early-stage analysis.

    Parameters
    ----------
    def_use_events:
        Def/use events table.

    Returns
    -------
    pa.Table
        Reaching-def edges table.
    """
    if def_use_events.num_rows == 0:
        return empty_table(REACHES_SCHEMA)

    defs_by_key, uses_by_key = _group_def_use_events(def_use_events)
    out_rows = _build_reaching_rows(defs_by_key, uses_by_key)

    if not out_rows:
        return empty_table(REACHES_SCHEMA)
    table = pa.Table.from_pylist(out_rows, schema=REACHES_SCHEMA)
    def_ids = _column_or_null(table, "def_event_id", pa.string())
    use_ids = _column_or_null(table, "use_event_id", pa.string())
    edge_prefixed = _prefixed_hash64("df_reach", [def_ids, use_ids])
    valid = pc.and_(pc.is_valid(def_ids), pc.is_valid(use_ids))
    edge_ids = pc.if_else(valid, edge_prefixed, pa.scalar(None, type=pa.string()))
    idx = table.schema.get_field_index("edge_id")
    return table.set_column(idx, "edge_id", edge_ids)


def _group_def_use_events(
    def_use_events: pa.Table,
) -> tuple[
    dict[tuple[str, str], list[dict[str, object]]], dict[tuple[str, str], list[dict[str, object]]]
]:
    defs_by_key: dict[tuple[str, str], list[dict[str, object]]] = {}
    uses_by_key: dict[tuple[str, str], list[dict[str, object]]] = {}
    arrays = [
        _column_or_null(def_use_events, "kind", pa.string()),
        _column_or_null(def_use_events, "code_unit_id", pa.string()),
        _column_or_null(def_use_events, "symbol", pa.string()),
        _column_or_null(def_use_events, "event_id", pa.string()),
        _column_or_null(def_use_events, "path", pa.string()),
        _column_or_null(def_use_events, "file_id", pa.string()),
    ]
    for kind, code_unit_id, symbol, event_id, path, file_id in _iter_arrays(arrays):
        if not isinstance(code_unit_id, str) or not isinstance(symbol, str):
            continue
        key = (code_unit_id, symbol)
        if kind == "def":
            defs_by_key.setdefault(key, []).append({"event_id": event_id})
        elif kind == "use":
            uses_by_key.setdefault(key, []).append(
                {"event_id": event_id, "path": path, "file_id": file_id}
            )
    return defs_by_key, uses_by_key


def _build_reaching_rows(
    defs_by_key: dict[tuple[str, str], list[dict[str, object]]],
    uses_by_key: dict[tuple[str, str], list[dict[str, object]]],
) -> list[dict[str, object]]:
    out_rows: list[dict[str, object]] = []
    for key in sorted(defs_by_key):
        defs = defs_by_key.get(key, [])
        uses = uses_by_key.get(key, [])
        if not defs or not uses:
            continue
        code_unit_id, symbol = key
        out_rows.extend(_reaching_rows_for_key(code_unit_id, symbol, defs, uses))
    return out_rows


def _reaching_rows_for_key(
    code_unit_id: str,
    symbol: str,
    defs: list[dict[str, object]],
    uses: list[dict[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for d in defs:
        def_id = d.get("event_id")
        if not isinstance(def_id, str):
            continue
        for u in uses:
            use_id = u.get("event_id")
            if not isinstance(use_id, str):
                continue
            rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "edge_id": None,
                    "code_unit_id": code_unit_id,
                    "def_event_id": def_id,
                    "use_event_id": use_id,
                    "symbol": symbol,
                    "path": u.get("path"),
                    "file_id": u.get("file_id"),
                }
            )
    return rows
