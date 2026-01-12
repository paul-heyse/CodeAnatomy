"""Derive bytecode def/use events and reaching-def edges."""

from __future__ import annotations

import arrowdsl.pyarrow_core as pa
from arrowdsl.column_ops import set_or_append_column
from arrowdsl.columns import coalesce_string
from arrowdsl.compute import pc
from arrowdsl.empty import empty_table
from arrowdsl.ids import prefixed_hash_id
from arrowdsl.iter import iter_arrays
from arrowdsl.predicates import And, FilterSpec, IsNull, Not
from arrowdsl.pyarrow_protocols import ArrayLike, ChunkedArrayLike, DataTypeLike, TableLike
from schema_spec.core import ArrowFieldSpec
from schema_spec.factories import make_table_spec
from schema_spec.fields import file_identity_bundle

SCHEMA_VERSION = 1

DEF_USE_SPEC = make_table_spec(
    name="py_bc_def_use_events_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(include_sha256=False),),
    fields=[
        ArrowFieldSpec(name="event_id", dtype=pa.string()),
        ArrowFieldSpec(name="instr_id", dtype=pa.string()),
        ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
        ArrowFieldSpec(name="kind", dtype=pa.string()),
        ArrowFieldSpec(name="symbol", dtype=pa.string()),
        ArrowFieldSpec(name="opname", dtype=pa.string()),
        ArrowFieldSpec(name="offset", dtype=pa.int32()),
    ],
)

REACHES_SPEC = make_table_spec(
    name="py_bc_reaches_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(include_sha256=False),),
    fields=[
        ArrowFieldSpec(name="edge_id", dtype=pa.string()),
        ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
        ArrowFieldSpec(name="def_event_id", dtype=pa.string()),
        ArrowFieldSpec(name="use_event_id", dtype=pa.string()),
        ArrowFieldSpec(name="symbol", dtype=pa.string()),
    ],
)

DEF_USE_SCHEMA = DEF_USE_SPEC.to_arrow_schema()
REACHES_SCHEMA = REACHES_SPEC.to_arrow_schema()

USE_PREFIXES = ("LOAD_",)
DEF_PREFIXES = ("STORE_", "DELETE_")
DEF_OPS = {"IMPORT_NAME", "IMPORT_FROM"}


def _prefixed_hash64(prefix: str, arrays: list[ArrayLike]) -> ArrayLike:
    return prefixed_hash_id(arrays, prefix=prefix)


def _column_or_null(
    table: TableLike,
    col: str,
    dtype: DataTypeLike,
) -> ArrayLike | ChunkedArrayLike:
    if col in table.column_names:
        return table[col]
    return pa.nulls(table.num_rows, type=dtype)


def build_def_use_events(py_bc_instructions: TableLike) -> TableLike:
    """Build def/use events from bytecode instruction rows.

    Parameters
    ----------
    py_bc_instructions:
        Bytecode instruction table with opname and argval data.

    Returns
    -------
    TableLike
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
    table = set_or_append_column(table, "kind", kind)

    predicate = And(preds=(Not(IsNull("symbol")), Not(IsNull("kind"))))
    table = FilterSpec(predicate).apply_kernel(table)
    if table.num_rows == 0:
        return empty_table(DEF_USE_SCHEMA)

    event_inputs = [
        _column_or_null(table, "code_unit_id", pa.string()),
        _column_or_null(table, "instr_id", pa.string()),
        table["kind"],
        table["symbol"],
    ]
    event_id = prefixed_hash_id(event_inputs, prefix="df_event")

    return pa.Table.from_arrays(
        [
            _column_or_null(table, "file_id", pa.string()),
            _column_or_null(table, "path", pa.string()),
            event_id,
            _column_or_null(table, "instr_id", pa.string()),
            _column_or_null(table, "code_unit_id", pa.string()),
            table["kind"],
            table["symbol"],
            _column_or_null(table, "opname", pa.string()),
            _column_or_null(table, "offset", pa.int32()),
        ],
        schema=DEF_USE_SCHEMA,
    )


def run_reaching_defs(def_use_events: TableLike) -> TableLike:
    """Compute a best-effort reaching-defs edge table.

    This is a conservative, symbol-matching approximation that joins definitions to uses
    within the same code unit. It is deterministic and safe for early-stage analysis.

    Parameters
    ----------
    def_use_events:
        Def/use events table.

    Returns
    -------
    TableLike
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
    return set_or_append_column(table, "edge_id", edge_ids)


def _group_def_use_events(
    def_use_events: TableLike,
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
    for kind, code_unit_id, symbol, event_id, path, file_id in iter_arrays(arrays):
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
