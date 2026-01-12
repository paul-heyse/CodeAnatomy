"""Derive bytecode def/use events and reaching-def edges."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.compute.kernels import apply_join, def_use_kind_array
from arrowdsl.compute.predicates import And, FilterSpec, IsNull, Not
from arrowdsl.core.ids import prefixed_hash_id
from arrowdsl.core.interop import TableLike, pc
from arrowdsl.plan.ops import JoinSpec
from arrowdsl.schema.arrays import coalesce_string, set_or_append_column
from arrowdsl.schema.schema import empty_table
from normalize.arrow_utils import column_or_null, compute_array
from normalize.ids import prefixed_hash64
from normalize.pipeline import canonical_sort
from normalize.schema_infer import align_table_to_schema
from normalize.schemas import DEF_USE_SCHEMA, REACHES_SCHEMA

USE_PREFIXES = ("LOAD_",)
DEF_PREFIXES = ("STORE_", "DELETE_")
DEF_OPS = ("IMPORT_NAME", "IMPORT_FROM")


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

    kind = def_use_kind_array(
        table["opname"],
        def_ops=DEF_OPS,
        def_prefixes=DEF_PREFIXES,
        use_prefixes=USE_PREFIXES,
    )
    table = set_or_append_column(table, "kind", kind)

    predicate = And(preds=(Not(IsNull("symbol")), Not(IsNull("kind"))))
    table = FilterSpec(predicate).apply_kernel(table)
    if table.num_rows == 0:
        return empty_table(DEF_USE_SCHEMA)

    event_inputs = [
        column_or_null(table, "code_unit_id", pa.string()),
        column_or_null(table, "instr_id", pa.string()),
        table["kind"],
        table["symbol"],
    ]
    event_id = prefixed_hash_id(event_inputs, prefix="df_event")

    return pa.Table.from_arrays(
        [
            column_or_null(table, "file_id", pa.string()),
            column_or_null(table, "path", pa.string()),
            event_id,
            column_or_null(table, "instr_id", pa.string()),
            column_or_null(table, "code_unit_id", pa.string()),
            table["kind"],
            table["symbol"],
            column_or_null(table, "opname", pa.string()),
            column_or_null(table, "offset", pa.int32()),
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

    defs = _build_def_use_table(def_use_events, kind="def", event_col="def_event_id")
    if defs.num_rows == 0:
        return empty_table(REACHES_SCHEMA)
    uses = _build_def_use_table(
        def_use_events,
        kind="use",
        event_col="use_event_id",
        include_meta=True,
    )
    if uses.num_rows == 0:
        return empty_table(REACHES_SCHEMA)

    joined = apply_join(
        defs,
        uses,
        spec=JoinSpec(
            join_type="inner",
            left_keys=("code_unit_id", "symbol"),
            right_keys=("code_unit_id", "symbol"),
            left_output=("code_unit_id", "symbol", "def_event_id"),
            right_output=("use_event_id", "path", "file_id"),
        ),
        use_threads=True,
    )
    if joined.num_rows == 0:
        return empty_table(REACHES_SCHEMA)

    edge_ids = prefixed_hash64(
        "df_reach",
        [joined["def_event_id"], joined["use_event_id"]],
    )
    out = set_or_append_column(joined, "edge_id", edge_ids)
    out = align_table_to_schema(out, REACHES_SCHEMA)
    return canonical_sort(
        out,
        sort_keys=[
            ("code_unit_id", "ascending"),
            ("symbol", "ascending"),
            ("def_event_id", "ascending"),
            ("use_event_id", "ascending"),
        ],
    )


def _build_def_use_table(
    def_use_events: TableLike,
    *,
    kind: str,
    event_col: str,
    include_meta: bool = False,
) -> TableLike:
    kind_arr = column_or_null(def_use_events, "kind", pa.string())
    mask = pc.equal(kind_arr, pa.scalar(kind))
    table = def_use_events.filter(mask)
    if table.num_rows == 0:
        names = ["code_unit_id", "symbol", event_col]
        if include_meta:
            names.extend(["path", "file_id"])
        return pa.Table.from_arrays(
            [pa.array([], type=pa.string()) for _ in names],
            names=names,
        )

    code_unit = column_or_null(table, "code_unit_id", pa.string())
    symbol = column_or_null(table, "symbol", pa.string())
    event_id = column_or_null(table, "event_id", pa.string())
    valid = pc.and_(pc.is_valid(code_unit), pc.and_(pc.is_valid(symbol), pc.is_valid(event_id)))

    arrays = [
        compute_array("filter", [code_unit, valid]),
        compute_array("filter", [symbol, valid]),
        compute_array("filter", [event_id, valid]),
    ]
    names = ["code_unit_id", "symbol", event_col]
    if include_meta:
        arrays.extend(
            [
                compute_array("filter", [column_or_null(table, "path", pa.string()), valid]),
                compute_array("filter", [column_or_null(table, "file_id", pa.string()), valid]),
            ]
        )
        names.extend(["path", "file_id"])
    return pa.Table.from_arrays(arrays, names=names)
