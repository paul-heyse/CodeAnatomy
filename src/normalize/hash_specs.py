"""HashSpec registry for normalized identifiers."""

from arrowdsl.core.ids import HashSpec

TYPE_EXPR_ID_SPEC = HashSpec(
    prefix="cst_type_expr",
    cols=("path", "bstart", "bend"),
    as_string=True,
    null_sentinel="None",
)

TYPE_ID_SPEC = HashSpec(
    prefix="type",
    cols=("type_repr",),
    as_string=True,
    null_sentinel="None",
)

DEF_USE_EVENT_ID_SPEC = HashSpec(
    prefix="df_event",
    cols=("code_unit_id", "instr_id", "kind", "symbol"),
    as_string=True,
    null_sentinel="None",
)

REACH_EDGE_ID_SPEC = HashSpec(
    prefix="df_reach",
    cols=("def_event_id", "use_event_id"),
    as_string=True,
    null_sentinel="None",
)

DIAG_ID_SPEC = HashSpec(
    prefix="diag",
    cols=("path", "bstart", "bend", "diag_source", "message"),
    as_string=True,
    null_sentinel="None",
)

__all__ = [
    "DEF_USE_EVENT_ID_SPEC",
    "DIAG_ID_SPEC",
    "REACH_EDGE_ID_SPEC",
    "TYPE_EXPR_ID_SPEC",
    "TYPE_ID_SPEC",
]
