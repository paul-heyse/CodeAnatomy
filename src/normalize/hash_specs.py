"""HashSpec registry for normalized identifiers."""

from arrowdsl.compute.ids import hash_spec_factory

TYPE_EXPR_ID_SPEC = hash_spec_factory(
    prefix="cst_type_expr",
    cols=("path", "bstart", "bend"),
    null_sentinel="None",
)

TYPE_ID_SPEC = hash_spec_factory(
    prefix="type",
    cols=("type_repr",),
    null_sentinel="None",
)

DEF_USE_EVENT_ID_SPEC = hash_spec_factory(
    prefix="df_event",
    cols=("code_unit_id", "instr_id", "kind", "symbol"),
    null_sentinel="None",
)

REACH_EDGE_ID_SPEC = hash_spec_factory(
    prefix="df_reach",
    cols=("def_event_id", "use_event_id"),
    null_sentinel="None",
)

DIAG_ID_SPEC = hash_spec_factory(
    prefix="diag",
    cols=("path", "bstart", "bend", "diag_source", "message"),
    null_sentinel="None",
)

__all__ = [
    "DEF_USE_EVENT_ID_SPEC",
    "DIAG_ID_SPEC",
    "REACH_EDGE_ID_SPEC",
    "TYPE_EXPR_ID_SPEC",
    "TYPE_ID_SPEC",
]
