"""Normalize identifier specs for stable hashing."""

from __future__ import annotations

from ibis_engine.hashing import HashExprSpec
from ibis_engine.hashing import hash_expr_spec_factory as hash_spec_factory

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

_HASH_SPECS: dict[str, HashExprSpec] = {
    "type_expr_id": TYPE_EXPR_ID_SPEC,
    "type_id": TYPE_ID_SPEC,
    "def_use_event_id": DEF_USE_EVENT_ID_SPEC,
    "reach_edge_id": REACH_EDGE_ID_SPEC,
    "diag_id": DIAG_ID_SPEC,
}


def hash_spec(name: str) -> HashExprSpec:
    """Return a hash spec by registry key.

    Returns
    -------
    HashExprSpec
        Hash specification for the key.
    """
    return _HASH_SPECS[name]


__all__ = [
    "DEF_USE_EVENT_ID_SPEC",
    "DIAG_ID_SPEC",
    "REACH_EDGE_ID_SPEC",
    "TYPE_EXPR_ID_SPEC",
    "TYPE_ID_SPEC",
    "hash_spec",
]
