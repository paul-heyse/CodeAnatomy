"""Tests for SCIP role-derived properties."""

from __future__ import annotations

import arrowdsl.pyarrow_core as pa
from cpg.build_props import PropsInputTables, build_cpg_props_raw
from cpg.kinds import SCIP_ROLE_FORWARD_DEFINITION, SCIP_ROLE_GENERATED, SCIP_ROLE_TEST


def test_scip_role_props() -> None:
    """Emits role props when any occurrence sets the role bit."""
    rows = [
        {"symbol": "sym1", "symbol_roles": SCIP_ROLE_GENERATED},
        {"symbol": "sym1", "symbol_roles": SCIP_ROLE_TEST},
        {"symbol": "sym2", "symbol_roles": SCIP_ROLE_FORWARD_DEFINITION},
    ]
    scip_occurrences = pa.Table.from_pylist(rows)
    props = build_cpg_props_raw(inputs=PropsInputTables(scip_occurrences=scip_occurrences))
    prop_keys = {(row["entity_id"], row["prop_key"]) for row in props.to_pylist()}
    assert ("sym1", "scip_role_generated") in prop_keys
    assert ("sym1", "scip_role_test") in prop_keys
    assert ("sym2", "scip_role_forward_definition") in prop_keys
