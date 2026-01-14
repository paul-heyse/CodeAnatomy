"""Arrow spec tables for CPG node/prop plan specs."""

from __future__ import annotations

from functools import cache

import pyarrow as pa

from arrowdsl.spec.tables.cpg import node_plan_table, prop_table_table
from cpg.spec_registry import (
    edge_prop_spec,
    node_plan_specs,
    prop_table_specs,
    scip_role_flag_prop_spec,
)


@cache
def node_plan_spec_table() -> pa.Table:
    """Return the node plan spec table.

    Returns
    -------
    pa.Table
        Arrow table of node plan specs.
    """
    return node_plan_table(node_plan_specs())


@cache
def prop_table_spec_table() -> pa.Table:
    """Return the property table spec table.

    Returns
    -------
    pa.Table
        Arrow table of property table specs.
    """
    specs = (*prop_table_specs(), scip_role_flag_prop_spec(), edge_prop_spec())
    return prop_table_table(specs)


__all__ = [
    "node_plan_spec_table",
    "prop_table_spec_table",
]
