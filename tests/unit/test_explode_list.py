"""Explode list column kernel checks."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.compute.expr_core import ExplodeSpec
from arrowdsl.compute.kernels import explode_list_column
from tests.utils import values_as_list


def test_explode_list_column_alignment() -> None:
    """Align exploded rows with parent IDs."""
    table = pa.table(
        {
            "parent_id": [1, 2, 3],
            "values": [[10, 11], [], [12]],
        }
    )
    spec = ExplodeSpec(
        parent_keys=("parent_id",),
        list_col="values",
        value_col="dst_id",
        idx_col="idx",
        keep_empty=False,
    )
    result = explode_list_column(table, spec=spec)
    assert values_as_list(result["parent_id"]) == [1, 1, 3]
    assert values_as_list(result["dst_id"]) == [10, 11, 12]
    assert values_as_list(result["idx"]) == [0, 1, 0]


def test_explode_list_column_struct_values() -> None:
    """Explode list<struct> values while preserving order."""
    struct_type = pa.struct([("id", pa.string()), ("score", pa.int64())])
    table = pa.table(
        {
            "parent_id": [1],
            "events": [[{"id": "a", "score": 1}, {"id": "b", "score": 2}]],
        },
        schema=pa.schema(
            [
                ("parent_id", pa.int64()),
                ("events", pa.list_(struct_type)),
            ]
        ),
    )
    spec = ExplodeSpec(
        parent_keys=("parent_id",),
        list_col="events",
        value_col="event",
        idx_col="idx",
        keep_empty=True,
    )
    result = explode_list_column(table, spec=spec)
    assert values_as_list(result["parent_id"]) == [1, 1]
    assert values_as_list(result["idx"]) == [0, 1]
    assert values_as_list(result["event"]) == [
        {"id": "a", "score": 1},
        {"id": "b", "score": 2},
    ]
